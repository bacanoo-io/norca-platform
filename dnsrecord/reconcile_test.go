// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shoenig/test/must"
	"github.com/shoenig/test/wait"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/wire"
	"github.com/bacanoo-io/norca-platform/providers/mock"
	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
	"github.com/bacanoo-io/norca/norca/controller/fake"
)

// fakeCluster holds the metadata of the namespaces.
type fakeCluster struct {
	mu   sync.Mutex
	meta map[string]map[string]string
}

func (c *fakeCluster) NamespaceMeta(_ context.Context, ns string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.meta[ns], nil
}

func (c *fakeCluster) allow(ns, suffixes string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.meta[ns] = map[string]string{MetaDNSSuffixes: suffixes}
}

type env struct {
	t       *testing.T
	b       *fake.Backend
	cluster *fakeCluster
	zone    *mock.DNS
	r       *reconciler

	// failDeletes makes the provider fail the deletions of record sets
	// (502), as a name server that is away for a while does.
	failDeletes *atomic.Bool

	// recordsOff makes the provider refuse the records contract (403), as
	// a zone whose configuration no longer says records = true does.
	recordsOff *atomic.Bool

	// forbidDeletes makes something in front of the provider (a proxy)
	// refuse the deletions of record sets with a 403 of its own.
	forbidDeletes *atomic.Bool

	// zoneURL is the endpoint of the zone.
	zoneURL string
}

// zoneRecords writes the records switch of the DNSZone object.
func (e *env) zoneRecords(records bool) {
	e.b.PutObject(&api.CustomObject{Group: ZoneKind.Group, Kind: ZoneKind.Kind, Name: "example.com", Spec: map[string]any{
		"zone": "example.com", "provider": "dns-lab", "endpoint": e.zoneURL, "tokenVariable": "providers/dns-lab", "records": records,
	}})
}

// startOperator starts norca-dns on a fake backend with the zone
// example.com of a mock provider, served over HTTP; records says whether the
// zone lets norca-dns write.
func startOperator(t *testing.T, records bool, zoneParams ...map[string]any) *env {
	t.Helper()
	e, c := newOperator(t, records, zoneParams...)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return e
}

// newOperator is startOperator without running the controller: the methods
// of the reconciler are called by the test.
func newOperator(t *testing.T, records bool, zoneParams ...map[string]any) (*env, *controller.Controller) {
	t.Helper()
	b := fake.New()
	// An object of another kind gives the first listing an index of its
	// own (see the tests of the LoadBalancer operator).
	b.PutObject(&api.CustomObject{Group: "test.norca.io", Kind: "Seed", Name: "seed"})

	var params map[string]any
	if len(zoneParams) > 0 {
		params = zoneParams[0]
	}
	z, err := provider.Default.NewZone("example.com", mock.TypeID, nil, params)
	must.NoError(t, err)
	z.Records = records
	failDeletes, recordsOff, forbidDeletes := &atomic.Bool{}, &atomic.Bool{}, &atomic.Bool{}
	handler := dns.NewHandler(z, "zone-token", nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodDelete && failDeletes.Load() {
			wire.WriteError(w, http.StatusBadGateway, wire.ReasonError, "update of www on ns1.internal.example:53: the name server is away")
			return
		}
		if req.Method == http.MethodDelete && forbidDeletes.Load() {
			http.Error(w, "403 Forbidden", http.StatusForbidden)
			return
		}
		if recordsOff.Load() && strings.HasPrefix(req.URL.Path, "/v1/records/") {
			wire.WriteError(w, http.StatusForbidden, dns.ReasonRecordsDisabled, "the zone example.com does not write records")
			return
		}
		handler.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	b.PutVariable(&api.Variable{Namespace: "default", Path: "providers/dns-lab", Items: api.VariableItems{"token": "zone-token"}})
	b.PutObject(&api.CustomObject{Group: ZoneKind.Group, Kind: ZoneKind.Kind, Name: "example.com", Spec: map[string]any{
		"zone": "example.com", "provider": "dns-lab", "endpoint": srv.URL, "tokenVariable": "providers/dns-lab", "records": records,
	}})

	cluster := &fakeCluster{meta: map[string]map[string]string{}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &reconciler{
		cluster: cluster,
		zones: &zones{
			backend: b, ttl: time.Minute, tokenNamespace: "default", allowHTTP: true,
			now: time.Now, cacheFor: 10 * time.Millisecond,
		},
		logger:         logger,
		now:            time.Now,
		requeueReady:   200 * time.Millisecond,
		requeueWaiting: 100 * time.Millisecond,
	}
	c, err := controller.New(b, r, controller.Options{
		Name:        "norca-dns",
		Kind:        Kind,
		Storage:     controller.StorageNative,
		Namespace:   "*",
		Sources:     []controller.Source{loadBalancerSource(), zoneSource(r.zones)},
		Logger:      logger,
		RateLimiter: controller.NewExponentialBackoff[controller.ObjectKey](10*time.Millisecond, 100*time.Millisecond),
	})
	must.NoError(t, err)
	r.ctrl = c
	return &env{t: t, b: b, cluster: cluster, zone: z.Provider.(*mock.DNS), r: r, failDeletes: failDeletes,
		recordsOff: recordsOff, forbidDeletes: forbidDeletes, zoneURL: srv.URL}, c
}

func (e *env) put(ns, name string, spec map[string]any) {
	e.t.Helper()
	cur := e.b.GetObject(Kind, ns, name)
	obj := &api.CustomObject{Group: Kind.Group, Kind: Kind.Kind, Namespace: ns, Name: name, Spec: spec}
	if cur != nil {
		obj = cur
		obj.Spec = spec
		obj.Generation++
	} else {
		obj.Generation = 1
	}
	e.b.PutObject(obj)
}

func (e *env) status(ns, name string) (*api.CustomObject, Status) {
	var st Status
	obj := e.b.GetObject(Kind, ns, name)
	if obj != nil {
		_ = decode(obj.Status, &st)
	}
	return obj, st
}

func (e *env) waitFor(what string, cond func() bool) {
	e.t.Helper()
	must.Wait(e.t, wait.InitialSuccess(wait.BoolFunc(cond), wait.Timeout(10*time.Second), wait.Gap(20*time.Millisecond)),
		must.Sprint(what))
}

func readyReason(st Status) string {
	c := st.Condition(controller.ConditionReady)
	if c == nil {
		return ""
	}
	if c.Status == controller.ConditionTrue {
		return "True"
	}
	return c.Reason
}

func (e *env) events(ns, name string) []string {
	var out []string
	for _, ev := range e.b.ObjectEvents(Kind, ns, name) {
		out = append(out, ev.Reason)
	}
	return out
}

func TestDNSRecord_WriteDriftAndDelete(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")

	e.put("shop", "web", map[string]any{"name": "www.shop.example.com.", "type": "a", "values": []any{"192.0.2.10", "192.0.2.11"}})
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	})
	_, st := e.status("shop", "web")
	must.Eq(t, "example.com", st.Zone)
	must.Eq(t, "www.shop.example.com", st.Name)
	must.Eq(t, "A", st.Type)
	must.Eq(t, dns.DefaultRecordTTL, st.TTL)
	must.True(t, st.Owned)
	must.Eq(t, []string{"192.0.2.10", "192.0.2.11"}, e.zone.RecordValues("www.shop.example.com", "A"))
	obj, _ := e.status("shop", "web")
	must.SliceContains(t, obj.Finalizers, Finalizer)
	must.SliceContains(t, e.events("shop", "web"), "RecordWritten")

	// Changed in the zone by hand: set back.
	must.NoError(t, e.zone.SetRecords(context.Background(), dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"203.0.113.66"}}))
	e.waitFor("drift set back", func() bool {
		_, st := e.status("shop", "web")
		return st.Drifts == 1 && slices.Equal(e.zone.RecordValues("www.shop.example.com", "A"), []string{"192.0.2.10", "192.0.2.11"})
	})
	must.SliceContains(t, e.events("shop", "web"), "DriftCorrected")

	// A new spec: an update, not a drift.
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "ttl": 60, "values": []any{"192.0.2.12"}})
	e.waitFor("updated", func() bool {
		_, st := e.status("shop", "web")
		return st.TTL == 60 && slices.Equal(e.zone.RecordValues("www.shop.example.com", "A"), []string{"192.0.2.12"})
	})
	_, st = e.status("shop", "web")
	must.Eq(t, 1, st.Drifts)

	// A new name: the old set goes.
	e.put("shop", "web", map[string]any{"name": "api.shop.example.com", "type": "A", "values": []any{"192.0.2.12"}})
	e.waitFor("moved", func() bool {
		return len(e.zone.RecordValues("www.shop.example.com", "A")) == 0 &&
			slices.Equal(e.zone.RecordValues("api.shop.example.com", "A"), []string{"192.0.2.12"})
	})

	// Deleted: the set goes with the object.
	e.b.RemoveObject(Kind, "shop", "web")
	e.waitFor("deleted", func() bool {
		return e.b.GetObject(Kind, "shop", "web") == nil && len(e.zone.RecordValues("api.shop.example.com", "A")) == 0
	})
}

func TestDNSRecord_Refusals(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")

	// A name outside the allow list.
	e.put("shop", "other", map[string]any{"name": "www.example.com", "type": "A", "values": []any{"192.0.2.1"}})
	e.waitFor("not allowed", func() bool {
		_, st := e.status("shop", "other")
		return readyReason(st) == "NotAllowed"
	})
	// A namespace without an allow list.
	e.put("blog", "www", map[string]any{"name": "www.blog.example.com", "type": "A", "values": []any{"192.0.2.1"}})
	e.waitFor("not allowed without a list", func() bool {
		_, st := e.status("blog", "www")
		return readyReason(st) == "NotAllowed"
	})
	// A name of no registered zone.
	e.cluster.allow("far", "example.org")
	e.put("far", "www", map[string]any{"name": "www.example.org", "type": "A", "values": []any{"192.0.2.1"}})
	e.waitFor("no zone", func() bool {
		_, st := e.status("far", "www")
		return readyReason(st) == "ZoneNotFound"
	})
	// An invalid value.
	e.put("shop", "bad", map[string]any{"name": "bad.shop.example.com", "type": "A", "values": []any{"2001:db8::1"}})
	e.waitFor("invalid", func() bool {
		_, st := e.status("shop", "bad")
		return readyReason(st) == "InvalidSpec"
	})
	must.Len(t, 0, e.zone.RecordValues("www.example.com", "A"))
	must.Len(t, 0, e.zone.RecordValues("www.blog.example.com", "A"))
}

func TestDNSRecord_ConflictsAndAdopt(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.cluster.allow("other", "example.com")
	ctx := context.Background()

	// Records of the zone that norca-dns did not write: left alone.
	must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "mail.shop.example.com", Type: "A", TTL: 300, Values: []string{"198.51.100.1"}}))
	e.put("shop", "mail", map[string]any{"name": "mail.shop.example.com", "type": "A", "values": []any{"192.0.2.25"}})
	e.waitFor("conflict with the zone", func() bool {
		_, st := e.status("shop", "mail")
		c := st.Condition(ConditionConflict)
		return readyReason(st) == ConditionConflict && c != nil && c.Status == controller.ConditionTrue
	})
	must.Eq(t, []string{"198.51.100.1"}, e.zone.RecordValues("mail.shop.example.com", "A"))

	// Taken over on request.
	e.put("shop", "mail", map[string]any{"name": "mail.shop.example.com", "type": "A", "values": []any{"192.0.2.25"}, "adopt": true})
	e.waitFor("adopted", func() bool {
		_, st := e.status("shop", "mail")
		return readyReason(st) == "True" && slices.Equal(e.zone.RecordValues("mail.shop.example.com", "A"), []string{"192.0.2.25"})
	})
	must.SliceContains(t, e.events("shop", "mail"), "Adopted")

	// A second record of the same name and type, in another namespace:
	// the first one keeps it. The second one asks for the values the zone
	// has, so it is the other record that stands in its way, not the zone
	// (a set found as wanted would be taken over).
	e.put("other", "mail", map[string]any{"name": "mail.shop.example.com", "type": "A", "values": []any{"192.0.2.25"}})
	e.waitFor("conflict with another record", func() bool {
		_, st := e.status("other", "mail")
		return readyReason(st) == ConditionConflict
	})
	time.Sleep(300 * time.Millisecond)
	_, st := e.status("other", "mail")
	must.Eq(t, ConditionConflict, readyReason(st))
	must.StrContains(t, st.Condition(ConditionConflict).Message, "the DNSRecord shop/mail writes")
	must.False(t, st.Owned)
	must.Eq(t, []string{"192.0.2.25"}, e.zone.RecordValues("mail.shop.example.com", "A"))

	// The record that lost is deleted: the set of the first one stays.
	e.b.RemoveObject(Kind, "other", "mail")
	e.waitFor("the loser is gone", func() bool { return e.b.GetObject(Kind, "other", "mail") == nil })
	time.Sleep(300 * time.Millisecond)
	must.Eq(t, []string{"192.0.2.25"}, e.zone.RecordValues("mail.shop.example.com", "A"))
	_, st = e.status("shop", "mail")
	must.Zero(t, st.Drifts, must.Sprint("the set of the first record was deleted under it"))
}

// The record that holds a record set keeps it when an older record is
// pointed at the same name and type; the record that lost holds nothing, and
// deleting it leaves the set alone.
func TestDNSRecord_HolderKeepsTheSet(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.cluster.allow("other", "example.com")
	ready := func(ns, name string) func() bool {
		return func() bool {
			_, st := e.status(ns, name)
			return readyReason(st) == "True" && st.Owned
		}
	}

	e.put("other", "old", map[string]any{"name": "old.shop.example.com", "type": "A", "values": []any{"192.0.2.1"}})
	e.waitFor("the older record", ready("other", "old"))
	e.put("shop", "www", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})
	e.waitFor("the record that holds the name", ready("shop", "www"))

	// The older record moves to the name the younger one holds, with the
	// same values: it does not take the set over.
	e.put("other", "old", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})
	e.waitFor("the older record is refused", func() bool {
		_, st := e.status("other", "old")
		return readyReason(st) == ConditionConflict
	})
	_, st := e.status("other", "old")
	must.False(t, st.Owned, must.Sprint("a record that lost a name holds no set"))
	must.StrContains(t, st.Condition(ConditionConflict).Message, "the DNSRecord shop/www writes")
	must.Len(t, 0, e.zone.RecordValues("old.shop.example.com", "A"), must.Sprint("the set of the name it left"))
	time.Sleep(300 * time.Millisecond)
	_, st = e.status("shop", "www")
	must.Eq(t, "True", readyReason(st))
	must.True(t, st.Owned)

	e.b.RemoveObject(Kind, "other", "old")
	e.waitFor("the loser is gone", func() bool { return e.b.GetObject(Kind, "other", "old") == nil })
	time.Sleep(300 * time.Millisecond)
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
	_, st = e.status("shop", "www")
	must.Zero(t, st.Drifts, must.Sprint("the set was deleted under the record that holds it"))
}

// A record of a namespace that may not write a name does not keep the name
// from the namespace that may, and deleting it removes nothing.
func TestDNSRecord_NotAllowedRecordHoldsNothing(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")

	e.put("evil", "www", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"203.0.113.66"}})
	e.waitFor("the squatter is not allowed", func() bool {
		_, st := e.status("evil", "www")
		return readyReason(st) == "NotAllowed"
	})
	e.put("shop", "www", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})
	e.waitFor("the namespace that may write the name writes it", func() bool {
		_, st := e.status("shop", "www")
		return readyReason(st) == "True" && st.Owned
	})
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))

	e.b.RemoveObject(Kind, "evil", "www")
	e.waitFor("the squatter is gone", func() bool { return e.b.GetObject(Kind, "evil", "www") == nil })
	time.Sleep(300 * time.Millisecond)
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
	_, st := e.status("shop", "www")
	must.Zero(t, st.Drifts)
}

// A deleted record holds its record set until the operator has removed it:
// a new record of the same spec waits, and is not left with a set that the
// deletion then removes under it.
func TestDNSRecord_DeletedHolderGoesFirst(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}

	e.put("shop", "old", spec)
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "old")
		return readyReason(st) == "True"
	})
	// The name server is away: the deletion can't remove the set yet.
	e.failDeletes.Store(true)
	e.b.RemoveObject(Kind, "shop", "old")
	e.waitFor("the deletion waits for the name server", func() bool {
		return slices.Contains(e.events("shop", "old"), "DeleteFailed")
	})

	e.put("shop", "new", spec)
	e.waitFor("the new record waits for the deleted one", func() bool {
		_, st := e.status("shop", "new")
		return readyReason(st) == ConditionConflict
	})
	_, st := e.status("shop", "new")
	must.False(t, st.Owned)

	e.failDeletes.Store(false)
	e.waitFor("the deleted record is gone and the new one wrote the set", func() bool {
		_, st := e.status("shop", "new")
		return e.b.GetObject(Kind, "shop", "old") == nil && readyReason(st) == "True" &&
			slices.Equal(e.zone.RecordValues("www.shop.example.com", "A"), []string{"192.0.2.2"})
	})
	time.Sleep(300 * time.Millisecond)
	_, st = e.status("shop", "new")
	must.Zero(t, st.Drifts, must.Sprint("the set was deleted under the new record"))
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
}

// hold writes into the status of a record that the operator holds the set
// www.shop.example.com A for it.
func (e *env) hold(ns, name string) {
	e.t.Helper()
	status := map[string]any{}
	if cur := e.b.GetObject(Kind, ns, name); cur != nil {
		maps.Copy(status, cur.Status)
	}
	maps.Copy(status, map[string]any{"zone": "example.com", "name": "www.shop.example.com", "type": "A", "ttl": 300,
		"values": []any{"192.0.2.2"}, "owned": true})
	_, err := e.b.UpdateObjectStatus(context.Background(), Kind, ns, name, status, 0)
	must.NoError(e.t, err)
}

// Two records whose statuses both hold one record set (both wrote it before
// either saw the other): the older one keeps it, the other one gives it up
// without removing it.
func TestDNSRecord_TwoHoldersOfOneSet(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}
	e.put("shop", "first", spec)
	e.waitFor("the first record", func() bool {
		_, st := e.status("shop", "first")
		return readyReason(st) == "True" && st.Owned
	})
	e.put("shop", "second", spec)
	e.waitFor("the second record is refused", func() bool {
		_, st := e.status("shop", "second")
		return readyReason(st) == ConditionConflict
	})

	e.hold("shop", "second")
	e.waitFor("the younger one gives the set up", func() bool {
		_, st := e.status("shop", "second")
		return readyReason(st) == ConditionConflict && !st.Owned
	})
	time.Sleep(300 * time.Millisecond)
	_, st := e.status("shop", "first")
	must.Eq(t, "True", readyReason(st))
	must.True(t, st.Owned)
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))

	// The record that lost is deleted: the set of the first one stays.
	e.b.RemoveObject(Kind, "shop", "second")
	e.waitFor("the loser is gone", func() bool { return e.b.GetObject(Kind, "shop", "second") == nil })
	time.Sleep(300 * time.Millisecond)
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
	_, st = e.status("shop", "first")
	must.Zero(t, st.Drifts, must.Sprint("the set was deleted under the record that holds it"))
}

// A deletion never removes a record set that the status of another record
// that lives holds: whatever brought two holders about, the name stays
// served.
func TestRemove_LeavesTheSetOfAnotherHolder(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}
	for _, name := range []string{"first", "second"} {
		e.put("shop", name, spec)
		e.hold("shop", name)
	}
	get := func(name string) (*controller.Object, *Status) {
		obj, err := e.r.ctrl.Objects().Get(ctx, controller.ObjectKey{Namespace: "shop", Name: name})
		must.NoError(t, err)
		var st Status
		must.NoError(t, obj.DecodeStatus(&st))
		must.True(t, st.Owned)
		return obj, &st
	}

	obj, st := get("first")
	kept, err := e.r.remove(ctx, obj, st)
	must.NoError(t, err)
	must.StrContains(t, kept, "the DNSRecord shop/second holds it")
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))

	// The other holder is gone: the set goes with the last one.
	e.b.RemoveObject(Kind, "shop", "second")
	must.Nil(t, e.b.GetObject(Kind, "shop", "second"))
	kept, err = e.r.remove(ctx, obj, st)
	must.NoError(t, err)
	must.Eq(t, "", kept)
	must.Len(t, 0, e.zone.RecordValues("www.shop.example.com", "A"))
}

// A record of a zone that no longer lets norca-dns write can be deleted: its
// record set stays in the zone, and an event says so.
func TestDNSRecord_DeletedAfterRecordsSwitchedOff(t *testing.T) {
	for _, tc := range []struct {
		name string
		off  func(e *env)
	}{
		{"the DNSZone object says records = false", func(e *env) { e.recordsOff.Store(true); e.zoneRecords(false) }},
		{"the provider refuses, the DNSZone object is behind", func(e *env) { e.recordsOff.Store(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := startOperator(t, true)
			e.cluster.allow("shop", "shop.example.com")
			e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
			e.waitFor("written", func() bool {
				_, st := e.status("shop", "web")
				return readyReason(st) == "True"
			})

			tc.off(e)
			e.b.RemoveObject(Kind, "shop", "web")
			e.waitFor("the record is gone", func() bool { return e.b.GetObject(Kind, "shop", "web") == nil })
			must.Eq(t, []string{"192.0.2.10"}, e.zone.RecordValues("www.shop.example.com", "A"))
		})
	}
}

// A 403 that is not the answer of the provider for a zone without records
// (a proxy in front of it, say) is a failure of the deletion like any
// other: the record stays until its record set is removed.
func TestDNSRecord_ForbiddenDeletionIsNoSwitchedOffZone(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	})

	e.forbidDeletes.Store(true)
	e.b.RemoveObject(Kind, "shop", "web")
	e.waitFor("the deletion fails", func() bool { return slices.Contains(e.events("shop", "web"), "DeleteFailed") })
	time.Sleep(300 * time.Millisecond)
	must.NotNil(t, e.b.GetObject(Kind, "shop", "web"), must.Sprint("the record went with its record set left in the zone"))
	must.SliceNotContains(t, e.events("shop", "web"), "RecordLeft")

	e.forbidDeletes.Store(false)
	e.waitFor("the record and its set are gone", func() bool {
		return e.b.GetObject(Kind, "shop", "web") == nil && len(e.zone.RecordValues("www.shop.example.com", "A")) == 0
	})
}

// The conditions and the events of a record are read by the team of its
// namespace: a failure shows there without the configuration of the
// operator (the endpoint of a zone, the path of its token Variable) and
// without what the provider says of its name server.
func TestDNSRecord_FailuresDoNotShowTheConfiguration(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	ready := func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	}
	e.waitFor("written", ready)
	zone := func(endpoint, tokenVariable string) {
		e.b.PutObject(&api.CustomObject{Group: ZoneKind.Group, Kind: ZoneKind.Kind, Name: "example.com", Spec: map[string]any{
			"zone": "example.com", "provider": "dns-lab", "endpoint": endpoint, "tokenVariable": tokenVariable, "records": true,
		}})
	}
	reason := func(want string) func() bool {
		return func() bool {
			_, st := e.status("shop", "web")
			return readyReason(st) == want
		}
	}
	hidden := func(what string, secret string) {
		t.Helper()
		_, st := e.status("shop", "web")
		texts := []string{st.Condition(controller.ConditionReady).Message}
		for _, ev := range e.b.ObjectEvents(Kind, "shop", "web") {
			texts = append(texts, ev.Message)
		}
		for _, text := range texts {
			if strings.Contains(text, secret) {
				t.Fatalf("%s: the record shows %q: %q", what, secret, text)
			}
		}
	}

	// A provider that can't be reached.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must.NoError(t, err)
	away := ln.Addr().String()
	must.NoError(t, ln.Close())
	zone("http://"+away+"/zones/example.com", "providers/dns-lab")
	e.waitFor("failed", reason("Failed"))
	hidden("an endpoint that can't be reached", away)

	// A token Variable that is not there.
	zone(e.zoneURL, "providers/missing-token")
	e.waitFor("the zone can't be used", reason("ZoneUnavailable"))
	hidden("a token Variable that is missing", "providers/missing-token")

	// The provider fails a deletion with the error of its name server.
	zone(e.zoneURL, "providers/dns-lab")
	e.waitFor("written again", ready)
	e.failDeletes.Store(true)
	e.cluster.allow("shop", "other.example.com")
	e.waitFor("the removal failed", func() bool {
		return slices.Contains(e.events("shop", "web"), "DeleteFailed") && reason("Failed")()
	})
	hidden("a deletion the provider failed", "ns1.internal.example")
}

// A namespace that lost the right to a name no longer has it served: the
// record set the operator wrote for it goes.
func TestDNSRecord_AllowListWithdrawn(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	})

	e.cluster.allow("shop", "other.example.com")
	e.waitFor("not allowed, and the set is gone", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "NotAllowed" && !st.Owned && len(e.zone.RecordValues("www.shop.example.com", "A")) == 0
	})
	must.SliceContains(t, e.events("shop", "web"), "RecordDeleted")

	e.cluster.allow("shop", "shop.example.com")
	e.waitFor("written again", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True" && st.Owned && slices.Equal(e.zone.RecordValues("www.shop.example.com", "A"), []string{"192.0.2.10"})
	})
}

// The record set of a record that follows a load balancer stays while the
// load balancer has no address for a while, and goes when the load balancer
// is deleted: its addresses are someone else's from then on.
func TestDNSRecord_LoadBalancerDeleted(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.b.PutObject(&api.CustomObject{Group: LoadBalancerKind.Group, Kind: LoadBalancerKind.Kind, Namespace: "shop", Name: "front",
		Spec: map[string]any{"service": "front", "port": 443}})
	setAddresses := func(addresses ...any) {
		_, err := e.b.UpdateObjectStatus(context.Background(), LoadBalancerKind, "shop", "front", map[string]any{"addresses": addresses}, 0)
		must.NoError(t, err)
	}
	setAddresses("198.51.100.7:443")
	e.put("shop", "front", map[string]any{"name": "front.shop.example.com", "type": "A", "source": map[string]any{"load_balancer": "front"}})
	e.waitFor("the address of the load balancer", func() bool {
		return slices.Equal(e.zone.RecordValues("front.shop.example.com", "A"), []string{"198.51.100.7"})
	})

	// No address for a while: the last one stays served.
	setAddresses()
	e.waitFor("no values", func() bool {
		_, st := e.status("shop", "front")
		return readyReason(st) == "NoValues"
	})
	time.Sleep(300 * time.Millisecond)
	_, st := e.status("shop", "front")
	must.True(t, st.Owned)
	must.Eq(t, []string{"198.51.100.7"}, e.zone.RecordValues("front.shop.example.com", "A"))

	// The load balancer is deleted: the name no longer points at its
	// address.
	e.b.RemoveObject(LoadBalancerKind, "shop", "front")
	e.waitFor("the set is gone with the load balancer", func() bool {
		_, st := e.status("shop", "front")
		return readyReason(st) == "NoValues" && !st.Owned && len(e.zone.RecordValues("front.shop.example.com", "A")) == 0
	})
}

// A spec that is changed while its record is being written: the reconcile
// that follows knows what the one before it wrote, and removes the set of
// the former name. (The cache of the operator can be behind its own status
// write at that moment.)
func TestDNSRecord_RenamedWhileWritten(t *testing.T) {
	e := startOperator(t, true, map[string]any{"latency": "100ms"})
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	// The reconcile has begun (the finalizer is on) and is at the slow
	// provider: the name changes under it.
	e.waitFor("the reconcile has begun", func() bool {
		obj := e.b.GetObject(Kind, "shop", "web")
		return obj != nil && slices.Contains(obj.Finalizers, Finalizer)
	})
	e.put("shop", "web", map[string]any{"name": "api.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})

	e.waitFor("the new name is written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True" && st.Name == "api.shop.example.com" &&
			slices.Equal(e.zone.RecordValues("api.shop.example.com", "A"), []string{"192.0.2.10"})
	})
	must.Len(t, 0, e.zone.RecordValues("www.shop.example.com", "A"), must.Sprint("the set of the former name was left behind"))
}

// The heartbeat a provider writes into its DNSZone objects every 30 seconds
// is no reason to compare every record with its zone: the zone source asks
// for that only when a zone appears or goes, its spec changes, or it turns
// available or unavailable.
func TestZoneSource_HeartbeatsDoNotReconcile(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	src := zoneSource(e.r.zones).(*controller.EventSource)
	keys := []controller.ObjectKey{{Namespace: "shop", Name: "a"}, {Namespace: "shop", Name: "b"}}
	sc := &controller.SourceContext{Objects: func() []*controller.Object {
		return []*controller.Object{{Key: keys[0]}, {Key: keys[1]}}
	}}
	event := func() []controller.ObjectKey { return src.Map(&api.Event{}, sc) }
	heartbeat := func(at time.Time) {
		_, err := e.b.UpdateObjectStatus(ctx, ZoneKind, "", "example.com", map[string]any{"heartbeat": at.UTC().Format(time.RFC3339)}, 0)
		must.NoError(t, err)
	}

	must.Eq(t, keys, event(), must.Sprint("the first event: the zones are new to the source"))
	must.Len(t, 0, event(), must.Sprint("nothing changed"))

	heartbeat(time.Now().Add(-20 * time.Second))
	must.Len(t, 0, event(), must.Sprint("the first heartbeat of a zone that was available"))
	heartbeat(time.Now())
	must.Len(t, 0, event(), must.Sprint("a heartbeat"))

	// The spec of a zone.
	e.zoneRecords(false)
	must.Eq(t, keys, event(), must.Sprint("records switched off"))
	must.Len(t, 0, event())
	e.zoneRecords(true)
	must.Eq(t, keys, event(), must.Sprint("records switched on"))

	// A provider that was away is back: its zone is available again.
	heartbeat(time.Now().Add(-time.Hour))
	_ = event()
	heartbeat(time.Now())
	must.Eq(t, keys, event(), must.Sprint("the zone is available again"))
	must.Len(t, 0, event())

	// A zone goes, a zone comes.
	e.b.RemoveObject(ZoneKind, "", "example.com")
	must.Eq(t, keys, event(), must.Sprint("the zone is gone"))
	e.zoneRecords(true)
	must.Eq(t, keys, event(), must.Sprint("the zone is registered"))

	// The event still drops the cached zones: a reconcile after it sees
	// the zones as they are.
	e.zoneRecords(false)
	_ = event()
	_, err := e.r.zones.find(ctx, "www.example.com")
	must.ErrorIs(t, err, errRecordsDisabled)
}

func TestDNSRecord_ZoneWithoutRecords(t *testing.T) {
	e := startOperator(t, false)
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	e.waitFor("records not enabled", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "RecordsNotEnabled"
	})
}

// A failure of the provider is in the status: a record that was ready does
// not stay Ready, and a new one gets its condition.
func TestDNSRecord_FailedIsInTheStatus(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	})

	// The provider refuses the token from now on (401).
	e.b.PutVariable(&api.Variable{Namespace: "default", Path: "providers/dns-lab", Items: api.VariableItems{"token": "wrong"}})
	e.waitFor("the ready record is Failed", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "Failed"
	})
	_, st := e.status("shop", "web")
	must.True(t, st.Owned, must.Sprint("a failure does not give up the record set"))
	must.Eq(t, controller.ConditionTrue, st.Condition(controller.ConditionDegraded).Status)

	e.put("shop", "new", map[string]any{"name": "new.shop.example.com", "type": "A", "values": []any{"192.0.2.11"}})
	e.waitFor("the new record is Failed", func() bool {
		_, st := e.status("shop", "new")
		return readyReason(st) == "Failed"
	})

	// The token is right again: both are written.
	e.b.PutVariable(&api.Variable{Namespace: "default", Path: "providers/dns-lab", Items: api.VariableItems{"token": "zone-token"}})
	e.waitFor("ready again", func() bool {
		_, a := e.status("shop", "web")
		_, b := e.status("shop", "new")
		return readyReason(a) == "True" && readyReason(b) == "True"
	})
}

func TestDNSRecord_LoadBalancerSource(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")

	e.put("shop", "front", map[string]any{"name": "front.shop.example.com", "type": "A", "source": map[string]any{"load_balancer": "front"}})
	e.waitFor("no load balancer yet", func() bool {
		_, st := e.status("shop", "front")
		return readyReason(st) == "NoValues"
	})

	// The LoadBalancer operator writes the addresses into the status.
	e.b.PutObject(&api.CustomObject{Group: LoadBalancerKind.Group, Kind: LoadBalancerKind.Kind, Namespace: "shop", Name: "front",
		Spec: map[string]any{"service": "front", "port": 443}})
	setAddresses := func(addresses ...any) {
		_, err := e.b.UpdateObjectStatus(context.Background(), LoadBalancerKind, "shop", "front", map[string]any{"addresses": addresses}, 0)
		must.NoError(t, err)
	}
	setAddresses("198.51.100.7:443", "[2001:db8::7]:443")
	e.waitFor("the address of the load balancer", func() bool {
		return slices.Equal(e.zone.RecordValues("front.shop.example.com", "A"), []string{"198.51.100.7"})
	})

	setAddresses("198.51.100.8:443")
	e.waitFor("the new address", func() bool {
		return slices.Equal(e.zone.RecordValues("front.shop.example.com", "A"), []string{"198.51.100.8"})
	})
	_, st := e.status("shop", "front")
	must.Zero(t, st.Drifts)
}

// A change of a load balancer reconciles the records that take their values
// from it, not every record that has a load balancer as its source.
func TestLoadBalancerSource_OnlyTheRecordsOfTheLoadBalancer(t *testing.T) {
	src := loadBalancerSource().(*controller.EventSource)
	record := func(ns, name string, spec map[string]any) *controller.Object {
		raw, err := json.Marshal(spec)
		must.NoError(t, err)
		return &controller.Object{Kind: Kind, Key: controller.ObjectKey{Namespace: ns, Name: name}, Spec: raw}
	}
	objects := []*controller.Object{
		record("shop", "front", map[string]any{"name": "front.shop.example.com", "type": "A", "source": map[string]any{"load_balancer": "front"}}),
		record("shop", "front6", map[string]any{"name": "front.shop.example.com", "type": "AAAA", "source": map[string]any{"load_balancer": "front"}}),
		record("shop", "back", map[string]any{"name": "back.shop.example.com", "type": "A", "source": map[string]any{"load_balancer": "back"}}),
		record("blog", "front", map[string]any{"name": "front.blog.example.com", "type": "A", "source": map[string]any{"load_balancer": "front"}}),
		record("shop", "www", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.1"}}),
	}
	sc := &controller.SourceContext{Objects: func() []*controller.Object { return objects }}
	event := func(lb *api.CustomObject) *api.Event {
		raw, err := json.Marshal(map[string]any{"Object": lb})
		must.NoError(t, err)
		var payload map[string]any
		must.NoError(t, json.Unmarshal(raw, &payload))
		return &api.Event{Topic: api.TopicCustomObject, Namespace: lb.Namespace, Payload: payload}
	}

	keys := src.Map(event(&api.CustomObject{Group: LoadBalancerKind.Group, Kind: LoadBalancerKind.Kind, Namespace: "shop", Name: "front"}), sc)
	must.Eq(t, []controller.ObjectKey{{Namespace: "shop", Name: "front"}, {Namespace: "shop", Name: "front6"}}, keys)

	keys = src.Map(event(&api.CustomObject{Group: LoadBalancerKind.Group, Kind: LoadBalancerKind.Kind, Namespace: "shop", Name: "gone"}), sc)
	must.Len(t, 0, keys, must.Sprint("a load balancer no record takes its values from"))

	// An event without the object: every record with a load balancer as
	// its source is looked at, as nothing says which one changed.
	keys = src.Map(&api.Event{Topic: api.TopicCustomObject}, sc)
	must.Eq(t, []controller.ObjectKey{{Namespace: "shop", Name: "front"}, {Namespace: "shop", Name: "front6"},
		{Namespace: "shop", Name: "back"}, {Namespace: "blog", Name: "front"}}, keys)
}

func TestSpecAndValues(t *testing.T) {
	for _, tc := range []struct {
		spec Spec
		err  string
	}{
		{Spec{Name: "www.example.com", Type: "A"}, "values or source"},
		{Spec{Name: "www.example.com", Type: "A", Values: []string{"x"}, Source: &Source{LoadBalancer: "lb"}}, "exclude"},
		{Spec{Name: "www.example.com", Type: "TXT", Source: &Source{LoadBalancer: "lb"}}, "TXT"},
		{Spec{Name: "www.example.com", Type: "MX", Values: []string{"x"}}, "type"},
		{Spec{Name: "*.example.com", Type: "A", Values: []string{"192.0.2.1"}}, "not a DNS name"},
		{Spec{Name: "www.example.com", Type: "a", Values: []string{"192.0.2.1"}}, ""},
	} {
		err := tc.spec.normalize()
		if tc.err == "" {
			must.NoError(t, err)
			continue
		}
		must.ErrorContains(t, err, tc.err)
	}

	must.True(t, allowed("www.team-a.example.com", map[string]string{MetaDNSSuffixes: "team-b.example.com, team-a.example.com"}))
	must.True(t, allowed("team-a.example.com", map[string]string{MetaDNSSuffixes: "*.team-a.example.com"}))
	must.False(t, allowed("www.notteam-a.example.com", map[string]string{MetaDNSSuffixes: "team-a.example.com"}))
	must.False(t, allowed("www.example.com", nil))

	addrs := []string{"198.51.100.7:443", "[2001:db8::7]:443", "lb-1.cloud.example.net:443", "198.51.100.7"}
	must.Eq(t, []string{"198.51.100.7"}, loadBalancerValues("A", addrs))
	must.Eq(t, []string{"2001:db8::7"}, loadBalancerValues("AAAA", addrs))
	must.Eq(t, []string{"lb-1.cloud.example.net"}, loadBalancerValues("CNAME", addrs))
}
