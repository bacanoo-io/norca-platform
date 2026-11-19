// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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

	// beforeWrite, when set, runs once before the provider gets the next
	// PUT or DELETE of a record set: someone else writes the zone between
	// the operator's read and its write.
	beforeWrite *atomic.Pointer[func()]
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
	return startOperatorIn(t, "*", records, zoneParams...)
}

// startOperatorIn is startOperator for the records of one namespace
// (-namespace).
func startOperatorIn(t *testing.T, namespace string, records bool, zoneParams ...map[string]any) *env {
	t.Helper()
	e, c := newOperatorIn(t, namespace, records, zoneParams...)
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
	return newOperatorIn(t, "*", records, zoneParams...)
}

// newOperatorIn is newOperator for the records of one namespace
// (-namespace).
func newOperatorIn(t *testing.T, namespace string, records bool, zoneParams ...map[string]any) (*env, *controller.Controller) {
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
	beforeWrite := &atomic.Pointer[func()]{}
	handler := dns.NewHandler(z, "zone-token", nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPut || req.Method == http.MethodDelete {
			if f := beforeWrite.Swap(nil); f != nil {
				(*f)()
			}
		}
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
		namespace:      namespace,
		requeueReady:   200 * time.Millisecond,
		requeueWaiting: 100 * time.Millisecond,
	}
	c, err := controller.New(b, r, controller.Options{
		Name:        "norca-dns",
		Kind:        Kind,
		Storage:     controller.StorageNative,
		Namespace:   namespace,
		Sources:     []controller.Source{loadBalancerSource(), zoneSource(r.zones)},
		Logger:      logger,
		RateLimiter: controller.NewExponentialBackoff[controller.ObjectKey](10*time.Millisecond, 100*time.Millisecond),
	})
	must.NoError(t, err)
	r.ctrl = c
	r.watchHandovers()
	return &env{t: t, b: b, cluster: cluster, zone: z.Provider.(*mock.DNS), r: r, failDeletes: failDeletes,
		recordsOff: recordsOff, forbidDeletes: forbidDeletes, zoneURL: srv.URL, beforeWrite: beforeWrite}, c
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
	// has: it is the other record that stands in its way, not the zone
	// (the conflict message names the record).
	e.put("other", "mail", map[string]any{"name": "mail.shop.example.com", "type": "A", "values": []any{"192.0.2.25"}})
	e.waitFor("conflict with another record", func() bool {
		_, st := e.status("other", "mail")
		return readyReason(st) == ConditionConflict
	})
	time.Sleep(300 * time.Millisecond)
	_, st := e.status("other", "mail")
	must.Eq(t, ConditionConflict, readyReason(st))
	must.StrContains(t, st.Condition(ConditionConflict).Message, "a DNSRecord in another namespace writes A mail.shop.example.com")
	must.StrNotContains(t, st.Condition(ConditionConflict).Message, "shop/")
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
	must.StrContains(t, st.Condition(ConditionConflict).Message, "a DNSRecord in another namespace writes")
	must.StrNotContains(t, st.Condition(ConditionConflict).Message, "shop/")
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

// A removal leaves a record set whose values are not the ones the status
// says the operator wrote: changed by hand, or written by a record that took
// the set over and whose status does not say so yet (ATM-20: a record that
// lost the right to its name and the record that takes the set over do not
// fight).
func TestRemove_LeavesASetWithOtherValues(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.9"}}))
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})
	e.hold("shop", "web")

	obj, st := e.get("shop", "web")
	kept, err := e.r.remove(ctx, obj, st)
	must.NoError(t, err)
	must.StrContains(t, kept, "other values than norca-dns wrote")
	must.Eq(t, []string{"192.0.2.9"}, e.zone.RecordValues("www.shop.example.com", "A"))

	// Another TTL is another set too.
	must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 60, Values: []string{"192.0.2.2"}}))
	kept, err = e.r.remove(ctx, obj, st)
	must.NoError(t, err)
	must.StrContains(t, kept, "other values than norca-dns wrote")
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
}

// A record set in the zone that norca-dns did not write is not the record's
// because it has the values the record asks for: it is taken over only with
// adopt = true, as a set with other values is (ATM-9 addition). The set of a
// record whose namespace lost the name is norca-dns's, and the record that
// may write the name takes it over without adopt.
func TestKeep_SameValuesAreAdoptedOnlyOnRequest(t *testing.T) {
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}
	setup := func(t *testing.T) *env {
		e, _ := newOperator(t, true)
		e.cluster.allow("shop", "shop.example.com")
		must.NoError(t, e.zone.SetRecords(context.Background(),
			dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
		return e
	}

	t.Run("without adopt", func(t *testing.T) {
		e := setup(t)
		e.put("shop", "web", spec)
		obj, st := e.get("shop", "web")
		out, _, err := e.r.keep(context.Background(), obj, st)
		must.NoError(t, err)
		must.Eq(t, ConditionConflict, out.reason)
		must.StrContains(t, out.conflict, "set adopt = true to take them over")
		must.False(t, st.Owned)
	})

	t.Run("with adopt", func(t *testing.T) {
		e := setup(t)
		adopt := maps.Clone(spec)
		adopt["adopt"] = true
		e.put("shop", "web", adopt)
		obj, st := e.get("shop", "web")
		out, _, err := e.r.keep(context.Background(), obj, st)
		must.NoError(t, err)
		must.True(t, out.ready)
		must.True(t, st.Owned)
		must.SliceContains(t, e.events("shop", "web"), "Adopted")
	})

	t.Run("from a record that lost the name", func(t *testing.T) {
		// An operator of one namespace reads the holders from the servers
		// (the informer of a test operator that does not run is empty).
		e, _ := newOperatorIn(t, "shop", true)
		e.cluster.allow("shop", "shop.example.com")
		must.NoError(t, e.zone.SetRecords(context.Background(),
			dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
		e.put("old", "web", spec)
		e.hold("old", "web")
		e.put("shop", "web", spec)
		obj, st := e.get("shop", "web")
		out, _, err := e.r.keep(context.Background(), obj, st)
		must.NoError(t, err)
		must.True(t, out.ready, must.Sprint(out.message))
		must.True(t, st.Owned)
		must.SliceContains(t, e.events("shop", "web"), "Adopted")
		must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
	})
}

// A record set the operator wrote is the record's also when the status
// write after it failed (the cluster API away for a moment): the status
// says before the write which set the record is about to write, and the
// next reconcile finds that set as its own, without adopt and without a
// Conflict.
func TestKeep_StatusLostAfterTheWrite(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})

	obj, st := e.get("shop", "web")
	out, _, err := e.r.keep(ctx, obj, st)
	must.NoError(t, err)
	must.True(t, out.ready, must.Sprint(out.message))
	e.b.FailNext("UpdateObjectStatus", errors.New("the cluster API is away"))
	must.Error(t, e.r.writeStatus(ctx, obj, st, out))
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))

	obj, st = e.get("shop", "web")
	must.False(t, st.Owned)
	out, _, err = e.r.keep(ctx, obj, st)
	must.NoError(t, err)
	must.True(t, out.ready, must.Sprint(out.message))
	must.True(t, st.Owned)
	must.Nil(t, st.Pending)
	must.SliceNotContains(t, e.events("shop", "web"), "Adopted")
}

// A set whose writing could not be recorded first is not written: the
// record would not know it as its own.
func TestKeep_NoWriteWithoutThePendingStatus(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})

	obj, st := e.get("shop", "web")
	e.b.FailNext("UpdateObjectStatus", errors.New("the cluster API is away"))
	out, _, err := e.r.keep(ctx, obj, st)
	must.Error(t, err)
	must.Eq(t, "Failed", out.reason)
	must.Len(t, 0, e.zone.RecordValues("www.shop.example.com", "A"))
	must.Nil(t, st.Pending)
}

// A set written for a record whose status was lost after the write goes
// with the record: a new set, and new values of a set it held.
func TestRelease_TheSetOfALostStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold bool
	}{
		{"a new set", false},
		{"new values of a set the record held", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newOperator(t, true)
			ctx := context.Background()
			e.cluster.allow("shop", "shop.example.com")
			e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.3"}})
			if tc.hold {
				must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
				e.hold("shop", "web")
			}
			obj, st := e.get("shop", "web")
			out, _, err := e.r.keep(ctx, obj, st)
			must.NoError(t, err)
			must.True(t, out.ready, must.Sprint(out.message))
			must.Eq(t, []string{"192.0.2.3"}, e.zone.RecordValues("www.shop.example.com", "A"))

			// The status write after the zone write is lost; the record
			// is deleted.
			obj, st = e.get("shop", "web")
			must.NoError(t, e.r.release(ctx, obj, st, "the record was deleted"))
			must.Len(t, 0, e.zone.RecordValues("www.shop.example.com", "A"), must.Sprint("the set written for the record"))
			must.False(t, st.Owned)
			must.Nil(t, st.Pending)
		})
	}
}

// A record set someone writes or changes in the zone after the operator read
// it and before its write is not written over: the write depends on the set
// as it was read, the provider checks that, and the record is in Conflict
// (review finding ATM-9).
func TestKeep_SetWrittenMeanwhileIsNotOverwritten(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold bool // the record holds 192.0.2.2 when the write begins
	}{
		{"created while the record writes a new set", false},
		{"changed while the record updates its set", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newOperator(t, true)
			ctx := context.Background()
			e.cluster.allow("shop", "shop.example.com")
			e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.3"}})
			if tc.hold {
				must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
				e.hold("shop", "web")
			}
			meanwhile := func() {
				_ = e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"198.51.100.1"}})
			}
			e.beforeWrite.Store(&meanwhile)

			obj, st := e.get("shop", "web")
			out, _, err := e.r.keep(ctx, obj, st)
			must.NoError(t, err)
			must.Eq(t, ConditionConflict, out.reason)
			must.StrContains(t, out.message, "changed while norca-dns wrote them")
			must.Eq(t, []string{"198.51.100.1"}, e.zone.RecordValues("www.shop.example.com", "A"), must.Sprint("the set written meanwhile"))
			must.Eq(t, tc.hold, st.Owned)
		})
	}
}

// A record set someone changes in the zone after the operator read it for a
// removal stays: the deletion depends on the values that were read (review
// finding ATM-9).
func TestRemove_SetChangedMeanwhileStays(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})
	e.hold("shop", "web")
	meanwhile := func() {
		_ = e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"198.51.100.1"}})
	}
	e.beforeWrite.Store(&meanwhile)

	obj, st := e.get("shop", "web")
	kept, err := e.r.remove(ctx, obj, st)
	must.NoError(t, err)
	must.StrContains(t, kept, "changed in the zone while norca-dns removed it")
	must.Eq(t, []string{"198.51.100.1"}, e.zone.RecordValues("www.shop.example.com", "A"))
}

// A record set of a zone whose provider can't make a write depend on the
// set is written and removed without the check, as before: the operator
// read it right before.
func TestKeep_ProviderWithoutConditions(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	// The zone is served by a provider in front of the mock that refuses
	// every conditional request, as one that can't check them does.
	conditional := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(req.Body)
		var body struct {
			Expect *dns.Expect `json:"expect"`
		}
		if json.Unmarshal(raw, &body) == nil && body.Expect != nil {
			conditional.Add(1)
			wire.WriteError(w, http.StatusNotImplemented, dns.ReasonConditionsUnsupported, "no conditions here")
			return
		}
		proxy, err := http.NewRequestWithContext(req.Context(), req.Method, e.zoneURL+req.URL.Path, strings.NewReader(string(raw)))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		proxy.Header = req.Header
		resp, err := http.DefaultClient.Do(proxy)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	e.b.PutObject(&api.CustomObject{Group: ZoneKind.Group, Kind: ZoneKind.Kind, Name: "example.com", Spec: map[string]any{
		"zone": "example.com", "provider": "dns-lab", "endpoint": srv.URL, "tokenVariable": "providers/dns-lab", "records": true,
	}})
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.3"}})

	obj, st := e.get("shop", "web")
	out, _, err := e.r.keep(ctx, obj, st)
	must.NoError(t, err)
	must.True(t, out.ready, must.Sprint(out.message))
	must.Eq(t, []string{"192.0.2.3"}, e.zone.RecordValues("www.shop.example.com", "A"))

	kept, err := e.r.remove(ctx, obj, st)
	must.NoError(t, err)
	must.Eq(t, "", kept)
	must.Len(t, 0, e.zone.RecordValues("www.shop.example.com", "A"))
	must.Eq(t, 2, conditional.Load(), must.Sprint("the write and the removal are tried with their condition first"))
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

// An operator for one namespace (-namespace) does not keep the records of
// the others, but a name is one for the whole cluster: a record of another
// namespace that holds a record set keeps it from the records of this one,
// and a deletion here does not remove it.
func TestDNSRecord_OneNamespaceSeesTheHoldersOfAll(t *testing.T) {
	e := startOperatorIn(t, "shop", true)
	e.cluster.allow("shop", "shop.example.com")
	e.cluster.allow("other", "example.com")
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}

	// The record of the other namespace holds the set (the operator of
	// that namespace wrote it).
	must.NoError(t, e.zone.SetRecords(context.Background(), dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
	e.put("other", "www", spec)
	e.hold("other", "www")

	// A record here with the same spec: the set is not taken over.
	e.put("shop", "www", spec)
	e.waitFor("the record waits for the holder of the other namespace", func() bool {
		_, st := e.status("shop", "www")
		return readyReason(st) != ""
	})
	_, st := e.status("shop", "www")
	must.Eq(t, ConditionConflict, readyReason(st))
	must.StrContains(t, st.Condition(ConditionConflict).Message, "a DNSRecord in another namespace writes")
	must.StrNotContains(t, st.Condition(ConditionConflict).Message, "other/")
	must.False(t, st.Owned)

	// Deleted here: the set of the other namespace stays.
	e.b.RemoveObject(Kind, "shop", "www")
	e.waitFor("the record is gone", func() bool { return e.b.GetObject(Kind, "shop", "www") == nil })
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
}

// The removal of an operator for one namespace leaves a record set that a
// record of another namespace holds too.
func TestRemove_OneNamespaceLeavesTheSetOfAnotherNamespace(t *testing.T) {
	e, _ := newOperatorIn(t, "shop", true)
	ctx := context.Background()
	must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}
	for _, ns := range []string{"shop", "other"} {
		e.put(ns, "www", spec)
		e.hold(ns, "www")
	}
	obj, err := e.r.ctrl.Objects().Get(ctx, controller.ObjectKey{Namespace: "shop", Name: "www"})
	must.NoError(t, err)
	var st Status
	must.NoError(t, obj.DecodeStatus(&st))

	kept, err := e.r.remove(ctx, obj, &st)
	must.NoError(t, err)
	must.Eq(t, "a DNSRecord in another namespace holds it", kept)
	must.Eq(t, []string{"192.0.2.2"}, e.zone.RecordValues("www.shop.example.com", "A"))
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

// A deletion that can't remove the record set (here: the token Variable of
// the zone is gone) is tried again and again, and the status says how to
// let the record go: deletion_policy = "retain", set while the record is
// being deleted, removes the finalizer and leaves the record set in the
// zone.
func TestDNSRecord_RetainLetsAStuckDeletionGo(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}}
	e.put("shop", "web", spec)
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	})

	e.b.RemoveVariable("default", "providers/dns-lab")
	e.b.RemoveObject(Kind, "shop", "web")
	e.waitFor("the deletion fails, and the status says so", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "DeleteFailed"
	})
	time.Sleep(300 * time.Millisecond)
	obj, st := e.status("shop", "web")
	must.NotNil(t, obj, must.Sprint("the record went while its record set is in the zone"))
	must.StrContains(t, st.Condition(controller.ConditionReady).Message, `deletion_policy = "retain"`)
	must.True(t, st.Owned)

	retained := maps.Clone(spec)
	retained["deletion_policy"] = "retain"
	e.put("shop", "web", retained)
	e.waitFor("the record is gone", func() bool { return e.b.GetObject(Kind, "shop", "web") == nil })
	must.Eq(t, []string{"192.0.2.10"}, e.zone.RecordValues("www.shop.example.com", "A"), must.Sprint("the record set stays"))
}

// With deletion_policy = "retain" a deleted record leaves its record set
// in the zone also when it could be removed.
func TestDNSRecord_RetainedRecordSetStays(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}, "deletion_policy": "retain"})
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	})
	e.b.RemoveObject(Kind, "shop", "web")
	e.waitFor("the record is gone", func() bool { return e.b.GetObject(Kind, "shop", "web") == nil })
	must.Eq(t, []string{"192.0.2.10"}, e.zone.RecordValues("www.shop.example.com", "A"))
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

// get reads a record from the servers with its status, as Reconcile does.
func (e *env) get(ns, name string) (*controller.Object, *Status) {
	e.t.Helper()
	obj, err := e.r.ctrl.Objects().Get(context.Background(), controller.ObjectKey{Namespace: ns, Name: name})
	must.NoError(e.t, err)
	var st Status
	must.NoError(e.t, obj.DecodeStatus(&st))
	return obj, &st
}

// A record whose namespace lost the right to the name it holds gives the
// record set up also when its spec can't be read: a spec made invalid on
// purpose does not keep a name the namespace no longer has (review finding
// SEC-9, ATM-20).
func TestKeep_LostRightReleasesWithAnInvalidSpec(t *testing.T) {
	e, _ := newOperator(t, true)
	ctx := context.Background()
	must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
	e.put("shop", "web", map[string]any{"name": "*.x.example.com", "type": "A", "values": []any{"192.0.2.2"}})
	e.hold("shop", "web")
	e.cluster.allow("shop", "other.example.com")

	obj, st := e.get("shop", "web")
	out, _, err := e.r.keep(ctx, obj, st)
	must.NoError(t, err)
	must.Eq(t, "InvalidSpec", out.reason)
	must.False(t, st.Owned)
	must.Len(t, 0, e.zone.RecordValues("www.shop.example.com", "A"), must.Sprint("the set of the name the namespace lost"))
	must.SliceContains(t, e.events("shop", "web"), "RecordDeleted")
}

// A spec moved to another name or type gives the set of the former one up
// also when the new one can't be written yet (no zone, a zone without
// records or without a heartbeat, no values yet): a name the record no
// longer asks for is not served on (ATM-20).
func TestKeep_MovedSpecReleasesBeforeItWaits(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		spec         map[string]any
		setup        func(e *env)
	}{
		{"no zone", "ZoneNotFound",
			map[string]any{"name": "www.example.org", "type": "A", "values": []any{"192.0.2.2"}}, nil},
		{"a zone without records", "RecordsNotEnabled",
			map[string]any{"name": "www.example.net", "type": "A", "values": []any{"192.0.2.2"}},
			func(e *env) {
				e.b.PutObject(&api.CustomObject{Group: ZoneKind.Group, Kind: ZoneKind.Kind, Name: "example.net", Spec: map[string]any{
					"zone": "example.net", "provider": "dns-lab", "endpoint": e.zoneURL, "tokenVariable": "providers/dns-lab", "records": false,
				}})
			}},
		{"a zone without a heartbeat", "ZoneUnavailable",
			map[string]any{"name": "www.example.net", "type": "A", "values": []any{"192.0.2.2"}},
			func(e *env) {
				e.b.PutObject(&api.CustomObject{Group: ZoneKind.Group, Kind: ZoneKind.Kind, Name: "example.net", Spec: map[string]any{
					"zone": "example.net", "provider": "dns-lab", "endpoint": e.zoneURL, "tokenVariable": "providers/dns-lab", "records": true,
				}})
				_, err := e.b.UpdateObjectStatus(context.Background(), ZoneKind, "", "example.net",
					map[string]any{"heartbeat": time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)}, 0)
				must.NoError(e.t, err)
			}},
		{"no values yet", "NoValues",
			map[string]any{"name": "api.shop.example.com", "type": "A", "source": map[string]any{"load_balancer": "front"}},
			func(e *env) {
				e.b.PutObject(&api.CustomObject{Group: LoadBalancerKind.Group, Kind: LoadBalancerKind.Kind, Namespace: "shop", Name: "front",
					Spec: map[string]any{"service": "front", "port": 443}})
			}},
		{"another type without values yet", "NoValues",
			map[string]any{"name": "www.shop.example.com", "type": "AAAA", "source": map[string]any{"load_balancer": "front"}},
			func(e *env) {
				e.b.PutObject(&api.CustomObject{Group: LoadBalancerKind.Group, Kind: LoadBalancerKind.Kind, Namespace: "shop", Name: "front",
					Spec: map[string]any{"service": "front", "port": 443}})
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newOperator(t, true)
			ctx := context.Background()
			e.cluster.allow("shop", "shop.example.com, example.org, example.net")
			if tc.setup != nil {
				tc.setup(e)
			}
			must.NoError(t, e.zone.SetRecords(ctx, dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
			e.put("shop", "web", tc.spec)
			e.hold("shop", "web")

			obj, st := e.get("shop", "web")
			out, _, err := e.r.keep(ctx, obj, st)
			must.NoError(t, err)
			must.Eq(t, tc.reason, out.reason)
			must.False(t, st.Owned)
			must.Len(t, 0, e.zone.RecordValues("www.shop.example.com", "A"), must.Sprint("the set of the former name"))
		})
	}
}

// The scenario of review finding SEC-9: a namespace makes the spec of its
// record invalid and then loses the suffix of its name to another
// namespace. The set it held goes, and the record of the namespace that may
// write the name now is not refused for good (ATM-20).
func TestDNSRecord_LostRightDoesNotBlockTheNewOwner(t *testing.T) {
	e := startOperator(t, true)
	e.cluster.allow("shop", "shop.example.com")
	e.put("shop", "web", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	e.waitFor("written", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "True"
	})
	e.put("shop", "web", map[string]any{"name": "*.x.example.com", "type": "A", "values": []any{"192.0.2.10"}})
	e.waitFor("invalid", func() bool {
		_, st := e.status("shop", "web")
		return readyReason(st) == "InvalidSpec"
	})

	e.cluster.allow("shop", "other.example.com")
	e.cluster.allow("newshop", "shop.example.com")
	e.put("newshop", "www", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.20"}})
	e.waitFor("the namespace that may write the name now writes it", func() bool {
		_, st := e.status("newshop", "www")
		return readyReason(st) == "True" && st.Owned &&
			slices.Equal(e.zone.RecordValues("www.shop.example.com", "A"), []string{"192.0.2.20"})
	})
	e.waitFor("the record that lost the right holds nothing", func() bool {
		_, st := e.status("shop", "web")
		return !st.Owned
	})
	time.Sleep(300 * time.Millisecond)
	must.Eq(t, []string{"192.0.2.20"}, e.zone.RecordValues("www.shop.example.com", "A"))
	_, st := e.status("newshop", "www")
	must.Eq(t, "True", readyReason(st))
	must.Zero(t, st.Drifts, must.Sprint("the set was deleted under the new owner"))
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

// The kind definition declares every field of Spec, with the deletion
// policies the operator knows.
func TestDNSRecord_KindFiles(t *testing.T) {
	raw, err := os.ReadFile("dnsrecord.schema.json")
	must.NoError(t, err)
	var schema struct {
		Properties map[string]struct {
			Properties map[string]map[string]any `json:"properties"`
		} `json:"properties"`
	}
	must.NoError(t, json.Unmarshal(raw, &schema))
	spec := schema.Properties["spec"].Properties
	for _, field := range []string{"name", "type", "ttl", "values", "source", "adopt", "deletion_policy"} {
		must.MapContainsKey(t, spec, field)
	}
	must.Eq[any](t, []any{DeletionDelete, DeletionRetain}, spec["deletion_policy"]["enum"])
	must.Eq[any](t, DeletionDelete, spec["deletion_policy"]["default"])

	kind, err := os.ReadFile("dnsrecord.kind.hcl")
	must.NoError(t, err)
	must.StrContains(t, string(kind), `column "Deletion" { path = "spec.deletion_policy" }`)
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
		{Spec{Name: "www.example.com", Type: "A", Values: []string{"192.0.2.1"}, DeletionPolicy: "retain"}, ""},
		{Spec{Name: "www.example.com", Type: "A", Values: []string{"192.0.2.1"}, DeletionPolicy: "keep"}, "deletion_policy"},
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

// A condition or an event of a record names another record of its own
// namespace, and of another namespace only that it is one: a team does not
// learn the records of the other namespaces from a conflict over a name.
func TestOtherRecord_NamesOnlyRecordsOfTheSameNamespace(t *testing.T) {
	key := controller.ObjectKey{Namespace: "shop", Name: "www"}
	must.Eq(t, "the DNSRecord shop/web", otherRecord(key, controller.ObjectKey{Namespace: "shop", Name: "web"}))
	must.Eq(t, "a DNSRecord in another namespace", otherRecord(key, controller.ObjectKey{Namespace: "team-b", Name: "web"}))
}
