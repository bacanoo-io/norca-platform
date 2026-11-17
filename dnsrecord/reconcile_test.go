// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/shoenig/test/must"
	"github.com/shoenig/test/wait"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
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
}

// startOperator starts norca-dns on a fake backend with the zone
// example.com of a mock provider, served over HTTP; records says whether the
// zone lets norca-dns write.
func startOperator(t *testing.T, records bool) *env {
	t.Helper()
	b := fake.New()
	// An object of another kind gives the first listing an index of its
	// own (see the tests of the LoadBalancer operator).
	b.PutObject(&api.CustomObject{Group: "test.norca.io", Kind: "Seed", Name: "seed"})

	z, err := provider.Default.NewZone("example.com", mock.TypeID, nil, nil)
	must.NoError(t, err)
	z.Records = records
	srv := httptest.NewServer(dns.NewHandler(z, "zone-token", nil))
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
	return &env{t: t, b: b, cluster: cluster, zone: z.Provider.(*mock.DNS), r: r}
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
	// the first one keeps it.
	e.put("other", "mail", map[string]any{"name": "mail.shop.example.com", "type": "A", "values": []any{"203.0.113.9"}})
	e.waitFor("conflict with another record", func() bool {
		_, st := e.status("other", "mail")
		return readyReason(st) == ConditionConflict
	})
	time.Sleep(300 * time.Millisecond)
	must.Eq(t, []string{"192.0.2.25"}, e.zone.RecordValues("mail.shop.example.com", "A"))
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
