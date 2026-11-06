// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"maps"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider/mock"
	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
	"github.com/bacanoo-io/norca/norca/controller/fake"
)

// fakeNodes is a NodeLister with the nodes set by the test. A node's detail
// (meta, allocations, drain) is in details; a node without one is empty.
type fakeNodes struct {
	mu         sync.Mutex
	nodes      []Node
	ineligible []string
	details    map[string]*NodeDetail
	drains     []string
	canceled   []string
	purged     []string
	// irreplaceable names the jobs whose allocations could not be placed
	// again without their node.
	irreplaceable map[string]bool
}

func (f *fakeNodes) SetIneligible(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ineligible = append(f.ineligible, id)
	f.setEligible(id, false)
	return nil
}

func (f *fakeNodes) setEligible(id string, eligible bool) {
	for i := range f.nodes {
		if f.nodes[i].ID == id {
			f.nodes[i].Eligible = eligible
		}
	}
	if d := f.details[id]; d != nil {
		d.Eligible = eligible
	}
}

func (f *fakeNodes) detail(id string) *NodeDetail {
	if f.details == nil {
		f.details = map[string]*NodeDetail{}
	}
	d := f.details[id]
	if d == nil {
		d = &NodeDetail{Ready: true, Eligible: true}
		for _, n := range f.nodes {
			if n.ID == id {
				d.Ready, d.Eligible = n.Ready, n.Eligible
			}
		}
		f.details[id] = d
	}
	return d
}

func (f *fakeNodes) Detail(_ context.Context, id string) (*NodeDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	found := false
	for _, n := range f.nodes {
		found = found || n.ID == id
	}
	if !found {
		return nil, ErrNodeNotFound
	}
	d := *f.detail(id)
	return &d, nil
}

func (f *fakeNodes) Drain(_ context.Context, id string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drains = append(f.drains, id)
	d := f.detail(id)
	d.Draining, d.LastDrain = true, "draining"
	f.setEligible(id, false)
	return nil
}

// finishDrain completes the drain of a node: its allocations are gone.
func (f *fakeNodes) finishDrain(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := f.detail(id)
	d.Draining, d.LastDrain, d.Allocs = false, "complete", nil
}

func (f *fakeNodes) CancelDrain(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.canceled = append(f.canceled, id)
	d := f.detail(id)
	d.Draining, d.LastDrain = false, "canceled"
	f.setEligible(id, true)
	return nil
}

func (f *fakeNodes) SetEligible(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setEligible(id, true)
	return nil
}

func (f *fakeNodes) Purge(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purged = append(f.purged, id)
	f.nodes = slices.DeleteFunc(f.nodes, func(n Node) bool { return n.ID == id })
	delete(f.details, id)
	return nil
}

func (f *fakeNodes) Replaceable(_ context.Context, a NodeAlloc, _ string) (bool, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.irreplaceable[a.JobID] {
		return false, "job " + a.JobID + " needs a node of its own", nil
	}
	return true, "", nil
}

// setDetail sets what a node reports.
func (f *fakeNodes) setDetail(id string, d NodeDetail) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.details == nil {
		f.details = map[string]*NodeDetail{}
	}
	f.details[id] = &d
}

// down marks a node down.
func (f *fakeNodes) down(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.nodes {
		if f.nodes[i].ID == id {
			f.nodes[i].Ready = false
		}
	}
	f.detail(id).Ready = false
}

func (f *fakeNodes) madeIneligible() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ineligible...)
}

func (f *fakeNodes) Nodes(context.Context, string, string, string) ([]Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Node(nil), f.nodes...), nil
}

func (f *fakeNodes) set(nodes ...Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes = nodes
}

// env is a reconciler on a fake backend, with a mock provider served over
// HTTP and registered as the NodeProvider "mock".
type env struct {
	t       *testing.T
	backend *fake.Backend
	nodes   *fakeNodes
	mock    *mock.Provider
	r       *Reconciler
	now     time.Time
	url     string
}

func newEnv(t *testing.T, token string) *env {
	t.Helper()
	e := &env{t: t, backend: fake.New(), nodes: &fakeNodes{},
		now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	e.mock = mock.New(map[string]mock.GroupConfig{
		"dc1": {Capacity: provider.Capacity{CPU: 4000, Memory: 8192, Disk: 40000}, Max: 5},
	})
	e.mock.Now = func() time.Time { return e.now }
	srv := httptest.NewServer(provider.NewHandler(e.mock, token, nil))
	t.Cleanup(srv.Close)
	e.url = srv.URL
	e.r = NewReconciler(e.backend, e.nodes, Config{
		Region: "eu", AllowHTTP: true, Now: func() time.Time { return e.now },
	})
	e.r.providers.cacheFor = 0
	e.registerProvider(e.now, "")
	return e
}

func (e *env) registerProvider(heartbeat time.Time, tokenVariable string) {
	spec := map[string]any{"endpoint": e.url, "groups": []any{map[string]any{"name": "dc1"}}}
	if tokenVariable != "" {
		spec["token_variable"] = tokenVariable
	}
	// An apply does not write the status: the heartbeat is a status write,
	// as the registrar of a provider does it.
	e.backend.PutObject(&api.CustomObject{Group: "norca.io", Kind: "NodeProvider", Name: "mock", Spec: spec})
	if _, err := e.backend.UpdateObjectStatus(context.Background(), NodeProviderKind, "", "mock",
		map[string]any{"heartbeat": heartbeat.Format(time.RFC3339)}, 0); err != nil {
		e.t.Fatal(err)
	}
}

// putGroup writes a node group with the replicas.
func (e *env) putGroup(replicas int, extra map[string]any) {
	spec := map[string]any{
		"pool": "workers", "datacenter": "dc1", "min": 0, "max": 10, "replicas": replicas,
		"provider": "mock", "enabled": true,
		"node": map[string]any{"reserved": map[string]any{"cpu": 100, "memory": 192}},
	}
	maps.Copy(spec, extra)
	cur := e.backend.GetObject(NodeGroupKind, "", "workers-dc1")
	obj := &api.CustomObject{Group: "norca.io", Kind: "NodeGroup", Name: "workers-dc1", Spec: spec}
	if cur != nil {
		obj.Status = cur.Status
	}
	e.backend.PutObject(obj)
}

func (e *env) reconcile() time.Duration {
	e.t.Helper()
	d, err := e.r.Reconcile(context.Background(), controller.ObjectKey{Name: "workers-dc1"})
	if err != nil {
		e.t.Fatal(err)
	}
	return d
}

func (e *env) status() map[string]any {
	return normalize(e.backend.GetObject(NodeGroupKind, "", "workers-dc1").Status)
}

func (e *env) instances() []provider.Instance {
	g, err := e.mock.Group(context.Background(), "dc1", nil)
	if err != nil {
		e.t.Fatal(err)
	}
	return g.Instances
}

func conditionOf(status map[string]any, typ string) map[string]any {
	list, _ := status["conditions"].([]any)
	for _, c := range list {
		if m, ok := c.(map[string]any); ok && m["type"] == typ {
			return m
		}
	}
	return nil
}

func (e *env) hasEvent(reason string) bool {
	for _, ev := range e.backend.ObjectEvents(NodeGroupKind, "", "workers-dc1") {
		if ev.Reason == reason {
			return true
		}
	}
	return false
}

func TestReconcile_ScaleUpAndJoin(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(3, nil)

	// Three instances are asked for, in one request.
	if d := e.reconcile(); d != e.r.cfg.JoinCheck {
		t.Fatalf("requeue %v, want the join check", d)
	}
	insts := e.instances()
	if len(insts) != 3 {
		t.Fatalf("instances: %+v", insts)
	}
	for _, inst := range insts {
		if inst.RequestID != "eu/workers-dc1/1" || inst.Labels[LabelNodeGroup] != "workers-dc1" ||
			inst.Labels[LabelRegion] != "eu" || inst.Labels[LabelPool] != "workers" {
			t.Fatalf("instance: %+v", inst)
		}
	}
	st := e.status()
	if st["replicas"] != 0.0 || st["joining"] != 3.0 || st["failed"] != 0.0 || st["lastRequest"] != 1.0 {
		t.Fatalf("status: %+v", st)
	}
	if c := st["capacity"].(map[string]any); c["cpu"] != 4000.0 || c["memory"] != 8192.0 {
		t.Fatalf("capacity: %+v", c)
	}
	if a := st["allocatable"].(map[string]any); a["cpu"] != 3900.0 || a["memory"] != 8000.0 {
		t.Fatalf("allocatable: %+v", a)
	}
	if c := conditionOf(st, CondProviderAvailable); c["status"] != "True" {
		t.Fatalf("ProviderAvailable: %+v", c)
	}
	if c := conditionOf(st, CondProvisioned); c["status"] != "True" {
		t.Fatalf("Provisioned: %+v", c)
	}
	if !e.hasEvent("InstancesRequested") {
		t.Fatal("no InstancesRequested event")
	}

	// The next look asks for nothing more.
	e.reconcile()
	if n := len(e.instances()); n != 3 {
		t.Fatalf("asked again: %d instances", n)
	}

	// Two nodes join: two ready, one joining.
	e.nodes.set(Node{ID: "n1", Instance: insts[0].Name, Ready: true}, Node{ID: "n2", Instance: insts[1].Name, Ready: true},
		Node{ID: "other", Instance: "", Ready: true})
	e.reconcile()
	st = e.status()
	if st["replicas"] != 2.0 || st["joining"] != 1.0 {
		t.Fatalf("after the join: %+v", st)
	}

	// The scale is raised: a new request for the one that is missing.
	e.putGroup(4, nil)
	e.reconcile()
	if n := len(e.instances()); n != 4 {
		t.Fatalf("after the raise: %d instances", n)
	}
	if st := e.status(); st["lastRequest"] != 2.0 {
		t.Fatalf("lastRequest: %+v", st["lastRequest"])
	}
}

func TestReconcile_KeepsTheDecisionOfTheAutoscaler(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	decision := map[string]any{"replicas": 2.0, "check": "blocked-demand"}
	obj := e.backend.GetObject(NodeGroupKind, "", "workers-dc1")
	if _, err := e.backend.UpdateObjectStatus(context.Background(), NodeGroupKind, "", "workers-dc1",
		map[string]any{"decision": decision}, obj.ModifyIndex); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	if st := e.status(); st["decision"] == nil || st["joining"] != 2.0 {
		t.Fatalf("status: %+v", st)
	}
}

// racingBackend writes a decision, as the autoscaler would, between the read
// and the first status write of the operator.
type racingBackend struct {
	*fake.Backend
	once sync.Once
}

func (b *racingBackend) UpdateObjectStatus(ctx context.Context, kind controller.Kind, ns, name string, status map[string]any, cas uint64) (*api.CustomObject, error) {
	b.once.Do(func() {
		cur := b.GetObject(kind, ns, name)
		st := normalize(cur.Status)
		st["decision"] = map[string]any{"replicas": 2.0}
		if _, err := b.Backend.UpdateObjectStatus(ctx, kind, ns, name, st, 0); err != nil {
			panic(err)
		}
	})
	return b.Backend.UpdateObjectStatus(ctx, kind, ns, name, status, cas)
}

func TestReconcile_StatusWriteThatLosesKeepsTheOtherWriter(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	racing := &racingBackend{Backend: e.backend}
	e.r.Objects = racing
	e.reconcile()
	st := e.status()
	if st["decision"] == nil {
		t.Fatalf("the decision written meanwhile is lost: %+v", st)
	}
	if st["joining"] != 2.0 {
		t.Fatalf("the operator's fields are missing: %+v", st)
	}
}

func TestReconcile_ProviderNotAvailable(t *testing.T) {
	e := newEnv(t, "")

	// An unknown provider.
	e.putGroup(2, map[string]any{"provider": "nowhere"})
	if d := e.reconcile(); d != e.r.cfg.RetryAfter {
		t.Fatalf("requeue %v", d)
	}
	if c := conditionOf(e.status(), CondProviderAvailable); c["status"] != "False" || c["reason"] != "UnknownProvider" {
		t.Fatalf("condition: %+v", c)
	}

	// A heartbeat older than the TTL.
	e.putGroup(2, nil)
	e.registerProvider(e.now.Add(-10*time.Minute), "")
	e.reconcile()
	if c := conditionOf(e.status(), CondProviderAvailable); c["reason"] != "ProviderUnavailable" {
		t.Fatalf("condition: %+v", c)
	}
	if n := len(e.instances()); n != 0 {
		t.Fatalf("instances asked from an unavailable provider: %d", n)
	}

	// A group the provider does not offer.
	e.registerProvider(e.now, "")
	e.putGroup(2, map[string]any{"group": "dc9"})
	e.reconcile()
	if c := conditionOf(e.status(), CondProviderAvailable); c["reason"] != "UnknownGroup" {
		t.Fatalf("condition: %+v", c)
	}
}

func TestReconcile_QuotaIsReportedAndRetried(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(7, nil) // the mock group has at most 5
	if d := e.reconcile(); d != e.r.cfg.RetryAfter {
		t.Fatalf("requeue %v", d)
	}
	c := conditionOf(e.status(), CondProvisioned)
	if c["status"] != "False" || c["reason"] != "Quota" {
		t.Fatalf("condition: %+v", c)
	}
	if !e.hasEvent("CreateFailed") {
		t.Fatal("no CreateFailed event")
	}
	if n := len(e.instances()); n != 0 {
		t.Fatalf("instances: %d", n)
	}
}

func TestReconcile_RequestAfterALostStatusWrite(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	// The instances are created, the status write fails.
	e.backend.FailNext("UpdateObjectStatus", errors.New("the servers are away"))
	if _, err := e.r.Reconcile(context.Background(), controller.ObjectKey{Name: "workers-dc1"}); err == nil {
		t.Fatal("the failed status write is not an error")
	}
	// The next look finds them: nothing more is asked for.
	e.reconcile()
	if n := len(e.instances()); n != 2 {
		t.Fatalf("instances: %d", n)
	}
	// A later raise gets a new request number from the instances, not the
	// number of the lost write again.
	if st := e.status(); st["lastRequest"] != nil {
		t.Fatalf("lastRequest was written: %+v", st["lastRequest"])
	}
	e.putGroup(3, nil)
	e.reconcile()
	insts := e.instances()
	if len(insts) != 3 || insts[2].RequestID == insts[0].RequestID {
		t.Fatalf("instances: %+v", insts)
	}
}

func TestReconcile_Token(t *testing.T) {
	e := newEnv(t, "s3cret")
	e.registerProvider(e.now, "nodescaler/mock/token")
	e.putGroup(1, nil)

	// The token Variable is missing.
	e.reconcile()
	if c := conditionOf(e.status(), CondProviderAvailable); c["reason"] != "InvalidProvider" {
		t.Fatalf("condition: %+v", c)
	}
	e.backend.PutVariable(&api.Variable{Namespace: "default", Path: "nodescaler/mock/token", Items: map[string]string{"token": "s3cret"}})
	e.reconcile()
	if n := len(e.instances()); n != 1 {
		t.Fatalf("instances: %d, status %+v", n, e.status())
	}
}

func TestReconcile_LeavesAlone(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, map[string]any{"enabled": false})
	e.reconcile()
	e.putGroup(2, map[string]any{"provider": "", "engine": "external"})
	e.reconcile()
	if n := len(e.instances()); n != 0 {
		t.Fatalf("instances: %d", n)
	}
	if st := e.status(); len(st) != 0 {
		t.Fatalf("status written: %+v", st)
	}
}
