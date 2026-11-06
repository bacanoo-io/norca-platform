// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
)

// putLB writes a load balancer of the service.
func (e *env) putLB(ns, name, service string) {
	e.backend.PutObject(&api.CustomObject{Group: "norca.io", Kind: "LoadBalancer", Namespace: ns, Name: name,
		Spec: map[string]any{"service": service}})
}

// lbLeaving writes status.leavingNodes of a load balancer, as the
// LoadBalancer operator does once its provider dropped the nodes.
func (e *env) lbLeaving(ns, name string, nodes ...string) {
	e.t.Helper()
	list := []any{}
	for _, n := range nodes {
		list = append(list, n)
	}
	if _, err := e.backend.UpdateObjectStatus(context.Background(), LoadBalancerKind, ns, name,
		map[string]any{"leavingNodes": list}, 0); err != nil {
		e.t.Fatal(err)
	}
}

// edgeNode puts an allocation of the service web (shutdown_delay delay) on
// the node, behind the load balancer web.
func (e *env) edgeNode(id string, delay time.Duration) {
	e.nodes.mu.Lock()
	d := e.nodes.detail(id)
	d.Allocs = []NodeAlloc{{ID: "web-" + id, Namespace: "default", JobID: "traefik", Group: "proxy"}}
	if e.nodes.services == nil {
		e.nodes.services = map[string]allocServices{}
	}
	e.nodes.services["web-"+id] = allocServices{names: []string{"web"}, delay: delay}
	e.nodes.mu.Unlock()
	e.putLB("default", "web", "web")
}

func (e *env) meta(id string) map[string]string {
	e.nodes.mu.Lock()
	defer e.nodes.mu.Unlock()
	return e.nodes.detail(id).Meta
}

// edgeGroup is a group of two nodes; the first runs the proxy behind the load
// balancer (and is the least busy), the second two other allocations.
func edgeGroup(t *testing.T, delay time.Duration) (*env, string) {
	t.Helper()
	extra := map[string]any{"policy": map[string]any{"scale_down": map[string]any{"drain_deadline": "2m"}}}
	e, insts := grown(t, 2, extra)
	victim := "node-" + insts[0].Name
	e.edgeNode(victim, delay)
	e.nodes.setDetail("node-"+insts[1].Name, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("api", 2)})
	e.putGroup(1, extra)
	return e, victim
}

func TestEdge_ScaleDownLeavesTheLoadBalancerFirst(t *testing.T) {
	e, victim := edgeGroup(t, 0)

	// The node gets no new work and is marked; nothing is drained yet.
	e.reconcile()
	if len(e.nodes.drains) != 0 || !slices.Contains(e.nodes.madeIneligible(), victim) {
		t.Fatalf("drains %v, ineligible %v", e.nodes.drains, e.nodes.madeIneligible())
	}
	if e.meta(victim)[MetaLeaving] != "true" {
		t.Fatalf("meta: %v", e.meta(victim))
	}
	if m := removalOf(e.status()); m["phase"] != phaseLeaving || m["node"] != victim {
		t.Fatalf("removal: %+v", m)
	}
	if !e.hasEvent("LeavingTraffic") || !e.hasEvent("ShortShutdownDelay") {
		t.Fatal("no LeavingTraffic or ShortShutdownDelay event")
	}

	// The load balancer has not dropped it yet.
	e.reconcile()
	if len(e.nodes.drains) != 0 {
		t.Fatalf("drained before the load balancer dropped the node: %v", e.nodes.drains)
	}

	// It did: the drain starts.
	e.lbLeaving("default", "web", victim)
	e.reconcile()
	if !slices.Equal(e.nodes.drains, []string{victim}) || !e.hasEvent("TrafficLeft") {
		t.Fatalf("drains %v, TrafficLeft %t", e.nodes.drains, e.hasEvent("TrafficLeft"))
	}
	if m := removalOf(e.status()); m["phase"] != phaseDraining {
		t.Fatalf("removal: %+v", m)
	}
}

func TestEdge_LongShutdownDelayRaisesNoWarning(t *testing.T) {
	e, _ := edgeGroup(t, 45*time.Second)
	e.reconcile()
	if !e.hasEvent("LeavingTraffic") || e.hasEvent("ShortShutdownDelay") {
		t.Fatalf("events: %v", e.backend.ObjectEvents(NodeGroupKind, "", "workers-dc1"))
	}
}

func TestEdge_LeaveTimeout(t *testing.T) {
	e, victim := edgeGroup(t, time.Minute)
	e.reconcile()
	// The load balancer never drops the node: after the drain deadline it
	// is drained anyway.
	e.advance(3 * time.Minute)
	e.reconcile()
	if !slices.Equal(e.nodes.drains, []string{victim}) || !e.hasEvent("LeaveTimeout") {
		t.Fatalf("drains %v, LeaveTimeout %t", e.nodes.drains, e.hasEvent("LeaveTimeout"))
	}
}

func TestEdge_CanceledScaleDownBringsTheTrafficBack(t *testing.T) {
	e, victim := edgeGroup(t, time.Minute)
	e.reconcile()
	if e.meta(victim)[MetaLeaving] != "true" {
		t.Fatal("not marked")
	}
	e.putGroup(2, nil)
	e.reconcile()
	if e.meta(victim)[MetaLeaving] != "" || !e.hasEvent("ScaleDownCanceled") {
		t.Fatalf("meta %v, ScaleDownCanceled %t", e.meta(victim), e.hasEvent("ScaleDownCanceled"))
	}
	e.nodes.mu.Lock()
	eligible := e.nodes.detail(victim).Eligible
	e.nodes.mu.Unlock()
	if !eligible || removalOf(e.status()) != nil {
		t.Fatalf("eligible %t, removal %+v", eligible, removalOf(e.status()))
	}
}

func TestEdge_NoLoadBalancerNoWait(t *testing.T) {
	e, insts := grown(t, 2, nil)
	victim := "node-" + insts[0].Name
	e.nodes.setDetail(victim, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("web", 1)})
	e.nodes.setDetail("node-"+insts[1].Name, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("api", 2)})
	// A load balancer of another service: the node is not behind it.
	e.putLB("default", "other", "other")
	e.putGroup(1, nil)
	e.reconcile()
	if !slices.Equal(e.nodes.drains, []string{victim}) || e.meta(victim)[MetaLeaving] != "" {
		t.Fatalf("drains %v, meta %v", e.nodes.drains, e.meta(victim))
	}
}

func TestEdge_RolloutLeavesBeforeTheDrain(t *testing.T) {
	e := rollEnv(t, 1, nil)
	old := e.firstNode()
	e.edgeNode(old, time.Minute)
	e.setMeta(old, map[string]string{MetaReplace: "true"})
	e.reconcile() // surge
	e.joinAll(nil)
	e.reconcile() // the new node is ready: the old one leaves first
	if len(e.nodes.drains) != 0 || e.meta(old)[MetaLeaving] != "true" {
		t.Fatalf("drains %v, meta %v", e.nodes.drains, e.meta(old))
	}
	if ro := e.rollout(); len(ro.Replacing) != 1 || ro.Replacing[0].Phase != phaseLeaving {
		t.Fatalf("rollout: %+v", ro.Replacing)
	}
	e.lbLeaving("default", "web", old)
	e.reconcile()
	if !slices.Equal(e.nodes.drains, []string{old}) {
		t.Fatalf("drains: %v", e.nodes.drains)
	}
	if ro := e.rollout(); ro.Replacing[0].Phase != phaseDraining {
		t.Fatalf("rollout: %+v", ro.Replacing)
	}
}

func TestEdge_DeletionLeavesBeforeTheDrain(t *testing.T) {
	e, _ := deletionEnv(t, nil)
	e.edgeNode("n1", time.Minute)
	e.reconcile()
	if len(e.nodes.drains) != 0 || e.meta("n1")[MetaLeaving] != "true" ||
		!slices.Contains(e.nodes.madeIneligible(), "n1") {
		t.Fatalf("drains %v, meta %v, ineligible %v", e.nodes.drains, e.meta("n1"), e.nodes.madeIneligible())
	}
	e.lbLeaving("default", "web", "n1")
	e.reconcile()
	if !slices.Equal(e.nodes.drains, []string{"n1"}) {
		t.Fatalf("drains: %v", e.nodes.drains)
	}
}
