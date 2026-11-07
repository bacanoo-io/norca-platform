// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
)

// grown is an env with a group of n instances whose nodes joined, in the
// order of their names (the first registered first).
func grown(t *testing.T, n int, extra map[string]any) (*env, []provider.Instance) {
	t.Helper()
	e := newEnv(t, "")
	e.putGroup(n, extra)
	e.reconcile()
	insts := e.instances()
	slices.SortFunc(insts, func(a, b provider.Instance) int { return strings.Compare(a.Name, b.Name) })
	var nodes []Node
	for i, inst := range insts {
		nodes = append(nodes, Node{ID: "node-" + inst.Name, Name: inst.Name, Instance: inst.Name,
			Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: uint64(100 + i)})
	}
	e.nodes.set(nodes...)
	e.reconcile()
	if st := e.status(); st["replicas"] != float64(n) {
		t.Fatalf("status: %+v", st)
	}
	return e, insts
}

func allocs(job string, n int) []NodeAlloc {
	var out []NodeAlloc
	for i := range n {
		out = append(out, NodeAlloc{ID: job + "-" + string(rune('a'+i)), Namespace: "default", JobID: job, Group: "g"})
	}
	return out
}

func removalOf(st map[string]any) map[string]any {
	m, _ := st["removal"].(map[string]any)
	return m
}

func TestScaleDown_DrainsTheLeastBusyThenDeletes(t *testing.T) {
	e, insts := grown(t, 3, nil)
	// The first two have one allocation each, the third two: the first
	// (the oldest of the least busy) goes.
	e.nodes.setDetail("node-"+insts[0].Name, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("web", 1)})
	e.nodes.setDetail("node-"+insts[1].Name, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("web", 1)})
	e.nodes.setDetail("node-"+insts[2].Name, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("api", 2)})
	victim := "node-" + insts[0].Name

	e.putGroup(2, nil)
	e.reconcile()
	if !slices.Equal(e.nodes.drains, []string{victim}) {
		t.Fatalf("drains: %v", e.nodes.drains)
	}
	st := e.status()
	if m := removalOf(st); m["phase"] != phaseDraining || m["node"] != victim || m["instance"] != insts[0].ID {
		t.Fatalf("removal: %+v", m)
	}
	if st["replicas"] != 3.0 {
		t.Fatalf("the node being drained is still ready: %+v", st)
	}
	if !e.hasEvent("ScaleDown") {
		t.Fatal("no ScaleDown event")
	}

	// The drain goes on: nothing is deleted.
	e.advance(time.Minute)
	e.reconcile()
	if len(e.instances()) != 3 {
		t.Fatal("deleted before the drain was complete")
	}

	// The drain is complete: the instance is deleted, and its node is
	// waited for.
	e.nodes.finishDrain(victim)
	e.reconcile()
	if len(e.instances()) != 2 || !e.hasEvent("InstanceDeleted") {
		t.Fatalf("instances %d, events %v", len(e.instances()), e.backend.ObjectEvents(NodeGroupKind, "", "workers-dc1"))
	}
	if m := removalOf(e.status()); m["phase"] != phaseDeleted {
		t.Fatalf("removal: %+v", m)
	}

	// The node still up is not a foreign node; once down it is purged.
	e.reconcile()
	if e.hasEvent(CondForeignNode) || len(e.nodes.madeIneligible()) != 0 {
		t.Fatal("the node of the deleted instance was taken for a foreign one")
	}
	e.nodes.down(victim)
	e.reconcile()
	if !slices.Equal(e.nodes.purged, []string{victim}) {
		t.Fatalf("purged: %v", e.nodes.purged)
	}
	st = e.status()
	if removalOf(st) != nil || st["replicas"] != 2.0 || e.hasEvent("NodeWithoutInstance") {
		t.Fatalf("status: %+v", st)
	}
	if c := conditionOf(st, CondScaleDownBlocked); c["status"] != "False" {
		t.Fatalf("ScaleDownBlocked: %+v", c)
	}
}

func TestScaleDown_UnjoinedInstanceFirst(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(3, nil)
	e.reconcile()
	insts := e.instances()
	slices.SortFunc(insts, func(a, b provider.Instance) int { return strings.Compare(a.Name, b.Name) })
	// Two joined, one did not yet.
	e.nodes.set(Node{ID: "n1", Name: insts[0].Name, Instance: insts[0].Name, Group: "workers-dc1", Ready: true, Eligible: true},
		Node{ID: "n2", Name: insts[1].Name, Instance: insts[1].Name, Group: "workers-dc1", Ready: true, Eligible: true})
	e.reconcile()

	e.putGroup(2, nil)
	e.reconcile()
	left := e.instances()
	if len(left) != 2 || slices.ContainsFunc(left, func(i provider.Instance) bool { return i.ID == insts[2].ID }) {
		t.Fatalf("instances: %+v", left)
	}
	if len(e.nodes.drains) != 0 || removalOf(e.status()) != nil {
		t.Fatal("a node was drained for an instance that had not joined")
	}
}

func TestScaleDown_EmptySelector(t *testing.T) {
	extra := map[string]any{"policy": map[string]any{"scale_down": map[string]any{"selector": "empty"}}}
	e, insts := grown(t, 2, extra)
	busy, empty := "node-"+insts[0].Name, "node-"+insts[1].Name
	e.nodes.setDetail(busy, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("match", 1)})

	e.putGroup(1, extra)
	if d := e.reconcile(); d != settleTime {
		t.Fatalf("requeue %v, want the settle time", d)
	}
	if !slices.Equal(e.nodes.madeIneligible(), []string{empty}) || len(e.nodes.drains) != 0 {
		t.Fatalf("ineligible %v, drains %v", e.nodes.madeIneligible(), e.nodes.drains)
	}
	// Not before the settle time.
	e.reconcile()
	if len(e.instances()) != 2 {
		t.Fatal("deleted before the settle time")
	}
	e.advance(settleTime + time.Second)
	e.reconcile()
	if len(e.instances()) != 1 {
		t.Fatalf("instances: %+v", e.instances())
	}

	// Work that lands in the settle time keeps the node.
	e, insts = grown(t, 2, extra)
	busy, empty = "node-"+insts[0].Name, "node-"+insts[1].Name
	e.nodes.setDetail(busy, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("match", 1)})
	e.putGroup(1, extra)
	e.reconcile()
	e.nodes.setDetail(empty, NodeDetail{Ready: true, Eligible: false, Allocs: allocs("late", 1)})
	e.advance(settleTime + time.Second)
	e.reconcile()
	if len(e.instances()) != 2 || !e.hasEvent("NodeNotEmpty") || removalOf(e.status()) != nil {
		t.Fatalf("instances %d, status %+v", len(e.instances()), e.status())
	}
	if d, _ := e.nodes.Detail(t.Context(), empty); !d.Eligible {
		t.Fatal("the node that received work is not eligible again")
	}
}

func TestScaleDown_Veto(t *testing.T) {
	e, insts := grown(t, 2, nil)
	// One node is protected, the other holds work that can't go elsewhere.
	e.nodes.setDetail("node-"+insts[0].Name, NodeDetail{Ready: true, Eligible: true, Meta: map[string]string{MetaProtect: "true"}})
	e.nodes.setDetail("node-"+insts[1].Name, NodeDetail{Ready: true, Eligible: true, Allocs: allocs("proxy", 1)})
	e.nodes.irreplaceable = map[string]bool{"proxy": true}

	e.putGroup(1, nil)
	d := e.reconcile()
	st := e.status()
	c := conditionOf(st, CondScaleDownBlocked)
	if c["status"] != "True" || c["reason"] != "NoCandidate" {
		t.Fatalf("ScaleDownBlocked: %+v", c)
	}
	if c["retryAfter"] != e.now.Add(vetoFirst).Format(time.RFC3339) || d != vetoFirst {
		t.Fatalf("retryAfter %v, requeue %v", c["retryAfter"], d)
	}
	if msg, _ := c["message"].(string); !strings.Contains(msg, "protected") || !strings.Contains(msg, "proxy") {
		t.Fatalf("message: %q", msg)
	}
	if len(e.nodes.drains) != 0 || e.eventCount(CondScaleDownBlocked) != 1 {
		t.Fatal("a node was drained, or the veto was not told once")
	}

	// Before retryAfter nothing is tried again; after it, the wait doubles.
	e.advance(5 * time.Minute)
	e.reconcile()
	if e.eventCount(CondScaleDownBlocked) != 1 {
		t.Fatal("tried again before retryAfter")
	}
	e.advance(6 * time.Minute)
	e.reconcile()
	c = conditionOf(e.status(), CondScaleDownBlocked)
	if c["retryAfter"] != e.now.Add(2*vetoFirst).Format(time.RFC3339) {
		t.Fatalf("second veto: %+v", c)
	}

	// The autoscaler sets the scale back: the condition turns False at
	// retryAfter.
	e.putGroup(2, nil)
	e.advance(21 * time.Minute)
	e.reconcile()
	if c := conditionOf(e.status(), CondScaleDownBlocked); c["status"] != "False" {
		t.Fatalf("ScaleDownBlocked after retryAfter: %+v", c)
	}
}

func TestScaleDown_DrainStuck(t *testing.T) {
	e, _ := grown(t, 2, nil)
	e.putGroup(1, nil)
	e.reconcile()
	victim := e.nodes.drains[0]

	e.advance(15*time.Minute + drainGrace + time.Second)
	e.reconcile()
	if !slices.Equal(e.nodes.canceled, []string{victim}) || len(e.instances()) != 2 {
		t.Fatalf("canceled %v, instances %d", e.nodes.canceled, len(e.instances()))
	}
	st := e.status()
	if c := conditionOf(st, CondScaleDownBlocked); c["status"] != "True" || c["reason"] != "DrainStuck" {
		t.Fatalf("ScaleDownBlocked: %+v", c)
	}
	if removalOf(st) != nil || !e.hasEvent("DrainStuck") {
		t.Fatalf("status: %+v", st)
	}
}

func TestScaleDown_RaisedAgainKeepsTheNode(t *testing.T) {
	e, _ := grown(t, 2, nil)
	e.putGroup(1, nil)
	e.reconcile()
	victim := e.nodes.drains[0]

	e.putGroup(2, nil)
	e.reconcile()
	if !slices.Equal(e.nodes.canceled, []string{victim}) || !e.hasEvent("ScaleDownCanceled") {
		t.Fatalf("canceled %v", e.nodes.canceled)
	}
	if removalOf(e.status()) != nil || len(e.instances()) != 2 {
		t.Fatalf("status: %+v", e.status())
	}
}

func TestScaleDown_GoesOnAfterARestart(t *testing.T) {
	e, _ := grown(t, 2, nil)
	e.putGroup(1, nil)
	e.reconcile()
	victim := e.nodes.drains[0]

	// Another operator instance takes over: it reads the removal from the
	// status.
	e.r = NewReconciler(e.backend, e.nodes, Config{Region: "eu", AllowHTTP: true, Now: func() time.Time { return e.now }})
	e.r.providers.cacheFor = 0
	e.nodes.finishDrain(victim)
	e.reconcile()
	if len(e.instances()) != 1 || len(e.nodes.drains) != 1 {
		t.Fatalf("instances %d, drains %v", len(e.instances()), e.nodes.drains)
	}
}

// outByHand makes a node ineligible as a person does (norca node drain, or
// node eligibility -disable), before the operator touches it.
func (e *env) outByHand(id string) {
	e.nodes.mu.Lock()
	defer e.nodes.mu.Unlock()
	e.nodes.detail(id)
	e.nodes.setEligible(id, false)
}

func (e *env) eligible(id string) bool {
	e.nodes.mu.Lock()
	defer e.nodes.mu.Unlock()
	return e.nodes.detail(id).Eligible
}

func TestScaleDown_HeldNodeStaysOutOfService(t *testing.T) {
	for _, selector := range []string{selectorLeastBusy, selectorEmpty} {
		t.Run(selector, func(t *testing.T) {
			extra := map[string]any{"policy": map[string]any{"scale_down": map[string]any{"selector": selector}}}
			e, insts := grown(t, 2, extra)
			held, other := "node-"+insts[0].Name, "node-"+insts[1].Name
			e.outByHand(held)

			// The scale down takes the node drained by hand, and says so.
			e.putGroup(1, extra)
			e.reconcile()
			m := removalOf(e.status())
			if m["node"] != held || m["held"] != true {
				t.Fatalf("removal: %+v", m)
			}

			// The scale is raised again: the node stays, ineligible as the
			// person left it; only the operator's own drain is stopped.
			e.putGroup(2, extra)
			e.reconcile()
			if removalOf(e.status()) != nil || len(e.instances()) != 2 {
				t.Fatalf("status: %+v", e.status())
			}
			if e.eligible(held) || len(e.nodes.canceled) != 0 {
				t.Fatalf("the node drained by hand was made eligible (canceled %v)", e.nodes.canceled)
			}
			if selector == selectorLeastBusy && !slices.Equal(e.nodes.stopped, []string{held}) {
				t.Fatalf("stopped drains %v, want the operator's own", e.nodes.stopped)
			}
			if selector == selectorEmpty && len(e.nodes.stopped) != 0 {
				t.Fatalf("a drain was stopped, none was started: %v", e.nodes.stopped)
			}
			msgs := e.eventsOf("ScaleDownCanceled")
			if len(msgs) != 1 || !strings.Contains(msgs[0], "stays ineligible") {
				t.Fatalf("ScaleDownCanceled: %q", msgs)
			}
			if !e.eligible(other) {
				t.Fatal("the other node was touched")
			}
		})
	}
}

func TestScaleDown_HeldNodeDrainStuck(t *testing.T) {
	e, insts := grown(t, 2, nil)
	held := "node-" + insts[0].Name
	e.outByHand(held)
	e.putGroup(1, nil)
	e.reconcile()
	if e.nodes.drains[0] != held {
		t.Fatalf("drains: %v", e.nodes.drains)
	}

	e.advance(15*time.Minute + drainGrace + time.Second)
	e.reconcile()
	if e.eligible(held) || len(e.nodes.canceled) != 0 || !slices.Equal(e.nodes.stopped, []string{held}) {
		t.Fatalf("eligible %t, canceled %v, stopped %v", e.eligible(held), e.nodes.canceled, e.nodes.stopped)
	}
	c := conditionOf(e.status(), CondScaleDownBlocked)
	if msg, _ := c["message"].(string); c["reason"] != "DrainStuck" || !strings.Contains(msg, "stays ineligible") {
		t.Fatalf("ScaleDownBlocked: %+v", c)
	}
}

func TestScaleDown_NodeNotHeldIsGivenBack(t *testing.T) {
	// A node the operator took eligible is eligible again: the drain is
	// canceled, not only stopped.
	e, _ := grown(t, 2, nil)
	e.putGroup(1, nil)
	e.reconcile()
	victim := e.nodes.drains[0]
	if m := removalOf(e.status()); m["held"] != nil {
		t.Fatalf("removal: %+v", m)
	}
	e.putGroup(2, nil)
	e.reconcile()
	if !e.eligible(victim) || len(e.nodes.stopped) != 0 {
		t.Fatalf("eligible %t, stopped %v", e.eligible(victim), e.nodes.stopped)
	}
}
