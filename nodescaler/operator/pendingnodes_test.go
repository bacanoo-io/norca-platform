// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"testing"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/controller"
)

// The nodes of instances the listing of the provider does not show yet
// (review finding CON-6i): a machine can boot and its node join before the
// listing shows its instance.

// eligible reports whether the node takes new work.
func (f *fakeNodes) eligible(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range f.nodes {
		if n.ID == id {
			return n.Eligible
		}
	}
	return false
}

// nodeOfInstance is a ready, eligible node with the marks of the instance.
func nodeOfInstance(id, instance string, createIndex uint64) Node {
	return Node{ID: id, Name: instance, Instance: instance, Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: createIndex}
}

// madeIneligibleOf returns the nodes of status.madeIneligible.
func madeIneligibleOf(st map[string]any) []string {
	list, _ := st["madeIneligible"].([]any)
	var out []string
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			id, _ := m["node"].(string)
			out = append(out, id)
		}
	}
	return out
}

// notStray checks that the group reports no stray node and touched none.
func (e *env) notStray(when string) {
	e.t.Helper()
	st := e.status()
	if got := strayKinds(st); len(got) != 0 {
		e.t.Errorf("%s: stray nodes: %v", when, got)
	}
	if c := conditionOf(st, CondForeignNode); c["status"] != "False" {
		e.t.Errorf("%s: ForeignNode: %+v", when, c)
	}
	if inel := e.nodes.madeIneligible(); len(inel) != 0 {
		e.t.Errorf("%s: made ineligible: %v", when, inel)
	}
}

// A node joins before the listing shows its instance: it is the node of an
// instance the operator asked for, not a foreign one. It stays eligible, its
// join record stays, so its certificate renews, and when the listing shows
// the instance it is its node.
func TestPendingNodes_ANodeJoinsBeforeItsInstanceIsListed(t *testing.T) {
	e, store := joinEnv(t)
	ctx := context.Background()
	e.putGroup(2, nil)
	e.reconcile()
	tokens := e.machineTokens()
	const name = "eu-workers-dc1-1-1"
	e.lag.hide(hideAll)

	// The machine joins and its node registers.
	rec, _, err := store.Redeem(ctx, name, tokens[name])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Joined(ctx, rec, tokens[name], "aa"); err != nil {
		t.Fatal(err)
	}
	e.nodes.set(nodeOfInstance("n1", name, 10))

	renews := func(when string) {
		t.Helper()
		if _, _, err := store.Renewing(ctx, name, "aa"); err != nil {
			t.Errorf("%s: the renewal of the node: %v", when, err)
		}
	}
	e.advance(time.Minute)
	e.reconcile()
	e.notStray("within the grace time")
	renews("within the grace time")
	if !e.nodes.eligible("n1") {
		t.Fatal("the node is ineligible")
	}

	// The request is made again after the grace time: still its node.
	e.advance(e.r.cfg.ListGrace)
	e.reconcile()
	if asked := e.lag.asked(); len(asked) != 2 {
		t.Fatalf("requests: %v", asked)
	}
	e.notStray("after the request was made again")
	renews("after the request was made again")

	// The listing shows the instances.
	e.lag.hide(nil)
	e.reconcile()
	e.notStray("after the listing")
	renews("after the listing")
	st := e.status()
	if st["replicas"] != 1.0 || st["joining"] != 1.0 || st["pendingRequests"] != nil || !e.nodes.eligible("n1") {
		t.Fatalf("after the listing: %+v", st)
	}
	if n := len(e.instances()); n != 2 {
		t.Fatalf("instances: %d", n)
	}
}

// Only the names of the pending requests count: a node with another name of
// the group is foreign as before, and so is a second ready node with a name
// that is pending.
func TestPendingNodes_OtherNamesAreForeign(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	e.reconcile()
	e.lag.hide(hideAll)
	e.nodes.set(
		nodeOfInstance("n1", "eu-workers-dc1-1-1", 10),
		// A second ready node with the name: the first is the node.
		Node{ID: "n2", Name: "copy", Instance: "eu-workers-dc1-1-1", Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 20},
		// The request asked for two instances: there is no third.
		nodeOfInstance("n3", "eu-workers-dc1-1-3", 30),
		// A request that was not made.
		nodeOfInstance("n4", "eu-workers-dc1-2-1", 40),
	)
	e.reconcile()
	st := e.status()
	if got := strayKinds(st); len(got) != 3 || got["copy"] != strayForeign || got["eu-workers-dc1-1-3"] != strayForeign ||
		got["eu-workers-dc1-2-1"] != strayForeign {
		t.Fatalf("stray nodes: %v", got)
	}
	if e.nodes.eligible("n2") || e.nodes.eligible("n3") || e.nodes.eligible("n4") || !e.nodes.eligible("n1") {
		t.Fatalf("made ineligible: %v", e.nodes.madeIneligible())
	}
	if c := conditionOf(st, CondForeignNode); c["status"] != "True" {
		t.Fatalf("ForeignNode: %+v", c)
	}
}

// The request is forgotten (the scale was lowered and the grace time
// passed) and the listing never shows its instances: from the next look a
// node with one of its names is handled as before, a ready node without an
// instance, and its join record goes.
func TestPendingNodes_AForgottenRequestFallsBack(t *testing.T) {
	e, store := joinEnv(t)
	ctx := context.Background()
	e.putGroup(1, nil)
	e.reconcile()
	tokens := e.machineTokens()
	const name = "eu-workers-dc1-1-1"
	e.lag.hide(hideAll)
	rec, _, err := store.Redeem(ctx, name, tokens[name])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Joined(ctx, rec, tokens[name], "aa"); err != nil {
		t.Fatal(err)
	}
	e.nodes.set(nodeOfInstance("n1", name, 10))
	e.reconcile()
	e.notStray("while the request is pending")

	e.putGroup(0, nil)
	e.advance(e.r.cfg.ListGrace + time.Second)
	e.reconcile() // the request is forgotten in this look
	if st := e.status(); st["pendingRequests"] != nil {
		t.Fatalf("the request is not forgotten: %+v", st["pendingRequests"])
	}
	e.reconcile()
	st := e.status()
	if got := strayKinds(st); len(got) != 1 || got[name] != strayForeign {
		t.Fatalf("stray nodes after the request was forgotten: %v", got)
	}
	if e.nodes.eligible("n1") || conditionOf(st, CondForeignNode)["status"] != "True" {
		t.Fatalf("the node is not made ineligible: %+v", conditionOf(st, CondForeignNode))
	}
	if rec := e.backend.GetVariable("default", "nodescaler/join/eu/"+name); rec != nil {
		t.Fatalf("the join record stays after the request was forgotten: %v", rec.Items)
	}
}

// lostStatusWrite is a group whose first request made its instance while the
// status write after it was lost, the listing does not show the instance,
// and its node (n1) has registered; ten seconds have passed.
func lostStatusWrite(t *testing.T, node Node) *env {
	t.Helper()
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.backend.FailNext("UpdateObjectStatus", errors.New("the servers are away"))
	if _, err := e.r.Reconcile(context.Background(), controller.ObjectKey{Name: "workers-dc1"}); err == nil {
		t.Fatal("the failed status write is not an error")
	}
	e.lag.hide(hideAll)
	e.nodes.set(node)
	e.advance(10 * time.Second)
	return e
}

// A node that was made ineligible as foreign while nothing said that its
// instance exists (the status write after the create was lost) is made
// eligible again once the listing shows the instance, which was created
// before the node was made ineligible.
func TestPendingNodes_ANodeMadeIneligibleIsRestoredWhenListed(t *testing.T) {
	const name = "eu-workers-dc1-1-1"
	e := lostStatusWrite(t, nodeOfInstance("n1", name, 10))

	// Neither the status nor the listing knows the request: the node is
	// foreign in this look, which then makes the request again.
	e.reconcile()
	st := e.status()
	if got := strayKinds(st); got[name] != strayForeign || e.nodes.eligible("n1") {
		t.Fatalf("stray nodes: %v, eligible %t", got, e.nodes.eligible("n1"))
	}
	if got := madeIneligibleOf(st); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("status.madeIneligible: %v", st["madeIneligible"])
	}
	if len(pendingOf(st)) != 1 {
		t.Fatalf("pendingRequests: %+v", st["pendingRequests"])
	}

	// The request is pending now: the node is no stray, and it waits for the
	// listing to be made eligible.
	e.reconcile()
	st = e.status()
	if got := strayKinds(st); len(got) != 0 || conditionOf(st, CondForeignNode)["status"] != "False" {
		t.Fatalf("stray nodes while the request is pending: %v", got)
	}
	if got := madeIneligibleOf(st); len(got) != 1 || e.nodes.eligible("n1") {
		t.Fatalf("while the request is pending: status.madeIneligible %v, eligible %t", got, e.nodes.eligible("n1"))
	}

	e.lag.hide(nil)
	e.reconcile()
	st = e.status()
	if !e.nodes.eligible("n1") || st["madeIneligible"] != nil || e.eventCount("NodeEligible") != 1 {
		t.Fatalf("after the listing: eligible %t, status.madeIneligible %v, NodeEligible events %d",
			e.nodes.eligible("n1"), st["madeIneligible"], e.eventCount("NodeEligible"))
	}
	if st["replicas"] != 1.0 || len(strayKinds(st)) != 0 {
		t.Fatalf("after the listing: %+v", st)
	}
	e.reconcile()
	if e.eventCount("NodeEligible") != 1 {
		t.Fatal("made eligible twice")
	}
}

// What the operator did not make ineligible it does not make eligible: a
// node that was ineligible already (by hand, by another controller), and a
// node that is drained meanwhile.
func TestPendingNodes_OnlyItsOwnIneligibilityIsUndone(t *testing.T) {
	const name = "eu-workers-dc1-1-1"
	t.Run("ineligible before", func(t *testing.T) {
		n := nodeOfInstance("n1", name, 10)
		n.Eligible = false
		e := lostStatusWrite(t, n)
		e.reconcile()
		if st := e.status(); strayKinds(st)[name] != strayForeign || st["madeIneligible"] != nil {
			t.Fatalf("status: strays %v, madeIneligible %v", strayKinds(st), st["madeIneligible"])
		}
		e.lag.hide(nil)
		e.reconcile()
		e.reconcile()
		if e.nodes.eligible("n1") || e.eventCount("NodeEligible") != 0 {
			t.Fatal("a node that was ineligible before was made eligible")
		}
	})
	t.Run("drained meanwhile", func(t *testing.T) {
		e := lostStatusWrite(t, nodeOfInstance("n1", name, 10))
		e.reconcile()
		if got := madeIneligibleOf(e.status()); len(got) != 1 {
			t.Fatalf("status.madeIneligible: %v", got)
		}
		if err := e.nodes.Drain(context.Background(), "n1", time.Hour); err != nil {
			t.Fatal(err)
		}
		e.lag.hide(nil)
		e.reconcile()
		if st := e.status(); e.nodes.eligible("n1") || st["madeIneligible"] != nil || e.eventCount("NodeEligible") != 0 {
			t.Fatalf("a node that is drained was made eligible, or is still to be: %v", st["madeIneligible"])
		}
	})
}

// A node that takes the name of an instance before the operator asks for it
// is foreign and made ineligible; when the instance of that name is made
// later, the node is not made eligible again: the instance did not exist
// when the node was found.
func TestPendingNodes_ANameTakenInAdvanceStaysIneligible(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	const name = "eu-workers-dc1-2-1"
	first := e.instances()[0]
	e.nodes.set(nodeOfInstance("n1", first.Name, 10), nodeOfInstance("ghost", name, 20))
	e.reconcile()
	if st := e.status(); strayKinds(st)[name] != strayForeign || e.nodes.eligible("ghost") || len(madeIneligibleOf(st)) != 1 {
		t.Fatalf("status: strays %v, madeIneligible %v", strayKinds(st), st["madeIneligible"])
	}

	e.advance(time.Minute)
	e.putGroup(2, nil)
	e.reconcile()
	insts := e.instances()
	if len(insts) != 2 || !slicesContainsName(insts, name) {
		t.Fatalf("instances: %+v", insts)
	}
	e.reconcile()
	st := e.status()
	if e.nodes.eligible("ghost") || e.eventCount("NodeEligible") != 0 {
		t.Fatal("a node that took the name in advance was made eligible")
	}
	if st["madeIneligible"] != nil {
		t.Fatalf("status.madeIneligible: %v", st["madeIneligible"])
	}
}

func slicesContainsName(insts []provider.Instance, name string) bool {
	for _, inst := range insts {
		if inst.Name == name {
			return true
		}
	}
	return false
}
