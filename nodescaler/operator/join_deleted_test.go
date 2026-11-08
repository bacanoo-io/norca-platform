// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"slices"
	"testing"
)

// deletedOf is status.join.deleted: instance -> node.
func deletedOf(st map[string]any) map[string]string {
	out := map[string]string{}
	join, _ := st["join"].(map[string]any)
	list, _ := join["deleted"].([]any)
	for _, item := range list {
		d, _ := item.(map[string]any)
		inst, _ := d["instance"].(string)
		node, _ := d["node"].(string)
		out[inst] = node
	}
	return out
}

// The node of an instance deleted because the provider reports it failed (a
// machine that stopped) is not a stray: while it still looks ready it is not
// foreign, and once down it is purged, as the node of a scale down.
func TestJoin_TheNodeOfAFailedInstanceIsPurged(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	inst := e.instances()[0]
	e.nodes.set(Node{ID: "n1", Name: inst.Name, Instance: inst.Name, Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 10})
	e.reconcile()

	if !e.mock.Fail("dc1", inst.ID, "error", "the container is exited (exit code 255)") {
		t.Fatal("the instance is not there")
	}
	e.reconcile()
	if e.eventCount("InstanceFailed") != 1 {
		t.Fatalf("InstanceFailed events: %d", e.eventCount("InstanceFailed"))
	}
	if got := deletedOf(e.status()); got[inst.Name] != "n1" {
		t.Fatalf("status.join.deleted: %v", got)
	}

	// The servers still see the node ready (its heartbeat has not run out):
	// not foreign, not made ineligible.
	e.reconcile()
	if n := e.eventCount(CondForeignNode); n != 0 {
		t.Fatalf("ForeignNode events: %d", n)
	}
	if inel := e.nodes.madeIneligible(); slices.Contains(inel, "n1") {
		t.Fatalf("made ineligible: %v", inel)
	}
	if got := strayKinds(e.status()); len(got) != 0 {
		t.Fatalf("stray nodes: %v", got)
	}

	// Down: purged, and forgotten.
	e.nodes.down("n1")
	e.reconcile()
	if !slices.Contains(e.nodes.purged, "n1") {
		t.Fatalf("purged: %v", e.nodes.purged)
	}
	if e.eventCount("NodePurged") != 1 || e.eventCount("NodeWithoutInstance") != 0 {
		t.Fatalf("events: purged %d, without instance %d", e.eventCount("NodePurged"), e.eventCount("NodeWithoutInstance"))
	}
	if got := deletedOf(e.status()); len(got) != 0 {
		t.Fatalf("status.join.deleted after the purge: %v", got)
	}
}

// A node that stays up after downWait is no longer waited for: from then on
// it is a stray like any other.
func TestJoin_TheNodeOfAFailedInstanceThatStaysUpIsLeft(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	inst := e.instances()[0]
	e.nodes.set(Node{ID: "n1", Name: inst.Name, Instance: inst.Name, Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 10})
	e.reconcile()
	e.mock.Fail("dc1", inst.ID, "error", "stopped")
	e.reconcile()

	e.now = e.now.Add(downWait + 1)
	e.registerProvider(e.now, "") // a fresh heartbeat: the provider is available
	e.reconcile()
	if got := deletedOf(e.status()); len(got) != 0 {
		t.Fatalf("status.join.deleted after downWait: %v", got)
	}
	if slices.Contains(e.nodes.purged, "n1") {
		t.Fatal("a node that is up was purged")
	}
	e.reconcile()
	if got := strayKinds(e.status()); got[inst.Name] != strayForeign {
		t.Fatalf("stray nodes: %v", got)
	}
}
