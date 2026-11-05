// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"slices"
	"strings"
	"testing"
)

func strayKinds(st map[string]any) map[string]string {
	out := map[string]string{}
	list, _ := st["strayNodes"].([]any)
	for _, item := range list {
		m := item.(map[string]any)
		out[m["name"].(string)] = m["kind"].(string)
	}
	return out
}

func TestStray_ForeignAndGoneNodes(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	inst := e.instances()[0]

	e.nodes.set(
		// The node of the instance.
		Node{ID: "n1", Name: inst.Name, Instance: inst.Name, Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 10},
		// A second ready node with the same name: foreign.
		Node{ID: "n2", Name: "copy", Instance: inst.Name, Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 20},
		// A ready node of the group whose instance does not exist: foreign.
		Node{ID: "n3", Name: "ghost", Instance: "workers-dc1-9-9", Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 30},
		// A node of a deleted instance, down: reported only.
		Node{ID: "n4", Name: "old", Instance: "workers-dc1-0-1", Group: "workers-dc1", Ready: false, Eligible: true, CreateIndex: 5},
		// Nodes that are not the group's: by hand, and of another group.
		Node{ID: "n5", Name: "hand", Ready: true, Eligible: true},
		Node{ID: "n6", Name: "other", Instance: "edge-1-1", Group: "edge", Ready: true, Eligible: true},
	)
	e.reconcile()
	st := e.status()
	if got := strayKinds(st); len(got) != 3 || got["copy"] != strayForeign || got["ghost"] != strayForeign || got["old"] != strayGone {
		t.Fatalf("stray nodes: %v", got)
	}
	if st["replicas"] != 1.0 {
		t.Fatalf("the node of the instance does not count: %+v", st["replicas"])
	}
	if c := conditionOf(st, CondForeignNode); c["status"] != "True" || !strings.Contains(c["message"].(string), "copy") {
		t.Fatalf("ForeignNode: %+v", c)
	}
	inel := e.nodes.madeIneligible()
	slices.Sort(inel)
	if !slices.Equal(inel, []string{"n2", "n3"}) {
		t.Fatalf("made ineligible: %v", inel)
	}
	if e.eventCount(CondForeignNode) != 2 || e.eventCount("NodeWithoutInstance") != 1 || e.eventCount("NodeIneligible") != 2 {
		t.Fatalf("events: foreign %d, gone %d, ineligible %d",
			e.eventCount(CondForeignNode), e.eventCount("NodeWithoutInstance"), e.eventCount("NodeIneligible"))
	}

	// The next look reports nothing again and does not touch the nodes again.
	e.reconcile()
	if e.eventCount(CondForeignNode) != 2 || e.eventCount("NodeWithoutInstance") != 1 || len(e.nodes.madeIneligible()) != 2 {
		t.Fatal("reported or made ineligible again")
	}

	// The foreign nodes go: the condition clears.
	e.nodes.set(Node{ID: "n1", Name: inst.Name, Instance: inst.Name, Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 10})
	e.reconcile()
	if c := conditionOf(e.status(), CondForeignNode); c["status"] != "False" {
		t.Fatalf("ForeignNode: %+v", c)
	}
}

func TestStray_ANodeThatCameUpAgainIsNotForeign(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	inst := e.instances()[0]
	// The machine came up again: its old node is down, the new one ready.
	e.nodes.set(
		Node{ID: "old", Name: inst.Name, Instance: inst.Name, Group: "workers-dc1", Ready: false, CreateIndex: 10},
		Node{ID: "new", Name: inst.Name, Instance: inst.Name, Group: "workers-dc1", Ready: true, Eligible: true, CreateIndex: 20},
	)
	e.reconcile()
	st := e.status()
	if got := strayKinds(st); len(got) != 0 {
		t.Fatalf("stray nodes: %v", got)
	}
	if st["replicas"] != 1.0 || len(e.nodes.madeIneligible()) != 0 {
		t.Fatalf("status: %+v, ineligible %v", st, e.nodes.madeIneligible())
	}
	if rec := st["nodes"].([]any)[0].(map[string]any); rec["node"] != "new" {
		t.Fatalf("the instance has node %v", rec["node"])
	}
}
