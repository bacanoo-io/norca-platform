// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"slices"
	"testing"
	"time"
)

// repairEnv is an environment whose group has n nodes, all joined.
func repairEnv(t *testing.T, n int, extra map[string]any) *env {
	t.Helper()
	e := newEnv(t, "")
	e.putGroup(n, extra)
	e.reconcile()
	e.joinAll(nil)
	e.reconcile()
	if got := len(e.instances()); got != n {
		t.Fatalf("instances: got %d, want %d", got, n)
	}
	return e
}

// A node that has been down for repair.after while its instance runs is
// replaced: the instance is deleted and the group asks for a new one.
func TestRepair_ReplacesANodeThatIsDown(t *testing.T) {
	e := repairEnv(t, 3, nil)
	downNode := e.firstNode()
	before := e.instanceIDs()

	// Down for less than repair.after (15m by default): left alone.
	e.nodes.downSince(downNode, e.now)
	e.advance(14 * time.Minute)
	e.reconcile()
	if e.eventCount("NodeRepaired") != 0 || !slices.Equal(before, e.instanceIDs()) {
		t.Fatalf("repaired too early: events %d, instances %v", e.eventCount("NodeRepaired"), e.instanceIDs())
	}

	e.advance(2 * time.Minute)
	e.reconcile()
	if e.eventCount("NodeRepaired") != 1 {
		t.Fatalf("NodeRepaired events: %d", e.eventCount("NodeRepaired"))
	}
	// The instance of the node is gone and one more was asked for.
	after := e.instanceIDs()
	if len(after) != 3 {
		t.Fatalf("instances after the repair: %v", after)
	}
	kept := 0
	for _, id := range after {
		if slices.Contains(before, id) {
			kept++
		}
	}
	if kept != 2 {
		t.Fatalf("instances kept: %d of %v, before %v", kept, after, before)
	}
	if c := conditionOf(e.status(), CondRepairHeld); c["status"] != "False" {
		t.Fatalf("RepairHeld: %+v", c)
	}
}

func TestRepair_Settings(t *testing.T) {
	tests := []struct {
		name         string
		repair       map[string]any
		downFor      time.Duration
		wantRepaired int
	}{
		{name: "the default waits fifteen minutes", downFor: 10 * time.Minute, wantRepaired: 0},
		{name: "a shorter time", repair: map[string]any{"after": "5m"}, downFor: 6 * time.Minute, wantRepaired: 1},
		{name: "a longer time", repair: map[string]any{"after": "1h"}, downFor: 30 * time.Minute, wantRepaired: 0},
		{name: "switched off", repair: map[string]any{"enabled": false}, downFor: 2 * time.Hour, wantRepaired: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var extra map[string]any
			if tt.repair != nil {
				extra = map[string]any{"repair": tt.repair}
			}
			e := repairEnv(t, 3, extra)

			e.nodes.downSince(e.firstNode(), e.now)
			e.advance(tt.downFor)
			e.reconcile()

			if got := e.eventCount("NodeRepaired"); got != tt.wantRepaired {
				t.Fatalf("NodeRepaired events: got %d, want %d", got, tt.wantRepaired)
			}
		})
	}
}

// When half or more of the nodes of a group are down, what failed is what
// they share: nothing is repaired, and the condition says so.
func TestRepair_HeldWhenTooManyAreDown(t *testing.T) {
	e := repairEnv(t, 4, nil)
	nodes := e.nodeIDs()
	before := e.instanceIDs()

	e.nodes.downSince(nodes[0], e.now)
	e.nodes.downSince(nodes[1], e.now)
	e.advance(20 * time.Minute)
	e.reconcile()

	if e.eventCount("NodeRepaired") != 0 || !slices.Equal(before, e.instanceIDs()) {
		t.Fatalf("repaired with half of the group down: events %d", e.eventCount("NodeRepaired"))
	}
	if c := conditionOf(e.status(), CondRepairHeld); c["status"] != "True" || c["reason"] != "TooManyDown" {
		t.Fatalf("RepairHeld: %+v", c)
	}
	if e.eventCount(CondRepairHeld) != 1 {
		t.Fatalf("RepairHeld events: %d", e.eventCount(CondRepairHeld))
	}

	// It is told once, not at every look.
	e.advance(time.Minute)
	e.reconcile()
	if e.eventCount(CondRepairHeld) != 1 {
		t.Fatalf("RepairHeld events after another look: %d", e.eventCount(CondRepairHeld))
	}
}

// The only node of a group is repaired: one node down is never "too many".
func TestRepair_TheOnlyNodeOfAGroup(t *testing.T) {
	e := repairEnv(t, 1, nil)
	before := e.instanceIDs()

	e.nodes.downSince(e.firstNode(), e.now)
	e.advance(16 * time.Minute)
	e.reconcile()

	if e.eventCount("NodeRepaired") != 1 {
		t.Fatalf("NodeRepaired events: %d", e.eventCount("NodeRepaired"))
	}
	after := e.instanceIDs()
	if len(after) != 1 || after[0] == before[0] {
		t.Fatalf("instances: before %v, after %v", before, after)
	}
}

// A node that carries the protect mark is never replaced.
func TestRepair_ProtectedNode(t *testing.T) {
	e := repairEnv(t, 3, nil)
	node := e.firstNode()
	meta := e.meta(node)
	meta[MetaProtect] = "true"
	e.setMeta(node, meta)

	e.nodes.downSince(node, e.now)
	e.advance(time.Hour)
	e.reconcile()

	if e.eventCount("NodeRepaired") != 0 {
		t.Fatalf("a protected node was repaired")
	}
}
