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

// deletionEnv is a group of two instances, the first with a ready node, the
// second without one, and the group marked for deletion.
func deletionEnv(t *testing.T, extra map[string]any) (*env, []string) {
	t.Helper()
	e := newEnv(t, "")
	e.putGroup(2, extra)
	e.reconcile()
	if g := e.backend.GetObject(NodeGroupKind, "", "workers-dc1"); !slices.Contains(g.Finalizers, Finalizer) {
		t.Fatalf("no finalizer: %v", g.Finalizers)
	}
	insts := e.instances()
	if len(insts) != 2 {
		t.Fatalf("instances: %+v", insts)
	}
	slices.SortFunc(insts, func(a, b provider.Instance) int { return strings.Compare(a.Name, b.Name) })
	e.nodes.set(Node{ID: "n1", Name: "node-1", Instance: insts[0].Name, Group: "workers-dc1", Ready: true, Eligible: true})
	e.reconcile()
	e.backend.RemoveObject(NodeGroupKind, "", "workers-dc1")
	if g := e.backend.GetObject(NodeGroupKind, "", "workers-dc1"); g == nil || g.DeletionTime == 0 {
		t.Fatal("the group is not kept by the finalizer")
	}
	return e, []string{insts[0].ID, insts[1].ID}
}

func (e *env) gone() bool {
	return e.backend.GetObject(NodeGroupKind, "", "workers-dc1") == nil
}

func (e *env) instanceIDs() []string {
	var ids []string
	for _, inst := range e.instances() {
		ids = append(ids, inst.ID)
	}
	slices.Sort(ids)
	return ids
}

func TestDeletion_Drain(t *testing.T) {
	e, ids := deletionEnv(t, nil)

	// The instance without a node goes at once; the node of the other is
	// drained, and its instance stays until the drain is complete.
	e.reconcile()
	if got := e.instanceIDs(); !slices.Equal(got, ids[:1]) {
		t.Fatalf("instances %v, want %v", got, ids[:1])
	}
	if !slices.Equal(e.nodes.drains, []string{"n1"}) {
		t.Fatalf("drains: %v", e.nodes.drains)
	}
	if e.gone() {
		t.Fatal("the group went before its node was drained")
	}
	st := e.status()
	if d, _ := st["deletion"].(map[string]any); d["policy"] != "drain" || len(d["waiting"].([]any)) != 1 {
		t.Fatalf("status.deletion: %+v", st["deletion"])
	}
	if c := conditionOf(st, CondDeletionBlocked); c["status"] != "False" {
		t.Fatalf("DeletionBlocked: %+v", c)
	}

	// Still draining: nothing more happens, the drain is not started again.
	e.reconcile()
	if len(e.nodes.drains) != 1 || len(e.instances()) != 1 {
		t.Fatalf("drains %v, instances %v", e.nodes.drains, e.instanceIDs())
	}

	// The drain is complete: the instance is deleted, and the group waits
	// for its node to go down.
	e.nodes.finishDrain("n1")
	e.reconcile()
	if n := len(e.instances()); n != 0 {
		t.Fatalf("%d instances left", n)
	}
	if e.gone() {
		t.Fatal("the group went before the node of its last instance went down")
	}

	// The node goes down: it is purged and the group goes.
	e.nodes.down("n1")
	e.reconcile()
	if !slices.Equal(e.nodes.purged, []string{"n1"}) {
		t.Fatalf("purged: %v", e.nodes.purged)
	}
	if !e.gone() {
		t.Fatal("the group is still there")
	}
}

func TestDeletion_Terminate(t *testing.T) {
	e, _ := deletionEnv(t, map[string]any{"deletion": map[string]any{"policy": "terminate"}})
	e.reconcile()
	if n := len(e.instances()); n != 0 || len(e.nodes.drains) != 0 {
		t.Fatalf("%d instances left, drains %v", n, e.nodes.drains)
	}
	e.nodes.down("n1")
	e.reconcile()
	if !e.gone() || !slices.Equal(e.nodes.purged, []string{"n1"}) {
		t.Fatalf("gone %t, purged %v", e.gone(), e.nodes.purged)
	}
}

func TestDeletion_Orphan(t *testing.T) {
	e, ids := deletionEnv(t, map[string]any{"deletion": map[string]any{"policy": "orphan"}})
	e.reconcile()
	if !e.gone() {
		t.Fatal("the group is still there")
	}
	if got := e.instanceIDs(); !slices.Equal(got, ids) {
		t.Fatalf("instances %v, want %v", got, ids)
	}
	if len(e.nodes.drains) != 0 || len(e.nodes.purged) != 0 {
		t.Fatalf("drains %v, purged %v", e.nodes.drains, e.nodes.purged)
	}
}

func TestDeletion_BlockedThenTerminate(t *testing.T) {
	e, _ := deletionEnv(t, map[string]any{"deletion": map[string]any{"drain_timeout": "30m"}})
	g := e.backend.GetObject(NodeGroupKind, "", "workers-dc1")
	e.now = time.Unix(0, g.DeletionTime)
	e.registerProvider(e.now, "")
	e.reconcile() // the drain starts

	// The drain does not finish: after drain_timeout the deletion is blocked.
	e.now = e.now.Add(31 * time.Minute)
	e.registerProvider(e.now, "")
	e.reconcile()
	c := conditionOf(e.status(), CondDeletionBlocked)
	if c["status"] != "True" || c["reason"] != "DrainTimeout" {
		t.Fatalf("DeletionBlocked: %+v", c)
	}
	if !e.hasEvent(CondDeletionBlocked) || len(e.instances()) != 1 {
		t.Fatalf("events %v, instances %v", e.backend.ObjectEvents(NodeGroupKind, "", "workers-dc1"), e.instanceIDs())
	}

	// A person chooses terminate: the instance is deleted at once.
	e.putGroup(2, map[string]any{"deletion": map[string]any{"policy": "terminate"}})
	e.reconcile()
	if n := len(e.instances()); n != 0 {
		t.Fatalf("%d instances left", n)
	}
	e.nodes.down("n1")
	e.reconcile()
	if !e.gone() {
		t.Fatal("the group is still there")
	}
}

func TestDeletion_NodeStaysUp(t *testing.T) {
	e, _ := deletionEnv(t, map[string]any{"deletion": map[string]any{"policy": "terminate"}})
	e.reconcile()
	// The node of the deleted instance stays up: it is waited for downWait,
	// then left as it is, and the group goes.
	e.now = e.now.Add(downWait + time.Minute)
	e.registerProvider(e.now, "")
	e.reconcile()
	if !e.gone() || len(e.nodes.purged) != 0 {
		t.Fatalf("gone %t, purged %v", e.gone(), e.nodes.purged)
	}
}

func TestDeletion_DisabledGroupHasNoFinalizer(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, map[string]any{"enabled": false})
	e.reconcile()
	if g := e.backend.GetObject(NodeGroupKind, "", "workers-dc1"); len(g.Finalizers) != 0 {
		t.Fatalf("finalizers: %v", g.Finalizers)
	}
	e.backend.RemoveObject(NodeGroupKind, "", "workers-dc1")
	if !e.gone() {
		t.Fatal("the group is kept")
	}
}
