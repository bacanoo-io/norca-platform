// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
)

// rollEnv is a group of n instances whose nodes have joined, with the marks of
// the operator in their meta (created now, the current revision and
// generation), and enforce = true.
func rollEnv(t *testing.T, n int, extra map[string]any) *env {
	t.Helper()
	e := newEnv(t, "")
	spec := map[string]any{"enforce": true}
	maps.Copy(spec, extra)
	e.putGroup(n, spec)
	e.reconcile()
	e.joinAll(nil)
	e.reconcile()
	if st := e.status(); st["replicas"] != float64(n) {
		t.Fatalf("replicas %v, want %d", st["replicas"], n)
	}
	return e
}

// revision is the template revision and the rolling generation of the group
// as it is now: what a node created now carries.
func (e *env) revision() (string, string) {
	spec, err := decodeSpec(e.backend.GetObject(NodeGroupKind, "", "workers-dc1").Spec)
	if err != nil {
		e.t.Fatal(err)
	}
	return spec.templateRevision(), fmt.Sprint(spec.Rolling.Generation)
}

// joinAll gives every instance without a node a ready node "node-<name>",
// with the marks of the operator (those of a node created now) and the extra
// meta.
func (e *env) joinAll(meta map[string]string) {
	e.t.Helper()
	e.nodes.mu.Lock()
	known := map[string]bool{}
	for _, n := range e.nodes.nodes {
		known[n.Instance] = true
	}
	nodes := slices.Clone(e.nodes.nodes)
	e.nodes.mu.Unlock()
	rev, gen := e.revision()
	for _, inst := range e.instances() {
		if known[inst.Name] {
			continue
		}
		id := "node-" + inst.Name
		nodes = append(nodes, Node{ID: id, Name: id, Instance: inst.Name, Group: "workers-dc1", Ready: true, Eligible: true,
			CreateIndex: uint64(len(nodes) + 1)})
		m := map[string]string{MetaInstance: inst.Name, MetaNodeGroup: "workers-dc1", MetaCreatedBy: "nodescaler",
			MetaCreatedAt: e.now.UTC().Format(time.RFC3339), MetaRevision: rev, MetaGeneration: gen}
		maps.Copy(m, meta)
		e.nodes.setDetail(id, NodeDetail{Meta: m, Ready: true, Eligible: true})
	}
	e.nodes.set(nodes...)
}

// setMeta changes the meta of a node.
func (e *env) setMeta(id string, meta map[string]string) {
	e.nodes.mu.Lock()
	defer e.nodes.mu.Unlock()
	d := e.nodes.detail(id)
	if d.Meta == nil {
		d.Meta = map[string]string{}
	}
	maps.Copy(d.Meta, meta)
}

func (e *env) rollout() *rollout { return readRollout(e.status()) }

func (e *env) nodeIDs() []string {
	e.nodes.mu.Lock()
	defer e.nodes.mu.Unlock()
	var ids []string
	for _, n := range e.nodes.nodes {
		ids = append(ids, n.ID)
	}
	slices.Sort(ids)
	return ids
}

func (e *env) firstNode() string { return e.nodeIDs()[0] }

// finishReplacement joins the new node, completes the drain of the old one,
// takes it down and purges it.
func (e *env) finishReplacement(old string) {
	e.t.Helper()
	e.joinAll(nil)
	e.reconcile() // the new node is ready: the old one is drained
	e.nodes.finishDrain(old)
	e.reconcile() // drained: the old instance is deleted
	e.nodes.down(old)
	e.reconcile() // down: purged
}

func TestRollout_MarkedSurge(t *testing.T) {
	e := rollEnv(t, 2, nil)
	old := e.firstNode()
	e.setMeta(old, map[string]string{MetaReplace: "true"})

	// The old node gets no new work and a new instance is asked for; the
	// scale does not count the old node.
	e.reconcile()
	if !slices.Contains(e.nodes.madeIneligible(), old) || len(e.instances()) != 3 {
		t.Fatalf("ineligible %v, %d instances", e.nodes.madeIneligible(), len(e.instances()))
	}
	st := e.status()
	if st["replicas"] != 1.0 || st["joining"] != 1.0 {
		t.Fatalf("replicas %v joining %v, want 1 and 1", st["replicas"], st["joining"])
	}
	ro := e.rollout()
	if len(ro.Replacing) != 1 || ro.Replacing[0].Node != old || ro.Replacing[0].Phase != phaseSurging || ro.Replacing[0].Reason != reasonMarked {
		t.Fatalf("rollout: %+v", ro.Replacing)
	}
	if c := conditionOf(st, CondRolling); c["status"] != "True" {
		t.Fatalf("Rolling: %+v", c)
	}

	// Nothing more while the new node joins.
	e.reconcile()
	if len(e.instances()) != 3 || len(e.nodes.drains) != 0 {
		t.Fatalf("%d instances, drains %v", len(e.instances()), e.nodes.drains)
	}

	// The new node is ready: the old one is drained, then deleted and purged.
	e.joinAll(nil)
	e.reconcile()
	if !slices.Equal(e.nodes.drains, []string{old}) {
		t.Fatalf("drains: %v", e.nodes.drains)
	}
	if st := e.status(); st["replicas"] != 2.0 {
		t.Fatalf("replicas %v during the drain, want 2", st["replicas"])
	}
	e.nodes.finishDrain(old)
	e.reconcile()
	if n := len(e.instances()); n != 2 {
		t.Fatalf("%d instances after the drain", n)
	}
	e.nodes.down(old)
	e.reconcile()
	if !slices.Equal(e.nodes.purged, []string{old}) {
		t.Fatalf("purged: %v", e.nodes.purged)
	}
	st = e.status()
	if st["rollout"] != nil || st["replicas"] != 2.0 || e.eventCount("ForeignNode")+e.eventCount("NodeWithoutInstance") != 0 {
		t.Fatalf("rollout %v replicas %v, stray events %d", st["rollout"], st["replicas"],
			e.eventCount("ForeignNode")+e.eventCount("NodeWithoutInstance"))
	}
	if c := conditionOf(st, CondRolling); c["status"] != "False" || !e.hasEvent("Replaced") {
		t.Fatalf("Rolling: %+v, Replaced event %t", c, e.hasEvent("Replaced"))
	}
	if spec := e.backend.GetObject(NodeGroupKind, "", "workers-dc1").Spec; spec["replicas"] != 2.0 {
		t.Fatalf("the scale changed: %v", spec["replicas"])
	}
}

func TestRollout_GenerationOneAtATime(t *testing.T) {
	e := rollEnv(t, 2, nil)
	first := e.nodeIDs()
	e.putGroup(2, map[string]any{"enforce": true, "rolling": map[string]any{"generation": 1}})

	// Both are outdated; one is replaced at a time, the oldest first.
	e.reconcile()
	ro := e.rollout()
	if len(ro.Replacing) != 1 || ro.Replacing[0].Reason != reasonGeneration || len(ro.Outdated) != 1 {
		t.Fatalf("rollout: %+v outdated %v", ro.Replacing, ro.Outdated)
	}
	e.finishReplacement(ro.Replacing[0].Node)

	// The second follows.
	e.reconcile()
	ro = e.rollout()
	if len(ro.Replacing) != 1 || ro.Replacing[0].Node == first[0] {
		t.Fatalf("rollout: %+v", ro.Replacing)
	}
	e.finishReplacement(ro.Replacing[0].Node)
	e.reconcile()
	ids := e.nodeIDs()
	if len(ids) != 2 || slices.Contains(ids, first[0]) || slices.Contains(ids, first[1]) || e.rollout().Replacing != nil {
		t.Fatalf("nodes %v, rollout %+v", ids, e.rollout().Replacing)
	}
}

func TestRollout_Revision(t *testing.T) {
	e := rollEnv(t, 1, nil)
	old := e.firstNode()
	e.putGroup(1, map[string]any{"enforce": true, "node": map[string]any{"meta": map[string]any{"image": "v2"}}})
	e.reconcile()
	if ro := e.rollout(); len(ro.Replacing) != 1 || ro.Replacing[0].Reason != reasonRevision || ro.Replacing[0].Node != old {
		t.Fatalf("rollout: %+v", ro.Replacing)
	}
}

func TestRollout_DryRunAndPaused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		extra  map[string]any
		reason string
	}{
		{"enforce false", map[string]any{"enforce": false}, "DryRun"},
		{"paused", map[string]any{"rolling": map[string]any{"mode": "paused"}}, "Paused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := rollEnv(t, 1, tc.extra)
			e.setMeta(e.firstNode(), map[string]string{MetaReplace: "true"})
			e.reconcile()
			st := e.status()
			if len(e.instances()) != 1 || len(e.nodes.madeIneligible()) != 0 || len(e.rollout().Outdated) != 1 {
				t.Fatalf("%d instances, ineligible %v, outdated %v", len(e.instances()), e.nodes.madeIneligible(), e.rollout().Outdated)
			}
			if c := conditionOf(st, CondRolling); c["status"] != "False" || c["reason"] != tc.reason {
				t.Fatalf("Rolling: %+v", c)
			}
			if tc.reason == "DryRun" && !e.hasEvent("WouldReplace") {
				t.Fatal("no WouldReplace event")
			}
		})
	}
}

func TestRollout_HardLimitOverridesPause(t *testing.T) {
	e := rollEnv(t, 1, map[string]any{"rolling": map[string]any{"mode": "paused"}})
	old := e.firstNode()
	// Created 60 days ago with a certificate of 60 days: past 55 of 60.
	created := e.now.Add(-56 * 24 * time.Hour)
	e.setMeta(old, map[string]string{MetaCreatedAt: created.Format(time.RFC3339),
		bootstrap.MetaCertExpires: created.Add(60 * 24 * time.Hour).Format(time.RFC3339)})
	e.reconcile()
	ro := e.rollout()
	if len(ro.Replacing) != 1 || ro.Replacing[0].Reason != reasonExpiry || !ro.Replacing[0].Drain {
		t.Fatalf("rollout: %+v", ro.Replacing)
	}
}

func TestRollout_AgeWaitsForEmpty(t *testing.T) {
	e := rollEnv(t, 1, map[string]any{"rolling": map[string]any{"max_node_age": "10d", "jitter": "0%"}})
	old := e.firstNode()
	e.setMeta(old, map[string]string{MetaCreatedAt: e.now.Add(-11 * 24 * time.Hour).Format(time.RFC3339)})
	e.nodes.mu.Lock()
	e.nodes.detail(old).Allocs = []NodeAlloc{{ID: "a1", Namespace: "default", JobID: "web", Group: "web"}}
	e.nodes.mu.Unlock()
	e.reconcile()
	e.joinAll(nil)
	e.reconcile()
	// Old by age only: not drained, it waits until its work is gone.
	ro := e.rollout()
	if len(ro.Replacing) != 1 || ro.Replacing[0].Reason != reasonAge || ro.Replacing[0].Phase != phaseEmptying || len(e.nodes.drains) != 0 {
		t.Fatalf("rollout %+v, drains %v", ro.Replacing, e.nodes.drains)
	}
	e.reconcile()
	if len(e.instances()) != 2 {
		t.Fatalf("the old instance went with work on it: %d instances", len(e.instances()))
	}
	e.nodes.mu.Lock()
	e.nodes.detail(old).Allocs = nil
	e.nodes.mu.Unlock()
	e.reconcile()
	if len(e.instances()) != 1 {
		t.Fatalf("%d instances once empty", len(e.instances()))
	}
}

func TestRollout_JitterSpreadsTheAgeLimit(t *testing.T) {
	spec := &groupSpec{}
	spec.Rolling.MaxNodeAge = "45d"
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	due := map[bool]int{}
	for i := range 200 {
		name := fmt.Sprintf("workers-1-%d", i)
		// Every node is due by 45 days, none before 38.25 (15%).
		if outdatedReason(spec, nil, name, created, time.Time{}, time.Time{}, created.Add(45*24*time.Hour)) != reasonAge {
			t.Fatalf("%s is not due at 45 days", name)
		}
		if outdatedReason(spec, nil, name, created, time.Time{}, time.Time{}, created.Add(38*24*time.Hour)) != "" {
			t.Fatalf("%s is due before 38.25 days", name)
		}
		due[outdatedReason(spec, nil, name, created, time.Time{}, time.Time{}, created.Add(42*24*time.Hour)) == reasonAge]++
	}
	if due[true] == 0 || due[false] == 0 {
		t.Fatalf("no spread at 42 days: %v", due)
	}
}

func TestRollout_RemoveFirst(t *testing.T) {
	e := rollEnv(t, 2, map[string]any{"rolling": map[string]any{"max_surge": 0}})
	old := e.firstNode()
	e.setMeta(old, map[string]string{MetaReplace: "true"})
	e.reconcile()
	// Drained first, no new instance yet, and it still counts.
	if !slices.Equal(e.nodes.drains, []string{old}) || len(e.instances()) != 2 || e.status()["replicas"] != 2.0 {
		t.Fatalf("drains %v, %d instances, replicas %v", e.nodes.drains, len(e.instances()), e.status()["replicas"])
	}
	// Once its instance is deleted the replacement is asked for.
	e.nodes.finishDrain(old)
	e.reconcile()
	insts := e.instances()
	if len(insts) != 2 || slices.ContainsFunc(insts, func(i provider.Instance) bool { return "node-"+i.Name == old }) {
		t.Fatalf("instances: %+v", insts)
	}
}

func TestRollout_SurgeRefused(t *testing.T) {
	// The mock group holds 5 instances: the new node of a replacement of a
	// group of 5 is refused.
	e := rollEnv(t, 5, nil)
	old := e.firstNode()
	e.setMeta(old, map[string]string{MetaReplace: "true"})
	e.reconcile()
	st := e.status()
	ro := e.rollout()
	if len(ro.Replacing) != 0 || ro.Backoff.Vetoes != 1 || ro.Backoff.Reason != "SurgeRefused" {
		t.Fatalf("rollout %+v backoff %+v", ro.Replacing, ro.Backoff)
	}
	if !e.hasEvent("ReplacementCanceled") || st["replicas"] != 5.0 {
		t.Fatalf("ReplacementCanceled %t, replicas %v", e.hasEvent("ReplacementCanceled"), st["replicas"])
	}
	e.nodes.mu.Lock()
	eligible := e.nodes.detail(old).Eligible
	e.nodes.mu.Unlock()
	if !eligible {
		t.Fatal("the old node is not eligible again")
	}
	// Backed off: nothing starts again, and RollBlocked stays False while
	// no node is close to its hard limit.
	e.reconcile()
	if len(e.rollout().Replacing) != 0 || len(e.instances()) != 5 {
		t.Fatalf("started again: %+v", e.rollout().Replacing)
	}
	if c := conditionOf(e.status(), CondRollBlocked); c["status"] != "False" {
		t.Fatalf("RollBlocked: %+v", c)
	}
	if c := conditionOf(e.status(), CondRolling); c["reason"] != "BackedOff" {
		t.Fatalf("Rolling: %+v", c)
	}
}

func TestRollout_SurgeNotReady(t *testing.T) {
	e := rollEnv(t, 1, map[string]any{"join": map[string]any{"deadline": "5m"}})
	old := e.firstNode()
	e.setMeta(old, map[string]string{MetaReplace: "true"})
	e.reconcile()
	// The new node never joins: after the join deadline and a grace the
	// replacement is canceled and the old node stays.
	e.advance(11 * time.Minute)
	e.reconcile()
	ro := e.rollout()
	if len(ro.Replacing) != 0 || ro.Backoff.Reason != "SurgeNotReady" {
		t.Fatalf("rollout %+v backoff %+v", ro.Replacing, ro.Backoff)
	}
	if !strings.Contains(ro.Backoff.Message, old) {
		t.Fatalf("message: %s", ro.Backoff.Message)
	}
}

func TestRollout_ProtectedNodeStays(t *testing.T) {
	e := rollEnv(t, 1, nil)
	e.setMeta(e.firstNode(), map[string]string{MetaReplace: "true", MetaProtect: "true"})
	e.reconcile()
	if ro := e.rollout(); len(ro.Replacing) != 0 || len(ro.Outdated) != 0 {
		t.Fatalf("rollout: %+v outdated %v", ro.Replacing, ro.Outdated)
	}
}

func TestRollout_RollBlockedNearTheHardLimit(t *testing.T) {
	e := rollEnv(t, 5, nil)
	// A node 50 days into a certificate of 60 (hard limit at 55, a week
	// before is day 48) and past its soft limit: its new node is refused.
	old := e.firstNode()
	created := e.now.Add(-50 * 24 * time.Hour)
	e.setMeta(old, map[string]string{MetaCreatedAt: created.Format(time.RFC3339),
		bootstrap.MetaCertExpires: created.Add(60 * 24 * time.Hour).Format(time.RFC3339)})
	e.reconcile() // refused: backed off
	e.reconcile() // backed off, the node close to its hard limit
	c := conditionOf(e.status(), CondRollBlocked)
	if c["status"] != "True" || c["reason"] != "SurgeRefused" || c["retryAfter"] == nil {
		t.Fatalf("RollBlocked: %+v", c)
	}
}
