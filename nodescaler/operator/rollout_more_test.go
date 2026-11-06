// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
)

// settle plays the world for n rounds: new instances join, drains finish,
// the nodes of deleted instances go down. It returns the most instances the
// provider had.
func (e *env) settle(n int) int {
	e.t.Helper()
	most := len(e.instances())
	for range n {
		e.joinAll(nil)
		live := map[string]bool{}
		for _, inst := range e.instances() {
			live[inst.Name] = true
		}
		e.nodes.mu.Lock()
		nodes := slices.Clone(e.nodes.nodes)
		e.nodes.mu.Unlock()
		for _, node := range nodes {
			e.nodes.mu.Lock()
			draining := e.nodes.detail(node.ID).Draining
			e.nodes.mu.Unlock()
			if draining {
				e.nodes.finishDrain(node.ID)
			}
			if !live[node.Instance] {
				e.nodes.down(node.ID)
			}
		}
		e.reconcile()
		most = max(most, len(e.instances()))
	}
	return most
}

func TestRollout_HeadroomDoesNotOverridePause(t *testing.T) {
	e := rollEnv(t, 5, map[string]any{"headroom_nodes": 1,
		"rolling": map[string]any{"mode": "paused", "max_node_age": "10d", "jitter": "0%"}})
	e.setMeta(e.firstNode(), map[string]string{MetaCreatedAt: e.now.Add(-11 * 24 * time.Hour).Format(time.RFC3339)})
	e.reconcile()
	e.reconcile()
	if ro := e.rollout(); len(ro.Replacing) != 0 || len(ro.Outdated) != 1 {
		t.Fatalf("replacing %+v outdated %v", ro.Replacing, ro.Outdated)
	}
}

func TestRollout_SurgeLimitAllTheWay(t *testing.T) {
	// Three nodes, two at a time: replicas + max_surge instances at most, all
	// the way to the end.
	for _, tc := range []struct {
		surge, most int
	}{{1, 4}, {0, 3}} {
		t.Run(fmt.Sprint("max_surge ", tc.surge), func(t *testing.T) {
			e := rollEnv(t, 3, map[string]any{"rolling": map[string]any{"max_unavailable": 2, "max_surge": tc.surge}})
			old := e.nodeIDs()
			for _, id := range old {
				e.setMeta(id, map[string]string{MetaReplace: "true"})
			}
			e.reconcile()
			most := max(len(e.instances()), e.settle(12))
			ro := e.rollout()
			if len(ro.Replacing) != 0 || len(ro.Outdated) != 0 {
				t.Fatalf("not done: replacing %+v outdated %v", ro.Replacing, ro.Outdated)
			}
			if most != tc.most {
				t.Fatalf("at most %d instances, want %d", most, tc.most)
			}
			for _, id := range old {
				if slices.Contains(e.nodeIDs(), id) {
					t.Fatalf("node %s is still there: %v", id, e.nodeIDs())
				}
			}
			if st := e.status(); st["replicas"] != 3.0 {
				t.Fatalf("replicas %v", st["replicas"])
			}
		})
	}
}

func TestRollout_WarnedIsForgottenWhenReplaced(t *testing.T) {
	e := rollEnv(t, 2, map[string]any{"rolling": map[string]any{"max_node_age": "100d"}})
	old := e.firstNode()
	created := e.now.Add(-49 * 24 * time.Hour)
	e.setMeta(old, map[string]string{MetaCreatedAt: created.Format(time.RFC3339),
		bootstrap.MetaCertExpires: created.Add(60 * 24 * time.Hour).Format(time.RFC3339)})
	e.reconcile()
	if ro := e.rollout(); !slices.Equal(ro.Warned, []string{old}) {
		t.Fatalf("warned %v", ro.Warned)
	}
	// Replaced by hand: the old node is no longer listed, and the new one has
	// no warning.
	e.setMeta(old, map[string]string{MetaReplace: "true"})
	e.reconcile()
	e.settle(6)
	if ro := e.rollout(); len(ro.Warned) != 0 || len(ro.Replacing) != 0 {
		t.Fatalf("warned %v replacing %+v", ro.Warned, ro.Replacing)
	}
	if n := len(e.eventsOf(CondRolling)); n != 1 {
		t.Fatalf("%d Rolling warnings, want 1", n)
	}
}

// behindSpec has a soft limit of day 54 (no jitter) and a replacement of
// 10h05m (the slow_after of 5m and a drain deadline of 10h).
func behindSpec(unavailable int, extra map[string]any) map[string]any {
	spec := map[string]any{
		"rolling": map[string]any{"max_node_age": "54d", "jitter": "0%", "max_unavailable": unavailable},
		"policy":  map[string]any{"scale_down": map[string]any{"drain_deadline": "10h"}},
	}
	maps.Copy(spec, extra)
	return spec
}

// behindEnv is a group of three nodes of behindSpec with certificates of 60
// days (the hard limit at day 55).
func behindEnv(t *testing.T, spec map[string]any) *env {
	t.Helper()
	e := rollEnv(t, 3, spec)
	for _, id := range e.nodeIDs() {
		e.setMeta(id, map[string]string{bootstrap.MetaCertExpires: e.now.Add(60 * 24 * time.Hour).Format(time.RFC3339)})
	}
	return e
}

func TestRollout_RollBehindNotRolling(t *testing.T) {
	for name, spec := range map[string]map[string]any{
		"paused": behindSpec(1, map[string]any{"rolling": map[string]any{"mode": "paused", "max_node_age": "54d",
			"jitter": "0%", "max_unavailable": 1}}),
		"dry run": behindSpec(1, map[string]any{"enforce": false}),
	} {
		t.Run(name, func(t *testing.T) {
			e := behindEnv(t, spec)
			e.reconcile()
			c := conditionOf(e.status(), CondRollBehind)
			if c["status"] != "False" || c["reason"] != "NotRolling" || len(e.eventsOf(CondRollBehind)) != 0 {
				t.Fatalf("RollBehind: %+v, %d events", c, len(e.eventsOf(CondRollBehind)))
			}
		})
	}
}

func TestRollout_RollBehindKeptWhileTheGroupGrows(t *testing.T) {
	e := behindEnv(t, behindSpec(1, nil))
	e.reconcile()
	if c := conditionOf(e.status(), CondRollBehind); c["status"] != "True" {
		t.Fatalf("RollBehind: %+v", c)
	}
	// The scale is raised: while the new instance joins the nodes are not
	// looked at, and the condition stays as it was.
	e.putGroup(4, behindSpec(1, map[string]any{"enforce": true}))
	e.reconcile()
	if st := e.status(); st["joining"] != 1.0 {
		t.Fatalf("joining %v", st["joining"])
	}
	if c := conditionOf(e.status(), CondRollBehind); c["status"] != "True" {
		t.Fatalf("RollBehind while the group grows: %+v", c)
	}
	if n := len(e.eventsOf(CondRollBehind)); n != 1 {
		t.Fatalf("%d RollBehind events, want 1", n)
	}
}

func TestRollout_RollBehindAfterABackoff(t *testing.T) {
	// Two at a time the three nodes are done 20 hours after day 54, before
	// day 55; backed off until day 55 they are late.
	e := behindEnv(t, behindSpec(2, nil))
	e.reconcile()
	if c := conditionOf(e.status(), CondRollBehind); c["status"] != "False" {
		t.Fatalf("RollBehind: %+v", c)
	}
	obj := e.backend.GetObject(NodeGroupKind, "", "workers-dc1")
	spec, err := decodeSpec(obj.Spec)
	if err != nil {
		t.Fatal(err)
	}
	var ages []nodeAge
	for _, id := range e.nodeIDs() {
		ages = append(ages, nodeAge{name: id, created: e.now,
			soft: e.now.Add(54 * 24 * time.Hour), hard: e.now.Add(55 * 24 * time.Hour)})
	}
	ro := &rollout{Backoff: downVeto{Vetoes: 1, RetryAfter: e.now.Add(55 * 24 * time.Hour)}}
	if c := e.r.behind(context.Background(), obj, spec, ro, ages, e.status(), true, e.now); c.Status != "True" {
		t.Fatalf("backed off until the hard limit: %+v", c)
	}
	if c := e.r.behind(context.Background(), obj, spec, ro, ages, e.status(), false, e.now); c.Status != "False" {
		t.Fatalf("not backed off: %+v", c)
	}
}
