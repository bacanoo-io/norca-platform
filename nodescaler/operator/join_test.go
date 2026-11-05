// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"testing"
	"time"
)

// advance moves the clock; the provider keeps its heartbeat, as a live one
// does.
func (e *env) advance(d time.Duration) {
	e.now = e.now.Add(d)
	e.registerProvider(e.now, "")
}

// eventCount counts the events of the group with the reason.
func (e *env) eventCount(reason string) int {
	n := 0
	for _, ev := range e.backend.ObjectEvents(NodeGroupKind, "", "workers-dc1") {
		if ev.Reason == reason {
			n += max(ev.Count, 1)
		}
	}
	return n
}

func joinFailures(st map[string]any) float64 {
	m, _ := st["join"].(map[string]any)
	v, _ := m["failures"].(float64)
	return v
}

func TestJoin_DeadlineDeletesAndAsksAgain(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	e.reconcile()
	first := e.instances()

	// Within the deadline nothing happens.
	e.advance(4 * time.Minute)
	e.reconcile()
	if n := len(e.instances()); n != 2 {
		t.Fatalf("instances: %d", n)
	}

	// After the deadline (15 minutes) both are deleted, two failures, and two
	// new instances are asked for in a new request.
	e.advance(12 * time.Minute)
	e.reconcile()
	if e.eventCount("JoinTimeout") != 2 {
		t.Fatalf("JoinTimeout events: %d", e.eventCount("JoinTimeout"))
	}
	insts := e.instances()
	if len(insts) != 2 || insts[0].ID == first[0].ID || insts[0].RequestID != "eu/workers-dc1/2" {
		t.Fatalf("instances: %+v", insts)
	}
	st := e.status()
	if joinFailures(st) != 2 {
		t.Fatalf("join: %+v", st["join"])
	}
	if c := conditionOf(st, CondJoinFailing); c["status"] != "False" {
		t.Fatalf("JoinFailing: %+v", c)
	}
}

func TestJoin_FailingStopsAndResumes(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, map[string]any{"join": map[string]any{"max_failures": 2}})
	e.reconcile()
	e.advance(16 * time.Minute)
	e.reconcile()

	// Two failures of two allowed: no instance is asked for.
	if n := len(e.instances()); n != 0 {
		t.Fatalf("instances while failing: %d", n)
	}
	st := e.status()
	if c := conditionOf(st, CondJoinFailing); c["status"] != "True" || c["reason"] != "TooManyFailures" {
		t.Fatalf("JoinFailing: %+v", c)
	}
	if c := conditionOf(st, CondProvisioned); c["status"] != "False" || c["reason"] != CondJoinFailing {
		t.Fatalf("Provisioned: %+v", c)
	}
	if e.eventCount(CondJoinFailing) != 1 {
		t.Fatalf("JoinFailing events: %d", e.eventCount(CondJoinFailing))
	}
	e.reconcile()
	if n := len(e.instances()); n != 0 || e.eventCount(CondJoinFailing) != 1 {
		t.Fatalf("a second look asks again or repeats the event: %d instances", n)
	}

	// A raised resume generation gives the group a new chance.
	e.putGroup(2, map[string]any{"join": map[string]any{"max_failures": 2, "resume_generation": 1}})
	e.reconcile()
	if n := len(e.instances()); n != 2 {
		t.Fatalf("instances after the resume: %d", n)
	}
	if st := e.status(); joinFailures(st) != 0 || conditionOf(st, CondJoinFailing)["status"] != "False" {
		t.Fatalf("after the resume: %+v", st["join"])
	}
}

func TestJoin_NewTemplateResumes(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, map[string]any{"join": map[string]any{"max_failures": 1}})
	e.reconcile()
	e.advance(16 * time.Minute)
	e.reconcile()
	if n := len(e.instances()); n != 0 {
		t.Fatalf("instances while failing: %d", n)
	}
	e.putGroup(1, map[string]any{"join": map[string]any{"max_failures": 1},
		"node": map[string]any{"parameters": map[string]any{"image": "norca-worker-4"}}})
	e.reconcile()
	if n := len(e.instances()); n != 1 {
		t.Fatalf("instances with the new template: %d", n)
	}
}

func TestJoin_ANodeThatJoinsEndsTheRow(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	e.reconcile()
	insts := e.instances()

	// One times out, one joins.
	e.advance(10 * time.Minute)
	e.nodes.set(Node{ID: "n1", ProviderID: insts[0].ID, Ready: true})
	e.reconcile()
	if e.eventCount("NodeJoined") != 1 {
		t.Fatalf("NodeJoined events: %d", e.eventCount("NodeJoined"))
	}
	e.advance(6 * time.Minute)
	e.reconcile()
	if st := e.status(); joinFailures(st) != 1 {
		t.Fatalf("join after one timeout: %+v", st["join"])
	}
	// The new one joins: the row ends.
	var fresh string
	for _, inst := range e.instances() {
		if inst.ID != insts[0].ID {
			fresh = inst.ID
		}
	}
	e.nodes.set(Node{ID: "n1", ProviderID: insts[0].ID, Ready: true}, Node{ID: "n2", ProviderID: fresh, Ready: true})
	e.reconcile()
	st := e.status()
	if joinFailures(st) != 0 || st["replicas"] != 2.0 || st["joining"] != 0.0 {
		t.Fatalf("status: %+v", st)
	}
	if e.eventCount("NodeJoined") != 2 {
		t.Fatalf("NodeJoined events: %d", e.eventCount("NodeJoined"))
	}
}

func TestJoin_SlowNodeNoLongerCounts(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	if st := e.status(); st["joining"] != 1.0 {
		t.Fatalf("joining: %+v", st["joining"])
	}
	e.advance(6 * time.Minute)
	if d := e.reconcile(); d != e.r.cfg.JoinCheck {
		t.Fatalf("a slow node is not checked again: %v", d)
	}
	st := e.status()
	if st["joining"] != 0.0 {
		t.Fatalf("a slow node still counts: %+v", st["joining"])
	}
	rec := st["nodes"].([]any)[0].(map[string]any)
	if rec["slow"] != true {
		t.Fatalf("record: %+v", rec)
	}
	e.reconcile()
	if e.eventCount("JoinSlow") != 1 {
		t.Fatalf("JoinSlow events: %d", e.eventCount("JoinSlow"))
	}
	// The slow instance is still one of the group: no new one is asked for
	// until the scale is raised (by the autoscaler, which no longer counts
	// it).
	if n := len(e.instances()); n != 1 {
		t.Fatalf("instances: %d", n)
	}
}

func TestJoin_FailedInstanceIsDeleted(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	id := e.instances()[0].ID
	if !e.mock.Fail("dc1", id, "no_capacity", "the zone is full") {
		t.Fatal("the instance is not there")
	}
	e.reconcile()
	if e.eventCount("InstanceFailed") != 1 {
		t.Fatalf("InstanceFailed events: %d", e.eventCount("InstanceFailed"))
	}
	insts := e.instances()
	if len(insts) != 1 || insts[0].ID == id {
		t.Fatalf("instances: %+v", insts)
	}
	if st := e.status(); joinFailures(st) != 1 {
		t.Fatalf("join: %+v", st["join"])
	}
}
