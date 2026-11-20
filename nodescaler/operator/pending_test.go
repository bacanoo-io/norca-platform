// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/controller"
)

// hideAll is a listing that shows no instance.
func hideAll(provider.Instance) bool { return true }

// pendingOf returns status.pendingRequests.
func pendingOf(st map[string]any) []map[string]any {
	list, _ := st["pendingRequests"].([]any)
	var out []map[string]any
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

// A listing that lags behind the create does not make the operator ask for
// the instances again: the request is in the status, and its instances count
// as live until the listing shows them (review finding CON-6d: the second
// look asked for a second set of machines with a new request number).
func TestPending_ListingThatLagsCreatesOneSet(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	e.reconcile()
	if n := len(e.instances()); n != 2 {
		t.Fatalf("instances after the request: %d", n)
	}

	e.lag.hide(hideAll)
	for range 2 {
		e.reconcile()
	}
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || len(asked) != 1 {
		t.Fatalf("a listing that lags: %d instances, want 2; requests %v, want one", n, asked)
	}
	st := e.status()
	if st["joining"] != 2.0 || st["lastRequest"] != 1.0 {
		t.Fatalf("the instances that are not listed do not count as joining: %+v", st)
	}
	if c := conditionOf(st, CondProvisioned); c["status"] != "True" {
		t.Fatalf("Provisioned: %+v", c)
	}
	if p := pendingOf(st); len(p) != 1 || p[0]["number"] != 1.0 || p[0]["count"] != 2.0 || p[0]["asks"] != 1.0 ||
		p[0]["at"] != e.now.Format(time.RFC3339) || p[0]["listed"] != nil {
		t.Fatalf("pendingRequests: %+v", p)
	}

	// The listing shows them: the request is forgotten, nothing is asked for.
	e.lag.hide(nil)
	e.reconcile()
	st = e.status()
	if st["pendingRequests"] != nil || st["joining"] != 2.0 {
		t.Fatalf("after the listing: %+v", st)
	}
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || !slices.Equal(asked, []string{"eu/workers-dc1/1"}) {
		t.Fatalf("after the listing: %d instances, requests %v", n, asked)
	}
}

// After the grace time the request counts as not fulfilled: it is made again
// with its own number, the provider answers with the instances of the first
// one, and the group says that they are not listed.
func TestPending_GraceTimePassesAsksAgainWithTheSameNumber(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	e.reconcile()
	e.lag.hide(hideAll)

	// Within the grace time nothing is asked.
	e.advance(e.r.cfg.ListGrace)
	e.reconcile()
	if asked := e.lag.asked(); len(asked) != 1 {
		t.Fatalf("asked again within the grace time: %v", asked)
	}
	if e.hasEvent("InstancesNotListed") {
		t.Fatal("InstancesNotListed within the grace time")
	}

	e.advance(time.Second)
	e.reconcile()
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/1"}) {
		t.Fatalf("after the grace time: %d instances, want 2; requests %v, want the first one twice", n, asked)
	}
	if !e.hasEvent("InstancesNotListed") {
		t.Fatal("no InstancesNotListed event")
	}
	st := e.status()
	if c := conditionOf(st, CondProvisioned); c["status"] != "False" || c["reason"] != "NotListed" {
		t.Fatalf("Provisioned: %+v", c)
	}
	if p := pendingOf(st); len(p) != 1 || p[0]["number"] != 1.0 || p[0]["count"] != 2.0 || p[0]["asks"] != 2.0 ||
		p[0]["at"] != e.now.Format(time.RFC3339) {
		t.Fatalf("pendingRequests: %+v", p)
	}
	if st["joining"] != 2.0 || st["lastRequest"] != 1.0 {
		t.Fatalf("status: %+v", st)
	}

	// The grace time started again: the next looks ask for nothing, and
	// the group still says so.
	e.advance(e.r.cfg.ListGrace)
	e.reconcile()
	if asked := e.lag.asked(); len(asked) != 2 {
		t.Fatalf("asked again within the second grace time: %v", asked)
	}
	if c := conditionOf(e.status(), CondProvisioned); c["reason"] != "NotListed" {
		t.Fatalf("Provisioned: %+v", c)
	}

	e.lag.hide(nil)
	e.reconcile()
	st = e.status()
	if c := conditionOf(st, CondProvisioned); c["status"] != "True" || st["pendingRequests"] != nil {
		t.Fatalf("after the listing: %+v", st)
	}
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || len(asked) != 2 {
		t.Fatalf("after the listing: %d instances, requests %v", n, asked)
	}
}

// The instances of a request come into the listing one by one: each counts
// once, listed or not, and one that was listed and is deleted is not waited
// for again.
func TestPending_InstancesAppearOneByOne(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(3, nil)
	e.reconcile()
	insts := e.instances()
	slices.SortFunc(insts, func(a, b provider.Instance) int { return strings.Compare(a.Name, b.Name) })
	shown := map[string]bool{}
	e.lag.hide(func(i provider.Instance) bool { return !shown[i.ID] })

	for i := range 2 {
		shown[insts[i].ID] = true
		e.reconcile()
		st := e.status()
		p := pendingOf(st)
		if st["joining"] != 3.0 || len(p) != 1 {
			t.Fatalf("%d listed: %+v", i+1, st)
		}
		if listed, _ := p[0]["listed"].([]any); len(listed) != i+1 || p[0]["count"] != 3.0 {
			t.Fatalf("%d listed: pendingRequests %+v", i+1, p)
		}
		if n, asked := len(e.instances()), e.lag.asked(); n != 3 || len(asked) != 1 {
			t.Fatalf("%d listed: %d instances, requests %v", i+1, n, asked)
		}
	}

	// A listed instance fails and is deleted: it is not counted as one that
	// the listing has yet to show, so the one that is missing is asked for
	// at once, in a new request.
	e.mock.Fail("dc1", insts[0].ID, provider.ReasonError, "it did not boot")
	e.reconcile()
	got := e.instances()
	if asked := e.lag.asked(); len(got) != 3 || !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/2"}) {
		t.Fatalf("after the failure: instances %+v, requests %v", got, asked)
	}
	if n := slices.IndexFunc(got, func(i provider.Instance) bool { return i.ID == insts[0].ID }); n >= 0 {
		t.Fatalf("the failed instance is still there: %+v", got)
	}
	e.reconcile()
	if n, asked := len(e.instances()), e.lag.asked(); n != 3 || len(asked) != 2 {
		t.Fatalf("asked again: %d instances, requests %v", n, asked)
	}
	if st := e.status(); st["joining"] != 3.0 || len(pendingOf(st)) != 2 {
		t.Fatalf("status: %+v", st)
	}

	e.lag.hide(nil)
	e.reconcile()
	if st := e.status(); st["pendingRequests"] != nil || st["joining"] != 3.0 || st["lastRequest"] != 2.0 {
		t.Fatalf("after the listing: %+v", st)
	}
	if n, asked := len(e.instances()), e.lag.asked(); n != 3 || len(asked) != 2 {
		t.Fatalf("after the listing: %d instances, requests %v", n, asked)
	}
}

// The status write after the create is lost, and the listing lags: the next
// look has neither the number nor the record, makes the request again with
// the same number, and writes the record with the answer.
func TestPending_StatusWriteLostAfterTheCreate(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	e.backend.FailNext("UpdateObjectStatus", errors.New("the servers are away"))
	if _, err := e.r.Reconcile(context.Background(), controller.ObjectKey{Name: "workers-dc1"}); err == nil {
		t.Fatal("the failed status write is not an error")
	}
	if st := e.status(); st["lastRequest"] != nil || st["pendingRequests"] != nil {
		t.Fatalf("the status was written: %+v", st)
	}
	e.lag.hide(hideAll)
	for range 3 {
		e.reconcile()
	}
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/1"}) {
		t.Fatalf("%d instances, want 2; requests %v, want the first one twice", n, asked)
	}
	st := e.status()
	if p := pendingOf(st); len(p) != 1 || p[0]["number"] != 1.0 || p[0]["count"] != 2.0 || st["lastRequest"] != 1.0 {
		t.Fatalf("status: %+v", st)
	}
}

// Another instance of the operator takes over while the listing lags: it
// reads the request from the status and asks for nothing; after the grace
// time it makes the same request again.
func TestPending_LeaderChange(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(2, nil)
	e.reconcile()
	e.lag.hide(hideAll)

	takeOver := func() {
		e.r = NewReconciler(e.backend, e.nodes, Config{Region: "eu", AllowHTTP: true, Now: func() time.Time { return e.now }})
		e.r.providers.cacheFor = 0
	}
	takeOver()
	e.reconcile()
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || len(asked) != 1 {
		t.Fatalf("the new leader: %d instances, want 2; requests %v, want one", n, asked)
	}
	e.advance(e.r.cfg.ListGrace + time.Second)
	takeOver()
	e.reconcile()
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/1"}) {
		t.Fatalf("the next leader after the grace time: %d instances, requests %v", n, asked)
	}
}

// The scale is lowered while instances are not listed: they are above the
// scale and their nodes have not joined, so they go, once the listing shows
// them; no node that works is drained in their place.
func TestPending_ScaleDownWaitsForTheListing(t *testing.T) {
	e, insts := grown(t, 2, nil)
	e.putGroup(4, nil)
	e.reconcile()
	if n := len(e.instances()); n != 4 {
		t.Fatalf("instances after the raise: %d", n)
	}
	e.lag.hide(func(i provider.Instance) bool { return i.RequestID == "eu/workers-dc1/2" })

	e.putGroup(2, nil)
	for range 2 {
		if d := e.reconcile(); d != e.r.cfg.JoinCheck {
			t.Fatalf("requeue %v, want the join check", d)
		}
	}
	if len(e.nodes.drains) != 0 || len(e.nodes.madeIneligible()) != 0 || removalOf(e.status()) != nil {
		t.Fatalf("a node is removed while instances are not listed: drains %v, status %+v", e.nodes.drains, e.status())
	}
	if n, asked := len(e.instances()), e.lag.asked(); n != 4 || len(asked) != 2 {
		t.Fatalf("while not listed: %d instances, requests %v", n, asked)
	}
	if st := e.status(); st["replicas"] != 2.0 || st["joining"] != 2.0 {
		t.Fatalf("status: %+v", st)
	}

	// The listing shows them: they are deleted, one at a look.
	e.lag.hide(nil)
	for range 3 {
		e.reconcile()
	}
	got := e.instances()
	slices.SortFunc(got, func(a, b provider.Instance) int { return strings.Compare(a.Name, b.Name) })
	if len(got) != 2 || got[0].ID != insts[0].ID || got[1].ID != insts[1].ID {
		t.Fatalf("instances after the scale down: %+v", got)
	}
	if len(e.nodes.drains) != 0 {
		t.Fatalf("drains: %v", e.nodes.drains)
	}
	if st := e.status(); st["pendingRequests"] != nil || st["replicas"] != 2.0 || st["joining"] != 0.0 {
		t.Fatalf("status: %+v", st)
	}
}

// The scale changes while instances are not listed.
func TestPending_ScaleChangedDuringTheGraceTime(t *testing.T) {
	t.Run("raised", func(t *testing.T) {
		e := newEnv(t, "")
		e.putGroup(2, nil)
		e.reconcile()
		e.lag.hide(hideAll)

		// Only the difference is asked for, in a new request.
		e.putGroup(3, nil)
		for range 2 {
			e.reconcile()
		}
		insts := e.instances()
		if asked := e.lag.asked(); len(insts) != 3 || !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/2"}) {
			t.Fatalf("instances %+v, requests %v", insts, asked)
		}
		st := e.status()
		if p := pendingOf(st); len(p) != 2 || p[0]["count"] != 2.0 || p[1]["number"] != 2.0 || p[1]["count"] != 1.0 {
			t.Fatalf("pendingRequests: %+v", p)
		}
		if st["joining"] != 3.0 || st["lastRequest"] != 2.0 {
			t.Fatalf("status: %+v", st)
		}
	})

	t.Run("lowered", func(t *testing.T) {
		e := newEnv(t, "")
		e.putGroup(3, nil)
		e.reconcile()
		e.lag.hide(hideAll)

		// Nothing is asked for, and nothing is deleted.
		e.putGroup(1, nil)
		for range 2 {
			e.reconcile()
		}
		if n, asked := len(e.instances()), e.lag.asked(); n != 3 || len(asked) != 1 {
			t.Fatalf("%d instances, requests %v", n, asked)
		}
		if st := e.status(); st["joining"] != 3.0 || len(pendingOf(st)) != 1 {
			t.Fatalf("status: %+v", st)
		}

		// After the grace time nothing is missing at a scale of 0: the
		// request is forgotten, not made again.
		e.putGroup(0, nil)
		e.advance(e.r.cfg.ListGrace + time.Second)
		e.reconcile()
		if st := e.status(); st["pendingRequests"] != nil || st["joining"] != 0.0 {
			t.Fatalf("after the grace time: %+v", st)
		}
		if n, asked := len(e.instances()), e.lag.asked(); n != 3 || len(asked) != 1 {
			t.Fatalf("after the grace time: %d instances, requests %v", n, asked)
		}
		if c := conditionOf(e.status(), CondProvisioned); c["status"] != "True" {
			t.Fatalf("Provisioned: %+v", c)
		}

		// When the listing shows them, they are above the scale and go.
		e.lag.hide(nil)
		for range 3 {
			e.reconcile()
		}
		if n := len(e.instances()); n != 0 {
			t.Fatalf("instances at a scale of 0: %d", n)
		}
	})
}

// A request that is in the status and that the provider never had (the
// answer came from a provider that lost it) is made again after the grace
// time, for no more than what is missing by then.
func TestPending_RequestTheProviderNeverHad(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	obj := e.backend.GetObject(NodeGroupKind, "", "workers-dc1")
	if _, err := e.backend.UpdateObjectStatus(context.Background(), NodeGroupKind, "", "workers-dc1", map[string]any{
		"lastRequest": 1,
		"pendingRequests": []any{map[string]any{
			"number": 1, "count": 3, "asks": 1, "at": e.now.Add(-10 * time.Minute).Format(time.RFC3339),
		}},
	}, obj.ModifyIndex); err != nil {
		t.Fatal(err)
	}
	e.reconcile()
	insts := e.instances()
	if len(insts) != 1 || insts[0].RequestID != "eu/workers-dc1/1" || insts[0].Name != "workers-dc1-1-1" {
		t.Fatalf("instances: %+v", insts)
	}
	e.reconcile()
	if st := e.status(); st["pendingRequests"] != nil || st["joining"] != 1.0 || st["lastRequest"] != 1.0 {
		t.Fatalf("status: %+v", st)
	}
	if n := len(e.instances()); n != 1 {
		t.Fatalf("instances: %d", n)
	}
}
