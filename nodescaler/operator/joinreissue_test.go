// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
	provider "github.com/bacanoo-io/norca-platform/provider/node"
)

// The join tokens of a request that is made again (review finding CON-6h):
// the provider answers with the machines of the first time, which were
// started with the tokens of the first time.

// joinEnv is a group whose instances join with a key of their own.
func joinEnv(t *testing.T) (*env, *join.Store) {
	t.Helper()
	e := newEnv(t, "")
	items, _ := bootstraptest.Items(t)
	e.withBootstrap(items)
	return e, e.withJoin("https://10.0.0.7:8490")
}

// tokenIn reads the join token of a bootstrap.
func tokenIn(t *testing.T, userData string) string {
	t.Helper()
	raw, ok := fileOf(t, userData, "/etc/norca/norca-join.json")
	if !ok {
		t.Fatalf("no join configuration:\n%s", userData)
	}
	var conf join.Config
	if err := json.Unmarshal([]byte(raw), &conf); err != nil {
		t.Fatal(err)
	}
	if conf.Token == "" {
		t.Fatal("a join configuration without a token")
	}
	return conf.Token
}

// machineTokens are the tokens the machines of the provider were started
// with, by the name of the instance.
func (e *env) machineTokens() map[string]string {
	e.t.Helper()
	out := map[string]string{}
	for _, inst := range e.instances() {
		out[inst.Name] = tokenIn(e.t, e.mock.UserData(inst.ID))
	}
	return out
}

// redeems checks that every machine can still join with its token.
func redeems(t *testing.T, store *join.Store, tokens map[string]string, when string) {
	t.Helper()
	for name, token := range tokens {
		if _, group, err := store.Redeem(context.Background(), name, token); err != nil || group != "workers-dc1" {
			t.Errorf("%s: the machine %s cannot join with the token it was started with: %v", when, name, err)
		}
	}
}

// The listing does not show the instances of a request within the grace
// time: the request is made again, the provider answers with the machines it
// has, and they still join with the tokens they have, however often the
// request is repeated. A join uses up every token of its name.
func TestJoinReissue_GraceTimeReAskKeepsTheTokensOfTheMachines(t *testing.T) {
	e, store := joinEnv(t)
	e.putGroup(2, nil)
	e.reconcile()
	first := e.machineTokens()
	if len(first) != 2 {
		t.Fatalf("machines: %v", first)
	}
	e.lag.hide(hideAll)

	for ask := 2; ask <= 3; ask++ {
		e.advance(e.r.cfg.ListGrace + time.Second)
		e.reconcile()
		if n, asked := len(e.instances()), e.lag.asked(); n != 2 || len(asked) != ask || asked[ask-1] != "eu/workers-dc1/1" {
			t.Fatalf("ask %d: %d instances, requests %v", ask, n, asked)
		}
		redeems(t, store, first, "after the request was made again")
		// The request carried new tokens, valid too: a provider that had
		// lost the first request would start its machines with them.
		for _, ir := range e.lag.lastRequest().Instances {
			if ir.Bootstrap == nil {
				t.Fatalf("ask %d: %s has no bootstrap", ask, ir.Name)
			}
			token := tokenIn(t, ir.Bootstrap.UserData)
			if token == first[ir.Name] {
				t.Fatalf("ask %d: the token of %s is not new", ask, ir.Name)
			}
			if _, _, err := store.Redeem(context.Background(), ir.Name, token); err != nil {
				t.Fatalf("ask %d: the new token of %s: %v", ask, ir.Name, err)
			}
		}
	}
	if t.Failed() {
		return
	}

	// One machine per name: the join with the first token voids the later
	// ones of the name.
	const name = "workers-dc1-1-1"
	var later string
	for _, ir := range e.lag.lastRequest().Instances {
		if ir.Name == name {
			later = tokenIn(t, ir.Bootstrap.UserData)
		}
	}
	rec, _, err := store.Redeem(context.Background(), name, first[name])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Joined(context.Background(), rec, first[name], "aa"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Redeem(context.Background(), name, later); err == nil {
		t.Fatal("a later token of the name joins after the machine joined")
	}
}

// The answer of a create is lost: the machines exist with their tokens, the
// request is made again with its number, and they still join.
func TestJoinReissue_LostCreateAnswerKeepsTheTokensOfTheMachines(t *testing.T) {
	e, store := joinEnv(t)
	e.putGroup(2, nil)
	e.lag.loseNextAnswer()
	e.reconcile()
	first := e.machineTokens()
	if c := conditionOf(e.status(), CondProvisioned); len(first) != 2 || c["status"] != "False" {
		t.Fatalf("after the lost answer: machines %v, Provisioned %+v", first, c)
	}
	// The listing does not show them yet: the request is made again.
	e.lag.hide(hideAll)
	e.reconcile()
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/1"}) {
		t.Fatalf("%d instances, requests %v", n, asked)
	}
	redeems(t, store, first, "after the request was made again")
}

// A request is made again for instances one of which has joined: its record
// keeps the serial of its certificate (the renewal of the node goes on), it
// gets no token, and the others get theirs. The request made again does not
// name it, whether the listing shows it or not: a provider that has it
// answers with it anyway, and one that has no instance of the request left
// would make a machine without a bootstrap of the name (review finding
// CON-6m).
func TestJoinReissue_AJoinedNodeKeepsItsRecord(t *testing.T) {
	const joined, missing = "workers-dc1-1-1", "workers-dc1-1-2"
	for name, hidden := range map[string]func(provider.Instance) bool{
		"the one that joined is listed":     func(i provider.Instance) bool { return i.Name == missing },
		"the one that joined is not listed": hideAll,
	} {
		t.Run(name, func(t *testing.T) { joinedNodeKeepsItsRecord(t, joined, missing, hidden) })
	}
}

func joinedNodeKeepsItsRecord(t *testing.T, joined, missing string, hidden func(provider.Instance) bool) {
	e, store := joinEnv(t)
	e.putGroup(2, nil)
	e.reconcile()
	first := e.machineTokens()
	ctx := context.Background()
	rec, _, err := store.Redeem(ctx, joined, first[joined])
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Joined(ctx, rec, first[joined], "aa"); err != nil {
		t.Fatal(err)
	}
	before := e.backend.GetVariable("default", "nodescaler/join/eu/"+joined)

	// The listing does not show the other.
	e.lag.hide(hidden)
	e.advance(e.r.cfg.ListGrace + time.Second)
	e.reconcile()
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/1"}) {
		t.Fatalf("%d instances, requests %v", n, asked)
	}

	after := e.backend.GetVariable("default", "nodescaler/join/eu/"+joined)
	if after == nil || after.Items["serial"] != "aa" || after.Items["token_sha256"] != "" || after.ModifyIndex != before.ModifyIndex {
		t.Fatalf("the record of the node that joined was written by the repeated request: %+v", after)
	}
	// Its renewal: the latest certificate is still the one it holds.
	rec, _, err = store.Renewing(ctx, joined, "aa")
	if err != nil {
		t.Fatalf("the renewal of the node that joined, after its request was made again: %v", err)
	}
	if err := store.Renewed(ctx, rec, "bb"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Redeem(ctx, joined, first[joined]); err == nil {
		t.Fatal("the used token of the node that joined is valid again")
	}

	req := e.lag.lastRequest()
	if !slices.Equal(namesOf(req), []string{missing}) {
		t.Fatalf("the repeated request names %v, want only %s", namesOf(req), missing)
	}
	if ir := req.Instances[0]; ir.Bootstrap == nil || tokenIn(t, ir.Bootstrap.UserData) == first[missing] {
		t.Fatal("the instance that is not listed got no new token")
	}
	redeems(t, store, map[string]string{missing: first[missing]}, "after the request was made again")
}

// joinAll makes every machine of the provider join with the token it was
// started with.
func joinAll(t *testing.T, e *env, store *join.Store) {
	t.Helper()
	ctx := context.Background()
	for name, token := range e.machineTokens() {
		rec, _, err := store.Redeem(ctx, name, token)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Joined(ctx, rec, token, "aa"); err != nil {
			t.Fatal(err)
		}
	}
}

// Every instance of a request has joined and none is listed after the grace
// time: a request made again could name only instances without a bootstrap,
// so none is made. While their nodes are ready the request stays, its grace
// time starts again, and the group says so.
func TestJoinReissue_OnlyJoinedInstancesAreLeftToAskFor(t *testing.T) {
	e, store := joinEnv(t)
	e.putGroup(2, nil)
	e.reconcile()
	joinAll(t, e, store)
	e.nodes.set(
		Node{ID: "n1", Name: "workers-dc1-1-1", Instance: "workers-dc1-1-1", Group: "workers-dc1", Ready: true, Eligible: true},
		Node{ID: "n2", Name: "workers-dc1-1-2", Instance: "workers-dc1-1-2", Group: "workers-dc1", Ready: true, Eligible: true})
	e.lag.hide(hideAll)

	e.advance(e.r.cfg.ListGrace + time.Second)
	if d := e.reconcile(); d != e.r.cfg.JoinCheck {
		t.Fatalf("requeue %v, want the join check", d)
	}
	if n, asked := len(e.instances()), e.lag.asked(); n != 2 || len(asked) != 1 {
		t.Fatalf("%d instances, requests %v: a request was made for instances that have joined", n, asked)
	}
	st := e.status()
	if p := pendingOf(st); len(p) != 1 || p[0]["asks"] != 2.0 || p[0]["at"] != e.now.Format(time.RFC3339) || !e.hasEvent("InstancesNotListed") {
		t.Fatalf("pendingRequests: %+v", p)
	}
	if c := conditionOf(st, CondProvisioned); c["status"] != "False" || c["reason"] != "NotListed" {
		t.Fatalf("Provisioned: %+v", c)
	}
	if len(e.nodes.madeIneligible()) != 0 {
		t.Fatalf("made ineligible: %v", e.nodes.madeIneligible())
	}

	// Their nodes are gone: the request is forgotten, and what is missing is
	// a new request.
	e.nodes.set()
	e.advance(e.r.cfg.ListGrace + time.Second)
	e.reconcile()
	if p := pendingOf(e.status()); len(p) != 0 {
		t.Fatalf("pendingRequests after the nodes went: %+v", p)
	}
	e.reconcile()
	if asked := e.lag.asked(); !slices.Equal(asked, []string{"eu/workers-dc1/1", "eu/workers-dc1/2"}) {
		t.Fatalf("requests after the request was forgotten: %v", asked)
	}
	for _, ir := range e.lag.lastRequest().Instances {
		if ir.Bootstrap == nil {
			t.Fatalf("%s has no bootstrap", ir.Name)
		}
	}
}
