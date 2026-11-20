// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/join"
	"github.com/bacanoo-io/norca/norca/api"
)

// The tokens of a request the operator makes again (review finding CON-6h):
// the provider answers a repeated request with the instances of the first
// one, which carry the tokens of the first one.

const (
	request1 = "eu/workers/1"
	request2 = "eu/workers/2"
)

// issue issues a token of the instance for the request.
func (e *env) issue(group, request, instance string) string {
	e.t.Helper()
	token, err := e.store.Issue(context.Background(), group, request, instance, 10*time.Minute)
	if err != nil {
		e.t.Fatal(err)
	}
	return token
}

// machine is norca-join on a machine that was started with the token.
func (e *env) machine(instance, token string) *join.Client {
	e.t.Helper()
	base, _ := e.node("unused-" + strings.ReplaceAll(instance, ".", "-"))
	dir := e.t.TempDir()
	return &join.Client{Config: &join.Config{Instance: instance, Region: "eu", URLs: []string{e.srv.URL},
		Endpoint: base.Config.Endpoint, Token: token, CAFile: base.Config.CAFile,
		CertFile: filepath.Join(dir, "client.pem"), KeyFile: filepath.Join(dir, "client-key.pem"),
		StateFile: filepath.Join(dir, "state.json")}}
}

func (e *env) record(instance string) api.VariableItems {
	e.t.Helper()
	v := e.backend.GetVariable("default", "nodescaler/join/eu/"+instance)
	if v == nil {
		e.t.Fatalf("no record of %s", instance)
	}
	return v.Items
}

func refusedWith(err error, text string) bool {
	var refused *join.RefusedError
	return errors.As(err, &refused) && strings.Contains(err.Error(), text)
}

// A request made again issues a new token and keeps the one before: the
// machine of the first request joins with its first token, and the join
// voids both.
func TestReissue_TheSameRequestKeepsTheTokenBefore(t *testing.T) {
	for _, use := range []string{"first", "second"} {
		t.Run("the "+use+" token joins", func(t *testing.T) {
			e := newEnv(t)
			ctx := context.Background()
			first := e.issue("workers", request1, "workers-1-1")
			second := e.issue("workers", request1, "workers-1-1")
			if first == second {
				t.Fatal("the second issue made no new token")
			}
			used, other := first, second
			if use == "second" {
				used, other = second, first
			}
			if err := e.machine("workers-1-1", used).Join(ctx, ""); err != nil {
				t.Fatalf("the %s token of a request made again does not join: %v", use, err)
			}
			// One machine per name: no token of the name is valid after a join.
			for name, token := range map[string]string{"the other": other, "the used": used} {
				if err := e.machine("workers-1-1", token).Join(ctx, ""); !refusedWith(err, "was used") {
					t.Fatalf("%s token after the join: %v", name, err)
				}
			}
			if rec := e.record("workers-1-1"); rec["token_sha256"] != "" || rec["previous_tokens"] != "" || rec["serial"] == "" {
				t.Fatalf("the record after the join: %v", rec)
			}
		})
	}
}

// A request is made again as often as the listing stays behind: the machine
// has the token of the first time, whatever number of issues follow.
func TestReissue_AThirdIssueKeepsTheFirstToken(t *testing.T) {
	e := newEnv(t)
	first := e.issue("workers", request1, "workers-1-1")
	e.issue("workers", request1, "workers-1-1")
	third := e.issue("workers", request1, "workers-1-1")
	if n := len(strings.Split(e.record("workers-1-1")["previous_tokens"], ",")); n != 2 {
		t.Fatalf("%d earlier tokens in the record, want 2", n)
	}
	if err := e.machine("workers-1-1", first).Join(context.Background(), ""); err != nil {
		t.Fatalf("the first token after a third issue: %v", err)
	}
	if err := e.machine("workers-1-1", third).Join(context.Background(), ""); !refusedWith(err, "was used") {
		t.Fatalf("the third token after the join: %v", err)
	}
}

// The record keeps a bounded number of earlier tokens: the oldest go first.
func TestReissue_TheEarlierTokensAreBounded(t *testing.T) {
	e := newEnv(t)
	first := e.issue("workers", request1, "workers-1-1")
	second := e.issue("workers", request1, "workers-1-1")
	for range 16 {
		e.issue("workers", request1, "workers-1-1")
	}
	if n := len(strings.Split(e.record("workers-1-1")["previous_tokens"], ",")); n != 16 {
		t.Fatalf("%d earlier tokens in the record, want 16", n)
	}
	ctx := context.Background()
	if _, _, err := e.store.Redeem(ctx, "workers-1-1", first); !errors.Is(err, join.ErrRefused) {
		t.Fatalf("the token dropped as the oldest: %v", err)
	}
	if _, _, err := e.store.Redeem(ctx, "workers-1-1", second); err != nil {
		t.Fatalf("the oldest token that is kept: %v", err)
	}
}

// The token before keeps its own expiry: the issue of a new one does not
// extend it, and an expired one is not carried into the next record.
func TestReissue_TheTokenBeforeExpiresAtItsOwnTime(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	start := time.Now()
	now := start
	e.store.Now = func() time.Time { return now }

	first := e.issue("workers", request1, "workers-1-1") // until start + 10m
	now = start.Add(6 * time.Minute)
	second := e.issue("workers", request1, "workers-1-1") // until start + 16m
	rec := e.record("workers-1-1")
	if want := "@" + start.Add(10*time.Minute).UTC().Format(time.RFC3339); !strings.HasSuffix(rec["previous_tokens"], want) {
		t.Fatalf("the expiry of the token before is %q, want its own, %s", rec["previous_tokens"], want)
	}

	now = start.Add(9 * time.Minute)
	if _, _, err := e.store.Redeem(ctx, "workers-1-1", first); err != nil {
		t.Fatalf("the first token within its life: %v", err)
	}
	now = start.Add(11 * time.Minute)
	if _, _, err := e.store.Redeem(ctx, "workers-1-1", first); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("the first token after its own expiry: %v", err)
	}
	if _, _, err := e.store.Redeem(ctx, "workers-1-1", second); err != nil {
		t.Fatalf("the second token within its life: %v", err)
	}
	// The next issue leaves the expired one behind.
	e.issue("workers", request1, "workers-1-1")
	if prev := e.record("workers-1-1")["previous_tokens"]; strings.Contains(prev, ",") || prev == "" {
		t.Fatalf("the earlier tokens after the expiry of the first: %q, want the second only", prev)
	}
	now = start.Add(17 * time.Minute)
	if _, _, err := e.store.Redeem(ctx, "workers-1-1", second); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("the second token after its own expiry: %v", err)
	}
}

// Only the same request keeps a token: an issue for the name by another
// request, by another group or without a request voids every token before,
// and so does an issue over a record that names no request.
func TestReissue_AnotherRequestVoidsTheTokens(t *testing.T) {
	ctx := context.Background()
	for name, next := range map[string][2]string{
		"another request": {"workers", request2},
		"another group":   {"workers-b", request1},
		"no request":      {"workers", ""},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			first := e.issue("workers", request1, "workers-1-1")
			second := e.issue("workers", request1, "workers-1-1")
			token := e.issue(next[0], next[1], "workers-1-1")
			if prev := e.record("workers-1-1")["previous_tokens"]; prev != "" {
				t.Fatalf("the record keeps tokens of the request before: %q", prev)
			}
			for _, old := range []string{first, second} {
				if err := e.machine("workers-1-1", old).Join(ctx, ""); !refusedWith(err, "a wrong token") {
					t.Fatalf("a token of the request before: %v", err)
				}
			}
			if _, group, err := e.store.Redeem(ctx, "workers-1-1", token); err != nil || group != next[0] {
				t.Fatalf("the new token: %q %v", group, err)
			}
		})
	}

	t.Run("a record without a request", func(t *testing.T) {
		e := newEnv(t)
		old := e.issue("workers", "", "workers-1-1")
		// Such a record is read as one without an earlier token.
		if _, _, err := e.store.Redeem(ctx, "workers-1-1", old); err != nil {
			t.Fatalf("the token of a record without a request: %v", err)
		}
		e.issue("workers", request1, "workers-1-1")
		if err := e.machine("workers-1-1", old).Join(ctx, ""); !refusedWith(err, "a wrong token") {
			t.Fatalf("the token of a record without a request, after an issue for a request: %v", err)
		}
	})

	// A token is of its name: the one of another instance of the request is
	// not a token of this one.
	t.Run("another instance", func(t *testing.T) {
		e := newEnv(t)
		e.issue("workers", request1, "workers-1-1")
		other := e.issue("workers", request1, "workers-1-2")
		e.issue("workers", request1, "workers-1-1")
		if err := e.machine("workers-1-1", other).Join(ctx, ""); !refusedWith(err, "a wrong token") {
			t.Fatalf("the token of another instance: %v", err)
		}
	})
}

// An instance that joined gets no token when its request is made again: its
// record is not written, so its certificate still renews; and no token of
// its name is valid again.
func TestReissue_AJoinedInstanceKeepsItsRecord(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	first := e.issue("workers", request1, "workers-1-1")
	second := e.issue("workers", request1, "workers-1-1")
	c := e.machine("workers-1-1", first)
	if err := c.Join(ctx, ""); err != nil {
		t.Fatal(err)
	}
	before := e.backend.GetVariable("default", "nodescaler/join/eu/workers-1-1")

	token, err := e.store.Issue(ctx, "workers", request1, "workers-1-1", 10*time.Minute)
	if !errors.Is(err, join.ErrJoined) || token != "" {
		t.Fatalf("an issue for an instance that joined: token %t, error %v; want none and ErrJoined", token != "", err)
	}
	after := e.backend.GetVariable("default", "nodescaler/join/eu/workers-1-1")
	if after.ModifyIndex != before.ModifyIndex || after.Items["serial"] != before.Items["serial"] || after.Items["token_sha256"] != "" {
		t.Fatalf("the record of an instance that joined was written: %v", after.Items)
	}
	if err := c.Renew(ctx); err != nil {
		t.Fatalf("the renewal after its request was made again: %v", err)
	}
	checkNodeCert(t, e, c, "workers-1-1")
	for _, old := range []string{first, second} {
		if err := e.machine("workers-1-1", old).Join(ctx, ""); !refusedWith(err, "was used") {
			t.Fatalf("a token of an instance that joined: %v", err)
		}
	}
}

// The addresses the operator recorded for an instance stay when its request
// is made again: the join stays bound to them.
func TestReissue_TheAddressesStay(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.issue("workers", request1, "workers-1-1")
	if _, err := e.store.Seen(ctx, "workers", "workers-1-1", []string{"10.0.0.9"}); err != nil {
		t.Fatal(err)
	}
	e.issue("workers", request1, "workers-1-1")
	if rec := e.record("workers-1-1"); rec["addresses"] != "10.0.0.9" || rec["seen"] == "" {
		t.Fatalf("the record after the second issue: %v", rec)
	}
	e.issue("workers", request2, "workers-1-1")
	if rec := e.record("workers-1-1"); rec["addresses"] != "" || rec["seen"] != "" {
		t.Fatalf("the record of another request keeps the addresses: %v", rec)
	}
}

// A join that is between the check of its token and the write of its
// certificate when the request is made again still goes through, and uses up
// the new token too.
func TestReissue_AJoinInFlight(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	first := e.issue("workers", request1, "workers-1-1")
	rec, _, err := e.store.Redeem(ctx, "workers-1-1", first)
	if err != nil {
		t.Fatal(err)
	}
	second := e.issue("workers", request1, "workers-1-1")
	if err := e.store.Joined(ctx, rec, first, "aa"); err != nil {
		t.Fatalf("a join over an issue for the same request: %v", err)
	}
	if _, _, err := e.store.Redeem(ctx, "workers-1-1", second); !errors.Is(err, join.ErrRefused) {
		t.Fatalf("the new token after the join: %v", err)
	}

	// An issue for another request in between voids the token: the join fails.
	third := e.issue("workers", request2, "workers-1-2")
	rec, _, err = e.store.Redeem(ctx, "workers-1-2", third)
	if err != nil {
		t.Fatal(err)
	}
	e.issue("workers", "eu/workers/3", "workers-1-2")
	if err := e.store.Joined(ctx, rec, third, "bb"); !errors.Is(err, join.ErrRefused) {
		t.Fatalf("a join over an issue for another request: %v", err)
	}
}

// The record of an unused token goes when no token of it can be used any
// more, not when the latest alone expired.
func TestReissue_PruneWaitsForEveryToken(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	start := time.Now()
	now := start
	e.store.Now = func() time.Time { return now }
	e.issue("workers", request1, "workers-1-1")
	if _, err := e.store.Issue(ctx, "workers", request1, "workers-1-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	now = start.Add(5 * time.Minute) // the latest expired, the first not
	if err := e.store.Prune(ctx, "workers", "workers", map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if e.backend.GetVariable("default", "nodescaler/join/eu/workers-1-1") == nil {
		t.Fatal("the record went while a token of it is valid")
	}
	now = start.Add(11 * time.Minute)
	if err := e.store.Prune(ctx, "workers", "workers", map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if e.backend.GetVariable("default", "nodescaler/join/eu/workers-1-1") != nil {
		t.Fatal("the record stays after every token expired")
	}
}
