// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bacanoo-io/norca/norca/controller"
	"github.com/bacanoo-io/norca/norca/controller/fake"
)

// serve asks a responder as the edge router or a service check does.
func serve(r *responder, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

// With leader election only the instance that leads is a target of the
// HTTP-01 challenge (review finding CON-6f): a standby is not ready, and it
// answers a challenge with 503 and says that it is a standby, instead of the
// 404 of a token nobody knows. Without leader election the instance is ready
// and answers as before.
func TestResponder_Standby(t *testing.T) {
	notLeading := func(context.Context) error { return fmt.Errorf("lease: %w", controller.ErrNotLeader) }
	leading := func(context.Context) error { return nil }

	tests := []struct {
		name    string
		leading func(context.Context) error
		set     bool

		ready, known, unknown int
	}{
		{name: "no leader election", ready: http.StatusNoContent, known: http.StatusOK, unknown: http.StatusNotFound},
		{name: "leader", leading: leading, set: true, ready: http.StatusNoContent, known: http.StatusOK, unknown: http.StatusNotFound},
		{name: "standby", leading: notLeading, set: true,
			ready: http.StatusServiceUnavailable, known: http.StatusServiceUnavailable, unknown: http.StatusServiceUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newResponder()
			if tt.set {
				r.follow(tt.leading)
			}
			r.present("tok", "tok.thumbprint")

			if rec := serve(r, http.MethodGet, ReadyPath); rec.Code != tt.ready {
				t.Errorf("%s: %d, want %d", ReadyPath, rec.Code, tt.ready)
			}
			// The process is up either way.
			if rec := serve(r, http.MethodGet, "/healthz"); rec.Code != http.StatusNoContent {
				t.Errorf("/healthz: %d", rec.Code)
			}
			for path, want := range map[string]int{ChallengePath + "tok": tt.known, ChallengePath + "other": tt.unknown} {
				rec := serve(r, http.MethodGet, path)
				if rec.Code != want {
					t.Errorf("%s: %d, want %d", path, rec.Code, want)
				}
				standby := strings.Contains(rec.Body.String(), "standby")
				if want == http.StatusServiceUnavailable && !standby {
					t.Errorf("%s: the answer of a standby does not say so: %q", path, rec.Body.String())
				}
				if rec.Code == http.StatusOK && rec.Body.String() != "tok.thumbprint" {
					t.Errorf("%s: %q", path, rec.Body.String())
				}
			}
			// What is no challenge is not found on a standby either.
			for _, path := range []string{"/", ChallengePath, ChallengePath + "tok/x"} {
				if rec := serve(r, http.MethodGet, path); rec.Code != http.StatusNotFound {
					t.Errorf("%s: %d", path, rec.Code)
				}
			}
			if rec := serve(r, http.MethodPost, ChallengePath+"tok"); rec.Code != http.StatusNotFound {
				t.Errorf("POST: %d", rec.Code)
			}
		})
	}
}

// Two instances with leader election behind an edge router that follows the
// readiness: the standby is not ready and does not answer the challenge of
// the order the leader works on as a token nobody knows; the leader answers
// it. When the leader stops, the other instance goes on with the order from
// its status, answers the challenge again, and the certificate authority
// finds the proof there (review finding CON-6f).
func TestRun_HTTP01OnlyAtTheLeader(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	// The first try to have the challenge checked fails, so the order stays
	// open with its challenge presented: the first instance waits a minute
	// of its clock, which stands still.
	e.ca.failNext("accept", errors.New("the certificate authority is busy"))
	order := e.order("default.web.1", "ca", "www.example.org")
	lease := fake.New()

	start := func(id string, op *Operator) (stop func()) {
		elector, err := controller.NewLeaderElector(lease, controller.LeaderElectionConfig{
			Path: controller.LeaseRoot + "/" + DefaultController, Identity: id, ReleaseOnCancel: true,
			LeaseDuration: 600 * time.Millisecond, RenewDeadline: 400 * time.Millisecond, RetryPeriod: 100 * time.Millisecond,
		}, op.log())
		if err != nil {
			t.Fatal(err)
		}
		// As Run does before it serves the challenges.
		op.follow(elector)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			op.run(ctx, 20*time.Millisecond, elector)
		}()
		var once sync.Once
		stop = func() {
			once.Do(func() {
				cancel()
				<-done
			})
		}
		t.Cleanup(stop)
		return stop
	}
	first, second := e.op, e.newOperator()
	// For the second instance the wait of the order is over.
	later := e.now.Add(2 * time.Minute)
	second.now = func() time.Time { return later }

	// The certificate authority asks through the edge router: an instance
	// that is ready gets the request.
	var mu sync.Mutex
	answeredBy := ""
	e.ca.checkHTTP = func(_, token, want string) bool {
		for name, op := range map[string]*Operator{"first": first, "second": second} {
			if serve(op.responder, http.MethodGet, ReadyPath).Code != http.StatusNoContent {
				continue
			}
			rec := serve(op.responder, http.MethodGet, ChallengePath+token)
			if rec.Code == http.StatusOK && rec.Body.String() == want {
				mu.Lock()
				answeredBy = name
				mu.Unlock()
				return true
			}
		}
		return false
	}

	waitUntil := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting: %s", what)
			}
		}
	}
	const token = "tok-1-0"

	stopFirst := start("first", first)
	waitUntil("the first instance answers the challenge", func() bool { return first.responder.has(token) })
	waitUntil("the first try of the order failed", func() bool {
		st := e.orderStatus(order)
		return st.Progress != nil && st.Progress.Attempts == 1
	})
	start("second", second)
	// Long enough for the second instance to have campaigned.
	time.Sleep(300 * time.Millisecond)

	if rec := serve(first.responder, http.MethodGet, ChallengePath+token); rec.Code != http.StatusOK || rec.Body.String() != token+".thumbprint" {
		t.Fatalf("the leader answers the challenge with %d %q", rec.Code, rec.Body.String())
	}
	if rec := serve(first.responder, http.MethodGet, ReadyPath); rec.Code != http.StatusNoContent {
		t.Fatalf("the leader is not ready: %d", rec.Code)
	}
	if rec := serve(second.responder, http.MethodGet, ReadyPath); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("the standby is ready: %s answers %d", ReadyPath, rec.Code)
	}
	if rec := serve(second.responder, http.MethodGet, ChallengePath+token); rec.Code != http.StatusServiceUnavailable ||
		!strings.Contains(rec.Body.String(), "standby") {
		t.Fatalf("the standby answers the challenge with %d %q", rec.Code, rec.Body.String())
	}

	// The leader stops: it is no target any more, and the other instance
	// goes on with the order.
	stopFirst()
	if rec := serve(first.responder, http.MethodGet, ReadyPath); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("the instance that led is still ready: %d", rec.Code)
	}
	if rec := serve(first.responder, http.MethodGet, ChallengePath+token); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("the instance that led still answers the challenge: %d", rec.Code)
	}
	waitUntil("the order is valid", func() bool { return e.orderStatus(order).State == StateValid })
	mu.Lock()
	defer mu.Unlock()
	if answeredBy != "second" {
		t.Fatalf("the challenge was answered by %q, want the new leader", answeredBy)
	}
	e.ca.mu.Lock()
	defer e.ca.mu.Unlock()
	if e.ca.placed != 1 {
		t.Fatalf("%d orders placed with the certificate authority", e.ca.placed)
	}
}
