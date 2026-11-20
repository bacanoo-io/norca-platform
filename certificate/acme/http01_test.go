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
	"sync/atomic"
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

// testClock is the clock of an operator whose passes run on their own while
// the test moves the time.
type testClock struct {
	base   time.Time
	offset atomic.Int64
}

func (c *testClock) now() time.Time          { return c.base.Add(time.Duration(c.offset.Load())) }
func (c *testClock) advance(d time.Duration) { c.offset.Add(int64(d)) }

// countHTTPChecks counts how often the certificate authority looks for the
// answer of an HTTP-01 challenge: once for every challenge it is told to
// check.
func (e *env) countHTTPChecks() *atomic.Int32 {
	checks := &atomic.Int32{}
	check := e.ca.checkHTTP
	e.ca.checkHTTP = func(name, token, want string) bool {
		checks.Add(1)
		return check(name, token, want)
	}
	return checks
}

// runInstance runs an operator with a leader election on the lease, as Run
// does, with a pass every 20ms. It returns the function that stops it.
func runInstance(t *testing.T, lease *fake.Backend, id string, op *Operator) (stop func()) {
	t.Helper()
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

// waitFor waits until the condition holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: %s", what)
		}
	}
}

// An instance that has only just become ready is not a target of the edge
// yet: the challenge of an order is presented at once, and the certificate
// authority is told to check it only when the settle time since the instance
// started to answer is over (review finding CON-6g: it was told in the same
// pass, the check reached nobody and the order failed once). Waiting counts
// no attempt and writes nothing. The settle time holds back HTTP-01 only, and
// only right after the instance started to answer.
func TestOrder_HTTP01Settle(t *testing.T) {
	const settle = 30 * time.Second
	start := func(t *testing.T, settle time.Duration) (*env, *atomic.Int32) {
		t.Helper()
		e := newEnv(t)
		e.op.http01Settle = settle
		e.issuer("ca", nil)
		checks := e.countHTTPChecks()
		// As run does when the passes start.
		e.op.answering()
		return e, checks
	}
	// held makes passes and fails if the challenge of the order is not
	// answered, the certificate authority was told to check it, or the wait
	// left a trace in the order.
	held := func(t *testing.T, e *env, checks *atomic.Int32, order, token string, want int32, when string) {
		t.Helper()
		e.pass()
		e.pass()
		if !e.op.responder.has(token) {
			t.Fatalf("%s: the challenge is not answered", when)
		}
		writes := e.cluster.statusWrites["CertificateOrder/"+order]
		e.pass()
		e.pass()
		st := e.orderStatus(order)
		if got := checks.Load(); got != want {
			t.Fatalf("%s: the certificate authority was told to check a challenge during the settle time (%d checks, want %d)", when, got, want)
		}
		if st.State != StateProcessing || st.Progress == nil || st.Progress.Attempts != 0 || st.Progress.RetryAt != "" {
			t.Fatalf("%s: the order that waits: %+v %+v", when, st, st.Progress)
		}
		if got := e.cluster.statusWrites["CertificateOrder/"+order]; got != writes {
			t.Fatalf("%s: the order that waits was written %d times", when, got-writes)
		}
	}
	valid := func(t *testing.T, e *env, order string) {
		t.Helper()
		if st := e.passes(order, 3); st.State != StateValid {
			t.Fatalf("the order: %+v %+v", st, st.Progress)
		}
	}

	t.Run("after the start", func(t *testing.T) {
		e, checks := start(t, settle)
		order := e.order("default.web.1", "ca", "www.example.org")
		held(t, e, checks, order, "tok-1-0", 0, "right after the start")
		e.advance(settle - time.Second)
		held(t, e, checks, order, "tok-1-0", 0, "a second before the settle time is over")
		e.advance(time.Second)
		valid(t, e, order)
		if checks.Load() != 1 || e.ca.placed != 1 {
			t.Fatalf("%d checks of the challenge, %d orders placed", checks.Load(), e.ca.placed)
		}
	})

	t.Run("no settle time", func(t *testing.T) {
		e, checks := start(t, 0)
		order := e.order("default.web.1", "ca", "www.example.org")
		valid(t, e, order)
		if checks.Load() != 1 {
			t.Fatalf("%d checks of the challenge", checks.Load())
		}
	})

	t.Run("an order long after the start", func(t *testing.T) {
		e, checks := start(t, settle)
		e.advance(time.Hour)
		order := e.order("default.web.1", "ca", "www.example.org")
		valid(t, e, order)
		if checks.Load() != 1 {
			t.Fatalf("%d checks of the challenge", checks.Load())
		}
	})

	t.Run("answering again", func(t *testing.T) {
		e, checks := start(t, settle)
		e.advance(settle)
		valid(t, e, e.order("default.web.1", "ca", "www.example.org"))
		// The lead was lost and gained again: the passes start again.
		e.op.answering()
		order := e.order("default.web.2", "ca", "www.example.org")
		held(t, e, checks, order, "tok-2-0", 1, "right after the passes started again")
		e.advance(settle)
		valid(t, e, order)
		if checks.Load() != 2 {
			t.Fatalf("%d checks of the challenges", checks.Load())
		}
	})

	t.Run("DNS-01 does not wait", func(t *testing.T) {
		e, checks := start(t, settle)
		order := e.order("default.wild.1", "ca", "example.com", "*.example.com")
		if st := e.passes(order, 5); st.State != StateValid {
			t.Fatalf("the order: %+v %+v", st, st.Progress)
		}
		if checks.Load() != 0 {
			t.Fatalf("%d HTTP-01 checks of an order proven with DNS-01", checks.Load())
		}
	})
}

// The lease passes to another instance while an order is open. The edge
// routes to the new leader only some time after it became ready, and until
// then the certificate authority finds nobody. The new leader answers the
// challenge at once, tells the certificate authority to check it only after
// the settle time, and the order is fulfilled (review finding CON-6g: it told
// the certificate authority in its first pass over the order, and the order
// failed).
func TestRun_HTTP01SettleAfterTakeover(t *testing.T) {
	const settle = 30 * time.Second
	const token = "tok-1-0"
	e := newEnv(t)
	e.issuer("ca", nil)
	// The first try to have the challenge checked fails, so the order stays
	// open with its challenge presented.
	e.ca.failNext("accept", errors.New("the certificate authority is busy"))
	order := e.order("default.web.1", "ca", "www.example.org")
	lease := fake.New()

	first, second := e.op, e.newOperator()
	// The first instance tells the certificate authority at once.
	first.http01Settle = 0
	// For the second instance the wait of the order is over.
	clock := &testClock{base: e.now.Add(2 * time.Minute)}
	second.now = clock.now
	second.http01Settle = settle
	passes := &countingCluster{cluster: second.cluster}
	second.cluster = passes

	// The certificate authority asks through the edge router, which routes
	// to the new leader only once it follows its readiness.
	var routed atomic.Bool
	var checks atomic.Int32
	e.ca.checkHTTP = func(_, token, want string) bool {
		checks.Add(1)
		if !routed.Load() {
			return false
		}
		rec := serve(second.responder, http.MethodGet, ChallengePath+token)
		return rec.Code == http.StatusOK && rec.Body.String() == want
	}

	stopFirst := runInstance(t, lease, "first", first)
	waitFor(t, "the first instance answers the challenge", func() bool { return first.responder.has(token) })
	waitFor(t, "the first try of the order failed", func() bool {
		st := e.orderStatus(order)
		return st.Progress != nil && st.Progress.Attempts == 1
	})
	runInstance(t, lease, "second", second)
	stopFirst()

	// The new leader answers at once, so the answer is there when the edge
	// routes to it.
	waitFor(t, "the new leader answers the challenge", func() bool {
		return serve(second.responder, http.MethodGet, ChallengePath+token).Code == http.StatusOK
	})
	seen := passes.passes.Load()
	waitFor(t, "the new leader made more passes", func() bool { return passes.passes.Load() >= seen+5 })
	if n := checks.Load(); n != 0 {
		t.Fatalf("the certificate authority was told to check the challenge during the settle time, before the edge routes to the new leader (%d checks): %+v",
			n, e.orderStatus(order))
	}
	if st := e.orderStatus(order); st.State != StateProcessing || st.Progress == nil || st.Progress.Attempts != 1 {
		t.Fatalf("the order during the settle time: %+v %+v", st, st.Progress)
	}

	routed.Store(true)
	clock.advance(settle)
	waitFor(t, "the order is valid", func() bool { return e.orderStatus(order).State == StateValid })
	e.ca.mu.Lock()
	defer e.ca.mu.Unlock()
	if checks.Load() != 1 || e.ca.placed != 1 {
		t.Fatalf("%d checks of the challenge, %d orders placed with the certificate authority", checks.Load(), e.ca.placed)
	}
}

// An instance that loses the lease and gains it again was a standby in
// between, so the edge has to find it again: the settle time starts again.
// An order placed long after the instance started to lead does not wait.
func TestRun_HTTP01SettleAfterLeadRegained(t *testing.T) {
	const settle = 30 * time.Second
	e := newEnv(t)
	e.issuer("ca", nil)
	checks := e.countHTTPChecks()
	op := e.op
	clock := &testClock{base: e.now}
	op.now = clock.now
	op.http01Settle = settle
	passes := &countingCluster{cluster: op.cluster}
	op.cluster = passes
	lease := fake.New()
	ready := func() bool { return serve(op.responder, http.MethodGet, ReadyPath).Code == http.StatusNoContent }
	valid := func(order string) func() bool {
		return func() bool { return e.orderStatus(order).State == StateValid }
	}

	runInstance(t, lease, "only", op)
	waitFor(t, "the instance leads and works", func() bool { return ready() && passes.passes.Load() > 0 })
	clock.advance(time.Hour)
	waitFor(t, "an order long after the start is valid", valid(e.order("default.web.1", "ca", "www.example.org")))
	if checks.Load() != 1 {
		t.Fatalf("%d checks of the challenge", checks.Load())
	}

	// Somebody else takes the lease and does not renew it.
	path := controller.LeaseRoot + "/" + DefaultController
	held := lease.GetVariable("", path)
	if held == nil {
		t.Fatal("no lease")
	}
	held.Items["holder"] = "another"
	lease.PutVariable(held)
	waitFor(t, "the instance lost the lead", func() bool { return !ready() })
	order := e.order("default.web.2", "ca", "www.example.org")
	waitFor(t, "the instance leads again", ready)

	waitFor(t, "the challenge of the second order is answered", func() bool { return op.responder.has("tok-2-0") })
	seen := passes.passes.Load()
	waitFor(t, "more passes", func() bool { return passes.passes.Load() >= seen+5 })
	if n := checks.Load(); n != 1 {
		t.Fatalf("the certificate authority was told to check the challenge right after the lead was gained again (%d checks)", n)
	}
	if st := e.orderStatus(order); st.State != StateProcessing || st.Progress == nil || st.Progress.Attempts != 0 {
		t.Fatalf("the order during the settle time: %+v %+v", st, st.Progress)
	}

	clock.advance(settle)
	waitFor(t, "the second order is valid", valid(order))
	if checks.Load() != 2 {
		t.Fatalf("%d checks of the challenges", checks.Load())
	}
}
