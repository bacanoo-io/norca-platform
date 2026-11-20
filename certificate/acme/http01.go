// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"context"
	"net/http"
	"strings"
	"sync"
)

// ChallengePath is the path prefix of the HTTP-01 challenge (RFC 8555,
// section 8.3).
const ChallengePath = "/.well-known/acme-challenge/"

// ReadyPath is the path of the readiness of the responder: 204 while this
// instance is a target for the challenges, 503 while it is a standby. The
// check of the service that routes ChallengePath asks it, so the edge sends
// the challenges to the instance that leads only.
const ReadyPath = "/readyz"

// standbyAnswer is the body of the 503 of an instance that does not lead.
const standbyAnswer = "norca-acme standby: only the instance that holds the leader election lease answers HTTP-01 challenges\n"

// responder answers the HTTP-01 challenges of the orders that are open. It
// is served on the port the edge routes ChallengePath to, for every host.
//
// With leader election the answers are in the memory of the instance that
// leads, which works on the orders: only that instance answers, and only it
// is ready (review finding CON-6f: a standby answered a challenge the edge
// sent it with 404, as a token nobody knows, and the order failed).
type responder struct {
	mu      sync.RWMutex
	answers map[string]string

	// leading returns nil while this instance leads
	// (controller.LeaderElector.Leading); nil: no leader election, the
	// instance always answers.
	leading func(ctx context.Context) error
}

func newResponder() *responder {
	return &responder{answers: map[string]string{}}
}

// follow makes the responder answer only while leading returns nil.
func (r *responder) follow(leading func(ctx context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leading = leading
}

// standby reports whether another instance is the one that answers.
func (r *responder) standby(ctx context.Context) bool {
	r.mu.RLock()
	leading := r.leading
	r.mu.RUnlock()
	return leading != nil && leading(ctx) != nil
}

// present makes the responder answer the token with the body.
func (r *responder) present(token, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.answers[token] = body
}

// cleanUp forgets a token.
func (r *responder) cleanUp(token string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.answers, token)
}

// has reports whether a token is answered.
func (r *responder) has(token string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.answers[token]
	return ok
}

// ServeHTTP answers a challenge with its key authorization, as text, and
// everything else with 404. /healthz says that the responder is up, and
// ReadyPath that it is a target for the challenges. A standby answers a
// challenge and ReadyPath with 503: what it holds from a time it led is not
// served, the instance that leads presents the challenges of the open
// orders itself.
func (r *responder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/healthz":
		w.WriteHeader(http.StatusNoContent)
		return
	case ReadyPath:
		if r.standby(req.Context()) {
			unavailable(w)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	token, ok := strings.CutPrefix(req.URL.Path, ChallengePath)
	if !ok || token == "" || strings.Contains(token, "/") || (req.Method != http.MethodGet && req.Method != http.MethodHead) {
		http.NotFound(w, req)
		return
	}
	if r.standby(req.Context()) {
		unavailable(w)
		return
	}
	r.mu.RLock()
	body, found := r.answers[token]
	r.mu.RUnlock()
	if !found {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(body))
}

// unavailable is the answer of a standby.
func unavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(standbyAnswer))
}
