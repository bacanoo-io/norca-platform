// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"net/http"
	"strings"
	"sync"
)

// ChallengePath is the path prefix of the HTTP-01 challenge (RFC 8555,
// section 8.3).
const ChallengePath = "/.well-known/acme-challenge/"

// responder answers the HTTP-01 challenges of the orders that are open. It
// is served on the port the edge routes ChallengePath to, for every host.
type responder struct {
	mu      sync.RWMutex
	answers map[string]string
}

func newResponder() *responder {
	return &responder{answers: map[string]string{}}
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
// everything else with 404. /healthz says that the responder is up.
func (r *responder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	token, ok := strings.CutPrefix(req.URL.Path, ChallengePath)
	if !ok || token == "" || strings.Contains(token, "/") || (req.Method != http.MethodGet && req.Method != http.MethodHead) {
		http.NotFound(w, req)
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
