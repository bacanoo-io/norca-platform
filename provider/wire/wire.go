// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package wire is what the HTTP contracts of the providers share: the shape
// of an error, the bearer token check and the JSON answers. The node
// contract (package node) and the load balancer contract (package lb) answer
// a failure in the same way:
//
//	{"reason": "invalid", "error": "port 70000 is out of range"}
package wire

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// The reasons of a failure that both contracts know. The node contract adds
// its own (quota, no_capacity).
const (
	// ReasonInvalid: the request or its parameters are wrong; the same
	// request again fails again.
	ReasonInvalid = "invalid"
	// ReasonError: any other failure, worth a retry.
	ReasonError = "error"
)

// Error is a failure with a reason, as it is sent over the wire.
type Error struct {
	Reason  string `json:"reason"`
	Message string `json:"error"`
}

func (e *Error) Error() string { return e.Reason + ": " + e.Message }

// Fail returns an Error with the reason.
func Fail(reason, format string, args ...any) error {
	return &Error{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ReasonOf returns the reason of an error: the reason of an Error, else
// ReasonError.
func ReasonOf(err error) string {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Reason
	}
	return ReasonError
}

// WriteJSON writes v as the JSON answer with the status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes a failure.
func WriteError(w http.ResponseWriter, status int, reason, message string) {
	WriteJSON(w, status, &Error{Reason: reason, Message: message})
}

// ValidToken reports whether the request carries the bearer token.
func ValidToken(r *http.Request, token string) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
}

// Bearer requires the token on every call except the paths of open (the
// health checks). An empty token disables the check: for tests and a
// provider on a trusted network (no_auth).
func Bearer(token string, next http.Handler, open ...string) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !slices.Contains(open, r.URL.Path) && !ValidToken(r, token) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			WriteError(w, http.StatusUnauthorized, ReasonInvalid, "invalid or missing token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
