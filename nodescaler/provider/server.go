// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

// maxRequestBody bounds the body of a create request; the bootstrap of a
// machine is the largest part.
const maxRequestBody = 1 << 20

// NewHandler serves the provider over the HTTP contract. With a token, every
// call must send it as a bearer token; an empty token disables the check (for
// tests and a provider on a private address).
func NewHandler(p Provider, token string, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &handler{p: p, token: token, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/groups/{group}", h.group)
	mux.HandleFunc("POST /v1/groups/{group}/instances", h.create)
	mux.HandleFunc("DELETE /v1/groups/{group}/instances/{id}", h.delete)
	return h.auth(mux)
}

type handler struct {
	p     Provider
	token string
	log   *slog.Logger
}

func (h *handler) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.token != "" && r.URL.Path != "/v1/health" {
			got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(h.token)) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeError(w, http.StatusUnauthorized, &Error{Reason: ReasonInvalid, Message: "invalid or missing token"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// group serves GET: the parameters of the machine kind are the query.
func (h *handler) group(w http.ResponseWriter, r *http.Request) {
	params := map[string]any{}
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	g, err := h.p.Group(r.Context(), r.PathValue("group"), params)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if g.Instances == nil {
		g.Instances = []Instance{}
	}
	writeJSON(w, http.StatusOK, g)
}

func (h *handler) create(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxRequestBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, &Error{Reason: ReasonInvalid, Message: "the request is not valid JSON: " + err.Error()})
		return
	}
	switch {
	case req.RequestID == "":
		writeError(w, http.StatusBadRequest, &Error{Reason: ReasonInvalid, Message: "request_id is required"})
		return
	case len(req.Instances) == 0:
		writeError(w, http.StatusBadRequest, &Error{Reason: ReasonInvalid, Message: "instances must not be empty"})
		return
	}
	names := map[string]bool{}
	for _, inst := range req.Instances {
		if inst.Name == "" || names[inst.Name] {
			writeError(w, http.StatusBadRequest, &Error{Reason: ReasonInvalid, Message: "every instance needs a name of its own"})
			return
		}
		names[inst.Name] = true
	}
	instances, err := h.p.Create(r.Context(), r.PathValue("group"), req)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.log.Info("instances created", "group", r.PathValue("group"), "request_id", req.RequestID, "count", len(instances))
	writeJSON(w, http.StatusOK, CreateResponse{Instances: instances})
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	if err := h.p.Delete(r.Context(), r.PathValue("group"), r.PathValue("id")); err != nil {
		h.fail(w, r, err)
		return
	}
	h.log.Info("instance deleted", "group", r.PathValue("group"), "id", r.PathValue("id"))
	w.WriteHeader(http.StatusNoContent)
}

// fail writes the error of the provider: 404 for an unknown group, 400 for
// an invalid request, 422 for another reason, 500 otherwise.
func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	var e *Error
	switch {
	case errors.Is(err, ErrUnknownGroup):
		writeError(w, http.StatusNotFound, &Error{Reason: ReasonInvalid, Message: "unknown group " + r.PathValue("group")})
	case errors.As(err, &e) && e.Reason == ReasonInvalid:
		writeError(w, http.StatusBadRequest, e)
	case errors.As(err, &e) && e.Reason != ReasonError:
		writeError(w, http.StatusUnprocessableEntity, e)
	default:
		h.log.Warn("the provider failed", "method", r.Method, "path", r.URL.Path, "error", err)
		writeError(w, http.StatusInternalServerError, &Error{Reason: ReasonError, Message: err.Error()})
	}
}

func writeError(w http.ResponseWriter, status int, e *Error) {
	writeJSON(w, status, e)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
