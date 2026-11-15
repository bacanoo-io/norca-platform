// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dns

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/wire"
)

// DefaultTimeout bounds one call of a provider.
const DefaultTimeout = 2 * time.Minute

// NewHandler serves one zone over the HTTP contract, at the paths below its
// endpoint (/v1/txt/...). With a token, every call must send it as a bearer
// token; an empty token disables the check (no_auth, a trusted network).
func NewHandler(z *Zone, token string, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &handler{z: z, log: log, timeout: DefaultTimeout}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/txt/{name}", h.put)
	mux.HandleFunc("DELETE /v1/txt/{name}", h.delete)
	return wire.Bearer(token, mux)
}

type handler struct {
	z       *Zone
	log     *slog.Logger
	timeout time.Duration

	mu    sync.Mutex
	names map[string]*nameLock
}

type nameLock struct {
	mu   sync.Mutex
	refs int
}

// lock serializes the calls for one name.
func (h *handler) lock(name string) (unlock func()) {
	h.mu.Lock()
	if h.names == nil {
		h.names = map[string]*nameLock{}
	}
	l := h.names[name]
	if l == nil {
		l = &nameLock{}
		h.names[name] = l
	}
	l.refs++
	h.mu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		h.mu.Lock()
		if l.refs--; l.refs == 0 {
			delete(h.names, name)
		}
		h.mu.Unlock()
	}
}

// record reads and checks the record of a request.
func (h *handler) record(w http.ResponseWriter, r *http.Request) (Record, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	var body RecordRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		wire.WriteError(w, http.StatusBadRequest, wire.ReasonInvalid, "invalid body: "+err.Error())
		return Record{}, false
	}
	rec, err := CheckRecord(h.z.Name, Record{Name: Normalize(r.PathValue("name")), Value: body.Value, TTL: body.TTL})
	if err != nil {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		return Record{}, false
	}
	return rec, true
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	rec, ok := h.record(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	unlock := h.lock(rec.Name)
	defer unlock()

	if err := h.z.Provider.Present(ctx, rec); err != nil {
		h.fail(w, "present", rec, err)
		return
	}
	h.log.Info("presented", "zone", h.z.Name, "name", rec.Name, "ttl", rec.TTL)
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	rec, ok := h.record(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	unlock := h.lock(rec.Name)
	defer unlock()

	if err := h.z.Provider.CleanUp(ctx, rec); err != nil {
		h.fail(w, "clean up", rec, err)
		return
	}
	h.log.Info("cleaned up", "zone", h.z.Name, "name", rec.Name)
	w.WriteHeader(http.StatusNoContent)
}

// fail answers the failure of a provider. The value of a record is the
// proof of a challenge: it is not logged.
func (h *handler) fail(w http.ResponseWriter, what string, rec Record, err error) {
	h.log.Warn(what+" failed", "zone", h.z.Name, "name", rec.Name, "error", err)
	if errors.Is(err, ErrInvalid) {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		return
	}
	wire.WriteError(w, http.StatusBadGateway, wire.ReasonError, err.Error())
}
