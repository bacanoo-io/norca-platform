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
	"slices"
	"strings"
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
	if rp, ok := z.Provider.(RecordProvider); ok && z.Records {
		h.records = rp
		mux.HandleFunc("GET /v1/records/{name}", h.getRecords)
		mux.HandleFunc("PUT /v1/records/{name}/{type}", h.putRecords)
		mux.HandleFunc("DELETE /v1/records/{name}/{type}", h.deleteRecords)
	} else {
		mux.HandleFunc("/v1/records/", func(w http.ResponseWriter, _ *http.Request) {
			wire.WriteError(w, http.StatusForbidden, wire.ReasonInvalid,
				"the zone "+z.Name+" does not write records: set records = true in its configuration (a type that can write them)")
		})
	}
	return wire.Bearer(token, mux)
}

type handler struct {
	z       *Zone
	log     *slog.Logger
	timeout time.Duration
	records RecordProvider

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

func (h *handler) getRecords(w http.ResponseWriter, r *http.Request) {
	name := Normalize(r.PathValue("name"))
	if err := CheckRecordName(h.z.Name, name); err != nil {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	sets, err := h.records.Records(ctx, name)
	if err != nil {
		h.failRecords(w, "read", name, err)
		return
	}
	if sets == nil {
		sets = []RecordSet{}
	}
	wire.WriteJSON(w, http.StatusOK, sets)
}

func (h *handler) putRecords(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	var body RecordSetRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		wire.WriteError(w, http.StatusBadRequest, wire.ReasonInvalid, "invalid body: "+err.Error())
		return
	}
	set, err := CheckRecordSet(h.z.Name, RecordSet{Name: r.PathValue("name"), Type: r.PathValue("type"), TTL: body.TTL, Values: body.Values})
	if err != nil {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	unlock := h.lock(set.Name)
	defer unlock()
	if err := h.records.SetRecords(ctx, set); err != nil {
		h.failRecords(w, "set", set.Name, err)
		return
	}
	h.log.Info("records set", "zone", h.z.Name, "name", set.Name, "type", set.Type, "values", len(set.Values), "ttl", set.TTL)
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) deleteRecords(w http.ResponseWriter, r *http.Request) {
	name := Normalize(r.PathValue("name"))
	recordType := strings.ToUpper(r.PathValue("type"))
	if err := CheckRecordName(h.z.Name, name); err != nil {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		return
	}
	if !slices.Contains(RecordTypes, recordType) {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, "unknown record type "+recordType)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	unlock := h.lock(name)
	defer unlock()
	if err := h.records.DeleteRecords(ctx, name, recordType); err != nil {
		h.failRecords(w, "delete", name, err)
		return
	}
	h.log.Info("records deleted", "zone", h.z.Name, "name", name, "type", recordType)
	w.WriteHeader(http.StatusNoContent)
}

// failRecords answers the failure of a provider for the records contract.
func (h *handler) failRecords(w http.ResponseWriter, what, name string, err error) {
	h.log.Warn("records "+what+" failed", "zone", h.z.Name, "name", name, "error", err)
	if errors.Is(err, ErrInvalid) {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		return
	}
	wire.WriteError(w, http.StatusBadGateway, wire.ReasonError, err.Error())
}
