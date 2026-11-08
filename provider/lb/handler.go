// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package lb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/wire"
)

// DefaultTimeout bounds one call of a provider.
const DefaultTimeout = 2 * time.Minute

// validName matches the namespace and name of an object; it keeps them safe
// to use in file names and cloud resource names.
var validName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// NewHandler serves one class over the HTTP contract, at the paths below its
// endpoint (/v1/loadbalancers/...). With a token, every call must send it as
// a bearer token; an empty token disables the check (no_auth, a trusted
// network).
func NewHandler(c *Class, token string, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	h := &handler{c: c, log: log, timeout: DefaultTimeout}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/loadbalancers/{namespace}/{name}", h.put)
	mux.HandleFunc("DELETE /v1/loadbalancers/{namespace}/{name}", h.delete)
	return wire.Bearer(token, mux)
}

type handler struct {
	c       *Class
	log     *slog.Logger
	locks   keyedLocks
	timeout time.Duration
}

func (h *handler) ref(w http.ResponseWriter, r *http.Request) (Ref, bool) {
	ref := Ref{Namespace: r.PathValue("namespace"), Name: r.PathValue("name")}
	if !validName.MatchString(ref.Namespace) || !validName.MatchString(ref.Name) ||
		strings.Contains(ref.Namespace, "..") || strings.Contains(ref.Name, "..") {
		wire.WriteError(w, http.StatusBadRequest, wire.ReasonInvalid, "invalid namespace or name")
		return Ref{}, false
	}
	return ref, true
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	ref, ok := h.ref(w, r)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var body EnsureRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		wire.WriteError(w, http.StatusBadRequest, wire.ReasonInvalid, "invalid body: "+err.Error())
		return
	}
	if (body.Name != "" && body.Name != ref.Name) || (body.Namespace != "" && body.Namespace != ref.Namespace) {
		wire.WriteError(w, http.StatusBadRequest, wire.ReasonInvalid, "name and namespace of the body differ from the path")
		return
	}
	req, err := h.request(ref, body)
	if err != nil {
		wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	unlock := h.locks.lock(ref.String())
	defer unlock()

	state, err := h.c.Provider.Ensure(ctx, req)
	if err != nil {
		h.log.Warn("ensure failed", "class", h.c.Name, "loadbalancer", ref.String(), "error", err)
		if errors.Is(err, ErrInvalid) {
			wire.WriteError(w, http.StatusUnprocessableEntity, wire.ReasonInvalid, err.Error())
		} else {
			wire.WriteError(w, http.StatusBadGateway, wire.ReasonError, err.Error())
		}
		return
	}
	// host is the address the operator used (the registered endpoint), local
	// the address the request arrived on; they differ behind a port mapping
	// or a proxy.
	h.log.Info("ensured", "class", h.c.Name, "loadbalancer", ref.String(),
		"targets", len(req.Targets), "ready", state.Ready, "host", r.Host, "local", localAddr(r))
	addrs := state.Addresses
	if addrs == nil {
		addrs = []string{}
	}
	wire.WriteJSON(w, http.StatusOK, EnsureResponse{
		Ready: state.Ready, Addresses: addrs, Resource: state.Resource, Message: state.Message,
	})
}

// request validates the body against the capabilities of the class and
// applies the dynamic parameters.
func (h *handler) request(ref Ref, body EnsureRequest) (Request, error) {
	if body.Port < 1 || body.Port > 65535 {
		return Request{}, fmt.Errorf("port %d is out of range", body.Port)
	}
	if body.Internal && !h.c.Caps.Internal {
		return Request{}, fmt.Errorf("class %q cannot create internal load balancers", h.c.Name)
	}
	if ranges, _ := ParsePorts(h.c.Caps.Ports); !PortAllowed(ranges, body.Port) {
		return Request{}, fmt.Errorf("class %q does not support port %d (supported: %s)", h.c.Name, body.Port, h.c.Caps.Ports)
	}
	targets := make([]Target, len(body.Targets))
	for i, t := range body.Targets {
		if t.Address == "" || t.Port < 1 || t.Port > 65535 {
			return Request{}, fmt.Errorf("invalid target %d: %q port %d", i, t.Address, t.Port)
		}
		targets[i] = Target{Address: t.Address, Port: t.Port, AllocID: t.AllocID}
	}
	values, err := h.c.Effective(body.Parameters)
	if err != nil {
		return Request{}, err
	}
	return Request{
		Ref: ref, Service: body.Service, Port: body.Port, Internal: body.Internal,
		Targets: targets, Params: values,
	}, nil
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	ref, ok := h.ref(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	unlock := h.locks.lock(ref.String())
	defer unlock()

	if err := h.c.Provider.Delete(ctx, ref); err != nil {
		h.log.Warn("delete failed", "class", h.c.Name, "loadbalancer", ref.String(), "error", err)
		wire.WriteError(w, http.StatusBadGateway, wire.ReasonError, err.Error())
		return
	}
	h.log.Info("deleted", "class", h.c.Name, "loadbalancer", ref.String(), "host", r.Host, "local", localAddr(r))
	w.WriteHeader(http.StatusNoContent)
}

// localAddr returns the local address the connection of the request arrived
// on.
func localAddr(r *http.Request) string {
	if a, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
		return a.String()
	}
	return ""
}

// keyedLocks serializes the calls for one load balancer.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

func (k *keyedLocks) lock(key string) (unlock func()) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*keyedLock{}
	}
	l := k.locks[key]
	if l == nil {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.refs++
	k.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		if l.refs--; l.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
