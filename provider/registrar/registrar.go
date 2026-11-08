// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package registrar writes the NodeProvider object of a node provider and
// keeps its heartbeat, so the node scaling operator knows the provider
// exists, where to call it, which groups it offers, and that it is alive.
package registrar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
)

// The object kind of the registration, registered by the core.
const (
	Group = "norca.io"
	Kind  = "NodeProvider"

	// ManagedBy marks the objects this program owns.
	ManagedBy = "norca-nodeprovider"
)

// ErrNotFound is returned by a Store when the object does not exist.
var ErrNotFound = errors.New("object not found")

// Store is the part of the Norca API the registrar needs.
type Store interface {
	// Apply creates or updates the object (spec and metadata).
	Apply(ctx context.Context, obj *api.CustomObject) error

	// UpdateStatus replaces the status of the object; it returns ErrNotFound
	// if the object does not exist.
	UpdateStatus(ctx context.Context, name string, status map[string]any) error

	// Delete removes the object. Removing a missing object is not an error.
	Delete(ctx context.Context, name string) error
}

// GroupSpec is a group (a zone) the provider offers.
type GroupSpec struct {
	Name string

	// Max is the most instances of the group; 0 is no limit.
	Max int

	// Parameters are the parameters of the group (its machine kinds, for
	// example).
	Parameters map[string]any
}

// Registration is the NodeProvider object of a provider.
type Registration struct {
	// Name is the name of the object, which node groups name in provider.
	Name string

	// Endpoint is the base URL the operator calls.
	Endpoint string

	// TokenVariable is the path of the Variable with the token the operator
	// sends; empty if the provider takes no token.
	TokenVariable string

	Groups []GroupSpec

	// Parameters are the published parameters of the provider.
	Parameters map[string]any
}

// Object returns the NodeProvider object of the registration.
func (r Registration) Object() *api.CustomObject {
	spec := map[string]any{"endpoint": r.Endpoint}
	if r.TokenVariable != "" {
		spec["token_variable"] = r.TokenVariable
	}
	if len(r.Groups) > 0 {
		groups := make([]any, 0, len(r.Groups))
		for _, g := range r.Groups {
			doc := map[string]any{"name": g.Name}
			if g.Max > 0 {
				doc["max"] = g.Max
			}
			if len(g.Parameters) > 0 {
				doc["parameters"] = g.Parameters
			}
			groups = append(groups, doc)
		}
		spec["groups"] = groups
	}
	if len(r.Parameters) > 0 {
		spec["parameters"] = r.Parameters
	}
	return &api.CustomObject{
		Group:     Group,
		Kind:      Kind,
		Name:      r.Name,
		ManagedBy: ManagedBy,
		Spec:      spec,
	}
}

// Registrar registers a provider and reports that it is alive.
type Registrar struct {
	Store        Store
	Registration Registration

	// Interval is the period of the heartbeat. It should be well below the
	// time after which the operator takes a provider for unavailable.
	Interval time.Duration

	// Version is reported in the status.
	Version string

	// DeregisterOnExit removes the registration when Run returns, so the
	// operator reports an unknown provider at once instead of waiting for
	// the heartbeat to expire.
	DeregisterOnExit bool

	Logger *slog.Logger

	// Now and Retry are replaced in tests.
	Now   func() time.Time
	Retry time.Duration
}

func (r *Registrar) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (r *Registrar) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Run registers the provider (retrying until it succeeds or ctx ends) and
// then reports the heartbeat until ctx ends.
func (r *Registrar) Run(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	retry := r.Retry
	if retry <= 0 {
		retry = 5 * time.Second
	}
	reg := r.Registration
	for {
		err := r.register(ctx)
		if err == nil {
			break
		}
		r.logger().Warn("registering the provider failed, retrying", "provider", reg.Name, "error", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retry):
		}
	}
	r.logger().Info("provider registered", "provider", reg.Name, "endpoint", reg.Endpoint)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if r.DeregisterOnExit {
				r.deregister()
			}
			return nil
		case <-ticker.C:
			if err := r.heartbeat(ctx); err != nil {
				r.logger().Warn("heartbeat failed", "provider", reg.Name, "error", err)
			}
		}
	}
}

// status is the status of the object: the heartbeat (RFC 3339) and the
// version.
func (r *Registrar) status() map[string]any {
	status := map[string]any{"heartbeat": r.now().UTC().Format(time.RFC3339)}
	if r.Version != "" {
		status["version"] = r.Version
	}
	return status
}

func (r *Registrar) register(ctx context.Context) error {
	if err := r.Store.Apply(ctx, r.Registration.Object()); err != nil {
		return err
	}
	return r.Store.UpdateStatus(ctx, r.Registration.Name, r.status())
}

// heartbeat updates the status; if the object was removed meanwhile, it is
// registered again.
func (r *Registrar) heartbeat(ctx context.Context) error {
	err := r.Store.UpdateStatus(ctx, r.Registration.Name, r.status())
	if errors.Is(err, ErrNotFound) {
		r.logger().Warn("the registration disappeared, registering again", "provider", r.Registration.Name)
		return r.register(ctx)
	}
	return err
}

func (r *Registrar) deregister() {
	// ctx is done at this point; use a fresh one.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Store.Delete(ctx, r.Registration.Name); err != nil {
		r.logger().Warn("removing the registration failed", "provider", r.Registration.Name, "error", err)
	}
}

// APIStore is a Store on the Norca API.
type APIStore struct {
	Client *api.Client
}

// NewAPIStore returns a Store using client; with nil it uses the
// environment (NORCA_ADDR, NORCA_TOKEN, ...). A provider that runs as a Norca
// job reaches the API on the task API socket with its workload identity:
// NORCA_ADDR=unix://${NORCA_SECRETS_DIR}/api.sock and, with identity { env =
// true }, NORCA_TOKEN. The unix:// address is supported here.
func NewAPIStore(client *api.Client) (*APIStore, error) {
	if client == nil {
		conf := api.DefaultConfig()
		if err := configureUnixSocket(conf); err != nil {
			return nil, err
		}
		var err error
		if client, err = api.NewClient(conf); err != nil {
			return nil, err
		}
	}
	return &APIStore{Client: client}, nil
}

// configureUnixSocket makes the client dial the socket of a unix:// address.
func configureUnixSocket(conf *api.Config) error {
	socket, ok := strings.CutPrefix(conf.Address, "unix://")
	if !ok {
		return nil
	}
	if socket == "" {
		return errors.New("the unix socket address has no path")
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conf.Address = "http://127.0.0.1"
	conf.HttpClient = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "unix", socket)
			},
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return nil
}

// Apply implements Store.
func (s *APIStore) Apply(ctx context.Context, obj *api.CustomObject) error {
	w := (&api.WriteOptions{}).WithContext(ctx)
	_, _, err := s.Client.Objects().Apply(obj, nil, w)
	return wrap(err)
}

// UpdateStatus implements Store.
func (s *APIStore) UpdateStatus(ctx context.Context, name string, status map[string]any) error {
	w := (&api.WriteOptions{}).WithContext(ctx)
	_, _, err := s.Client.Objects().UpdateStatus(Group, Kind, name, status, nil, w)
	return wrap(err)
}

// Delete implements Store.
func (s *APIStore) Delete(ctx context.Context, name string) error {
	w := (&api.WriteOptions{}).WithContext(ctx)
	_, _, err := s.Client.Objects().Delete(Group, Kind, name, nil, w)
	if err = wrap(err); errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func wrap(err error) error {
	if err == nil {
		return nil
	}
	var respErr api.UnexpectedResponseError
	if errors.As(err, &respErr) && respErr.StatusCode() == http.StatusNotFound {
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	return err
}
