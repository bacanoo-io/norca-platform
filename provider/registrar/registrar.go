// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package registrar writes the objects a provider registers itself with and
// keeps their heartbeat: a NodeProvider object for a node provider (for the
// node scaling operator) and a LoadBalancerClass object for every load
// balancer class (for the LoadBalancer operator). Both say where the
// operator calls the provider and with which token, and carry the same
// status: the heartbeat (RFC 3339) and the version of the provider.
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

// The object kinds of the registrations.
const (
	Group = "norca.io"

	// KindNodeProvider is registered by the core.
	KindNodeProvider = "NodeProvider"

	// KindLoadBalancerClass is registered with the LoadBalancer operator
	// (loadbalancerclass.kind.hcl).
	KindLoadBalancerClass = "LoadBalancerClass"

	// ManagedBy marks the objects this program owns.
	ManagedBy = "norca-provider"
)

// ErrNotFound is returned by a Store when the object does not exist.
var ErrNotFound = errors.New("object not found")

// Store is the part of the Norca API the registrar needs.
type Store interface {
	// Apply creates or updates the object (spec and metadata).
	Apply(ctx context.Context, obj *api.CustomObject) error

	// UpdateStatus replaces the status of the object; it returns ErrNotFound
	// if the object does not exist.
	UpdateStatus(ctx context.Context, kind, name string, status map[string]any) error

	// Delete removes the object. Removing a missing object is not an error.
	Delete(ctx context.Context, kind, name string) error
}

// Registration is one object: its kind (KindNodeProvider or
// KindLoadBalancerClass), its name and its spec.
type Registration struct {
	Kind string
	Name string
	Spec map[string]any
}

// Object returns the object of the registration.
func (r Registration) Object() *api.CustomObject {
	return &api.CustomObject{
		Group:     Group,
		Kind:      r.Kind,
		Name:      r.Name,
		ManagedBy: ManagedBy,
		Spec:      r.Spec,
	}
}

func (r Registration) String() string { return r.Kind + "/" + r.Name }

// GroupSpec is a group (a zone) a node provider offers.
type GroupSpec struct {
	Name string

	// Max is the most instances of the group; 0 is no limit.
	Max int

	// Parameters are the parameters of the group (its machine kinds, for
	// example).
	Parameters map[string]any
}

// NodeProvider returns the registration of a node provider: name is the
// name of the object, which node groups name in provider; endpoint the base
// URL the operator calls; tokenVariable the path of the Variable with the
// token the operator sends (empty: no token); parameters the published
// parameters of the provider.
func NodeProvider(name, endpoint, tokenVariable string, groups []GroupSpec, parameters map[string]any) Registration {
	spec := map[string]any{"endpoint": endpoint}
	if tokenVariable != "" {
		spec["token_variable"] = tokenVariable
	}
	if len(groups) > 0 {
		docs := make([]any, 0, len(groups))
		for _, g := range groups {
			doc := map[string]any{"name": g.Name}
			if g.Max > 0 {
				doc["max"] = g.Max
			}
			if len(g.Parameters) > 0 {
				doc["parameters"] = g.Parameters
			}
			docs = append(docs, doc)
		}
		spec["groups"] = docs
	}
	if len(parameters) > 0 {
		spec["parameters"] = parameters
	}
	return Registration{Kind: KindNodeProvider, Name: name, Spec: spec}
}

// LoadBalancerClass returns the registration of a load balancer class: the
// endpoint the operator calls for the class, the token Variable, what the
// class supports (internal load balancers, the frontend ports; empty ports
// is any) and the published parameters, which the operator sends back with
// every call.
func LoadBalancerClass(class, endpoint, tokenVariable string, internal bool, ports string, parameters map[string]any) Registration {
	caps := map[string]any{"internal": internal}
	if ports != "" {
		caps["ports"] = ports
	}
	spec := map[string]any{
		"endpoint":     endpoint,
		"capabilities": caps,
	}
	if tokenVariable != "" {
		spec["tokenVariable"] = tokenVariable
	}
	if len(parameters) > 0 {
		spec["parameters"] = parameters
	}
	return Registration{Kind: KindLoadBalancerClass, Name: class, Spec: spec}
}

// Registrar registers the objects of a provider and reports that it is
// alive.
type Registrar struct {
	Store         Store
	Registrations []Registration

	// Interval is the period of the heartbeat. It should be well below the
	// time after which an operator takes a provider for unavailable (the
	// -class-ttl of the LoadBalancer operator, three minutes by default).
	Interval time.Duration

	// Version is reported in the status.
	Version string

	// DeregisterOnExit removes the registrations when Run returns, so an
	// operator reports an unknown provider or class at once instead of
	// waiting for the heartbeat to expire.
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

// Run registers the objects (retrying each until it succeeds or ctx ends)
// and then reports the heartbeat until ctx ends.
func (r *Registrar) Run(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	retry := r.Retry
	if retry <= 0 {
		retry = 5 * time.Second
	}
	for _, reg := range r.Registrations {
		for {
			err := r.register(ctx, reg)
			if err == nil {
				break
			}
			r.logger().Warn("registering failed, retrying", "object", reg.String(), "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(retry):
			}
		}
		r.logger().Info("registered", "object", reg.String(), "endpoint", reg.Spec["endpoint"])
	}

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
			for _, reg := range r.Registrations {
				if err := r.heartbeat(ctx, reg); err != nil {
					r.logger().Warn("heartbeat failed", "object", reg.String(), "error", err)
				}
			}
		}
	}
}

// status is the status of every object: the heartbeat (RFC 3339) and the
// version.
func (r *Registrar) status() map[string]any {
	status := map[string]any{"heartbeat": r.now().UTC().Format(time.RFC3339)}
	if r.Version != "" {
		status["version"] = r.Version
	}
	return status
}

func (r *Registrar) register(ctx context.Context, reg Registration) error {
	if err := r.Store.Apply(ctx, reg.Object()); err != nil {
		return err
	}
	return r.Store.UpdateStatus(ctx, reg.Kind, reg.Name, r.status())
}

// heartbeat updates the status; if the object was removed meanwhile, it is
// registered again.
func (r *Registrar) heartbeat(ctx context.Context, reg Registration) error {
	err := r.Store.UpdateStatus(ctx, reg.Kind, reg.Name, r.status())
	if errors.Is(err, ErrNotFound) {
		r.logger().Warn("the registration disappeared, registering again", "object", reg.String())
		return r.register(ctx, reg)
	}
	return err
}

func (r *Registrar) deregister() {
	// ctx is done at this point; use a fresh one.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, reg := range r.Registrations {
		if err := r.Store.Delete(ctx, reg.Kind, reg.Name); err != nil {
			r.logger().Warn("removing the registration failed", "object", reg.String(), "error", err)
		}
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
func (s *APIStore) UpdateStatus(ctx context.Context, kind, name string, status map[string]any) error {
	w := (&api.WriteOptions{}).WithContext(ctx)
	_, _, err := s.Client.Objects().UpdateStatus(Group, kind, name, status, nil, w)
	return wrap(err)
}

// Delete implements Store.
func (s *APIStore) Delete(ctx context.Context, kind, name string) error {
	w := (&api.WriteOptions{}).WithContext(ctx)
	_, _, err := s.Client.Objects().Delete(Group, kind, name, nil, w)
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
