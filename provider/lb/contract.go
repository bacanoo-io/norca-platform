// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package lb is the contract between the LoadBalancer operator
// (norca-loadbalancer) and a load balancer provider: an application outside
// the core that creates the load balancers of one class. The operator calls
// the provider over HTTP:
//
//	PUT    {endpoint}/v1/loadbalancers/{namespace}/{name}   create or update (idempotent)
//	DELETE {endpoint}/v1/loadbalancers/{namespace}/{name}   remove
//
// The endpoint is the one of the LoadBalancerClass object of the class;
// norca-provider serves every class at {advertise_url}/classes/{class}. A
// platform implements Provider; NewHandler serves one class and Client is
// the operator's side. Failures have the shape of package wire, and the
// operator sends a bearer token, as in the node contract.
package lb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/bacanoo-io/norca-platform/provider/params"
)

// Ref identifies a load balancer: the namespace and name of the LoadBalancer
// object of the operator.
type Ref struct {
	Namespace string
	Name      string
}

func (r Ref) String() string { return r.Namespace + "/" + r.Name }

// Target is a healthy instance of the service behind a load balancer.
type Target struct {
	Address string
	Port    int
	AllocID string
}

// Request is what a provider is asked to make true. Ensure is called with
// the complete desired state every time something changes.
type Request struct {
	Ref

	// Service is the name of the service the targets belong to.
	Service string

	// Port is the frontend port.
	Port int

	// Internal asks for a load balancer that is not reachable from the
	// internet. It is only set if the class has the capability.
	Internal bool

	// Targets are the healthy instances. The list may be empty.
	Targets []Target

	// Params are the effective parameters of the call: the configured ones
	// (those shared by the provider and those of the class), with the
	// dynamic ones overridden by the LoadBalancerClass object.
	Params params.Values
}

// State is what the provider reports back.
type State struct {
	// Ready is true when the load balancer serves traffic as requested.
	Ready bool

	// Addresses are the frontend addresses ("host:port").
	Addresses []string

	// Resource identifies what was created, for example the cloud ID.
	Resource string

	// Message explains a state that is not ready.
	Message string
}

// Capabilities are what a class supports. They are published in the
// LoadBalancerClass object and checked by the operator before a call, and
// by the handler.
type Capabilities struct {
	// Internal is true if the class can create load balancers that are not
	// reachable from the internet.
	Internal bool

	// Ports limits the frontend ports ("80,443,8000-8100"); empty means any.
	// A provider leaves it empty: the ports of a class are configured.
	Ports string
}

// Provider is an instance of a type: it creates the load balancers of one
// class. All methods must be safe for concurrent use; calls for the same
// load balancer are serialized by the handler.
type Provider interface {
	// Capabilities says whether the instance can create internal load
	// balancers.
	Capabilities() Capabilities

	// Ensure creates or updates the load balancer so that it forwards to
	// the targets. It must be idempotent. An error wrapping ErrInvalid
	// means the request cannot be satisfied (the operator does not retry it
	// with the same input); any other error is a failure to try again.
	Ensure(ctx context.Context, req Request) (State, error)

	// Delete removes everything created for the load balancer. Deleting
	// what does not exist is not an error.
	Delete(ctx context.Context, ref Ref) error
}

// Closer is implemented by providers that hold resources.
type Closer interface {
	Close() error
}

// ErrInvalid marks a request a provider cannot satisfy.
var ErrInvalid = errors.New("invalid request")

// Invalid returns an error wrapping ErrInvalid.
func Invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Config is the input of the constructor of a type.
type Config struct {
	// Provider is the name of the provider (the entry of the
	// configuration) the class belongs to.
	Provider string

	// Class is the class name.
	Class string

	// Values are the resolved parameters: the shared ones of the type and
	// those of the class.
	Values params.Values

	// Logger is the logger for the instance; may be nil.
	Logger *slog.Logger
}

// Log returns the configured logger, or slog.Default() if none was set.
func (c Config) Log() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// Class is a configured class: a provider instance with its parameters and
// the capabilities it offers.
type Class struct {
	// Name is the class name.
	Name string

	// Type is the ID of the type that serves the class.
	Type string

	Provider Provider

	// Params are the declarations of the class parameters, the ones the
	// LoadBalancerClass object may override when they are dynamic.
	Params []params.Param

	// Values are the parameters of the class as configured (shared and
	// class parameters).
	Values params.Values

	// Published are the parameters written to the LoadBalancerClass object.
	Published map[string]any

	// Caps are the capabilities the class offers: those of the provider,
	// narrowed by the configuration.
	Caps Capabilities
}

// Effective returns the parameters of one call: the configured values with
// the dynamic ones of the LoadBalancerClass object applied.
func (c *Class) Effective(raw map[string]any) (params.Values, error) {
	return params.Overlay(c.Params, c.Values, raw)
}

// Close releases the provider if it holds resources.
func (c *Class) Close() error {
	if cl, ok := c.Provider.(Closer); ok {
		return cl.Close()
	}
	return nil
}

var validClass = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)

// ValidClass reports whether name can be a class name.
func ValidClass(name string) bool { return validClass.MatchString(name) }

// ParsePorts parses a list of ports and port ranges, "80,443,8000-8100".
// Empty is any port (no range).
func ParsePorts(s string) ([][2]int, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out [][2]int
	for part := range strings.SplitSeq(s, ",") {
		lo, hi, isRange := strings.Cut(strings.TrimSpace(part), "-")
		from, err := strconv.Atoi(lo)
		if err != nil || from < 1 || from > 65535 {
			return nil, fmt.Errorf("invalid port %q", lo)
		}
		to := from
		if isRange {
			if to, err = strconv.Atoi(hi); err != nil || to < from || to > 65535 {
				return nil, fmt.Errorf("invalid port range %q", part)
			}
		}
		out = append(out, [2]int{from, to})
	}
	return out, nil
}

// PortAllowed reports whether port is in the ranges; no range allows any.
func PortAllowed(ranges [][2]int, port int) bool {
	return len(ranges) == 0 || slices.ContainsFunc(ranges, func(r [2]int) bool { return port >= r[0] && port <= r[1] })
}

// The bodies of the HTTP contract.
type (
	// EnsureRequest is the body of a PUT.
	EnsureRequest struct {
		Name       string         `json:"name"`
		Namespace  string         `json:"namespace"`
		Service    string         `json:"service"`
		Port       int            `json:"port"`
		Internal   bool           `json:"internal"`
		Targets    []WireTarget   `json:"targets"`
		Parameters map[string]any `json:"parameters,omitempty"`
	}

	// WireTarget is a target of an EnsureRequest.
	WireTarget struct {
		Address string `json:"address"`
		Port    int    `json:"port"`
		AllocID string `json:"allocID,omitempty"`
	}

	// EnsureResponse is the answer of a PUT.
	EnsureResponse struct {
		Ready     bool     `json:"ready"`
		Addresses []string `json:"addresses"`
		Resource  string   `json:"resource"`
		Message   string   `json:"message"`
	}
)
