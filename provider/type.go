// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package provider is the framework of norca-provider, the application that
// connects Norca to the platforms it runs on. One platform is one type: it
// creates the machines of node groups for the node scaling operator (the
// node contract, package node), the load balancers of load balancer classes
// for the LoadBalancer operator (the load balancer contract, package lb), or
// both, with the same credentials.
//
// A platform lives in its own package under providers/, declares a Type and
// registers it in init(), like a database/sql driver:
//
//	func init() {
//		provider.Register(provider.Type{
//			ID: "hcloud", Description: "Hetzner Cloud", Params: shared,
//			LB: &provider.LBSpec{Params: classParams, New: newLB},
//		})
//	}
//
// The binary links the platforms it wants with blank imports (see
// providers/all). A configuration file (File) then names any number of
// providers, each an instance of a type with a node section, a load
// balancer section or both; Build checks it against the types and creates
// the instances; Server serves all of them on one HTTP server; and the
// registrar (package registrar) writes their NodeProvider and
// LoadBalancerClass objects and keeps the heartbeat.
package provider

import (
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// Type is a platform: its ID, the parameters its node and load balancer
// providers share (the credentials and the API of the platform), and what
// it implements. A section the type does not implement is nil.
type Type struct {
	// ID is the identifier of the type, such as "hcloud" or "mock".
	ID string

	Description string

	// Params are the parameters shared by the node and the load balancer
	// provider of the type ("params" of a provider in the configuration).
	// They cannot be dynamic.
	Params []params.Param

	// Node is the node provider of the type; nil if it has none.
	Node *NodeSpec

	// LB is the load balancer provider of the type; nil if it has none.
	LB *LBSpec
}

// NodeSpec is the node provider of a type.
type NodeSpec struct {
	// Params are the parameters of the node section ("node.params").
	Params []params.Param

	// GroupParams are the parameters of a group ("node.groups[].params").
	// The ones that are not secret are published in the NodeProvider object.
	GroupParams []params.Param

	// New creates the node provider of a configured provider.
	New func(NodeConfig) (node.Provider, error)
}

// NodeConfig is the input of the constructor of a node provider.
type NodeConfig struct {
	// Name is the name of the provider (its NodeProvider object).
	Name string

	// Values are the resolved shared and node section parameters.
	Values params.Values

	// Groups are the configured groups.
	Groups []NodeGroup

	// Endpoint is the URL the operator calls the provider on.
	Endpoint string

	Logger *slog.Logger
}

// Log returns the configured logger, or slog.Default() if none was set.
func (c NodeConfig) Log() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// NodeGroup is a configured group of a node provider.
type NodeGroup struct {
	Name string

	// Max is the most instances of the group; 0 is no limit.
	Max int

	// Values are the resolved group parameters.
	Values params.Values
}

// CapacityParams are the group parameters of a node provider whose
// machines have the size its configuration gives them (the mock and the
// docker provider): CPU in MHz, memory and disk in MB.
var CapacityParams = []params.Param{
	{Name: "cpu", Type: params.ParamInt, Required: true, Description: "CPU of a machine of the group, in MHz"},
	{Name: "memory", Type: params.ParamInt, Required: true, Description: "memory of a machine of the group, in MB"},
	{Name: "disk", Type: params.ParamInt, Default: 0, Description: "disk of a machine of the group, in MB"},
}

// GroupCapacity reads the capacity of a group from its CapacityParams.
func GroupCapacity(g NodeGroup) (node.Capacity, error) {
	c := node.Capacity{CPU: int64(g.Values.Int("cpu")), Memory: int64(g.Values.Int("memory")), Disk: int64(g.Values.Int("disk"))}
	if c.CPU <= 0 || c.Memory <= 0 || c.Disk < 0 {
		return c, fmt.Errorf("group %q: cpu and memory must be positive, disk must not be negative", g.Name)
	}
	return c, nil
}

// LBSpec is the load balancer provider of a type.
type LBSpec struct {
	// Params are the parameters of a class
	// ("loadbalancer.classes[].params"). Dynamic ones can be changed in the
	// LoadBalancerClass object while the provider runs; the ones that are
	// not secret are published there.
	Params []params.Param

	// New creates the provider of a class. lb.Config.Values hold the shared
	// and the class parameters.
	New func(lb.Config) (lb.Provider, error)
}

// Capabilities returns what the type implements: "node", "loadbalancer".
func (t Type) Capabilities() []string {
	var out []string
	if t.Node != nil {
		out = append(out, "node")
	}
	if t.LB != nil {
		out = append(out, "loadbalancer")
	}
	return out
}

var (
	validTypeID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	validName   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
)

// ValidName reports whether name can be the name of a provider or a group.
func ValidName(name string) bool { return validName.MatchString(name) }

// Registry holds the known types. Platforms register in Default from
// init(); tests use their own.
type Registry struct {
	mu    sync.RWMutex
	types map[string]Type
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{types: map[string]Type{}} }

// Default is the registry the platforms register in.
var Default = NewRegistry()

// Register adds a type to the Default registry. Platforms call it from
// init().
func Register(t Type) { Default.Register(t) }

// Register adds a type. It panics on a programming error: an invalid or
// duplicate ID, a type that implements nothing, a section without a
// constructor, or invalid parameter declarations.
func (r *Registry) Register(t Type) {
	if err := check(t); err != nil {
		panic(fmt.Sprintf("provider: type %q: %v", t.ID, err))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.types[t.ID]; dup {
		panic(fmt.Sprintf("provider: type %q registered twice", t.ID))
	}
	r.types[t.ID] = t
}

func check(t Type) error {
	if !validTypeID.MatchString(t.ID) {
		return fmt.Errorf("invalid type ID")
	}
	if t.Node == nil && t.LB == nil {
		return fmt.Errorf("implements neither a node nor a load balancer provider")
	}
	if err := params.Validate(t.Params); err != nil {
		return fmt.Errorf("params: %w", err)
	}
	fixed := func(section string, ps []params.Param) error {
		for _, p := range ps {
			if p.Dynamic {
				return fmt.Errorf("%s parameter %q: only class parameters can be dynamic", section, p.Name)
			}
		}
		return nil
	}
	if err := fixed("shared", t.Params); err != nil {
		return err
	}
	// The parameters of a section are handed to the constructor together
	// with the shared ones: the names must differ.
	distinct := func(section string, ps []params.Param) error {
		if err := params.Validate(ps); err != nil {
			return fmt.Errorf("%s params: %w", section, err)
		}
		for _, p := range ps {
			for _, s := range t.Params {
				if s.Name == p.Name {
					return fmt.Errorf("%s parameter %q is also a shared parameter", section, p.Name)
				}
			}
		}
		return nil
	}
	if n := t.Node; n != nil {
		if n.New == nil {
			return fmt.Errorf("the node provider has no constructor")
		}
		if err := distinct("node", n.Params); err != nil {
			return err
		}
		if err := params.Validate(n.GroupParams); err != nil {
			return fmt.Errorf("group params: %w", err)
		}
		if err := fixed("node", n.Params); err != nil {
			return err
		}
		if err := fixed("group", n.GroupParams); err != nil {
			return err
		}
	}
	if l := t.LB; l != nil {
		if l.New == nil {
			return fmt.Errorf("the load balancer provider has no constructor")
		}
		if err := distinct("class", l.Params); err != nil {
			return err
		}
	}
	return nil
}

// Lookup returns a type by ID.
func (r *Registry) Lookup(id string) (Type, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.types[id]
	return t, ok
}

// Types returns the registered types sorted by ID.
func (r *Registry) Types() []Type {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Type, 0, len(r.types))
	for _, t := range r.types {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Registry) lookup(id string) (Type, error) {
	t, ok := r.Lookup(id)
	if !ok {
		return Type{}, fmt.Errorf("unknown type %q (linked types: %s)", id, r.names())
	}
	return t, nil
}

func (r *Registry) names() string {
	types := r.Types()
	if len(types) == 0 {
		return "none"
	}
	ids := make([]string, len(types))
	for i, t := range types {
		ids[i] = t.ID
	}
	return strings.Join(ids, ", ")
}

// NewClass creates the load balancer class of a type, from the shared and
// the class parameters as configured; the capabilities are those of the
// provider. The configuration goes through Build; NewClass is for tests and
// programs that serve a class of their own.
func (r *Registry) NewClass(class, typeID string, shared, classParams map[string]any) (*lb.Class, error) {
	t, err := r.lookup(typeID)
	if err != nil {
		return nil, fmt.Errorf("class %q: %w", class, err)
	}
	return newClass("", t, class, shared, classParams, slog.Default())
}

// Instantiate is NewClass with the shared and the class parameters in one
// map, told apart by their declarations: for tests.
func (r *Registry) Instantiate(class, typeID string, raw map[string]any) (*lb.Class, error) {
	t, err := r.lookup(typeID)
	if err != nil {
		return nil, fmt.Errorf("class %q: %w", class, err)
	}
	shared := map[string]any{}
	rest := map[string]any{}
	for k, v := range raw {
		if hasParam(t.Params, k) {
			shared[k] = v
		} else {
			rest[k] = v
		}
	}
	return newClass("", t, class, shared, rest, slog.Default())
}

func hasParam(ps []params.Param, name string) bool {
	for _, p := range ps {
		if p.Name == name {
			return true
		}
	}
	return false
}

// newClass resolves the parameters and runs the constructor of the class.
func newClass(providerName string, t Type, class string, shared, classParams map[string]any, log *slog.Logger) (*lb.Class, error) {
	if !lb.ValidClass(class) {
		return nil, fmt.Errorf("invalid class name %q", class)
	}
	if t.LB == nil {
		return nil, fmt.Errorf("class %q: type %s has no load balancer provider", class, t.ID)
	}
	sv, err := params.Resolve(t.Params, shared)
	if err != nil {
		return nil, fmt.Errorf("class %q (type %s): %w", class, t.ID, err)
	}
	cv, err := params.Resolve(t.LB.Params, classParams)
	if err != nil {
		return nil, fmt.Errorf("class %q (type %s): %w", class, t.ID, err)
	}
	values := params.Merge(sv, cv)
	p, err := t.LB.New(lb.Config{Provider: providerName, Class: class, Values: values, Logger: log.With("class", class)})
	if err != nil {
		return nil, fmt.Errorf("class %q (type %s): %w", class, t.ID, err)
	}
	published := params.Published(t.Params, sv)
	for k, v := range params.Published(t.LB.Params, cv) {
		published[k] = v
	}
	return &lb.Class{
		Name: class, Type: t.ID, Provider: p, Params: t.LB.Params, Values: values,
		Published: published, Caps: p.Capabilities(),
	}, nil
}
