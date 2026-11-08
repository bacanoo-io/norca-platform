// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package mock

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

var classParams = []params.Param{
	{Name: "address", Type: params.ParamString, Default: "192.0.2.10",
		Description: "address reported as the frontend of every load balancer"},
	{Name: "ready", Type: params.ParamBool, Default: true, Dynamic: true,
		Description: "whether the load balancers report ready; change it in the LoadBalancerClass object to simulate an outage"},
	{Name: "latency", Type: params.ParamDuration, Default: time.Duration(0), Dynamic: true,
		Description: "time every call takes (simulates a slow cloud API)"},
}

// LoadBalancer is a load balancer of the mock.
type LoadBalancer struct {
	Ref      lb.Ref
	Port     int
	Internal bool
	Targets  []string
}

// LB is the load balancer provider of a mock class: it keeps the load
// balancers in memory. It can create internal load balancers.
type LB struct {
	class string
	caps  lb.Capabilities
	addr  string

	mu  sync.Mutex
	lbs map[lb.Ref]LoadBalancer
}

// NewLB creates the provider of a class.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	if ip := cfg.Values.String("address"); net.ParseIP(ip) == nil {
		return nil, fmt.Errorf("address %q is not an IP address", ip)
	}
	return &LB{
		class: cfg.Class,
		addr:  cfg.Values.String("address"),
		caps:  lb.Capabilities{Internal: true},
		lbs:   map[lb.Ref]LoadBalancer{},
	}, nil
}

// Capabilities implements lb.Provider.
func (p *LB) Capabilities() lb.Capabilities { return p.caps }

// Ensure implements lb.Provider.
func (p *LB) Ensure(ctx context.Context, req lb.Request) (lb.State, error) {
	if d := req.Params.Duration("latency"); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return lb.State{}, ctx.Err()
		}
	}
	targets := make([]string, len(req.Targets))
	for i, t := range req.Targets {
		targets[i] = net.JoinHostPort(t.Address, strconv.Itoa(t.Port))
	}
	sort.Strings(targets)

	p.mu.Lock()
	p.lbs[req.Ref] = LoadBalancer{Ref: req.Ref, Port: req.Port, Internal: req.Internal, Targets: targets}
	p.mu.Unlock()

	state := lb.State{
		Addresses: []string{net.JoinHostPort(p.addr, strconv.Itoa(req.Port))},
		Resource:  fmt.Sprintf("mock/%s/%s/%s", p.class, req.Namespace, req.Name),
	}
	switch {
	case !req.Params.Bool("ready"):
		state.Message = "the mock is set to not ready"
	case len(targets) == 0:
		state.Message = "no targets"
	default:
		state.Ready = true
	}
	return state, nil
}

// Delete implements lb.Provider.
func (p *LB) Delete(_ context.Context, ref lb.Ref) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.lbs, ref)
	return nil
}

// Snapshot returns the load balancers, sorted by reference.
func (p *LB) Snapshot() []LoadBalancer {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]LoadBalancer, 0, len(p.lbs))
	for _, curLB := range p.lbs {
		out = append(out, curLB)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref.String() < out[j].Ref.String() })
	return out
}
