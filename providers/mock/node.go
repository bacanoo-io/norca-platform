// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package mock

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/node"
)

// GroupConfig is a group of the mock provider.
type GroupConfig struct {
	// Capacity is the size of one machine.
	Capacity node.Capacity

	// Max is the most instances the group has at a time; 0 is no limit. A
	// request beyond it fails with ReasonQuota.
	Max int
}

// Provider is the mock provider. Its zero value has no group.
type Provider struct {
	mu       sync.Mutex
	groups   map[string]GroupConfig
	inst     map[string]map[string]node.Instance // by group and ID
	requests map[string][]string                 // instance IDs by request ID
	userData map[string]string                   // the bootstrap of an instance, by ID
	next     int

	// Now is replaced in tests.
	Now func() time.Time
}

// New returns a mock provider with the groups.
func New(groups map[string]GroupConfig) *Provider {
	p := &Provider{
		groups:   maps.Clone(groups),
		inst:     map[string]map[string]node.Instance{},
		requests: map[string][]string{},
		userData: map[string]string{},
	}
	for name := range groups {
		p.inst[name] = map[string]node.Instance{}
	}
	return p
}

func (p *Provider) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now().UTC()
}

// Group implements node.Provider. The parameters are not used: every
// machine of a group has the size of the group.
func (p *Provider) Group(_ context.Context, group string, _ map[string]any) (*node.Group, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg, ok := p.groups[group]
	if !ok {
		return nil, node.ErrUnknownGroup
	}
	capacity := cfg.Capacity
	g := &node.Group{Name: group, Capacity: &capacity, Instances: []node.Instance{}}
	for _, id := range slices.Sorted(maps.Keys(p.inst[group])) {
		g.Instances = append(g.Instances, p.inst[group][id])
	}
	return g, nil
}

// Create implements node.Provider.
func (p *Provider) Create(_ context.Context, group string, req node.CreateRequest) ([]node.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg, ok := p.groups[group]
	if !ok {
		return nil, node.ErrUnknownGroup
	}
	if ids, ok := p.requests[req.RequestID]; ok {
		out := make([]node.Instance, 0, len(ids))
		for _, id := range ids {
			if inst, ok := p.inst[group][id]; ok {
				out = append(out, inst)
			}
		}
		return out, nil
	}
	if cfg.Max > 0 && len(p.inst[group])+len(req.Instances) > cfg.Max {
		return nil, node.Fail(node.ReasonQuota, "group %s has %d of %d instances, %d more asked for",
			group, len(p.inst[group]), cfg.Max, len(req.Instances))
	}
	out := make([]node.Instance, 0, len(req.Instances))
	for _, ir := range req.Instances {
		p.next++
		inst := node.Instance{
			ID:        fmt.Sprintf("mock-%s-%d", group, p.next),
			Name:      ir.Name,
			State:     node.StateRunning,
			RequestID: req.RequestID,
			Labels:    maps.Clone(req.Labels),
			Addresses: Addresses(p.next),
			CreatedAt: p.now(),
		}
		p.inst[group][inst.ID] = inst
		p.requests[req.RequestID] = append(p.requests[req.RequestID], inst.ID)
		if ir.Bootstrap != nil {
			p.userData[inst.ID] = ir.Bootstrap.UserData
		}
		out = append(out, inst)
	}
	return out, nil
}

// Addresses are the addresses of the n-th instance the mock makes.
func Addresses(n int) []string {
	return []string{fmt.Sprintf("192.0.2.%d", n%254+1), fmt.Sprintf("2001:db8::%x", n)}
}

// UserData returns the bootstrap an instance was created with, as a platform
// would run it.
func (p *Provider) UserData(id string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.userData[id]
}

// Delete implements node.Provider.
func (p *Provider) Delete(_ context.Context, group, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.groups[group]; !ok {
		return node.ErrUnknownGroup
	}
	delete(p.inst[group], id)
	return nil
}

// Fail marks an instance as failed with the reason, as a platform does with
// a machine that did not come up. It reports whether the instance exists.
func (p *Provider) Fail(group, id, reason, message string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	inst, ok := p.inst[group][id]
	if !ok {
		return false
	}
	inst.State, inst.Reason, inst.Message = node.StateFailed, reason, message
	p.inst[group][id] = inst
	return true
}
