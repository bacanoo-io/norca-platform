// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package mock is a node provider in memory, for trials and tests: it keeps
// the instances it is asked for and creates no machine. An instance is
// running as soon as it is created.
package mock

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
)

// GroupConfig is a group of the mock provider.
type GroupConfig struct {
	// Capacity is the size of one machine.
	Capacity provider.Capacity

	// Max is the most instances the group has at a time; 0 is no limit. A
	// request beyond it fails with ReasonQuota.
	Max int
}

// Provider is the mock provider. Its zero value has no group.
type Provider struct {
	mu       sync.Mutex
	groups   map[string]GroupConfig
	inst     map[string]map[string]provider.Instance // by group and ID
	requests map[string][]string                     // instance IDs by request ID
	next     int

	// Now is replaced in tests.
	Now func() time.Time
}

// New returns a mock provider with the groups.
func New(groups map[string]GroupConfig) *Provider {
	p := &Provider{
		groups:   maps.Clone(groups),
		inst:     map[string]map[string]provider.Instance{},
		requests: map[string][]string{},
	}
	for name := range groups {
		p.inst[name] = map[string]provider.Instance{}
	}
	return p
}

func (p *Provider) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now().UTC()
}

// Group implements provider.Provider. The parameters are not used: every
// machine of a group has the size of the group.
func (p *Provider) Group(_ context.Context, group string, _ map[string]any) (*provider.Group, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg, ok := p.groups[group]
	if !ok {
		return nil, provider.ErrUnknownGroup
	}
	capacity := cfg.Capacity
	g := &provider.Group{Name: group, Capacity: &capacity, Instances: []provider.Instance{}}
	for _, id := range slices.Sorted(maps.Keys(p.inst[group])) {
		g.Instances = append(g.Instances, p.inst[group][id])
	}
	return g, nil
}

// Create implements provider.Provider.
func (p *Provider) Create(_ context.Context, group string, req provider.CreateRequest) ([]provider.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cfg, ok := p.groups[group]
	if !ok {
		return nil, provider.ErrUnknownGroup
	}
	if ids, ok := p.requests[req.RequestID]; ok {
		out := make([]provider.Instance, 0, len(ids))
		for _, id := range ids {
			if inst, ok := p.inst[group][id]; ok {
				out = append(out, inst)
			}
		}
		return out, nil
	}
	if cfg.Max > 0 && len(p.inst[group])+req.Count > cfg.Max {
		return nil, provider.Fail(provider.ReasonQuota, "group %s has %d of %d instances, %d more asked for",
			group, len(p.inst[group]), cfg.Max, req.Count)
	}
	out := make([]provider.Instance, 0, req.Count)
	for range req.Count {
		p.next++
		inst := provider.Instance{
			ID:        fmt.Sprintf("mock-%s-%d", group, p.next),
			State:     provider.StateRunning,
			RequestID: req.RequestID,
			Labels:    maps.Clone(req.Labels),
			CreatedAt: p.now(),
		}
		p.inst[group][inst.ID] = inst
		p.requests[req.RequestID] = append(p.requests[req.RequestID], inst.ID)
		out = append(out, inst)
	}
	return out, nil
}

// Delete implements provider.Provider.
func (p *Provider) Delete(_ context.Context, group, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.groups[group]; !ok {
		return provider.ErrUnknownGroup
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
	inst.State, inst.Reason, inst.Message = provider.StateFailed, reason, message
	p.inst[group][id] = inst
	return true
}
