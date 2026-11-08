// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package mock is the platform "mock": it keeps everything in memory and
// creates nothing, for trying the operators and the provider contracts
// without a platform, and as a model of how a platform is written. It has a
// node provider and a load balancer provider.
//
// The node provider keeps the instances it is asked for. An instance is
// running as soon as it is created, with the addresses of the n-th instance
// the mock made: 192.0.2.n and 2001:db8::n (n in hex), from the ranges for
// documentation. Every group has the capacity of its parameters (cpu,
// memory, disk) and refuses instances beyond its max.
//
// The load balancer provider keeps the load balancers it is asked for and
// reports the address of its class as their frontend.
package mock

import (
	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/node"
)

// TypeID is the ID of the type.
const TypeID = "mock"

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "in memory, for trials and tests",
		Node: &provider.NodeSpec{
			GroupParams: provider.CapacityParams,
			New: func(cfg provider.NodeConfig) (node.Provider, error) {
				groups := map[string]GroupConfig{}
				for _, g := range cfg.Groups {
					c, err := provider.GroupCapacity(g)
					if err != nil {
						return nil, err
					}
					groups[g.Name] = GroupConfig{Capacity: c, Max: g.Max}
				}
				return New(groups), nil
			},
		},
		LB: &provider.LBSpec{
			Params: classParams,
			New:    NewLB,
		},
	})
}
