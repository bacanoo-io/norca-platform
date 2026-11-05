// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"sync"

	"github.com/bacanoo-io/norca/norca/api"
)

// Node is a node of the cluster as the operator sees it.
type Node struct {
	ID   string
	Name string

	// Instance is the name of the instance the node runs on (the meta key
	// norca.instance), and Group its node group (norca.node_group); both
	// empty for a node the operator did not create.
	Instance string
	Group    string

	// Ready is a ready node, Eligible one that takes new work.
	Ready    bool
	Eligible bool

	// CreateIndex tells which of two nodes registered first.
	CreateIndex uint64
}

// NodeLister lists the nodes of a datacenter, pool and class, and makes a
// node ineligible for new work.
type NodeLister interface {
	Nodes(ctx context.Context, datacenter, pool, class string) ([]Node, error)
	SetIneligible(ctx context.Context, nodeID string) error
}

// APINodes lists the nodes with the Norca API. The node list has no meta, so
// a node is read once for its marks, which do not change; they are cached by
// node.
type APINodes struct {
	Client *api.Client

	mu    sync.Mutex
	marks map[string][2]string // instance and node group, by node ID
}

// Nodes implements NodeLister.
func (n *APINodes) Nodes(ctx context.Context, datacenter, pool, class string) ([]Node, error) {
	q := (&api.QueryOptions{}).WithContext(ctx)
	stubs, _, err := n.Client.Nodes().List(q)
	if err != nil {
		return nil, err
	}
	var out []Node
	for _, s := range stubs {
		if s.Datacenter != datacenter || s.NodePool != pool || (class != "" && s.NodeClass != class) {
			continue
		}
		marks, err := n.marksOf(ctx, s.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, Node{
			ID: s.ID, Name: s.Name, Instance: marks[0], Group: marks[1],
			Ready:       s.Status == api.NodeStatusReady,
			Eligible:    s.SchedulingEligibility == api.NodeSchedulingEligible,
			CreateIndex: s.CreateIndex,
		})
	}
	return out, nil
}

// SetIneligible implements NodeLister.
func (n *APINodes) SetIneligible(ctx context.Context, nodeID string) error {
	_, err := n.Client.Nodes().ToggleEligibility(nodeID, false, (&api.WriteOptions{}).WithContext(ctx))
	return err
}

func (n *APINodes) marksOf(ctx context.Context, nodeID string) ([2]string, error) {
	n.mu.Lock()
	m, ok := n.marks[nodeID]
	n.mu.Unlock()
	if ok {
		return m, nil
	}
	node, _, err := n.Client.Nodes().Info(nodeID, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return [2]string{}, err
	}
	m = [2]string{node.Meta[MetaInstance], node.Meta[MetaNodeGroup]}
	n.mu.Lock()
	if n.marks == nil {
		n.marks = map[string][2]string{}
	}
	n.marks[nodeID] = m
	n.mu.Unlock()
	return m, nil
}
