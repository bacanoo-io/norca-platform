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
	ID string

	// ProviderID is the ID of the instance the node runs on (the meta key
	// norca.provider_id); empty for a node the operator did not create.
	ProviderID string

	// Ready is a ready node.
	Ready bool
}

// NodeLister lists the nodes of a datacenter, pool and class.
type NodeLister interface {
	Nodes(ctx context.Context, datacenter, pool, class string) ([]Node, error)
}

// APINodes lists the nodes with the Norca API. The node list has no meta, so
// a node is read once for its provider ID, which does not change; the IDs are
// cached by node.
type APINodes struct {
	Client *api.Client

	mu         sync.Mutex
	providerID map[string]string // by node ID
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
		id, err := n.providerIDOf(ctx, s.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, Node{ID: s.ID, ProviderID: id, Ready: s.Status == api.NodeStatusReady})
	}
	return out, nil
}

func (n *APINodes) providerIDOf(ctx context.Context, nodeID string) (string, error) {
	n.mu.Lock()
	id, ok := n.providerID[nodeID]
	n.mu.Unlock()
	if ok {
		return id, nil
	}
	node, _, err := n.Client.Nodes().Info(nodeID, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return "", err
	}
	id = node.Meta[MetaProviderID]
	n.mu.Lock()
	if n.providerID == nil {
		n.providerID = map[string]string{}
	}
	n.providerID[nodeID] = id
	n.mu.Unlock()
	return id, nil
}
