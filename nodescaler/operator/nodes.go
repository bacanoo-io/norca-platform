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

	// Instance is the name of the instance the node runs on (the meta key
	// norca.instance); empty for a node the operator did not create.
	Instance string

	// Ready is a ready node.
	Ready bool
}

// NodeLister lists the nodes of a datacenter, pool and class.
type NodeLister interface {
	Nodes(ctx context.Context, datacenter, pool, class string) ([]Node, error)
}

// APINodes lists the nodes with the Norca API. The node list has no meta, so
// a node is read once for its instance name, which does not change; the
// names are cached by node.
type APINodes struct {
	Client *api.Client

	mu       sync.Mutex
	instance map[string]string // by node ID
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
		id, err := n.instanceOf(ctx, s.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, Node{ID: s.ID, Instance: id, Ready: s.Status == api.NodeStatusReady})
	}
	return out, nil
}

func (n *APINodes) instanceOf(ctx context.Context, nodeID string) (string, error) {
	n.mu.Lock()
	id, ok := n.instance[nodeID]
	n.mu.Unlock()
	if ok {
		return id, nil
	}
	node, _, err := n.Client.Nodes().Info(nodeID, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return "", err
	}
	id = node.Meta[MetaInstance]
	n.mu.Lock()
	if n.instance == nil {
		n.instance = map[string]string{}
	}
	n.instance[nodeID] = id
	n.mu.Unlock()
	return id, nil
}
