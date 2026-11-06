// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/bacanoo-io/norca/norca/api"
)

// Nodes without an instance. A node that carries the marks of the group (its
// meta norca.node_group, or the name of one of its instances) is matched with
// the instance of that name. A node whose instance is gone is reported:
//
//   - one that is down is what is left of a deleted instance (NodeWithoutInstance);
//   - one that is ready runs without a machine the operator asked for, with a
//     bootstrap that left through the user data, for example: it is foreign
//     (ForeignNode), and is made ineligible for new work;
//   - a second ready node with the name of an instance that already has one is
//     foreign too; the one that registered first is the node of the instance.
//
// Nodes without the marks (installed by hand) are left alone. This detects; it
// does not prevent.

// CondForeignNode is True when a ready node of the group runs without an
// instance of the operator.
const CondForeignNode = "ForeignNode"

// The kinds of a stray node.
const (
	strayForeign = "foreign"
	strayGone    = "gone"
)

// strayNode is a node of the group without an instance.
type strayNode struct {
	node   Node
	kind   string
	reason string
}

// matchNodes sets the node of each instance in o.nodeOf and returns the
// nodes of the group that have no instance. Of the nodes with the name of
// one instance, the ready one that registered first is its node, or the one
// that registered last when none is ready (a machine that came up again
// registers a new node and leaves the old one down). The nodes of the
// instances in removing, deleted by a scale down or a replacement, are left
// out: they go down and are purged.
func matchNodes(group string, o *observed, nodes []Node, removing map[string]bool) []strayNode {
	known := map[string]bool{}
	for _, inst := range o.instances {
		known[instanceKey(inst)] = true
	}
	byInstance := map[string][]Node{}
	var strays []strayNode
	for _, n := range nodes {
		if n.Instance == "" || (n.Group != group && !known[n.Instance]) {
			continue // not a node of this group
		}
		if removing[n.Instance] && !known[n.Instance] {
			continue // the node of an instance a scale down or a replacement deleted
		}
		if !known[n.Instance] {
			if n.Ready {
				strays = append(strays, strayNode{n, strayForeign,
					fmt.Sprintf("node %s is ready with the name of instance %s, which the provider does not have", n.Name, n.Instance)})
			} else {
				strays = append(strays, strayNode{n, strayGone,
					fmt.Sprintf("node %s is down and its instance %s is gone", n.Name, n.Instance)})
			}
			continue
		}
		byInstance[n.Instance] = append(byInstance[n.Instance], n)
	}
	for _, name := range slices.Sorted(maps.Keys(byInstance)) {
		list := byInstance[name]
		slices.SortFunc(list, func(a, b Node) int { return cmp.Compare(a.CreateIndex, b.CreateIndex) })
		var ready []Node
		for _, n := range list {
			if n.Ready {
				ready = append(ready, n)
			}
		}
		if len(ready) == 0 {
			o.nodeOf[name] = list[len(list)-1]
			continue
		}
		o.nodeOf[name] = ready[0]
		for _, n := range ready[1:] {
			strays = append(strays, strayNode{n, strayForeign,
				fmt.Sprintf("node %s is a second ready node with the name of instance %s (the first is %s)", n.Name, name, ready[0].Name)})
		}
	}
	slices.SortFunc(strays, func(a, b strayNode) int { return strings.Compare(a.node.ID, b.node.ID) })
	return strays
}

// previousStrays reads status.strayNodes: the kind of each node reported.
func previousStrays(status map[string]any) map[string]string {
	out := map[string]string{}
	list, _ := status["strayNodes"].([]any)
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			id, _ := m["node"].(string)
			kind, _ := m["kind"].(string)
			out[id] = kind
		}
	}
	return out
}

// handleStrays reports the stray nodes that are new (an event each) and makes
// the foreign ones ineligible. It returns status.strayNodes and the
// condition ForeignNode.
func (r *Reconciler) handleStrays(ctx context.Context, obj *api.CustomObject, strays []strayNode, status map[string]any) ([]any, condition) {
	prev := previousStrays(status)
	records := make([]any, 0, len(strays))
	var foreign []string
	for _, s := range strays {
		if prev[s.node.ID] != s.kind {
			reason := "NodeWithoutInstance"
			typ := api.ObjectEventTypeNormal
			if s.kind == strayForeign {
				reason, typ = CondForeignNode, api.ObjectEventTypeWarning
			}
			r.event(ctx, obj, typ, reason, s.reason)
		}
		if s.kind == strayForeign {
			foreign = append(foreign, s.node.Name)
			if s.node.Eligible {
				if err := r.Nodes.SetIneligible(ctx, s.node.ID); err != nil {
					r.log.Warn("making a foreign node ineligible failed", "group", obj.Name, "node", s.node.Name, "error", err)
				} else {
					r.event(ctx, obj, api.ObjectEventTypeWarning, "NodeIneligible",
						fmt.Sprintf("foreign node %s is made ineligible for new work", s.node.Name))
				}
			}
		}
		records = append(records, map[string]any{
			"node": s.node.ID, "name": s.node.Name, "instance": s.node.Instance, "kind": s.kind,
		})
	}
	if len(foreign) > 0 {
		return records, condition{Type: CondForeignNode, Status: "True", Reason: "ForeignNode",
			Message: fmt.Sprintf("ready nodes without an instance of the operator, made ineligible: %s", strings.Join(foreign, ", "))}
	}
	return records, condition{Type: CondForeignNode, Status: "False", Reason: "None", Message: "every ready node of the group runs on an instance of the operator"}
}
