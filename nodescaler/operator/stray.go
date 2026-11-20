// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
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
//
// The instances of the requests the listing of the provider has not all
// shown (pending.go) are instances of the group too: a node with the name of
// one is its node, not a node without an instance (review finding CON-6i: a
// machine that booted before the listing showed it was foreign, and its node
// stayed ineligible).
//
// A node the operator made ineligible because its instance was not known is
// remembered (status.madeIneligible) and made eligible again when the listing
// shows the instance, if the instance was created before the node was made
// ineligible: then the listing was behind, and the node was the node of an
// instance all along. A node that had the name before the instance was made
// stays ineligible, and so does one that was ineligible already, that a
// second ready node made foreign, or that is being drained: the operator only
// undoes what it did, for this one reason.

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
	// unknown is a node whose instance the provider does not have, as far
	// as the operator knows; not a second node of an instance.
	unknown bool
}

// matchNodes sets the node of each instance in o.nodeOf and returns the
// nodes of the group that have no instance. Of the nodes with the name of
// one instance, the ready one that registered first is its node, or the one
// that registered last when none is ready (a machine that came up again
// registers a new node and leaves the old one down). The nodes of the
// instances in removing, deleted by a scale down or a replacement, are left
// out: they go down and are purged. awaited are the names of the instances
// of the requests the listing has not all shown: they are known like the
// listed ones, and the node of one is in o.nodeOf under its name.
func matchNodes(group string, o *observed, nodes []Node, removing, awaited map[string]bool) []strayNode {
	known := maps.Clone(awaited)
	if known == nil {
		known = map[string]bool{}
	}
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
				strays = append(strays, strayNode{node: n, kind: strayForeign, unknown: true,
					reason: fmt.Sprintf("node %s is ready with the name of instance %s, which the provider does not have", n.Name, n.Instance)})
			} else {
				strays = append(strays, strayNode{node: n, kind: strayGone, unknown: true,
					reason: fmt.Sprintf("node %s is down and its instance %s is gone", n.Name, n.Instance)})
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
			strays = append(strays, strayNode{node: n, kind: strayForeign,
				reason: fmt.Sprintf("node %s is a second ready node with the name of instance %s (the first is %s)", n.Name, name, ready[0].Name)})
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

// ineligibleNode is a node the operator made ineligible because its instance
// was not known, and when.
type ineligibleNode struct {
	Name string
	At   time.Time
}

// ineligibleNodes is status.madeIneligible, by node ID: only these nodes are
// made eligible again when the listing shows their instance.
type ineligibleNodes map[string]ineligibleNode

func readMadeIneligible(status map[string]any) ineligibleNodes {
	out := ineligibleNodes{}
	list, _ := status["madeIneligible"].([]any)
	for _, item := range list {
		m, _ := item.(map[string]any)
		id, _ := m["node"].(string)
		name, _ := m["name"].(string)
		at, _ := m["at"].(string)
		t, err := time.Parse(time.RFC3339, at)
		if id == "" || err != nil {
			continue
		}
		out[id] = ineligibleNode{Name: name, At: t}
	}
	return out
}

// docs is status.madeIneligible, nil without a node.
func (m ineligibleNodes) docs() []any {
	if len(m) == 0 {
		return nil
	}
	out := make([]any, 0, len(m))
	for _, id := range slices.Sorted(maps.Keys(m)) {
		out = append(out, map[string]any{"node": id, "name": m[id].Name, "at": m[id].At.UTC().Format(time.RFC3339)})
	}
	return out
}

// handleStrays reports the stray nodes that are new (an event each) and makes
// the foreign ones ineligible. It returns status.strayNodes, the condition
// ForeignNode and status.madeIneligible: the nodes it made ineligible because
// their instance was not known, those of them whose instance the listing
// shows now made eligible again (restoreEligible).
func (r *Reconciler) handleStrays(ctx context.Context, obj *api.CustomObject, strays []strayNode, status map[string]any,
	o *observed, now time.Time) ([]any, condition, ineligibleNodes) {

	prev := previousStrays(status)
	before := readMadeIneligible(status)
	made := ineligibleNodes{}
	stray := make(map[string]bool, len(strays))
	records := make([]any, 0, len(strays))
	var foreign []string
	for _, s := range strays {
		stray[s.node.ID] = true
		if prev[s.node.ID] != s.kind {
			reason := "NodeWithoutInstance"
			typ := api.ObjectEventTypeNormal
			if s.kind == strayForeign {
				reason, typ = CondForeignNode, api.ObjectEventTypeWarning
			}
			r.event(ctx, obj, typ, reason, s.reason)
		}
		if m, ok := before[s.node.ID]; ok && s.unknown {
			made[s.node.ID] = m // its instance is still not known
		}
		if s.kind == strayForeign {
			foreign = append(foreign, s.node.Name)
			if s.node.Eligible {
				if err := r.Nodes.SetIneligible(ctx, s.node.ID); err != nil {
					r.log.Warn("making a foreign node ineligible failed", "group", obj.Name, "node", s.node.Name, "error", err)
				} else {
					r.event(ctx, obj, api.ObjectEventTypeWarning, "NodeIneligible",
						fmt.Sprintf("foreign node %s is made ineligible for new work", s.node.Name))
					if s.unknown {
						made[s.node.ID] = ineligibleNode{Name: s.node.Name, At: now}
					}
				}
			}
		}
		records = append(records, map[string]any{
			"node": s.node.ID, "name": s.node.Name, "instance": s.node.Instance, "kind": s.kind,
		})
	}
	r.restoreEligible(ctx, obj, before, stray, o, made)
	if len(foreign) > 0 {
		return records, condition{Type: CondForeignNode, Status: "True", Reason: "ForeignNode",
			Message: fmt.Sprintf("ready nodes without an instance of the operator, made ineligible: %s", strings.Join(foreign, ", "))}, made
	}
	return records, condition{Type: CondForeignNode, Status: "False", Reason: "None",
		Message: "every ready node of the group runs on an instance of the operator"}, made
}

// restoreEligible makes eligible again the nodes the operator made
// ineligible because their instance was not known (before) that are no
// strays any more, once the listing shows their instance. Such a node is
// still to be made eligible, and stays in made, while its instance is one of
// a request the listing has not shown, or while the call fails. It is left
// ineligible, and forgotten, when the instance was not created before the
// node was made ineligible (the node had the name first: it was not the
// node of this instance then), when the instance is not live, and when the
// node is being drained or is eligible already (other hands). A node that
// is gone is forgotten.
func (r *Reconciler) restoreEligible(ctx context.Context, obj *api.CustomObject, before ineligibleNodes, stray map[string]bool,
	o *observed, made ineligibleNodes) {

	for _, id := range slices.Sorted(maps.Keys(before)) {
		if stray[id] {
			continue
		}
		var name string
		var node Node
		for n, cand := range o.nodeOf {
			if cand.ID == id {
				name, node = n, cand
			}
		}
		if name == "" {
			continue // gone, or the node of an instance that is being removed
		}
		m := before[id]
		i := slices.IndexFunc(o.instances, func(inst provider.Instance) bool { return instanceKey(inst) == name })
		if i < 0 {
			made[id] = m // the instance of a request the listing has not shown
			continue
		}
		inst := o.instances[i]
		if node.Eligible || !isLive(inst) {
			continue
		}
		if inst.CreatedAt.IsZero() || !inst.CreatedAt.Before(m.At) {
			r.log.Info("a node that had the name of an instance before the instance was made stays ineligible",
				"group", obj.Name, "node", node.Name, "instance", name)
			continue
		}
		d, err := r.Nodes.Detail(ctx, id)
		switch {
		case errors.Is(err, ErrNodeNotFound):
			continue
		case err != nil:
			r.log.Warn("reading a node to make it eligible again failed", "group", obj.Name, "node", node.Name, "error", err)
			made[id] = m
			continue
		case d.Draining || d.Eligible:
			continue
		}
		if err := r.Nodes.SetEligible(ctx, id); err != nil {
			r.log.Warn("making a node eligible again failed", "group", obj.Name, "node", node.Name, "error", err)
			made[id] = m
			continue
		}
		r.event(ctx, obj, api.ObjectEventTypeNormal, "NodeEligible", fmt.Sprintf(
			"node %s is eligible again: the provider lists its instance %s, which it did not when the node was made ineligible",
			node.Name, name))
	}
}
