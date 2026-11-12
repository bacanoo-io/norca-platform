// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"fmt"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/api"
)

// The repair of nodes. A node can be down while its instance runs: the
// machine hangs, its agent died, its network is gone. Such a node is neither
// ready nor joining, so nothing replaces it: it holds a place under the
// maximum of the group for good, and the autoscaler never lowers a group
// whose scale differs from its ready nodes. After repair.after the instance
// is deleted; the group is then one short of its scale and asks for a new
// one, like after any instance that is gone.
//
// When half or more of the nodes of the group are down, at least two, it is
// not the nodes that failed but what they share (the datacenter, the
// network to the servers), and new machines would fare no better: nothing
// is repaired until the nodes are back or few enough are down.

const (
	// CondRepairHeld is True while nodes that are due for repair are left
	// alone because too many of the group are down.
	CondRepairHeld = "RepairHeld"

	// defaultRepairAfter is how long a node is down before its instance is
	// replaced: longer than a reboot and than the deadline of a join.
	defaultRepairAfter = 15 * time.Minute
)

// repairEnabled reports whether the instances of nodes that are down are
// replaced (repair.enabled, true by default).
func (s *groupSpec) repairEnabled() bool {
	return s.Repair.Enabled == nil || *s.Repair.Enabled
}

// repairAfter is how long a node must be down before its instance is
// replaced (repair.after, 15 minutes by default).
func (s *groupSpec) repairAfter() time.Duration {
	return durationOr(s.Repair.After, defaultRepairAfter)
}

// repairDown deletes the instances whose node has been down for
// repair.after, unless too many nodes of the group are down, and returns the
// RepairHeld condition. The nodes of the deleted instances are added to the
// join state to be purged; the instances leave o.
func (r *Reconciler) repairDown(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	spec *groupSpec, o *observed, js *joinState, now time.Time) condition {

	notHeld := condition{Type: CondRepairHeld, Status: "False", Reason: "NotHeld"}
	if !spec.repairEnabled() {
		return notHeld
	}

	after := spec.repairAfter()
	withNode, down := 0, 0
	due := map[string]Node{} // by instance ID
	for _, inst := range o.instances {
		node, ok := o.nodeOf[instanceKey(inst)]
		if !isLive(inst) || !ok || o.retiring[inst.ID] {
			continue
		}
		withNode++
		if node.DownSince.IsZero() {
			continue
		}
		down++
		if now.Sub(node.DownSince) >= after {
			due[inst.ID] = node
		}
	}
	if len(due) == 0 {
		return notHeld
	}
	if down >= 2 && down*2 >= withNode {
		return condition{Type: CondRepairHeld, Status: "True", Reason: "TooManyDown",
			Message: fmt.Sprintf("%d of the %d nodes of the group are down: not repaired while half or more are", down, withNode)}
	}

	kept := o.instances[:0]
	for _, inst := range o.instances {
		node, isDue := due[inst.ID]
		if !isDue || r.isProtected(ctx, node.ID) {
			kept = append(kept, inst)
			continue
		}
		if err := client.Delete(ctx, group, inst.ID); err != nil {
			// Kept, and repaired at a later look.
			r.log.Warn("deleting the instance of a node that is down failed",
				"group", obj.Name, "instance", inst.ID, "node", node.ID, "error", err)
			kept = append(kept, inst)
			continue
		}
		js.Deleted = append(js.Deleted, deletedInstance{Instance: instanceKey(inst), Node: node.ID, At: now})
		r.event(ctx, obj, api.ObjectEventTypeWarning, "NodeRepaired", fmt.Sprintf(
			"node %s of instance %s has been down for %s; the instance is deleted and replaced",
			node.ID, inst.ID, now.Sub(node.DownSince).Round(time.Second)))
	}
	o.instances = kept
	return notHeld
}

// isProtected reports whether a node carries the mark that keeps the
// operator from replacing it. A node that cannot be read is not replaced.
func (r *Reconciler) isProtected(ctx context.Context, nodeID string) bool {
	d, err := r.Nodes.Detail(ctx, nodeID)
	if err != nil {
		return true
	}
	return d.Meta[MetaProtect] == "true"
}
