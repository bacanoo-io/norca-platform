// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// The deletion of a node group. The operator puts its finalizer on every group
// it creates instances for, so deleting the object only marks it; the
// operator then deals with the instances of the group by spec.deletion.policy:
//
//   - drain (the default): the node of every instance is drained with the
//     drain_deadline of the scale down, and the instance is deleted once its
//     drain is complete; an instance whose node has not joined (or is down) is
//     deleted at once;
//   - terminate: every instance is deleted at once, its allocations are
//     rescheduled when its node goes down;
//   - orphan: the instances stay, with the labels of the group, and only the
//     object goes. The orphan sweep reports them (orphans.go).
//
// Once the provider lists no instance of the group, and the nodes of the
// deleted instances are down (and purged) or did not go down within downWait,
// the finalizer is removed and the object goes. A deletion that waits longer
// than deletion.drain_timeout (1 hour by default) raises the condition
// DeletionBlocked with what it waits for: a person changes the policy to
// terminate or orphan, which the operator picks up at once.

const (
	// Finalizer keeps a NodeGroup until the operator has dealt with its
	// instances.
	Finalizer = "norca.io/nodescaler"

	// CondDeletionBlocked is True while the deletion of a group has waited
	// longer than deletion.drain_timeout.
	CondDeletionBlocked = "DeletionBlocked"

	deletionDrain     = "drain"
	deletionOrphan    = "orphan"
	deletionTerminate = "terminate"
)

// deletionState is status.deletion: the policy being applied, when each
// instance was deleted (its node is waited for, up to downWait), and what the
// deletion waits for.
type deletionState struct {
	Policy  string
	Deleted map[string]time.Time // by the operator's name of the instance
	Waiting []string
}

func readDeletion(status map[string]any) deletionState {
	d, _ := status["deletion"].(map[string]any)
	s := deletionState{Deleted: map[string]time.Time{}}
	s.Policy, _ = d["policy"].(string)
	deleted, _ := d["deleted"].(map[string]any)
	for name, at := range deleted {
		if t, err := time.Parse(time.RFC3339, fmt.Sprint(at)); err == nil {
			s.Deleted[name] = t
		}
	}
	return s
}

func (s deletionState) doc() map[string]any {
	deleted := map[string]any{}
	for name, at := range s.Deleted {
		deleted[name] = at.UTC().Format(time.RFC3339)
	}
	waiting := make([]any, 0, len(s.Waiting))
	for _, w := range s.Waiting {
		waiting = append(waiting, w)
	}
	return map[string]any{"policy": s.Policy, "deleted": deleted, "waiting": waiting}
}

// deleteGroup deals with the instances of a group marked for deletion, and
// removes the finalizer when they are gone.
func (r *Reconciler) deleteGroup(ctx context.Context, obj *api.CustomObject) (time.Duration, error) {
	if !slices.Contains(obj.Finalizers, Finalizer) {
		return 0, nil
	}
	spec, err := decodeSpec(obj.Spec)
	if err != nil || spec.Provider == "" {
		// Nothing the operator could find: the object goes.
		if err != nil {
			r.log.Warn("the spec of a deleted node group is not readable; its instances are left", "group", obj.Name, "error", err)
		}
		return 0, r.release(ctx, obj)
	}
	policy := spec.deletionPolicy()
	if policy == deletionOrphan {
		r.log.Info("node group deleted, its instances are orphaned", "group", obj.Name, "provider", spec.Provider)
		return 0, r.release(ctx, obj)
	}

	now := r.now()
	prev := normalize(obj.Status)
	state := readDeletion(prev)
	if state.Policy != policy {
		r.event(ctx, obj, api.ObjectEventTypeNormal, "Deleting",
			fmt.Sprintf("the node group is deleted with the policy %s", policy))
	}
	state.Policy, state.Waiting = policy, nil

	group := spec.providerGroup()
	client, err := r.providers.client(ctx, spec.Provider, group)
	var pg *provider.Group
	if err == nil {
		pg, err = client.Group(ctx, group, spec.Node.Parameters)
	}
	if err != nil {
		state.Waiting = append(state.Waiting, fmt.Sprintf("provider %q: %v", spec.Provider, err))
		return r.deletionStatus(ctx, obj, spec, state, nil, now)
	}
	o := r.observe(obj.Name, pg)
	nodes, err := r.Nodes.Nodes(ctx, spec.Datacenter, spec.Pool, spec.NodeClass)
	if err != nil {
		return 0, fmt.Errorf("listing the nodes of node group %s: %w", obj.Name, err)
	}
	strays := matchNodes(obj.Name, o, nodes, "")

	for _, inst := range slices.Clone(o.instances) {
		name := instanceKey(inst)
		node, hasNode := o.nodeOf[name]
		if inst.State == provider.StateDeleting {
			state.Waiting = append(state.Waiting, "instance "+inst.ID+" is being deleted")
			continue
		}
		if policy == deletionDrain && isLive(inst) && hasNode && node.Ready {
			done, why := r.drainForDeletion(ctx, obj, spec, node)
			if !done {
				state.Waiting = append(state.Waiting, why)
				continue
			}
		}
		if err := client.Delete(ctx, group, inst.ID); err != nil {
			r.deletionFailed(ctx, obj, "deleting an instance failed", inst.ID, err)
			state.Waiting = append(state.Waiting, fmt.Sprintf("instance %s: %v", inst.ID, err))
			continue
		}
		o.drop(inst.ID)
		state.Deleted[name] = now
		how := "its node is drained"
		switch {
		case !hasNode:
			how = "its node had not joined"
		case !node.Ready:
			how = "its node is down"
		case policy == deletionTerminate:
			how = "policy terminate"
		}
		r.event(ctx, obj, api.ObjectEventTypeNormal, "InstanceDeleted",
			fmt.Sprintf("instance %s of the deleted node group is deleted (%s)", inst.ID, how))
		if hasNode && node.Ready {
			state.Waiting = append(state.Waiting, fmt.Sprintf("node %s of the deleted instance %s to go down", node.Name, inst.ID))
		}
	}

	// The nodes of the instances that are gone: purged once down, waited for
	// while up, up to downWait after their instance was deleted.
	for _, s := range strays {
		n := s.node
		deletedAt, ours := state.Deleted[n.Instance]
		switch {
		case !n.Ready && (ours || n.Group == obj.Name):
			if err := r.Nodes.Purge(ctx, n.ID); err != nil {
				r.deletionFailed(ctx, obj, "purging a node failed", n.ID, err)
				state.Waiting = append(state.Waiting, fmt.Sprintf("node %s: %v", n.Name, err))
			}
		case n.Ready && ours && now.Sub(deletedAt) < downWait:
			state.Waiting = append(state.Waiting, fmt.Sprintf("node %s of the deleted instance %s to go down", n.Name, n.Instance))
		case n.Ready && ours:
			r.log.Warn("the node of a deleted instance is still up, it is left as it is", "group", obj.Name, "node", n.ID)
		}
	}
	state.Waiting = uniq(state.Waiting)

	if len(state.Waiting) == 0 {
		r.log.Info("node group deleted, its instances are gone", "group", obj.Name, "policy", policy)
		return 0, r.release(ctx, obj)
	}
	return r.deletionStatus(ctx, obj, spec, state, o, now)
}

// drainForDeletion drains the node of an instance of a deleted group, and
// reports whether the drain is complete.
func (r *Reconciler) drainForDeletion(ctx context.Context, obj *api.CustomObject, spec *groupSpec, node Node) (bool, string) {
	d, err := r.Nodes.Detail(ctx, node.ID)
	if errors.Is(err, ErrNodeNotFound) {
		return true, ""
	}
	if err != nil {
		r.deletionFailed(ctx, obj, "reading a node failed", node.ID, err)
		return false, fmt.Sprintf("node %s: %v", node.Name, err)
	}
	switch {
	case d.Draining:
		return false, fmt.Sprintf("the drain of node %s", node.Name)
	case d.LastDrain == string(api.DrainStatusComplete) && !d.Eligible:
		// Drained, and not made eligible again since.
		return true, ""
	}
	if err := r.Nodes.Drain(ctx, node.ID, spec.drainDeadline()); err != nil {
		r.deletionFailed(ctx, obj, "starting a drain failed", node.ID, err)
		return false, fmt.Sprintf("node %s: %v", node.Name, err)
	}
	r.event(ctx, obj, api.ObjectEventTypeNormal, "DrainStarted",
		fmt.Sprintf("node %s of the deleted node group is drained (deadline %s)", node.Name, spec.drainDeadline()))
	return false, fmt.Sprintf("the drain of node %s", node.Name)
}

// deletionStatus writes what the deletion waits for, and DeletionBlocked
// once it has waited longer than deletion.drain_timeout.
func (r *Reconciler) deletionStatus(ctx context.Context, obj *api.CustomObject, spec *groupSpec, state deletionState,
	o *observed, now time.Time) (time.Duration, error) {

	since := time.Unix(0, obj.DeletionTime)
	blocked := condition{Type: CondDeletionBlocked, Status: "False", Reason: "Deleting",
		Message: "waiting for " + strings.Join(state.Waiting, "; ")}
	if waited := now.Sub(since); waited > spec.drainTimeout() {
		blocked = condition{Type: CondDeletionBlocked, Status: "True", Reason: "DrainTimeout",
			Message: fmt.Sprintf("after %s still waiting for %s; set spec.deletion.policy to terminate or orphan",
				spec.drainTimeout(), strings.Join(state.Waiting, "; "))}
		if conditionStatusOf(normalize(obj.Status), CondDeletionBlocked) != "True" {
			r.event(ctx, obj, api.ObjectEventTypeWarning, CondDeletionBlocked, blocked.Message)
		}
	}
	err := r.writeStatus(ctx, obj, func(status map[string]any) {
		status["deletion"] = state.doc()
		if o != nil {
			ready, joining, failed, _ := o.counts()
			status["replicas"], status["joining"], status["failed"] = ready, joining, failed
			status["nodes"] = nodeRecords(o, nil)
		}
		delete(status, "removal")
		setCondition(status, blocked, now)
	})
	if errors.Is(err, controller.ErrNotFound) {
		return 0, nil
	}
	return r.cfg.JoinCheck, err
}

// release removes the finalizer: the object goes.
func (r *Reconciler) release(ctx context.Context, obj *api.CustomObject) error {
	_, err := r.Objects.UpdateObjectFinalizers(ctx, NodeGroupKind, obj.Namespace, obj.Name, nil, []string{Finalizer}, 0)
	if errors.Is(err, controller.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("removing the finalizer of node group %s: %w", obj.Name, err)
	}
	return nil
}

// deletionFailed reports a step of a deletion that failed, as downFailed does
// for a scale down.
func (r *Reconciler) deletionFailed(ctx context.Context, obj *api.CustomObject, what, target string, err error) {
	r.log.Warn(what, "group", obj.Name, "target", target, "error", err)
	r.event(ctx, obj, api.ObjectEventTypeWarning, "DeletionFailed", fmt.Sprintf("%s (%s): %v", what, target, err))
}

// uniq drops repeated entries, keeping the order.
func uniq(list []string) []string {
	seen := map[string]bool{}
	out := list[:0]
	for _, s := range list {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
