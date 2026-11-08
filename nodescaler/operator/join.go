// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/api"
)

// The join of the nodes. An instance whose node has not joined by the join
// deadline is deleted, and so is one the provider reports as failed; both
// are join failures. After join.max_failures in a row the group stops
// asking for instances (condition JoinFailing), because a template that does
// not work would create and delete machines for ever; a node that joins ends
// the row, and so does a new template (spec.node, the provider, its group)
// or a raised join.resume_generation. A node still joining after
// join.slow_after no longer counts as capacity (status.joining), so the
// autoscaler may ask for another one; it stays in status.nodes as slow.

// CondJoinFailing is True when the group stopped asking for instances
// because their nodes did not join.
const CondJoinFailing = "JoinFailing"

// joinState is status.join: the join failures in a row, for which template
// and resume generation they count, and the last one.
type joinState struct {
	Failures    int
	Revision    string
	Resumed     int64
	LastFailure string
	// Deleted are the instances deleted here (failed or late) whose node
	// registered: the node is purged once it is down (status.join.deleted).
	Deleted []deletedInstance
}

// deletedInstance is an instance deleted for a join failure, with its node.
type deletedInstance struct {
	Instance string
	Node     string
	At       time.Time
}

func readJoinState(status map[string]any) joinState {
	m, _ := status["join"].(map[string]any)
	var j joinState
	if v, ok := m["failures"].(float64); ok {
		j.Failures = int(v)
	}
	j.Revision, _ = m["revision"].(string)
	if v, ok := m["resumed"].(float64); ok {
		j.Resumed = int64(v)
	}
	j.LastFailure, _ = m["lastFailure"].(string)
	list, _ := m["deleted"].([]any)
	for _, item := range list {
		d, _ := item.(map[string]any)
		inst, _ := d["instance"].(string)
		node, _ := d["node"].(string)
		at, _ := d["at"].(string)
		t, err := time.Parse(time.RFC3339, at)
		if inst == "" || node == "" || err != nil {
			continue
		}
		j.Deleted = append(j.Deleted, deletedInstance{Instance: inst, Node: node, At: t})
	}
	return j
}

func (j joinState) doc() map[string]any {
	doc := map[string]any{"failures": j.Failures, "revision": j.Revision, "resumed": j.Resumed}
	if j.LastFailure != "" {
		doc["lastFailure"] = j.LastFailure
	}
	if len(j.Deleted) > 0 {
		list := make([]any, 0, len(j.Deleted))
		for _, d := range j.Deleted {
			list = append(list, map[string]any{"instance": d.Instance, "node": d.Node, "at": d.At.UTC().Format(time.RFC3339)})
		}
		doc["deleted"] = list
	}
	return doc
}

// deletedNames are the instances of j.Deleted, for matchNodes: their nodes
// are not strays.
func (j joinState) deletedNames() map[string]bool {
	out := make(map[string]bool, len(j.Deleted))
	for _, d := range j.Deleted {
		out[d.Instance] = true
	}
	return out
}

// purgeDeleted purges the nodes of the instances deleted for a join failure
// once they are down, as a scale down does, and forgets the ones that are
// gone, purged, or still up after downWait (they are strays from then on).
func (r *Reconciler) purgeDeleted(ctx context.Context, obj *api.CustomObject, j *joinState, now time.Time) {
	kept := j.Deleted[:0]
	for _, d := range j.Deleted {
		detail, err := r.Nodes.Detail(ctx, d.Node)
		switch {
		case errors.Is(err, ErrNodeNotFound):
			// purged already
		case err != nil:
			r.log.Warn("reading the node of a deleted instance failed", "group", obj.Name, "node", d.Node, "error", err)
			kept = append(kept, d)
		case !detail.Ready:
			if err := r.Nodes.Purge(ctx, d.Node); err != nil {
				r.log.Warn("purging the node of a deleted instance failed", "group", obj.Name, "node", d.Node, "error", err)
				kept = append(kept, d)
				continue
			}
			r.event(ctx, obj, api.ObjectEventTypeNormal, "NodePurged",
				fmt.Sprintf("node %s of the deleted instance %s is down and purged", d.Node, d.Instance))
		case now.Sub(d.At) > downWait:
			r.log.Warn("the node of a deleted instance is still up, it is left as it is", "group", obj.Name, "node", d.Node)
		default:
			kept = append(kept, d)
		}
	}
	j.Deleted = kept
}

// prevRecord is what the last status said about an instance.
type prevRecord struct {
	joined, slow bool
}

// previousRecords reads status.nodes.
func previousRecords(status map[string]any) map[string]prevRecord {
	out := map[string]prevRecord{}
	list, _ := status["nodes"].([]any)
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id, _ := m["instance"].(string)
		_, joined := m["node"].(string)
		slow, _ := m["slow"].(bool)
		out[id] = prevRecord{joined: joined, slow: slow}
	}
	return out
}

// trackJoins deletes the instances that failed or did not join in time,
// counts the join failures, marks the slow ones, and returns the join state
// of the group. The instances that are deleted leave o.
func (r *Reconciler) trackJoins(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	spec *groupSpec, o *observed, status map[string]any, now time.Time) joinState {

	j := readJoinState(status)
	if rev := spec.templateRevision(); j.Revision != rev || spec.Join.ResumeGeneration > j.Resumed {
		// A new template ends the row of failures; the nodes of the deleted
		// instances are still to be purged.
		j = joinState{Revision: rev, Resumed: spec.Join.ResumeGeneration, Deleted: j.Deleted}
	}
	prev := previousRecords(status)
	deadline, slowAfter := spec.joinDeadline(), spec.slowAfter()
	o.slow = map[string]bool{}

	kept := o.instances[:0]
	for _, inst := range o.instances {
		node, hasNode := o.nodeOf[instanceKey(inst)]
		age := now.Sub(inst.CreatedAt)
		var failure string
		switch {
		case inst.State == provider.StateFailed:
			failure = fmt.Sprintf("instance %s failed: %s %s", inst.ID, inst.Reason, inst.Message)
		case (inst.State == provider.StateCreating || inst.State == provider.StateRunning) && !hasNode &&
			!inst.CreatedAt.IsZero() && age > deadline:
			failure = fmt.Sprintf("the node of instance %s did not join in %s", inst.ID, deadline)
		}
		if failure != "" {
			if err := client.Delete(ctx, group, inst.ID); err != nil {
				// Kept, and deleted at a later look.
				r.log.Warn("deleting an instance failed", "group", obj.Name, "instance", inst.ID, "error", err)
				kept = append(kept, inst)
				continue
			}
			j.Failures++
			j.LastFailure = failure
			// The node of an instance deleted here was not purged before: Once the instance was gone, the
			// node was a stray: ForeignNode (and made ineligible) while it
			// still looked ready, then NodeWithoutInstance, down for good.
			if hasNode && node.ID != "" {
				j.Deleted = append(j.Deleted, deletedInstance{Instance: instanceKey(inst), Node: node.ID, At: now})
			}
			reason := "JoinTimeout"
			if inst.State == provider.StateFailed {
				reason = "InstanceFailed"
			}
			r.event(ctx, obj, api.ObjectEventTypeWarning, reason, failure+"; the instance is deleted")
			continue
		}
		kept = append(kept, inst)
		switch {
		case hasNode && !prev[inst.ID].joined:
			// A node joined: the row of failures ends.
			j.Failures, j.LastFailure = 0, ""
			r.event(ctx, obj, api.ObjectEventTypeNormal, "NodeJoined",
				fmt.Sprintf("node %s of instance %s joined", node.ID, inst.ID))
		case !hasNode && (inst.State == provider.StateCreating || inst.State == provider.StateRunning) &&
			!inst.CreatedAt.IsZero() && age > slowAfter:
			o.slow[inst.ID] = true
			if !prev[inst.ID].slow {
				r.event(ctx, obj, api.ObjectEventTypeWarning, "JoinSlow",
					fmt.Sprintf("the node of instance %s has not joined in %s; it no longer counts as capacity", inst.ID, slowAfter))
			}
		}
	}
	o.instances = kept
	return j
}

// joinCondition is the condition JoinFailing of the join state.
func joinCondition(j joinState, max int) condition {
	if j.Failures >= max {
		return condition{Type: CondJoinFailing, Status: "True", Reason: "TooManyFailures",
			Message: fmt.Sprintf("%d nodes in a row did not join, no instance is asked for until the template changes or join.resume_generation is raised; last: %s",
				j.Failures, j.LastFailure)}
	}
	return condition{Type: CondJoinFailing, Status: "False", Reason: "Joining",
		Message: fmt.Sprintf("%d join failures in a row of %d allowed", j.Failures, max)}
}
