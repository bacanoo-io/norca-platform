// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"fmt"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
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
	return j
}

func (j joinState) doc() map[string]any {
	doc := map[string]any{"failures": j.Failures, "revision": j.Revision, "resumed": j.Resumed}
	if j.LastFailure != "" {
		doc["lastFailure"] = j.LastFailure
	}
	return doc
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
		j = joinState{Revision: rev, Resumed: spec.Join.ResumeGeneration}
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
