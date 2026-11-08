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

	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/api"
)

// Scale down. When a group has more live instances than its scale, the
// operator removes one at a time:
//
//   - an instance whose node has not joined goes first, at once: there is
//     nothing on it;
//   - otherwise a node is chosen by the selector of spec.policy.scale_down:
//     least_busy (the fewest allocations, drained with drain_deadline) or empty
//     (only a node without allocations, made ineligible, checked again after
//     a settle time, and never drained). A node with the meta
//     norca.scale.protect = "true" is never chosen, and least_busy does not
//     choose a node one of whose allocations could not be placed again
//     (distinct_hosts with no other node free for it);
//   - when no node may go, the condition ScaleDownBlocked is set with a
//     retryAfter time that doubles with each veto (10 minutes to 6 hours); the
//     autoscaler sets the scale back and does not lower it before that time;
//   - a node behind a load balancer leaves it before its drain (edge.go);
//   - when the drain is complete (or the node still empty), the instance is
//     deleted, and the node, once it is down, is purged; a drain that does not
//     finish within its deadline and a grace time is stopped, the node made
//     eligible again (DrainStuck), and the group backed off the same way;
//   - a node that was out of service by hand when it was chosen (ineligible:
//     drained or made ineligible by a person) is held: when the removal stops
//     (the scale raised again, a stuck drain), only the operator's own drain
//     is stopped and the node stays ineligible, as the person left it.
//
// The removal in progress is status.removal, so an operator that restarts, or
// another instance after a leader change, goes on with it. A raise of the
// scale during a removal stops it.

const (
	// CondScaleDownBlocked is True while no node of the group may go.
	CondScaleDownBlocked = "ScaleDownBlocked"

	// MetaProtect on a node keeps it from being chosen by a scale down.
	MetaProtect = "norca.scale.protect"

	selectorLeastBusy = "least_busy"
	selectorEmpty     = "empty"

	phaseLeaving  = "leaving" // an edge node leaves the load balancers (edge.go)
	phaseDraining = "draining"
	phaseSettling = "settling"
	phaseDeleting = "deleting"
	phaseDeleted  = "deleted"

	// settleTime is the wait of an empty node between its ineligibility
	// and the second look, for plans that were already in flight.
	settleTime = 10 * time.Second

	// drainGrace is how long a drain may run past its deadline (when the
	// remaining allocations are told to stop) before it counts as stuck.
	drainGrace = 5 * time.Minute

	// downWait is how long the node of a deleted instance may stay up
	// before the operator stops waiting for it.
	downWait = 15 * time.Minute

	vetoFirst = 10 * time.Minute
	vetoMax   = 6 * time.Hour
)

// removal is status.removal: the instance being removed.
type removal struct {
	Instance    string    // the provider's ID
	Name        string    // the operator's name, the meta norca.instance of its node
	Node        string    // empty for an instance whose node had not joined
	Selector    string    // least_busy or empty
	Phase       string    // draining, settling, deleting, deleted
	Started     time.Time // of the phase
	SettleUntil time.Time
	// Held is a node that was out of service by hand (ineligible: drained
	// or made ineligible) when the removal took it: given back, it stays so.
	Held bool
}

func (m *removal) doc() map[string]any {
	d := map[string]any{
		"instance": m.Instance, "name": m.Name, "selector": m.Selector, "phase": m.Phase,
		"started": m.Started.UTC().Format(time.RFC3339),
	}
	if m.Node != "" {
		d["node"] = m.Node
	}
	if !m.SettleUntil.IsZero() {
		d["settleUntil"] = m.SettleUntil.UTC().Format(time.RFC3339Nano)
	}
	if m.Held {
		d["held"] = true
	}
	return d
}

func readRemoval(status map[string]any) *removal {
	d, ok := status["removal"].(map[string]any)
	if !ok {
		return nil
	}
	str := func(k string) string { s, _ := d[k].(string); return s }
	m := &removal{Instance: str("instance"), Name: str("name"), Node: str("node"), Selector: str("selector"), Phase: str("phase")}
	m.Started, _ = time.Parse(time.RFC3339, str("started"))
	m.SettleUntil, _ = time.Parse(time.RFC3339Nano, str("settleUntil"))
	m.Held, _ = d["held"].(bool)
	if m.Instance == "" {
		return nil
	}
	return m
}

// downVeto is status.scaleDown: the vetoes in a row and the time the
// autoscaler may try again.
type downVeto struct {
	Vetoes     int
	RetryAfter time.Time
	Reason     string
	Message    string
}

func readVeto(status map[string]any) downVeto {
	d, _ := status["scaleDown"].(map[string]any)
	v := downVeto{}
	if n, ok := d["vetoes"].(float64); ok {
		v.Vetoes = int(n)
	}
	v.RetryAfter, _ = time.Parse(time.RFC3339, fmt.Sprint(d["retryAfter"]))
	v.Reason, _ = d["reason"].(string)
	v.Message, _ = d["message"].(string)
	return v
}

func (v downVeto) doc() map[string]any {
	if v.Vetoes == 0 {
		return nil
	}
	return map[string]any{"vetoes": v.Vetoes, "retryAfter": v.RetryAfter.UTC().Format(time.RFC3339),
		"reason": v.Reason, "message": v.Message}
}

// block records one more veto: the wait doubles from vetoFirst to vetoMax.
func (v *downVeto) block(now time.Time, reason, message string) {
	v.Vetoes++
	wait := vetoFirst
	for i := 1; i < v.Vetoes && wait < vetoMax; i++ {
		wait *= 2
	}
	v.RetryAfter = now.Add(min(wait, vetoMax))
	v.Reason, v.Message = reason, message
}

// condition is ScaleDownBlocked of the veto.
func (v downVeto) condition(now time.Time) condition {
	if v.Vetoes > 0 && now.Before(v.RetryAfter) {
		at := v.RetryAfter.UTC().Format(time.RFC3339)
		return condition{Type: CondScaleDownBlocked, Status: "True", Reason: v.Reason,
			Message: fmt.Sprintf("%s; no scale down before %s", v.Message, at), RetryAfter: at}
	}
	return condition{Type: CondScaleDownBlocked, Status: "False", Reason: "None"}
}

// downResult is what a look at the scale down gives the status.
type downResult struct {
	removal *removal
	veto    downVeto
	requeue time.Duration
}

// scaleDown removes instances above the scale, one at a time.
func (r *Reconciler) scaleDown(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	spec *groupSpec, o *observed, status map[string]any, rolling bool, now time.Time) downResult {

	res := downResult{removal: readRemoval(status), veto: readVeto(status)}
	want := spec.replicas()
	_, _, _, live := o.counts()

	if m := res.removal; m != nil {
		r.continueRemoval(ctx, obj, client, group, spec, o, &res, want, live, now)
		return res
	}
	if live <= want || rolling {
		// No removal starts while nodes are replaced: the scale counts
		// neither the old nodes nor the new ones that join.
		return res
	}

	// An instance whose node has not joined goes first, the newest of them.
	var unjoined []provider.Instance
	for _, inst := range o.instances {
		if isLive(inst) {
			if _, ok := o.nodeOf[instanceKey(inst)]; !ok {
				unjoined = append(unjoined, inst)
			}
		}
	}
	if len(unjoined) > 0 {
		slices.SortFunc(unjoined, func(a, b provider.Instance) int { return b.CreatedAt.Compare(a.CreatedAt) })
		inst := unjoined[0]
		if err := client.Delete(ctx, group, inst.ID); err != nil {
			r.downFailed(ctx, obj, "deleting an instance for a scale down failed", inst.ID, err)
			res.requeue = r.cfg.RetryAfter
			return res
		}
		o.drop(inst.ID)
		r.event(ctx, obj, api.ObjectEventTypeNormal, "InstanceDeleted",
			fmt.Sprintf("instance %s, whose node had not joined, is deleted for a scale of %d", inst.ID, want))
		res.requeue = r.cfg.JoinCheck
		return res
	}

	if res.veto.Vetoes > 0 && now.Before(res.veto.RetryAfter) {
		// The autoscaler sets the scale back; nothing is removed meanwhile.
		return res
	}

	inst, node, why := r.chooseNode(ctx, spec, o)
	if inst == nil {
		res.veto.block(now, "NoCandidate", why)
		r.event(ctx, obj, api.ObjectEventTypeWarning, CondScaleDownBlocked, res.veto.condition(now).Message)
		return res
	}
	m := &removal{Instance: inst.ID, Name: instanceKey(*inst), Node: node.ID, Selector: spec.selector(), Started: now,
		Held: !node.Eligible}
	switch m.Selector {
	case selectorEmpty:
		if err := r.Nodes.SetIneligible(ctx, node.ID); err != nil {
			r.downFailed(ctx, obj, "making a node ineligible failed", node.ID, err)
			res.requeue = r.cfg.RetryAfter
			return res
		}
		m.Phase, m.SettleUntil = phaseSettling, now.Add(settleTime)
		r.event(ctx, obj, api.ObjectEventTypeNormal, "ScaleDown",
			fmt.Sprintf("node %s of instance %s is empty and made ineligible, for a scale of %d", node.Name, inst.ID, want))
		res.requeue = settleTime
	default:
		if done, _ := r.leaveTraffic(ctx, obj, spec, node.ID, node.Name, now, now); !done {
			// An edge node leaves the load balancers first.
			if err := r.Nodes.SetIneligible(ctx, node.ID); err != nil {
				r.downFailed(ctx, obj, "making a node ineligible failed", node.ID, err)
				res.requeue = r.cfg.RetryAfter
				return res
			}
			m.Phase = phaseLeaving
			r.event(ctx, obj, api.ObjectEventTypeNormal, "ScaleDown",
				fmt.Sprintf("node %s of instance %s leaves the load balancers, then it is drained (deadline %s), for a scale of %d",
					node.Name, inst.ID, spec.drainDeadline(), want))
			res.requeue = leaveCheck
			break
		}
		if err := r.Nodes.Drain(ctx, node.ID, spec.drainDeadline()); err != nil {
			r.downFailed(ctx, obj, "starting a drain failed", node.ID, err)
			res.requeue = r.cfg.RetryAfter
			return res
		}
		m.Phase = phaseDraining
		r.event(ctx, obj, api.ObjectEventTypeNormal, "ScaleDown",
			fmt.Sprintf("node %s of instance %s is drained (deadline %s), for a scale of %d", node.Name, inst.ID, spec.drainDeadline(), want))
		res.requeue = r.cfg.JoinCheck
	}
	res.removal = m
	return res
}

// chooseNode picks the node that goes, or says why none may.
func (r *Reconciler) chooseNode(ctx context.Context, spec *groupSpec, o *observed) (*provider.Instance, Node, string) {
	type candidate struct {
		inst   provider.Instance
		node   Node
		allocs int
	}
	var cands []candidate
	var reasons []string
	for _, inst := range o.instances {
		node, ok := o.nodeOf[instanceKey(inst)]
		if !isLive(inst) || !ok || !node.Ready {
			continue
		}
		d, err := r.Nodes.Detail(ctx, node.ID)
		if err != nil {
			reasons = append(reasons, fmt.Sprintf("%s: %v", node.Name, err))
			continue
		}
		if d.Meta[MetaProtect] == "true" {
			reasons = append(reasons, node.Name+": protected ("+MetaProtect+")")
			continue
		}
		if spec.selector() == selectorEmpty {
			if len(d.Allocs) > 0 {
				reasons = append(reasons, fmt.Sprintf("%s: %d allocations (selector empty)", node.Name, len(d.Allocs)))
				continue
			}
		} else if why := r.irreplaceable(ctx, d.Allocs, node.ID); why != "" {
			reasons = append(reasons, node.Name+": "+why)
			continue
		}
		cands = append(cands, candidate{inst, node, len(d.Allocs)})
	}
	if len(cands) == 0 {
		if len(reasons) == 0 {
			reasons = append(reasons, "no ready node of the group")
		}
		return nil, Node{}, "no node of the group may go: " + strings.Join(reasons, "; ")
	}
	// The least busy, then the oldest: it is the closest to its age limit.
	slices.SortStableFunc(cands, func(a, b candidate) int {
		if a.allocs != b.allocs {
			return a.allocs - b.allocs
		}
		switch {
		case a.node.CreateIndex < b.node.CreateIndex:
			return -1
		case a.node.CreateIndex > b.node.CreateIndex:
			return 1
		}
		return 0
	})
	return &cands[0].inst, cands[0].node, ""
}

// irreplaceable says why an allocation of the node could not be placed again
// without it, or "" when all could.
func (r *Reconciler) irreplaceable(ctx context.Context, allocs []NodeAlloc, nodeID string) string {
	seen := map[string]bool{}
	for _, a := range allocs {
		key := a.Namespace + "/" + a.JobID + "/" + a.Group
		if seen[key] {
			continue
		}
		seen[key] = true
		ok, why, err := r.Nodes.Replaceable(ctx, a, nodeID)
		if err != nil {
			return fmt.Sprintf("the placement of job %s could not be checked: %v", a.JobID, err)
		}
		if !ok {
			return why
		}
	}
	return ""
}

// continueRemoval goes on with the removal in progress.
func (r *Reconciler) continueRemoval(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	spec *groupSpec, o *observed, res *downResult, want, live int64, now time.Time) {

	m := res.removal
	inst := o.find(m.Instance)
	res.requeue = r.cfg.JoinCheck

	// The scale was raised again before the instance was deleted: the node
	// stays.
	if inst != nil && (m.Phase == phaseLeaving || m.Phase == phaseDraining || m.Phase == phaseSettling) && live <= want {
		if m.Node != "" {
			if err := r.giveBack(ctx, m.Node, m.Held, m.Phase == phaseDraining); err != nil {
				r.downFailed(ctx, obj, "giving a node back failed", m.Node, err)
				return
			}
			r.backToTraffic(ctx, m.Node)
		}
		r.event(ctx, obj, api.ObjectEventTypeNormal, "ScaleDownCanceled",
			fmt.Sprintf("the scale is %d again: node %s of instance %s stays%s", want, m.Node, m.Instance, heldNote(m.Held)))
		res.removal, res.requeue = nil, 0
		return
	}

	switch {
	case inst == nil || m.Phase == phaseDeleted:
		// The instance is gone; its node is purged once it is down.
		if m.Phase != phaseDeleted {
			m.Phase, m.Started = phaseDeleted, now
		}
		if m.Node == "" {
			res.removal, res.requeue = nil, 0
			return
		}
		d, err := r.Nodes.Detail(ctx, m.Node)
		switch {
		case errors.Is(err, ErrNodeNotFound):
			res.removal, res.requeue = nil, 0
		case err != nil:
			r.downFailed(ctx, obj, "reading a node failed", m.Node, err)
		case !d.Ready || now.Sub(m.Started) > downWait:
			if d.Ready {
				r.log.Warn("the node of a deleted instance is still up, it is left as it is", "group", obj.Name, "node", m.Node)
			} else if err := r.Nodes.Purge(ctx, m.Node); err != nil {
				r.downFailed(ctx, obj, "purging a node failed", m.Node, err)
				return
			}
			res.removal, res.requeue = nil, 0
		}

	case m.Phase == phaseDeleting:
		r.deleteInstance(ctx, obj, client, group, o, m, now)

	case m.Phase == phaseLeaving:
		if done, _ := r.leaveTraffic(ctx, obj, spec, m.Node, m.Name, m.Started, now); !done {
			res.requeue = leaveCheck
			return
		}
		if err := r.Nodes.Drain(ctx, m.Node, spec.drainDeadline()); err != nil {
			r.downFailed(ctx, obj, "starting a drain failed", m.Node, err)
			return
		}
		m.Phase, m.Started = phaseDraining, now
		r.event(ctx, obj, api.ObjectEventTypeNormal, "ScaleDown",
			fmt.Sprintf("node %s of instance %s is drained (deadline %s)", m.Name, m.Instance, spec.drainDeadline()))

	case m.Phase == phaseSettling:
		d, err := r.Nodes.Detail(ctx, m.Node)
		if err != nil {
			r.downFailed(ctx, obj, "reading a node failed", m.Node, err)
			return
		}
		if len(d.Allocs) > 0 {
			// Work arrived before the ineligibility: the node stays.
			if err := r.giveBack(ctx, m.Node, m.Held, false); err != nil {
				r.downFailed(ctx, obj, "giving a node back failed", m.Node, err)
				return
			}
			how := "it stays and is eligible again"
			if m.Held {
				how = "it stays" + heldNote(true)
			}
			r.event(ctx, obj, api.ObjectEventTypeNormal, "NodeNotEmpty",
				fmt.Sprintf("node %s received work while it was made ineligible; %s", m.Node, how))
			res.removal = nil
			return
		}
		if now.Before(m.SettleUntil) {
			res.requeue = m.SettleUntil.Sub(now)
			return
		}
		m.Phase, m.Started = phaseDeleting, now
		r.deleteInstance(ctx, obj, client, group, o, m, now)

	case m.Phase == phaseDraining:
		d, err := r.Nodes.Detail(ctx, m.Node)
		if err != nil {
			r.downFailed(ctx, obj, "reading a node failed", m.Node, err)
			return
		}
		switch {
		case !d.Draining && d.LastDrain == string(api.DrainStatusComplete):
			m.Phase, m.Started = phaseDeleting, now
			r.deleteInstance(ctx, obj, client, group, o, m, now)
		case !d.Draining || now.Sub(m.Started) > spec.drainDeadline()+drainGrace:
			// Canceled by someone else, or stuck: the node stays.
			if err := r.giveBack(ctx, m.Node, m.Held, d.Draining); err != nil {
				r.downFailed(ctx, obj, "stopping a drain failed", m.Node, err)
				return
			}
			r.backToTraffic(ctx, m.Node)
			why := fmt.Sprintf("the drain of node %s did not finish in %s", m.Node, spec.drainDeadline()+drainGrace)
			if !d.Draining {
				why = fmt.Sprintf("the drain of node %s was stopped (%s)", m.Node, d.LastDrain)
			}
			res.veto.block(now, "DrainStuck", why+givenBack(m.Held))
			r.event(ctx, obj, api.ObjectEventTypeWarning, "DrainStuck", res.veto.condition(now).Message)
			res.removal = nil
		}
	}
}

// giveBack undoes what a removal or a replacement did to a node that stays:
// its drain (draining) or its ineligibility. A node that was out of service
// by hand when it was taken (held) stays ineligible: only the drain the
// operator started is stopped, and the eligibility is left as the person set
// it.
func (r *Reconciler) giveBack(ctx context.Context, nodeID string, held, draining bool) error {
	switch {
	case held && draining:
		return r.Nodes.StopDrain(ctx, nodeID)
	case held:
		return nil
	case draining:
		return r.Nodes.CancelDrain(ctx, nodeID)
	default:
		return r.Nodes.SetEligible(ctx, nodeID)
	}
}

// heldNote follows "stays" in an event: a node given back stays ineligible.
func heldNote(held bool) string {
	if held {
		return " ineligible, as it was out of service by hand when it was taken"
	}
	return ""
}

// givenBack ends the message of a node given back after a stuck or stopped
// drain.
func givenBack(held bool) string {
	if held {
		return "; the node stays ineligible, as it was out of service by hand when it was taken"
	}
	return "; the node is eligible again"
}

// deleteInstance deletes the instance of the removal; the removal goes on
// with its node.
func (r *Reconciler) deleteInstance(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	o *observed, m *removal, now time.Time) {

	if err := client.Delete(ctx, group, m.Instance); err != nil {
		r.downFailed(ctx, obj, "deleting an instance failed", m.Instance, err)
		return
	}
	o.drop(m.Instance)
	how := "drained"
	if m.Selector == selectorEmpty {
		how = "found empty"
	}
	r.event(ctx, obj, api.ObjectEventTypeNormal, "InstanceDeleted",
		fmt.Sprintf("instance %s is deleted, its node %s %s", m.Instance, m.Node, how))
	m.Phase, m.Started = phaseDeleted, now
}

// downFailed reports a step of a scale down that failed: in the log, and as
// a warning in the log of the group, so it is seen. The step is tried again at
// the next look.
func (r *Reconciler) downFailed(ctx context.Context, obj *api.CustomObject, what, target string, err error) {
	r.log.Warn(what, "group", obj.Name, "target", target, "error", err)
	r.event(ctx, obj, api.ObjectEventTypeWarning, "ScaleDownFailed", fmt.Sprintf("%s (%s): %v", what, target, err))
}

// isLive reports an instance that is created or being created.
func isLive(inst provider.Instance) bool {
	return inst.State == provider.StateCreating || inst.State == provider.StateRunning
}

// find returns the instance with the provider's ID.
func (o *observed) find(id string) *provider.Instance {
	for i := range o.instances {
		if o.instances[i].ID == id {
			return &o.instances[i]
		}
	}
	return nil
}

// drop removes a deleted instance.
func (o *observed) drop(id string) {
	o.instances = slices.DeleteFunc(o.instances, func(i provider.Instance) bool { return i.ID == id })
}
