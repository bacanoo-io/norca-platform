// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca/norca/api"
)

// The rolling replacement of nodes. A node the operator created is outdated
// when
//
//   - it carries the meta norca.scale.replace = "true" (marked);
//   - its revision (norca.node_group_revision) is not the one of the template
//     of the group, the provider, its group and spec.node (revision);
//   - its generation (norca.node_group_generation) is below
//     rolling.generation (generation);
//   - it is older than its soft age limit, rolling.max_node_age less a jitter
//     of up to rolling.jitter from a hash of its name (age); without
//     max_node_age the limit is three quarters of the life of its
//     certificate;
//   - its certificate expires within a twelfth of its life (expiry: the hard
//     limit, 55 of 60 days).
//
// A node with norca.scale.protect = "true" is not replaced. At most
// rolling.max_unavailable nodes (1) are replaced at a time, the expiring
// first, then the oldest. With rolling.max_surge >= 1 (the default) the new
// node comes first: the old node is made ineligible and no longer counts in
// the scale, so the operator asks for one more instance, and when the new
// node is ready the old one is emptied. With max_surge = 0 the old node is
// emptied first and its instance replaced once deleted. At most max_surge
// replacements surge at a time; the others remove first. Emptied means drained
// with the drain_deadline of the scale down, except for a node outdated only
// by age, or of a group with the selector empty, which only gets no new work
// and goes when it is empty; a node at its hard limit is always drained. A
// node behind a load balancer leaves it before its drain (edge.go).
//
// The scale and status.replicas do not see a replacement: the old node does
// not count, the new one counts once ready, and the autoscaler does not act
// on them. A refused new node (the provider's quota or capacity) or a new
// node that does not join gives the old node back its eligibility and backs
// the rollout off, as a veto of a scale down does (10 minutes, doubling to 6
// hours); a node at its hard limit is then replaced remove first, and so is a
// node at its soft age limit when the group keeps a headroom of a node or more
// (headroom_nodes, headroom_percent) and nothing has to be drained for it. A
// backed off rollout whose nodes are close to their hard limit raises
// RollBlocked.
//
// A week before its hard limit (an eighth of its life for a short one) a node
// not being replaced raises a Rolling event, once. RollBehind is raised early
// when the queue cannot be done in time: the nodes with a hard limit, in the
// order of their limits, each no sooner than its soft limit, max_unavailable
// at a time, each taking as long as the last replacement took
// (status.rollout.took; the slow_after of the join and the drain deadline
// before the first).
// rolling.mode = "paused" starts no replacement except at the hard limit, and
// enforce = false starts none: the outdated nodes are reported only. The
// state is status.rollout, so another operator instance goes on with it.

const (
	// CondRolling is True while nodes of the group are replaced.
	CondRolling = "Rolling"
	// CondRollBlocked is True while a backed off rollout has a node close to
	// its hard age limit.
	CondRollBlocked = "RollBlocked"
	// CondRollBehind is True when the outdated nodes would not all be
	// replaced before their hard age limits.
	CondRollBehind = "RollBehind"

	reasonMarked     = "marked"
	reasonRevision   = "revision"
	reasonGeneration = "generation"
	reasonAge        = "age"
	reasonExpiry     = "expiry"

	phaseSurging = "surging" // the old node is ineligible, the new one joins
	// phaseLeaving (scaledown.go): the old node leaves the load balancers.
	phaseEmptying = "emptying" // the old node gets no new work, waiting for it to be empty

	// rollWarn is how long before its hard limit a node of a backed off
	// rollout raises RollBlocked (at most a week, or an eighth of its life).
	rollWarn = 7 * 24 * time.Hour
)

// replacement is one node being replaced.
type replacement struct {
	Instance string // the provider's ID of the old instance
	Name     string // its name, the meta norca.instance of its node
	Node     string
	NodeName string
	Reason   string
	Surge    bool
	Drain    bool      // emptied by a drain, not by waiting
	Phase    string    // surging, emptying, draining, deleted
	Since    time.Time // of the replacement
	Started  time.Time // of the phase
	Expires  time.Time // the hard limit of the node, zero when unknown
}

func (m *replacement) doc() map[string]any {
	d := map[string]any{
		"instance": m.Instance, "name": m.Name, "node": m.Node, "nodeName": m.NodeName, "reason": m.Reason,
		"surge": m.Surge, "drain": m.Drain, "phase": m.Phase,
		"since": m.Since.UTC().Format(time.RFC3339), "started": m.Started.UTC().Format(time.RFC3339),
	}
	if !m.Expires.IsZero() {
		d["expires"] = m.Expires.UTC().Format(time.RFC3339)
	}
	return d
}

func readReplacement(d map[string]any) *replacement {
	str := func(k string) string { s, _ := d[k].(string); return s }
	m := &replacement{Instance: str("instance"), Name: str("name"), Node: str("node"), NodeName: str("nodeName"),
		Reason: str("reason"), Phase: str("phase")}
	m.Surge, _ = d["surge"].(bool)
	m.Drain, _ = d["drain"].(bool)
	m.Since, _ = time.Parse(time.RFC3339, str("since"))
	m.Started, _ = time.Parse(time.RFC3339, str("started"))
	m.Expires, _ = time.Parse(time.RFC3339, str("expires"))
	if m.Instance == "" {
		return nil
	}
	return m
}

// rollout is status.rollout.
type rollout struct {
	Replacing []*replacement
	Backoff   downVeto
	Outdated  []string // the names of the outdated nodes not being replaced
	// Due is the soonest hard limit of an outdated node, and Warn when a
	// backed off rollout raises RollBlocked for it: a week before, or an
	// eighth of the life of the node for a short one.
	Due, Warn time.Time
	// Warned are the nodes that raised the Rolling event of their hard limit.
	Warned []string
	// Took is how long the last replacement took.
	Took time.Duration
	// Behind is RollBehind, nil when the nodes were not looked at.
	Behind *condition
}

func readRollout(status map[string]any) *rollout {
	d, _ := status["rollout"].(map[string]any)
	ro := &rollout{}
	list, _ := d["replacing"].([]any)
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			if rep := readReplacement(m); rep != nil {
				ro.Replacing = append(ro.Replacing, rep)
			}
		}
	}
	if b, ok := d["backoff"].(map[string]any); ok {
		ro.Backoff = readVeto(map[string]any{"scaleDown": b})
	}
	ro.Outdated = append(ro.Outdated, asList(d["outdated"])...)
	ro.Warned = asList(d["warned"])
	if s, ok := d["took"].(string); ok {
		ro.Took, _ = time.ParseDuration(s)
	}
	return ro
}

func asList(v any) []string {
	list, _ := v.([]any)
	var out []string
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (ro *rollout) doc() map[string]any {
	if len(ro.Replacing) == 0 && ro.Backoff.Vetoes == 0 && len(ro.Outdated) == 0 && len(ro.Warned) == 0 && ro.Took == 0 {
		return nil
	}
	replacing := make([]any, 0, len(ro.Replacing))
	for _, m := range ro.Replacing {
		replacing = append(replacing, m.doc())
	}
	outdated := make([]any, 0, len(ro.Outdated))
	for _, name := range ro.Outdated {
		outdated = append(outdated, name)
	}
	d := map[string]any{"replacing": replacing, "outdated": outdated}
	if b := ro.Backoff.doc(); b != nil {
		d["backoff"] = b
	}
	if len(ro.Warned) > 0 {
		warned := make([]any, 0, len(ro.Warned))
		for _, name := range ro.Warned {
			warned = append(warned, name)
		}
		d["warned"] = warned
	}
	if ro.Took > 0 {
		d["took"] = ro.Took.Round(time.Second).String()
	}
	return d
}

// retiring reports the instances that no longer count in the scale: the old
// ones of the replacements that surge.
func (ro *rollout) retiring() map[string]bool {
	out := map[string]bool{}
	for _, m := range ro.Replacing {
		if m.Surge && m.Phase != phaseDeleted {
			out[m.Instance] = true
		}
	}
	return out
}

func (ro *rollout) find(instance string) *replacement {
	for _, m := range ro.Replacing {
		if m.Instance == instance {
			return m
		}
	}
	return nil
}

func (ro *rollout) drop(m *replacement) {
	ro.Replacing = slices.DeleteFunc(ro.Replacing, func(x *replacement) bool { return x == m })
}

// conditions are Rolling and RollBlocked of the rollout.
func (ro *rollout) conditions(spec *groupSpec, now time.Time) (condition, condition) {
	rolling := condition{Type: CondRolling, Status: "False", Reason: "UpToDate"}
	switch {
	case len(ro.Replacing) > 0:
		var names []string
		for _, m := range ro.Replacing {
			names = append(names, fmt.Sprintf("%s (%s, %s)", m.NodeName, m.Reason, m.Phase))
		}
		rolling = condition{Type: CondRolling, Status: "True", Reason: "Replacing",
			Message: "replacing " + strings.Join(names, ", ")}
	case len(ro.Outdated) > 0 && !spec.Enforce:
		rolling.Reason, rolling.Message = "DryRun", "outdated, not replaced (enforce = false): "+strings.Join(ro.Outdated, ", ")
	case len(ro.Outdated) > 0 && spec.paused():
		rolling.Reason, rolling.Message = "Paused", "outdated, not replaced (rolling.mode = paused): "+strings.Join(ro.Outdated, ", ")
	case len(ro.Outdated) > 0 && ro.Backoff.Vetoes > 0 && now.Before(ro.Backoff.RetryAfter):
		rolling.Reason, rolling.Message = "BackedOff", fmt.Sprintf("%s; outdated: %s", ro.Backoff.Message, strings.Join(ro.Outdated, ", "))
	case len(ro.Outdated) > 0:
		rolling.Reason, rolling.Message = "Waiting", "outdated: "+strings.Join(ro.Outdated, ", ")
	}
	blocked := condition{Type: CondRollBlocked, Status: "False", Reason: "None"}
	if ro.Backoff.Vetoes > 0 && now.Before(ro.Backoff.RetryAfter) && !ro.Warn.IsZero() && !now.Before(ro.Warn) {
		blocked = condition{Type: CondRollBlocked, Status: "True", Reason: ro.Backoff.Reason,
			Message:    fmt.Sprintf("%s; a node reaches its hard age limit at %s", ro.Backoff.Message, ro.Due.UTC().Format(time.RFC3339)),
			RetryAfter: ro.Backoff.RetryAfter.UTC().Format(time.RFC3339)}
	}
	return rolling, blocked
}

// rollResult is what a look at the rollout gives the status.
type rollResult struct {
	state   *rollout
	requeue time.Duration
}

// outdatedNode is a node to replace.
type outdatedNode struct {
	inst    provider.Instance
	node    Node
	reason  string
	created time.Time
	expires time.Time // the hard limit, zero when unknown
	allocs  []NodeAlloc
}

// roll goes on with the replacements in progress and starts new ones. It
// marks the old instances of surging replacements in o.retiring, so the
// scale does not count them.
func (r *Reconciler) roll(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	spec *groupSpec, o *observed, status map[string]any, failing bool, now time.Time) rollResult {

	ro := readRollout(status)
	res := rollResult{state: ro}
	ro.Outdated, ro.Due, ro.Warn = nil, time.Time{}, time.Time{}
	if ro.Backoff.Vetoes > 0 && !now.Before(ro.Backoff.RetryAfter.Add(vetoMax)) {
		ro.Backoff = downVeto{} // long past: forgotten
	}

	for _, m := range slices.Clone(ro.Replacing) {
		r.continueReplacement(ctx, obj, client, group, spec, o, ro, m, now)
	}
	o.retiring = ro.retiring()
	if len(ro.Replacing) > 0 {
		res.requeue = r.cfg.JoinCheck
	}

	// New replacements: not while the group grows, shrinks or fails to join.
	want := spec.replicas()
	ready, joining, _, live := o.counts()
	if failing || joining > 0 || ready < want || live > want || readRemoval(status) != nil {
		return res
	}
	outdated, ages := r.outdatedNodes(ctx, obj, spec, o, ro, now)
	for _, c := range outdated {
		ro.Outdated = append(ro.Outdated, c.node.Name)
		if !c.expires.IsZero() && (ro.Due.IsZero() || c.expires.Before(ro.Due)) {
			ro.Due = c.expires
			ro.Warn = warnAt(c.created, c.expires)
		}
	}
	r.warnHardLimits(ctx, obj, ro, ages, now)
	backedOff := ro.Backoff.Vetoes > 0 && now.Before(ro.Backoff.RetryAfter)
	ro.Behind = r.behind(ctx, obj, spec, ro, ages, status, backedOff, now)
	if !spec.Enforce {
		if len(outdated) > 0 && !slices.Equal(ro.Outdated, readRollout(status).Outdated) {
			r.event(ctx, obj, api.ObjectEventTypeNormal, "WouldReplace",
				"enforce = false: the outdated nodes are not replaced: "+strings.Join(ro.Outdated, ", "))
		}
		return res
	}
	surging := 0
	for _, m := range ro.Replacing {
		if m.Surge && m.Phase != phaseDeleted {
			surging++
		}
	}
	for _, c := range outdated {
		if len(ro.Replacing) >= spec.maxUnavailable() {
			break
		}
		atLimit := c.reason == reasonExpiry
		// At the soft limit, a refused surge does not make the node wait when
		// the headroom of the group covers it: it is removed first.
		covered := c.reason == reasonAge && spec.headroomNodes() >= 1
		if !atLimit && (spec.paused() || (backedOff && !covered)) {
			continue
		}
		surge := surging < spec.maxSurge() && !backedOff
		if surge {
			surging++
		}
		drain := atLimit || (spec.selector() == selectorLeastBusy && c.reason != reasonAge)
		if !surge && drain && !atLimit {
			if why := r.irreplaceable(ctx, c.allocs, c.node.ID); why != "" {
				continue // nowhere to place its work while it is gone
			}
		}
		m := &replacement{Instance: c.inst.ID, Name: instanceKey(c.inst), Node: c.node.ID, NodeName: c.node.Name,
			Reason: c.reason, Surge: surge, Drain: drain, Since: now, Started: now, Expires: c.expires}
		var err error
		switch {
		case surge:
			m.Phase = phaseSurging
			err = r.Nodes.SetIneligible(ctx, c.node.ID)
		case drain:
			// An edge node leaves the load balancers before the drain.
			m.Phase = phaseLeaving
			if err = r.Nodes.SetIneligible(ctx, c.node.ID); err == nil {
				r.startDrain(ctx, obj, spec, m, now)
			}
		default:
			m.Phase = phaseEmptying
			err = r.Nodes.SetIneligible(ctx, c.node.ID)
		}
		if err != nil {
			r.rollFailed(ctx, obj, "starting a replacement failed", c.node.ID, err)
			break
		}
		how := "a new node is asked for first"
		switch {
		case !surge && backedOff && !atLimit:
			how = fmt.Sprintf("it is removed first: new nodes are backed off (%s), and the headroom of %d nodes covers it",
				ro.Backoff.Reason, spec.headroomNodes())
		case !surge:
			how = "it is removed first"
		}
		r.event(ctx, obj, api.ObjectEventTypeNormal, "Replacing",
			fmt.Sprintf("node %s of instance %s is replaced (%s): %s", c.node.Name, c.inst.ID, c.reason, how))
		ro.Replacing = append(ro.Replacing, m)
		ro.Outdated = slices.DeleteFunc(ro.Outdated, func(n string) bool { return n == c.node.Name })
		res.requeue = r.cfg.JoinCheck
	}
	o.retiring = ro.retiring()
	return res
}

// nodeAge is a node of the group with a hard age limit, not being replaced.
type nodeAge struct {
	name                string
	created, soft, hard time.Time
}

// outdatedNodes lists the ready nodes of the group to replace, those at their
// hard limit first, then the oldest, and the age limits of the nodes that
// have a hard one.
func (r *Reconciler) outdatedNodes(ctx context.Context, obj *api.CustomObject, spec *groupSpec, o *observed,
	ro *rollout, now time.Time) ([]outdatedNode, []nodeAge) {

	var out []outdatedNode
	var ages []nodeAge
	for _, inst := range o.instances {
		node, ok := o.nodeOf[instanceKey(inst)]
		if !isLive(inst) || !ok || !node.Ready || ro.find(inst.ID) != nil {
			continue
		}
		d, err := r.Nodes.Detail(ctx, node.ID)
		if err != nil {
			continue
		}
		if d.Meta[MetaProtect] == "true" {
			continue
		}
		c := outdatedNode{inst: inst, node: node, allocs: d.Allocs, created: inst.CreatedAt}
		if t, err := time.Parse(time.RFC3339, d.Meta[MetaCreatedAt]); err == nil {
			c.created = t
		}
		var certExpires time.Time
		if t, err := time.Parse(time.RFC3339, d.Meta[bootstrap.MetaCertExpires]); err == nil {
			certExpires = t
			if life := t.Sub(c.created); life > 0 {
				c.expires = t.Add(-life / 12)
			}
		}
		c.reason = outdatedReason(spec, d.Meta, instanceKey(inst), c.created, certExpires, c.expires, now)
		if c.reason != "" {
			out = append(out, c)
		}
		if !c.expires.IsZero() {
			soft := c.created.Add(softAge(spec, instanceKey(inst), c.created, certExpires))
			if c.reason != "" && c.reason != reasonAge {
				soft = now // outdated for another reason: due now
			}
			ages = append(ages, nodeAge{name: node.Name, created: c.created, soft: soft, hard: c.expires})
		}
	}
	slices.SortStableFunc(out, func(a, b outdatedNode) int {
		if (a.reason == reasonExpiry) != (b.reason == reasonExpiry) {
			if a.reason == reasonExpiry {
				return -1
			}
			return 1
		}
		return a.created.Compare(b.created)
	})
	return out, ages
}

// outdatedReason says why a node is outdated, or "" when it is not.
func outdatedReason(spec *groupSpec, meta map[string]string, name string, created, certExpires, hard, now time.Time) string {
	switch {
	case !hard.IsZero() && !now.Before(hard):
		return reasonExpiry
	case meta[MetaReplace] == "true":
		return reasonMarked
	case meta[MetaRevision] != "" && meta[MetaRevision] != spec.templateRevision():
		return reasonRevision
	}
	if gen, err := strconv.ParseInt(meta[MetaGeneration], 10, 64); spec.Rolling.Generation > 0 && (err != nil || gen < spec.Rolling.Generation) {
		return reasonGeneration
	}
	if soft := softAge(spec, name, created, certExpires); soft > 0 && !created.IsZero() && now.Sub(created) >= soft {
		return reasonAge
	}
	return ""
}

// softAge is the soft age limit of a node: rolling.max_node_age, or three
// quarters of the life of its certificate, less its jitter; 0 when it has
// none.
func softAge(spec *groupSpec, name string, created, certExpires time.Time) time.Duration {
	maxAge, err := parseDuration(spec.Rolling.MaxNodeAge)
	if (err != nil || maxAge <= 0) && !certExpires.IsZero() {
		maxAge = certExpires.Sub(created) * 3 / 4
	}
	if maxAge <= 0 {
		return 0
	}
	return time.Duration(float64(maxAge) * (1 - spec.jitter()*jitterOf(name)))
}

// warnAt is when a node raises the warnings of its hard limit: a week before
// it, or an eighth of its life for a short one.
func warnAt(created, hard time.Time) time.Time {
	return hard.Add(-min(rollWarn, hard.Sub(created)/8))
}

// warnHardLimits raises a Rolling event, once, for each node that is not
// being replaced and whose hard limit is close: it is drained then.
func (r *Reconciler) warnHardLimits(ctx context.Context, obj *api.CustomObject, ro *rollout, ages []nodeAge, now time.Time) {
	var warned []string
	for _, a := range ages {
		if now.Before(warnAt(a.created, a.hard)) {
			continue
		}
		warned = append(warned, a.name)
		if !slices.Contains(ro.Warned, a.name) {
			r.event(ctx, obj, api.ObjectEventTypeWarning, CondRolling,
				fmt.Sprintf("node %s reaches its hard age limit at %s and is drained then, if it is not replaced before",
					a.name, a.hard.UTC().Format(time.RFC3339)))
		}
	}
	ro.Warned = warned
}

// behind is the condition RollBehind: whether the nodes with a hard limit can
// be replaced before it, max_unavailable at a time. It turning True raises
// an event.
func (r *Reconciler) behind(ctx context.Context, obj *api.CustomObject, spec *groupSpec, ro *rollout, ages []nodeAge,
	status map[string]any, backedOff bool, now time.Time) *condition {

	c := &condition{Type: CondRollBehind, Status: "False", Reason: "OnTime"}
	if !spec.Enforce || spec.paused() {
		c.Reason, c.Message = "NotRolling", "no replacement starts before the hard limit (enforce = false or rolling.mode = paused)"
		return c
	}
	per := ro.Took
	if per <= 0 {
		per = spec.slowAfter() + spec.drainDeadline()
	}
	start := now
	if backedOff {
		start = ro.Backoff.RetryAfter
	}
	late := lateNodes(ages, ro.Replacing, spec.maxUnavailable(), per, start)
	if len(late) == 0 {
		return c
	}
	c.Status, c.Reason = "True", "Behind"
	c.Message = fmt.Sprintf("%d nodes would reach their hard age limit before they are replaced, %d at a time, %s each "+
		"(first: %s at %s); raise rolling.max_unavailable",
		len(late), spec.maxUnavailable(), per.Round(time.Second), late[0].name, late[0].hard.UTC().Format(time.RFC3339))
	if conditionStatusOf(status, CondRollBehind) != "True" {
		r.event(ctx, obj, api.ObjectEventTypeWarning, CondRollBehind, c.Message)
	}
	return c
}

// lateNodes plays the queue of replacements: the nodes in the order of their
// hard limits, each no sooner than its soft limit, on slots replacements at a
// time (the ones in progress hold a slot until they are done), each taking
// per. It returns the nodes that would be done after their hard limit.
func lateNodes(ages []nodeAge, replacing []*replacement, slots int, per time.Duration, start time.Time) []nodeAge {
	free := make([]time.Time, max(slots, 1))
	for i := range free {
		free[i] = start
		if i < len(replacing) {
			if done := replacing[i].Since.Add(per); done.After(start) {
				free[i] = done
			}
		}
	}
	queue := slices.SortedFunc(slices.Values(ages), func(a, b nodeAge) int { return a.hard.Compare(b.hard) })
	var late []nodeAge
	for _, a := range queue {
		k := 0
		for i := range free {
			if free[i].Before(free[k]) {
				k = i
			}
		}
		begin := free[k]
		if a.soft.After(begin) {
			begin = a.soft
		}
		free[k] = begin.Add(per)
		if free[k].After(a.hard) {
			late = append(late, a)
		}
	}
	return late
}

// jitterOf is a fixed number in [0, 1) from the name of a node.
func jitterOf(name string) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return float64(h.Sum32()%10000) / 10000
}

// continueReplacement goes on with one replacement.
func (r *Reconciler) continueReplacement(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	spec *groupSpec, o *observed, ro *rollout, m *replacement, now time.Time) {

	inst := o.find(m.Instance)
	if inst == nil && m.Phase != phaseDeleted {
		// Gone by other hands (a failure, the provider): nothing to replace.
		m.Phase, m.Started = phaseDeleted, now
	}
	switch m.Phase {
	case phaseSurging:
		// The new node: the scale is met without the old one.
		o.retiring = ro.retiring()
		ready, _, _, _ := o.counts()
		if ready >= spec.replicas() {
			r.emptyOld(ctx, obj, spec, m, now)
			return
		}
		if now.Sub(m.Started) > spec.joinDeadline()+drainGrace {
			r.cancelReplacement(ctx, obj, ro, m, "SurgeNotReady",
				fmt.Sprintf("the new node for %s did not join in %s", m.NodeName, spec.joinDeadline()+drainGrace), now)
		}

	case phaseEmptying:
		d, err := r.Nodes.Detail(ctx, m.Node)
		switch {
		case errors.Is(err, ErrNodeNotFound):
			r.deleteOld(ctx, obj, client, group, o, m, now)
		case err != nil:
			r.rollFailed(ctx, obj, "reading a node failed", m.Node, err)
		case len(d.Allocs) == 0:
			r.deleteOld(ctx, obj, client, group, o, m, now)
		case !m.Expires.IsZero() && !now.Before(m.Expires):
			// The hard limit came while it emptied by itself: drained.
			r.event(ctx, obj, api.ObjectEventTypeNormal, "Replacing",
				fmt.Sprintf("node %s reached its hard age limit and is drained", m.NodeName))
			m.Drain, m.Phase, m.Started = true, phaseLeaving, now
			r.startDrain(ctx, obj, spec, m, now)
		}

	case phaseLeaving:
		r.startDrain(ctx, obj, spec, m, now)

	case phaseDraining:
		d, err := r.Nodes.Detail(ctx, m.Node)
		switch {
		case errors.Is(err, ErrNodeNotFound):
			r.deleteOld(ctx, obj, client, group, o, m, now)
		case err != nil:
			r.rollFailed(ctx, obj, "reading a node failed", m.Node, err)
		case !d.Draining && d.LastDrain == string(api.DrainStatusComplete):
			r.deleteOld(ctx, obj, client, group, o, m, now)
		case !d.Draining || now.Sub(m.Started) > spec.drainDeadline()+drainGrace:
			if err := r.Nodes.CancelDrain(ctx, m.Node); err != nil {
				r.rollFailed(ctx, obj, "stopping a drain failed", m.Node, err)
				return
			}
			r.backToTraffic(ctx, m.Node)
			why := fmt.Sprintf("the drain of node %s did not finish in %s", m.NodeName, spec.drainDeadline()+drainGrace)
			if !d.Draining {
				why = fmt.Sprintf("the drain of node %s was stopped (%s)", m.NodeName, d.LastDrain)
			}
			ro.Backoff.block(now, "DrainStuck", why+"; the node is eligible again")
			r.event(ctx, obj, api.ObjectEventTypeWarning, "DrainStuck", ro.Backoff.Message)
			ro.drop(m)
		}

	case phaseDeleted:
		// The old node is purged once down.
		d, err := r.Nodes.Detail(ctx, m.Node)
		switch {
		case errors.Is(err, ErrNodeNotFound):
			ro.drop(m)
		case err != nil:
			r.rollFailed(ctx, obj, "reading a node failed", m.Node, err)
		case !d.Ready || now.Sub(m.Started) > downWait:
			if d.Ready {
				r.log.Warn("the node of a replaced instance is still up, it is left as it is", "group", obj.Name, "node", m.Node)
			} else if err := r.Nodes.Purge(ctx, m.Node); err != nil {
				r.rollFailed(ctx, obj, "purging a node failed", m.Node, err)
				return
			}
			r.event(ctx, obj, api.ObjectEventTypeNormal, "Replaced",
				fmt.Sprintf("node %s (%s) is replaced", m.NodeName, m.Reason))
			ro.Took = now.Sub(m.Since)
			ro.drop(m)
		}
	}
}

// emptyOld starts emptying the old node once the new one is ready.
func (r *Reconciler) emptyOld(ctx context.Context, obj *api.CustomObject, spec *groupSpec, m *replacement, now time.Time) {
	if !m.Drain {
		m.Phase, m.Started = phaseEmptying, now
		return
	}
	m.Phase, m.Started = phaseLeaving, now
	r.startDrain(ctx, obj, spec, m, now)
}

// startDrain drains the old node of a replacement in phase leaving, once it
// left the load balancers (edge.go).
func (r *Reconciler) startDrain(ctx context.Context, obj *api.CustomObject, spec *groupSpec, m *replacement, now time.Time) {
	if done, _ := r.leaveTraffic(ctx, obj, spec, m.Node, m.NodeName, m.Started, now); !done {
		return
	}
	if err := r.Nodes.Drain(ctx, m.Node, spec.drainDeadline()); err != nil {
		r.rollFailed(ctx, obj, "starting a drain failed", m.Node, err)
		return
	}
	m.Phase, m.Started = phaseDraining, now
	r.event(ctx, obj, api.ObjectEventTypeNormal, "Replacing",
		fmt.Sprintf("node %s is drained (deadline %s)", m.NodeName, spec.drainDeadline()))
}

// deleteOld deletes the old instance; the replacement goes on with its node.
func (r *Reconciler) deleteOld(ctx context.Context, obj *api.CustomObject, client *provider.Client, group string,
	o *observed, m *replacement, now time.Time) {

	if err := client.Delete(ctx, group, m.Instance); err != nil {
		r.rollFailed(ctx, obj, "deleting an instance failed", m.Instance, err)
		return
	}
	o.drop(m.Instance)
	r.event(ctx, obj, api.ObjectEventTypeNormal, "InstanceDeleted",
		fmt.Sprintf("instance %s is deleted, its node %s replaced (%s)", m.Instance, m.NodeName, m.Reason))
	m.Phase, m.Started = phaseDeleted, now
}

// cancelReplacement gives the old node back its eligibility and backs the
// rollout off.
func (r *Reconciler) cancelReplacement(ctx context.Context, obj *api.CustomObject, ro *rollout, m *replacement,
	reason, why string, now time.Time) {

	if err := r.Nodes.SetEligible(ctx, m.Node); err != nil && !errors.Is(err, ErrNodeNotFound) {
		r.rollFailed(ctx, obj, "making a node eligible again failed", m.Node, err)
		return
	}
	r.backToTraffic(ctx, m.Node)
	ro.Backoff.block(now, reason, why+"; node "+m.NodeName+" stays and is eligible again")
	r.event(ctx, obj, api.ObjectEventTypeWarning, "ReplacementCanceled", ro.Backoff.Message)
	ro.drop(m)
}

// surgeRefused cancels the replacements whose new node the provider refused.
func (r *Reconciler) surgeRefused(ctx context.Context, obj *api.CustomObject, ro *rollout, why string, now time.Time) {
	for _, m := range slices.Clone(ro.Replacing) {
		if m.Phase == phaseSurging {
			r.cancelReplacement(ctx, obj, ro, m, "SurgeRefused", "the new node was refused: "+why, now)
		}
	}
}

// rollFailed reports a step of a replacement that failed; it is tried again at
// the next look.
func (r *Reconciler) rollFailed(ctx context.Context, obj *api.CustomObject, what, target string, err error) {
	r.log.Warn(what, "group", obj.Name, "target", target, "error", err)
	r.event(ctx, obj, api.ObjectEventTypeWarning, "ReplacementFailed", fmt.Sprintf("%s (%s): %v", what, target, err))
}
