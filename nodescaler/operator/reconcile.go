// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package operator is the node scaling operator (norca-nodescaler): it makes
// the machines of a NodeGroup match its scale. The core decides the scale
// (spec.replicas, written by the built-in autoscaler, an external one or a
// person); the operator asks the group's provider for the instances that are
// missing, matches the nodes that join with their instances, and reports
// what there is in the status of the group: the ready nodes (replicas), the
// instances whose node has not joined (joining), the failed ones, the size of
// a machine (capacity, allocatable), the instances (nodes) and the
// conditions ProviderAvailable and Provisioned.
//
// A request to the provider is idempotent by its ID (region/group/number):
// a request repeated after a failure or a restart creates nothing more. The
// join of the nodes is tracked (join.go), the instances of groups that are
// gone are swept (orphans.go), the instances above the scale are removed one
// at a time (scaledown.go), and a deleted group keeps its object until its
// instances are dealt with by spec.deletion (deletion.go).
package operator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// Config are the settings of the operator.
type Config struct {
	// Region is the region of the cluster: a label of the instances and the
	// first part of the request IDs.
	Region string

	// ProviderTTL is how old the heartbeat of a provider may be (default 3
	// minutes).
	ProviderTTL time.Duration

	// TokenNamespace is the namespace of the Variables with the tokens of the
	// providers (default "default").
	TokenNamespace string

	// AllowHTTP accepts http endpoints of providers (development only).
	AllowHTTP bool

	// HTTP is the client of the calls to the providers; nil uses one with a
	// 30 second timeout.
	HTTP *http.Client

	// RetryAfter is the wait after a failure of the provider (default 30
	// seconds); JoinCheck the wait while instances join (default 15
	// seconds).
	RetryAfter time.Duration
	JoinCheck  time.Duration

	// OrphanAfter is the age after which an orphaned instance without a
	// node is deleted (default 15 minutes, the default join deadline).
	OrphanAfter time.Duration

	// DeleteOrphans deletes orphaned instances that run a node too; without
	// it they are only reported (orphans.go).
	DeleteOrphans bool

	// Metrics receives the operator's own measurements, such as the orphans
	// a sweep found (controller.TextMetrics); nil drops them.
	Metrics OwnMetrics

	// Bootstrap gives the template of the bootstrap of a node; nil sends
	// no bootstrap, only the names of the instances.
	Bootstrap BootstrapSource

	// Network gives the cluster ranges of the region, which the network of
	// a group is checked against; nil leaves the check out.
	Network NetworkSource

	// Join makes the new nodes join with a key of their own (package join):
	// their bootstrap carries a one-time token and the join endpoints, no
	// key and no certificate. nil puts the key and the certificate into the
	// bootstrap. It needs Bootstrap.
	Join JoinIssuer

	Logger *slog.Logger
	Now    func() time.Time
}

// OwnMetrics receives the measurements of the operator itself. The labels
// are name and value pairs.
type OwnMetrics interface {
	SetGauge(name string, v float64, labels ...string)
	AddCounter(name string, v float64, labels ...string)
}

// Reconciler reconciles NodeGroup objects.
type Reconciler struct {
	Objects controller.ObjectsAPI
	Nodes   NodeLister

	cfg       Config
	providers *providers
	log       *slog.Logger
	now       func() time.Time
}

// NewReconciler returns the reconciler of node groups.
func NewReconciler(backend controller.Backend, nodes NodeLister, cfg Config) *Reconciler {
	if cfg.ProviderTTL <= 0 {
		cfg.ProviderTTL = 3 * time.Minute
	}
	if cfg.TokenNamespace == "" {
		cfg.TokenNamespace = "default"
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = 30 * time.Second
	}
	if cfg.JoinCheck <= 0 {
		cfg.JoinCheck = 15 * time.Second
	}
	if cfg.OrphanAfter <= 0 {
		cfg.OrphanAfter = 15 * time.Minute
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Reconciler{
		Objects: backend,
		Nodes:   nodes,
		cfg:     cfg,
		log:     cfg.Logger,
		now:     cfg.Now,
		providers: &providers{
			objs: backend, vars: backend, ttl: cfg.ProviderTTL, tokenNamespace: cfg.TokenNamespace,
			allowHTTP: cfg.AllowHTTP, http: cfg.HTTP, now: cfg.Now, cacheFor: 3 * time.Second,
		},
	}
}

// observed is what the operator found for a group.
type observed struct {
	instances []provider.Instance // of this group
	capacity  *provider.Capacity
	nodeOf    map[string]Node // the joined node of an instance, by instanceKey
	slow      map[string]bool // instances still joining after join.slow_after
	// retiring are the old instances of replacements that surge: they no
	// longer count in the scale (rollout.go).
	retiring map[string]bool
}

// observe takes the instances of the group from the provider's group.
func (r *Reconciler) observe(group string, pg *provider.Group) *observed {
	o := &observed{capacity: pg.Capacity, nodeOf: map[string]Node{}}
	for _, inst := range pg.Instances {
		if inst.Labels[LabelNodeGroup] == group && (inst.Labels[LabelRegion] == "" || inst.Labels[LabelRegion] == r.cfg.Region) {
			o.instances = append(o.instances, inst)
		}
	}
	return o
}

// counts returns the ready nodes, the instances whose node has not joined
// (the slow ones left out: they no longer count as capacity), the failed
// instances and the live instances (creating or running).
func (o *observed) counts() (ready, joining, failed, live int64) {
	for _, inst := range o.instances {
		if o.retiring[inst.ID] {
			continue
		}
		switch inst.State {
		case provider.StateFailed:
			failed++
			continue
		case provider.StateCreating, provider.StateRunning:
			live++
		default:
			continue
		}
		switch n, ok := o.nodeOf[instanceKey(inst)]; {
		case ok && n.Ready:
			ready++
		case !ok && !o.slow[inst.ID]:
			joining++
		}
	}
	return ready, joining, failed, live
}

// Reconcile implements controller.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, key controller.ObjectKey) (time.Duration, error) {
	obj, err := r.Objects.ReadObject(ctx, NodeGroupKind, key.Namespace, key.Name)
	if errors.Is(err, controller.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if obj.DeletionTime != 0 {
		// The finalizer keeps the group until its instances are dealt with
		// by spec.deletion (deletion.go).
		return r.deleteGroup(ctx, obj)
	}
	spec, err := decodeSpec(obj.Spec)
	if err != nil {
		r.log.Warn("node group skipped", "group", obj.Name, "error", err)
		return 0, nil
	}
	if !spec.enabled() || spec.Provider == "" {
		// A disabled group, or one without a provider (an external engine
		// that brings its own machines), is left alone.
		return 0, nil
	}
	if !slices.Contains(obj.Finalizers, Finalizer) {
		// Before the first instance: a deletion of the group waits for the
		// operator from now on.
		// Without the capability (manage-finalizers) the group is still
		// scaled; a deletion then leaves its instances to the orphan sweep.
		withFinalizer, err := r.Objects.UpdateObjectFinalizers(ctx, NodeGroupKind, obj.Namespace, obj.Name, []string{Finalizer}, nil, 0)
		switch {
		case errors.Is(err, controller.ErrConflict) || errors.Is(err, controller.ErrNotFound):
			return 0, nil // deleted meanwhile: the deletion is reconciled next
		case err != nil:
			r.log.Warn("adding the finalizer failed; a deletion of the group will not wait for its instances",
				"group", obj.Name, "error", err)
		default:
			obj = withFinalizer
		}
	}
	group := spec.providerGroup()
	now := r.now()

	client, err := r.providers.client(ctx, spec.Provider, group)
	if un, ok := errors.AsType[*errUnavailable](err); ok {
		return r.unavailable(ctx, obj, un.reason, un.message)
	}
	if err != nil {
		return 0, err
	}
	pg, err := client.Group(ctx, group, spec.Node.Parameters)
	if errors.Is(err, provider.ErrUnknownGroup) {
		return r.unavailable(ctx, obj, "UnknownGroup", fmt.Sprintf("provider %q has no group %q", spec.Provider, group))
	}
	if err != nil {
		return r.unavailable(ctx, obj, "ProviderUnavailable", fmt.Sprintf("provider %q: %v", spec.Provider, err))
	}

	o := r.observe(obj.Name, pg)
	if r.cfg.Join != nil {
		live := make(map[string]bool, len(o.instances))
		for _, inst := range o.instances {
			live[instanceKey(inst)] = true
		}
		if err := r.cfg.Join.Prune(ctx, obj.Name, instancePrefix(obj.Name), live); err != nil {
			r.log.Warn("pruning the join records failed", "group", obj.Name, "error", err)
		}
		r.recordSeen(ctx, obj.Name, o.instances)
	}
	nodes, err := r.Nodes.Nodes(ctx, spec.Datacenter, spec.Pool, spec.NodeClass)
	if err != nil {
		return 0, fmt.Errorf("listing the nodes of node group %s: %w", obj.Name, err)
	}
	prevStatus := normalize(obj.Status)
	// The node of an instance a scale down or a replacement deleted is not a
	// stray: it goes down and is purged.
	removing := map[string]bool{}
	if m := readRemoval(prevStatus); m != nil {
		removing[m.Name] = true
	}
	for _, m := range readRollout(prevStatus).Replacing {
		removing[m.Name] = true
	}
	// Nor is the node of an instance deleted for a join failure.
	for name := range readJoinState(prevStatus).deletedNames() {
		removing[name] = true
	}
	strays := matchNodes(obj.Name, o, nodes, removing)

	// Nodes without an instance: reported, the foreign ones made ineligible.
	strayRecords, foreignCond := r.handleStrays(ctx, obj, strays, prevStatus)
	netCond := r.networkCondition(ctx, spec)
	if netCond.Status == "False" && conditionStatusOf(prevStatus, CondNetworkCovered) != "False" {
		r.event(ctx, obj, api.ObjectEventTypeWarning, "NetworkNotCovered", netCond.Message)
	}

	// The join of the nodes: failed and late instances are deleted, slow ones
	// marked.
	js := r.trackJoins(ctx, obj, client, group, spec, o, prevStatus, now)

	// The instances of nodes that have been down for long are replaced.
	repairCond := r.repairDown(ctx, obj, client, group, spec, o, &js, now)
	if repairCond.Status == "True" && conditionStatusOf(prevStatus, CondRepairHeld) != "True" {
		r.event(ctx, obj, api.ObjectEventTypeWarning, CondRepairHeld, repairCond.Message)
	}
	r.purgeDeleted(ctx, obj, &js, now)
	failing := js.Failures >= spec.maxFailures()
	if failing && readJoinState(prevStatus).Failures < spec.maxFailures() {
		r.event(ctx, obj, api.ObjectEventTypeWarning, CondJoinFailing, joinCondition(js, spec.maxFailures()).Message)
	}

	// Replace the outdated nodes; the old node of a replacement that surges
	// no longer counts in the scale.
	roll := r.roll(ctx, obj, client, group, spec, o, prevStatus, failing, now)

	// Remove the instances above the scale, one at a time.
	down := r.scaleDown(ctx, obj, client, group, spec, o, prevStatus, len(roll.state.Replacing) > 0, now)

	// Ask for the instances that are missing.
	want := spec.replicas()
	_, _, _, live := o.counts()
	seq := r.nextRequest(obj, o.instances)
	provisioned := condition{Type: CondProvisioned, Status: "True", Reason: "Requested",
		Message: fmt.Sprintf("%d instances for a scale of %d", live, want)}
	requested, createFailed := false, false
	if failing && want > live {
		provisioned = condition{Type: CondProvisioned, Status: "False", Reason: CondJoinFailing,
			Message: fmt.Sprintf("%d instances for a scale of %d; none is asked for while the nodes do not join", live, want)}
	}
	if !failing && want > live {
		reqID := r.requestID(obj.Name, seq)
		instances, issued, err := r.describe(ctx, obj, spec, seq, int(want-live), now)
		if err != nil {
			createFailed = true
			provisioned = condition{Type: CondProvisioned, Status: "False", Reason: "NoBootstrap", Message: err.Error()}
			r.event(ctx, obj, api.ObjectEventTypeWarning, "BootstrapFailed", err.Error())
		} else {
			req := provider.CreateRequest{
				RequestID:  reqID,
				Instances:  instances,
				Parameters: spec.Node.Parameters,
				Labels: map[string]string{
					LabelRegion: r.cfg.Region, LabelPool: spec.Pool, LabelDatacenter: spec.Datacenter,
					LabelClass: spec.NodeClass, LabelNodeGroup: obj.Name, LabelRequest: reqID,
				},
			}
			created, err := client.Create(ctx, group, req)
			if err != nil {
				createFailed = true
				reason := provider.ReasonOf(err)
				provisioned = condition{Type: CondProvisioned, Status: "False", Reason: reasonName(reason),
					Message: fmt.Sprintf("provider %q could not create %d instances: %v", spec.Provider, len(req.Instances), err)}
				r.event(ctx, obj, api.ObjectEventTypeWarning, "CreateFailed", provisioned.Message)
				// A new node of a replacement that is refused: the old node
				// stays.
				r.surgeRefused(ctx, obj, roll.state, provisioned.Message, now)
				o.retiring = roll.state.retiring()
			} else {
				requested = true
				// The addresses of the new instances, before their nodes
				// join: the endpoint waits for them.
				r.recordSeen(ctx, obj.Name, created)
				for _, inst := range created {
					if slices.ContainsFunc(o.instances, func(i provider.Instance) bool { return i.ID == inst.ID }) {
						continue // an instance of a repeated request
					}
					o.instances = append(o.instances, inst)
					if rd, ok := issued[inst.Name]; ok {
						r.event(ctx, obj, api.ObjectEventTypeNormal, "CertificateIssued",
							fmt.Sprintf("certificate %s of node %s, valid until %s", rd.Serial, inst.Name, rd.NotAfter.UTC().Format(time.RFC3339)))
					}
				}
				_, _, _, live = o.counts()
				provisioned.Message = fmt.Sprintf("%d instances for a scale of %d", live, want)
				r.event(ctx, obj, api.ObjectEventTypeNormal, "InstancesRequested",
					fmt.Sprintf("asked provider %q for %d instances (request %s)", spec.Provider, len(req.Instances), req.RequestID))
			}
		}
	}

	ready, joining, failed, _ := o.counts()
	err = r.writeStatus(ctx, obj, func(status map[string]any) {
		status["replicas"] = ready
		status["joining"] = joining
		status["failed"] = failed
		status["nodes"] = nodeRecords(o, down.removal, roll.state)
		if d := roll.state.doc(); d != nil {
			status["rollout"] = d
		} else {
			delete(status, "rollout")
		}
		rolling, rollBlocked := roll.state.conditions(spec, now)
		setCondition(status, rolling, now)
		setCondition(status, rollBlocked, now)
		if roll.state.Behind != nil {
			// Kept as it was while the group grows or shrinks.
			setCondition(status, *roll.state.Behind, now)
		}
		if down.removal != nil {
			status["removal"] = down.removal.doc()
		} else {
			delete(status, "removal")
		}
		if v := down.veto.doc(); v != nil {
			status["scaleDown"] = v
		} else {
			delete(status, "scaleDown")
		}
		setCondition(status, down.veto.condition(now), now)
		if o.capacity != nil {
			status["capacity"] = map[string]any{"cpu": o.capacity.CPU, "memory": o.capacity.Memory, "disk": o.capacity.Disk}
			status["allocatable"] = allocatable(*o.capacity, spec.Node.Reserved)
		}
		if requested {
			status["lastRequest"] = seq
		}
		status["join"] = js.doc()
		status["strayNodes"] = strayRecords
		setCondition(status, foreignCond, now)
		setCondition(status, netCond, now)
		setCondition(status, repairCond, now)
		setCondition(status, joinCondition(js, spec.maxFailures()), now)
		status["observedGeneration"] = obj.Generation
		setCondition(status, condition{Type: CondProviderAvailable, Status: "True", Reason: "Available"}, now)
		setCondition(status, provisioned, now)
	})
	if err != nil {
		return 0, err
	}
	var requeue time.Duration
	switch {
	case createFailed:
		// The failure is in the condition and the event; the provider is
		// asked again after a while, not at the pace of the error backoff.
		requeue = r.cfg.RetryAfter
	case joining > 0 || len(o.slow) > 0:
		requeue = r.cfg.JoinCheck
	}
	for _, d := range []time.Duration{down.requeue, roll.requeue} {
		if d > 0 && (requeue == 0 || d < requeue) {
			requeue = d
		}
	}
	if v := down.veto; v.Vetoes > 0 && now.Before(v.RetryAfter) && requeue == 0 {
		// The condition turns False at retryAfter.
		requeue = v.RetryAfter.Sub(now)
	}
	return requeue, nil
}

// describe names the instances of a request and renders the bootstrap of
// each, with the marks of the operator in the meta of its node. Without a
// bootstrap source the instances have names only (a provider that brings its
// own machines, tests).
func (r *Reconciler) describe(ctx context.Context, obj *api.CustomObject, spec *groupSpec, seq int64, count int,
	now time.Time) ([]provider.InstanceRequest, map[string]*bootstrap.Rendered, error) {

	var tmpl *bootstrap.Template
	if r.cfg.Bootstrap != nil {
		var err error
		if tmpl, err = r.cfg.Bootstrap.Template(ctx); err != nil {
			return nil, nil, fmt.Errorf("the bootstrap of the nodes is not available: %w", err)
		}
	}
	out := make([]provider.InstanceRequest, 0, count)
	issued := map[string]*bootstrap.Rendered{}
	for i := 1; i <= count; i++ {
		name := instanceName(obj.Name, seq, i)
		ir := provider.InstanceRequest{Name: name}
		if tmpl != nil {
			meta := maps.Clone(spec.Node.Meta)
			if meta == nil {
				meta = map[string]string{}
			}
			maps.Copy(meta, map[string]string{
				MetaCreatedBy: "nodescaler", MetaNodeGroup: obj.Name, MetaInstance: name,
				MetaRequest: r.requestID(obj.Name, seq), MetaCreatedAt: now.UTC().Format(time.RFC3339),
				MetaRevision: spec.templateRevision(), MetaGeneration: strconv.FormatInt(spec.Rolling.Generation, 10),
			})
			node := bootstrap.Node{Name: name, Datacenter: spec.Datacenter, Pool: spec.Pool, Class: spec.NodeClass, Meta: meta}
			if r.cfg.Join != nil {
				// The node makes its key and fetches its certificate.
				ticket, err := r.cfg.Join.Ticket(ctx, obj.Name, name, spec.joinDeadline()+joinSlack)
				if err != nil {
					return nil, nil, fmt.Errorf("the join of %s: %w", name, err)
				}
				if spec.Join.KeyStore != "" {
					ticket.KeyStore = spec.Join.KeyStore
				}
				rd, err := tmpl.RenderJoin(node, ticket)
				if err != nil {
					return nil, nil, err
				}
				ir.Bootstrap = &provider.Bootstrap{UserData: rd.UserData}
				out = append(out, ir)
				continue
			}
			rd, err := tmpl.Render(node)
			if err != nil {
				return nil, nil, err
			}
			ir.Bootstrap = &provider.Bootstrap{UserData: rd.UserData}
			issued[name] = rd
		}
		out = append(out, ir)
	}
	return out, issued, nil
}

// instanceName is the name of the i-th instance of a request: the group, the
// number of the request and i, as a host name (lower case letters, digits and
// dashes, at most 63 characters).
func instanceName(group string, seq int64, i int) string {
	suffix := fmt.Sprintf("-%d-%d", seq, i)
	var b strings.Builder
	for _, c := range strings.ToLower(group) {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			b.WriteRune(c)
		default:
			b.WriteByte('-')
		}
	}
	base := strings.Trim(b.String(), "-")
	if base == "" {
		base = "node"
	}
	if max := 63 - len(suffix); len(base) > max {
		base = strings.TrimRight(base[:max], "-")
	}
	return base + suffix
}

// instanceKey is how an instance is matched with its node: the name the
// operator gave it, or the provider's ID for an instance without one.
func instanceKey(inst provider.Instance) string {
	if inst.Name != "" {
		return inst.Name
	}
	return inst.ID
}

// unavailable reports a provider that can't be used and tries again later.
func (r *Reconciler) unavailable(ctx context.Context, obj *api.CustomObject, reason, message string) (time.Duration, error) {
	now := r.now()
	err := r.writeStatus(ctx, obj, func(status map[string]any) {
		status["observedGeneration"] = obj.Generation
		setCondition(status, condition{Type: CondProviderAvailable, Status: "False", Reason: reason, Message: message}, now)
	})
	return r.cfg.RetryAfter, err
}

// requestID is the ID of the request with the number.
func (r *Reconciler) requestID(group string, seq int64) string {
	return r.cfg.Region + "/" + group + "/" + strconv.FormatInt(seq, 10)
}

// nextRequest is the number of the next request: one more than the last in
// the status and the last one an instance was created by. When a request
// created instances and its number could not be written to the status, the
// instances still carry it, so a later request gets a new number; when the
// instances are not listed yet, the number is used again, and the provider
// answers with the instances of the first request.
func (r *Reconciler) nextRequest(obj *api.CustomObject, instances []provider.Instance) int64 {
	last := lastRequest(normalize(obj.Status))
	prefix := r.cfg.Region + "/" + obj.Name + "/"
	for _, inst := range instances {
		if n, err := strconv.ParseInt(strings.TrimPrefix(inst.RequestID, prefix), 10, 64); err == nil &&
			strings.HasPrefix(inst.RequestID, prefix) && n > last {
			last = n
		}
	}
	return last + 1
}

// nodeRecords is status.nodes: an entry per instance with its node, the phase
// of the one a scale down removes (removing) and of those being replaced
// (replacing).
func nodeRecords(o *observed, m *removal, ro *rollout) []any {
	insts := slices.Clone(o.instances)
	slices.SortFunc(insts, func(a, b provider.Instance) int { return strings.Compare(a.ID, b.ID) })
	out := make([]any, 0, len(insts))
	for _, inst := range insts {
		rec := map[string]any{
			"instance": inst.ID, "state": inst.State, "request": inst.RequestID,
			"created": inst.CreatedAt.UTC().Format(time.RFC3339),
		}
		if inst.Reason != "" {
			rec["reason"] = inst.Reason
		}
		if n, ok := o.nodeOf[instanceKey(inst)]; ok {
			rec["node"] = n.ID
			rec["ready"] = n.Ready
		}
		if o.slow[inst.ID] {
			rec["slow"] = true
		}
		if m != nil && m.Instance == inst.ID {
			rec["removing"] = m.Phase
		}
		if ro != nil {
			if rep := ro.find(inst.ID); rep != nil {
				rec["replacing"] = rep.Phase
			}
		}
		out = append(out, rec)
	}
	return out
}

// allocatable is the capacity less the reserved resources.
func allocatable(c provider.Capacity, reserved *nodeResource) map[string]any {
	var res nodeResource
	if reserved != nil {
		res = *reserved
	}
	return map[string]any{
		"cpu":    max(c.CPU-res.CPU, 0),
		"memory": max(c.Memory-res.Memory, 0),
		"disk":   max(c.Disk-res.Disk, 0),
	}
}

// reasonName is the CamelCase reason of a condition for a provider reason.
func reasonName(reason string) string {
	switch reason {
	case provider.ReasonQuota:
		return "Quota"
	case provider.ReasonNoCapacity:
		return "NoCapacity"
	case provider.ReasonInvalid:
		return "Invalid"
	}
	return "ProviderError"
}

// event writes an event to the log of the group; a failure is logged only.
func (r *Reconciler) event(ctx context.Context, obj *api.CustomObject, typ, reason, message string) {
	err := r.Objects.RecordObjectEvents(ctx, NodeGroupKind, obj.Namespace, obj.Name,
		[]*api.ObjectEvent{{Type: typ, Reason: reason, Message: message}})
	if err != nil {
		r.log.Warn("writing an event failed", "group", obj.Name, "reason", reason, "error", err)
	}
}
