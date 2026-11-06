// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
)

// Node is a node of the cluster as the operator sees it.
type Node struct {
	ID   string
	Name string

	// Instance is the name of the instance the node runs on (the meta key
	// norca.instance), and Group its node group (norca.node_group); both
	// empty for a node the operator did not create.
	Instance string
	Group    string

	// Ready is a ready node, Eligible one that takes new work.
	Ready    bool
	Eligible bool

	// CreateIndex tells which of two nodes registered first.
	CreateIndex uint64
}

// NodeLister lists the nodes of a datacenter, pool and class, and does what
// a scale down does to a node.
type NodeLister interface {
	Nodes(ctx context.Context, datacenter, pool, class string) ([]Node, error)
	SetIneligible(ctx context.Context, nodeID string) error

	// Detail reads what a scale down needs to know of a node; a node that
	// does not exist is ErrNodeNotFound.
	Detail(ctx context.Context, nodeID string) (*NodeDetail, error)
	// Drain starts a drain with the deadline (the node becomes ineligible);
	// CancelDrain stops one and makes the node eligible again.
	Drain(ctx context.Context, nodeID string, deadline time.Duration) error
	CancelDrain(ctx context.Context, nodeID string) error
	SetEligible(ctx context.Context, nodeID string) error
	// Purge removes a node that is down from the cluster.
	Purge(ctx context.Context, nodeID string) error
	// Replaceable reports whether the allocation could be placed again
	// without the node: an allocation under distinct_hosts needs another
	// node that holds none of its job (or group). The reason says why not.
	Replaceable(ctx context.Context, alloc NodeAlloc, nodeID string) (bool, string, error)

	// SetMeta changes the dynamic meta of a node: a nil value removes a key.
	SetMeta(ctx context.Context, nodeID string, meta map[string]*string) error
	// AllocServices returns the names of the services an allocation
	// registers (of its group and of its tasks) and the time it keeps
	// running after their deregistration: the larger of the shutdown_delay
	// of its group and of its tasks.
	AllocServices(ctx context.Context, alloc NodeAlloc) ([]string, time.Duration, error)
}

// ErrNodeNotFound is returned for a node that does not exist.
var ErrNodeNotFound = errors.New("node not found")

// NodeDetail is what a scale down reads of a node.
type NodeDetail struct {
	Meta     map[string]string
	Ready    bool
	Eligible bool
	// Draining is a drain in progress; LastDrain is the status of the last
	// one (draining, complete, canceled), empty when there was none.
	Draining  bool
	LastDrain string
	// Allocs are the allocations that have not finished, those of system
	// jobs left out: they run on every node and do not make it busy.
	Allocs []NodeAlloc
}

// NodeAlloc is an allocation on a node.
type NodeAlloc struct {
	ID, Namespace, JobID, Group string
}

// APINodes lists the nodes with the Norca API. The node list has no meta, so
// a node is read once for its marks, which do not change; they are cached by
// node.
type APINodes struct {
	Client *api.Client

	mu    sync.Mutex
	marks map[string][2]string // instance and node group, by node ID
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
		marks, err := n.marksOf(ctx, s.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, Node{
			ID: s.ID, Name: s.Name, Instance: marks[0], Group: marks[1],
			Ready:       s.Status == api.NodeStatusReady,
			Eligible:    s.SchedulingEligibility == api.NodeSchedulingEligible,
			CreateIndex: s.CreateIndex,
		})
	}
	return out, nil
}

// SetIneligible implements NodeLister.
func (n *APINodes) SetIneligible(ctx context.Context, nodeID string) error {
	_, err := n.Client.Nodes().ToggleEligibility(nodeID, false, (&api.WriteOptions{}).WithContext(ctx))
	return err
}

// drainMeta marks the drains the operator starts.
var drainMeta = map[string]string{"norca.nodescaler": "scale-down"}

// Detail implements NodeLister.
func (n *APINodes) Detail(ctx context.Context, nodeID string) (*NodeDetail, error) {
	q := (&api.QueryOptions{}).WithContext(ctx)
	node, _, err := n.Client.Nodes().Info(nodeID, q)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNodeNotFound
		}
		return nil, err
	}
	d := &NodeDetail{
		Meta:     node.Meta,
		Ready:    node.Status == api.NodeStatusReady,
		Eligible: node.SchedulingEligibility == api.NodeSchedulingEligible,
		Draining: node.DrainStrategy != nil,
	}
	if node.LastDrain != nil {
		d.LastDrain = string(node.LastDrain.Status)
	}
	allocs, _, err := n.Client.Nodes().Allocations(nodeID, q)
	if err != nil {
		return nil, err
	}
	for _, a := range allocs {
		if a.DesiredStatus != api.AllocDesiredStatusRun ||
			(a.ClientStatus != api.AllocClientStatusPending && a.ClientStatus != api.AllocClientStatusRunning) {
			continue
		}
		if a.Job != nil && a.Job.Type != nil && (*a.Job.Type == api.JobTypeSystem || *a.Job.Type == api.JobTypeSysbatch) {
			continue
		}
		d.Allocs = append(d.Allocs, NodeAlloc{ID: a.ID, Namespace: a.Namespace, JobID: a.JobID, Group: a.TaskGroup})
	}
	return d, nil
}

// Drain implements NodeLister.
func (n *APINodes) Drain(ctx context.Context, nodeID string, deadline time.Duration) error {
	_, err := n.Client.Nodes().UpdateDrainOpts(nodeID, &api.DrainOptions{
		DrainSpec: &api.DrainSpec{Deadline: deadline},
		Meta:      drainMeta,
	}, (&api.WriteOptions{}).WithContext(ctx))
	return err
}

// CancelDrain implements NodeLister.
func (n *APINodes) CancelDrain(ctx context.Context, nodeID string) error {
	_, err := n.Client.Nodes().UpdateDrainOpts(nodeID, &api.DrainOptions{MarkEligible: true, Meta: drainMeta},
		(&api.WriteOptions{}).WithContext(ctx))
	return err
}

// SetEligible implements NodeLister.
func (n *APINodes) SetEligible(ctx context.Context, nodeID string) error {
	_, err := n.Client.Nodes().ToggleEligibility(nodeID, true, (&api.WriteOptions{}).WithContext(ctx))
	return err
}

// SetMeta implements NodeLister.
func (n *APINodes) SetMeta(ctx context.Context, nodeID string, meta map[string]*string) error {
	_, err := n.Client.Nodes().Meta().Apply(&api.NodeMetaApplyRequest{NodeID: nodeID, Meta: meta},
		(&api.QueryOptions{}).WithContext(ctx))
	return err
}

// AllocServices implements NodeLister.
func (n *APINodes) AllocServices(ctx context.Context, a NodeAlloc) ([]string, time.Duration, error) {
	alloc, _, err := n.Client.Allocations().Info(a.ID, (&api.QueryOptions{Namespace: a.Namespace}).WithContext(ctx))
	if err != nil {
		return nil, 0, err
	}
	if alloc.Job == nil {
		return nil, 0, nil
	}
	var names []string
	var delay time.Duration
	for _, tg := range alloc.Job.TaskGroups {
		if tg == nil || tg.Name == nil || *tg.Name != alloc.TaskGroup {
			continue
		}
		if tg.ShutdownDelay != nil {
			delay = max(delay, *tg.ShutdownDelay)
		}
		for _, s := range tg.Services {
			if s != nil && s.Name != "" {
				names = append(names, s.Name)
			}
		}
		for _, t := range tg.Tasks {
			if t == nil {
				continue
			}
			delay = max(delay, t.ShutdownDelay)
			for _, s := range t.Services {
				if s != nil && s.Name != "" {
					names = append(names, s.Name)
				}
			}
		}
	}
	return names, delay, nil
}

// Purge implements NodeLister.
func (n *APINodes) Purge(ctx context.Context, nodeID string) error {
	_, _, err := n.Client.Nodes().Purge(nodeID, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil && isNotFound(err) {
		return nil
	}
	n.mu.Lock()
	delete(n.marks, nodeID)
	n.mu.Unlock()
	return err
}

// Replaceable implements NodeLister. Only distinct_hosts is checked: the
// allocation needs a node of the job's datacenters and node pool that is
// ready, eligible, not the node that goes, and holds no allocation of the job
// (a constraint of the job) or of its group (a constraint of the group).
// Other constraints of the job are not looked at.
func (n *APINodes) Replaceable(ctx context.Context, alloc NodeAlloc, nodeID string) (bool, string, error) {
	q := (&api.QueryOptions{Namespace: alloc.Namespace}).WithContext(ctx)
	job, _, err := n.Client.Jobs().Info(alloc.JobID, q)
	if err != nil {
		if isNotFound(err) {
			return true, "", nil
		}
		return false, "", err
	}
	byJob := hasDistinctHosts(job.Constraints)
	byGroup := false
	for _, tg := range job.TaskGroups {
		if tg.Name != nil && *tg.Name == alloc.Group {
			byGroup = hasDistinctHosts(tg.Constraints)
		}
	}
	if !byJob && !byGroup {
		return true, "", nil
	}

	allocs, _, err := n.Client.Jobs().Allocations(alloc.JobID, false, q)
	if err != nil {
		return false, "", err
	}
	holding := map[string]bool{}
	for _, a := range allocs {
		if a.DesiredStatus != api.AllocDesiredStatusRun || (a.ClientStatus != api.AllocClientStatusPending && a.ClientStatus != api.AllocClientStatusRunning) {
			continue
		}
		if byJob || a.TaskGroup == alloc.Group {
			holding[a.NodeID] = true
		}
	}
	pool := "default"
	if job.NodePool != nil && *job.NodePool != "" {
		pool = *job.NodePool
	}
	stubs, _, err := n.Client.Nodes().List((&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return false, "", err
	}
	for _, s := range stubs {
		if s.ID == nodeID || holding[s.ID] || s.Status != api.NodeStatusReady || s.SchedulingEligibility != api.NodeSchedulingEligible {
			continue
		}
		if pool != "all" && s.NodePool != pool {
			continue
		}
		for _, dc := range job.Datacenters {
			if ok, _ := path.Match(dc, s.Datacenter); ok {
				return true, "", nil
			}
		}
	}
	return false, fmt.Sprintf("allocation %s of job %s needs a node of its own (distinct_hosts) and no other node is free for it",
		alloc.ID, alloc.JobID), nil
}

// hasDistinctHosts reports whether the constraints ask for distinct hosts.
func hasDistinctHosts(cs []*api.Constraint) bool {
	for _, c := range cs {
		if c != nil && c.Operand == api.ConstraintDistinctHosts && (c.RTarget == "" || c.RTarget == "true") {
			return true
		}
	}
	return false
}

// isStatus reports an API error with the HTTP status code.
func isStatus(err error, code int) bool {
	ue, ok := errors.AsType[api.UnexpectedResponseError](err)
	return ok && ue.StatusCode() == code
}

// isNotFound reports a 404 of the API.
func isNotFound(err error) bool {
	if ue, ok := errors.AsType[api.UnexpectedResponseError](err); ok {
		return ue.StatusCode() == http.StatusNotFound
	}
	return false
}

func (n *APINodes) marksOf(ctx context.Context, nodeID string) ([2]string, error) {
	n.mu.Lock()
	m, ok := n.marks[nodeID]
	n.mu.Unlock()
	if ok {
		return m, nil
	}
	node, _, err := n.Client.Nodes().Info(nodeID, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return [2]string{}, err
	}
	m = [2]string{node.Meta[MetaInstance], node.Meta[MetaNodeGroup]}
	n.mu.Lock()
	if n.marks == nil {
		n.marks = map[string][2]string{}
	}
	n.marks[nodeID] = m
	n.mu.Unlock()
	return m, nil
}
