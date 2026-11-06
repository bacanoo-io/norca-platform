// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// Edge nodes. A node that runs an instance of a service exposed through a
// load balancer (a norca.io/LoadBalancer object whose spec.service it
// registers) leaves the traffic before it is drained:
//
//  1. the node is made ineligible and marked with the meta
//     norca.lb.leaving = "true";
//  2. the LoadBalancer operator drops the instances on a marked node from
//     the targets and lists the node in status.leavingNodes once its provider
//     applied that;
//  3. when every such load balancer lists the node (or after the drain
//     deadline), the node is drained; the shutdown_delay of the task covers
//     what the platform of the load balancer still needs (its health checks).
//
// A node that is not removed after all (a cancelled scale down or
// replacement, a stuck drain) loses the mark and gets the traffic back. An
// instance whose shutdown_delay is below edge.min_shutdown_delay (30s) raises
// ShortShutdownDelay: the proxy may stop before the platform stops sending.

const (
	// MetaLeaving marks a node that leaves the load balancers.
	MetaLeaving = "norca.lb.leaving"

	// leaveCheck is how often a node leaving the load balancers is looked at.
	leaveCheck = 5 * time.Second
)

// LoadBalancerKind is the kind of the load balancers of the LoadBalancer
// operator.
var LoadBalancerKind = controller.Kind{Group: "norca.io", Kind: "LoadBalancer"}

// balancer is a load balancer with an instance on the node.
type balancer struct {
	key     string // namespace/name
	leaving []string
}

// edgeBalancers returns the load balancers with an instance on the node, and
// the instances whose shutdown_delay is shorter than min.
func (r *Reconciler) edgeBalancers(ctx context.Context, allocs []NodeAlloc, min time.Duration) ([]balancer, []string, error) {
	if len(allocs) == 0 {
		return nil, nil, nil
	}
	objs, _, err := r.Objects.ListObjects(ctx, LoadBalancerKind, "*", 0)
	switch {
	case errors.Is(err, controller.ErrNotFound) || isStatus(err, http.StatusNotFound):
		return nil, nil, nil // no load balancer operator in this cluster
	case isStatus(err, http.StatusForbidden):
		// The policy of the operator does not let it read the load
		// balancers: the node is drained without waiting for them.
		r.log.Warn("the load balancers can't be read; edge nodes are drained without leaving them first", "error", err)
		return nil, nil, nil
	case err != nil:
		return nil, nil, fmt.Errorf("listing the load balancers: %w", err)
	}
	if len(objs) == 0 {
		return nil, nil, nil
	}
	var out []balancer
	var short []string
	seen := map[string]bool{}
	for _, a := range allocs {
		services, delay, err := r.Nodes.AllocServices(ctx, a)
		if err != nil {
			return nil, nil, fmt.Errorf("reading allocation %s: %w", a.ID, err)
		}
		exposed := false
		for _, o := range objs {
			svc, _ := o.Spec["service"].(string)
			if o.Namespace != a.Namespace || svc == "" || !slices.Contains(services, svc) {
				continue
			}
			exposed = true
			key := o.Namespace + "/" + o.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			b := balancer{key: key}
			if st := normalize(o.Status); st != nil {
				b.leaving = asList(st["leavingNodes"])
			}
			out = append(out, b)
		}
		if exposed && delay < min {
			short = append(short, fmt.Sprintf("%s/%s group %s (shutdown_delay %s)", a.Namespace, a.JobID, a.Group, delay))
		}
	}
	slices.SortFunc(out, func(x, y balancer) int { return strings.Compare(x.key, y.key) })
	return out, uniq(short), nil
}

// leaveTraffic takes a node out of the load balancers before its drain. It
// reports whether the drain may start: the node is not behind a load
// balancer, every load balancer dropped it, or it waited longer than the
// drain deadline since since. Otherwise it says what it waits for.
func (r *Reconciler) leaveTraffic(ctx context.Context, obj *api.CustomObject, spec *groupSpec, nodeID, nodeName string,
	since, now time.Time) (bool, string) {

	d, err := r.Nodes.Detail(ctx, nodeID)
	if errors.Is(err, ErrNodeNotFound) {
		return true, ""
	}
	if err != nil {
		r.log.Warn("reading a node failed", "group", obj.Name, "node", nodeID, "error", err)
		return now.Sub(since) > spec.drainDeadline(), fmt.Sprintf("node %s: %v", nodeName, err)
	}
	balancers, short, err := r.edgeBalancers(ctx, d.Allocs, spec.minShutdownDelay())
	if err != nil {
		r.log.Warn("finding the load balancers of a node failed", "group", obj.Name, "node", nodeID, "error", err)
		return now.Sub(since) > spec.drainDeadline(), fmt.Sprintf("node %s: %v", nodeName, err)
	}
	if len(balancers) == 0 {
		return true, ""
	}
	if d.Meta[MetaLeaving] != "true" {
		if err := r.Nodes.SetMeta(ctx, nodeID, map[string]*string{MetaLeaving: new("true")}); err != nil {
			r.log.Warn("marking a node as leaving failed", "group", obj.Name, "node", nodeID, "error", err)
			return now.Sub(since) > spec.drainDeadline(), fmt.Sprintf("node %s: %v", nodeName, err)
		}
		var keys []string
		for _, b := range balancers {
			keys = append(keys, b.key)
		}
		r.event(ctx, obj, api.ObjectEventTypeNormal, "LeavingTraffic",
			fmt.Sprintf("node %s leaves the load balancers %s before its drain", nodeName, strings.Join(keys, ", ")))
		for _, s := range short {
			r.event(ctx, obj, api.ObjectEventTypeWarning, "ShortShutdownDelay",
				fmt.Sprintf("%s is behind a load balancer; below %s the proxy may stop before the platform stops sending to it",
					s, spec.minShutdownDelay()))
		}
	}
	var pending []string
	for _, b := range balancers {
		if !slices.Contains(b.leaving, nodeID) {
			pending = append(pending, b.key)
		}
	}
	if len(pending) == 0 {
		r.event(ctx, obj, api.ObjectEventTypeNormal, "TrafficLeft",
			fmt.Sprintf("the load balancers dropped node %s; it is drained", nodeName))
		return true, ""
	}
	if now.Sub(since) > spec.drainDeadline() {
		r.event(ctx, obj, api.ObjectEventTypeWarning, "LeaveTimeout",
			fmt.Sprintf("the load balancers %s still forward to node %s after %s; it is drained anyway",
				strings.Join(pending, ", "), nodeName, spec.drainDeadline()))
		return true, ""
	}
	return false, fmt.Sprintf("the load balancers %s to drop node %s", strings.Join(pending, ", "), nodeName)
}

// backToTraffic removes the leaving mark of a node that stays.
func (r *Reconciler) backToTraffic(ctx context.Context, nodeID string) {
	d, err := r.Nodes.Detail(ctx, nodeID)
	if err != nil || d.Meta[MetaLeaving] == "" {
		return
	}
	if err := r.Nodes.SetMeta(ctx, nodeID, map[string]*string{MetaLeaving: nil}); err != nil {
		r.log.Warn("removing the leaving mark of a node failed", "node", nodeID, "error", err)
	}
}
