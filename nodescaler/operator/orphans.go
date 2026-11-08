// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// Orphans. An instance with the labels of this operator (its region and a
// node group) whose node group is gone, or no longer names the provider and
// the group the instance is in, is an orphan: no reconcile of a group sees
// it. The sweep deletes an orphan that has no node once it is older than the
// join deadline. An orphan with a node runs work: it is left as it is and
// reported (an OrphanedInstance event in the log of its NodeProvider), and
// deleted only when the operator runs with -delete-orphans (Config
// DeleteOrphans). Orphans with a node come from a group deleted with the
// deletion policy orphan, or whose finalizer was removed by force.

// SweepOrphans looks at every group of every available provider once and
// deletes the orphans without a node. It returns the number deleted.
func (r *Reconciler) SweepOrphans(ctx context.Context) (int, error) {
	all, err := r.providers.list(ctx)
	if err != nil {
		return 0, err
	}
	now := r.now()
	deleted := 0
	for _, name := range slices.Sorted(maps.Keys(all)) {
		reg := all[name]
		if len(reg.Groups) == 0 || r.providers.available(reg) != nil {
			continue
		}
		client, err := r.providers.clientOf(ctx, reg)
		if err != nil {
			r.log.Warn("orphan sweep: provider skipped", "provider", name, "error", err)
			continue
		}
		left, complete := 0, true
		for _, group := range slices.Sorted(maps.Keys(reg.Groups)) {
			pg, err := client.Group(ctx, group, nil)
			if err != nil {
				r.log.Warn("orphan sweep: group skipped", "provider", name, "group", group, "error", err)
				complete = false
				continue
			}
			for _, inst := range pg.Instances {
				orphan, node := r.orphan(ctx, name, group, inst, now)
				if !orphan {
					continue
				}
				if node != nil && !r.cfg.DeleteOrphans {
					r.reportOrphan(ctx, name, inst, *node)
					left++
					continue
				}
				if err := client.Delete(ctx, group, inst.ID); err != nil {
					r.log.Warn("orphan sweep: deleting an instance failed", "provider", name, "instance", inst.ID, "error", err)
					left++
					continue
				}
				deleted++
				r.addCounter(metricOrphansDeleted, 1, "provider", name)
				r.log.Info("orphaned instance deleted", "provider", name, "group", group, "instance", inst.ID,
					"node_group", inst.Labels[LabelNodeGroup], "with_node", node != nil)
			}
		}
		if complete {
			// A provider not swept to the end keeps the count of its last
			// complete sweep.
			r.setGauge(metricOrphans, float64(left), "provider", name)
		}
	}
	return deleted, nil
}

// The metrics of the orphan sweep, by provider.
const (
	// metricOrphans is the number of orphaned instances the last sweep
	// left: those with a node (without -delete-orphans) and those whose
	// deletion failed.
	metricOrphans = "norca_nodescaler_orphaned_instances"
	// metricOrphansDeleted counts the orphaned instances deleted.
	metricOrphansDeleted = "norca_nodescaler_orphans_deleted_total"
)

func (r *Reconciler) setGauge(name string, v float64, labels ...string) {
	if r.cfg.Metrics != nil {
		r.cfg.Metrics.SetGauge(name, v, labels...)
	}
}

func (r *Reconciler) addCounter(name string, v float64, labels ...string) {
	if r.cfg.Metrics != nil {
		r.cfg.Metrics.AddCounter(name, v, labels...)
	}
}

// orphan reports whether an instance is an orphan: of this region, of a node
// group that is gone or no longer this provider's group, and older than the
// orphan age. The node of the instance is returned when it has one.
func (r *Reconciler) orphan(ctx context.Context, providerName, group string, inst provider.Instance, now time.Time) (bool, *Node) {
	ng := inst.Labels[LabelNodeGroup]
	if ng == "" || inst.Labels[LabelRegion] != r.cfg.Region || inst.State == provider.StateDeleting {
		return false, nil
	}
	if inst.CreatedAt.IsZero() || now.Sub(inst.CreatedAt) < r.cfg.OrphanAfter {
		return false, nil
	}
	obj, err := r.Objects.ReadObject(ctx, NodeGroupKind, "", ng)
	switch {
	case errors.Is(err, controller.ErrNotFound):
	case err != nil:
		return false, nil // not known: nothing is done on a guess
	case obj.DeletionTime != 0:
		return false, nil // the deletion of the group deals with it
	default:
		spec, err := decodeSpec(obj.Spec)
		if err != nil || (spec.Provider == providerName && spec.providerGroup() == group) {
			return false, nil
		}
	}
	nodes, err := r.Nodes.Nodes(ctx, inst.Labels[LabelDatacenter], inst.Labels[LabelPool], inst.Labels[LabelClass])
	if err != nil {
		return false, nil
	}
	for _, n := range nodes {
		if n.Instance != "" && n.Instance == instanceKey(inst) {
			return true, &n
		}
	}
	return true, nil
}

// reportOrphan writes an OrphanedInstance event to the log of the provider:
// the instance runs a node, and is left as it is.
func (r *Reconciler) reportOrphan(ctx context.Context, providerName string, inst provider.Instance, node Node) {
	msg := fmt.Sprintf("instance %s of node group %s, which is gone, runs node %s; it is left as it is "+
		"(delete it, or run the operator with -delete-orphans)", inst.ID, inst.Labels[LabelNodeGroup], node.Name)
	r.log.Warn("orphaned instance has a node, left alone", "provider", providerName, "instance", inst.ID,
		"node", node.ID, "node_group", inst.Labels[LabelNodeGroup])
	err := r.Objects.RecordObjectEvents(ctx, NodeProviderKind, "", providerName,
		[]*api.ObjectEvent{{Type: api.ObjectEventTypeWarning, Reason: "OrphanedInstance", Message: msg}})
	if err != nil {
		r.log.Warn("writing an event failed", "provider", providerName, "reason", "OrphanedInstance", "error", err)
	}
}

// OrphanSweep is a source of the controller that sweeps the orphans every
// Interval. Sources run on the elected leader only.
type OrphanSweep struct {
	Reconciler *Reconciler
	Interval   time.Duration
}

// Name implements controller.Source.
func (s *OrphanSweep) Name() string { return "orphan-instances" }

// Run implements controller.Source.
func (s *OrphanSweep) Run(ctx context.Context, _ *controller.SourceContext) {
	interval := s.Interval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.Reconciler.SweepOrphans(ctx); err != nil {
				s.Reconciler.log.Warn("orphan sweep failed", "error", err)
			}
		}
	}
}
