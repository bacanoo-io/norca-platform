// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca/norca/controller"
)

// Orphans. An instance with the labels of this operator (its region and a
// node group) whose node group is gone, or no longer names the provider and
// the group the instance is in, is an orphan: no reconcile of a group sees
// it. The sweep deletes an orphan that has no node once it is older than the
// join deadline. An orphan with a node is left alone and logged: it runs work,
// and what happens to the nodes of a deleted group is decided by the deletion
// policy of the group, not by the sweep.

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
		for _, group := range slices.Sorted(maps.Keys(reg.Groups)) {
			pg, err := client.Group(ctx, group, nil)
			if err != nil {
				r.log.Warn("orphan sweep: group skipped", "provider", name, "group", group, "error", err)
				continue
			}
			for _, inst := range pg.Instances {
				if !r.orphan(ctx, name, group, inst, now) {
					continue
				}
				if err := client.Delete(ctx, group, inst.ID); err != nil {
					r.log.Warn("orphan sweep: deleting an instance failed", "provider", name, "instance", inst.ID, "error", err)
					continue
				}
				deleted++
				r.log.Info("orphaned instance deleted", "provider", name, "group", group, "instance", inst.ID,
					"node_group", inst.Labels[LabelNodeGroup])
			}
		}
	}
	return deleted, nil
}

// orphan reports whether an instance is an orphan to delete: of this region,
// of a node group that is gone or no longer this provider's group, older
// than the orphan age, and without a node.
func (r *Reconciler) orphan(ctx context.Context, providerName, group string, inst provider.Instance, now time.Time) bool {
	ng := inst.Labels[LabelNodeGroup]
	if ng == "" || inst.Labels[LabelRegion] != r.cfg.Region || inst.State == provider.StateDeleting {
		return false
	}
	if inst.CreatedAt.IsZero() || now.Sub(inst.CreatedAt) < r.cfg.OrphanAfter {
		return false
	}
	obj, err := r.Objects.ReadObject(ctx, NodeGroupKind, "", ng)
	switch {
	case errors.Is(err, controller.ErrNotFound):
	case err != nil:
		return false // not known: nothing is deleted on a guess
	case obj.DeletionTime != 0:
		return false // the deletion of the group deals with it
	default:
		spec, err := decodeSpec(obj.Spec)
		if err != nil || (spec.Provider == providerName && spec.providerGroup() == group) {
			return false
		}
	}
	nodes, err := r.Nodes.Nodes(ctx, inst.Labels[LabelDatacenter], inst.Labels[LabelPool], inst.Labels[LabelClass])
	if err != nil {
		return false
	}
	for _, n := range nodes {
		if n.Instance != "" && n.Instance == instanceKey(inst) {
			r.log.Warn("orphaned instance has a node, left alone", "provider", providerName, "instance", inst.ID,
				"node", n.ID, "node_group", ng)
			return false
		}
	}
	return true
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
