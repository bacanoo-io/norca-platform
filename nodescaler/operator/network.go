// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/bacanoo-io/norca/norca/controller"
)

// The network of a node group. A node must reach the RPC and Serf ports of
// the servers, and the firewall of the servers must let it in: the range of
// a group (spec.network) must be inside the cluster ranges of the region. The
// operator does not know the firewall; the deployer publishes the ranges in
// the Variable nodescaler/network/<region> (items cluster_cidrs and
// api_cidrs). The check is a condition: a group whose range is not covered
// still gets its instances, whose nodes will not join.

// CondNetworkCovered says whether the firewall of the servers covers the
// network of the group.
const CondNetworkCovered = "NetworkCovered"

// NetworkSource reads the cluster ranges of the region.
type NetworkSource interface {
	// ClusterRanges returns the ranges, or ok false when they are not
	// published.
	ClusterRanges(ctx context.Context) (ranges []netip.Prefix, ok bool, err error)
}

// VariableNetwork reads the ranges from the Variable the deployer writes.
type VariableNetwork struct {
	Vars      controller.VariablesAPI
	Namespace string
	Path      string
}

// ClusterRanges implements NetworkSource. Entries that are not a range or an
// address are left out.
func (v *VariableNetwork) ClusterRanges(ctx context.Context) ([]netip.Prefix, bool, error) {
	vr, err := v.Vars.ReadVariable(ctx, v.Namespace, v.Path)
	if errors.Is(err, controller.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading the Variable %q: %w", v.Path, err)
	}
	var out []netip.Prefix
	for item := range strings.SplitSeq(vr.Items["cluster_cidrs"], ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if p, err := netip.ParsePrefix(item); err == nil {
			out = append(out, p.Masked())
		} else if a, err := netip.ParseAddr(item); err == nil {
			out = append(out, netip.PrefixFrom(a, a.BitLen()))
		}
	}
	return out, true, nil
}

// covers reports whether the range is inside one of the ranges.
func covers(ranges []netip.Prefix, network netip.Prefix) bool {
	for _, r := range ranges {
		if r.Bits() <= network.Bits() && r.Contains(network.Addr()) {
			return true
		}
	}
	return false
}

// networkCondition checks the network of a group against the cluster ranges.
func (r *Reconciler) networkCondition(ctx context.Context, spec *groupSpec) condition {
	c := condition{Type: CondNetworkCovered, Status: "Unknown"}
	if spec.Network == "" {
		c.Reason, c.Message = "NoNetwork", "the group names no network (spec.network)"
		return c
	}
	network, err := netip.ParsePrefix(spec.Network)
	if err != nil {
		c.Reason, c.Message = "InvalidNetwork", fmt.Sprintf("spec.network %q is not a range", spec.Network)
		return c
	}
	network = network.Masked()
	if r.cfg.Network == nil {
		c.Reason, c.Message = "NetworkUnchecked", "the operator has no source of the cluster ranges"
		return c
	}
	ranges, ok, err := r.cfg.Network.ClusterRanges(ctx)
	switch {
	case err != nil:
		c.Reason, c.Message = "NetworkUnchecked", err.Error()
	case !ok || len(ranges) == 0:
		c.Reason, c.Message = "NetworkUnchecked", "the cluster ranges of the region are not published (the node_scaling of the deployer)"
	case covers(ranges, network):
		c.Status, c.Reason, c.Message = "True", "Covered", fmt.Sprintf("%s is inside the cluster ranges", network)
	default:
		var list []string
		for _, p := range ranges {
			list = append(list, p.String())
		}
		c.Status, c.Reason = "False", "NotCovered"
		c.Message = fmt.Sprintf("%s is not inside the cluster ranges (%s): the firewall of the servers will not let its nodes in",
			network, strings.Join(list, ", "))
	}
	return c
}
