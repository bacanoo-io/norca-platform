// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"encoding/json"
	"fmt"

	"github.com/bacanoo-io/norca/norca/controller"
)

// The kinds the operator works with, registered by the core.
var (
	NodeGroupKind    = controller.Kind{Group: "norca.io", Kind: "NodeGroup"}
	NodeProviderKind = controller.Kind{Group: "norca.io", Kind: "NodeProvider"}
)

// The labels a provider puts on an instance, and the meta keys a node of the
// operator carries.
const (
	LabelRegion     = "norca.region"
	LabelPool       = "norca.pool"
	LabelDatacenter = "norca.datacenter"
	LabelClass      = "norca.class"
	LabelNodeGroup  = "norca.node_group"
	LabelRequest    = "norca.request"

	// MetaProviderID is the meta key of a node with the ID of its instance:
	// how a node is matched with the instance it runs on.
	MetaProviderID = "norca.provider_id"
)

// nodeResource is a size: CPU in MHz, memory and disk in MB.
type nodeResource struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
	Disk   int64 `json:"disk"`
}

// groupSpec is the part of the spec of a NodeGroup the operator reads.
type groupSpec struct {
	Pool       string `json:"pool"`
	Datacenter string `json:"datacenter"`
	NodeClass  string `json:"node_class"`
	Enabled    *bool  `json:"enabled"`
	Replicas   *int64 `json:"replicas"`
	Min        int64  `json:"min"`
	Max        int64  `json:"max"`
	Provider   string `json:"provider"`
	Group      string `json:"group"`
	Node       struct {
		Meta       map[string]string `json:"meta"`
		Reserved   *nodeResource     `json:"reserved"`
		Capacity   *nodeResource     `json:"capacity"`
		Parameters map[string]any    `json:"parameters"`
	} `json:"node"`
	Join struct {
		Deadline string `json:"deadline"`
	} `json:"join"`
}

// decodeSpec reads the spec of a NodeGroup.
func decodeSpec(spec map[string]any) (*groupSpec, error) {
	data, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	var s groupSpec
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("the spec of the node group is not readable: %w", err)
	}
	return &s, nil
}

// enabled reports whether the group is not disabled.
func (s *groupSpec) enabled() bool { return s.Enabled == nil || *s.Enabled }

// replicas is the scale: spec.replicas, or min when it is not set.
func (s *groupSpec) replicas() int64 {
	if s.Replicas != nil {
		return *s.Replicas
	}
	return s.Min
}

// providerGroup is the group (zone) of the provider: spec.group, or the
// datacenter when it is not set.
func (s *groupSpec) providerGroup() string {
	if s.Group != "" {
		return s.Group
	}
	return s.Datacenter
}
