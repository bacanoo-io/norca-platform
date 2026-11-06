// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

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

	// MetaInstance is the meta key of a node with the name of its instance
	// (the name the operator gave it in the request): how a node is matched
	// with the instance it runs on. The provider's ID of the instance is not
	// known when the machine is described, so the name is the operator's.
	MetaInstance = "norca.instance"

	// The other marks of a node the operator created.
	MetaCreatedBy = "norca.created_by"
	MetaNodeGroup = "norca.node_group"
	MetaRequest   = "norca.request"
	MetaCreatedAt = "norca.created_at"
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
	Network    string `json:"network"`
	Node       struct {
		Meta       map[string]string `json:"meta"`
		Reserved   *nodeResource     `json:"reserved"`
		Capacity   *nodeResource     `json:"capacity"`
		Parameters map[string]any    `json:"parameters"`
	} `json:"node"`
	Join struct {
		Deadline         string `json:"deadline"`
		SlowAfter        string `json:"slow_after"`
		MaxFailures      int    `json:"max_failures"`
		ResumeGeneration int64  `json:"resume_generation"`
	} `json:"join"`
	Policy struct {
		ScaleDown struct {
			Selector      string `json:"selector"`
			DrainDeadline string `json:"drain_deadline"`
		} `json:"scale_down"`
	} `json:"policy"`
	Deletion struct {
		Policy       string `json:"policy"`
		DrainTimeout string `json:"drain_timeout"`
	} `json:"deletion"`
}

// deletionPolicy is what the deletion of the group does with its instances:
// drain (the default), orphan or terminate.
func (s *groupSpec) deletionPolicy() string {
	switch s.Deletion.Policy {
	case deletionOrphan, deletionTerminate:
		return s.Deletion.Policy
	}
	return deletionDrain
}

// drainTimeout is how long the deletion of a group may wait for its nodes
// before DeletionBlocked asks a person (deletion.drain_timeout, 1 hour by
// default).
func (s *groupSpec) drainTimeout() time.Duration {
	return durationOr(s.Deletion.DrainTimeout, time.Hour)
}

// selector chooses the node of a scale down: least_busy (the default) or
// empty.
func (s *groupSpec) selector() string {
	if s.Policy.ScaleDown.Selector == selectorEmpty {
		return selectorEmpty
	}
	return selectorLeastBusy
}

// drainDeadline is the deadline of the drain of a scale down
// (scale_down.drain_deadline, 15 minutes by default).
func (s *groupSpec) drainDeadline() time.Duration {
	return durationOr(s.Policy.ScaleDown.DrainDeadline, 15*time.Minute)
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

// joinDeadline is how long an instance may live without its node joining
// (join.deadline, 15 minutes by default).
func (s *groupSpec) joinDeadline() time.Duration {
	return durationOr(s.Join.Deadline, 15*time.Minute)
}

// slowAfter is when a node still joining no longer counts as capacity
// (join.slow_after, 5 minutes by default).
func (s *groupSpec) slowAfter() time.Duration {
	return durationOr(s.Join.SlowAfter, 5*time.Minute)
}

// maxFailures is the number of join failures in a row after which the group
// stops asking for instances (join.max_failures, 3 by default).
func (s *groupSpec) maxFailures() int {
	if s.Join.MaxFailures > 0 {
		return s.Join.MaxFailures
	}
	return 3
}

// templateRevision identifies the template of a machine: the provider, its
// group and spec.node. A change of it is a new chance for a group whose
// nodes did not join.
func (s *groupSpec) templateRevision() string {
	data, _ := json.Marshal(map[string]any{"provider": s.Provider, "group": s.providerGroup(), "node": s.Node})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:6])
}

func durationOr(s string, def time.Duration) time.Duration {
	if d, err := parseDuration(s); err == nil && d > 0 {
		return d
	}
	return def
}

// parseDuration reads a duration with s, m, h or d, as the schema of the
// kind allows (a day is 24 hours).
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	if n := len(s); n > 1 && s[n-1] == 'd' {
		var days int64
		if _, err := fmt.Sscanf(s[:n-1], "%d", &days); err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
