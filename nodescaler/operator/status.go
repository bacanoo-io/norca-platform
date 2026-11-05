// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// The conditions the operator sets on a NodeGroup.
const (
	// CondProviderAvailable: the provider is registered, alive, offers the
	// group and answers.
	CondProviderAvailable = "ProviderAvailable"

	// CondProvisioned: the instances the scale asks for are requested.
	// False with the reason of the provider when it can't create them.
	CondProvisioned = "Provisioned"
)

// maxStatusRetries bounds the writes of a status that lose to another writer.
const maxStatusRetries = 5

// condition is an entry of status.conditions.
type condition struct {
	Type, Status, Reason, Message string
}

// writeStatus writes the fields the operator owns into the status of the
// object. The status is shared: the autoscaler writes status.decision, and a
// status write replaces the whole status, so the fields are set on the
// status as it is read, and written with the index it was read at; a write
// that loses is done again on the object read again.
func (r *Reconciler) writeStatus(ctx context.Context, obj *api.CustomObject, set func(status map[string]any)) error {
	for attempt := 0; ; attempt++ {
		status := normalize(obj.Status)
		set(status)
		status = normalize(status)
		if reflect.DeepEqual(status, normalize(obj.Status)) {
			return nil
		}
		_, err := r.Objects.UpdateObjectStatus(ctx, NodeGroupKind, obj.Namespace, obj.Name, status, obj.ModifyIndex)
		if err == nil || !errors.Is(err, controller.ErrConflict) || attempt >= maxStatusRetries {
			if err != nil {
				return fmt.Errorf("writing the status of node group %s: %w", obj.Name, err)
			}
			return nil
		}
		fresh, err := r.Objects.ReadObject(ctx, NodeGroupKind, obj.Namespace, obj.Name)
		if err != nil {
			return err
		}
		if fresh.ID != obj.ID {
			return fmt.Errorf("node group %s was re-created: %w", obj.Name, controller.ErrNotFound)
		}
		obj = fresh
	}
}

// normalize returns a copy of a JSON document as the API returns it
// (numbers as float64), so documents can be compared.
func normalize(doc map[string]any) map[string]any {
	if doc == nil {
		return map[string]any{}
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return maps.Clone(doc)
	}
	var out map[string]any
	if json.Unmarshal(data, &out) != nil || out == nil {
		return maps.Clone(doc)
	}
	return out
}

// setCondition sets a condition in the status. The transition time stays
// when the status of the condition does not change.
func setCondition(status map[string]any, c condition, now time.Time) {
	list, _ := status["conditions"].([]any)
	entry := map[string]any{
		"type": c.Type, "status": c.Status, "reason": c.Reason,
		"lastTransitionTime": now.UTC().Format(time.RFC3339),
	}
	if c.Message != "" {
		entry["message"] = c.Message
	}
	for i, item := range list {
		old, ok := item.(map[string]any)
		if !ok || old["type"] != c.Type {
			continue
		}
		if old["status"] == c.Status {
			if t, ok := old["lastTransitionTime"].(string); ok {
				entry["lastTransitionTime"] = t
			}
		}
		list[i] = entry
		status["conditions"] = list
		return
	}
	status["conditions"] = append(list, entry)
}

// lastRequest is the number of the last request the operator made for the
// group (status.lastRequest).
func lastRequest(status map[string]any) int64 {
	if v, ok := status["lastRequest"].(float64); ok {
		return int64(v)
	}
	return 0
}

// conditionStatusOf returns the status of a condition of a status.
func conditionStatusOf(status map[string]any, typ string) string {
	list, _ := status["conditions"].([]any)
	for _, item := range list {
		if m, ok := item.(map[string]any); ok && m["type"] == typ {
			s, _ := m["status"].(string)
			return s
		}
	}
	return ""
}
