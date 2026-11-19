// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"github.com/bacanoo-io/norca/norca/controller"
)

// watchHandovers lets the records that wait for a name and type hear when
// another record gives them up (review finding MUK-12): a record that
// releases its record set, is deleted, or moves its spec to another name.
// The informer calls the handler after its cache holds the change, so the
// records it enqueues see it in holder. Only the records of the operator's
// own namespace (or of every namespace with -namespace "*") are kept by
// this informer; a record that waits for one of another namespace comes
// back after requeueConflict.
func (r *reconciler) watchHandovers() {
	r.ctrl.Informer().AddHandler(controller.EventHandler{
		OnUpdate: func(oldObj, newObj *controller.Object) { r.handOver(oldObj, newObj) },
		OnDelete: func(obj *controller.Object) { r.handOver(obj, nil) },
	})
}

// handOver enqueues the records that claim a name and type that old held or
// claimed and next (nil: deleted) no longer does.
func (r *reconciler) handOver(old, next *controller.Object) {
	type nameType struct{ name, recordType string }
	var freed []nameType

	var st Status
	if old.DecodeStatus(&st) == nil && st.Owned && (next == nil || !holds(next, st.Name, st.Type)) {
		freed = append(freed, nameType{st.Name, st.Type})
	}
	var oldSpec Spec
	if old.DecodeSpec(&oldSpec) == nil && oldSpec.normalize() == nil {
		var nextSpec Spec
		if next == nil || next.Deleting() || next.DecodeSpec(&nextSpec) != nil || nextSpec.normalize() != nil ||
			nextSpec.Name != oldSpec.Name || nextSpec.Type != oldSpec.Type {
			freed = append(freed, nameType{oldSpec.Name, oldSpec.Type})
		}
	}
	if len(freed) == 0 {
		return
	}

	for _, other := range r.ctrl.Informer().List() {
		if other.Key == old.Key || other.Deleting() {
			continue
		}
		var spec Spec
		if other.DecodeSpec(&spec) != nil || spec.normalize() != nil {
			continue
		}
		for _, f := range freed {
			if spec.Name == f.name && spec.Type == f.recordType {
				r.ctrl.Enqueue(other.Key)
				break
			}
		}
	}
}
