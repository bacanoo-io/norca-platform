// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// Cluster is what the operator reads of the cluster besides objects.
type Cluster interface {
	// NamespaceMeta returns the metadata of a namespace; an error wrapping
	// controller.ErrNotFound if it does not exist.
	NamespaceMeta(ctx context.Context, namespace string) (map[string]string, error)
}

// reconciler keeps the record sets of the DNSRecord objects.
type reconciler struct {
	ctrl    *controller.Controller
	cluster Cluster
	zones   *zones
	logger  *slog.Logger
	now     func() time.Time

	// namespace is the namespace of the records the operator keeps ("*":
	// every namespace).
	namespace string

	// requeueReady is how often a record that is as it should be is looked
	// at again, which finds changes made in the zone by hand.
	requeueReady time.Duration

	// requeueWaiting is how soon a record that waits for something that
	// produces no event (a zone's heartbeat, the namespace's allow list) is
	// looked at again.
	requeueWaiting time.Duration
}

// outcome is what a reconcile found, for the status.
type outcome struct {
	ready           bool
	reason, message string
	conflict        string

	// evented says the reconcile recorded the event of the outcome itself.
	evented bool
}

func (r *reconciler) Reconcile(ctx context.Context, key controller.ObjectKey) (time.Duration, error) {
	obj, ok := r.ctrl.Get(key)
	if !ok {
		r.ctrl.Recorder().Forget(key)
		return 0, nil
	}
	var status Status
	_ = obj.DecodeStatus(&status)

	if obj.Deleting() {
		return r.finishDeletion(ctx, obj, &status)
	}
	if !obj.HasFinalizer(Finalizer) {
		next, err := r.ctrl.Objects().AddFinalizer(ctx, obj, Finalizer)
		if err != nil {
			if errors.Is(err, controller.ErrConflict) || errors.Is(err, controller.ErrNotFound) {
				return time.Second, nil
			}
			return 0, err
		}
		obj = next
	}

	// keep compares with the generation the operator acted on last; the
	// status observes the current one when it is written.
	out, requeue, err := r.keep(ctx, obj, &status)
	if err != nil && out.reason == "" {
		out = outcome{reason: "Failed", message: err.Error()}
	}
	// A failure is an outcome too: the status says it before the reconcile
	// is tried again, or a record that can't be written stays Ready.
	if werr := r.writeStatus(ctx, obj, &status, out); werr != nil && err == nil {
		return 0, werr
	}
	if err != nil {
		return 0, err
	}
	return requeue, nil
}

// keep brings the record set of an object to what its spec says, and
// returns what it found and when to look again.
func (r *reconciler) keep(ctx context.Context, obj *controller.Object, status *Status) (outcome, time.Duration, error) {
	// A problem of the spec or of the zone is an outcome for the status,
	// not an error of the reconcile: it is not retried until something
	// changes.
	var spec Spec
	if msg := problem(obj.DecodeSpec(&spec)); msg != "" {
		return outcome{reason: "InvalidSpec", message: msg}, 0, nil
	}
	if msg := problem(spec.normalize()); msg != "" {
		return outcome{reason: "InvalidSpec", message: msg}, 0, nil
	}

	meta, err := r.cluster.NamespaceMeta(ctx, obj.Key.Namespace)
	if err != nil && !errors.Is(err, controller.ErrNotFound) {
		return outcome{}, 0, fmt.Errorf("reading the namespace %s: %w", obj.Key.Namespace, err)
	}
	if !allowed(spec.Name, meta) {
		return outcome{reason: "NotAllowed", message: fmt.Sprintf(
			"the namespace %q may not write %s: its allow list (namespace metadata %s) is %q",
			obj.Key.Namespace, spec.Name, MetaDNSSuffixes, meta[MetaDNSSuffixes])}, r.requeueWaiting, nil
	}

	other, err := r.holder(ctx, obj, spec, status)
	if err != nil {
		return outcome{}, 0, err
	}
	if other != "" {
		// The record lost the name: it holds no record set from here on.
		// The set of the contested name and type is the other record's and
		// stays; a set it wrote under the name its spec had before goes.
		if status.Owned && status.Name == spec.Name && status.Type == spec.Type {
			status.Owned = false
		} else if err := r.release(ctx, obj, status, fmt.Sprintf("the record moved to %s %s", spec.Type, spec.Name)); err != nil {
			return outcome{reason: "Failed", message: err.Error(), evented: true}, 0, err
		}
		msg := fmt.Sprintf("the DNSRecord %s writes %s %s", other, spec.Type, spec.Name)
		return outcome{reason: ConditionConflict, message: msg, conflict: msg}, r.requeueWaiting, nil
	}

	values, why, err := r.values(ctx, obj, spec)
	if err != nil {
		return outcome{}, 0, err
	}
	if why != "" {
		return outcome{reason: "NoValues", message: why}, r.requeueWaiting, nil
	}

	zone, err := r.zones.find(ctx, spec.Name)
	switch {
	case errors.Is(err, errNoZone):
		return outcome{reason: "ZoneNotFound", message: fmt.Sprintf("no registered DNS zone holds %s", spec.Name)}, r.requeueWaiting, nil
	case errors.Is(err, errRecordsDisabled):
		return outcome{reason: "RecordsNotEnabled", message: err.Error()}, r.requeueWaiting, nil
	case errors.Is(err, errZoneAway):
		return outcome{reason: "ZoneUnavailable", message: err.Error()}, r.requeueWaiting, nil
	case err != nil:
		return outcome{}, 0, err
	}
	want, checkErr := dns.CheckRecordSet(zone.Name, dns.RecordSet{Name: spec.Name, Type: spec.Type, TTL: spec.TTL, Values: values})
	if msg := problem(checkErr); msg != "" {
		return outcome{reason: "InvalidSpec", message: msg}, 0, nil
	}

	// A set the operator wrote under another name, type or zone goes
	// first: the spec moved it.
	if status.Owned && (status.Name != want.Name || status.Type != want.Type || status.Zone != zone.Name) {
		if err := r.release(ctx, obj, status, fmt.Sprintf("the record moved to %s %s", want.Type, want.Name)); err != nil {
			return outcome{reason: "Failed", message: err.Error(), evented: true}, 0, err
		}
	}

	client, clientErr := r.zones.provider(ctx, zone)
	if msg := problem(clientErr); msg != "" {
		return outcome{reason: "ZoneUnavailable", message: msg}, r.requeueWaiting, nil
	}
	sets, err := client.Records(ctx, want.Name)
	if err != nil {
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "Failed", "reading the zone: "+err.Error())
		return outcome{reason: "Failed", message: "reading the zone: " + err.Error(), evented: true}, 0, err
	}
	var have *dns.RecordSet
	for i := range sets {
		if strings.EqualFold(sets[i].Type, want.Type) {
			set, err := dns.CheckRecordSet(zone.Name, sets[i])
			if err == nil {
				have = &set
			} else {
				have = &sets[i]
			}
		}
	}
	owned := status.Owned && status.Name == want.Name && status.Type == want.Type && status.Zone == zone.Name

	switch {
	case have != nil && !owned && !spec.Adopt && !dns.SameRecords(*have, want):
		msg := fmt.Sprintf("the zone %s has %s records of %s that norca-dns did not write (%s); set adopt = true to take them over",
			zone.Name, want.Type, want.Name, strings.Join(have.Values, ", "))
		return outcome{reason: ConditionConflict, message: msg, conflict: msg}, r.requeueWaiting, nil

	case have != nil && dns.SameRecords(*have, want):
		// As it should be; a set that was there already as wanted is the
		// operator's from now on.

	default:
		if err := client.SetRecords(ctx, want); err != nil {
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "Failed", err.Error())
			var re *dns.ResponseError
			if errors.As(err, &re) && re.Invalid() {
				return outcome{reason: "Failed", message: err.Error(), evented: true}, r.requeueWaiting, nil
			}
			return outcome{reason: "Failed", message: err.Error(), evented: true}, 0, err
		}
		now := r.now().UTC().Format(time.RFC3339)
		status.WrittenAt = now
		switch {
		case owned && obj.Generation != 0 && status.ObservedGeneration == obj.Generation &&
			status.TTL == want.TTL && slices.Equal(status.Values, want.Values):
			// Nothing of the record changed on the operator's side: the
			// zone was changed (or the set deleted) by hand.
			found := "deleted"
			if have != nil {
				found = strings.Join(have.Values, ", ")
			}
			status.Drifts++
			status.LastDrift = now
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "DriftCorrected",
				fmt.Sprintf("%s %s was changed outside norca-dns (%s) and was set back", want.Type, want.Name, found))
		case have != nil && !owned:
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "Adopted",
				fmt.Sprintf("%s %s taken over: %s", want.Type, want.Name, strings.Join(want.Values, ", ")))
		default:
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "RecordWritten",
				fmt.Sprintf("%s %s: %s (TTL %d)", want.Type, want.Name, strings.Join(want.Values, ", "), want.TTL))
		}
	}

	status.Zone, status.Name, status.Type, status.TTL = zone.Name, want.Name, want.Type, want.TTL
	status.Values = want.Values
	status.Owned = true
	return outcome{ready: true, reason: "RecordWritten",
		message: fmt.Sprintf("the zone %s holds %s %s", zone.Name, want.Type, want.Name)}, r.requeueReady, nil
}

// holder returns the namespace/name of another DNSRecord that keeps this
// record from the name and type of its spec, or "".
//
// The record whose status holds the record set keeps it, whichever record
// is older: a record that is deleted holds its set until the operator has
// removed it. A set nobody holds yet goes to the oldest record that may
// write the name: a record of a namespace whose allow list does not hold
// the name claims nothing.
func (r *reconciler) holder(ctx context.Context, obj *controller.Object, spec Spec, status *Status) (string, error) {
	mine := status.Owned && status.Name == spec.Name && status.Type == spec.Type
	var owner, first *controller.Object
	for _, other := range r.ctrl.Informer().List() {
		if other.Key == obj.Key {
			continue
		}
		if holds(other, spec.Name, spec.Type) {
			if owner == nil || before(other, owner) {
				owner = other
			}
			continue
		}
		if mine || other.Deleting() || !before(other, obj) || (first != nil && !before(other, first)) {
			continue
		}
		var os Spec
		if other.DecodeSpec(&os) != nil || os.normalize() != nil || os.Name != spec.Name || os.Type != spec.Type {
			continue
		}
		meta, err := r.cluster.NamespaceMeta(ctx, other.Key.Namespace)
		if err != nil && !errors.Is(err, controller.ErrNotFound) {
			return "", fmt.Errorf("reading the namespace %s: %w", other.Key.Namespace, err)
		}
		if allowed(os.Name, meta) {
			first = other
		}
	}
	switch {
	case owner != nil && (!mine || before(owner, obj)):
		// Two records that both hold the set (written at the same moment):
		// the older one keeps it.
		return owner.Key.Namespace + "/" + owner.Key.Name, nil
	case owner == nil && first != nil:
		return first.Key.Namespace + "/" + first.Key.Name, nil
	}
	return "", nil
}

// holds reports whether the status of a record says the operator holds the
// record set of the name and type for it. A deleted record holds it while
// the finalizer of the operator is on it.
func holds(o *controller.Object, name, recordType string) bool {
	if o.Deleting() && !o.HasFinalizer(Finalizer) {
		return false
	}
	var st Status
	return o.DecodeStatus(&st) == nil && st.Owned && st.Name == name && st.Type == recordType
}

// before reports whether a record came before another one.
func before(a, b *controller.Object) bool {
	return a.CreateIndex < b.CreateIndex || (a.CreateIndex == b.CreateIndex && keyLess(a.Key, b.Key))
}

func keyLess(a, b controller.ObjectKey) bool {
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	return a.Name < b.Name
}

// values returns the values of a record: from the spec, or from its
// source. why says why there are none yet.
func (r *reconciler) values(ctx context.Context, obj *controller.Object, spec Spec) (values []string, why string, err error) {
	if spec.Source == nil || spec.Source.LoadBalancer == "" {
		return spec.Values, "", nil
	}
	lb, err := r.ctrl.Backend().ReadObject(ctx, LoadBalancerKind, obj.Key.Namespace, spec.Source.LoadBalancer)
	if errors.Is(err, controller.ErrNotFound) {
		return nil, fmt.Sprintf("the LoadBalancer %q of the namespace does not exist", spec.Source.LoadBalancer), nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("reading the LoadBalancer %s: %w", spec.Source.LoadBalancer, err)
	}
	var st struct {
		Addresses []string `json:"addresses"`
	}
	_ = decode(lb.Status, &st)
	values = loadBalancerValues(spec.Type, st.Addresses)
	if len(values) == 0 {
		return nil, fmt.Sprintf("the LoadBalancer %q has no %s address yet (addresses: %s)",
			spec.Source.LoadBalancer, spec.Type, strings.Join(st.Addresses, ", ")), nil
	}
	return values, "", nil
}

// release removes the record set the status of a record holds and gives it
// up; why is for the event. A record that holds none is left as it is.
func (r *reconciler) release(ctx context.Context, obj *controller.Object, status *Status, why string) error {
	if !status.Owned {
		return nil
	}
	kept, err := r.remove(ctx, obj, status)
	if err != nil {
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "DeleteFailed", err.Error())
		return err
	}
	if kept != "" {
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "RecordLeft",
			fmt.Sprintf("%s %s stays in the zone %s: %s", status.Type, status.Name, status.Zone, kept))
	} else {
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "RecordDeleted",
			fmt.Sprintf("%s %s removed from the zone %s: %s", status.Type, status.Name, status.Zone, why))
	}
	status.Owned = false
	return nil
}

// remove deletes the record set a status says the operator wrote for obj.
// kept says why the set stays in the zone instead: the status of another
// record holds it too (two records that wrote it at the same moment), or
// its zone is no longer registered and can't be written.
func (r *reconciler) remove(ctx context.Context, obj *controller.Object, status *Status) (kept string, err error) {
	namespace := r.namespace
	if namespace == "" {
		namespace = "*"
	}
	// Read from the servers, not from the cache: a set that is deleted
	// under a record that holds it is an outage of its name.
	others, _, err := r.ctrl.Objects().List(ctx, namespace)
	if err != nil {
		return "", fmt.Errorf("listing the DNS records: %w", err)
	}
	for _, other := range others {
		if other.Key == obj.Key || other.Deleting() {
			continue
		}
		var st Status
		if other.DecodeStatus(&st) == nil && st.Owned && st.Zone == status.Zone && st.Name == status.Name && st.Type == status.Type {
			return fmt.Sprintf("the DNSRecord %s/%s holds it", other.Key.Namespace, other.Key.Name), nil
		}
	}

	zone, err := r.zones.byName(ctx, status.Zone)
	if err != nil {
		return "", err
	}
	if zone == nil {
		r.logger.Warn("the zone of a record is no longer registered: its record set stays", "zone", status.Zone,
			"name", status.Name, "type", status.Type)
		return "the zone is no longer registered", nil
	}
	client, err := r.zones.provider(ctx, zone)
	if err != nil {
		return "", err
	}
	return "", client.DeleteRecords(ctx, status.Name, status.Type)
}

// finishDeletion removes the record set of a deleted object and lets the
// object go.
func (r *reconciler) finishDeletion(ctx context.Context, obj *controller.Object, status *Status) (time.Duration, error) {
	if !obj.HasFinalizer(Finalizer) {
		return 0, nil
	}
	if err := r.release(ctx, obj, status, "the record was deleted"); err != nil {
		return 0, err
	}
	_, err := r.ctrl.Objects().RemoveFinalizer(ctx, obj, Finalizer)
	if errors.Is(err, controller.ErrNotFound) {
		return 0, nil
	}
	return 0, err
}

// writeStatus writes the conditions of an outcome.
func (r *reconciler) writeStatus(ctx context.Context, obj *controller.Object, status *Status, out outcome) error {
	status.Observe(obj)
	now := r.now()
	ready := controller.ConditionFalse
	if out.ready {
		ready = controller.ConditionTrue
	}
	status.SetCondition(controller.Condition{Type: controller.ConditionReady, Status: ready, Reason: out.reason, Message: out.message}, now)
	conflict := controller.ConditionFalse
	if out.conflict != "" {
		conflict = controller.ConditionTrue
	}
	status.SetCondition(controller.Condition{Type: ConditionConflict, Status: conflict, Reason: ConditionConflict, Message: out.conflict}, now)
	degraded := controller.ConditionTrue
	if out.ready {
		degraded = controller.ConditionFalse
	}
	status.SetCondition(controller.Condition{Type: controller.ConditionDegraded, Status: degraded, Reason: out.reason}, now)
	if !out.ready && !out.evented {
		if c := previousReady(obj); c == nil || c.Reason != out.reason {
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, out.reason, out.message)
		}
	}
	_, err := r.ctrl.Objects().UpdateStatus(ctx, obj, status)
	if errors.Is(err, controller.ErrNotFound) || errors.Is(err, controller.ErrConflict) {
		return nil
	}
	return err
}

// previousReady returns the Ready condition of the stored status.
func previousReady(obj *controller.Object) *controller.Condition {
	var st Status
	if obj.DecodeStatus(&st) != nil {
		return nil
	}
	return st.Condition(controller.ConditionReady)
}

// loadBalancerSource reconciles the records with a load balancer source
// when a load balancer changes.
func loadBalancerSource() controller.Source {
	return &controller.EventSource{
		SourceName: "loadbalancers",
		Topics:     map[api.Topic][]string{api.TopicCustomObject: {LoadBalancerKind.Group + "/" + LoadBalancerKind.Kind}},
		Map: func(_ *api.Event, sc *controller.SourceContext) []controller.ObjectKey {
			var keys []controller.ObjectKey
			for _, obj := range sc.Objects() {
				var spec Spec
				if obj.DecodeSpec(&spec) == nil && spec.Source != nil && spec.Source.LoadBalancer != "" {
					keys = append(keys, obj.Key)
				}
			}
			return keys
		},
	}
}

// zoneSource reconciles every record when a DNS zone changes (registered,
// its records switch, its heartbeat back).
func zoneSource(z *zones) controller.Source {
	return &controller.EventSource{
		SourceName: "zones",
		Topics:     map[api.Topic][]string{api.TopicCustomObject: {ZoneKind.Group + "/" + ZoneKind.Kind}},
		Map: func(_ *api.Event, sc *controller.SourceContext) []controller.ObjectKey {
			z.forget()
			var keys []controller.ObjectKey
			for _, obj := range sc.Objects() {
				keys = append(keys, obj.Key)
			}
			return keys
		},
	}
}

// forget drops the cached zones.
func (z *zones) forget() {
	z.mu.Lock()
	z.cached = nil
	z.mu.Unlock()
}

// problem returns the message of an error that is an outcome, or "".
func problem(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
