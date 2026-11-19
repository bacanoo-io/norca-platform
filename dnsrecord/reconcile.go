// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
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

	// requeueConflict is how soon a record that an operator of one namespace
	// keeps is looked at again while a record of another namespace holds or
	// claims its name: the informer of this operator does not see that
	// record give the name up (MUK-12). A record of its own informer that
	// gives it up enqueues the waiting records at once (watchHandovers).
	requeueConflict time.Duration

	// unconditionalZones are the zones whose provider refused a
	// conditional write, as the log said once.
	unconditionalZones sync.Map
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
	if _, ok := r.ctrl.Get(key); !ok {
		r.ctrl.Recorder().Forget(key)
		return 0, nil
	}
	// What the record holds is in its status, and the cache can be behind
	// the status the reconcile before this one wrote (a spec changed while
	// it ran starts this one at once): a reconcile that acted on the old
	// status would leave the set of a former name in the zone for good.
	// So the object is read from the servers.
	obj, err := r.ctrl.Objects().Get(ctx, key)
	if errors.Is(err, controller.ErrNotFound) {
		r.ctrl.Recorder().Forget(key)
		return 0, nil
	}
	if err != nil {
		return 0, err
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
		// What keep returns without an outcome are failures to read the
		// cluster; the controller logs the error.
		out = outcome{reason: "Failed", message: failureText("reading the cluster", err)}
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
	meta, err := r.cluster.NamespaceMeta(ctx, obj.Key.Namespace)
	if err != nil && !errors.Is(err, controller.ErrNotFound) {
		return outcome{}, 0, fmt.Errorf("reading the namespace %s: %w", obj.Key.Namespace, err)
	}
	// The set the record holds goes first when the namespace may no longer
	// write its name, whatever the spec says: a spec that can't be read, or
	// one that waits for a zone or for values, keeps no name the namespace
	// lost (review finding SEC-9).
	if c := status.claimed(); c != nil && !allowed(c.Name, meta) {
		if err := r.release(ctx, obj, status, fmt.Sprintf("the namespace %q may no longer write the name", obj.Key.Namespace)); err != nil {
			return failed("removing the record set", err), 0, err
		}
	}

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

	if !allowed(spec.Name, meta) {
		// The namespace has no right to the name (any more): a record set
		// the operator wrote for it while it had is not served on.
		if err := r.release(ctx, obj, status, fmt.Sprintf("the namespace %q may no longer write the name", obj.Key.Namespace)); err != nil {
			return failed("removing the record set", err), 0, err
		}
		return outcome{reason: "NotAllowed", message: fmt.Sprintf(
			"the namespace %q may not write %s: its allow list (namespace metadata %s) is %q",
			obj.Key.Namespace, spec.Name, MetaDNSSuffixes, meta[MetaDNSSuffixes])}, r.requeueWaiting, nil
	}

	// A set the record holds under another name or type goes before
	// anything can keep the record from its new one (a zone that is not
	// there or can't be written, values that are not there yet): a name
	// the record no longer asks for is not served on.
	if c := status.claimed(); c != nil && (c.Name != spec.Name || c.Type != spec.Type) {
		if err := r.release(ctx, obj, status, fmt.Sprintf("the record moved to %s %s", spec.Type, spec.Name)); err != nil {
			return failed("removing the record set", err), 0, err
		}
	}

	other, lapsed, err := r.holder(ctx, obj, spec, status)
	if err != nil {
		return outcome{}, 0, err
	}
	if other != (controller.ObjectKey{}) {
		// The record lost the name: it holds no record set from here on.
		// The set of the contested name and type is the other record's and
		// stays (a set under a former name went above).
		status.Owned, status.Pending = false, nil
		msg := fmt.Sprintf("%s writes %s %s", otherRecord(obj.Key, other), spec.Type, spec.Name)
		// The message may not name the other record; the log does, once
		// for every new conflict, not at every requeue while it lasts.
		if c := status.Condition(ConditionConflict); c == nil || c.Status != controller.ConditionTrue || c.Message != msg {
			r.logger.Info("another record holds the name and type of a record", "record", obj.Key.String(),
				"holder", other.String(), "name", spec.Name, "type", spec.Type)
		}
		requeue := r.requeueWaiting
		if r.namespace != "*" && other.Namespace != obj.Key.Namespace && r.requeueConflict > 0 {
			requeue = min(requeue, r.requeueConflict)
		}
		return outcome{reason: ConditionConflict, message: msg, conflict: msg}, requeue, nil
	}

	values, why, gone, err := r.values(ctx, obj, spec)
	if err != nil {
		return outcome{}, 0, err
	}
	if why != "" {
		// A source that has no value for a while leaves the last values
		// served. A source that was deleted does not: its addresses are
		// free for someone else, and the name must not point at them.
		if gone {
			if err := r.release(ctx, obj, status, why); err != nil {
				return failed("removing the record set", err), 0, err
			}
		}
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
	if c := status.claimed(); c != nil && (c.Name != want.Name || c.Type != want.Type || c.Zone != zone.Name) {
		if err := r.release(ctx, obj, status, fmt.Sprintf("the record moved to %s %s", want.Type, want.Name)); err != nil {
			return failed("removing the record set", err), 0, err
		}
	}

	client, err := r.zones.provider(ctx, zone)
	if err != nil {
		// The endpoint of the zone or its token Variable is wrong; the log
		// says which, the record does not.
		r.logger.Warn("the registration of a DNS zone can't be used", "zone", zone.Name, "record", obj.Key.String(), "error", err)
		msg := fmt.Sprintf("norca-dns can't use the registration of the DNS zone %s (its endpoint or its token); the error is in the log of norca-dns", zone.Name)
		return outcome{reason: "ZoneUnavailable", message: msg}, r.requeueWaiting, nil
	}
	sets, err := client.Records(ctx, want.Name)
	if err != nil {
		out := failed("reading the zone", err)
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "Failed", out.message)
		return out, 0, err
	}
	// have is the set as the contract checks it; read is the set as the
	// provider answered it, which a conditional write expects.
	var have, read *dns.RecordSet
	for i := range sets {
		if strings.EqualFold(sets[i].Type, want.Type) {
			read = &sets[i]
			set, err := dns.CheckRecordSet(zone.Name, sets[i])
			if err == nil {
				have = &set
			} else {
				have = &sets[i]
			}
		}
	}
	held := status.Owned && status.Name == want.Name && status.Type == want.Type && status.Zone == zone.Name
	// A set exactly as the status said the operator was about to write it
	// is the record's own: the status write after the zone write was lost
	// (the cluster API away for a moment).
	owned := held || (have != nil && status.Pending != nil && status.Pending.Name == want.Name &&
		status.Pending.Type == want.Type && status.Pending.is(zone.Name, *have))
	// A set that a record whose namespace may no longer write the name
	// holds, as its status says it wrote it, is the operator's: the record
	// that may write the name takes it over, and the other one gives it up
	// at its own next reconcile (review finding SEC-9).
	takeOver := have != nil && !owned && slices.ContainsFunc(lapsed, func(st Status) bool {
		return st.Zone == zone.Name && wrote(&st, zone.Name, *have)
	})

	switch {
	case have != nil && !owned && !takeOver && !spec.Adopt:
		// A set norca-dns did not write is not the record's, also when it
		// has the values the record asks for: the zone carries no marker,
		// and a record that took it would remove it with itself.
		with := ""
		if dns.SameRecords(*have, want) {
			with = ", with the values this record asks for"
		}
		msg := fmt.Sprintf("the zone %s has %s records of %s that norca-dns did not write (%s)%s; set adopt = true to take them over",
			zone.Name, want.Type, want.Name, strings.Join(have.Values, ", "), with)
		return outcome{reason: ConditionConflict, message: msg, conflict: msg}, r.requeueWaiting, nil

	case have != nil && dns.SameRecords(*have, want):
		// As it should be. A set the record did not hold is taken over
		// without a write (adopt, or the set of a record whose namespace
		// may no longer write the name).
		if !owned {
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "Adopted", adoptedText(want, takeOver))
		}

	default:
		// The status says which set the record is about to write before
		// the zone is written, and no set is written when it can't.
		prev := status.Pending
		status.Pending = &PendingSet{Zone: zone.Name, Name: want.Name, Type: want.Type, TTL: want.TTL, Values: want.Values}
		if err := r.recordPending(ctx, obj, status); err != nil {
			status.Pending = prev
			out := failed("recording the record set before writing it", err)
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "Failed", out.message)
			return out, 0, err
		}
		if err := r.setRecords(ctx, client, zone.Name, want, read); err != nil {
			var re *dns.ResponseError
			if errors.As(err, &re) && (re.PreconditionFailed() || re.Invalid()) {
				// Nothing was written.
				status.Pending = nil
			}
			if re != nil && re.PreconditionFailed() {
				// Someone wrote the set after it was read: nothing was
				// written, and the next reconcile reads it again.
				msg := fmt.Sprintf("the %s records of %s in the zone %s changed while norca-dns wrote them; nothing was written",
					want.Type, want.Name, zone.Name)
				return outcome{reason: ConditionConflict, message: msg, conflict: msg}, r.requeueWaiting, nil
			}
			out := failed("writing the record set", err)
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "Failed", out.message)
			if re != nil && re.Invalid() {
				// Not retried before something changes: the error is not
				// returned, so it is logged here.
				r.logger.Warn("the provider refused a record set", "zone", zone.Name, "record", obj.Key.String(), "error", err)
				return out, r.requeueWaiting, nil
			}
			return out, 0, err
		}
		now := r.now().UTC().Format(time.RFC3339)
		status.WrittenAt = now
		switch {
		case held && obj.Generation != 0 && status.ObservedGeneration == obj.Generation &&
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
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "Adopted", adoptedText(want, takeOver))
		default:
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "RecordWritten",
				fmt.Sprintf("%s %s: %s (TTL %d)", want.Type, want.Name, strings.Join(want.Values, ", "), want.TTL))
		}
	}

	status.Zone, status.Name, status.Type, status.TTL = zone.Name, want.Name, want.Type, want.TTL
	status.Values = want.Values
	status.Owned, status.Pending = true, nil
	return outcome{ready: true, reason: "RecordWritten",
		message: fmt.Sprintf("the zone %s holds %s %s", zone.Name, want.Type, want.Name)}, r.requeueReady, nil
}

// adoptedText is the message of the event of a record that took a record
// set over; lapsed says it was the set of a record whose namespace may no
// longer write the name.
func adoptedText(want dns.RecordSet, lapsed bool) string {
	if lapsed {
		return fmt.Sprintf("%s %s taken over from a record whose namespace may no longer write the name: %s",
			want.Type, want.Name, strings.Join(want.Values, ", "))
	}
	return fmt.Sprintf("%s %s taken over: %s", want.Type, want.Name, strings.Join(want.Values, ", "))
}

// holder returns the key of another DNSRecord that keeps this record from
// the name and type of its spec, or the zero key, and the statuses of the
// records that hold the set although their namespace may no longer write
// the name.
//
// The record whose status holds the record set keeps it, whichever record
// is older: a record that is deleted holds its set until the operator has
// removed it. A set nobody holds yet goes to the oldest record that may
// write the name: a record of a namespace whose allow list does not hold
// the name claims nothing, and holds nothing either (review finding SEC-9):
// such a record is asked to give its set up (when this operator keeps it),
// and keeps no other record from the name.
func (r *reconciler) holder(ctx context.Context, obj *controller.Object, spec Spec, status *Status) (controller.ObjectKey, []Status, error) {
	mine := status.Owned && status.Name == spec.Name && status.Type == spec.Type
	all, err := r.records(ctx)
	if err != nil {
		return controller.ObjectKey{}, nil, err
	}
	metas := map[string]map[string]string{}
	mayWrite := func(namespace string) (bool, error) {
		if meta, ok := metas[namespace]; ok {
			return allowed(spec.Name, meta), nil
		}
		meta, err := r.cluster.NamespaceMeta(ctx, namespace)
		if err != nil && !errors.Is(err, controller.ErrNotFound) {
			return false, fmt.Errorf("reading the namespace %s: %w", namespace, err)
		}
		metas[namespace] = meta
		return allowed(spec.Name, meta), nil
	}
	var owner, first *controller.Object
	var lapsed []Status
	for _, other := range all {
		if other.Key == obj.Key {
			continue
		}
		if holds(other, spec.Name, spec.Type) {
			ok, err := mayWrite(other.Key.Namespace)
			if err != nil {
				return controller.ObjectKey{}, nil, err
			}
			if !ok {
				var st Status
				_ = other.DecodeStatus(&st)
				lapsed = append(lapsed, st)
				if r.namespace == "" || r.namespace == "*" || other.Key.Namespace == r.namespace {
					r.ctrl.Enqueue(other.Key)
				}
				continue
			}
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
		ok, err := mayWrite(other.Key.Namespace)
		if err != nil {
			return controller.ObjectKey{}, nil, err
		}
		if ok {
			first = other
		}
	}
	switch {
	case owner != nil && (!mine || before(owner, obj)):
		// Two records that both hold the set (written at the same moment):
		// the older one keeps it.
		return owner.Key, nil, nil
	case owner == nil && first != nil:
		return first.Key, nil, nil
	}
	return controller.ObjectKey{}, lapsed, nil
}

// otherRecord names another DNSRecord in a condition or an event of the
// record key, which the team of its namespace reads: by namespace and name
// when it is in the same namespace, and only as a record of another
// namespace otherwise. A team does not learn the namespaces and records of
// the others from a conflict over a name; the operator logs the full key.
func otherRecord(key, other controller.ObjectKey) string {
	if other.Namespace == key.Namespace {
		return "the DNSRecord " + other.Namespace + "/" + other.Name
	}
	return "a DNSRecord in another namespace"
}

// records returns the DNSRecords of every namespace. A name is one for the
// whole cluster: the records that hold or claim it can be in any namespace,
// also in those an operator for one namespace (-namespace) does not keep.
// The operator of every namespace has them in its cache; the operator of
// one reads them from the servers.
func (r *reconciler) records(ctx context.Context) ([]*controller.Object, error) {
	if r.namespace == "" || r.namespace == "*" {
		return r.ctrl.Informer().List(), nil
	}
	all, _, err := r.ctrl.Objects().List(ctx, "*")
	if err != nil {
		return nil, fmt.Errorf("listing the DNS records: %w", err)
	}
	return all, nil
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
// source. why says why there are none yet; gone says the source itself does
// not exist.
func (r *reconciler) values(ctx context.Context, obj *controller.Object, spec Spec) (values []string, why string, gone bool, err error) {
	if spec.Source == nil || spec.Source.LoadBalancer == "" {
		return spec.Values, "", false, nil
	}
	lb, err := r.ctrl.Backend().ReadObject(ctx, LoadBalancerKind, obj.Key.Namespace, spec.Source.LoadBalancer)
	if errors.Is(err, controller.ErrNotFound) {
		return nil, fmt.Sprintf("the LoadBalancer %q of the namespace does not exist", spec.Source.LoadBalancer), true, nil
	}
	if err != nil {
		return nil, "", false, fmt.Errorf("reading the LoadBalancer %s: %w", spec.Source.LoadBalancer, err)
	}
	var st struct {
		Addresses []string `json:"addresses"`
	}
	_ = decode(lb.Status, &st)
	values = loadBalancerValues(spec.Type, st.Addresses)
	if len(values) == 0 {
		return nil, fmt.Sprintf("the LoadBalancer %q has no %s address yet (addresses: %s)",
			spec.Source.LoadBalancer, spec.Type, strings.Join(st.Addresses, ", ")), false, nil
	}
	return values, "", false, nil
}

// release removes the record set the status of a record holds, or the one
// it was about to write (Pending: the status write after the zone write may
// have been lost), and gives it up; why is for the event. A record that
// holds none is left as it is.
func (r *reconciler) release(ctx context.Context, obj *controller.Object, status *Status, why string) error {
	c := status.claimed()
	if c == nil {
		return nil
	}
	target := *status
	target.Zone, target.Name, target.Type, target.TTL, target.Values, target.Owned = c.Zone, c.Name, c.Type, c.TTL, c.Values, true
	kept, err := r.remove(ctx, obj, &target)
	if err != nil {
		// The caller returns the error, and the controller logs it.
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "DeleteFailed", failureText("removing the record set", err))
		return err
	}
	if kept != "" {
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeWarning, "RecordLeft",
			fmt.Sprintf("%s %s stays in the zone %s: %s", target.Type, target.Name, target.Zone, kept))
	} else {
		r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "RecordDeleted",
			fmt.Sprintf("%s %s removed from the zone %s: %s", target.Type, target.Name, target.Zone, why))
	}
	status.Owned, status.Pending = false, nil
	return nil
}

// remove deletes the record set a status says the operator wrote for obj.
// kept says why the set stays in the zone instead: the status of another
// record holds it too (two records that wrote it at the same moment, or a
// record that took it over), the zone holds other values than the operator
// wrote, or the zone can't be written any more (no longer registered, or
// records switched off).
func (r *reconciler) remove(ctx context.Context, obj *controller.Object, status *Status) (kept string, err error) {
	zone, err := r.zones.byName(ctx, status.Zone)
	if err != nil {
		return "", err
	}
	if zone == nil {
		r.logger.Warn("the zone of a record is no longer registered: its record set stays", "zone", status.Zone,
			"name", status.Name, "type", status.Type)
		return "the zone is no longer registered", nil
	}
	// A zone that no longer lets norca-dns write is like one that is no
	// longer registered: nothing of it can be removed, and a record of it
	// must not wait for that forever.
	const switchedOff = "the zone no longer lets norca-dns write records (records = true in the configuration of norca-provider)"
	if !zone.Spec.Records {
		r.logger.Warn("the zone of a record no longer allows records: its record set stays", "zone", status.Zone,
			"name", status.Name, "type", status.Type)
		return switchedOff, nil
	}
	client, err := r.zones.provider(ctx, zone)
	if err != nil {
		return "", err
	}
	// The provider answers so for the records of a zone without the switch;
	// the DNSZone object may not say so yet. Another 403 (of a proxy in
	// front of the provider, say) is a failure like any other: the record
	// must not go while its set is in the zone.
	disabled := func(err error) bool {
		re, ok := errors.AsType[*dns.ResponseError](err)
		if ok && re.RecordsDisabled() {
			r.logger.Warn("the provider of the zone refuses the records contract: the record set stays", "zone", status.Zone,
				"name", status.Name, "type", status.Type, "answer", re.Message)
		}
		return ok && re.RecordsDisabled()
	}

	// The set is read first and the records that hold it after: a record
	// that takes a set over writes it before its status says so, and then
	// the zone already holds its values.
	sets, err := client.Records(ctx, status.Name)
	if disabled(err) {
		return switchedOff, nil
	}
	if err != nil {
		return "", err
	}
	var have *dns.RecordSet
	for i := range sets {
		if strings.EqualFold(sets[i].Type, status.Type) {
			have = &sets[i]
		}
	}
	if have == nil {
		// Nothing to remove: the set is gone already.
		return "", nil
	}

	// Read from the servers, not from the cache: a set that is deleted
	// under a record that holds it is an outage of its name. Every
	// namespace: the record that holds it can be in any (see records).
	others, _, err := r.ctrl.Objects().List(ctx, "*")
	if err != nil {
		return "", fmt.Errorf("listing the DNS records: %w", err)
	}
	for _, other := range others {
		if other.Key == obj.Key || other.Deleting() {
			continue
		}
		var st Status
		if other.DecodeStatus(&st) == nil && st.Owned && st.Zone == status.Zone && st.Name == status.Name && st.Type == status.Type {
			r.logger.Info("a record set stays: another record holds it", "record", obj.Key.String(),
				"holder", other.Key.String(), "zone", status.Zone, "name", status.Name, "type", status.Type)
			return otherRecord(obj.Key, other.Key) + " holds it", nil
		}
	}

	// A set with other values than the operator wrote is not the
	// operator's any more (changed by hand, or written by a record that
	// took it over): it stays.
	if !wrote(status, zone.Name, *have) {
		r.logger.Info("a record set stays: the zone holds other values than norca-dns wrote", "record", obj.Key.String(),
			"zone", status.Zone, "name", status.Name, "type", status.Type, "values", have.Values, "ttl", have.TTL)
		return fmt.Sprintf("the zone holds other values than norca-dns wrote (%s, TTL %d)", strings.Join(have.Values, ", "), have.TTL), nil
	}

	// Removed only if the set still holds what was read (review finding
	// ATM-9): a set changed after the read stays.
	err = client.DeleteRecordsIf(ctx, status.Name, status.Type, dns.Expect{Values: have.Values})
	if r.unconditional(zone.Name, err) {
		err = client.DeleteRecords(ctx, status.Name, status.Type)
	}
	if disabled(err) {
		return switchedOff, nil
	}
	if re, ok := errors.AsType[*dns.ResponseError](err); ok && re.PreconditionFailed() {
		r.logger.Info("a record set stays: it changed while norca-dns removed it", "record", obj.Key.String(),
			"zone", status.Zone, "name", status.Name, "type", status.Type)
		return "it changed in the zone while norca-dns removed it", nil
	}
	return "", err
}

// setRecords writes a record set only over the set the zone held when it
// was read (have; nil: none of the name and type), checked by the provider
// in the same step (review finding ATM-9): a set written or changed after
// the read is not written over. A provider that can't check it gets the
// write without the check; the set was read right before.
func (r *reconciler) setRecords(ctx context.Context, client *dns.Client, zone string, want dns.RecordSet, have *dns.RecordSet) error {
	expect := dns.Expect{Absent: have == nil}
	if have != nil {
		expect.Values = have.Values
	}
	err := client.SetRecordsIf(ctx, want, expect)
	if r.unconditional(zone, err) {
		return client.SetRecords(ctx, want)
	}
	return err
}

// unconditional reports whether the provider of a zone refused a
// conditional write because it can't check the condition. The log says so
// once for every zone.
func (r *reconciler) unconditional(zone string, err error) bool {
	re, ok := errors.AsType[*dns.ResponseError](err)
	if !ok || !re.ConditionsUnsupported() {
		return false
	}
	if _, logged := r.unconditionalZones.LoadOrStore(zone, true); !logged {
		r.logger.Warn("the provider of a DNS zone can't make a write depend on the record set: norca-dns writes and removes "+
			"record sets of the zone right after it read them, without the check at the provider", "zone", zone)
	}
	return true
}

// wrote reports whether a record set of the zone is the one a status says
// the operator wrote (the same TTL and values as the set it holds), or the
// one it was about to write (Pending).
func wrote(status *Status, zone string, have dns.RecordSet) bool {
	if status.Owned && (&PendingSet{Zone: status.Zone, Name: status.Name, Type: status.Type, TTL: status.TTL, Values: status.Values}).is(zone, have) {
		return true
	}
	return status.Pending != nil && status.Pending.is(zone, have)
}

// claimed returns the record set the status holds or, when it holds none,
// the one the operator was about to write when the status was written last
// (nil: neither).
func (s *Status) claimed() *PendingSet {
	if s.Owned {
		return &PendingSet{Zone: s.Zone, Name: s.Name, Type: s.Type, TTL: s.TTL, Values: s.Values}
	}
	return s.Pending
}

// is reports whether a record set of a zone, as the provider answered it,
// is this one: the same zone, name, type, TTL and values.
func (p *PendingSet) is(zone string, have dns.RecordSet) bool {
	if set, err := dns.CheckRecordSet(zone, have); err == nil {
		have = set
	} else {
		have.Name, have.Type = dns.Normalize(have.Name), strings.ToUpper(have.Type)
		have.Values = slices.Sorted(slices.Values(have.Values))
	}
	return p.Zone == zone && p.Name == have.Name && p.Type == have.Type && p.TTL == have.TTL && slices.Equal(p.Values, have.Values)
}

// recordPending writes the status with the record set the operator is about
// to write (status.Pending), with the check-and-set of the object; obj
// follows the write, so that the status write at the end of the reconcile
// goes on from it.
func (r *reconciler) recordPending(ctx context.Context, obj *controller.Object, status *Status) error {
	next, err := r.ctrl.Objects().UpdateStatus(ctx, obj, status)
	if err != nil {
		return fmt.Errorf("recording the record set to write: %w", err)
	}
	obj.Status, obj.ModifyIndex = next.Status, next.ModifyIndex
	return nil
}

// finishDeletion removes the record set of a deleted object and lets the
// object go. A record whose deletion policy is retain goes without touching
// the zone. A removal that fails is tried again, and the status says how to
// let the record go instead: a set must not be left in the zone unless the
// writer of the record asks for it, and the spec can be changed while the
// record is being deleted.
func (r *reconciler) finishDeletion(ctx context.Context, obj *controller.Object, status *Status) (time.Duration, error) {
	if !obj.HasFinalizer(Finalizer) {
		return 0, nil
	}
	var spec Spec
	if obj.DecodeSpec(&spec) == nil && spec.DeletionPolicy == DeletionRetain {
		if status.Owned {
			r.logger.Info("a deleted record leaves its record set in the zone (deletion_policy = retain)", "record", obj.Key.String(),
				"zone", status.Zone, "name", status.Name, "type", status.Type)
			r.ctrl.Recorder().Event(ctx, obj.Key, controller.EventTypeNormal, "RecordLeft",
				fmt.Sprintf("%s %s stays in the zone %s: the record says deletion_policy = %q", status.Type, status.Name, status.Zone, DeletionRetain))
		}
	} else if err := r.release(ctx, obj, status, "the record was deleted"); err != nil {
		out := failed("removing the record set", err)
		out.reason = "DeleteFailed"
		out.message += fmt.Sprintf(`; it is tried again. deletion_policy = %q in the spec lets the record go and leaves the record set in the zone`, DeletionRetain)
		if werr := r.writeStatus(ctx, obj, status, out); werr != nil {
			r.logger.Warn("writing the status of a record failed", "record", obj.Key.String(), "error", werr)
		}
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

// loadBalancerSource reconciles the records that take their values from a
// load balancer when it changes. An event that does not say which load
// balancer changed reconciles every record with a load balancer source.
func loadBalancerSource() controller.Source {
	return &controller.EventSource{
		SourceName: "loadbalancers",
		Topics:     map[api.Topic][]string{api.TopicCustomObject: {LoadBalancerKind.Group + "/" + LoadBalancerKind.Kind}},
		Map: func(ev *api.Event, sc *controller.SourceContext) []controller.ObjectKey {
			lb, err := ev.CustomObject()
			if err != nil {
				// An event that can't be read names no load balancer.
				lb = nil
			}
			var keys []controller.ObjectKey
			for _, obj := range sc.Objects() {
				var spec Spec
				if obj.DecodeSpec(&spec) != nil || spec.Source == nil || spec.Source.LoadBalancer == "" {
					continue
				}
				if lb != nil && (obj.Key.Namespace != lb.Namespace || spec.Source.LoadBalancer != lb.Name) {
					continue
				}
				keys = append(keys, obj.Key)
			}
			return keys
		},
	}
}

// zoneSource reconciles every record when a DNS zone changes (registered
// or gone, its spec with the records switch, its heartbeat back or away).
//
// A provider writes the heartbeat of each of its zones every 30 seconds,
// and each of those writes is an event: the source remembers the zones and
// asks for the records only when something of a zone changed that a record
// depends on. A heartbeat that only says the provider is still there
// changes nothing.
func zoneSource(z *zones) controller.Source {
	var (
		mu    sync.Mutex
		known bool
		seen  map[string]zoneInfo
	)
	// changed reads the zones and compares them with those of the event
	// before. When they can't be read, the records are reconciled: they
	// read the zones themselves.
	changed := func() bool {
		z.forget()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		all, err := z.list(ctx)
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			known = false
			return true
		}
		now := z.now()
		cur := make(map[string]zoneInfo, len(all))
		for _, zone := range all {
			cur[zone.Name] = *zone
		}
		differs := !known || len(cur) != len(seen)
		for name, zone := range cur {
			// The availability of both is taken now: a zone whose
			// heartbeat was fresh at the last event and is too old by
			// now turns available with this one.
			old, ok := seen[name]
			if !ok || old.Spec != zone.Spec || old.available(now, z.ttl) != zone.available(now, z.ttl) {
				differs = true
			}
		}
		known, seen = true, cur
		return differs
	}
	return &controller.EventSource{
		SourceName: "zones",
		Topics:     map[api.Topic][]string{api.TopicCustomObject: {ZoneKind.Group + "/" + ZoneKind.Kind}},
		Map: func(_ *api.Event, sc *controller.SourceContext) []controller.ObjectKey {
			if !changed() {
				return nil
			}
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

// The conditions and the events of a record are read by whoever may read
// the record, the team of its namespace too. Of a failure they say what
// failed, not the configuration of the operator (the endpoint of a zone,
// the path of its token Variable) nor what the provider or the cluster API
// says of their own failures (addresses of name servers and servers): the
// error itself goes to the log of norca-dns.

// failed is the outcome of a failure whose event the reconcile records
// itself.
func failed(what string, err error) outcome {
	return outcome{reason: "Failed", message: failureText(what, err), evented: true}
}

// failureText returns what the status and the events of a record say of a
// failure. A request the provider refuses as one it can't satisfy (the
// values of the record, a zone without records) is shown with the answer
// of the provider, which is about the request.
func failureText(what string, err error) string {
	if re, ok := errors.AsType[*dns.ResponseError](err); ok {
		switch {
		case re.Status == http.StatusUnauthorized:
			return what + ": the provider of the zone refused the token of norca-dns"
		case re.Invalid() && re.Reason != "":
			return what + ": the provider of the zone refused the request: " + re.Message
		default:
			return fmt.Sprintf("%s: the provider of the zone answered %d; the error is in the log of norca-dns", what, re.Status)
		}
	}
	return what + " failed; the error is in the log of norca-dns"
}
