// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"fmt"
	"slices"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
)

// Requests the provider does not list yet. The listing of a platform can lag
// behind a create: the provider answers the request with its instances, and
// its listing shows them only later. A look in between would find fewer
// instances than the scale and ask for them again with a new request number:
// a second set of machines (review finding CON-6d). So the status remembers a
// request until a listing has shown all of its instances
// (status.pendingRequests: the number, how many instances the provider
// answered it with, when, and the instances a listing has shown so far), and
// the instances no listing has shown count as live and as joining for a grace
// time (Config.ListGrace). An instance counts once: the listing is matched
// with the request by the request ID of the instance, and an instance that
// was listed stays so in the record when it is deleted later.
//
// The machine of such an instance can boot and its node join before a listing
// shows it (review finding CON-6i). So the names of the instances of the
// requests in the status (pendingNames: the names describe gives the
// instances of a request, for as many as it was asked for) are names of
// instances of the group, like those of the listing: a node with one of them
// is not a node without an instance (stray.go), and the join record of one is
// not dropped (join.Store.Prune). This holds while the request is in the
// status, also past the grace time while it is made again; once it is
// forgotten, the names are handled like any other.
//
// A request whose instances are still missing after the grace time counts as
// not fulfilled. If the group is then short of its scale, the request is made
// again with its own number: a provider that has the instances answers with
// them and creates nothing. The answer says what the request made, and the
// grace time starts again for those of its instances the listing does not
// show. This is reported: the event InstancesNotListed, and Provisioned is
// False (NotListed) until the listing shows them. With the join, the request
// made again carries new tokens and the ones of the first time stay valid
// for the machines that have them (join.Store.Issue; review finding CON-6h);
// an instance that has joined gets none. If the group is not short
// (the scale was lowered meanwhile), the request is forgotten.
//
// The record is written with the status of the look that made the request,
// after the call. When that write is lost, the next look finds neither the
// record nor lastRequest: it counts the instances if the listing shows them,
// and otherwise makes the request again with the same number (nextRequest),
// which the provider answers with the instances of the first one; the record
// is written then. Another instance after a leader change reads the same
// status and does the same.

// defaultListGrace is how long the instances of a request may be missing
// from the listing of the provider: well over the lag of the list calls of
// the platforms (seconds, at times a minute), and under the default of
// join.slow_after, after which an instance that is listed no longer counts
// as capacity either.
const defaultListGrace = 2 * time.Minute

// pendingRequest is an entry of status.pendingRequests: a request the
// provider answered whose instances its listing has not all shown.
type pendingRequest struct {
	Number int64
	// Count is how many instances the request made: those of the answer of
	// the provider and those a listing has shown.
	Count int64
	// At is when the request was made, the last time.
	At time.Time
	// Asks is how many times it was made: more than once after the grace
	// time passed without its instances.
	Asks int
	// Asked is the largest number of instances it was made for, or answered
	// with: its instances have the names instanceName gives the first Asked.
	Asked int64
	// Listed are the provider's IDs of its instances a listing has shown.
	Listed []string
}

func readPending(status map[string]any) []*pendingRequest {
	list, _ := status["pendingRequests"].([]any)
	var out []*pendingRequest
	for _, item := range list {
		d, ok := item.(map[string]any)
		if !ok {
			continue
		}
		number, _ := d["number"].(float64)
		count, _ := d["count"].(float64)
		asks, _ := d["asks"].(float64)
		at, _ := d["at"].(string)
		t, err := time.Parse(time.RFC3339, at)
		if number < 1 || count < 1 || err != nil {
			continue
		}
		asked, _ := d["asked"].(float64)
		p := &pendingRequest{Number: int64(number), Count: int64(count), At: t, Asks: int(asks), Asked: int64(asked)}
		listed, _ := d["listed"].([]any)
		for _, v := range listed {
			if id, ok := v.(string); ok && id != "" {
				p.Listed = append(p.Listed, id)
			}
		}
		out = append(out, p)
	}
	return out
}

// pendingDocs is status.pendingRequests, nil without a request.
func pendingDocs(pend []*pendingRequest) []any {
	if len(pend) == 0 {
		return nil
	}
	out := make([]any, 0, len(pend))
	for _, p := range pend {
		d := map[string]any{"number": p.Number, "count": p.Count, "asks": p.Asks, "at": p.At.UTC().Format(time.RFC3339)}
		if p.Asked > 0 {
			d["asked"] = p.Asked
		}
		if len(p.Listed) > 0 {
			listed := make([]any, 0, len(p.Listed))
			for _, id := range p.Listed {
				listed = append(listed, id)
			}
			d["listed"] = listed
		}
		out = append(out, d)
	}
	return out
}

// unlisted is how many instances of the request no listing has shown.
func (p *pendingRequest) unlisted() int64 {
	return max(p.Count-int64(len(p.Listed)), 0)
}

// overdue reports a request whose instances are missing from the listing
// for longer than the grace time.
func (p *pendingRequest) overdue(now time.Time, grace time.Duration) bool {
	return now.Sub(p.At) > grace
}

// pendingNames are the names of the instances of the requests: those
// describe gave them. A record without the number asked for has the names of
// as many as the request made.
func pendingNames(group string, pend []*pendingRequest) map[string]bool {
	out := map[string]bool{}
	for _, p := range pend {
		for i := 1; int64(i) <= max(p.Asked, p.Count); i++ {
			out[instanceName(group, p.Number, i)] = true
		}
	}
	return out
}

// answered records the answer of the provider to the request made for asked
// instances: the request made the instances of the answer and those a
// listing has shown, and the grace time of the ones not listed starts now.
func (p *pendingRequest) answered(created []provider.Instance, asked int, now time.Time) {
	p.Asked = max(p.Asked, int64(asked), int64(len(created)))
	p.Count = int64(len(p.Listed))
	for _, inst := range created {
		if !slices.Contains(p.Listed, inst.ID) {
			p.Count++
		}
	}
	p.At = now
	p.Asks++
}

// complete drops the requests all of whose instances a listing has shown.
func complete(pend []*pendingRequest) []*pendingRequest {
	return slices.DeleteFunc(pend, func(p *pendingRequest) bool { return p.unlisted() == 0 })
}

// pending reads the requests of the status, marks the instances of each
// that the listing shows now, and drops the requests that are complete.
func (r *Reconciler) pending(group string, status map[string]any, instances []provider.Instance) []*pendingRequest {
	pend := readPending(status)
	for _, p := range pend {
		id := r.requestID(group, p.Number)
		for _, inst := range instances {
			if inst.RequestID == id && !slices.Contains(p.Listed, inst.ID) {
				p.Listed = append(p.Listed, inst.ID)
			}
		}
	}
	return complete(pend)
}

// awaited is how many instances count as live without being listed: those
// of the requests within the grace time.
func (r *Reconciler) awaited(pend []*pendingRequest, now time.Time) int64 {
	var n int64
	for _, p := range pend {
		if !p.overdue(now, r.cfg.ListGrace) {
			n += p.unlisted()
		}
	}
	return n
}

// overdueRequest returns the first request past the grace time.
func (r *Reconciler) overdueRequest(pend []*pendingRequest, now time.Time) *pendingRequest {
	for _, p := range pend {
		if p.overdue(now, r.cfg.ListGrace) {
			return p
		}
	}
	return nil
}

// notListed is Provisioned of a group one of whose requests was made again
// and is still not listed, nil when there is none.
func (r *Reconciler) notListed(group, providerName string, pend []*pendingRequest) *condition {
	for _, p := range pend {
		if p.Asks > 1 {
			return &condition{Type: CondProvisioned, Status: "False", Reason: "NotListed",
				Message: fmt.Sprintf("provider %q does not list %d instances of request %s, made %d times, the last time at %s",
					providerName, p.unlisted(), r.requestID(group, p.Number), p.Asks, p.At.UTC().Format(time.RFC3339))}
		}
	}
	return nil
}

// provisionedMessage is the message of Provisioned for the live instances
// of a group, awaited of them not listed yet.
func provisionedMessage(live, want, awaited int64) string {
	msg := fmt.Sprintf("%d instances for a scale of %d", live, want)
	if awaited > 0 {
		msg += fmt.Sprintf("; %d of them are asked for and not in the listing of the provider yet", awaited)
	}
	return msg
}
