// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/shoenig/test/must"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca/norca/controller"
)

// runOperator runs a controller of newOperatorIn until the test ends.
func runOperator(t *testing.T, c *controller.Controller) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// A record that waits for a name gets it as soon as the record that held
// or claimed it gives it up, not at its next requeue (review finding
// MUK-12): the informer's change of the other record enqueues it. The
// requeue of a waiting record is an hour here, so only that event can do it.
func TestDNSRecord_HandOverWithoutRequeue(t *testing.T) {
	for _, release := range []string{"deleted", "renamed"} {
		t.Run(release, func(t *testing.T) {
			e, c := newOperatorIn(t, "*", true)
			e.r.requeueWaiting = time.Hour
			runOperator(t, c)
			e.cluster.allow("shop", "shop.example.com")
			spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}

			e.put("shop", "old", spec)
			e.waitFor("the first record holds the name", func() bool {
				_, st := e.status("shop", "old")
				return readyReason(st) == "True" && st.Owned
			})
			e.put("shop", "new", map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.3"}})
			e.waitFor("the second record waits", func() bool {
				_, st := e.status("shop", "new")
				return readyReason(st) == ConditionConflict
			})

			switch release {
			case "deleted":
				e.b.RemoveObject(Kind, "shop", "old")
			case "renamed":
				e.put("shop", "old", map[string]any{"name": "old.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}})
			}
			e.waitFor("the second record takes the name over", func() bool {
				_, st := e.status("shop", "new")
				return readyReason(st) == "True" && st.Owned &&
					slices.Equal(e.zone.RecordValues("www.shop.example.com", "A"), []string{"192.0.2.3"})
			})
		})
	}
}

// An operator of one namespace does not see a record of another namespace
// give a name up; a record of its own that waits for it is looked at again
// after -conflict-requeue instead of the minute of the other waits (review
// finding MUK-12).
func TestDNSRecord_OneNamespaceConflictRequeue(t *testing.T) {
	e, c := newOperatorIn(t, "shop", true)
	e.r.requeueWaiting = time.Hour
	e.r.requeueConflict = 200 * time.Millisecond
	runOperator(t, c)
	e.cluster.allow("shop", "shop.example.com")
	e.cluster.allow("other", "example.com")
	spec := map[string]any{"name": "www.shop.example.com", "type": "A", "values": []any{"192.0.2.2"}}

	// The record of the other namespace holds the set (its operator wrote
	// it).
	must.NoError(t, e.zone.SetRecords(context.Background(), dns.RecordSet{Name: "www.shop.example.com", Type: "A", TTL: 300, Values: []string{"192.0.2.2"}}))
	e.put("other", "www", spec)
	e.hold("other", "www")

	e.put("shop", "www", spec)
	e.waitFor("the record waits for the holder of the other namespace", func() bool {
		_, st := e.status("shop", "www")
		return readyReason(st) == ConditionConflict
	})

	e.b.RemoveObject(Kind, "other", "www")
	e.waitFor("the record takes the name over after the conflict requeue", func() bool {
		_, st := e.status("shop", "www")
		return readyReason(st) == "True" && st.Owned
	})
}
