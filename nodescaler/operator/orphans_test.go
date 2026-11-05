// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
)

// createRaw creates an instance in the mock provider with the labels.
func (e *env) createRaw(requestID string, labels map[string]string) string {
	e.t.Helper()
	insts, err := e.mock.Create(context.Background(), "dc1", provider.CreateRequest{RequestID: requestID, Count: 1, Labels: labels})
	if err != nil {
		e.t.Fatal(err)
	}
	return insts[0].ID
}

func (e *env) sweep() int {
	e.t.Helper()
	n, err := e.r.SweepOrphans(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) has(id string) bool {
	for _, inst := range e.instances() {
		if inst.ID == id {
			return true
		}
	}
	return false
}

func TestOrphans(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	owned := e.instances()[0].ID

	gone := e.createRaw("r-gone", map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group", LabelDatacenter: "dc1", LabelPool: "workers"})
	withNode := e.createRaw("r-node", map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group", LabelDatacenter: "dc1", LabelPool: "workers"})
	otherRegion := e.createRaw("r-us", map[string]string{LabelRegion: "us", LabelNodeGroup: "deleted-group"})
	unlabelled := e.createRaw("r-hand", nil)
	e.nodes.set(Node{ID: "n9", ProviderID: withNode, Ready: true})

	// Younger than the orphan age: nothing is deleted.
	if n := e.sweep(); n != 0 {
		t.Fatalf("deleted %d young instances", n)
	}

	// Older: only the orphan without a node goes. The instance of the group
	// that exists, the one with a node, the one of another region and one
	// without the labels of the operator stay.
	e.advance(16 * time.Minute)
	if n := e.sweep(); n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}
	if e.has(gone) {
		t.Fatal("the orphan is still there")
	}
	for name, id := range map[string]string{"owned": owned, "with a node": withNode, "other region": otherRegion, "unlabelled": unlabelled} {
		if !e.has(id) {
			t.Errorf("the %s instance was deleted", name)
		}
	}

	// A group that moved to another provider group leaves its instances
	// orphaned.
	e.putGroup(1, map[string]any{"group": "dc2"})
	if n := e.sweep(); n != 1 || e.has(owned) {
		t.Fatalf("the instance of the moved group: deleted %d", n)
	}
}

func TestOrphans_UnavailableProviderIsLeftAlone(t *testing.T) {
	e := newEnv(t, "")
	gone := e.createRaw("r-gone", map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group"})
	e.now = e.now.Add(16 * time.Minute) // the heartbeat is old now
	if n := e.sweep(); n != 0 || !e.has(gone) {
		t.Fatalf("deleted %d through an unavailable provider", n)
	}
}
