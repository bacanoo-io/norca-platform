// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"fmt"
	"testing"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
)

// createRaw creates an instance in the mock provider with the labels, named
// after the request.
func (e *env) createRaw(requestID string, labels map[string]string) string {
	e.t.Helper()
	insts, err := e.mock.Create(context.Background(), "dc1", provider.CreateRequest{RequestID: requestID,
		Instances: []provider.InstanceRequest{{Name: requestID}}, Labels: labels})
	if err != nil {
		e.t.Fatal(err)
	}
	return insts[0].Name
}

func (e *env) sweep() int {
	e.t.Helper()
	n, err := e.r.SweepOrphans(context.Background())
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) has(name string) bool {
	for _, inst := range e.instances() {
		if inst.Name == name {
			return true
		}
	}
	return false
}

func TestOrphans(t *testing.T) {
	e := newEnv(t, "")
	e.putGroup(1, nil)
	e.reconcile()
	owned := e.instances()[0].Name

	gone := e.createRaw("r-gone", map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group", LabelDatacenter: "dc1", LabelPool: "workers"})
	withNode := e.createRaw("r-node", map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group", LabelDatacenter: "dc1", LabelPool: "workers"})
	otherRegion := e.createRaw("r-us", map[string]string{LabelRegion: "us", LabelNodeGroup: "deleted-group"})
	unlabelled := e.createRaw("r-hand", nil)
	e.nodes.set(Node{ID: "n9", Instance: withNode, Ready: true})

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

func TestOrphans_WithNodeReportedOrDeleted(t *testing.T) {
	e := newEnv(t, "")
	labels := map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group", LabelDatacenter: "dc1", LabelPool: "workers"}
	withNode := e.createRaw("r-node", labels)
	e.nodes.set(Node{ID: "n9", Name: "node-9", Instance: withNode, Ready: true})
	e.advance(16 * time.Minute)

	// Reported in the log of the provider, and left.
	if n := e.sweep(); n != 0 || !e.has(withNode) {
		t.Fatalf("deleted %d", n)
	}
	reported := false
	for _, ev := range e.backend.ObjectEvents(NodeProviderKind, "", "mock") {
		reported = reported || ev.Reason == "OrphanedInstance"
	}
	if !reported {
		t.Fatal("no OrphanedInstance event in the log of the provider")
	}

	// With DeleteOrphans it is deleted.
	e.r.cfg.DeleteOrphans = true
	if n := e.sweep(); n != 1 || e.has(withNode) {
		t.Fatalf("deleted %d, the instance is there: %t", n, e.has(withNode))
	}
}

// recMetrics records the operator's own metrics.
type recMetrics struct{ gauges, counters map[string]float64 }

func newRecMetrics() *recMetrics {
	return &recMetrics{gauges: map[string]float64{}, counters: map[string]float64{}}
}

func metricKey(name string, labels []string) string { return name + fmt.Sprint(labels) }

func (m *recMetrics) SetGauge(name string, v float64, labels ...string) {
	m.gauges[metricKey(name, labels)] = v
}

func (m *recMetrics) AddCounter(name string, v float64, labels ...string) {
	m.counters[metricKey(name, labels)] += v
}

func TestOrphans_Metrics(t *testing.T) {
	e := newEnv(t, "")
	m := newRecMetrics()
	e.r.cfg.Metrics = m
	labels := map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group", LabelDatacenter: "dc1", LabelPool: "workers"}
	e.createRaw("r-gone", labels)
	withNode := e.createRaw("r-node", labels)
	e.nodes.set(Node{ID: "n9", Name: "node-9", Instance: withNode, Ready: true})
	e.advance(16 * time.Minute)

	// The orphan without a node is deleted, the one with a node is left.
	e.sweep()
	orphans, deleted := metricKey(metricOrphans, []string{"provider", "mock"}), metricKey(metricOrphansDeleted, []string{"provider", "mock"})
	if m.gauges[orphans] != 1 || m.counters[deleted] != 1 {
		t.Fatalf("gauges %v counters %v", m.gauges, m.counters)
	}
	// It goes too: none left.
	e.r.cfg.DeleteOrphans = true
	e.sweep()
	if m.gauges[orphans] != 0 || m.counters[deleted] != 2 {
		t.Fatalf("gauges %v counters %v", m.gauges, m.counters)
	}
}

func TestOrphans_MetricsOfAnUnfinishedSweep(t *testing.T) {
	e := newEnv(t, "")
	m := newRecMetrics()
	e.r.cfg.Metrics = m
	labels := map[string]string{LabelRegion: "eu", LabelNodeGroup: "deleted-group", LabelDatacenter: "dc1", LabelPool: "workers"}
	withNode := e.createRaw("r-node", labels)
	e.nodes.set(Node{ID: "n9", Name: "node-9", Instance: withNode, Ready: true})
	e.advance(16 * time.Minute)
	e.sweep()
	orphans := metricKey(metricOrphans, []string{"provider", "mock"})
	if m.gauges[orphans] != 1 {
		t.Fatalf("gauges %v", m.gauges)
	}

	// The provider names a group it cannot list: the sweep does not get to
	// the end of the provider, so the count of its last complete sweep stays,
	// though the orphan it saw is gone.
	if err := e.mock.Delete(context.Background(), "dc1", e.instances()[0].ID); err != nil {
		t.Fatal(err)
	}
	obj := e.backend.GetObject(NodeProviderKind, "", "mock")
	obj.Spec["groups"] = []any{map[string]any{"name": "dc1"}, map[string]any{"name": "dc9"}}
	e.backend.PutObject(obj)
	e.sweep()
	if m.gauges[orphans] != 1 {
		t.Fatalf("an unfinished sweep changed the count: %v", m.gauges)
	}
}
