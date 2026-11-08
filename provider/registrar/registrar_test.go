// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package registrar

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
)

// fakeStore keeps the objects in memory.
type fakeStore struct {
	mu      sync.Mutex
	objects map[string]*api.CustomObject
	deleted []string
}

func (s *fakeStore) Apply(_ context.Context, obj *api.CustomObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = map[string]*api.CustomObject{}
	}
	cp := *obj
	s.objects[obj.Name] = &cp
	return nil
}

func (s *fakeStore) UpdateStatus(_ context.Context, name string, status map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[name]
	if !ok {
		return ErrNotFound
	}
	obj.Status = status
	return nil
}

func (s *fakeStore) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, name)
	s.deleted = append(s.deleted, name)
	return nil
}

func (s *fakeStore) get(name string) *api.CustomObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	if obj, ok := s.objects[name]; ok {
		cp := *obj
		return &cp
	}
	return nil
}

func TestRegistration_Object(t *testing.T) {
	obj := Registration{
		Name: "hetzner-eu", Endpoint: "https://provider.example:8443",
		TokenVariable: "nodescaler/hetzner-eu/token",
		Groups: []GroupSpec{
			{Name: "fsn1", Max: 20, Parameters: map[string]any{"sizes": []any{"cx22", "cx32"}}},
			{Name: "nbg1"},
		},
	}.Object()
	if obj.Group != "norca.io" || obj.Kind != "NodeProvider" || obj.Name != "hetzner-eu" || obj.ManagedBy != ManagedBy {
		t.Fatalf("object: %+v", obj)
	}
	want := map[string]any{
		"endpoint":       "https://provider.example:8443",
		"token_variable": "nodescaler/hetzner-eu/token",
		"groups": []any{
			map[string]any{"name": "fsn1", "max": 20, "parameters": map[string]any{"sizes": []any{"cx22", "cx32"}}},
			map[string]any{"name": "nbg1"},
		},
	}
	if !reflect.DeepEqual(obj.Spec, want) {
		t.Fatalf("spec:\n got %#v\nwant %#v", obj.Spec, want)
	}
}

func TestRegistrar_HeartbeatAndRegisterAgain(t *testing.T) {
	store := &fakeStore{}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	r := &Registrar{
		Store:            store,
		Registration:     Registration{Name: "mock", Endpoint: "http://127.0.0.1:8480"},
		Interval:         10 * time.Millisecond,
		Version:          "0.1.0",
		DeregisterOnExit: true,
		Now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			now = now.Add(time.Second)
			return now
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()

	waitFor(t, func() bool {
		obj := store.get("mock")
		return obj != nil && obj.Status["heartbeat"] != nil && obj.Status["version"] == "0.1.0"
	})
	first := store.get("mock").Status["heartbeat"]
	waitFor(t, func() bool { return store.get("mock").Status["heartbeat"] != first })

	// The object is removed by someone: the next heartbeat registers again.
	_ = store.Delete(context.Background(), "mock")
	waitFor(t, func() bool { return store.get("mock") != nil })

	cancel()
	<-done
	if store.get("mock") != nil {
		t.Fatal("the registration is left after the exit")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
