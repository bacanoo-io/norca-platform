// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package registrar

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
)

// fakeStore keeps the objects in memory, by kind and name.
type fakeStore struct {
	mu      sync.Mutex
	objects map[string]*api.CustomObject
}

func key(kind, name string) string { return kind + "/" + name }

func (s *fakeStore) Apply(_ context.Context, obj *api.CustomObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = map[string]*api.CustomObject{}
	}
	cp := *obj
	s.objects[key(obj.Kind, obj.Name)] = &cp
	return nil
}

func (s *fakeStore) UpdateStatus(_ context.Context, kind, name string, status map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[key(kind, name)]
	if !ok {
		return ErrNotFound
	}
	obj.Status = status
	return nil
}

func (s *fakeStore) Delete(_ context.Context, kind, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key(kind, name))
	return nil
}

func (s *fakeStore) get(kind, name string) *api.CustomObject {
	s.mu.Lock()
	defer s.mu.Unlock()
	if obj, ok := s.objects[key(kind, name)]; ok {
		cp := *obj
		return &cp
	}
	return nil
}

func TestNodeProvider_Object(t *testing.T) {
	obj := NodeProvider("hetzner-eu", "https://provider.example:8443/providers/hetzner-eu", "nodescaler/hetzner-eu/token",
		[]GroupSpec{
			{Name: "fsn1", Max: 20, Parameters: map[string]any{"sizes": []any{"cx22", "cx32"}}},
			{Name: "nbg1"},
		}, map[string]any{"type": "hcloud"}).Object()
	if obj.Group != "norca.io" || obj.Kind != "NodeProvider" || obj.Name != "hetzner-eu" || obj.ManagedBy != ManagedBy {
		t.Fatalf("object: %+v", obj)
	}
	want := map[string]any{
		"endpoint":       "https://provider.example:8443/providers/hetzner-eu",
		"token_variable": "nodescaler/hetzner-eu/token",
		"groups": []any{
			map[string]any{"name": "fsn1", "max": 20, "parameters": map[string]any{"sizes": []any{"cx22", "cx32"}}},
			map[string]any{"name": "nbg1"},
		},
		"parameters": map[string]any{"type": "hcloud"},
	}
	if !reflect.DeepEqual(obj.Spec, want) {
		t.Fatalf("spec:\n got %#v\nwant %#v", obj.Spec, want)
	}
}

func TestLoadBalancerClass_Object(t *testing.T) {
	obj := LoadBalancerClass("nginx-edge", "https://lb.example:8443/classes/nginx-edge", "lb-provider/nginx-edge/token",
		true, "80,443", map[string]any{"advertise_address": "203.0.113.5"}).Object()
	if obj.Group != "norca.io" || obj.Kind != "LoadBalancerClass" || obj.Name != "nginx-edge" || obj.ManagedBy != ManagedBy {
		t.Fatalf("object: %+v", obj)
	}
	want := map[string]any{
		"endpoint":      "https://lb.example:8443/classes/nginx-edge",
		"tokenVariable": "lb-provider/nginx-edge/token",
		"capabilities":  map[string]any{"internal": true, "ports": "80,443"},
		"parameters":    map[string]any{"advertise_address": "203.0.113.5"},
	}
	if !reflect.DeepEqual(obj.Spec, want) {
		t.Fatalf("spec:\n got %#v\nwant %#v", obj.Spec, want)
	}
	// Without a token, ports and parameters.
	bare := LoadBalancerClass("mock", "http://x/classes/mock", "", false, "", nil).Object()
	if !reflect.DeepEqual(bare.Spec, map[string]any{"endpoint": "http://x/classes/mock", "capabilities": map[string]any{"internal": false}}) {
		t.Fatalf("bare spec: %#v", bare.Spec)
	}
}

// One registrar keeps the objects of both kinds: the same status (heartbeat
// and version) for both, registered again when removed, removed on exit.
func TestRegistrar_BothKinds(t *testing.T) {
	store := &fakeStore{}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	r := &Registrar{
		Store: store,
		Registrations: []Registration{
			NodeProvider("mock", "http://127.0.0.1:8480/providers/mock", "", []GroupSpec{{Name: "dc1"}}, nil),
			LoadBalancerClass("mock-lb", "http://127.0.0.1:8480/classes/mock-lb", "", true, "", nil),
		},
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

	for _, k := range [][2]string{{KindNodeProvider, "mock"}, {KindLoadBalancerClass, "mock-lb"}} {
		waitFor(t, func() bool {
			obj := store.get(k[0], k[1])
			return obj != nil && obj.Status["heartbeat"] != nil && obj.Status["version"] == "0.1.0"
		})
		first := store.get(k[0], k[1]).Status["heartbeat"]
		waitFor(t, func() bool { return store.get(k[0], k[1]).Status["heartbeat"] != first })
	}

	// An object is removed by someone: the next heartbeat registers it again.
	_ = store.Delete(context.Background(), KindLoadBalancerClass, "mock-lb")
	waitFor(t, func() bool { return store.get(KindLoadBalancerClass, "mock-lb") != nil })

	cancel()
	<-done
	if store.get(KindNodeProvider, "mock") != nil || store.get(KindLoadBalancerClass, "mock-lb") != nil {
		t.Fatal("a registration is left after the exit")
	}
}

// failingStore fails the first applies.
type failingStore struct {
	*fakeStore
	failures int
}

func (s *failingStore) Apply(ctx context.Context, obj *api.CustomObject) error {
	s.mu.Lock()
	if s.failures > 0 {
		s.failures--
		s.mu.Unlock()
		return http.ErrHandlerTimeout
	}
	s.mu.Unlock()
	return s.fakeStore.Apply(ctx, obj)
}

func TestRegistrationIsRetried(t *testing.T) {
	store := &failingStore{fakeStore: &fakeStore{}, failures: 2}
	r := &Registrar{Store: store, Interval: time.Hour, Retry: 5 * time.Millisecond,
		Registrations: []Registration{LoadBalancerClass("a", "http://x/classes/a", "", false, "", nil)}}
	go func() { _ = r.Run(t.Context()) }()
	waitFor(t, func() bool { return store.get(KindLoadBalancerClass, "a") != nil })
}

// TestAPIStore checks the requests against the shape of the Norca API, for
// both kinds.
func TestAPIStore(t *testing.T) {
	type call struct{ method, path string }
	var mu sync.Mutex
	var calls []call
	missing := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		mu.Lock()
		calls = append(calls, call{r.Method, r.URL.Path})
		notFound := missing
		mu.Unlock()
		if notFound {
			http.Error(w, "object not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	cfg := api.DefaultConfig()
	cfg.Address = srv.URL
	client, err := api.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewAPIStore(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, reg := range []Registration{
		LoadBalancerClass("nginx-edge", "https://x/classes/nginx-edge", "", false, "", nil),
		NodeProvider("lab", "https://x/providers/lab", "", nil, nil),
	} {
		if err := store.Apply(ctx, reg.Object()); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateStatus(ctx, reg.Kind, reg.Name, map[string]any{"heartbeat": "x"}); err != nil {
			t.Fatal(err)
		}
		if err := store.Delete(ctx, reg.Kind, reg.Name); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	got := append([]call(nil), calls...)
	missing = true
	mu.Unlock()

	want := []call{
		{http.MethodPut, "/v1/object/norca.io/LoadBalancerClass/nginx-edge"},
		{http.MethodPut, "/v1/object/norca.io/LoadBalancerClass/nginx-edge/status"},
		{http.MethodDelete, "/v1/object/norca.io/LoadBalancerClass/nginx-edge"},
		{http.MethodPut, "/v1/object/norca.io/NodeProvider/lab"},
		{http.MethodPut, "/v1/object/norca.io/NodeProvider/lab/status"},
		{http.MethodDelete, "/v1/object/norca.io/NodeProvider/lab"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("calls:\n got %+v\nwant %+v", got, want)
	}

	// A missing object maps to ErrNotFound on a status update, and a delete
	// of a missing object is not an error.
	if err := store.UpdateStatus(ctx, KindNodeProvider, "lab", map[string]any{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("status of a missing object: %v", err)
	}
	if err := store.Delete(ctx, KindNodeProvider, "lab"); err != nil {
		t.Fatalf("delete of a missing object: %v", err)
	}
}

func TestConfigureUnixSocket(t *testing.T) {
	conf := api.DefaultConfig()
	conf.Address = "unix:///secrets/api.sock"
	if err := configureUnixSocket(conf); err != nil || conf.Address != "http://127.0.0.1" || conf.HttpClient == nil {
		t.Fatalf("unix socket: %v %q", err, conf.Address)
	}
	conf.Address = "unix://"
	if err := configureUnixSocket(conf); err == nil {
		t.Fatal("an empty socket path is accepted")
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
