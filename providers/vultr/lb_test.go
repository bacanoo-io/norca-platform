// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package vultr

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/lb"
)

type fakeVultrServer struct {
	mu        sync.Mutex
	created   int
	apiKey    string
	lbs       map[string]*vultrLoadBalancer
	instances []vultrInstance
}

func newFakeVultr(t *testing.T, apiKey string) (*fakeVultrServer, *httptest.Server) {
	t.Helper()
	f := &fakeVultrServer{
		apiKey: apiKey,
		lbs:    map[string]*vultrLoadBalancer{},
		instances: []vultrInstance{
			{ID: "inst-1", MainIP: "203.0.113.1", InternalIP: "10.20.0.1"},
			{ID: "inst-2", MainIP: "203.0.113.2", InternalIP: "10.20.0.2"},
			{ID: "inst-3", MainIP: "203.0.113.3", InternalIP: "10.20.0.3"},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeVultrServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+f.apiKey {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(vultrErrorResponse{Error: "unauthorized", Status: 401})
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/instances":
		_ = json.NewEncoder(w).Encode(vultrListInstancesResponse{Instances: f.instances})

	case r.Method == http.MethodGet && r.URL.Path == "/load-balancers":
		var list []vultrLoadBalancer
		for _, curLB := range f.lbs {
			list = append(list, *curLB)
		}
		_ = json.NewEncoder(w).Encode(vultrListLBsResponse{LoadBalancers: list})

	case r.Method == http.MethodPost && r.URL.Path == "/load-balancers":
		var curLB vultrLoadBalancer
		_ = json.NewDecoder(r.Body).Decode(&curLB)
		f.created++
		curLB.ID = "vultr-lb-" + strconv.Itoa(f.created)
		curLB.Status = "active"
		curLB.IP = "45.76.0.10"
		f.lbs[curLB.ID] = &curLB
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(vultrSingleLBResponse{LoadBalancer: curLB})

	case strings.HasPrefix(r.URL.Path, "/load-balancers/"):
		id := strings.TrimPrefix(r.URL.Path, "/load-balancers/")
		existing, ok := f.lbs[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(vultrSingleLBResponse{LoadBalancer: *existing})
		case http.MethodPatch:
			var patch vultrLoadBalancer
			_ = json.NewDecoder(r.Body).Decode(&patch)
			existing.BalancingAlgorithm = patch.BalancingAlgorithm
			existing.ProxyProtocol = patch.ProxyProtocol
			existing.ForwardingRules = patch.ForwardingRules
			existing.HealthCheck = patch.HealthCheck
			existing.Instances = patch.Instances
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			delete(f.lbs, id)
			w.WriteHeader(http.StatusNoContent)
		}

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestVultrLifecycle(t *testing.T) {
	fake, srv := newFakeVultr(t, "vultr-api-key")

	inst, err := provider.Default.Instantiate("vultr-eu", TypeID, map[string]any{
		"api_key":  "vultr-api-key",
		"region":   "fra",
		"vpc":      "vpc-1",
		"endpoint": srv.URL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	if !inst.Provider.Capabilities().Internal {
		t.Fatalf("expected Internal capability when vpc is configured")
	}

	ctx := context.Background()
	ref := lb.Ref{Namespace: "prod", Name: "payments"}

	// 1. Create LB with 2 target instances.
	params1, _ := inst.Effective(nil)
	state, err := inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "payments",
		Port:    80,
		Targets: []lb.Target{
			{Address: "10.20.0.1", Port: 8080},
			{Address: "10.20.0.2", Port: 8080},
		},
		Params: params1,
	})
	if err != nil {
		t.Fatalf("Ensure create: %v", err)
	}
	if !state.Ready || state.Resource != "vultr/lb/vultr-lb-1" {
		t.Fatalf("unexpected state: %+v", state)
	}
	if !slices.Equal(state.Addresses, []string{"45.76.0.10:80"}) {
		t.Fatalf("unexpected addresses: %v", state.Addresses)
	}

	// 2. Update: dynamic algorithm, proxy_protocol, port, and instance pool.
	params2, err := inst.Effective(map[string]any{
		"algorithm":      "leastconn",
		"proxy_protocol": true,
	})
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	state, err = inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "payments",
		Port:    443,
		Targets: []lb.Target{
			{Address: "10.20.0.2", Port: 8443},
			{Address: "10.20.0.3", Port: 8443},
		},
		Params: params2,
	})
	if err != nil {
		t.Fatalf("Ensure update: %v", err)
	}
	if !state.Ready || !slices.Equal(state.Addresses, []string{"45.76.0.10:443"}) {
		t.Fatalf("unexpected updated state: %+v", state)
	}

	fake.mu.Lock()
	curLB := fake.lbs["vultr-lb-1"]
	if curLB.BalancingAlgorithm != "leastconn" || !curLB.ProxyProtocol {
		t.Fatalf("unexpected LB settings: %+v", curLB)
	}
	if !slices.Equal(curLB.Instances, []string{"inst-2", "inst-3"}) {
		t.Fatalf("unexpected instances: %v", curLB.Instances)
	}
	if len(curLB.ForwardingRules) != 1 || curLB.ForwardingRules[0].FrontendPort != 443 || curLB.ForwardingRules[0].BackendPort != 8443 {
		t.Fatalf("unexpected forwarding rules: %+v", curLB.ForwardingRules)
	}
	fake.mu.Unlock()

	// 3. Delete is idempotent.
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
}

// TestVultrCollidingRefs: namespace team-a with load balancer web and
// namespace team with load balancer a-web got one cloud load balancer
// (review finding ATM-19): the Ensure of the second rewrote the instances of
// the first, and its Delete removed it.
func TestVultrCollidingRefs(t *testing.T) {
	fake, srv := newFakeVultr(t, "vultr-api-key")
	inst, err := provider.Default.Instantiate("vultr-eu", TypeID, map[string]any{
		"api_key":  "vultr-api-key",
		"region":   "fra",
		"endpoint": srv.URL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	params, err := inst.Effective(nil)
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	ensure := func(ref lb.Ref, ip string) {
		t.Helper()
		if _, err := inst.Provider.Ensure(context.Background(), lb.Request{
			Ref: ref, Service: "web", Port: 443,
			Targets: []lb.Target{{Address: ip, Port: 8443}},
			Params:  params,
		}); err != nil {
			t.Fatalf("Ensure %s: %v", ref, err)
		}
	}
	refA := lb.Ref{Namespace: "team-a", Name: "web"}
	refB := lb.Ref{Namespace: "team", Name: "a-web"}
	ensure(refA, "10.20.0.1")
	ensure(refB, "10.20.0.2")

	fake.mu.Lock()
	if len(fake.lbs) != 2 {
		t.Errorf("want a cloud load balancer for each ref, got %d", len(fake.lbs))
	}
	if curLB := fake.lbs["vultr-lb-1"]; curLB == nil || !slices.Equal(curLB.Instances, []string{"inst-1"}) {
		t.Errorf("instances of the load balancer of team-a/web: %+v", curLB)
	}
	fake.mu.Unlock()

	if err := inst.Provider.Delete(context.Background(), refB); err != nil {
		t.Fatalf("Delete B: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.lbs["vultr-lb-1"] == nil {
		t.Fatalf("the Delete of team/a-web removed the load balancer of team-a/web")
	}
}
