// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package gcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/lb"
)

type fakeGCPServer struct {
	mu              sync.Mutex
	token           string
	healthChecks    map[string]gcpHealthCheck
	negs            map[string]gcpNEG
	endpoints       map[string][]gcpEndpoint
	backendServices map[string]gcpBackendService
	forwardingRules map[string]gcpForwardingRule
}

func newFakeGCP(t *testing.T, token string) (*fakeGCPServer, *httptest.Server) {
	t.Helper()
	f := &fakeGCPServer{
		token:           token,
		healthChecks:    map[string]gcpHealthCheck{},
		negs:            map[string]gcpNEG{},
		endpoints:       map[string][]gcpEndpoint{},
		backendServices: map[string]gcpBackendService{},
		forwardingRules: map[string]gcpForwardingRule{},
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeGCPServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	const prefix = "/compute/v1/projects/my-proj/regions/europe-west1/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, prefix), "/")
	collection := parts[0]

	switch collection {
	case "healthChecks":
		if len(parts) == 1 && r.Method == http.MethodPost {
			var hc gcpHealthCheck
			_ = json.NewDecoder(r.Body).Decode(&hc)
			f.healthChecks[hc.Name] = hc
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
			return
		}
		name := parts[1]
		switch r.Method {
		case http.MethodGet:
			hc, ok := f.healthChecks[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(hc)
		case http.MethodPut:
			var hc gcpHealthCheck
			_ = json.NewDecoder(r.Body).Decode(&hc)
			f.healthChecks[name] = hc
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
		case http.MethodDelete:
			delete(f.healthChecks, name)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
		}

	case "networkEndpointGroups":
		if len(parts) == 1 && r.Method == http.MethodPost {
			var neg gcpNEG
			_ = json.NewDecoder(r.Body).Decode(&neg)
			f.negs[neg.Name] = neg
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
			return
		}
		name := parts[1]
		if len(parts) == 2 {
			switch r.Method {
			case http.MethodGet:
				neg, ok := f.negs[name]
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_ = json.NewEncoder(w).Encode(neg)
			case http.MethodDelete:
				delete(f.negs, name)
				delete(f.endpoints, name)
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
			}
			return
		}
		action := parts[2]
		switch action {
		case "listNetworkEndpoints":
			var resp gcpListEndpointsResponse
			for _, ep := range f.endpoints[name] {
				resp.Items = append(resp.Items, struct {
					NetworkEndpoint gcpEndpoint `json:"networkEndpoint"`
				}{NetworkEndpoint: ep})
			}
			_ = json.NewEncoder(w).Encode(resp)
		case "attachNetworkEndpoints":
			var b struct {
				NetworkEndpoints []gcpEndpoint `json:"networkEndpoints"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			f.endpoints[name] = append(f.endpoints[name], b.NetworkEndpoints...)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
		case "detachNetworkEndpoints":
			var b struct {
				NetworkEndpoints []gcpEndpoint `json:"networkEndpoints"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			f.endpoints[name] = slices.DeleteFunc(f.endpoints[name], func(ep gcpEndpoint) bool {
				return slices.Contains(b.NetworkEndpoints, ep)
			})
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
		}

	case "backendServices":
		if len(parts) == 1 && r.Method == http.MethodPost {
			var bs gcpBackendService
			_ = json.NewDecoder(r.Body).Decode(&bs)
			f.backendServices[bs.Name] = bs
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
			return
		}
		name := parts[1]
		switch r.Method {
		case http.MethodGet:
			bs, ok := f.backendServices[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(bs)
		case http.MethodPut:
			var bs gcpBackendService
			_ = json.NewDecoder(r.Body).Decode(&bs)
			f.backendServices[name] = bs
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
		case http.MethodDelete:
			delete(f.backendServices, name)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
		}

	case "forwardingRules":
		if len(parts) == 1 && r.Method == http.MethodPost {
			var fr gcpForwardingRule
			_ = json.NewDecoder(r.Body).Decode(&fr)
			if fr.LoadBalancingScheme == "INTERNAL" {
				fr.IPAddress = "10.128.0.50"
			} else {
				fr.IPAddress = "34.76.0.10"
			}
			f.forwardingRules[fr.Name] = fr
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
			return
		}
		name := parts[1]
		switch r.Method {
		case http.MethodGet:
			fr, ok := f.forwardingRules[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(fr)
		case http.MethodDelete:
			delete(f.forwardingRules, name)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "DONE"})
		}

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestGCPLifecycle(t *testing.T) {
	fake, srv := newFakeGCP(t, "gcp-oauth-token")

	inst, err := provider.Default.Instantiate("gcp-lb", TypeID, map[string]any{
		"project":    "my-proj",
		"region":     "europe-west1",
		"token":      "gcp-oauth-token",
		"network":    "default",
		"subnetwork": "subnet-1",
		"endpoint":   srv.URL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	if !inst.Provider.Capabilities().Internal {
		t.Fatalf("expected Internal capability for GCP provider")
	}

	ctx := context.Background()
	ref := lb.Ref{Namespace: "prod", Name: "checkout"}

	// 1. Create EXTERNAL load balancer.
	params1, _ := inst.Effective(nil)
	state, err := inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "checkout",
		Port:    80,
		Targets: []lb.Target{
			{Address: "10.128.0.1", Port: 8080},
			{Address: "10.128.0.2", Port: 8080},
		},
		Params: params1,
	})
	if err != nil {
		t.Fatalf("Ensure create: %v", err)
	}
	if !state.Ready || !slices.Equal(state.Addresses, []string{"34.76.0.10:80"}) {
		t.Fatalf("unexpected state: %+v", state)
	}

	// 2. Reconcile endpoints, port, and switch to INTERNAL scheme.
	params2, _ := inst.Effective(map[string]any{"network_tier": "STANDARD"})
	state, err = inst.Provider.Ensure(ctx, lb.Request{
		Ref:      ref,
		Service:  "checkout",
		Port:     8443,
		Internal: true,
		Targets: []lb.Target{
			{Address: "10.128.0.2", Port: 9443},
			{Address: "10.128.0.3", Port: 9443},
		},
		Params: params2,
	})
	if err != nil {
		t.Fatalf("Ensure update: %v", err)
	}
	if !state.Ready || !slices.Equal(state.Addresses, []string{"10.128.0.50:8443"}) {
		t.Fatalf("unexpected internal state: %+v", state)
	}

	fake.mu.Lock()
	eps := fake.endpoints["norca-prod-checkout"]
	if len(eps) != 2 || eps[0].IPAddress != "10.128.0.2" || eps[1].IPAddress != "10.128.0.3" {
		t.Fatalf("unexpected NEG endpoints: %+v", eps)
	}
	fake.mu.Unlock()

	// 3. Delete removes all 4 regional resources idempotently.
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	fake.mu.Lock()
	if len(fake.forwardingRules)+len(fake.backendServices)+len(fake.negs)+len(fake.healthChecks) != 0 {
		t.Fatalf("expected all resources deleted")
	}
	fake.mu.Unlock()
}
