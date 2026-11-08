// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package hcloud

import (
	"context"
	"encoding/json"
	"errors"
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

type fakeHCloudServer struct {
	mu     sync.Mutex
	nextID int64
	lbs    map[int64]*hcLoadBalancer
	auth   string
}

func newFakeHCloud(t *testing.T, token string) (*fakeHCloudServer, *httptest.Server) {
	t.Helper()
	f := &fakeHCloudServer{
		nextID: 42,
		lbs:    map[int64]*hcLoadBalancer{},
		auth:   "Bearer " + token,
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeHCloudServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != f.auth {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"code": "unauthorized", "message": "invalid token"},
		})
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/load_balancers":
		name := r.URL.Query().Get("name")
		var list []hcLoadBalancer
		for _, curLB := range f.lbs {
			if name == "" || curLB.Name == name {
				list = append(list, *curLB)
			}
		}
		_ = json.NewEncoder(w).Encode(hcListResponse{LoadBalancers: list})

	case r.Method == http.MethodPost && r.URL.Path == "/load_balancers":
		var body struct {
			Name             string            `json:"name"`
			LoadBalancerType string            `json:"load_balancer_type"`
			Algorithm        hcAlgorithm       `json:"algorithm"`
			PublicInterface  bool              `json:"public_interface"`
			Network          int               `json:"network"`
			Services         []hcService       `json:"services"`
			Targets          []hcTarget        `json:"targets"`
			Labels           map[string]string `json:"labels"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		id := f.nextID
		f.nextID++
		curLB := &hcLoadBalancer{
			ID:   id,
			Name: body.Name,
			PublicNet: hcPublicNet{
				Enabled: body.PublicInterface,
				IPv4:    hcPublicIP{IP: "203.0.113.10"},
			},
			LoadBalancerType: hcLBType{Name: body.LoadBalancerType},
			Algorithm:        body.Algorithm,
			Services:         body.Services,
			Targets:          body.Targets,
			Labels:           body.Labels,
		}
		if body.Network > 0 {
			curLB.PrivateNet = []hcPrivateNet{{Network: body.Network, IP: "10.0.0.10"}}
		}
		f.lbs[id] = curLB
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(hcSingleResponse{LoadBalancer: *curLB})

	case strings.HasPrefix(r.URL.Path, "/load_balancers/"):
		rest := strings.TrimPrefix(r.URL.Path, "/load_balancers/")
		parts := strings.Split(rest, "/")
		id, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		curLB, ok := f.lbs[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{"code": "not_found", "message": "load balancer not found"},
			})
			return
		}
		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(hcSingleResponse{LoadBalancer: *curLB})
			case http.MethodDelete:
				delete(f.lbs, id)
				w.WriteHeader(http.StatusNoContent)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
			return
		}
		if len(parts) == 3 && parts[1] == "actions" && r.Method == http.MethodPost {
			f.handleAction(w, r, curLB, parts[2])
			return
		}
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeHCloudServer) handleAction(w http.ResponseWriter, r *http.Request, curLB *hcLoadBalancer, action string) {
	switch action {
	case "change_type":
		var b struct {
			LoadBalancerType string `json:"load_balancer_type"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		curLB.LoadBalancerType.Name = b.LoadBalancerType
	case "change_algorithm":
		var b hcAlgorithm
		_ = json.NewDecoder(r.Body).Decode(&b)
		curLB.Algorithm = b
	case "attach_to_network":
		var b struct {
			Network int `json:"network"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		curLB.PrivateNet = append(curLB.PrivateNet, hcPrivateNet{Network: b.Network, IP: "10.0.0.10"})
	case "enable_public_interface":
		curLB.PublicNet.Enabled = true
	case "disable_public_interface":
		curLB.PublicNet.Enabled = false
	case "add_service":
		var svc hcService
		_ = json.NewDecoder(r.Body).Decode(&svc)
		curLB.Services = append(curLB.Services, svc)
	case "update_service":
		var svc hcService
		_ = json.NewDecoder(r.Body).Decode(&svc)
		for i := range curLB.Services {
			if curLB.Services[i].ListenPort == svc.ListenPort {
				curLB.Services[i] = svc
			}
		}
	case "delete_service":
		var b struct {
			ListenPort int `json:"listen_port"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		curLB.Services = slices.DeleteFunc(curLB.Services, func(s hcService) bool {
			return s.ListenPort == b.ListenPort
		})
	case "add_target":
		var t hcTarget
		_ = json.NewDecoder(r.Body).Decode(&t)
		curLB.Targets = append(curLB.Targets, t)
	case "remove_target":
		var t hcTarget
		_ = json.NewDecoder(r.Body).Decode(&t)
		curLB.Targets = slices.DeleteFunc(curLB.Targets, func(existing hcTarget) bool {
			return existing.Type == "ip" && existing.IP != nil && t.IP != nil && existing.IP.IP == t.IP.IP
		})
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"action": map[string]any{"status": "success"}})
}

func TestHCloudLifecycle(t *testing.T) {
	fake, srv := newFakeHCloud(t, "secret-token")

	inst, err := provider.Default.Instantiate("hetzner", TypeID, map[string]any{
		"token":      "secret-token",
		"location":   "fsn1",
		"network_id": 100,
		"endpoint":   srv.URL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	if !inst.Provider.Capabilities().Internal {
		t.Fatalf("expected Internal capability when network_id > 0")
	}

	ctx := context.Background()
	ref := lb.Ref{Namespace: "prod", Name: "web"}

	// 1. Initial Ensure creates LB with 2 targets.
	params1, err := inst.Effective(nil)
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	state, err := inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "web",
		Port:    443,
		Targets: []lb.Target{
			{Address: "10.0.1.1", Port: 8443},
			{Address: "10.0.1.2", Port: 8443},
		},
		Params: params1,
	})
	if err != nil {
		t.Fatalf("Ensure create: %v", err)
	}
	if !state.Ready || state.Resource != "hcloud/lb/42" {
		t.Fatalf("unexpected state: %+v", state)
	}
	if !slices.Equal(state.Addresses, []string{"203.0.113.10:443"}) {
		t.Fatalf("unexpected addresses: %v", state.Addresses)
	}

	// 2. Reconcile: change dynamic type, algorithm, port, internal=true, and target set.
	params2, err := inst.Effective(map[string]any{
		"type":      "lb21",
		"algorithm": "least_connections",
	})
	if err != nil {
		t.Fatalf("Effective override: %v", err)
	}
	state, err = inst.Provider.Ensure(ctx, lb.Request{
		Ref:      ref,
		Service:  "web",
		Port:     80,
		Internal: true,
		Targets: []lb.Target{
			{Address: "10.0.1.2", Port: 8080},
			{Address: "10.0.1.3", Port: 8080},
		},
		Params: params2,
	})
	if err != nil {
		t.Fatalf("Ensure update: %v", err)
	}
	if !state.Ready || !slices.Equal(state.Addresses, []string{"10.0.0.10:80"}) {
		t.Fatalf("unexpected internal state: %+v", state)
	}

	fake.mu.Lock()
	curLB := fake.lbs[42]
	if curLB.LoadBalancerType.Name != "lb21" || curLB.Algorithm.Type != "least_connections" {
		t.Fatalf("type/algorithm not updated: %+v %+v", curLB.LoadBalancerType, curLB.Algorithm)
	}
	if curLB.PublicNet.Enabled {
		t.Fatalf("expected public interface disabled for internal LB")
	}
	if len(curLB.Services) != 1 || curLB.Services[0].ListenPort != 80 || curLB.Services[0].DestinationPort != 8080 {
		t.Fatalf("unexpected services: %+v", curLB.Services)
	}
	var gotIPs []string
	for _, tgt := range curLB.Targets {
		gotIPs = append(gotIPs, tgt.IP.IP)
	}
	slices.Sort(gotIPs)
	if !slices.Equal(gotIPs, []string{"10.0.1.2", "10.0.1.3"}) {
		t.Fatalf("unexpected targets: %v", gotIPs)
	}
	fake.mu.Unlock()

	// 3. Empty targets reports Ready=false with "no targets".
	state, err = inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "web",
		Port:    80,
		Targets: nil,
		Params:  params2,
	})
	if err != nil {
		t.Fatalf("Ensure empty targets: %v", err)
	}
	if state.Ready || state.Message != "no targets" {
		t.Fatalf("expected not ready with 'no targets', got %+v", state)
	}

	// 4. Delete is idempotent.
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
}

func TestHCloudValidationErrors(t *testing.T) {
	_, srv := newFakeHCloud(t, "secret-token")

	if _, err := provider.Default.Instantiate("bad", TypeID, map[string]any{
		"token":          "secret-token",
		"use_private_ip": true,
		"endpoint":       srv.URL,
	}); err == nil {
		t.Fatalf("expected error when use_private_ip=true without network_id")
	}

	inst, err := provider.Default.Instantiate("public-only", TypeID, map[string]any{
		"token":    "secret-token",
		"endpoint": srv.URL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}

	vals, _ := inst.Effective(nil)
	_, err = inst.Provider.Ensure(context.Background(), lb.Request{
		Ref:     lb.Ref{Namespace: "default", Name: "app"},
		Port:    80,
		Targets: []lb.Target{{Address: "10.0.0.1", Port: 8080}, {Address: "10.0.0.2", Port: 9090}},
		Params:  vals,
	})
	if !errors.Is(err, lb.ErrInvalid) {
		t.Fatalf("expected ErrInvalid on mismatched target ports, got %v", err)
	}
}
