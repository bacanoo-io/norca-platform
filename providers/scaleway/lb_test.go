// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package scaleway

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

type fakeScalewayServer struct {
	mu          sync.Mutex
	secretKey   string
	lbs         map[string]*scwLB
	backends    map[string]*scwBackend
	frontends   map[string]*scwFrontend
	privNets    map[string][]scwPrivateNetwork
	initialStat string

	// lbOf is the load balancer of each backend and frontend; created
	// counts the objects created, for their IDs.
	lbOf    map[string]string
	created map[string]int
}

func newFakeScaleway(t *testing.T, secretKey string) (*fakeScalewayServer, *httptest.Server) {
	t.Helper()
	f := &fakeScalewayServer{
		secretKey:   secretKey,
		lbs:         map[string]*scwLB{},
		backends:    map[string]*scwBackend{},
		frontends:   map[string]*scwFrontend{},
		privNets:    map[string][]scwPrivateNetwork{},
		initialStat: "ready",
		lbOf:        map[string]string{},
		created:     map[string]int{},
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(srv.Close)
	return f, srv
}

// newID returns the ID of a new object of the kind: "lb-1", "lb-2", ...
func (f *fakeScalewayServer) newID(kind string) string {
	f.created[kind]++
	return kind + "-" + strconv.Itoa(f.created[kind])
}

func (f *fakeScalewayServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Auth-Token") != f.secretKey {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(scwErrorResponse{Message: "unauthorized", Type: "denied_authentication"})
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	const prefix = "/lb/v1/zones/fr-par-1"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sub := strings.TrimPrefix(r.URL.Path, prefix)

	switch {
	case r.Method == http.MethodGet && sub == "/lbs":
		name := r.URL.Query().Get("name")
		var list []scwLB
		for _, curLB := range f.lbs {
			if name == "" || curLB.Name == name {
				list = append(list, *curLB)
			}
		}
		_ = json.NewEncoder(w).Encode(scwListLBsResponse{LBs: list})

	case r.Method == http.MethodPost && sub == "/lbs":
		var body struct {
			Name             string   `json:"name"`
			Type             string   `json:"type"`
			AssignFlexibleIP bool     `json:"assign_flexible_ip"`
			Tags             []string `json:"tags"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		curLB := &scwLB{
			ID:     f.newID("lb"),
			Name:   body.Name,
			Status: f.initialStat,
			Type:   body.Type,
			Tags:   body.Tags,
		}
		if body.AssignFlexibleIP {
			curLB.IP = []scwIP{{ID: "ip-1", IPAddress: "51.15.0.10"}}
		} else {
			curLB.IP = []scwIP{{ID: "ip-priv", IPAddress: "172.16.0.10"}}
		}
		f.lbs[curLB.ID] = curLB
		_ = json.NewEncoder(w).Encode(curLB)

	case strings.HasPrefix(sub, "/lbs/"):
		parts := strings.Split(strings.TrimPrefix(sub, "/lbs/"), "/")
		lbID := parts[0]
		curLB, ok := f.lbs[lbID]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if len(parts) == 1 {
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(curLB)
			case http.MethodDelete:
				delete(f.lbs, lbID)
				w.WriteHeader(http.StatusNoContent)
			}
			return
		}
		switch parts[1] {
		case "migrate":
			var b struct {
				Type string `json:"type"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			curLB.Type = b.Type
			_ = json.NewEncoder(w).Encode(curLB)
		case "private-networks":
			_ = json.NewEncoder(w).Encode(scwListPrivateNetworksResponse{PrivateNetwork: f.privNets[lbID]})
		case "attach-private-network":
			var b struct {
				PrivateNetworkID string `json:"private_network_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			pn := scwPrivateNetwork{PrivateNetworkID: b.PrivateNetworkID, Status: "ready"}
			f.privNets[lbID] = append(f.privNets[lbID], pn)
			_ = json.NewEncoder(w).Encode(pn)
		case "backends":
			if r.Method == http.MethodGet {
				var list []scwBackend
				for _, be := range f.backends {
					if f.lbOf[be.ID] == lbID {
						list = append(list, *be)
					}
				}
				_ = json.NewEncoder(w).Encode(scwListBackendsResponse{Backends: list})
				return
			}
			var b struct {
				Name                 string          `json:"name"`
				ForwardProtocol      string          `json:"forward_protocol"`
				ForwardPort          int             `json:"forward_port"`
				ForwardPortAlgorithm string          `json:"forward_port_algorithm"`
				ServerIP             []string        `json:"server_ip"`
				HealthCheck          *scwHealthCheck `json:"health_check"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			be := &scwBackend{
				ID:                   f.newID("be"),
				Name:                 b.Name,
				ForwardProtocol:      b.ForwardProtocol,
				ForwardPort:          b.ForwardPort,
				ForwardPortAlgorithm: b.ForwardPortAlgorithm,
				Pool:                 b.ServerIP,
				HealthCheck:          b.HealthCheck,
			}
			f.backends[be.ID] = be
			f.lbOf[be.ID] = lbID
			_ = json.NewEncoder(w).Encode(be)
		case "frontends":
			if r.Method == http.MethodGet {
				var list []scwFrontend
				for _, fe := range f.frontends {
					if f.lbOf[fe.ID] == lbID {
						list = append(list, *fe)
					}
				}
				_ = json.NewEncoder(w).Encode(scwListFrontendsResponse{Frontends: list})
				return
			}
			var b struct {
				Name        string `json:"name"`
				InboundPort int    `json:"inbound_port"`
				BackendID   string `json:"backend_id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			fe := &scwFrontend{
				ID:          f.newID("fe"),
				Name:        b.Name,
				InboundPort: b.InboundPort,
				Backend:     &scwBackend{ID: b.BackendID},
			}
			f.frontends[fe.ID] = fe
			f.lbOf[fe.ID] = lbID
			_ = json.NewEncoder(w).Encode(fe)
		}

	case strings.HasPrefix(sub, "/backends/"):
		parts := strings.Split(strings.TrimPrefix(sub, "/backends/"), "/")
		be := f.backends[parts[0]]
		if be == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if len(parts) == 1 && r.Method == http.MethodPut {
			var b struct {
				Name                 string `json:"name"`
				ForwardProtocol      string `json:"forward_protocol"`
				ForwardPort          int    `json:"forward_port"`
				ForwardPortAlgorithm string `json:"forward_port_algorithm"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			be.ForwardProtocol = b.ForwardProtocol
			be.ForwardPort = b.ForwardPort
			be.ForwardPortAlgorithm = b.ForwardPortAlgorithm
			_ = json.NewEncoder(w).Encode(be)
			return
		}
		switch parts[1] {
		case "healthcheck":
			var hc scwHealthCheck
			_ = json.NewDecoder(r.Body).Decode(&hc)
			be.HealthCheck = &hc
			_ = json.NewEncoder(w).Encode(hc)
		case "servers":
			var b struct {
				ServerIP []string `json:"server_ip"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			be.Pool = b.ServerIP
			_ = json.NewEncoder(w).Encode(be)
		}

	case strings.HasPrefix(sub, "/frontends/"):
		feID := strings.TrimPrefix(sub, "/frontends/")
		fe := f.frontends[feID]
		if fe == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var b struct {
			Name        string `json:"name"`
			InboundPort int    `json:"inbound_port"`
			BackendID   string `json:"backend_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		fe.InboundPort = b.InboundPort
		fe.Backend = &scwBackend{ID: b.BackendID}
		_ = json.NewEncoder(w).Encode(fe)

	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestScalewayLifecycle(t *testing.T) {
	fake, srv := newFakeScaleway(t, "scw-secret")

	inst, err := provider.Default.Instantiate("scw", TypeID, map[string]any{
		"secret_key":         "scw-secret",
		"project_id":         "proj-123",
		"zone":               "fr-par-1",
		"private_network_id": "pn-1",
		"endpoint":           srv.URL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	if !inst.Provider.Capabilities().Internal {
		t.Fatalf("expected Internal capability when private_network_id is configured")
	}

	ctx := context.Background()
	ref := lb.Ref{Namespace: "prod", Name: "api"}

	// 1. Create public LB.
	params1, _ := inst.Effective(nil)
	state, err := inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "api",
		Port:    80,
		Targets: []lb.Target{
			{Address: "10.10.0.1", Port: 8080},
			{Address: "10.10.0.2", Port: 8080},
		},
		Params: params1,
	})
	if err != nil {
		t.Fatalf("Ensure create: %v", err)
	}
	if !state.Ready || state.Resource != "scaleway/fr-par-1/lb/lb-1" {
		t.Fatalf("unexpected state: %+v", state)
	}
	if !slices.Equal(state.Addresses, []string{"51.15.0.10:80"}) {
		t.Fatalf("unexpected addresses: %v", state.Addresses)
	}

	// 2. Update: migrate type, change algorithm, frontend port, and server pool.
	params2, err := inst.Effective(map[string]any{
		"type":      "LB-GP-M",
		"algorithm": "leastconn",
	})
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	state, err = inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "api",
		Port:    443,
		Targets: []lb.Target{
			{Address: "10.10.0.2", Port: 8443},
			{Address: "10.10.0.3", Port: 8443},
		},
		Params: params2,
	})
	if err != nil {
		t.Fatalf("Ensure update: %v", err)
	}
	if !state.Ready || !slices.Equal(state.Addresses, []string{"51.15.0.10:443"}) {
		t.Fatalf("unexpected updated state: %+v", state)
	}

	fake.mu.Lock()
	if fake.lbs["lb-1"].Type != "LB-GP-M" {
		t.Fatalf("expected migrated type LB-GP-M, got %s", fake.lbs["lb-1"].Type)
	}
	be := fake.backends["be-1"]
	if be.ForwardPort != 8443 || be.ForwardPortAlgorithm != "leastconn" {
		t.Fatalf("unexpected backend: %+v", be)
	}
	if !slices.Equal(be.Pool, []string{"10.10.0.2", "10.10.0.3"}) {
		t.Fatalf("unexpected pool: %v", be.Pool)
	}
	if fake.frontends["fe-1"].InboundPort != 443 {
		t.Fatalf("unexpected frontend port: %d", fake.frontends["fe-1"].InboundPort)
	}
	fake.mu.Unlock()

	// 3. Simulate pending LB status.
	fake.mu.Lock()
	fake.lbs["lb-1"].Status = "pending"
	fake.mu.Unlock()
	state, err = inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "api",
		Port:    443,
		Targets: []lb.Target{{Address: "10.10.0.2", Port: 8443}},
		Params:  params2,
	})
	if err != nil {
		t.Fatalf("Ensure pending: %v", err)
	}
	if state.Ready || state.Message != "load balancer status is pending" {
		t.Fatalf("expected pending state, got %+v", state)
	}

	// 4. Delete is idempotent.
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
}

func newTestClass(t *testing.T, srvURL string) (*lb.Class, *Provider) {
	t.Helper()
	inst, err := provider.Default.Instantiate("scw", TypeID, map[string]any{
		"secret_key": "scw-secret",
		"project_id": "proj-123",
		"zone":       "fr-par-1",
		"endpoint":   srvURL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	return inst, inst.Provider.(*Provider)
}

func ensureTarget(t *testing.T, inst *lb.Class, ref lb.Ref, ip string) error {
	t.Helper()
	params, err := inst.Effective(nil)
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	_, err = inst.Provider.Ensure(context.Background(), lb.Request{
		Ref: ref, Service: "web", Port: 443,
		Targets: []lb.Target{{Address: ip, Port: 8443}},
		Params:  params,
	})
	return err
}

// TestScalewayCollidingRefs: namespace team-a with load balancer web and
// namespace team with load balancer a-web got one cloud load balancer
// (review finding ATM-19): the Ensure of the second rewrote the backend
// servers of the first, and its Delete removed it.
func TestScalewayCollidingRefs(t *testing.T) {
	fake, srv := newFakeScaleway(t, "scw-secret")
	inst, _ := newTestClass(t, srv.URL)
	refA := lb.Ref{Namespace: "team-a", Name: "web"}
	refB := lb.Ref{Namespace: "team", Name: "a-web"}

	if err := ensureTarget(t, inst, refA, "10.0.1.1"); err != nil {
		t.Fatalf("Ensure A: %v", err)
	}
	if err := ensureTarget(t, inst, refB, "10.0.1.2"); err != nil {
		t.Fatalf("Ensure B: %v", err)
	}
	fake.mu.Lock()
	if len(fake.lbs) != 2 {
		t.Errorf("want a cloud load balancer for each ref, got %d", len(fake.lbs))
	}
	if be := fake.backends["be-1"]; be == nil || !slices.Equal(be.Pool, []string{"10.0.1.1"}) {
		t.Errorf("backend servers of the load balancer of team-a/web: %+v", be)
	}
	fake.mu.Unlock()

	if err := inst.Provider.Delete(context.Background(), refB); err != nil {
		t.Fatalf("Delete B: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.lbs["lb-1"] == nil {
		t.Fatalf("the Delete of team/a-web removed the load balancer of team-a/web")
	}
}

// TestScalewayForeignLoadBalancer: a cloud load balancer under the name of
// the ref without its owner tags is neither updated nor deleted (review
// finding ATM-19).
func TestScalewayForeignLoadBalancer(t *testing.T) {
	for name, tags := range map[string][]string{
		"no owner tags": nil,
		"another owner": {"norca-namespace=intruder", "norca-name=web", "norca-class=scw"},
		"another class": {"norca-namespace=prod", "norca-name=web", "norca-class=other"},
	} {
		t.Run(name, func(t *testing.T) {
			fake, srv := newFakeScaleway(t, "scw-secret")
			inst, p := newTestClass(t, srv.URL)
			ref := lb.Ref{Namespace: "prod", Name: "web"}
			fake.mu.Lock()
			fake.lbs["lb-9"] = &scwLB{ID: "lb-9", Name: p.resourceName(ref), Status: "ready", Type: "LB-S", Tags: tags}
			fake.backends["be-9"] = &scwBackend{ID: "be-9", Name: p.resourceName(ref), ForwardProtocol: "tcp",
				ForwardPort: 8443, ForwardPortAlgorithm: "roundrobin", Pool: []string{"192.0.2.9"}}
			fake.lbOf["be-9"] = "lb-9"
			fake.mu.Unlock()

			err := ensureTarget(t, inst, ref, "10.0.1.1")
			if !errors.Is(err, lb.ErrConflict) {
				t.Fatalf("Ensure: want a conflict, got %v", err)
			}
			if strings.Contains(err.Error(), "intruder") {
				t.Errorf("the conflict names the other owner: %v", err)
			}
			fake.mu.Lock()
			if len(fake.lbs) != 1 || len(fake.backends) != 1 || len(fake.frontends) != 0 ||
				!slices.Equal(fake.backends["be-9"].Pool, []string{"192.0.2.9"}) {
				t.Errorf("the foreign load balancer was changed: %d load balancers, %d backends, %d frontends, pool %v",
					len(fake.lbs), len(fake.backends), len(fake.frontends), fake.backends["be-9"].Pool)
			}
			fake.mu.Unlock()

			if err := inst.Provider.Delete(context.Background(), ref); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if fake.lbs["lb-9"] == nil {
				t.Fatalf("Delete removed the foreign load balancer")
			}
		})
	}
}
