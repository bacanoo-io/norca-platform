// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider"
)

func TestPrintTypes(t *testing.T) {
	var buf bytes.Buffer
	printTypes(&buf, provider.Default)
	out := buf.String()
	for _, want := range []string{
		"mock - in memory, for trials and tests [node, loadbalancer]",
		"docker - ", "[node]", "nginx - ", "tcpproxy - ", "hcloud - ",
		"node.groups[].params:", "loadbalancer.classes[].params:", "config_dir", "advertise_address", "host_addresses",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestRun_Flags(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"-version"}, &out); err != nil || strings.TrimSpace(out.String()) != "dev" {
		t.Fatalf("-version: %q %v", out.String(), err)
	}
	if err := run(context.Background(), []string{"-serve", "both"}, &out); err == nil || !strings.Contains(err.Error(), "want node, lb or all") {
		t.Fatalf("-serve both: %v", err)
	}
	cfg := filepath.Join(t.TempDir(), "providers.json")
	_ = os.WriteFile(cfg, []byte(`{"advertise_url":"http://x","providers":[{"name":"p","type":"nginx","no_auth":true,
	  "node":{"groups":[{"name":"dc1"}]}}]}`), 0o600)
	if err := run(context.Background(), []string{"-config", cfg, "-register=false"}, &out); err == nil ||
		!strings.Contains(err.Error(), "type nginx has no node provider") {
		t.Fatalf("capability mismatch: %v", err)
	}
}

// The mock serves a node provider and a class from one process: both are
// registered against the Norca API, and removed on exit.
func TestRun_RegistersBothAndDeregisters(t *testing.T) {
	var mu sync.Mutex
	applied := map[string]int{}
	deleted := map[string]int{}
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		switch {
		case r.Method == http.MethodPut && !strings.HasSuffix(r.URL.Path, "/status"):
			applied[r.URL.Path]++
		case r.Method == http.MethodDelete:
			deleted[r.URL.Path]++
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer apiSrv.Close()

	t.Setenv("NORCA_ADDR", apiSrv.URL)
	cfgPath := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(cfgPath, []byte(`{
		"listen": "127.0.0.1:0",
		"advertise_url": "https://provider.example.com:8443",
		"heartbeat_interval": "50ms",
		"providers": [{"name": "lab", "type": "mock", "no_auth": true,
			"node": {"groups": [{"name": "dc1", "max": 2, "params": {"cpu": 1000, "memory": 1024}}]},
			"loadbalancer": {"classes": [{"class": "mock-edge"}]}}]
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-config", cfgPath, "-log-level", "error"}, &bytes.Buffer{}) }()

	const np, lbc = "/v1/object/norca.io/NodeProvider/lab", "/v1/object/norca.io/LoadBalancerClass/mock-edge"
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		ok := applied[np] >= 1 && applied[lbc] >= 1
		mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for the registrations")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if deleted[np] != 1 || deleted[lbc] != 1 {
		t.Fatalf("deregistrations before run returned: %v", deleted)
	}
}
