// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package nginx

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// recorder replaces the reload command.
type recorder struct {
	mu      sync.Mutex
	calls   [][]string
	failing bool
}

func (r *recorder) run(_ context.Context, argv []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, argv)
	if r.failing {
		return errors.New("nginx: [emerg] bad configuration")
	}
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func newProvider(t *testing.T, dir string, extra map[string]any) (*Provider, *recorder) {
	t.Helper()
	raw := map[string]any{"config_dir": dir, "advertise_address": "203.0.113.5"}
	maps.Copy(raw, extra)
	values, err := params.Resolve(classParams, raw)
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewLB(lb.Config{Class: "nginx-edge", Values: values})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	p.(*Provider).Run = rec.run
	return p.(*Provider), rec
}

func request(name string, port int, targets ...string) lb.Request {
	req := lb.Request{Ref: lb.Ref{Namespace: "default", Name: name}, Service: "api", Port: port}
	for _, t := range targets {
		host, p, _ := strings.Cut(t, ":")
		n := 0
		for _, c := range p {
			n = n*10 + int(c-'0')
		}
		req.Targets = append(req.Targets, lb.Target{Address: host, Port: n})
	}
	return req
}

func TestEnsureWritesFileAndReloads(t *testing.T) {
	dir := t.TempDir()
	p, rec := newProvider(t, dir, nil)

	state, err := p.Ensure(context.Background(), request("api", 443, "10.0.0.2:8001", "10.0.0.1:8001"))
	if err != nil {
		t.Fatal(err)
	}
	if !state.Ready || len(state.Addresses) != 1 || state.Addresses[0] != "203.0.113.5:443" {
		t.Fatalf("state: %+v", state)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "default.api.conf"))
	if err != nil {
		t.Fatal(err)
	}
	conf := string(raw)
	for _, want := range []string{"listen 0.0.0.0:443;", "server 10.0.0.1:8001;", "server 10.0.0.2:8001;", "proxy_pass lb_"} {
		if !strings.Contains(conf, want) {
			t.Fatalf("missing %q in\n%s", want, conf)
		}
	}
	if strings.Index(conf, "10.0.0.1:8001") > strings.Index(conf, "10.0.0.2:8001") {
		t.Fatal("the targets are not sorted, so the file would change without a reason")
	}
	if rec.count() != 1 || rec.calls[0][0] != "nginx" {
		t.Fatalf("reloads: %v", rec.calls)
	}

	// The same request changes nothing, so nginx is not reloaded again.
	if _, err := p.Ensure(context.Background(), request("api", 443, "10.0.0.1:8001", "10.0.0.2:8001")); err != nil {
		t.Fatal(err)
	}
	if rec.count() != 1 {
		t.Fatalf("an unchanged load balancer reloaded nginx: %d", rec.count())
	}

	// No targets: a down upstream, not ready.
	state, err = p.Ensure(context.Background(), request("api", 443))
	if err != nil || state.Ready || state.Message == "" {
		t.Fatalf("no targets: %+v, %v", state, err)
	}
	raw, _ = os.ReadFile(filepath.Join(dir, "default.api.conf"))
	if !strings.Contains(string(raw), "down;") || rec.count() != 2 {
		t.Fatalf("no targets must still be a valid file:\n%s", raw)
	}

	if err := p.Delete(context.Background(), request("api", 443).Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "default.api.conf")); !os.IsNotExist(err) {
		t.Fatal("the file was not removed")
	}
	if rec.count() != 3 {
		t.Fatalf("reloads after delete: %d", rec.count())
	}
	// Deleting what is not there is fine and needs no reload.
	if err := p.Delete(context.Background(), request("api", 443).Ref); err != nil || rec.count() != 3 {
		t.Fatalf("second delete: %v, %d reloads", err, rec.count())
	}
}

func TestPortConflict(t *testing.T) {
	p, _ := newProvider(t, t.TempDir(), nil)
	if _, err := p.Ensure(context.Background(), request("a", 443, "10.0.0.1:1")); err != nil {
		t.Fatal(err)
	}
	_, err := p.Ensure(context.Background(), request("b", 443, "10.0.0.1:1"))
	if !errors.Is(err, lb.ErrInvalid) || !strings.Contains(err.Error(), "default/a") {
		t.Fatalf("a second load balancer on the same port: %v", err)
	}

	// Moving the first one frees the port.
	if _, err := p.Ensure(context.Background(), request("a", 8443, "10.0.0.1:1")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Ensure(context.Background(), request("b", 443, "10.0.0.1:1")); err != nil {
		t.Fatalf("the port should be free: %v", err)
	}
}

func TestRollbackWhenNginxRejects(t *testing.T) {
	dir := t.TempDir()
	p, rec := newProvider(t, dir, nil)
	good := request("api", 443, "10.0.0.1:1")
	if _, err := p.Ensure(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "default.api.conf"))

	rec.failing = true
	if _, err := p.Ensure(context.Background(), request("api", 443, "10.0.0.9:1")); err == nil ||
		!strings.Contains(err.Error(), "did not accept") {
		t.Fatalf("expected a reload error, got %v", err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "default.api.conf"))
	if string(after) != string(before) {
		t.Fatalf("the rejected configuration stayed:\n%s", after)
	}

	// A new load balancer that nginx rejects leaves no file behind.
	if _, err := p.Ensure(context.Background(), request("new", 444, "10.0.0.1:1")); err == nil {
		t.Fatal("expected an error")
	}
	if _, err := os.Stat(filepath.Join(dir, "default.new.conf")); !os.IsNotExist(err) {
		t.Fatal("a rejected new load balancer left its file")
	}
}

func TestInternalAndCapabilities(t *testing.T) {
	p, _ := newProvider(t, t.TempDir(), nil)
	if p.Capabilities().Internal {
		t.Fatal("no internal_listen_address, no internal load balancers")
	}
	req := request("api", 443, "10.0.0.1:1")
	req.Internal = true
	if _, err := p.Ensure(context.Background(), req); !errors.Is(err, lb.ErrInvalid) {
		t.Fatalf("internal without an address: %v", err)
	}

	p, _ = newProvider(t, t.TempDir(), map[string]any{"internal_listen_address": "10.1.0.5"})
	if c := p.Capabilities(); !c.Internal {
		t.Fatalf("capabilities: %+v", c)
	}
	state, err := p.Ensure(context.Background(), req)
	if err != nil || state.Addresses[0] != "10.1.0.5:443" {
		t.Fatalf("internal: %+v, %v", state, err)
	}
	raw, _ := os.ReadFile(filepath.Join(p.dir, "default.api.conf"))
	if !strings.Contains(string(raw), "listen 10.1.0.5:443;") {
		t.Fatalf("an internal load balancer must listen on the internal address:\n%s", raw)
	}
}

// TestRestart: the state is the directory, so a new instance knows the
// listeners of the files an earlier run wrote.
func TestRestart(t *testing.T) {
	dir := t.TempDir()
	p, _ := newProvider(t, dir, nil)
	if _, err := p.Ensure(context.Background(), request("a", 443, "10.0.0.1:1")); err != nil {
		t.Fatal(err)
	}

	again, rec := newProvider(t, dir, nil)
	if _, err := again.Ensure(context.Background(), request("b", 443, "10.0.0.1:1")); !errors.Is(err, lb.ErrInvalid) {
		t.Fatalf("the restarted provider forgot the port: %v", err)
	}
	// The same load balancer again is unchanged: no reload.
	if _, err := again.Ensure(context.Background(), request("a", 443, "10.0.0.1:1")); err != nil || rec.count() != 0 {
		t.Fatalf("unchanged after a restart: %v, %d reloads", err, rec.count())
	}
}

func TestNoReloadCommand(t *testing.T) {
	p, rec := newProvider(t, t.TempDir(), map[string]any{"reload_command": []any{}})
	if _, err := p.Ensure(context.Background(), request("api", 443, "10.0.0.1:1")); err != nil || rec.count() != 0 {
		t.Fatalf("an empty reload_command must not run anything: %v, %d", err, rec.count())
	}
}

func TestNewValidates(t *testing.T) {
	values, _ := params.Resolve(classParams, map[string]any{"config_dir": t.TempDir(), "advertise_address": "not-an-ip"})
	if _, err := NewLB(lb.Config{Class: "x", Values: values}); err == nil {
		t.Fatal("an invalid advertise_address must be rejected")
	}
}
