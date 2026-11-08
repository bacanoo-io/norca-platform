// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package lbtest checks that a load balancer provider keeps the contract of
// package lb, the way the LoadBalancer operator relies on it. Run serves the
// provider with lb.NewHandler and calls it with lb.Client, over HTTP, so the
// checks see what the operator sees:
//
//   - an ensure is idempotent: the same request again reports the same
//     resource and addresses;
//   - the targets can change, and become none;
//   - a load balancer is deleted, and deleting it again, or one that never
//     existed, is not an error;
//   - the token is required;
//   - an internal load balancer is refused by a class without the
//     capability, before the provider is called.
//
// A provider for a real platform runs it against a test account or a fake
// of the platform's API; the mock of this repository runs it in its tests.
package lbtest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// Config describes the provider under test.
type Config struct {
	// New returns the provider of the class under test.
	New func(t *testing.T) lb.Provider

	// Params are the parameters of the class (shared and class ones,
	// resolved), with which the calls are made.
	Params params.Values

	// Port is the frontend port of the load balancers (default 8080).
	Port int

	// Targets are two addresses the provider accepts as targets (default
	// 10.0.0.1 and 10.0.0.2).
	Targets []string
}

// Run runs the checks.
func Run(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.Port == 0 {
		cfg.Port = 8080
	}
	if len(cfg.Targets) < 2 {
		cfg.Targets = []string{"10.0.0.1", "10.0.0.2"}
	}
	newClient := func(t *testing.T, token string) (*lb.Client, *lb.Class) {
		t.Helper()
		p := cfg.New(t)
		c := &lb.Class{Name: "conformance", Type: "test", Provider: p, Values: cfg.Params, Caps: p.Capabilities()}
		srv := httptest.NewServer(lb.NewHandler(c, token, nil))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { _ = c.Close() })
		return &lb.Client{Endpoint: srv.URL, Token: token}, c
	}
	ctx := func(t *testing.T) context.Context {
		c, cancel := context.WithTimeout(context.Background(), time.Minute)
		t.Cleanup(cancel)
		return c
	}
	req := func(targets ...string) lb.EnsureRequest {
		r := lb.EnsureRequest{Service: "web", Port: cfg.Port, Targets: []lb.WireTarget{}}
		for i, a := range targets {
			r.Targets = append(r.Targets, lb.WireTarget{Address: a, Port: 9000 + i, AllocID: "alloc-" + a})
		}
		return r
	}
	ref := lb.Ref{Namespace: "default", Name: "conformance"}

	t.Run("ensure is idempotent", func(t *testing.T) {
		c, _ := newClient(t, "")
		first, err := c.Ensure(ctx(t), ref, req(cfg.Targets[0], cfg.Targets[1]))
		if err != nil {
			t.Fatalf("ensure: %v", err)
		}
		again, err := c.Ensure(ctx(t), ref, req(cfg.Targets[0], cfg.Targets[1]))
		if err != nil {
			t.Fatalf("ensure again: %v", err)
		}
		if again.Resource != first.Resource || !slices.Equal(again.Addresses, first.Addresses) {
			t.Fatalf("a repeated ensure changed the load balancer: %+v, then %+v", first, again)
		}
		if first.Addresses == nil {
			t.Fatal("the addresses are null, not a list")
		}
	})

	t.Run("targets change", func(t *testing.T) {
		c, _ := newClient(t, "")
		if _, err := c.Ensure(ctx(t), ref, req(cfg.Targets[0])); err != nil {
			t.Fatalf("ensure: %v", err)
		}
		if _, err := c.Ensure(ctx(t), ref, req(cfg.Targets[1])); err != nil {
			t.Fatalf("other targets: %v", err)
		}
		st, err := c.Ensure(ctx(t), ref, req())
		if err != nil {
			t.Fatalf("no targets: %v", err)
		}
		if st.Ready && st.Message != "" {
			t.Fatalf("a ready load balancer has a message: %+v", st)
		}
	})

	t.Run("delete", func(t *testing.T) {
		c, _ := newClient(t, "")
		if _, err := c.Ensure(ctx(t), ref, req(cfg.Targets[0])); err != nil {
			t.Fatalf("ensure: %v", err)
		}
		if err := c.Delete(ctx(t), ref); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if err := c.Delete(ctx(t), ref); err != nil {
			t.Fatalf("delete again: %v", err)
		}
		if err := c.Delete(ctx(t), lb.Ref{Namespace: "default", Name: "never-created"}); err != nil {
			t.Fatalf("delete of an unknown load balancer: %v", err)
		}
	})

	t.Run("token", func(t *testing.T) {
		c, _ := newClient(t, "s3cret")
		c.Token = "wrong"
		_, err := c.Ensure(ctx(t), ref, req(cfg.Targets[0]))
		var re *lb.ResponseError
		if !errors.As(err, &re) || re.Status != http.StatusUnauthorized {
			t.Fatalf("a wrong token: %v", err)
		}
		c.Token = "s3cret"
		if _, err := c.Ensure(ctx(t), ref, req(cfg.Targets[0])); err != nil {
			t.Fatalf("the right token: %v", err)
		}
		_ = c.Delete(ctx(t), ref)
	})

	t.Run("internal without the capability", func(t *testing.T) {
		c, class := newClient(t, "")
		class.Caps.Internal = false
		r := req(cfg.Targets[0])
		r.Internal = true
		_, err := c.Ensure(ctx(t), ref, r)
		var re *lb.ResponseError
		if !errors.As(err, &re) || !re.Invalid() {
			t.Fatalf("an internal load balancer of a class without the capability: %v", err)
		}
	})
}
