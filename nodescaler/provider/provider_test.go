// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package provider_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider/mock"
)

// serve serves a mock provider with two groups over the HTTP contract and
// returns a client of it.
func serve(t *testing.T, token string) *provider.Client {
	t.Helper()
	p := mock.New(map[string]mock.GroupConfig{
		"fsn1": {Capacity: provider.Capacity{CPU: 4000, Memory: 8192, Disk: 40000}, Max: 3},
		"nbg1": {Capacity: provider.Capacity{CPU: 2000, Memory: 4096, Disk: 20000}},
	})
	srv := httptest.NewServer(provider.NewHandler(p, token, nil))
	t.Cleanup(srv.Close)
	return &provider.Client{Endpoint: srv.URL, Token: token}
}

func TestContract_GroupHasTheCapacityWithoutInstances(t *testing.T) {
	c := serve(t, "")
	g, err := c.Group(context.Background(), "fsn1", map[string]any{"size": "cx32"})
	if err != nil {
		t.Fatal(err)
	}
	if g.Capacity == nil || *g.Capacity != (provider.Capacity{CPU: 4000, Memory: 8192, Disk: 40000}) {
		t.Fatalf("capacity: %+v", g.Capacity)
	}
	if g.Instances == nil || len(g.Instances) != 0 {
		t.Fatalf("instances: %+v", g.Instances)
	}
}

func TestContract_CreateIsIdempotentByRequestID(t *testing.T) {
	c := serve(t, "")
	ctx := context.Background()
	req := provider.CreateRequest{
		RequestID: "eu/workers-dc-a/1", Count: 2,
		Labels:    map[string]string{"norca.node_group": "workers-dc-a"},
		Bootstrap: &provider.Bootstrap{UserData: "#cloud-config\n"},
	}
	first, err := c.Create(ctx, "fsn1", req)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].State != provider.StateRunning || first[0].Labels["norca.node_group"] != "workers-dc-a" {
		t.Fatalf("first: %+v", first)
	}
	again, err := c.Create(ctx, "fsn1", req)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 2 || again[0].ID != first[0].ID || again[1].ID != first[1].ID {
		t.Fatalf("again: %+v, first: %+v", again, first)
	}
	g, err := c.Group(ctx, "fsn1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Instances) != 2 {
		t.Fatalf("the same request created more: %+v", g.Instances)
	}
}

func TestContract_QuotaHasAReason(t *testing.T) {
	c := serve(t, "")
	_, err := c.Create(context.Background(), "fsn1", provider.CreateRequest{RequestID: "r1", Count: 4})
	if provider.ReasonOf(err) != provider.ReasonQuota {
		t.Fatalf("want a quota error, got %v", err)
	}
}

func TestContract_InvalidRequests(t *testing.T) {
	c := serve(t, "")
	ctx := context.Background()
	for name, req := range map[string]provider.CreateRequest{
		"no request id": {Count: 1},
		"no count":      {RequestID: "r1"},
	} {
		_, err := c.Create(ctx, "fsn1", req)
		if provider.ReasonOf(err) != provider.ReasonInvalid {
			t.Errorf("%s: want an invalid request, got %v", name, err)
		}
	}
	if _, err := c.Group(ctx, "nowhere", nil); !errors.Is(err, provider.ErrUnknownGroup) {
		t.Errorf("unknown group: %v", err)
	}
}

func TestContract_Delete(t *testing.T) {
	c := serve(t, "")
	ctx := context.Background()
	inst, err := c.Create(ctx, "nbg1", provider.CreateRequest{RequestID: "r1", Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, "nbg1", inst[0].ID); err != nil {
		t.Fatal(err)
	}
	// Deleting one that is gone is not an error.
	if err := c.Delete(ctx, "nbg1", inst[0].ID); err != nil {
		t.Fatal(err)
	}
	g, err := c.Group(ctx, "nbg1", nil)
	if err != nil || len(g.Instances) != 0 {
		t.Fatalf("after the delete: %+v %v", g, err)
	}
}

func TestContract_Token(t *testing.T) {
	c := serve(t, "s3cret")
	ctx := context.Background()
	if _, err := c.Group(ctx, "fsn1", nil); err != nil {
		t.Fatalf("with the token: %v", err)
	}
	wrong := *c
	wrong.Token = "guess"
	if _, err := wrong.Group(ctx, "fsn1", nil); err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("a wrong token is accepted: %v", err)
	}
	// The health check needs no token.
	resp, err := http.Get(c.Endpoint + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: %s", resp.Status)
	}
}
