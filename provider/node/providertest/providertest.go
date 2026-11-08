// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package providertest checks that a node provider keeps the contract of
// package node, the way the node scaling operator relies on it. Run
// serves the provider with node.NewHandler and calls it with
// node.Client, over HTTP, so the checks see what the operator sees:
//
//   - a group reports the capacity of a machine also without instances, and
//     an empty list of instances, not none;
//   - a create is idempotent by its request ID, and the instances carry the
//     names, the labels and the request ID they were asked for;
//   - invalid requests and unknown groups are refused with their reason;
//   - a full group refuses with the reason quota and creates nothing;
//   - a deleted instance goes, and deleting it again is not an error;
//   - a restarted provider lists the instances of before and still knows
//     their request IDs (a provider keeps its state on the platform, not in
//     memory);
//   - the token is required, the health check is not;
//   - the addresses of an instance are IP addresses, and with
//     Config.Addresses a running instance reports at least one.
//
// A provider for a real platform runs it against a test account, with a
// group of its own; the mock and the docker provider of this repository run
// it in their tests.
package providertest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/node"
)

// Config describes the provider under test.
type Config struct {
	// New returns the provider under test, with no instances in Group and
	// QuotaGroup. Each check gets a new one.
	New func(t *testing.T) node.Provider

	// Restart returns a new provider of the same platform as p, as after a
	// restart of the application. nil leaves the restart check out.
	Restart func(t *testing.T, p node.Provider) node.Provider

	// Group is a group with room for at least two instances, and Params the
	// parameters of a machine kind of it.
	Group  string
	Params map[string]any

	// Capacity is the capacity Group reports for Params; nil checks only
	// that one is reported.
	Capacity *node.Capacity

	// QuotaGroup is a group that holds QuotaMax instances; creating
	// QuotaMax+1 must fail with node.ReasonQuota. Empty leaves the check
	// out.
	QuotaGroup string
	QuotaMax   int

	// Bootstrap returns the bootstrap of an instance, for a provider that
	// reads it (default: a cloud-init with nothing in it).
	Bootstrap func(t *testing.T, name string) *node.Bootstrap

	// Settle is how long a list may take to show a change, on a platform
	// whose lists lag behind its writes (default: no wait).
	Settle time.Duration

	// Addresses says that the provider reports the addresses of its
	// instances (optional in the contract): a running instance must then
	// list at least one. Without it only the form of reported addresses is
	// checked.
	Addresses bool
}

const token = "providertest-token"

// Run runs the checks as subtests of t.
func Run(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.New == nil || cfg.Group == "" {
		t.Fatal("providertest: Config.New and Config.Group are required")
	}
	if cfg.Bootstrap == nil {
		cfg.Bootstrap = func(*testing.T, string) *node.Bootstrap {
			return &node.Bootstrap{UserData: "#cloud-config\n"}
		}
	}
	c := checks{cfg: cfg}
	t.Run("GroupHasTheCapacityWithoutInstances", c.capacity)
	t.Run("CreateIsIdempotentByRequestID", c.idempotent)
	t.Run("InvalidRequestsAreRefused", c.invalid)
	t.Run("QuotaHasAReason", c.quota)
	t.Run("Delete", c.delete)
	t.Run("RestartKeepsTheInstances", c.restart)
	t.Run("Token", c.token)
	t.Run("Addresses", c.addresses)
}

type checks struct{ cfg Config }

// serve serves p over the HTTP contract and returns a client of it.
func serve(t *testing.T, p node.Provider) *node.Client {
	t.Helper()
	srv := httptest.NewServer(node.NewHandler(p, token, nil))
	t.Cleanup(srv.Close)
	return &node.Client{Endpoint: srv.URL, Token: token}
}

// request asks for the named instances, with a label and the bootstrap of
// each.
func (c checks) request(t *testing.T, id string, names ...string) node.CreateRequest {
	req := node.CreateRequest{RequestID: id, Parameters: c.cfg.Params,
		Labels: map[string]string{"norca.node_group": "providertest", "norca.request": id}}
	for _, n := range names {
		req.Instances = append(req.Instances, node.InstanceRequest{Name: n, Bootstrap: c.cfg.Bootstrap(t, n)})
	}
	return req
}

// names returns n instance names unique to the test.
func names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i+1)
	}
	return out
}

// eventually checks cond until it holds or Settle passes.
func (c checks) eventually(t *testing.T, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(c.cfg.Settle)
	for {
		ok, detail := cond()
		if ok {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s: %s", what, detail)
		}
		time.Sleep(min(time.Second, c.cfg.Settle/10+time.Millisecond))
	}
}

// ofRequest returns the instances of a group made by a request.
func ofRequest(g *node.Group, requestID string) []node.Instance {
	var out []node.Instance
	for _, inst := range g.Instances {
		if inst.RequestID == requestID && inst.State != node.StateDeleting {
			out = append(out, inst)
		}
	}
	return out
}

func (c checks) capacity(t *testing.T) {
	cl := serve(t, c.cfg.New(t))
	g, err := cl.Group(context.Background(), c.cfg.Group, c.cfg.Params)
	if err != nil {
		t.Fatal(err)
	}
	if g.Capacity == nil || g.Capacity.CPU <= 0 || g.Capacity.Memory <= 0 {
		t.Fatalf("a group reports the capacity of a machine, also without instances: %+v", g.Capacity)
	}
	if c.cfg.Capacity != nil && *g.Capacity != *c.cfg.Capacity {
		t.Fatalf("capacity %+v, want %+v", *g.Capacity, *c.cfg.Capacity)
	}
	if g.Instances == nil {
		t.Fatal("a group without instances lists none as an empty list, not null")
	}
}

func (c checks) idempotent(t *testing.T) {
	ctx := context.Background()
	cl := serve(t, c.cfg.New(t))
	want := names("pt-idem", 2)
	req := c.request(t, "providertest/idempotent/1", want...)
	first, err := cl.Create(ctx, c.cfg.Group, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("%d instances for a request of 2: %+v", len(first), first)
	}
	for i, inst := range first {
		switch {
		case inst.ID == "":
			t.Fatalf("instance %d has no ID", i)
		case inst.Name != want[i]:
			t.Fatalf("instance %d is named %q, asked for %q: the node is matched by the name", i, inst.Name, want[i])
		case inst.RequestID != req.RequestID:
			t.Fatalf("instance %d has the request %q, want %q", i, inst.RequestID, req.RequestID)
		case inst.Labels["norca.node_group"] != "providertest":
			t.Fatalf("instance %d lost the labels of the request: %v", i, inst.Labels)
		case inst.State != node.StateCreating && inst.State != node.StateRunning:
			t.Fatalf("instance %d is %q, want creating or running", i, inst.State)
		}
	}
	again, err := cl.Create(ctx, c.cfg.Group, req)
	if err != nil {
		t.Fatalf("the same request again: %v", err)
	}
	ids := func(list []node.Instance) []string {
		var out []string
		for _, inst := range list {
			out = append(out, inst.ID)
		}
		slices.Sort(out)
		return out
	}
	if !slices.Equal(ids(again), ids(first)) {
		t.Fatalf("the same request again returned %v, the first %v: it must create nothing", ids(again), ids(first))
	}
	c.eventually(t, "the group lists the instances of the request once", func() (bool, string) {
		g, err := cl.Group(ctx, c.cfg.Group, c.cfg.Params)
		if err != nil {
			return false, err.Error()
		}
		got := ofRequest(g, req.RequestID)
		return slices.Equal(ids(got), ids(first)), fmt.Sprintf("listed %v, created %v", ids(got), ids(first))
	})
}

func (c checks) invalid(t *testing.T) {
	ctx := context.Background()
	cl := serve(t, c.cfg.New(t))
	b := c.cfg.Bootstrap(t, "pt-invalid")
	for what, req := range map[string]node.CreateRequest{
		"no request id": {Instances: []node.InstanceRequest{{Name: "pt-invalid", Bootstrap: b}}},
		"no instance":   {RequestID: "providertest/invalid/1"},
		"an instance without a name": {RequestID: "providertest/invalid/2",
			Instances: []node.InstanceRequest{{Bootstrap: b}}},
		"the same name twice": {RequestID: "providertest/invalid/3",
			Instances: []node.InstanceRequest{{Name: "pt-invalid", Bootstrap: b}, {Name: "pt-invalid", Bootstrap: b}}},
	} {
		req.Parameters = c.cfg.Params
		if _, err := cl.Create(ctx, c.cfg.Group, req); node.ReasonOf(err) != node.ReasonInvalid {
			t.Errorf("%s: want the reason %q, got %v", what, node.ReasonInvalid, err)
		}
	}
	if _, err := cl.Group(ctx, "providertest-no-such-group", nil); !errors.Is(err, node.ErrUnknownGroup) {
		t.Errorf("an unknown group: want node.ErrUnknownGroup, got %v", err)
	}
}

func (c checks) quota(t *testing.T) {
	if c.cfg.QuotaGroup == "" {
		t.Skip("no QuotaGroup")
	}
	ctx := context.Background()
	cl := serve(t, c.cfg.New(t))
	req := c.request(t, "providertest/quota/1", names("pt-quota", c.cfg.QuotaMax+1)...)
	if _, err := cl.Create(ctx, c.cfg.QuotaGroup, req); node.ReasonOf(err) != node.ReasonQuota {
		t.Fatalf("%d instances in a group of %d: want the reason %q, got %v", c.cfg.QuotaMax+1, c.cfg.QuotaMax,
			node.ReasonQuota, err)
	}
	g, err := cl.Group(ctx, c.cfg.QuotaGroup, c.cfg.Params)
	if err != nil {
		t.Fatal(err)
	}
	if left := ofRequest(g, req.RequestID); len(left) != 0 {
		t.Fatalf("a refused request left instances: %+v", left)
	}
}

func (c checks) delete(t *testing.T) {
	ctx := context.Background()
	cl := serve(t, c.cfg.New(t))
	req := c.request(t, "providertest/delete/1", names("pt-delete", 1)...)
	inst, err := cl.Create(ctx, c.cfg.Group, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.Delete(ctx, c.cfg.Group, inst[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := cl.Delete(ctx, c.cfg.Group, inst[0].ID); err != nil {
		t.Fatalf("deleting an instance that is gone is not an error: %v", err)
	}
	c.eventually(t, "a deleted instance goes (or is deleting)", func() (bool, string) {
		g, err := cl.Group(ctx, c.cfg.Group, c.cfg.Params)
		if err != nil {
			return false, err.Error()
		}
		left := ofRequest(g, req.RequestID)
		return len(left) == 0, fmt.Sprintf("listed %+v", left)
	})
}

func (c checks) restart(t *testing.T) {
	if c.cfg.Restart == nil {
		t.Skip("no Restart")
	}
	ctx := context.Background()
	p := c.cfg.New(t)
	req := c.request(t, "providertest/restart/1", names("pt-restart", 2)...)
	first, err := serve(t, p).Create(ctx, c.cfg.Group, req)
	if err != nil {
		t.Fatal(err)
	}
	cl := serve(t, c.cfg.Restart(t, p))
	c.eventually(t, "a restarted provider lists the instances of before", func() (bool, string) {
		g, err := cl.Group(ctx, c.cfg.Group, c.cfg.Params)
		if err != nil {
			return false, err.Error()
		}
		got := ofRequest(g, req.RequestID)
		var names []string
		for _, inst := range got {
			names = append(names, inst.Name)
		}
		slices.Sort(names)
		return len(got) == 2 && strings.Join(names, ",") == "pt-restart-1,pt-restart-2",
			fmt.Sprintf("listed %+v", got)
	})
	again, err := cl.Create(ctx, c.cfg.Group, req)
	if err != nil {
		t.Fatalf("the request of before, after the restart: %v", err)
	}
	if len(again) != 2 || !slices.ContainsFunc(again, func(i node.Instance) bool { return i.ID == first[0].ID }) {
		t.Fatalf("after a restart the request of before created more: %+v, before %+v", again, first)
	}
}

func (c checks) addresses(t *testing.T) {
	ctx := context.Background()
	cl := serve(t, c.cfg.New(t))
	req := c.request(t, "providertest/addresses/1", names("pt-addr", 1)...)
	created, err := cl.Create(ctx, c.cfg.Group, req)
	if err != nil {
		t.Fatal(err)
	}
	valid := func(list []node.Instance) string {
		for _, inst := range list {
			for _, a := range inst.Addresses {
				if _, err := netip.ParseAddr(a); err != nil {
					return fmt.Sprintf("instance %s reports %q, not an IP address", inst.ID, a)
				}
			}
		}
		return ""
	}
	if bad := valid(created); bad != "" {
		t.Fatal(bad)
	}
	c.eventually(t, "the addresses of a running instance", func() (bool, string) {
		g, err := cl.Group(ctx, c.cfg.Group, c.cfg.Params)
		if err != nil {
			return false, err.Error()
		}
		got := ofRequest(g, req.RequestID)
		if bad := valid(got); bad != "" {
			return false, bad
		}
		if !c.cfg.Addresses {
			return true, ""
		}
		return len(got) == 1 && got[0].State == node.StateRunning && len(got[0].Addresses) > 0,
			fmt.Sprintf("listed %+v: a running instance reports at least one address", got)
	})
}

func (c checks) token(t *testing.T) {
	ctx := context.Background()
	cl := serve(t, c.cfg.New(t))
	wrong := *cl
	wrong.Token = "guess"
	if _, err := wrong.Group(ctx, c.cfg.Group, c.cfg.Params); err == nil || !strings.Contains(err.Error(), "token") {
		t.Fatalf("a wrong token is accepted: %v", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, cl.Endpoint+"/v1/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(hreq)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the health check needs no token: %s", resp.Status)
	}
}
