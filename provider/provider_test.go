// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package provider_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca-platform/provider/params"
	"github.com/bacanoo-io/norca-platform/provider/registrar"
	"github.com/bacanoo-io/norca-platform/providers/mock"
)

func parse(t *testing.T, content string) *provider.File {
	t.Helper()
	f, err := provider.Parse([]byte(content), os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// testRegistry has a type with both providers, one with node only and one
// with load balancers only.
func testRegistry(t *testing.T) *provider.Registry {
	t.Helper()
	r := provider.NewRegistry()
	mockType, ok := provider.Default.Lookup(mock.TypeID)
	if !ok {
		t.Fatal("the mock type did not register itself")
	}
	r.Register(mockType)
	r.Register(provider.Type{ID: "nodeonly", Node: mockType.Node})
	r.Register(provider.Type{ID: "lbonly", LB: mockType.LB})
	return r
}

const both = `{
  "listen": "127.0.0.1:0",
  "advertise_url": "https://provider.service.norca:8443/",
  "heartbeat_interval": "15s",
  "providers": [{
    "name": "lab", "type": "mock", "token": "env:PROVIDER_TEST_TOKEN", "token_variable": "providers/lab",
    "node": {"groups": [
      {"name": "dc1", "max": 3, "params": {"cpu": 2000, "memory": 2048, "disk": 10000}},
      {"name": "dc2", "params": {"cpu": 4000, "memory": 8192}}
    ]},
    "loadbalancer": {"classes": [
      {"class": "mock-public", "params": {"address": "203.0.113.7"}, "internal": false, "ports": "80,443"},
      {"class": "mock-private", "endpoint": "http://10.0.0.5:18443/classes/mock-private"}
    ]}
  }]
}`

func TestBuild_BothSections(t *testing.T) {
	t.Setenv("PROVIDER_TEST_TOKEN", "s3cret")
	f := parse(t, both)
	if f.Listen != "127.0.0.1:0" || f.Interval() != 15*time.Second {
		t.Fatalf("listen %q, interval %v", f.Listen, f.Interval())
	}
	rt, err := f.Build(testRegistry(t), provider.ServeAll, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if len(rt.Nodes) != 1 || len(rt.Classes) != 2 {
		t.Fatalf("%d node providers, %d classes", len(rt.Nodes), len(rt.Classes))
	}
	n := rt.Nodes[0]
	if n.Endpoint != "https://provider.service.norca:8443/providers/lab" || n.Token != "s3cret" || n.TokenVariable != "providers/lab" {
		t.Fatalf("node provider: %+v", n)
	}
	if len(n.Groups) != 2 || n.Groups[0].Max != 3 || n.Groups[0].Parameters["cpu"] != 2000 || n.Groups[1].Parameters["disk"] != 0 {
		t.Fatalf("groups: %+v", n.Groups)
	}
	pub, priv := rt.Classes[0], rt.Classes[1]
	if pub.Endpoint != "https://provider.service.norca:8443/classes/mock-public" || pub.Token != "s3cret" ||
		pub.Caps.Internal || pub.Caps.Ports != "80,443" || pub.Published["address"] != "203.0.113.7" {
		t.Fatalf("public class: %+v %+v", pub, pub.Class)
	}
	// The mock can create internal load balancers: unset offers it.
	if priv.Endpoint != "http://10.0.0.5:18443/classes/mock-private" || !priv.Caps.Internal || priv.TokenVariable != "providers/lab" {
		t.Fatalf("private class: %+v", priv)
	}

	regs := rt.Registrations()
	if len(regs) != 3 || regs[0].Kind != registrar.KindNodeProvider || regs[1].Kind != registrar.KindLoadBalancerClass {
		t.Fatalf("registrations: %+v", regs)
	}
	if regs[0].Spec["token_variable"] != "providers/lab" || regs[0].Spec["parameters"].(map[string]any)["type"] != "mock" {
		t.Fatalf("NodeProvider spec: %#v", regs[0].Spec)
	}
	if regs[1].Spec["tokenVariable"] != "providers/lab" ||
		regs[1].Spec["capabilities"].(map[string]any)["ports"] != "80,443" {
		t.Fatalf("LoadBalancerClass spec: %#v", regs[1].Spec)
	}
}

func TestBuild_ServeFilter(t *testing.T) {
	t.Setenv("PROVIDER_TEST_TOKEN", "s3cret")
	reg := testRegistry(t)
	f := parse(t, both)
	for serve, want := range map[provider.Serve][2]int{provider.ServeNode: {1, 0}, provider.ServeLB: {0, 2}, provider.ServeAll: {1, 2}} {
		rt, err := f.Build(reg, serve, nil)
		if err != nil {
			t.Fatalf("%s: %v", serve, err)
		}
		if len(rt.Nodes) != want[0] || len(rt.Classes) != want[1] {
			t.Errorf("%s: %d node providers, %d classes", serve, len(rt.Nodes), len(rt.Classes))
		}
		rt.Close()
	}
	// The filter only narrows: a configuration without classes has nothing
	// to serve with -serve lb.
	nodeOnly := parse(t, `{"advertise_url":"http://x","providers":[{"name":"n","type":"mock","no_auth":true,
	  "node":{"groups":[{"name":"dc1","params":{"cpu":1,"memory":1}}]}}]}`)
	if _, err := nodeOnly.Build(reg, provider.ServeLB, nil); err == nil || !strings.Contains(err.Error(), "nothing to serve") {
		t.Fatalf("serve lb without classes: %v", err)
	}
	if _, err := provider.ParseServe("both"); err == nil {
		t.Fatal("an unknown -serve value is accepted")
	}
	if s, err := provider.ParseServe(""); err != nil || s != provider.ServeAll {
		t.Fatalf("default serve: %q %v", s, err)
	}
}

func TestBuild_CapabilityMismatch(t *testing.T) {
	reg := testRegistry(t)
	cases := map[string]struct{ config, want string }{
		"no node provider": {`{"advertise_url":"http://x","providers":[{"name":"p","type":"lbonly","no_auth":true,
		  "node":{"groups":[{"name":"dc1"}]}}]}`, `provider "p": type lbonly has no node provider`},
		"no lb provider": {`{"advertise_url":"http://x","providers":[{"name":"p","type":"nodeonly","no_auth":true,
		  "loadbalancer":{"classes":[{"class":"c"}]}}]}`, `provider "p": type nodeonly has no load balancer provider`},
		"unknown type":       {`{"advertise_url":"http://x","providers":[{"name":"p","type":"nope","no_auth":true,"loadbalancer":{"classes":[{"class":"c"}]}}]}`, `unknown type "nope" (linked types: lbonly, mock, nodeonly)`},
		"missing param":      {`{"advertise_url":"http://x","providers":[{"name":"p","type":"mock","no_auth":true,"node":{"groups":[{"name":"dc1","params":{"cpu":1}}]}}]}`, `group "dc1": parameter "memory" is required`},
		"unknown param":      {`{"advertise_url":"http://x","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"c","params":{"colour":"red"}}]}}]}`, "unknown parameters: colour"},
		"shared param":       {`{"advertise_url":"http://x","providers":[{"name":"p","type":"mock","no_auth":true,"params":{"x":1},"loadbalancer":{"classes":[{"class":"c"}]}}]}`, "unknown parameters: x"},
		"bad value":          {`{"advertise_url":"http://x","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"c","params":{"ready":"perhaps"}}]}}]}`, "must be true or false"},
		"unset token":        {`{"advertise_url":"http://x","providers":[{"name":"p","type":"mock","token":"env:PROVIDER_TEST_UNSET","loadbalancer":{"classes":[{"class":"c"}]}}]}`, "PROVIDER_TEST_UNSET"},
		"missing token file": {`{"advertise_url":"http://x","providers":[{"name":"p","type":"mock","token_file":"/nonexistent/token","loadbalancer":{"classes":[{"class":"c"}]}}]}`, "reading the token"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, tc.config).Build(reg, provider.ServeAll, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}

	// The capabilities are checked also when -serve leaves the section out:
	// the configuration is wrong either way.
	_, err := parse(t, cases["no lb provider"].config).Build(reg, provider.ServeNode, nil)
	if err == nil || !strings.Contains(err.Error(), cases["no lb provider"].want) {
		t.Fatalf("a mismatch in a section left out by -serve: %v", err)
	}

	// A class asking for internal load balancers of a type that cannot make
	// them.
	reg.Register(provider.Type{ID: "public", LB: &provider.LBSpec{New: func(lb.Config) (lb.Provider, error) { return publicOnly{}, nil }}})
	_, err = parse(t, `{"advertise_url":"http://x","providers":[{"name":"p","type":"public","no_auth":true,
	  "loadbalancer":{"classes":[{"class":"c","internal":true}]}}]}`).Build(reg, provider.ServeAll, nil)
	if err == nil || !strings.Contains(err.Error(), "cannot create internal load balancers") {
		t.Fatalf("internal of a public type: %v", err)
	}
}

type publicOnly struct{}

func (publicOnly) Capabilities() lb.Capabilities                        { return lb.Capabilities{} }
func (publicOnly) Ensure(context.Context, lb.Request) (lb.State, error) { return lb.State{}, nil }
func (publicOnly) Delete(context.Context, lb.Ref) error                 { return nil }

func TestParse_Errors(t *testing.T) {
	cases := map[string]struct{ content, want string }{
		"unknown field": {`{"advertise_url":"https://x","bogus":1,"providers":[]}`, "bogus"},
		"no providers":  {`{"advertise_url":"https://x"}`, "at least one provider"},
		"no section":    {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true}]}`, "neither a node nor a loadbalancer section"},
		"no advertise":  {`{"providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a"}]}}]}`, "needs an endpoint, or advertise_url"},
		"no token":      {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","loadbalancer":{"classes":[{"class":"a"}]}}]}`, "token or token_file is required"},
		"two tokens":    {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","token":"a","token_file":"b","loadbalancer":{"classes":[{"class":"a"}]}}]}`, "exclude each other"},
		"no type":       {`{"advertise_url":"https://x","providers":[{"name":"p","no_auth":true,"loadbalancer":{"classes":[{"class":"a"}]}}]}`, "type is required"},
		"bad name":      {`{"advertise_url":"https://x","providers":[{"name":"a b","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a"}]}}]}`, "invalid name"},
		"twice": {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a"}]}},
		  {"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"b"}]}}]}`, `provider "p" is configured twice`},
		"class twice": {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a"}]}},
		  {"name":"q","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a"}]}}]}`, `class "a" is configured twice`},
		"bad class":    {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a b"}]}}]}`, "invalid class name"},
		"no classes":   {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[]}}]}`, "at least one class"},
		"no groups":    {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,"node":{"groups":[]}}]}`, "at least one group"},
		"group twice":  {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,"node":{"groups":[{"name":"a"},{"name":"a"}]}}]}`, `group "a" is configured twice`},
		"bad ports":    {`{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a","ports":"90-80"}]}}]}`, "ports"},
		"bad interval": {`{"advertise_url":"https://x","heartbeat_interval":"soon","providers":[{"name":"p","type":"mock","no_auth":true,"loadbalancer":{"classes":[{"class":"a"}]}}]}`, "heartbeat_interval"},
		"unset env":    {`{"advertise_url":"https://${PROVIDER_TEST_UNSET}","providers":[]}`, "PROVIDER_TEST_UNSET"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := provider.Parse([]byte(tc.content), os.LookupEnv)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

func TestExpandEnv(t *testing.T) {
	t.Setenv("NORCA_PORT_api", "23456")
	t.Setenv("NORCA_IP_api", "10.77.1.12")
	got, err := provider.ExpandEnv([]byte(`{"listen":":${NORCA_PORT_api}","list":["a","${NORCA_IP_api}"],"n":5,
	  "nested":{"url":"http://${NORCA_IP_api}:${NORCA_PORT_api}","def":"${LB_UNSET_X:-fallback}","bare":"$NORCA_PORT_api"}}`), os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	s := string(got)
	for _, want := range []string{`":23456"`, `"10.77.1.12"`, `"http://10.77.1.12:23456"`, `"fallback"`, `"$NORCA_PORT_api"`, `"n":5`} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %s in %s", want, s)
		}
	}
	if _, err := provider.ExpandEnv([]byte(`{"listen":":${LB_UNSET_Y}"}`), os.LookupEnv); err == nil ||
		!strings.Contains(err.Error(), "LB_UNSET_Y") {
		t.Fatalf("an unset variable: %v", err)
	}
}

func TestLoad_TokenFile(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "providers.json")
	content := `{"advertise_url":"http://x","providers":[{"name":"p","type":"mock","token_file":"` + filepath.ToSlash(token) + `",
	  "loadbalancer":{"classes":[{"class":"a"}]}}]}`
	if err := os.WriteFile(cfg, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := provider.Load(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := f.Build(provider.Default, provider.ServeAll, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if rt.Classes[0].Token != "from-file" {
		t.Fatalf("token %q", rt.Classes[0].Token)
	}
}

func TestRegister_Panics(t *testing.T) {
	newLB := func(lb.Config) (lb.Provider, error) { return publicOnly{}, nil }
	lbSpec := &provider.LBSpec{New: newLB}
	cases := map[string]provider.Type{
		"bad id":              {ID: "Bad_ID", LB: lbSpec},
		"nothing":             {ID: "x"},
		"no constructor":      {ID: "x", LB: &provider.LBSpec{}},
		"no node constructor": {ID: "x", Node: &provider.NodeSpec{}},
		"dynamic shared":      {ID: "x", LB: lbSpec, Params: []params.Param{{Name: "n", Type: params.ParamString, Dynamic: true}}},
		"dynamic group": {ID: "x", Node: &provider.NodeSpec{New: func(provider.NodeConfig) (node.Provider, error) { return nil, nil },
			GroupParams: []params.Param{{Name: "n", Type: params.ParamString, Dynamic: true}}}},
		"shared twice": {ID: "x", Params: []params.Param{{Name: "n", Type: params.ParamString}},
			LB: &provider.LBSpec{New: newLB, Params: []params.Param{{Name: "n", Type: params.ParamString}}}},
		"bad param": {ID: "x", LB: lbSpec, Params: []params.Param{{Name: "n", Type: params.ParamInt, Default: "many"}}},
	}
	for name, typ := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected a panic")
				}
			}()
			provider.NewRegistry().Register(typ)
		})
	}
	r := provider.NewRegistry()
	r.Register(provider.Type{ID: "dup", LB: lbSpec})
	defer func() {
		if recover() == nil {
			t.Fatal("registering a type twice must panic")
		}
	}()
	r.Register(provider.Type{ID: "dup", LB: lbSpec})
}

// One process serves the node contract of a provider and its classes on
// one server, with the token of the provider for both.
func TestServer_BothContracts(t *testing.T) {
	t.Setenv("PROVIDER_TEST_TOKEN", "s3cret")
	reg := testRegistry(t)
	rt, err := parse(t, both).Build(reg, provider.ServeAll, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	srv := httptest.NewServer(provider.NewHandler(rt, reg.Types(), nil))
	defer srv.Close()
	ctx := context.Background()

	// The node contract, at /providers/lab.
	nc := &node.Client{Endpoint: srv.URL + "/providers/lab", Token: "s3cret"}
	g, err := nc.Group(ctx, "dc1", nil)
	if err != nil || g.Capacity == nil || g.Capacity.CPU != 2000 {
		t.Fatalf("group: %+v %v", g, err)
	}
	insts, err := nc.Create(ctx, "dc1", node.CreateRequest{RequestID: "r1", Instances: []node.InstanceRequest{{Name: "n1"}}})
	if err != nil || len(insts) != 1 || insts[0].State != node.StateRunning {
		t.Fatalf("create: %+v %v", insts, err)
	}
	if _, err := (&node.Client{Endpoint: nc.Endpoint, Token: "wrong"}).Group(ctx, "dc1", nil); node.ReasonOf(err) != node.ReasonInvalid {
		t.Fatalf("node call with a wrong token: %v", err)
	}

	// The load balancer contract, at /classes/<class>.
	lc := &lb.Client{Endpoint: srv.URL + "/classes/mock-public", Token: "s3cret"}
	st, err := lc.Ensure(ctx, lb.Ref{Namespace: "default", Name: "web"}, lb.EnsureRequest{
		Service: "web", Port: 443, Targets: []lb.WireTarget{{Address: "10.0.0.1", Port: 8080}}})
	if err != nil || !st.Ready || st.Addresses[0] != "203.0.113.7:443" {
		t.Fatalf("ensure: %+v %v", st, err)
	}
	// The ports of the class are checked by the server.
	if _, err := lc.Ensure(ctx, lb.Ref{Namespace: "default", Name: "web"}, lb.EnsureRequest{Port: 8080}); err == nil {
		t.Fatal("a port the class does not offer is accepted")
	}
	if _, err := (&lb.Client{Endpoint: lc.Endpoint, Token: "wrong"}).Ensure(ctx, lb.Ref{Namespace: "default", Name: "web"},
		lb.EnsureRequest{Port: 443}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("lb call with a wrong token: %v", err)
	}

	// Unknown providers and classes, the health check and the listings.
	for path, want := range map[string]int{
		"/providers/other/v1/groups/dc1":              http.StatusNotFound,
		"/classes/other/v1/loadbalancers/default/web": http.StatusNotFound,
		"/healthz":                 http.StatusOK,
		"/providers/lab/v1/health": http.StatusOK,
	} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode != want || (want == http.StatusNotFound && out["reason"] != "invalid") {
			t.Errorf("%s: %d %v", path, resp.StatusCode, out)
		}
	}

	var providers []struct {
		Name    string                     `json:"name"`
		Node    *struct{ Endpoint string } `json:"node"`
		Classes []struct{ Class string }   `json:"classes"`
	}
	getJSON(t, srv.URL+"/v1/providers", &providers)
	if len(providers) != 1 || providers[0].Name != "lab" || providers[0].Node == nil || len(providers[0].Classes) != 2 {
		t.Fatalf("providers: %+v", providers)
	}
	var types []struct {
		ID           string   `json:"id"`
		Capabilities []string `json:"capabilities"`
	}
	getJSON(t, srv.URL+"/v1/types", &types)
	if len(types) != 3 || types[0].ID != "lbonly" || strings.Join(types[1].Capabilities, ",") != "node,loadbalancer" {
		t.Fatalf("types: %+v", types)
	}
	var classes []map[string]any
	getJSON(t, srv.URL+"/v1/classes", &classes)
	if len(classes) != 2 || classes[0]["class"] != "mock-private" || classes[0]["tokenChecked"] != true {
		t.Fatalf("classes: %v", classes)
	}
	if _, leaked := classes[0]["token"]; leaked {
		t.Fatal("the token is listed")
	}
}

func getJSON(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("%s: %v: %s", url, err, raw)
	}
}
