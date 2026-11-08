// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package lb_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/lb/lbtest"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// memory is a provider that keeps its load balancers in a map.
type memory struct {
	mu       sync.Mutex
	internal bool
	lbs      map[lb.Ref]lb.Request
	fail     error
}

func newMemory() *memory { return &memory{internal: true, lbs: map[lb.Ref]lb.Request{}} }

func (m *memory) Capabilities() lb.Capabilities { return lb.Capabilities{Internal: m.internal} }

func (m *memory) Ensure(_ context.Context, req lb.Request) (lb.State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return lb.State{}, m.fail
	}
	m.lbs[req.Ref] = req
	st := lb.State{Addresses: []string{"192.0.2.1:" + strconv.Itoa(req.Port)}, Resource: "memory/" + req.Ref.String()}
	if req.Params.Bool("ready") && len(req.Targets) > 0 {
		st.Ready = true
	} else {
		st.Message = "not ready"
	}
	return st, nil
}

func (m *memory) Delete(_ context.Context, ref lb.Ref) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.lbs, ref)
	return nil
}

func (m *memory) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.lbs)
}

var classParams = []params.Param{
	{Name: "ready", Type: params.ParamBool, Default: true, Dynamic: true},
	{Name: "zone", Type: params.ParamString, Default: "a"},
}

func newClass(t *testing.T, m *memory) *lb.Class {
	t.Helper()
	values, err := params.Resolve(classParams, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &lb.Class{Name: "edge", Type: "memory", Provider: m, Params: classParams, Values: values, Caps: m.Capabilities()}
}

func TestConformance_Memory(t *testing.T) {
	values, _ := params.Resolve(classParams, nil)
	lbtest.Run(t, lbtest.Config{New: func(*testing.T) lb.Provider { return newMemory() }, Params: values})
}

const body = `{"name":"api","namespace":"default","service":"api","port":443,
 "targets":[{"address":"10.0.0.2","port":8001,"allocID":"b"},{"address":"10.0.0.1","port":8001,"allocID":"a"}]}`

func do(t *testing.T, srv *httptest.Server, method, path, token, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

const lbPath = "/v1/loadbalancers/default/api"

func TestHandler_PutDeleteAndAuth(t *testing.T) {
	m := newMemory()
	srv := httptest.NewServer(lb.NewHandler(newClass(t, m), "s3cret", nil))
	defer srv.Close()

	for name, token := range map[string]string{"none": "", "wrong": "nope"} {
		status, out := do(t, srv, http.MethodPut, lbPath, token, body)
		if status != http.StatusUnauthorized || out["reason"] != "invalid" || out["error"] == nil {
			t.Fatalf("%s token: %d %v", name, status, out)
		}
	}
	status, out := do(t, srv, http.MethodPut, lbPath, "s3cret", body)
	if status != http.StatusOK || out["ready"] != true || out["resource"] != "memory/default/api" {
		t.Fatalf("put: %d %v", status, out)
	}
	if status, _ = do(t, srv, http.MethodDelete, lbPath, "s3cret", ""); status != http.StatusNoContent || m.count() != 0 {
		t.Fatalf("delete: %d", status)
	}
}

func TestHandler_Validation(t *testing.T) {
	m := newMemory()
	c := newClass(t, m)
	c.Caps = lb.Capabilities{Internal: false, Ports: "80,443,8000-8100"}
	srv := httptest.NewServer(lb.NewHandler(c, "", nil))
	defer srv.Close()
	cases := map[string]struct {
		path, body string
		want       int
	}{
		"bad port":       {lbPath, `{"port":70000}`, http.StatusUnprocessableEntity},
		"port not given": {lbPath, `{"port":9000}`, http.StatusUnprocessableEntity},
		"bad target":     {lbPath, `{"port":80,"targets":[{"address":"","port":1}]}`, http.StatusUnprocessableEntity},
		"internal":       {lbPath, `{"port":80,"internal":true}`, http.StatusUnprocessableEntity},
		"bad json":       {lbPath, `{`, http.StatusBadRequest},
		"name mismatch":  {lbPath, `{"name":"other","port":80}`, http.StatusBadRequest},
		"path traversal": {"/v1/loadbalancers/default/..%2Fx", `{"port":80}`, http.StatusBadRequest},
		"bad dynamic":    {lbPath, `{"port":80,"parameters":{"ready":"perhaps"}}`, http.StatusUnprocessableEntity},
		"port in range":  {lbPath, `{"port":8050}`, http.StatusOK},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if status, out := do(t, srv, http.MethodPut, tc.path, "", tc.body); status != tc.want {
				t.Fatalf("status %d (%v), want %d", status, out, tc.want)
			}
		})
	}
}

// The parameters the operator sends back override the dynamic ones, not
// the fixed ones.
func TestHandler_DynamicParameters(t *testing.T) {
	m := newMemory()
	srv := httptest.NewServer(lb.NewHandler(newClass(t, m), "", nil))
	defer srv.Close()
	_, out := do(t, srv, http.MethodPut, lbPath, "", `{"port":443,"targets":[{"address":"10.0.0.1","port":1}],"parameters":{"ready":false,"zone":"b"}}`)
	if out["ready"] != false {
		t.Fatalf("the dynamic parameter was not applied: %v", out)
	}
	m.mu.Lock()
	zone := m.lbs[lb.Ref{Namespace: "default", Name: "api"}].Params.String("zone")
	m.mu.Unlock()
	if zone != "a" {
		t.Fatalf("a fixed parameter was overridden: %q", zone)
	}
}

func TestClient_Errors(t *testing.T) {
	m := newMemory()
	m.fail = lb.Invalid("no such network")
	srv := httptest.NewServer(lb.NewHandler(newClass(t, m), "", nil))
	defer srv.Close()
	c := &lb.Client{Endpoint: srv.URL + "/"}
	ref := lb.Ref{Namespace: "default", Name: "api"}

	_, err := c.Ensure(context.Background(), ref, lb.EnsureRequest{Port: 80})
	var re *lb.ResponseError
	if !errors.As(err, &re) || !re.Invalid() || re.Status != http.StatusUnprocessableEntity || !strings.Contains(re.Message, "no such network") {
		t.Fatalf("invalid: %#v", err)
	}
	m.fail = errors.New("the cloud is down")
	_, err = c.Ensure(context.Background(), ref, lb.EnsureRequest{Port: 80})
	if !errors.As(err, &re) || re.Invalid() || re.Status != http.StatusBadGateway || err.Error() != "answered 502: the cloud is down" {
		t.Fatalf("failure: %v", err)
	}

	// A provider that does not know the load balancer (404) has nothing to
	// delete.
	gone := httptest.NewServer(http.NotFoundHandler())
	defer gone.Close()
	if err := (&lb.Client{Endpoint: gone.URL}).Delete(context.Background(), ref); err != nil {
		t.Fatalf("delete on 404: %v", err)
	}
}

func TestParsePorts(t *testing.T) {
	r, err := lb.ParsePorts("80, 443,8000-8100")
	if err != nil || !lb.PortAllowed(r, 443) || !lb.PortAllowed(r, 8050) || lb.PortAllowed(r, 81) {
		t.Fatalf("ranges %v %v", r, err)
	}
	if r, err := lb.ParsePorts(""); err != nil || !lb.PortAllowed(r, 1) {
		t.Fatal("empty is any port")
	}
	for _, bad := range []string{"0", "70000", "90-80", "x"} {
		if _, err := lb.ParsePorts(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
