// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
	"github.com/bacanoo-io/norca/norca/api"
)

// withBootstrap makes the reconciler read the bootstrap Variable of the
// region; items nil leaves the Variable out.
func (e *env) withBootstrap(items map[string]string) {
	e.r.cfg.Bootstrap = &VariableBootstrap{Vars: e.backend, Namespace: "default", Path: "nodescaler/bootstrap/eu", Region: "eu"}
	if items != nil {
		e.backend.PutVariable(&api.Variable{Namespace: "default", Path: "nodescaler/bootstrap/eu", Items: items})
	}
}

// agentConfig decodes the agent configuration of a cloud-init document.
func agentConfig(t *testing.T, doc string) string {
	t.Helper()
	lines := strings.Split(doc, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) == "- path: /etc/norca/norca.hcl" {
			for _, l := range lines[i:] {
				if c, ok := strings.CutPrefix(strings.TrimSpace(l), "content: "); ok {
					data, err := base64.StdEncoding.DecodeString(c)
					if err != nil {
						t.Fatal(err)
					}
					return string(data)
				}
			}
		}
	}
	t.Fatalf("no agent configuration in:\n%s", doc)
	return ""
}

func TestBootstrap_EachInstanceGetsItsOwn(t *testing.T) {
	e := newEnv(t, "")
	items, _ := bootstraptest.Items(t)
	e.withBootstrap(items)
	e.putGroup(2, map[string]any{"node": map[string]any{"meta": map[string]any{"tier": "batch"}}})
	e.reconcile()

	insts := e.instances()
	if len(insts) != 2 || insts[0].Name != "eu-workers-dc1-1-1" || insts[1].Name != "eu-workers-dc1-1-2" {
		t.Fatalf("instances: %+v", insts)
	}
	for _, inst := range insts {
		doc := e.mock.UserData(inst.ID)
		if !strings.HasPrefix(doc, "#cloud-config\n") || !strings.Contains(doc, "hostname: "+inst.Name+"\n") {
			t.Fatalf("user data of %s:\n%s", inst.Name, doc)
		}
		cfg := agentConfig(t, doc)
		for _, want := range []string{
			`name       = "` + inst.Name + `"`, `datacenter = "dc1"`, `node_pool  = "workers"`,
			`"norca.instance" = "` + inst.Name + `"`, `"norca.created_by" = "nodescaler"`,
			`"norca.node_group" = "workers-dc1"`, `"norca.request" = "eu/workers-dc1/1"`, `"tier" = "batch"`,
		} {
			if !strings.Contains(cfg, want) {
				t.Errorf("the configuration of %s has no %s:\n%s", inst.Name, want, cfg)
			}
		}
	}
	if e.mock.UserData(insts[0].ID) == e.mock.UserData(insts[1].ID) {
		t.Fatal("two instances got the same bootstrap")
	}
	if n := e.eventCount("CertificateIssued"); n != 2 {
		t.Fatalf("CertificateIssued events: %d", n)
	}

	// The node registers with the name of its instance: it is matched.
	e.nodes.set(Node{ID: "n1", Instance: insts[0].Name, Ready: true})
	e.reconcile()
	if st := e.status(); st["replicas"] != 1.0 || st["joining"] != 1.0 {
		t.Fatalf("status: %+v", st)
	}
}

func TestBootstrap_MissingAsksForNothing(t *testing.T) {
	e := newEnv(t, "")
	e.withBootstrap(nil)
	e.putGroup(2, nil)
	if d := e.reconcile(); d != e.r.cfg.RetryAfter {
		t.Fatalf("requeue %v", d)
	}
	if n := len(e.instances()); n != 0 {
		t.Fatalf("instances without a bootstrap: %d", n)
	}
	c := conditionOf(e.status(), CondProvisioned)
	if c["status"] != "False" || c["reason"] != "NoBootstrap" || !strings.Contains(c["message"].(string), "nodescaler/bootstrap/eu") {
		t.Fatalf("Provisioned: %+v", c)
	}
	if e.eventCount("BootstrapFailed") != 1 {
		t.Fatal("no BootstrapFailed event")
	}

	// A Variable that is broken is reported the same way.
	items, _ := bootstraptest.Items(t)
	delete(items, "node_ca_key")
	e.withBootstrap(items)
	e.reconcile()
	if c := conditionOf(e.status(), CondProvisioned); c["reason"] != "NoBootstrap" || !strings.Contains(c["message"].(string), "node_ca_key") {
		t.Fatalf("Provisioned: %+v", c)
	}
}

// hashOf is the mark of a group of a region in the names of its instances.
func hashOf(region, group string) string {
	sum := sha256.Sum256([]byte(region + "\x00" + group))
	return hex.EncodeToString(sum[:])[:8]
}

func TestInstanceName(t *testing.T) {
	long := strings.Repeat("a", 70)
	for _, tc := range []struct {
		region, group string
		seq           int64
		i             int
		want          string
	}{
		// A region without a dash and a group that is a host name are in
		// the names as they are.
		{"eu", "workers-dc1", 3, 1, "eu-workers-dc1-3-1"},
		{"global", "workers", 1, 2, "global-workers-1-2"},
		{"eu", strings.Repeat("a", 36), 1, 1, "eu-" + strings.Repeat("a", 36) + "-1-1"},
		// A group name that had to be changed carries the mark of the pair.
		{"eu", "Edge_Pool", 1, 1, "eu-edge-pool-c26f0640-1-1"},
		{"eu", "Workers_DC.1", 12, 2, "eu-workers-dc-1-" + hashOf("eu", "Workers_DC.1") + "-12-2"},
		{"eu", "---", 1, 1, "eu-" + hashOf("eu", "---") + "-1-1"},
		{"eu", long, 1, 1, "eu-" + strings.Repeat("a", 27) + "-" + hashOf("eu", long) + "-1-1"},
		{"eu", strings.Repeat("a", 37), 1, 1, "eu-" + strings.Repeat("a", 27) + "-" + hashOf("eu", strings.Repeat("a", 37)) + "-1-1"},
		// The cut does not depend on the number of the request.
		{"eu", long, 999999999999, 3, "eu-" + strings.Repeat("a", 27) + "-" + hashOf("eu", long) + "-999999999999-3"},
		// A base that ends like a mark gets its own, so it is not the base
		// of a pair with that mark.
		{"eu", "edge-pool-0a1b2c3d", 1, 1, "eu-edge-pool-0a1b2c3d-" + hashOf("eu", "edge-pool-0a1b2c3d") + "-1-1"},
		{"eu", "deadbeef", 1, 1, "eu-deadbeef-" + hashOf("eu", "deadbeef") + "-1-1"},
		// A region with a dash: where it ends is not in the plain base, so
		// the mark says it.
		{"eu-west", "workers", 1, 1, "eu-west-workers-55cfcdb7-1-1"},
		{"eu", "west-workers", 1, 1, "eu-west-workers-1-1"},
		// A region that is not a host name, or is none.
		{"EU", "workers", 1, 1, "eu-workers-" + hashOf("EU", "workers") + "-1-1"},
		{"", "workers", 1, 1, "workers-" + hashOf("", "workers") + "-1-1"},
		{"", "---", 1, 1, "node-" + hashOf("", "---") + "-1-1"},
		{strings.Repeat("r", 40), "workers", 1, 1, strings.Repeat("r", 30) + "-" + hashOf(strings.Repeat("r", 40), "workers") + "-1-1"},
	} {
		got := instanceName(tc.region, tc.group, tc.seq, tc.i)
		if got != tc.want || !join.ValidInstance(got) {
			t.Errorf("%q %q %d %d: got %q, want %q", tc.region, tc.group, tc.seq, tc.i, got, tc.want)
		}
	}
}

// Two groups never give an instance the same name (review finding CON-6l:
// Edge_Pool and edge-pool both made edge-pool-1-1, one machine name and one
// join record for two machines), and neither do two regions, nor a region
// and a group that read the same as another pair once joined (CON-6n).
func TestInstanceName_UniquePerRegionAndGroup(t *testing.T) {
	long := strings.Repeat("a", 70)
	type pair struct{ region, group string }
	for _, tc := range [][2]pair{
		{{"eu", "Edge_Pool"}, {"eu", "edge-pool"}},
		{{"eu", "Edge_Pool"}, {"eu", "edge.pool"}},
		{{"eu", "edge-pool"}, {"eu", "-edge-pool-"}},
		{{"eu", long + "1"}, {"eu", long + "2"}},
		{{"eu", "a"}, {"eu", "A"}},
		// The base one pair gets from its mark, as the plain base of another.
		{{"eu", "Edge_Pool"}, {"eu", "edge-pool-" + hashOf("eu", "Edge_Pool")}},
		// Two regions.
		{{"eu", "workers"}, {"us", "workers"}},
		{{"eu", "Edge_Pool"}, {"us", "Edge_Pool"}},
		{{"eu", "workers"}, {"EU", "workers"}},
		// The dash between the region and the group.
		{{"eu-west", "workers"}, {"eu", "west-workers"}},
		{{"eu-west-1", "workers"}, {"eu-west", "1-workers"}},
		{{"eu-west", "Edge_Pool"}, {"eu", "west-Edge_Pool"}},
		{{"eu", "west-workers"}, {"", "eu-west-workers"}},
	} {
		for _, seq := range []int64{1, 12, 999999999999} {
			a, b := instanceName(tc[0].region, tc[0].group, seq, 1), instanceName(tc[1].region, tc[1].group, seq, 1)
			if a == b {
				t.Errorf("%+v and %+v: both name an instance %q", tc[0], tc[1], a)
			}
			for _, name := range []string{a, b} {
				if !join.ValidInstance(name) {
					t.Errorf("%+v and %+v: %q is not a host name", tc[0], tc[1], name)
				}
			}
		}
	}
}

// Two regions whose groups have the same name and use the same group of a
// provider do not give their instances the same names (review finding
// CON-6n: both named a machine eu-workers-dc1-1-1).
func TestInstanceName_RegionsOnOneProviderGroup(t *testing.T) {
	eu := newEnv(t, "")
	eu.putGroup(1, nil)
	eu.reconcile()

	// The operator of another region, with its own servers, on the same
	// platform.
	us := newEnv(t, "")
	us.lag.Provider = eu.mock
	us.r = NewReconciler(us.backend, us.nodes, Config{Region: "us", AllowHTTP: true, Now: func() time.Time { return us.now }})
	us.r.providers.cacheFor = 0
	us.putGroup(1, nil)
	us.reconcile()

	insts := eu.instances()
	if len(insts) != 2 {
		t.Fatalf("instances of the two regions: %+v", insts)
	}
	if insts[0].Name == insts[1].Name {
		t.Fatalf("the instances of regions %s and %s are both named %s",
			insts[0].Labels[LabelRegion], insts[1].Labels[LabelRegion], insts[0].Name)
	}
}
