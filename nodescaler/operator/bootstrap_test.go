// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
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
	if len(insts) != 2 || insts[0].Name != "workers-dc1-1-1" || insts[1].Name != "workers-dc1-1-2" {
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

func TestInstanceName(t *testing.T) {
	for _, tc := range []struct {
		group string
		seq   int64
		i     int
		want  string
	}{
		{"workers-dc1", 3, 1, "workers-dc1-3-1"},
		{"Workers_DC.1", 12, 2, "workers-dc-1-12-2"},
		{"---", 1, 1, "node-1-1"},
		{strings.Repeat("a", 70), 1, 1, strings.Repeat("a", 59) + "-1-1"},
	} {
		got := instanceName(tc.group, tc.seq, tc.i)
		if got != tc.want || len(got) > 63 {
			t.Errorf("%q %d %d: got %q, want %q", tc.group, tc.seq, tc.i, got, tc.want)
		}
	}
}
