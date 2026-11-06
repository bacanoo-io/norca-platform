// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
)

// fileOf decodes a file of a cloud-init document.
func fileOf(t *testing.T, doc, path string) (string, bool) {
	t.Helper()
	lines := strings.Split(doc, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) == "- path: "+path {
			for _, l := range lines[i:] {
				if c, ok := strings.CutPrefix(strings.TrimSpace(l), "content: "); ok {
					data, err := base64.StdEncoding.DecodeString(c)
					if err != nil {
						t.Fatal(err)
					}
					return string(data), true
				}
			}
		}
	}
	return "", false
}

func (e *env) withJoin(urls ...string) *join.Store {
	store := &join.Store{Vars: e.backend, Namespace: "default", Region: "eu", Now: func() time.Time { return e.now }}
	e.r.cfg.Join = &StoreJoin{Store: store, URLs: func(context.Context) ([]string, error) { return urls, nil }}
	return store
}

// With the join, an instance gets a one-time token and the endpoints, no
// key and no certificate; the token is recorded for its group, and the
// record of an instance that is gone is dropped.
func TestJoin_InstancesGetATokenNotAKey(t *testing.T) {
	e := newEnv(t, "")
	items, _ := bootstraptest.Items(t)
	e.withBootstrap(items)
	store := e.withJoin("https://10.0.0.7:8490")
	e.putGroup(1, nil)
	e.reconcile()

	insts := e.instances()
	if len(insts) != 1 {
		t.Fatalf("instances: %+v", insts)
	}
	doc := e.mock.UserData(insts[0].ID)
	if _, ok := fileOf(t, doc, "/etc/norca/tls/client-key.pem"); ok {
		t.Fatal("the bootstrap carries a key")
	}
	if _, ok := fileOf(t, doc, "/etc/norca/tls/client.pem"); ok {
		t.Fatal("the bootstrap carries a certificate")
	}
	raw, ok := fileOf(t, doc, "/etc/norca/norca-join.json")
	if !ok {
		t.Fatalf("no join configuration:\n%s", doc)
	}
	var conf join.Config
	if err := json.Unmarshal([]byte(raw), &conf); err != nil {
		t.Fatal(err)
	}
	if conf.Instance != "workers-dc1-1-1" || conf.Token == "" || len(conf.URLs) != 1 || conf.URLs[0] != "https://10.0.0.7:8490" {
		t.Fatalf("join configuration: %+v", conf)
	}
	if cfg := agentConfig(t, doc); !strings.Contains(cfg, `"norca.identity" = "join"`) || strings.Contains(cfg, "norca.cert_expires") {
		t.Fatalf("agent configuration:\n%s", cfg)
	}
	// The certificate is issued at the join, by the endpoint.
	if n := e.eventCount("CertificateIssued"); n != 0 {
		t.Fatalf("CertificateIssued events before a join: %d", n)
	}
	rec := e.backend.GetVariable("default", "nodescaler/join/eu/workers-dc1-1-1")
	if rec == nil || rec.Items["group"] != "workers-dc1" {
		t.Fatalf("join record: %+v", rec)
	}
	// The token of the bootstrap is the recorded one.
	if _, group, err := store.Redeem(context.Background(), "workers-dc1-1-1", conf.Token); err != nil || group != "workers-dc1" {
		t.Fatalf("the token does not redeem: %q %v", group, err)
	}

	// The instance joined and is gone (by other hands): its record goes.
	v, _, _ := store.Redeem(context.Background(), "workers-dc1-1-1", conf.Token)
	if err := store.Joined(context.Background(), v, "abc"); err != nil {
		t.Fatal(err)
	}
	if err := e.mock.Delete(context.Background(), "dc1", insts[0].ID); err != nil {
		t.Fatal(err)
	}
	e.putGroup(0, nil)
	e.reconcile()
	if rec := e.backend.GetVariable("default", "nodescaler/join/eu/workers-dc1-1-1"); rec != nil {
		t.Fatalf("the record of a gone instance stays: %+v", rec.Items)
	}
}

// Without a join endpoint no instance is asked for: its node could not get
// a certificate.
func TestJoin_NoEndpointAsksForNothing(t *testing.T) {
	e := newEnv(t, "")
	items, _ := bootstraptest.Items(t)
	e.withBootstrap(items)
	e.withJoin()
	e.putGroup(1, nil)
	e.reconcile()
	if n := len(e.instances()); n != 0 {
		t.Fatalf("instances without a join endpoint: %d", n)
	}
	c := conditionOf(e.status(), CondProvisioned)
	if c["status"] != "False" || !strings.Contains(c["message"].(string), "no join endpoint") {
		t.Fatalf("Provisioned: %+v", c)
	}
}

func TestInstancePrefix(t *testing.T) {
	for _, group := range []string{"workers-dc1", "Edge_Pool", strings.Repeat("a", 80)} {
		p := instancePrefix(group)
		for _, seq := range []int64{1, 12345, 999999999999} {
			if name := instanceName(group, seq, 3); !strings.HasPrefix(name, p) {
				t.Errorf("%s: %q does not start with %q", group, name, p)
			}
		}
	}
}
