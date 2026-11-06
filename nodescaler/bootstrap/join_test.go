// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package bootstrap

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
)

// The bootstrap of a node that joins with a key of its own carries no key
// and no certificate: the cluster CA, the agent configuration with the meta
// norca.identity = "join" (and no expiry of a certificate), the
// configuration of norca-join with the token, and its unit.
func TestRenderJoin(t *testing.T) {
	items, _ := bootstraptest.Items(t)
	items[ItemJoinBinary] = "/opt/norca/bin/norca-join"
	tmpl, err := Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	r, err := tmpl.RenderJoin(Node{Name: "workers-dc1-3-1", Datacenter: "dc1", Pool: "workers",
		Meta: map[string]string{"norca.instance": "workers-dc1-3-1"}},
		JoinTicket{Token: "secret-token", URLs: []string{"https://10.0.0.7:8490"}})
	if err != nil {
		t.Fatal(err)
	}
	f := files(t, r.UserData)
	if _, ok := f["/etc/norca/tls/client-key.pem"]; ok || strings.Contains(r.UserData, "PRIVATE KEY") {
		t.Fatal("the bootstrap of a join carries a key")
	}
	if _, ok := f["/etc/norca/tls/client.pem"]; ok {
		t.Fatal("the bootstrap of a join carries a certificate")
	}
	if string(f["/etc/norca/tls/ca.pem"]) != items[ItemCA] {
		t.Fatal("no cluster CA")
	}
	agent := string(f["/etc/norca/norca.hcl"])
	if !strings.Contains(agent, `"norca.identity" = "join"`) || strings.Contains(agent, MetaCertExpires) ||
		!strings.Contains(agent, `cert_file = "/etc/norca/tls/client.pem"`) {
		t.Fatalf("agent configuration:\n%s", agent)
	}

	var conf join.Config
	if err := json.Unmarshal(f["/etc/norca/norca-join.json"], &conf); err != nil {
		t.Fatal(err)
	}
	if err := conf.Check(); err != nil {
		t.Fatal(err)
	}
	if conf.Instance != "workers-dc1-3-1" || conf.Token != "secret-token" || conf.Service != "norca" ||
		conf.Endpoint != "spiffe://lab.norca/region/eu/nodescaler/join" || conf.KeyFile != "/etc/norca/tls/client-key.pem" {
		t.Fatalf("join configuration: %+v", conf)
	}
	unit := string(f["/etc/systemd/system/norca-join.service"])
	if !strings.Contains(unit, "ExecStart=/opt/norca/bin/norca-join run -config /etc/norca/norca-join.json") {
		t.Fatalf("unit:\n%s", unit)
	}
	if !strings.Contains(r.UserData, "[systemctl, enable, --now, norca-join]") ||
		strings.Contains(r.UserData, "[systemctl, enable, --now, norca]") {
		t.Fatalf("runcmd: norca-join starts the agent, not the bootstrap:\n%s", r.UserData)
	}

	// A provider that runs the node itself reads the document back.
	doc, err := Decode(r.UserData)
	if err != nil {
		t.Fatal(err)
	}
	if doc.JoinConfig() != "/etc/norca/norca-join.json" {
		t.Fatalf("the join configuration of the document: %q", doc.JoinConfig())
	}
	if cfg, err := doc.ConfigFile(); err != nil || cfg != "/etc/norca/norca.hcl" {
		t.Fatalf("the agent configuration of the document: %q %v", cfg, err)
	}

	// Without a token, an endpoint, or with a bad binary path, no bootstrap.
	if _, err := tmpl.RenderJoin(Node{Name: "a-1-1"}, JoinTicket{URLs: []string{"https://x:1"}}); err == nil {
		t.Fatal("a join without a token")
	}
	if _, err := tmpl.RenderJoin(Node{Name: "a-1-1"}, JoinTicket{Token: "t"}); err == nil {
		t.Fatal("a join without an endpoint")
	}
	if _, err := tmpl.RenderJoin(Node{Name: "a-1-1"}, JoinTicket{Token: "t", URLs: []string{"http://x:1"}}); err == nil {
		t.Fatal("a join with an http endpoint")
	}
	tmpl.JoinBinary = "norca-join"
	if _, err := tmpl.RenderJoin(Node{Name: "a-1-1"}, JoinTicket{Token: "t", URLs: []string{"https://x:1"}}); err == nil {
		t.Fatal("a relative join binary")
	}
}
