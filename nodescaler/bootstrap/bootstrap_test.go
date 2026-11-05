// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package bootstrap

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
)

// files reads the write_files of a rendered document.
func files(t *testing.T, doc string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	var path string
	for line := range strings.SplitSeq(doc, "\n") {
		line = strings.TrimSpace(line)
		if p, ok := strings.CutPrefix(line, "- path: "); ok {
			path = p
		}
		if c, ok := strings.CutPrefix(line, "content: "); ok {
			data, err := base64.StdEncoding.DecodeString(c)
			if err != nil {
				t.Fatal(err)
			}
			out[path] = data
		}
	}
	return out
}

func TestRender(t *testing.T) {
	items, root := bootstraptest.Items(t)
	tmpl, err := Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	r, err := tmpl.Render(Node{Name: "workers-dc1-3-1", Datacenter: "dc1", Pool: "workers", Class: "gpu",
		Meta: map[string]string{"norca.instance": "workers-dc1-3-1", "norca.node_group": "workers-dc1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(r.UserData, "#cloud-config\n") || !strings.Contains(r.UserData, "hostname: workers-dc1-3-1\n") ||
		!strings.Contains(r.UserData, "[systemctl, enable, --now, norca]") {
		t.Fatalf("document:\n%s", r.UserData)
	}
	f := files(t, r.UserData)

	// The chain the node sends verifies against the cluster CA, for a client.
	pair, err := tls.X509KeyPair(f["/etc/norca/tls/client.pem"], f["/etc/norca/tls/client-key.pem"])
	if err != nil {
		t.Fatalf("the certificate and the key do not match: %v", err)
	}
	if len(pair.Certificate) != 2 {
		t.Fatalf("the certificate file has %d certificates, want the leaf and the node CA", len(pair.Certificate))
	}
	leaf, _ := x509.ParseCertificate(pair.Certificate[0])
	inter, _ := x509.ParseCertificate(pair.Certificate[1])
	roots := x509.NewCertPool()
	roots.AddCert(root.Certificate)
	ints := x509.NewCertPool()
	ints.AddCert(inter)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: ints,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Fatalf("the node certificate does not verify: %v", err)
	}
	if leaf.Subject.CommonName != "client.eu.norca" || len(leaf.URIs) != 1 ||
		leaf.URIs[0].String() != "spiffe://lab.norca/region/eu/node/workers-dc1-3-1" {
		t.Fatalf("names: %s %v", leaf.Subject.CommonName, leaf.URIs)
	}
	if left := time.Until(leaf.NotAfter); left < 59*24*time.Hour || left > 60*24*time.Hour {
		t.Fatalf("validity: %s", left)
	}
	if r.Serial != leaf.SerialNumber.Text(16) || !r.NotAfter.Equal(leaf.NotAfter) {
		t.Fatalf("serial %s / %s", r.Serial, leaf.SerialNumber.Text(16))
	}
	if block, _ := pem.Decode(f["/etc/norca/tls/ca.pem"]); block == nil {
		t.Fatal("no CA file")
	}

	// The agent configuration has the node's own parts.
	cfg := string(f["/etc/norca/norca.hcl"])
	for _, want := range []string{`name       = "workers-dc1-3-1"`, `region     = "eu"`, `datacenter = "dc1"`,
		`node_pool  = "workers"`, `node_class = "gpu"`, `"norca.instance" = "workers-dc1-3-1"`,
		`cert_file = "/etc/norca/tls/client.pem"`} {
		if !strings.Contains(cfg, want) {
			t.Errorf("the configuration has no %s:\n%s", want, cfg)
		}
	}

	// Each node gets a key of its own.
	r2, err := tmpl.Render(Node{Name: "workers-dc1-3-2", Datacenter: "dc1", Pool: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	if string(files(t, r2.UserData)["/etc/norca/tls/client-key.pem"]) == string(f["/etc/norca/tls/client-key.pem"]) {
		t.Fatal("two nodes have the same key")
	}
}

func TestParse(t *testing.T) {
	items, _ := bootstraptest.Items(t)
	for name, mutate := range map[string]func(map[string]string){
		"no CA":              func(m map[string]string) { delete(m, ItemCA) },
		"no node CA key":     func(m map[string]string) { delete(m, ItemNodeCAKey) },
		"a bad template":     func(m map[string]string) { m[ItemAgentConfig] = "{{.Name" },
		"a bad trust domain": func(m map[string]string) { m[ItemTrustDomain] = "Bad Domain" },
		"a bad validity":     func(m map[string]string) { m[ItemValidity] = "soon" },
		"a bad service":      func(m map[string]string) { m[ItemService] = "norca; reboot" },
		"a CA that is not":   func(m map[string]string) { m[ItemCA] = "hello" },
		"a mismatched key":   func(m map[string]string) { other, _ := bootstraptest.Items(t); m[ItemNodeCAKey] = other[ItemNodeCAKey] },
	} {
		m := maps.Clone(items)
		mutate(m)
		if _, err := Parse("eu", m); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	items[ItemValidity] = "30d"
	items[ItemTLSDir] = "/opt/norca/tls"
	tmpl, err := Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.Validity != 30*24*time.Hour || tmpl.TLSDir != "/opt/norca/tls" {
		t.Fatalf("template: %+v", tmpl)
	}
	// A field the template does not know is an error of the render.
	items[ItemAgentConfig] = "{{.Nope}}"
	tmpl, err = Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmpl.Render(Node{Name: "n"}); err == nil {
		t.Fatal("an unknown field is rendered")
	}
}

func TestDecodeReadsRender(t *testing.T) {
	items, _ := bootstraptest.Items(t)
	tmpl, err := Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	r, err := tmpl.Render(Node{Name: "workers-dc1-1-1", Datacenter: "dc1", Pool: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	d, err := Decode(r.UserData)
	if err != nil {
		t.Fatal(err)
	}
	if d.Hostname != "workers-dc1-1-1" || len(d.Files) != 4 || len(d.RunCmd) != 1 {
		t.Fatalf("document: %+v", d)
	}
	want := files(t, r.UserData)
	for _, f := range d.Files {
		if string(f.Content) != string(want[f.Path]) {
			t.Errorf("%s: content differs", f.Path)
		}
		if f.Path == "/etc/norca/tls/client-key.pem" && f.Mode != 0o600 {
			t.Errorf("the key has mode %o", f.Mode)
		}
	}
	if cfg, err := d.ConfigFile(); err != nil || cfg != "/etc/norca/norca.hcl" {
		t.Fatalf("config file %q %v", cfg, err)
	}

	for name, doc := range map[string]string{
		"not cloud-config": "hostname: x\n",
		"unknown key":      "#cloud-config\nusers:\n  - a\n",
		"other encoding":   "#cloud-config\nwrite_files:\n  - path: /a\n    encoding: gzip\n",
		"relative path":    "#cloud-config\nwrite_files:\n  - path: a\n",
		"bad base64":       "#cloud-config\nwrite_files:\n  - path: /a\n    content: !!!\n",
	} {
		if _, err := Decode(doc); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
