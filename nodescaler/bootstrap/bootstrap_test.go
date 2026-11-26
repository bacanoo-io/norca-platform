// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package bootstrap

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
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
	r, err := tmpl.Render(Node{Name: "eu-workers-dc1-3-1", Datacenter: "dc1", Pool: "workers", Class: "gpu",
		Meta: map[string]string{"norca.instance": "eu-workers-dc1-3-1", "norca.node_group": "workers-dc1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(r.UserData, "#cloud-config\n") || !strings.Contains(r.UserData, "hostname: eu-workers-dc1-3-1\n") ||
		!strings.HasSuffix(r.UserData, "runcmd:\n  - '"+enableLine("norca", "")+"'\n") {
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
		leaf.URIs[0].String() != "spiffe://lab.norca/region/eu/node/eu-workers-dc1-3-1" {
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
	for _, want := range []string{`name       = "eu-workers-dc1-3-1"`, `region     = "eu"`, `datacenter = "dc1"`,
		`node_pool  = "workers"`, `node_class = "gpu"`, `"norca.instance" = "eu-workers-dc1-3-1"`,
		`"norca.cert_expires" = "` + leaf.NotAfter.UTC().Format(time.RFC3339) + `"`,
		`cert_file = "/etc/norca/tls/client.pem"`} {
		if !strings.Contains(cfg, want) {
			t.Errorf("the configuration has no %s:\n%s", want, cfg)
		}
	}

	// Each node gets a key of its own.
	r2, err := tmpl.Render(Node{Name: "eu-workers-dc1-3-2", Datacenter: "dc1", Pool: "workers"})
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
		"a quoted service":   func(m map[string]string) { m[ItemService] = "norca'" },
		"an option":          func(m map[string]string) { m[ItemService] = "-norca" },
		"a unit instance":    func(m map[string]string) { m[ItemService] = "norca@eu" },
		"a unit file name":   func(m map[string]string) { m[ItemService] = "norca.service" },
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
	// Names of services of systemd and OpenRC alike.
	for _, service := range []string{"norca", "norca-agent", "norca_2", "norca.eu", "0norca"} {
		m := maps.Clone(items)
		m[ItemService] = service
		if _, err := Parse("eu", m); err != nil {
			t.Errorf("service %q: %v", service, err)
		}
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
	r, err := tmpl.Render(Node{Name: "eu-workers-dc1-1-1", Datacenter: "dc1", Pool: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	d, err := Decode(r.UserData)
	if err != nil {
		t.Fatal(err)
	}
	if d.Hostname != "eu-workers-dc1-1-1" || len(d.Files) != 4 || len(d.RunCmd) != 1 || d.RunCmd[0] != enableLine("norca", "") {
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

// enableShells are the shells the line of runcmd runs in that this system
// has: sh, dash, bash and busybox sh (NORCA_TEST_BUSYBOX names one, or
// busybox on the PATH), as cloud-init (sh: dash on Debian, bash on RHEL)
// and tiny-cloud and cloud-init on Alpine (busybox ash) run it.
func enableShells(t *testing.T) map[string][]string {
	t.Helper()
	res := map[string][]string{}
	for _, name := range []string{"sh", "dash", "bash"} {
		if p, err := exec.LookPath(name); err == nil {
			res[name] = []string{p}
		}
	}
	bb := os.Getenv("NORCA_TEST_BUSYBOX")
	if bb == "" {
		bb, _ = exec.LookPath("busybox")
	}
	if bb != "" {
		// A busybox built with FEATURE_PREFER_APPLETS runs its applets
		// before the commands of PATH: the stubs work only because none
		// of their names is an applet.
		list, err := exec.Command(bb, "--list").Output()
		if err != nil {
			t.Fatalf("%s --list: %v", bb, err)
		}
		for _, stub := range []string{"systemctl", "rc-update", "rc-service"} {
			if slices.Contains(strings.Fields(string(list)), stub) {
				t.Fatalf("%s has the applet %s: the stub would not run", bb, stub)
			}
		}
		res["busybox"] = []string{bb, "sh"}
	}
	return res
}

// The line of runcmd, run in each shell against stubs of systemctl,
// rc-update and rc-service, enables and starts the service with the init
// system that runs: systemd when its directory exists (a directory of the
// test stands in for /run/systemd/system), OpenRC when rc-service is
// there, and fails on a machine with neither. A failed command stops it.
// Under systemd the OpenRC service of norca-join is removed first.
func TestEnableLine(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the line runs on Linux")
	}
	items, _ := bootstraptest.Items(t)
	tmpl, err := Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	node := Node{Name: "eu-workers-dc1-3-1", Datacenter: "dc1", Pool: "workers"}
	basic, err := tmpl.Render(node)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := tmpl.RenderJoin(node, JoinTicket{Token: "t", URLs: []string{"https://10.0.0.7:8490"}})
	if err != nil {
		t.Fatal(err)
	}
	// The lines as cloud-init reads them from the document.
	lines := map[string]string{}
	for service, userData := range map[string]string{"norca": basic.UserData, JoinUnit: joined.UserData} {
		doc, err := Decode(userData)
		if err != nil {
			t.Fatal(err)
		}
		lines[service] = doc.RunCmd[len(doc.RunCmd)-1]
	}

	tests := []struct {
		name    string
		systemd bool
		stubs   []string
		fail    string
		calls   string
		ok      bool
	}{
		{name: "systemd", systemd: true, stubs: []string{"systemctl", "rc-update", "rc-service"},
			calls: "systemctl daemon-reload\nsystemctl enable --now SVC\n", ok: true},
		{name: "openrc", stubs: []string{"rc-update", "rc-service"},
			calls: "rc-update add SVC default\nrc-service SVC start\n", ok: true},
		{name: "systemctl on a machine without systemd", stubs: []string{"systemctl", "rc-update", "rc-service"},
			calls: "rc-update add SVC default\nrc-service SVC start\n", ok: true},
		{name: "neither", stubs: []string{"systemctl"}},
		{name: "a failed daemon-reload", systemd: true, stubs: []string{"systemctl"}, fail: "systemctl",
			calls: "systemctl daemon-reload\n"},
		{name: "a failed rc-update", stubs: []string{"rc-update", "rc-service"}, fail: "rc-update",
			calls: "rc-update add SVC default\n"},
	}
	rm, err := exec.LookPath("rm")
	if err != nil {
		t.Skip("no rm")
	}
	for shellName, argv := range enableShells(t) {
		for service, line := range lines {
			for _, tt := range tests {
				t.Run(shellName+"/"+service+"/"+tt.name, func(t *testing.T) {
					dir := t.TempDir()
					bin := filepath.Join(dir, "bin")
					systemdDir := filepath.Join(dir, "run-systemd-system")
					log := filepath.Join(dir, "calls")
					// The OpenRC service of norca-join, which a systemd
					// machine removes before it enables the unit.
					openRC := filepath.Join(dir, "init.d-norca-join")
					if err := os.Mkdir(bin, 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(rm, filepath.Join(bin, "rm")); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(openRC, []byte("#!/sbin/openrc-run\n"), 0o755); err != nil {
						t.Fatal(err)
					}
					if tt.systemd {
						if err := os.Mkdir(systemdDir, 0o755); err != nil {
							t.Fatal(err)
						}
					}
					for _, stub := range tt.stubs {
						code := "0"
						if stub == tt.fail {
							code = "1"
						}
						script := "#!/bin/sh\necho \"" + stub + " $*\" >>\"$CALLS\"\nexit " + code + "\n"
						if err := os.WriteFile(filepath.Join(bin, stub), []byte(script), 0o755); err != nil {
							t.Fatal(err)
						}
					}
					run := strings.NewReplacer("/run/systemd/system", systemdDir, JoinOpenRCFile, openRC).Replace(line)
					cmd := exec.Command(argv[0], append(argv[1:], "-c", run)...)
					cmd.Env = []string{"PATH=" + bin, "CALLS=" + log}
					var stderr bytes.Buffer
					cmd.Stderr = &stderr
					err := cmd.Run()
					if tt.ok != (err == nil) {
						t.Fatalf("exit %v, want ok %v; stderr: %s", err, tt.ok, stderr.String())
					}
					calls, rerr := os.ReadFile(log)
					if rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
						t.Fatal(rerr)
					}
					if want := strings.ReplaceAll(tt.calls, "SVC", service); string(calls) != want {
						t.Fatalf("calls:\n%s\nwant:\n%s", calls, want)
					}
					if tt.name == "neither" && !strings.Contains(stderr.String(), "neither systemd nor OpenRC runs on this machine: "+service+" is not started") {
						t.Fatalf("stderr: %q", stderr.String())
					}
					// Only a systemd machine removes the OpenRC service,
					// and only that of norca-join, which the bootstrap
					// wrote.
					_, statErr := os.Stat(openRC)
					if removed := errors.Is(statErr, os.ErrNotExist); removed != (tt.systemd && service == JoinUnit) {
						t.Fatalf("the OpenRC service removed: %v (%v)", removed, statErr)
					}
				})
			}
		}
	}
}
