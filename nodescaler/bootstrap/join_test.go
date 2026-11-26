// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package bootstrap

import (
	"encoding/base64"
	"encoding/json"
	"os/exec"
	"runtime"
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
	r, err := tmpl.RenderJoin(Node{Name: "eu-workers-dc1-3-1", Datacenter: "dc1", Pool: "workers",
		Meta: map[string]string{"norca.instance": "eu-workers-dc1-3-1"}},
		JoinTicket{Token: "secret-token", URLs: []string{"https://10.0.0.7:8490"}, KeyStore: join.KeyStoreAuto})
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
	if conf.Instance != "eu-workers-dc1-3-1" || conf.Token != "secret-token" || conf.Service != "norca" ||
		conf.Endpoint != "spiffe://lab.norca/region/eu/nodescaler/join" || conf.KeyFile != "/etc/norca/tls/client-key.pem" ||
		conf.KeyStore != join.KeyStoreAuto {
		t.Fatalf("join configuration: %+v", conf)
	}
	unit := string(f["/etc/systemd/system/norca-join.service"])
	if !strings.Contains(unit, "ExecStart=/opt/norca/bin/norca-join run -config /etc/norca/norca-join.json") {
		t.Fatalf("unit:\n%s", unit)
	}
	// The OpenRC service of the same program: supervised, started again
	// after 10 seconds as the unit is.
	openrc := string(f["/etc/init.d/norca-join"])
	for _, want := range []string{"#!/sbin/openrc-run\n", "\nsupervisor=supervise-daemon\n",
		"\ncommand=\"/opt/norca/bin/norca-join\"\n", "\ncommand_args=\"run -config /etc/norca/norca-join.json\"\n",
		"\nrespawn_delay=10\n", "\nrespawn_max=0\n", "\tneed net\n"} {
		if !strings.Contains(openrc, want) {
			t.Fatalf("the OpenRC service has no %q:\n%s", want, openrc)
		}
	}
	if !strings.HasSuffix(r.UserData, "runcmd:\n  - '"+enableLine(JoinUnit, JoinOpenRCFile)+"'\n") {
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
	for _, file := range doc.Files {
		if file.Path == JoinOpenRCFile && file.Mode != 0o755 {
			t.Fatalf("the OpenRC service has mode %o", file.Mode)
		}
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
	// The paths go into the unit and the OpenRC service unquoted.
	for _, binary := range []string{"norca-join", "/opt/norca join", "/opt/$(reboot)", "/opt/`reboot`", `/opt/"x`} {
		tmpl.JoinBinary = binary
		if _, err := tmpl.RenderJoin(Node{Name: "a-1-1"}, JoinTicket{Token: "t", URLs: []string{"https://x:1"}}); err == nil {
			t.Fatalf("the join binary %q", binary)
		}
	}
	tmpl.JoinBinary = "/usr/local/bin/norca-join"
	tmpl.ConfigFile = "/etc/$HOME/norca.hcl"
	if _, err := tmpl.RenderJoin(Node{Name: "a-1-1"}, JoinTicket{Token: "t", URLs: []string{"https://x:1"}}); err == nil {
		t.Fatal("a join configuration with a $")
	}
}

// The OpenRC service of norca-join is a script of sh: it parses, and it
// defines what openrc-run reads.
func TestJoinOpenRC(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the script runs on Linux")
	}
	script := joinOpenRC("/usr/local/bin/norca-join", "/etc/norca/norca-join.json")
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	// openrc-run sources the script; here sh does, and prints the variables.
	probe := script + "\nprintf '%s|%s|%s|%s\\n' \"$supervisor\" \"$command\" \"$command_args\" \"$respawn_delay\"\n"
	out, err := exec.Command(sh, "-c", probe).CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if got, want := string(out), "supervise-daemon|/usr/local/bin/norca-join|run -config /etc/norca/norca-join.json|10\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

const installScript = "#!/usr/bin/env bash\nset -euo pipefail\necho install\n"

// With an install script (install_script of the bootstrap) the document
// writes it and runs it first, before norca-join or the agent is started;
// without one the document is the one of a bootstrap that never knew the
// item.
func TestInstallScript(t *testing.T) {
	items, _ := bootstraptest.Items(t)
	plain, err := Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	items[ItemInstallScript] = installScript
	scripted, err := Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	if plain.InstallScript != "" || scripted.InstallScript != installScript {
		t.Fatalf("install scripts: %q %q", plain.InstallScript, scripted.InstallScript)
	}

	node := Node{Name: "eu-workers-dc1-3-1", Datacenter: "dc1", Pool: "workers"}
	ticket := JoinTicket{Token: "secret-token", URLs: []string{"https://10.0.0.7:8490"}}
	without, err := plain.RenderJoin(node, ticket)
	if err != nil {
		t.Fatal(err)
	}
	with, err := scripted.RenderJoin(node, ticket)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(without.UserData, InstallScriptPath) {
		t.Fatalf("a bootstrap without a script names it:\n%s", without.UserData)
	}
	// The join documents are deterministic: the one with the script is the
	// one without, plus the file and the first command.
	entry := "  - path: " + InstallScriptPath + "\n    permissions: '0700'\n    encoding: b64\n    content: " +
		base64.StdEncoding.EncodeToString([]byte(installScript)) + "\n"
	run := "  - '" + InstallScriptPath + "'\n"
	enableJoin := "  - '" + enableLine(JoinUnit, JoinOpenRCFile) + "'\n"
	if !strings.Contains(with.UserData, entry+"runcmd:\n"+run+enableJoin) {
		t.Fatalf("the script is not written and run first:\n%s", with.UserData)
	}
	if got := strings.Replace(strings.Replace(with.UserData, entry, "", 1), run, "", 1); got != without.UserData {
		t.Fatalf("without the script the documents differ:\n%s\n---\n%s", got, without.UserData)
	}
	if !strings.HasSuffix(without.UserData, "runcmd:\n"+enableJoin) {
		t.Fatalf("runcmd without a script:\n%s", without.UserData)
	}

	// Basic mode: the script runs before the agent is enabled and started.
	r, err := scripted.Render(node)
	if err != nil {
		t.Fatal(err)
	}
	enableAgent := "  - '" + enableLine("norca", "") + "'\n"
	if !strings.HasSuffix(r.UserData, "runcmd:\n"+run+enableAgent) {
		t.Fatalf("basic runcmd:\n%s", r.UserData)
	}
	if string(files(t, r.UserData)[InstallScriptPath]) != installScript {
		t.Fatal("basic: the script is not written")
	}
	r, err = plain.Render(node)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.UserData, InstallScriptPath) || !strings.HasSuffix(r.UserData, "runcmd:\n"+enableAgent) {
		t.Fatalf("basic without a script:\n%s", r.UserData)
	}

	// A provider that runs the node itself still reads the document.
	doc, err := Decode(with.UserData)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.RunCmd) != 2 || doc.RunCmd[0] != InstallScriptPath || doc.RunCmd[1] != enableLine(JoinUnit, JoinOpenRCFile) {
		t.Fatalf("runcmd: %q", doc.RunCmd)
	}
	if cfg, err := doc.ConfigFile(); err != nil || cfg != "/etc/norca/norca.hcl" || doc.JoinConfig() == "" {
		t.Fatalf("config %q %v, join %q", cfg, err, doc.JoinConfig())
	}

	// A script that is not one is refused; blank is none.
	items[ItemInstallScript] = "echo no shebang\n"
	if _, err := Parse("eu", items); err == nil {
		t.Fatal("a script without #! is accepted")
	}
	items[ItemInstallScript] = " \n"
	if tmpl, err := Parse("eu", items); err != nil || tmpl.InstallScript != "" {
		t.Fatalf("a blank script: %v", err)
	}
}
