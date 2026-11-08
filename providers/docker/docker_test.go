// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package docker

import (
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/node"
)

func TestHostAddresses(t *testing.T) {
	for _, tc := range []struct {
		configured []string
		endpoint   string
		want       string
	}{
		{nil, "http://10.77.1.12:8480/providers/lab", "10.77.1.12"},
		{nil, "http://127.0.0.1:8480", ""},
		{nil, "http://[::]:8480", ""},
		{nil, "http://provider.lab:8480", ""},
		{[]string{"10.0.0.1", " 2001:db8::1"}, "http://10.77.1.12:8480", "10.0.0.1,2001:db8::1"},
		{[]string{"::ffff:10.0.0.1"}, "", "10.0.0.1"},
	} {
		got, err := HostAddresses(tc.configured, tc.endpoint)
		if err != nil || strings.Join(got, ",") != tc.want {
			t.Errorf("%q %q: got %v %v, want %q", tc.configured, tc.endpoint, got, err, tc.want)
		}
	}
	if _, err := HostAddresses([]string{"host.lab"}, ""); err == nil {
		t.Error("a name is accepted as an address")
	}
}

// The docker type is configured from a node section: the settings of the
// containers, the groups with their capacity and max, and the address of
// the host from the endpoint of the provider.
func TestType_FromConfig(t *testing.T) {
	f, err := provider.Parse([]byte(`{
	  "advertise_url": "http://10.77.1.12:8480",
	  "providers": [{"name": "lab", "type": "docker", "no_auth": true,
	    "node": {"params": {"state_dir": "`+strings.ReplaceAll(t.TempDir(), `\`, `/`)+`", "image": "debian:trixie-slim"},
	      "groups": [{"name": "dc2", "max": 3, "params": {"cpu": 2000, "memory": 1024, "disk": 10000}}]}}]
	}`), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	rt, err := f.Build(provider.Default, provider.ServeAll, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	p, ok := rt.Nodes[0].Provider.(*Provider)
	if !ok {
		t.Fatalf("provider %T", rt.Nodes[0].Provider)
	}
	if p.cfg.Image != "debian:trixie-slim" || p.cfg.Network != "bridge" || p.cfg.Binary != "/usr/local/bin/norca" ||
		strings.Join(p.cfg.HostAddresses, ",") != "10.77.1.12" {
		t.Fatalf("config: %+v", p.cfg)
	}
	if g := p.cfg.Groups["dc2"]; g.Max != 3 || g.Capacity != (node.Capacity{CPU: 2000, Memory: 1024, Disk: 10000}) {
		t.Fatalf("group: %+v", g)
	}
	if rt.Nodes[0].Parameters["image"] != "debian:trixie-slim" || rt.Nodes[0].Parameters["type"] != "docker" {
		t.Fatalf("published: %v", rt.Nodes[0].Parameters)
	}
}
