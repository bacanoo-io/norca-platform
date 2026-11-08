// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider/node"
)

func TestParseGroup(t *testing.T) {
	name, cfg, err := parseGroup("dc1:4000:8192:40000:10")
	if err != nil {
		t.Fatal(err)
	}
	if name != "dc1" || cfg.Capacity != (node.Capacity{CPU: 4000, Memory: 8192, Disk: 40000}) || cfg.Max != 10 {
		t.Fatalf("got %s %+v", name, cfg)
	}
	if _, cfg, err := parseGroup("dc2:2000:4096:0"); err != nil || cfg.Max != 0 {
		t.Fatalf("without max: %+v %v", cfg, err)
	}
	for _, bad := range []string{"dc1", "dc1:4000:8192", ":4000:8192:0", "dc1:x:8192:0", "dc1:0:8192:0", "dc1:4000:-1:0"} {
		if _, _, err := parseGroup(bad); err == nil {
			t.Errorf("%q is accepted", bad)
		}
	}
}

func TestHostAddresses(t *testing.T) {
	for _, tc := range []struct{ flag, endpoint, want string }{
		{"", "http://10.77.1.12:8480", "10.77.1.12"},
		{"", "http://127.0.0.1:8480", ""},
		{"", "http://[::]:8480", ""},
		{"", "http://provider.lab:8480", ""},
		{"10.0.0.1, 2001:db8::1", "http://10.77.1.12:8480", "10.0.0.1,2001:db8::1"},
		{"::ffff:10.0.0.1", "", "10.0.0.1"},
	} {
		got, err := hostAddresses(tc.flag, tc.endpoint)
		if err != nil || strings.Join(got, ",") != tc.want {
			t.Errorf("%q %q: got %v %v, want %q", tc.flag, tc.endpoint, got, err, tc.want)
		}
	}
	if _, err := hostAddresses("host.lab", ""); err == nil {
		t.Error("a name is accepted as an address")
	}
}

func TestRun_Flags(t *testing.T) {
	for args, want := range map[string]string{
		"-group dc1:4000:8192:0": "-name is required",
		"-name p":                "at least one -group",
		"-name p -type aws -group dc1:4000:8192:0":        "unknown type",
		"-name p -group dc1:4000:8192:0 -group dc1:1:1:0": "given twice",
	} {
		err := run(strings.Fields(args))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want %q, got %v", args, want, err)
		}
	}
}
