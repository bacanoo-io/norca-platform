// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
)

func TestParseGroup(t *testing.T) {
	name, cfg, err := parseGroup("dc1:4000:8192:40000:10")
	if err != nil {
		t.Fatal(err)
	}
	if name != "dc1" || cfg.Capacity != (provider.Capacity{CPU: 4000, Memory: 8192, Disk: 40000}) || cfg.Max != 10 {
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
