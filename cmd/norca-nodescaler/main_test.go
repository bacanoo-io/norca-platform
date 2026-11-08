// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseFlags(t *testing.T) {
	t.Setenv("NORCA_REGION", "eu")
	f, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if f.region != "eu" || f.providerTTL != 3*time.Minute || !f.leaderElection || f.workers != 2 ||
		!f.bootstrap || f.bootstrapVar != "nodescaler/bootstrap/eu" || f.networkVar != "nodescaler/network/eu" ||
		!f.joinCheckAddr || f.joinKeyStore != "file" {
		t.Fatalf("defaults: %+v", f)
	}
	if f, err := parseFlags([]string{"-join-key-store=auto"}); err != nil || f.joinKeyStore != "auto" {
		t.Fatalf("-join-key-store=auto: %+v %v", f, err)
	}
	if f, err := parseFlags([]string{"-join-check-address=false"}); err != nil || f.joinCheckAddr {
		t.Fatalf("-join-check-address=false: %+v %v", f, err)
	}
	for args, want := range map[string]string{
		"-region=":            "-region must not be empty",
		"-workers=0":          "-workers must be at least 1",
		"-join-key-store=hsm": "-join-key-store \"hsm\" is not file, tpm or auto",
		"-unknown=1":          "flag provided but not defined",
	} {
		if _, err := parseFlags(strings.Fields(args)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: want %q, got %v", args, want, err)
		}
	}
}
