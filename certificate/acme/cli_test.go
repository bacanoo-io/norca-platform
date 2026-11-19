// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"flag"
	"testing"
)

// The operator's own data (its secrets, the tokens of the DNS zones) is in
// the operations namespace, as the norca-acme pack runs it.
func TestSettings_Defaults(t *testing.T) {
	var s Settings
	fs := flag.NewFlagSet("norca-acme", flag.ContinueOnError)
	s.register(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if s.SecretNamespace != "norca-system" || s.ZoneTokenNamespace != "norca-system" {
		t.Fatalf("namespaces: secrets %q, zone tokens %q", s.SecretNamespace, s.ZoneTokenNamespace)
	}
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
}
