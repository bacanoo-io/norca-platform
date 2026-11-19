// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"flag"
	"testing"

	"github.com/shoenig/test/must"
)

// The operator's own Variables (the lease, the tokens of the zones) are in
// the operations namespace; the records are watched in every namespace, as
// the norca-dns pack runs it.
func TestSettings_Defaults(t *testing.T) {
	var s Settings
	fs := flag.NewFlagSet("norca-dns", flag.ContinueOnError)
	s.register(fs)
	must.NoError(t, fs.Parse(nil))
	must.Eq(t, "*", s.Namespace)
	must.Eq(t, "norca-system", s.LeaseNamespace)
	must.Eq(t, "norca-system", s.ZoneTokenNamespace)
	must.NoError(t, s.validate())
}
