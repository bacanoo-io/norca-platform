// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package bootstraptest makes the items of a bootstrap Variable for tests: a
// new cluster CA, the node CA of the region eu, and an agent configuration
// template.
package bootstraptest

import (
	"testing"
	"time"

	pki "github.com/bacanoo-io/norca-pki"
)

// AgentTemplate is an agent configuration template with every field.
const AgentTemplate = `name       = {{hcl .Name}}
region     = {{hcl .Region}}
datacenter = {{hcl .Datacenter}}
tls {
  rpc       = true
  ca_file   = {{hcl .CAFile}}
  cert_file = {{hcl .CertFile}}
  key_file  = {{hcl .KeyFile}}
}
client {
  enabled    = true
  node_pool  = {{hcl .Pool}}
  node_class = {{hcl .Class}}
  meta = {{hclmap .Meta}}
  server_join {
    retry_join = ["10.0.0.1:4647"]
  }
}
`

// Items returns the items of a bootstrap Variable of the region eu and the
// cluster CA that signed its node CA.
func Items(t testing.TB) (map[string]string, *pki.CertificateAuthority) {
	t.Helper()
	td, _ := pki.TrustDomain("lab.norca")
	root, err := pki.NewCA("test", 1, td)
	if err != nil {
		t.Fatal(err)
	}
	nodeCA, err := root.IssueNodeCA("test eu Node CA", "eu", 180*24*time.Hour, td)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"ca": string(root.CertPEM), "node_ca": string(nodeCA.CertPEM), "node_ca_key": string(nodeCA.KeyPEM),
		"agent_config": AgentTemplate, "trust_domain": "lab.norca",
	}, root
}
