// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package bootstrap renders what a new machine of a node group runs first:
// a cloud-init document that writes the CA of the cluster, the certificate
// and key of the node, and the agent configuration, and starts the agent.
//
// The static parts come from the deployer, in the Variable
// nodescaler/bootstrap/<region> (see Template); the operator fills in the
// parts that belong to one node: its name, datacenter, pool, class and meta,
// and its certificate, which it signs with the node CA of the region. The key
// of the node travels in the user data, so the document is handed to the
// provider only, and the certificate is short-lived (60 days by default):
// the node is replaced before it expires.
//
// RenderJoin is the hardened bootstrap (package join): no key and no
// certificate, a one-time token with which norca-join on the machine fetches
// a certificate for a key it makes, and renews it.
package bootstrap

import (
	"bytes"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/join"

	pki "github.com/bacanoo-io/norca-pki"
)

// The items of the bootstrap Variable.
const (
	// ItemCA is the PEM of the cluster CA (the ca_file of the agents).
	ItemCA = "ca"
	// ItemNodeCA and ItemNodeCAKey are the node CA of the region and its
	// key (norca-deployer pki issue-node-ca).
	ItemNodeCA    = "node_ca"
	ItemNodeCAKey = "node_ca_key"
	// ItemAgentConfig is the agent configuration, a Go text/template with
	// the fields of Node (see the package documentation of Render).
	ItemAgentConfig = "agent_config"
	// ItemTrustDomain is the SPIFFE trust domain; empty for none.
	ItemTrustDomain = "trust_domain"
	// ItemValidity is the validity of a node certificate (default 60d).
	ItemValidity = "validity"
	// ItemTLSDir, ItemConfigFile and ItemService are where the files go and
	// the systemd unit of the agent (defaults /etc/norca/tls,
	// /etc/norca/norca.hcl and norca).
	ItemTLSDir     = "tls_dir"
	ItemConfigFile = "config_file"
	ItemService    = "service"
	// ItemJoinBinary is where norca-join is on the machines of the nodes
	// that join with a key of their own (default /usr/local/bin/norca-join).
	ItemJoinBinary = "join_binary"
)

// MetaCertExpires is the meta key of a node with the expiry of its
// certificate (RFC 3339), which Render adds: the hard age limit of the node
// comes from it.
const MetaCertExpires = "norca.cert_expires"

// DefaultValidity is the validity of a node certificate: the soft and hard
// age limits of a node (45 and 55 days) replace it before it expires.
const DefaultValidity = 60 * 24 * time.Hour

// Template is the static part of the bootstrap of a region.
type Template struct {
	Region      string
	CAPEM       []byte
	NodeCA      *pki.CertificateAuthority
	TrustDomain *url.URL
	Validity    time.Duration
	TLSDir      string
	ConfigFile  string
	Service     string
	JoinBinary  string
	agent       *template.Template
}

// JoinAuthority is what the join endpoint signs with.
func (t *Template) JoinAuthority() *join.Authority {
	return &join.Authority{CAPEM: t.CAPEM, NodeCA: t.NodeCA, TrustDomain: t.TrustDomain}
}

// Parse reads a template from the items of the bootstrap Variable.
func Parse(region string, items map[string]string) (*Template, error) {
	for _, item := range []string{ItemCA, ItemNodeCA, ItemNodeCAKey, ItemAgentConfig} {
		if strings.TrimSpace(items[item]) == "" {
			return nil, fmt.Errorf("the bootstrap has no %q", item)
		}
	}
	if block, _ := pem.Decode([]byte(items[ItemCA])); block == nil {
		return nil, errors.New("the bootstrap item \"ca\" is not a PEM certificate")
	}
	nodeCA, err := pki.ParseCA([]byte(items[ItemNodeCA]), []byte(items[ItemNodeCAKey]))
	if err != nil {
		return nil, fmt.Errorf("the node CA of the bootstrap: %w", err)
	}
	if !nodeCA.Certificate.IsCA {
		return nil, errors.New("the node CA of the bootstrap is not a CA")
	}
	agent, err := template.New("agent").Option("missingkey=error").Funcs(template.FuncMap{
		"hcl": hclString, "hclmap": hclMap,
	}).Parse(items[ItemAgentConfig])
	if err != nil {
		return nil, fmt.Errorf("the agent configuration of the bootstrap: %w", err)
	}
	t := &Template{
		Region: region, CAPEM: []byte(items[ItemCA]), NodeCA: nodeCA, agent: agent,
		Validity: DefaultValidity, TLSDir: "/etc/norca/tls", ConfigFile: "/etc/norca/norca.hcl", Service: "norca",
		JoinBinary: "/usr/local/bin/norca-join",
	}
	if td := items[ItemTrustDomain]; td != "" {
		if t.TrustDomain, err = pki.TrustDomain(td); err != nil {
			return nil, err
		}
	}
	if v := items[ItemValidity]; v != "" {
		d, err := parseDays(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("the validity %q of the bootstrap is not a duration", v)
		}
		t.Validity = d
	}
	for item, field := range map[string]*string{ItemTLSDir: &t.TLSDir, ItemConfigFile: &t.ConfigFile, ItemService: &t.Service,
		ItemJoinBinary: &t.JoinBinary} {
		if v := strings.TrimSpace(items[item]); v != "" {
			*field = v
		}
	}
	if !validService.MatchString(t.Service) {
		return nil, fmt.Errorf("the service %q of the bootstrap is not a systemd unit name", t.Service)
	}
	return t, nil
}

var validService = regexp.MustCompile(`^[A-Za-z0-9@._-]+$`)

// Node is what belongs to one node, the fields of the agent configuration
// template.
type Node struct {
	Name       string
	Region     string
	Datacenter string
	Pool       string
	Class      string
	Meta       map[string]string

	// The paths of the TLS files, filled in by Render.
	CAFile, CertFile, KeyFile string
}

// Rendered is the bootstrap of one node.
type Rendered struct {
	UserData string
	Serial   string
	NotAfter time.Time
}

// Render issues the certificate of the node and renders its cloud-init
// document. The agent configuration template sees the fields of Node and
// two functions: hcl quotes a string, hclmap writes a map as an HCL object.
func (t *Template) Render(n Node) (*Rendered, error) {
	if n.Name == "" {
		return nil, errors.New("a node needs a name")
	}
	n.Region = t.Region
	n.CAFile = t.TLSDir + "/ca.pem"
	n.CertFile = t.TLSDir + "/client.pem"
	n.KeyFile = t.TLSDir + "/client-key.pem"

	id := pki.ClientIdentity(t.Region)
	if t.TrustDomain != nil {
		spiffe, err := pki.SPIFFEID(t.TrustDomain, "region", t.Region, "node", n.Name)
		if err != nil {
			return nil, err
		}
		id = id.WithSPIFFE(spiffe)
	}
	cert, err := t.NodeCA.IssueFor(id, t.Validity)
	if err != nil {
		return nil, fmt.Errorf("issuing the certificate of node %s: %w", n.Name, err)
	}
	leaf, err := parseCert(cert.CertPEM)
	if err != nil {
		return nil, err
	}
	meta := make(map[string]string, len(n.Meta)+1)
	maps.Copy(meta, n.Meta)
	meta[MetaCertExpires] = leaf.NotAfter.UTC().Format(time.RFC3339)
	n.Meta = meta

	var agent bytes.Buffer
	if err := t.agent.Execute(&agent, n); err != nil {
		return nil, fmt.Errorf("rendering the agent configuration of node %s: %w", n.Name, err)
	}

	// The certificate file holds the leaf and the node CA: the servers verify
	// the chain the node sends against the cluster CA.
	chain := append(append([]byte{}, cert.CertPEM...), t.NodeCA.CertPEM...)
	var doc strings.Builder
	doc.WriteString("#cloud-config\n")
	doc.WriteString("hostname: " + n.Name + "\n")
	doc.WriteString("write_files:\n")
	writeFile(&doc, n.CAFile, "0644", t.CAPEM)
	writeFile(&doc, n.CertFile, "0644", chain)
	writeFile(&doc, n.KeyFile, "0600", cert.KeyPEM)
	writeFile(&doc, t.ConfigFile, "0640", agent.Bytes())
	doc.WriteString("runcmd:\n")
	doc.WriteString("  - [systemctl, enable, --now, " + t.Service + "]\n")
	return &Rendered{UserData: doc.String(), Serial: leaf.SerialNumber.Text(16), NotAfter: leaf.NotAfter}, nil
}

// writeFile writes a write_files entry with the content in base64, so no
// content needs YAML quoting.
func writeFile(b *strings.Builder, path, perm string, content []byte) {
	fmt.Fprintf(b, "  - path: %s\n    permissions: '%s'\n    encoding: b64\n    content: %s\n",
		path, perm, base64.StdEncoding.EncodeToString(content))
}

func parseCert(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, errors.New("the issued certificate is not PEM")
	}
	return x509.ParseCertificate(block.Bytes)
}

// hclString quotes a string for HCL.
func hclString(s string) string {
	return strconv.Quote(strings.ReplaceAll(s, "${", "$${"))
}

// hclMap writes a map as an HCL object, the keys sorted.
func hclMap(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteString("{\n")
	for _, k := range slices.Sorted(maps.Keys(m)) {
		fmt.Fprintf(&b, "    %s = %s\n", hclString(k), hclString(m[k]))
	}
	b.WriteString("  }")
	return b.String()
}

// parseDays reads a duration with s, m, h or d.
func parseDays(s string) (time.Duration, error) {
	if n := len(s); n > 1 && s[n-1] == 'd' {
		days, err := strconv.Atoi(s[:n-1])
		if err != nil {
			return 0, err
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
