// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package acme is the ACME operator of Norca (norca-acme): the controller
// of certificate issuers that sign through an ACME certificate authority
// (RFC 8555), such as Let's Encrypt.
//
// The servers keep the certificates: a norca.io/Certificate object names an
// issuer, the servers make the key and the signing request, and for an
// issuer that is not built in they write a norca.io/CertificateOrder object
// with the request. This operator is the controller of the issuers whose
// spec says so:
//
//	kind = "norca.io/CertificateIssuer"
//	name = "letsencrypt"
//	spec {
//	  external {
//	    controller = "norca-acme"
//	    config {
//	      directory = "https://acme-v02.api.letsencrypt.org/directory"
//	      email     = "ops@example.com"
//	      dns01 { zones = ["example.com"] }
//	    }
//	  }
//	}
//
// For each of them it keeps an account with the certificate authority (the
// key is a secret of the operator, norca-acme/accounts/<issuer>), reports a
// heartbeat and the roots its chains lead to in the status of the issuer,
// and fulfils the orders: it places the order with the certificate
// authority, proves the control of every name, has the request signed and
// writes the chain into the status of the order. The servers check the
// chain, write the secret and delete the order. The operator never sees a
// private key of a certificate and needs no access to the secrets of the
// namespaces.
//
// A name is proven with the DNS-01 challenge when a DNS zone covers it (a
// norca.io/DNSZone object of norca-provider; always for a wildcard), and
// with the HTTP-01 challenge otherwise, which the operator answers itself
// at /.well-known/acme-challenge/ (-http01-listen). The TLS-ALPN-01
// challenge is not supported.
//
// What the operator does with an order survives a restart: the order of the
// certificate authority is in the status of the object (progress), and a
// challenge is presented again when it is still open. Run one instance: the
// HTTP-01 answers are in the memory of the instance that placed the order.
package acme

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// The kinds the operator works with. The first two are registered by the
// servers, the third with norca-provider (provider/dns/dnszone.kind.hcl).
var (
	IssuerKind = controller.Kind{Group: "norca.io", Kind: "CertificateIssuer"}
	OrderKind  = controller.Kind{Group: "norca.io", Kind: "CertificateOrder"}
	ZoneKind   = controller.Kind{Group: "norca.io", Kind: "DNSZone"}
)

// DefaultController is the name issuers name this operator by.
const DefaultController = "norca-acme"

// The states of an order, as the servers read them.
const (
	StatePending    = "pending"
	StateProcessing = "processing"
	StateValid      = "valid"
	StateFailed     = "failed"
)

// IssuerSpec is the part of the spec of a CertificateIssuer this operator
// reads.
type IssuerSpec struct {
	External *struct {
		Controller string       `json:"controller"`
		Config     IssuerConfig `json:"config"`
	} `json:"external"`
}

// IssuerConfig is the configuration of an issuer of this operator
// (spec.external.config). It is public, like every object: it holds no
// secret, only the paths of secrets.
type IssuerConfig struct {
	// Directory is the URL of the ACME directory of the certificate
	// authority.
	Directory string `json:"directory"`

	// Email is the contact of the account.
	Email string `json:"email,omitempty"`

	// EABSecret is the path of a secret of the operator with the items kid
	// and hmac_key (base64url), for a certificate authority that requires
	// an external account binding.
	EABSecret string `json:"eab_secret,omitempty"`

	// Roots are the root certificates (PEM) the chains of the certificate
	// authority lead to. Without them the chains are verified against the
	// roots of the system the operator runs on. A private or test
	// certificate authority needs them.
	Roots string `json:"roots,omitempty"`

	// HTTP01 turns the HTTP-01 challenge off when it is false; it is on
	// unless said otherwise.
	HTTP01 *bool `json:"http01,omitempty"`

	// DNS01 configures the DNS-01 challenge.
	DNS01 *DNS01Config `json:"dns01,omitempty"`
}

// DNS01Config selects the DNS zones of an issuer.
type DNS01Config struct {
	// Zones are the names of the DNSZone objects the issuer may use; empty
	// allows every zone.
	Zones []string `json:"zones,omitempty"`

	// Disabled turns the DNS-01 challenge off.
	Disabled bool `json:"disabled,omitempty"`
}

// http01 reports whether the issuer may use the HTTP-01 challenge.
func (c *IssuerConfig) http01() bool { return c.HTTP01 == nil || *c.HTTP01 }

// validate checks the configuration.
func (c *IssuerConfig) validate() error {
	u, err := url.Parse(c.Directory)
	if err != nil || u.Host == "" || u.Scheme != "https" {
		return fmt.Errorf("directory %q must be an https URL", c.Directory)
	}
	if c.Email != "" && !strings.Contains(c.Email, "@") {
		return fmt.Errorf("email %q is not an address", c.Email)
	}
	if !c.http01() && c.DNS01 != nil && c.DNS01.Disabled {
		return fmt.Errorf("http01 and dns01 are both turned off")
	}
	return nil
}

// IssuerStatus is the status of an issuer of this operator. The operator
// owns all of it; the servers read heartbeat and roots.
type IssuerStatus struct {
	// Heartbeat (RFC 3339) is when the operator last had a working account
	// with the certificate authority.
	Heartbeat string `json:"heartbeat,omitempty"`

	// Roots are the root certificates (PEM) the chains of the issuer lead
	// to: the configured ones and those the chains so far ended in.
	Roots string `json:"roots,omitempty"`

	// Account is the URL of the account at the certificate authority.
	Account string `json:"account,omitempty"`

	// Directory is the directory the account belongs to.
	Directory string `json:"directory,omitempty"`

	// Problem says why the issuer is not available.
	Problem string `json:"problem,omitempty"`

	// Controller and Version name the operator that wrote the status.
	Controller string `json:"controller,omitempty"`
	Version    string `json:"version,omitempty"`
}

// OrderSpec is the spec of a CertificateOrder, written by the servers.
type OrderSpec struct {
	Issuer      string   `json:"issuer"`
	Certificate string   `json:"certificate"`
	CSR         string   `json:"csr,omitempty"`
	DNSNames    []string `json:"dns_names,omitempty"`
	Revoke      string   `json:"revoke,omitempty"`

	// Reason is the reason of a revocation, the name of an RFC 5280
	// CRLReason (e.g. cessationOfOperation); empty for none.
	Reason string `json:"reason,omitempty"`
}

// OrderStatus is the status of a CertificateOrder, written by this
// operator.
type OrderStatus struct {
	State      string    `json:"state,omitempty"`
	Chain      string    `json:"chain,omitempty"`
	RenewAfter string    `json:"renew_after,omitempty"`
	Problem    string    `json:"problem,omitempty"`
	Progress   *Progress `json:"progress,omitempty"`
}

// Progress is what the operator remembers of an order between its passes
// and across a restart. It holds nothing secret: the proof of a challenge
// is derived from the account key when it is needed.
type Progress struct {
	// OrderURL is the order at the certificate authority.
	OrderURL string `json:"order_url,omitempty"`

	// Attempts counts the passes that failed with an error worth a retry;
	// RetryAt (RFC 3339) is when the next one is made.
	Attempts int    `json:"attempts,omitempty"`
	RetryAt  string `json:"retry_at,omitempty"`

	// LastError is the error of the last failed pass.
	LastError string `json:"last_error,omitempty"`

	// Presented are the DNS records the operator added and has to remove.
	Presented []PresentedRecord `json:"presented,omitempty"`
}

// PresentedRecord is a challenge record in a zone.
type PresentedRecord struct {
	Zone  string `json:"zone"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

// retryAt returns when the order is tried again; the zero time if at once.
func (p *Progress) retryAt() time.Time {
	if p == nil || p.RetryAt == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, p.RetryAt)
	return t
}

// decode reads a part of an object (its spec or status) into out.
func decode(part map[string]any, out any) error {
	if len(part) == 0 {
		return nil
	}
	raw, err := json.Marshal(part)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

// encode writes a value as a part of an object.
func encode(in any) (map[string]any, error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	return out, json.Unmarshal(raw, &out)
}

// issuerOf reads the configuration of an issuer of the controller; ok is
// false for an issuer of somebody else.
func issuerOf(obj *api.CustomObject, controllerName string) (cfg IssuerConfig, ok bool, err error) {
	var spec IssuerSpec
	if err := decode(obj.Spec, &spec); err != nil {
		return cfg, false, err
	}
	if spec.External == nil || spec.External.Controller != controllerName {
		return cfg, false, nil
	}
	return spec.External.Config, true, spec.External.Config.validate()
}
