// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package join is the hardened identity of the nodes the node scaling
// operator creates: the key of a node never leaves it.
//
// The bootstrap of such a node carries no key and no certificate, only a
// one-time token (bootstrap.Template.RenderJoin). On the node, norca-join
// makes a key, sends a certificate request with the token to the join
// endpoint of the operator (Server), and receives its certificate, signed by
// the node CA of the region. It renews the certificate with the current one
// at two thirds of its life and has the agent reload it (SIGHUP): the agent
// keeps its connections, and the node has no age limit.
//
// The endpoint proves itself with a certificate of the node CA whose only
// name is the URI EndpointURI: no node certificate carries it. A node
// certificate carries the name of its instance (NodeDNSName), by which a
// renewal is matched with the record of the instance (Store); only the
// holder of the latest certificate of an instance may renew it.
package join

import (
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// The meta of a node that joined with its own key.
const (
	MetaIdentity = "norca.identity"
	IdentityJoin = "join"
)

// ConfigFileName is the name of the configuration of norca-join in the
// bootstrap; a provider that runs the node itself (docker) recognizes a node
// that joins by it.
const ConfigFileName = "norca-join.json"

// NodeDNSName is the name of an instance in its certificate: a subdomain of
// client.<region>.norca, which the node CA may sign.
func NodeDNSName(instance, region string) string {
	return instance + ".client." + region + ".norca"
}

var instanceRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// ValidInstance reports whether a name can be the name of an instance (a
// host name label).
func ValidInstance(name string) bool { return instanceRe.MatchString(name) }

// InstanceOf returns the instance of a node certificate: the one name of the
// form <instance>.client.<region>.norca. A certificate without exactly one
// such name is not a certificate of a node that joined.
func InstanceOf(cert *x509.Certificate, region string) (string, error) {
	suffix := ".client." + region + ".norca"
	var found []string
	for _, name := range cert.DNSNames {
		if inst, ok := strings.CutSuffix(name, suffix); ok && ValidInstance(inst) {
			found = append(found, inst)
		}
	}
	switch len(found) {
	case 0:
		return "", errors.New("the certificate names no instance")
	case 1:
		return found[0], nil
	default:
		return "", fmt.Errorf("the certificate names %d instances", len(found))
	}
}

// NoTrustDomain is the URI domain of the endpoint of a cluster without
// SPIFFE: the node CA then signs URIs of this domain only (and no node
// certificate carries one).
const NoTrustDomain = "no-spiffe.invalid"

// EndpointURI is the name of the join endpoint of a region in its
// certificate.
func EndpointURI(trustDomain *url.URL, region string) *url.URL {
	host := NoTrustDomain
	if trustDomain != nil {
		host = trustDomain.Host
	}
	return &url.URL{Scheme: "spiffe", Host: host, Path: "/region/" + region + "/nodescaler/join"}
}

// Config is the configuration of norca-join on a node, written by the
// bootstrap.
type Config struct {
	// Instance is the name of the instance and of the node.
	Instance string `json:"instance"`
	Region   string `json:"region"`

	// URLs are the join endpoints, tried in order; Endpoint is the URI the
	// certificate of an endpoint must carry.
	URLs     []string `json:"urls"`
	Endpoint string   `json:"endpoint"`

	// Token is the one-time token of the join; empty once used.
	Token string `json:"token,omitempty"`

	// The files of the agent: the cluster CA (written by the bootstrap),
	// the certificate (the leaf and the node CA) and the key.
	CAFile   string `json:"ca_file"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`

	// StateFile keeps the endpoints the operator last named and when the
	// certificate was issued.
	StateFile string `json:"state_file"`

	// Service is the systemd unit of the agent: started after the join,
	// sent SIGHUP after a renewal. Unused when norca-join runs the agent.
	Service string `json:"service,omitempty"`

	// KeyStore is where the key of the node is made and kept, at the join
	// and at each renewal: KeyStoreFile (the default), KeyStoreTPM or
	// KeyStoreAuto.
	KeyStore string `json:"key_store,omitempty"`
}

// The key stores of a node.
const (
	// KeyStoreFile: the key is a PEM file (EC PRIVATE KEY).
	KeyStoreFile = "file"
	// KeyStoreTPM: the key is made in the TPM 2.0 of the machine and never
	// leaves it; the key file is a TSS2 PEM key file that only this TPM
	// can load. Without a TPM (/dev/tpmrm0) the join fails. Linux only.
	KeyStoreTPM = "tpm"
	// KeyStoreAuto: the TPM when the machine has one (/dev/tpmrm0 on
	// Linux), else a file.
	KeyStoreAuto = "auto"
)

// ValidKeyStore reports whether s is a key store; empty is the default,
// KeyStoreFile.
func ValidKeyStore(s string) bool {
	switch s {
	case "", KeyStoreFile, KeyStoreTPM, KeyStoreAuto:
		return true
	}
	return false
}

// LoadConfig reads and checks a configuration.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.Check(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

// Check checks that the configuration is complete.
func (c *Config) Check() error {
	switch {
	case !ValidInstance(c.Instance):
		return fmt.Errorf("the instance %q is not a host name", c.Instance)
	case c.Region == "":
		return errors.New("no region")
	case len(c.URLs) == 0:
		return errors.New("no join endpoint (urls)")
	case c.Endpoint == "":
		return errors.New("no endpoint name (endpoint)")
	case c.CAFile == "" || c.CertFile == "" || c.KeyFile == "" || c.StateFile == "":
		return errors.New("ca_file, cert_file, key_file and state_file are required")
	case !ValidKeyStore(c.KeyStore):
		return fmt.Errorf("the key_store %q is not file, tpm or auto", c.KeyStore)
	}
	for _, u := range c.URLs {
		p, err := url.Parse(u)
		if err != nil || p.Scheme != "https" || p.Host == "" {
			return fmt.Errorf("the join endpoint %q is not an https URL", u)
		}
	}
	return nil
}

// JoinRequest is the body of POST /v1/join.
type JoinRequest struct {
	Instance string `json:"instance"`
	Token    string `json:"token"`
	// CSR is the PEM certificate request; only its key is used.
	CSR string `json:"csr"`
	// KeyStore is where the node keeps the key (file or tpm), as the node
	// reports it: for the log, not verified.
	KeyStore string `json:"key_store,omitempty"`
}

// RenewRequest is the body of POST /v1/renew, sent with the current
// certificate of the node as the client certificate.
type RenewRequest struct {
	CSR      string `json:"csr"`
	KeyStore string `json:"key_store,omitempty"`
}

// Response is the answer to a join or a renewal.
type Response struct {
	// Certificate is the PEM of the leaf and the node CA (the cert file of
	// the agent); CA is the PEM of the cluster CA.
	Certificate string    `json:"certificate"`
	CA          string    `json:"ca"`
	NotAfter    time.Time `json:"not_after"`
	// URLs are the join endpoints now: the node uses them for its renewals.
	URLs []string `json:"urls,omitempty"`
}

// ErrorResponse is the body of a refusal.
type ErrorResponse struct {
	Error string `json:"error"`
}
