// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package rfc2136 is the platform "rfc2136": a DNS provider for name servers
// that accept dynamic updates (RFC 2136) signed with a TSIG key (RFC 8945),
// such as BIND, Knot DNS and PowerDNS. It has a DNS provider only.
//
// A value is added to and removed from the TXT records of a name with one
// update message each, sent over TCP to the server of the provider (the
// primary of the zone). Adding a record that is there and removing one that
// is not are no changes for the server, so both calls are idempotent as the
// contract asks.
package rfc2136

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"time"

	mdns "github.com/miekg/dns"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// TypeID is the ID of the type.
const TypeID = "rfc2136"

// The TSIG algorithms of the parameter tsig_algorithm.
var algorithms = map[string]string{
	"hmac-sha256": mdns.HmacSHA256,
	"hmac-sha384": mdns.HmacSHA384,
	"hmac-sha512": mdns.HmacSHA512,
}

var sharedParams = []params.Param{
	{Name: "server", Type: params.ParamString, Required: true,
		Description: "address of the name server that accepts the updates (the primary of the zones), host:port or host (port 53)"},
	{Name: "tsig_key", Type: params.ParamString,
		Description: "name of the TSIG key the updates are signed with; without it they are not signed"},
	{Name: "tsig_secret", Type: params.ParamString, Secret: true,
		Description: "secret of the TSIG key, base64 (supports env:NAME or file:/path)"},
	{Name: "tsig_algorithm", Type: params.ParamString, Default: "hmac-sha256", Enum: []string{"hmac-sha256", "hmac-sha384", "hmac-sha512"},
		Description: "algorithm of the TSIG key"},
	{Name: "timeout", Type: params.ParamDuration, Default: 10 * time.Second,
		Description: "time one update may take"},
}

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "DNS dynamic update (RFC 2136) with TSIG",
		Params:      sharedParams,
		DNS:         &provider.DNSSpec{New: New},
	})
}

// Provider writes the TXT records of one zone with dynamic updates.
type Provider struct {
	zone      string
	server    string
	keyName   string
	secret    string
	algorithm string
	timeout   time.Duration
}

// New creates the provider of a zone.
func New(cfg dns.Config) (dns.Provider, error) {
	p := &Provider{
		zone:      cfg.Zone,
		algorithm: algorithms[cfg.Values.String("tsig_algorithm")],
		timeout:   cfg.Values.Duration("timeout"),
	}
	server := strings.TrimSpace(cfg.Values.String("server"))
	if server == "" {
		return nil, fmt.Errorf("server is required")
	}
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(strings.Trim(server, "[]"), "53")
	}
	p.server = server
	if p.timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive")
	}

	key, secret := strings.TrimSpace(cfg.Values.String("tsig_key")), strings.TrimSpace(cfg.Values.String("tsig_secret"))
	switch {
	case key == "" && secret == "":
		cfg.Log().Warn("the updates of the zone are not signed: set tsig_key and tsig_secret", "zone", cfg.Zone)
	case key == "" || secret == "":
		return nil, fmt.Errorf("tsig_key and tsig_secret are set together")
	default:
		if _, err := base64.StdEncoding.DecodeString(secret); err != nil {
			return nil, fmt.Errorf("tsig_secret is not base64")
		}
		if !dns.ValidName(dns.Normalize(key)) {
			return nil, fmt.Errorf("invalid tsig_key %q", key)
		}
		p.keyName, p.secret = mdns.Fqdn(dns.Normalize(key)), secret
	}
	return p, nil
}

// Present implements dns.Provider.
func (p *Provider) Present(ctx context.Context, rec dns.Record) error {
	return p.update(ctx, rec, func(m *mdns.Msg, rr mdns.RR) { m.Insert([]mdns.RR{rr}) })
}

// CleanUp implements dns.Provider.
func (p *Provider) CleanUp(ctx context.Context, rec dns.Record) error {
	return p.update(ctx, rec, func(m *mdns.Msg, rr mdns.RR) { m.Remove([]mdns.RR{rr}) })
}

// update sends one update message for the record and reads the answer of
// the server.
func (p *Provider) update(ctx context.Context, rec dns.Record, change func(*mdns.Msg, mdns.RR)) error {
	rr := &mdns.TXT{
		Hdr: mdns.RR_Header{Name: mdns.Fqdn(rec.Name), Rrtype: mdns.TypeTXT, Class: mdns.ClassINET, Ttl: uint32(rec.TTL)}, //nolint:gosec // the TTL is 1 to 3600
		Txt: []string{rec.Value},
	}
	msg := new(mdns.Msg)
	msg.SetUpdate(mdns.Fqdn(p.zone))
	change(msg, rr)

	client := &mdns.Client{Net: "tcp", Timeout: p.timeout}
	if p.keyName != "" {
		msg.SetTsig(p.keyName, p.algorithm, 300, time.Now().Unix())
		client.TsigSecret = map[string]string{p.keyName: p.secret}
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	reply, _, err := client.ExchangeContext(ctx, msg, p.server)
	if err != nil {
		return fmt.Errorf("update of %s on %s: %w", rec.Name, p.server, err)
	}
	if reply.Rcode != mdns.RcodeSuccess {
		return fmt.Errorf("update of %s on %s: the server answered %s", rec.Name, p.server, mdns.RcodeToString[reply.Rcode])
	}
	return nil
}
