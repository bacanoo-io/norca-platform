// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package rfc2136 is the platform "rfc2136": a DNS provider for name servers
// that accept dynamic updates (RFC 2136) signed with a TSIG key (RFC 8945),
// such as BIND, Knot DNS and PowerDNS. It has a DNS provider only, which
// writes the challenge records of norca-acme and, with records = true, the
// records of norca-dns.
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
	"slices"
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

// update sends one update message for a challenge record.
func (p *Provider) update(ctx context.Context, rec dns.Record, change func(*mdns.Msg, mdns.RR)) error {
	rr := &mdns.TXT{
		Hdr: mdns.RR_Header{Name: mdns.Fqdn(rec.Name), Rrtype: mdns.TypeTXT, Class: mdns.ClassINET, Ttl: uint32(rec.TTL)}, //nolint:gosec // the TTL is 1 to 3600
		Txt: []string{rec.Value},
	}
	return p.send(ctx, rec.Name, func(m *mdns.Msg) { change(m, rr) })
}

// The records contract: the record sets are read with queries to the server
// of the provider (the primary of the zone, which answers with authority)
// and written with one update message each, which the server applies at
// once: the old values of the name and type go and the new ones come in
// the same message.

var recordTypeCodes = map[string]uint16{
	dns.TypeA:     mdns.TypeA,
	dns.TypeAAAA:  mdns.TypeAAAA,
	dns.TypeCNAME: mdns.TypeCNAME,
	dns.TypeTXT:   mdns.TypeTXT,
}

// Records implements dns.RecordProvider.
func (p *Provider) Records(ctx context.Context, name string) ([]dns.RecordSet, error) {
	client := &mdns.Client{Net: "tcp", Timeout: p.timeout}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var out []dns.RecordSet
	for _, t := range dns.RecordTypes {
		code := recordTypeCodes[t]
		q := new(mdns.Msg)
		q.SetQuestion(mdns.Fqdn(name), code)
		q.RecursionDesired = false
		reply, _, err := client.ExchangeContext(ctx, q, p.server)
		if err != nil {
			return nil, fmt.Errorf("query of %s %s on %s: %w", name, t, p.server, err)
		}
		if reply.Rcode != mdns.RcodeSuccess && reply.Rcode != mdns.RcodeNameError {
			return nil, fmt.Errorf("query of %s %s on %s: the server answered %s", name, t, p.server, mdns.RcodeToString[reply.Rcode])
		}
		set := dns.RecordSet{Name: name, Type: t}
		for _, rr := range reply.Answer {
			h := rr.Header()
			if h.Rrtype != code || dns.Normalize(h.Name) != name {
				continue
			}
			set.TTL = int(h.Ttl)
			switch v := rr.(type) {
			case *mdns.A:
				set.Values = append(set.Values, v.A.String())
			case *mdns.AAAA:
				set.Values = append(set.Values, v.AAAA.String())
			case *mdns.CNAME:
				set.Values = append(set.Values, dns.Normalize(v.Target))
			case *mdns.TXT:
				set.Values = append(set.Values, strings.Join(v.Txt, ""))
			}
		}
		if len(set.Values) > 0 {
			slices.Sort(set.Values)
			out = append(out, set)
		}
	}
	return out, nil
}

// SetRecords implements dns.RecordProvider.
func (p *Provider) SetRecords(ctx context.Context, set dns.RecordSet) error {
	rrs := make([]mdns.RR, 0, len(set.Values))
	for _, v := range set.Values {
		rr, err := newRR(set, v)
		if err != nil {
			return dns.Invalid("%v", err)
		}
		rrs = append(rrs, rr)
	}
	return p.send(ctx, set.Name, func(m *mdns.Msg) {
		m.RemoveRRset([]mdns.RR{&mdns.ANY{Hdr: mdns.RR_Header{Name: mdns.Fqdn(set.Name), Rrtype: recordTypeCodes[set.Type], Class: mdns.ClassINET}}})
		m.Insert(rrs)
	})
}

// DeleteRecords implements dns.RecordProvider.
func (p *Provider) DeleteRecords(ctx context.Context, name, recordType string) error {
	code, ok := recordTypeCodes[recordType]
	if !ok {
		return dns.Invalid("unknown record type %q", recordType)
	}
	return p.send(ctx, name, func(m *mdns.Msg) {
		m.RemoveRRset([]mdns.RR{&mdns.ANY{Hdr: mdns.RR_Header{Name: mdns.Fqdn(name), Rrtype: code, Class: mdns.ClassINET}}})
	})
}

// newRR returns the resource record of one value of a set.
func newRR(set dns.RecordSet, value string) (mdns.RR, error) {
	hdr := mdns.RR_Header{Name: mdns.Fqdn(set.Name), Rrtype: recordTypeCodes[set.Type], Class: mdns.ClassINET, Ttl: uint32(set.TTL)} //nolint:gosec // the TTL is 1 to 86400
	switch set.Type {
	case dns.TypeA:
		ip := net.ParseIP(value).To4()
		if ip == nil {
			return nil, fmt.Errorf("%q is not an IPv4 address", value)
		}
		return &mdns.A{Hdr: hdr, A: ip}, nil
	case dns.TypeAAAA:
		ip := net.ParseIP(value)
		if ip == nil || ip.To4() != nil {
			return nil, fmt.Errorf("%q is not an IPv6 address", value)
		}
		return &mdns.AAAA{Hdr: hdr, AAAA: ip}, nil
	case dns.TypeCNAME:
		return &mdns.CNAME{Hdr: hdr, Target: mdns.Fqdn(value)}, nil
	case dns.TypeTXT:
		return &mdns.TXT{Hdr: hdr, Txt: []string{value}}, nil
	}
	return nil, fmt.Errorf("unknown record type %q", set.Type)
}

// send sends one update message of the zone, built by change, and reads the
// answer of the server.
func (p *Provider) send(ctx context.Context, name string, change func(*mdns.Msg)) error {
	msg := new(mdns.Msg)
	msg.SetUpdate(mdns.Fqdn(p.zone))
	change(msg)
	client := &mdns.Client{Net: "tcp", Timeout: p.timeout}
	if p.keyName != "" {
		msg.SetTsig(p.keyName, p.algorithm, 300, time.Now().Unix())
		client.TsigSecret = map[string]string{p.keyName: p.secret}
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	reply, _, err := client.ExchangeContext(ctx, msg, p.server)
	if err != nil {
		return fmt.Errorf("update of %s on %s: %w", name, p.server, err)
	}
	if reply.Rcode != mdns.RcodeSuccess {
		return fmt.Errorf("update of %s on %s: the server answered %s", name, p.server, mdns.RcodeToString[reply.Rcode])
	}
	return nil
}
