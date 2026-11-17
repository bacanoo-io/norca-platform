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
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
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
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	var out []dns.RecordSet
	for _, t := range dns.RecordTypes {
		set, err := p.query(ctx, name, t)
		if err != nil {
			return nil, err
		}
		if len(set.Values) == 0 || p.synthesized(ctx, set) {
			continue
		}
		out = append(out, set)
	}
	return out, nil
}

// query asks the server for the records of a name and type. The records of
// another type in the answer (the CNAME of a name that is an alias) are not
// records of the type.
func (p *Provider) query(ctx context.Context, name, recordType string) (dns.RecordSet, error) {
	client := &mdns.Client{Net: "tcp", Timeout: p.timeout}
	code := recordTypeCodes[recordType]
	set := dns.RecordSet{Name: name, Type: recordType}
	q := new(mdns.Msg)
	q.SetQuestion(mdns.Fqdn(name), code)
	q.RecursionDesired = false
	reply, _, err := client.ExchangeContext(ctx, q, p.server)
	if err != nil {
		return set, fmt.Errorf("query of %s %s on %s: %w", name, recordType, p.server, err)
	}
	if reply.Rcode != mdns.RcodeSuccess && reply.Rcode != mdns.RcodeNameError {
		return set, fmt.Errorf("query of %s %s on %s: the server answered %s", name, recordType, p.server, mdns.RcodeToString[reply.Rcode])
	}
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
	slices.Sort(set.Values)
	return set, nil
}

// synthesized reports whether the answer of a query was made by the server
// from a wildcard of the zone (RFC 4592) and is no record set of the name:
// such an answer carries the name asked and can't be told from a set by
// itself.
//
// A name beside it that can't exist is asked first: the same answer there
// says a wildcard covers both. Then the name may still hold a set of its
// own with the values of the wildcard, so the server is asked with an
// update that has only a prerequisite, "the RRset exists" (RFC 2136 3.2.1),
// and changes nothing: prerequisites are checked against the records of
// the zone, not against wildcards. Whatever is not a clear "no such RRset"
// leaves the answer a record set, which is the careful reading (a set that
// is there is not written over without adopt).
func (p *Provider) synthesized(ctx context.Context, set dns.RecordSet) bool {
	_, parent, ok := strings.Cut(set.Name, ".")
	if !ok || set.Name == p.zone || !dns.InZone(parent, p.zone) {
		return false
	}
	var label [8]byte
	if _, err := rand.Read(label[:]); err != nil {
		return false
	}
	beside := "norca-probe-" + hex.EncodeToString(label[:]) + "." + parent
	if !dns.ValidName(beside) {
		return false
	}
	probe, err := p.query(ctx, beside, set.Type)
	if err != nil || probe.TTL != set.TTL || !slices.Equal(probe.Values, set.Values) {
		return false
	}
	reply, err := p.exchange(ctx, func(m *mdns.Msg) {
		m.RRsetUsed([]mdns.RR{&mdns.ANY{Hdr: mdns.RR_Header{Name: mdns.Fqdn(set.Name), Rrtype: recordTypeCodes[set.Type], Class: mdns.ClassINET}}})
	})
	return err == nil && (reply.Rcode == mdns.RcodeNXRrset || reply.Rcode == mdns.RcodeNameError)
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
	if err := p.send(ctx, set.Name, func(m *mdns.Msg) {
		m.RemoveRRset([]mdns.RR{&mdns.ANY{Hdr: mdns.RR_Header{Name: mdns.Fqdn(set.Name), Rrtype: recordTypeCodes[set.Type], Class: mdns.ClassINET}}})
		m.Insert(rrs)
	}); err != nil {
		return err
	}

	// A server answers an update with success also when it left records
	// out of it: a CNAME for a name that has other data, and other data
	// for a name that is a CNAME, are ignored (RFC 2136 3.4.2.2). So the
	// set is read back, or a set that was never written would be reported
	// as written.
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	have, err := p.query(ctx, set.Name, set.Type)
	if err != nil {
		return err
	}
	want := slices.Clone(set.Values)
	slices.Sort(want)
	if !slices.Equal(have.Values, want) {
		why := "the name is a CNAME, which can't have other records"
		if set.Type == dns.TypeCNAME {
			why = "the name has other records, and a CNAME stands alone"
		}
		return dns.Invalid("the name server %s took the update of %s %s but does not hold its values (it holds %q): if %s, the server ignores the update",
			p.server, set.Type, set.Name, have.Values, why)
	}
	return nil
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
	reply, err := p.exchange(ctx, change)
	if err != nil {
		return fmt.Errorf("update of %s on %s: %w", name, p.server, err)
	}
	if reply.Rcode != mdns.RcodeSuccess {
		return fmt.Errorf("update of %s on %s: the server answered %s", name, p.server, mdns.RcodeToString[reply.Rcode])
	}
	return nil
}

// exchange sends one update message of the zone, built by change, and
// returns the answer of the server.
func (p *Provider) exchange(ctx context.Context, change func(*mdns.Msg)) (*mdns.Msg, error) {
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
	return reply, err
}
