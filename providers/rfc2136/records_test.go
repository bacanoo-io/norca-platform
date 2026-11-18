// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package rfc2136_test

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	mdns "github.com/miekg/dns"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/dns/dnstest"
)

// recordServer is a name server of one zone that answers queries from its
// records and applies dynamic updates of any type signed with the test key,
// the way the standards ask of a primary:
//
//   - queries: a CNAME at the name answers every type (RFC 1034 3.6.2); a
//     name that does not exist is answered from the wildcard of its closest
//     encloser, with the name asked as the owner (RFC 4592 3.3.1), and with
//     NXDOMAIN without one; an empty non-terminal exists;
//   - prerequisites (RFC 2136 3.2): RRset exists (value independent), RRset
//     does not exist, name is in use, name is not in use, checked against
//     the records the zone holds, never against a wildcard;
//   - updates (RFC 2136 2.5, 3.4.2): a deletion of an RRset (class ANY), of
//     one record (class NONE), and additions (class IN), where a CNAME
//     added to a name with other data, and other data added to a name with
//     a CNAME, are ignored without an error (3.4.2.2).
type recordServer struct {
	addr string

	mu  sync.Mutex
	rrs map[string]map[uint16][]mdns.RR

	// unsignedQueries counts the queries that came without a signature.
	unsignedQueries int

	// stripSignatures sends the answers without their signature, as a
	// forger between the provider and the server would.
	stripSignatures bool
}

func startRecordServer(t *testing.T) *recordServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rs := &recordServer{addr: ln.Addr().String(), rrs: map[string]map[uint16][]mdns.RR{}}
	mux := mdns.NewServeMux()
	mux.HandleFunc(mdns.Fqdn(testZone), rs.handle)
	srv := &mdns.Server{Listener: ln, Handler: mux, TsigSecret: map[string]string{testKey: testSecret}}
	srv.MsgAcceptFunc = func(mdns.Header) mdns.MsgAcceptAction { return mdns.MsgAccept }
	started := make(chan struct{})
	srv.NotifyStartedFunc = func() { close(started) }
	go func() { _ = srv.ActivateAndServe() }()
	<-started
	t.Cleanup(func() { _ = srv.Shutdown() })
	return rs
}

func (rs *recordServer) handle(w mdns.ResponseWriter, r *mdns.Msg) {
	reply := new(mdns.Msg)
	reply.SetReply(r)
	rs.mu.Lock()
	strip := rs.stripSignatures
	switch {
	case r.Opcode == mdns.OpcodeQuery && len(r.Question) == 1:
		if r.IsTsig() == nil {
			rs.unsignedQueries++
		}
		q := r.Question[0]
		reply.Authoritative = true
		reply.Answer, reply.Rcode = rs.answer(strings.ToLower(q.Name), q.Qtype)
	case r.Opcode != mdns.OpcodeUpdate:
		reply.Rcode = mdns.RcodeNotImplemented
	case r.IsTsig() == nil || w.TsigStatus() != nil:
		reply.Rcode = mdns.RcodeNotAuth
	case rs.prerequisites(r.Answer) != mdns.RcodeSuccess:
		reply.Rcode = rs.prerequisites(r.Answer)
	default:
		for _, rr := range r.Ns {
			h := rr.Header()
			name := strings.ToLower(h.Name)
			// RFC 2136 3.4.2.2: a CNAME does not come to a name with
			// other data, nor other data to a name with a CNAME.
			_, isAlias := rs.rrs[name][mdns.TypeCNAME]
			if h.Class == mdns.ClassINET && ((h.Rrtype == mdns.TypeCNAME && !isAlias && len(rs.rrs[name]) > 0) ||
				(h.Rrtype != mdns.TypeCNAME && isAlias)) {
				continue
			}
			if rs.rrs[name] == nil {
				rs.rrs[name] = map[uint16][]mdns.RR{}
			}
			switch h.Class {
			case mdns.ClassANY:
				delete(rs.rrs[name], h.Rrtype)
			case mdns.ClassNONE:
				kept := rs.rrs[name][h.Rrtype][:0]
				for _, have := range rs.rrs[name][h.Rrtype] {
					if !sameData(have, rr) {
						kept = append(kept, have)
					}
				}
				rs.rrs[name][h.Rrtype] = kept
			case mdns.ClassINET:
				dup := false
				for _, have := range rs.rrs[name][h.Rrtype] {
					dup = dup || sameData(have, rr)
				}
				if !dup {
					rs.rrs[name][h.Rrtype] = append(rs.rrs[name][h.Rrtype], mdns.Copy(rr))
				}
				// The TTL of a set is that of its last record.
				for _, have := range rs.rrs[name][h.Rrtype] {
					have.Header().Ttl = h.Ttl
				}
			}
			if len(rs.rrs[name][h.Rrtype]) == 0 {
				delete(rs.rrs[name], h.Rrtype)
			}
			if len(rs.rrs[name]) == 0 {
				delete(rs.rrs, name)
			}
		}
	}
	rs.mu.Unlock()
	if tsig := r.IsTsig(); tsig != nil && w.TsigStatus() == nil && !strip {
		reply.SetTsig(tsig.Hdr.Name, tsig.Algorithm, 300, time.Now().Unix())
	}
	_ = w.WriteMsg(reply)
}

// exists reports whether a name is in the zone: it has records, or names
// below it have (an empty non-terminal).
func (rs *recordServer) exists(name string) bool {
	for have := range rs.rrs {
		if have == name || strings.HasSuffix(have, "."+name) {
			return true
		}
	}
	return false
}

// answer returns the answer of a query. The caller holds the lock.
func (rs *recordServer) answer(name string, qtype uint16) ([]mdns.RR, int) {
	source := name
	if !rs.exists(name) {
		// The closest encloser is the longest name above that exists
		// (the apex at the latest); its wildcard answers.
		encloser := name
		for !rs.exists(encloser) && encloser != mdns.Fqdn(testZone) {
			_, rest, _ := strings.Cut(encloser, ".")
			encloser = rest
		}
		source = "*." + encloser
		if len(rs.rrs[source]) == 0 {
			return nil, mdns.RcodeNameError
		}
	}
	rrs := rs.rrs[source][qtype]
	if alias := rs.rrs[source][mdns.TypeCNAME]; len(alias) > 0 {
		rrs = alias
	}
	out := make([]mdns.RR, 0, len(rrs))
	for _, rr := range rrs {
		c := mdns.Copy(rr)
		c.Header().Name = name
		out = append(out, c)
	}
	return out, mdns.RcodeSuccess
}

// prerequisites checks the prerequisite section of an update against the
// records of the zone. The caller holds the lock.
func (rs *recordServer) prerequisites(prereqs []mdns.RR) int {
	for _, rr := range prereqs {
		h := rr.Header()
		name := strings.ToLower(h.Name)
		inUse, hasSet := len(rs.rrs[name]) > 0, len(rs.rrs[name][h.Rrtype]) > 0
		switch {
		case h.Class == mdns.ClassANY && h.Rrtype == mdns.TypeANY && !inUse:
			return mdns.RcodeNameError
		case h.Class == mdns.ClassANY && h.Rrtype != mdns.TypeANY && !hasSet:
			return mdns.RcodeNXRrset
		case h.Class == mdns.ClassNONE && h.Rrtype == mdns.TypeANY && inUse:
			return mdns.RcodeYXDomain
		case h.Class == mdns.ClassNONE && h.Rrtype != mdns.TypeANY && hasSet:
			return mdns.RcodeYXRrset
		}
	}
	return mdns.RcodeSuccess
}

// seed puts records into the zone as its administrator would.
func (rs *recordServer) seed(t *testing.T, records ...string) {
	t.Helper()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, text := range records {
		rr, err := mdns.NewRR(text)
		if err != nil {
			t.Fatal(err)
		}
		name := strings.ToLower(rr.Header().Name)
		if rs.rrs[name] == nil {
			rs.rrs[name] = map[uint16][]mdns.RR{}
		}
		rs.rrs[name][rr.Header().Rrtype] = append(rs.rrs[name][rr.Header().Rrtype], rr)
	}
}

// sameData compares the data of two records, not their TTL or class.
func sameData(a, b mdns.RR) bool {
	ca, cb := mdns.Copy(a), mdns.Copy(b)
	ca.Header().Ttl, cb.Header().Ttl = 0, 0
	ca.Header().Class, cb.Header().Class = mdns.ClassINET, mdns.ClassINET
	return mdns.IsDuplicate(ca, cb)
}

// values returns the values of a name and type as the server holds them.
func (rs *recordServer) values(name, recordType string) []string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	var out []string
	for _, rr := range rs.rrs[mdns.Fqdn(name)][mdns.StringToType[recordType]] {
		switch v := rr.(type) {
		case *mdns.A:
			out = append(out, v.A.String())
		case *mdns.AAAA:
			out = append(out, v.AAAA.String())
		case *mdns.CNAME:
			out = append(out, strings.TrimSuffix(v.Target, "."))
		case *mdns.TXT:
			out = append(out, strings.Join(v.Txt, ""))
		}
	}
	return out
}

// The rfc2136 type keeps the records contract.
func TestRecordsContract_RFC2136(t *testing.T) {
	var rs *recordServer
	dnstest.RunRecords(t, dnstest.RecordsConfig{
		Zone: testZone,
		New: func(t *testing.T) dns.Provider {
			rs = startRecordServer(t)
			return newZone(t, map[string]any{
				"server": rs.addr, "tsig_key": testKey, "tsig_secret": testSecret,
			}).Provider
		},
		Lookup: func(_ *testing.T, name, recordType string) []string { return rs.values(name, recordType) },
	})
}

// newRecords returns the records side of an rfc2136 zone on a new name
// server.
func newRecords(t *testing.T) (*recordServer, dns.RecordProvider) {
	t.Helper()
	rs := startRecordServer(t)
	p := newZone(t, map[string]any{"server": rs.addr, "tsig_key": testKey, "tsig_secret": testSecret}).Provider
	return rs, p.(dns.RecordProvider)
}

// A name server ignores a CNAME for a name with other data, and other data
// for a name with a CNAME, and answers the update with success (RFC 2136
// 3.4.2.2): the provider says the set was not written.
func TestRecords_CNAMEStandsAlone(t *testing.T) {
	rs, p := newRecords(t)
	ctx := context.Background()
	www, alias := "www."+testZone, "alias."+testZone

	if err := p.SetRecords(ctx, dns.RecordSet{Name: www, Type: dns.TypeA, TTL: 300, Values: []string{"192.0.2.1"}}); err != nil {
		t.Fatal(err)
	}
	err := p.SetRecords(ctx, dns.RecordSet{Name: www, Type: dns.TypeCNAME, TTL: 300, Values: []string{"web." + testZone}})
	if !errors.Is(err, dns.ErrInvalid) {
		t.Fatalf("a CNAME next to an A record: %v, want an error wrapping ErrInvalid", err)
	}
	// The refusal goes to the writer of the record, who has no business
	// with the address of the name server.
	if strings.Contains(err.Error(), rs.addr) {
		t.Fatalf("the refusal names the name server: %v", err)
	}
	if got := rs.values(www, dns.TypeCNAME); len(got) != 0 {
		t.Fatalf("the name server holds the CNAME %q", got)
	}
	if got := rs.values(www, dns.TypeA); !slices.Equal(got, []string{"192.0.2.1"}) {
		t.Fatalf("the A records are %q", got)
	}

	if err := p.SetRecords(ctx, dns.RecordSet{Name: alias, Type: dns.TypeCNAME, TTL: 300, Values: []string{"web." + testZone}}); err != nil {
		t.Fatal(err)
	}
	err = p.SetRecords(ctx, dns.RecordSet{Name: alias, Type: dns.TypeTXT, TTL: 300, Values: []string{"x"}})
	if !errors.Is(err, dns.ErrInvalid) {
		t.Fatalf("a TXT record next to a CNAME: %v, want an error wrapping ErrInvalid", err)
	}
	// The CNAME answers the queries of the other types; that is not a set
	// of theirs.
	sets, err := p.Records(ctx, alias)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 1 || sets[0].Type != dns.TypeCNAME || !slices.Equal(sets[0].Values, []string{"web." + testZone}) {
		t.Fatalf("the record sets of the alias: %+v", sets)
	}
}

// An answer the name server makes from a wildcard is not a record set of
// the name: the name is free to write, and a set written at the name is
// one, also with the values of the wildcard.
func TestRecords_WildcardIsNotARecordSet(t *testing.T) {
	rs, p := newRecords(t)
	ctx := context.Background()
	rs.seed(t, "*.apps.example.com. 300 IN A 192.0.2.1", "*.apps.example.com. 300 IN TXT \"wild\"")
	shop, deep := "shop.apps."+testZone, "a.b.apps."+testZone
	types := func(name string) []string {
		t.Helper()
		sets, err := p.Records(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, set := range sets {
			out = append(out, set.Type+"="+strings.Join(set.Values, ","))
		}
		return out
	}

	for _, name := range []string{shop, deep} {
		if got := types(name); len(got) != 0 {
			t.Fatalf("the answers of the wildcard are record sets of %s: %q", name, got)
		}
	}
	// The wildcard itself is not asked through the contract, and a name
	// outside it has no answer.
	if got := types("www." + testZone); len(got) != 0 {
		t.Fatalf("www: %q", got)
	}

	// A set at the name with the values of the wildcard is a set.
	if err := p.SetRecords(ctx, dns.RecordSet{Name: shop, Type: dns.TypeA, TTL: 300, Values: []string{"192.0.2.1"}}); err != nil {
		t.Fatal(err)
	}
	if got := types(shop); !slices.Equal(got, []string{"A=192.0.2.1"}) {
		t.Fatalf("after the set was written: %q", got)
	}
	if got := rs.values(shop, dns.TypeA); !slices.Equal(got, []string{"192.0.2.1"}) {
		t.Fatalf("the name server holds %q", got)
	}
	// Other values.
	if err := p.SetRecords(ctx, dns.RecordSet{Name: shop, Type: dns.TypeA, TTL: 300, Values: []string{"192.0.2.9"}}); err != nil {
		t.Fatal(err)
	}
	if got := types(shop); !slices.Equal(got, []string{"A=192.0.2.9"}) {
		t.Fatalf("after other values: %q", got)
	}
	// Deleted: the wildcard answers again, and that is no set.
	if err := p.DeleteRecords(ctx, shop, dns.TypeA); err != nil {
		t.Fatal(err)
	}
	if got := types(shop); len(got) != 0 {
		t.Fatalf("after the deletion: %q", got)
	}
}

// With a TSIG key the provider signs its queries too, and takes no answer
// that is not signed: what it reads back decides whether a set is written
// and whose it is.
func TestRecords_AnswersAreSigned(t *testing.T) {
	rs, p := newRecords(t)
	ctx := context.Background()
	www := "www." + testZone
	set := dns.RecordSet{Name: www, Type: dns.TypeA, TTL: 300, Values: []string{"192.0.2.1"}}

	if err := p.SetRecords(ctx, set); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Records(ctx, www); err != nil {
		t.Fatal(err)
	}
	rs.mu.Lock()
	unsigned := rs.unsignedQueries
	rs.stripSignatures = true
	rs.mu.Unlock()
	if unsigned != 0 {
		t.Fatalf("%d queries were sent without a signature", unsigned)
	}

	if sets, err := p.Records(ctx, www); err == nil {
		t.Fatalf("the record sets of unsigned answers were taken: %+v", sets)
	}
	set.Values = []string{"192.0.2.2"}
	if err := p.SetRecords(ctx, set); err == nil {
		t.Fatal("a write was confirmed by unsigned answers")
	}
	if err := p.DeleteRecords(ctx, www, dns.TypeA); err == nil {
		t.Fatal("a deletion was confirmed by an unsigned answer")
	}
}
