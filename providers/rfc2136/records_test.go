// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package rfc2136_test

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	mdns "github.com/miekg/dns"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/dns/dnstest"
)

// recordServer is a name server of one zone that answers queries from its
// records and applies dynamic updates of any type signed with the test key:
// a deletion of an RRset (class ANY), of one record (class NONE), and
// additions (class IN), as RFC 2136 section 2.5 writes them.
type recordServer struct {
	addr string

	mu  sync.Mutex
	rrs map[string]map[uint16][]mdns.RR
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
	switch {
	case r.Opcode == mdns.OpcodeQuery && len(r.Question) == 1:
		q := r.Question[0]
		name := strings.ToLower(q.Name)
		reply.Authoritative = true
		reply.Answer = append(reply.Answer, rs.rrs[name][q.Qtype]...)
		if len(rs.rrs[name]) == 0 {
			reply.Rcode = mdns.RcodeNameError
		}
	case r.Opcode != mdns.OpcodeUpdate:
		reply.Rcode = mdns.RcodeNotImplemented
	case r.IsTsig() == nil || w.TsigStatus() != nil:
		reply.Rcode = mdns.RcodeNotAuth
	default:
		for _, rr := range r.Ns {
			h := rr.Header()
			name := strings.ToLower(h.Name)
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
	if tsig := r.IsTsig(); tsig != nil && w.TsigStatus() == nil {
		reply.SetTsig(tsig.Hdr.Name, tsig.Algorithm, 300, time.Now().Unix())
	}
	_ = w.WriteMsg(reply)
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
