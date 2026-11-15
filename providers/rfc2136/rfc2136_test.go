// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package rfc2136_test

import (
	"context"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	mdns "github.com/miekg/dns"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/dns/dnstest"
	"github.com/bacanoo-io/norca-platform/providers/rfc2136"
)

const (
	testZone   = "example.com"
	testKey    = "norca-acme."
	testSecret = "c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3JldA=="
)

// nameServer is a name server that takes the dynamic updates of one zone,
// signed with one TSIG key, and keeps the TXT records in memory.
type nameServer struct {
	addr string

	mu      sync.Mutex
	records map[string][]string
	updates int
}

func startNameServer(t *testing.T) *nameServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ns := &nameServer{addr: ln.Addr().String(), records: map[string][]string{}}
	mux := mdns.NewServeMux()
	mux.HandleFunc(mdns.Fqdn(testZone), ns.handle)
	srv := &mdns.Server{Listener: ln, Handler: mux, TsigSecret: map[string]string{testKey: testSecret}}
	// The server of the library takes queries only unless it is told
	// otherwise.
	srv.MsgAcceptFunc = func(mdns.Header) mdns.MsgAcceptAction { return mdns.MsgAccept }
	started := make(chan struct{})
	srv.NotifyStartedFunc = func() { close(started) }
	go func() { _ = srv.ActivateAndServe() }()
	<-started
	t.Cleanup(func() { _ = srv.Shutdown() })
	return ns
}

func (ns *nameServer) handle(w mdns.ResponseWriter, r *mdns.Msg) {
	reply := new(mdns.Msg)
	reply.SetReply(r)
	switch {
	case r.Opcode != mdns.OpcodeUpdate:
		reply.Rcode = mdns.RcodeNotImplemented
	case r.IsTsig() == nil || w.TsigStatus() != nil:
		reply.Rcode = mdns.RcodeNotAuth
	default:
		ns.mu.Lock()
		ns.updates++
		for _, rr := range r.Ns {
			txt, ok := rr.(*mdns.TXT)
			if !ok {
				continue
			}
			name := strings.TrimSuffix(txt.Hdr.Name, ".")
			value := strings.Join(txt.Txt, "")
			switch txt.Hdr.Class {
			case mdns.ClassINET:
				if !slices.Contains(ns.records[name], value) {
					ns.records[name] = append(ns.records[name], value)
				}
			case mdns.ClassNONE:
				ns.records[name] = slices.DeleteFunc(ns.records[name], func(v string) bool { return v == value })
			}
		}
		ns.mu.Unlock()
	}
	if tsig := r.IsTsig(); tsig != nil && w.TsigStatus() == nil {
		reply.SetTsig(tsig.Hdr.Name, tsig.Algorithm, 300, time.Now().Unix())
	}
	_ = w.WriteMsg(reply)
}

func (ns *nameServer) values(name string) []string {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return slices.Clone(ns.records[name])
}

func newZone(t *testing.T, shared map[string]any) *dns.Zone {
	t.Helper()
	z, err := provider.Default.NewZone(testZone, rfc2136.TypeID, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	return z
}

// The provider keeps the contract against a name server that takes signed
// dynamic updates.
func TestContract_RFC2136(t *testing.T) {
	ns := startNameServer(t)
	dnstest.Run(t, dnstest.Config{
		Zone: testZone,
		New: func(t *testing.T) dns.Provider {
			return newZone(t, map[string]any{"server": ns.addr, "tsig_key": "norca-acme", "tsig_secret": testSecret}).Provider
		},
		Lookup: func(_ *testing.T, name string) []string { return ns.values(name) },
	})
}

// An update the server refuses is an error with the answer of the server,
// and nothing is written.
func TestRFC2136_Refused(t *testing.T) {
	ns := startNameServer(t)
	rec := dns.Record{Name: "_acme-challenge.www.example.com", Value: "proof", TTL: 60}

	wrongKey := newZone(t, map[string]any{"server": ns.addr, "tsig_key": "norca-acme",
		"tsig_secret": "d3Jvbmctd3Jvbmctd3Jvbmctd3Jvbmctd3Jvbmctd3Jvbmc="})
	if err := wrongKey.Provider.Present(context.Background(), rec); err == nil {
		t.Fatal("an update signed with another secret was accepted")
	}
	unsigned := newZone(t, map[string]any{"server": ns.addr})
	err := unsigned.Provider.Present(context.Background(), rec)
	if err == nil || !strings.Contains(err.Error(), "NOTAUTH") {
		t.Fatalf("an update without a signature: %v", err)
	}
	if got := ns.values(rec.Name); len(got) != 0 {
		t.Fatalf("refused updates wrote %q", got)
	}

	// A server that does not answer is a failure to try again, not a
	// refusal.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	down := newZone(t, map[string]any{"server": addr, "timeout": "500ms"})
	if err := down.Provider.Present(context.Background(), rec); err == nil || strings.Contains(err.Error(), dns.ErrInvalid.Error()) {
		t.Fatalf("a server that is down: %v", err)
	}
}

// What the parameters may not be.
func TestRFC2136_Params(t *testing.T) {
	cases := map[string]map[string]any{
		"key without secret": {"server": "127.0.0.1", "tsig_key": "k"},
		"secret without key": {"server": "127.0.0.1", "tsig_secret": testSecret},
		"secret not base64":  {"server": "127.0.0.1", "tsig_key": "k", "tsig_secret": "not base64!"},
		"algorithm":          {"server": "127.0.0.1", "tsig_algorithm": "hmac-md5"},
		"no server":          {},
	}
	for name, shared := range cases {
		if _, err := provider.Default.NewZone(testZone, rfc2136.TypeID, shared, nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A host without a port is the DNS port; the secret is never
	// published.
	z := newZone(t, map[string]any{"server": "ns1.example.com", "tsig_key": "k", "tsig_secret": testSecret})
	if z.Published["server"] != "ns1.example.com" {
		t.Fatalf("published: %v", z.Published)
	}
	if _, ok := z.Published["tsig_secret"]; ok {
		t.Fatal("the TSIG secret is published")
	}
}
