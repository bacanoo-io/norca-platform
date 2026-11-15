// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

//go:build pebble

package acme

import (
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
	_ "github.com/bacanoo-io/norca-platform/providers/rfc2136"
)

// The operator against Pebble, the test certificate authority of the ACME
// protocol, and a name server that takes dynamic updates. It needs what
// the environment names (see certificate/acme/testdata/pebble/README.md):
//
//	PEBBLE_DIRECTORY   https://pebble:14000/dir
//	PEBBLE_MANAGEMENT  https://pebble:15000
//	PEBBLE_CA_FILE     the CA of the https certificate of Pebble
//	PEBBLE_DNS         address of the name server (host:port), which is
//	                   also the resolver of Pebble
//	PEBBLE_TSIG_SECRET the secret of the TSIG key norca-acme
//	PEBBLE_HTTP_NAME   a name of the name server that points at this test
//	PEBBLE_DNS_ZONE    a zone of the name server that takes updates
//
// Pebble asks for the HTTP-01 challenge on port 5002 of the test.
func TestPebble(t *testing.T) {
	directory := os.Getenv("PEBBLE_DIRECTORY")
	if directory == "" {
		t.Skip("PEBBLE_DIRECTORY is not set")
	}
	management, nameServer := os.Getenv("PEBBLE_MANAGEMENT"), os.Getenv("PEBBLE_DNS")
	httpName, zone := os.Getenv("PEBBLE_HTTP_NAME"), os.Getenv("PEBBLE_DNS_ZONE")

	hc, err := httpClient(os.Getenv("PEBBLE_CA_FILE"), "ACME")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Get(management + "/roots/0")
	if err != nil {
		t.Fatalf("the root of Pebble: %v", err)
	}
	rootPEM, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	e := newEnv(t)
	e.op.connect = connectACME(hc, "norca-acme/test")
	e.op.now = time.Now
	e.op.zones.now = time.Now
	e.op.retryBase, e.op.retryMax = 2*time.Second, 10*time.Second

	check, err := newNameServers(nameServer)
	if err != nil {
		t.Fatal(err)
	}
	e.op.propagation = check

	// The HTTP-01 answers on the port Pebble asks.
	srv := &http.Server{Addr: ":5002", Handler: e.op.responder, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })

	// The zone of the name server, served over the DNS contract.
	z, err := provider.Default.NewZone(zone, "rfc2136", map[string]any{
		"server": nameServer, "tsig_key": "norca-acme", "tsig_secret": os.Getenv("PEBBLE_TSIG_SECRET"),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	zoneSrv := httptest.NewServer(dns.NewHandler(z, "zone-token", nil))
	t.Cleanup(zoneSrv.Close)
	e.cluster.remove(ZoneKind, "example.com")
	e.cluster.put(ZoneKind, zone, map[string]any{
		"zone": zone, "provider": "lab", "endpoint": zoneSrv.URL, "tokenVariable": "providers/lab",
	}, nil)

	e.cluster.put(IssuerKind, "pebble", map[string]any{
		"external": map[string]any{"controller": DefaultController, "config": map[string]any{
			"directory": directory, "email": "lab@example.com", "roots": string(rootPEM),
		}},
	}, nil)

	wait := func(order string) OrderStatus {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for {
			if err := e.op.Pass(context.Background()); err != nil {
				t.Fatalf("pass: %v", err)
			}
			st := e.orderStatus(order)
			if st.State == StateValid || st.State == StateFailed {
				return st
			}
			if time.Now().After(deadline) {
				t.Fatalf("the order %s is not done: %+v %+v", order, st, st.Progress)
			}
			time.Sleep(time.Second)
		}
	}

	st := wait(e.order("default.http.1", "pebble", httpName))
	if st.State != StateValid {
		t.Fatalf("the HTTP-01 order: %+v", st)
	}
	leaf := leafOf(t, st)
	if !slices.Equal(leaf.DNSNames, []string{httpName}) {
		t.Fatalf("the certificate is for %v", leaf.DNSNames)
	}
	t.Logf("HTTP-01: serial %x, valid until %s, renew_after %q", leaf.SerialNumber, leaf.NotAfter.Format(time.RFC3339), st.RenewAfter)
	if is := e.issuerStatus("pebble"); is.Account == "" || is.Heartbeat == "" || is.Problem != "" {
		t.Fatalf("the status of the issuer: %+v", is)
	}

	st = wait(e.order("default.wild.1", "pebble", zone, "*."+zone))
	if st.State != StateValid {
		t.Fatalf("the DNS-01 order: %+v", st)
	}
	wild := leafOf(t, st)
	if !slices.Contains(wild.DNSNames, "*."+zone) || !slices.Contains(wild.DNSNames, zone) {
		t.Fatalf("the certificate is for %v", wild.DNSNames)
	}
	t.Logf("DNS-01: names %v, renew_after %q", wild.DNSNames, st.RenewAfter)
	if seen, err := check.visible(context.Background(), zone, dns.ChallengeName(zone), "anything"); err != nil || seen {
		t.Fatalf("the name servers after the order: %v %v", seen, err)
	}

	serial := hex.EncodeToString(leaf.SerialNumber.Bytes())
	e.cluster.put(OrderKind, "default.http.revoke-1", map[string]any{"issuer": "pebble", "certificate": "default/http", "revoke": serial}, nil)
	if st = wait("default.http.revoke-1"); st.State != StateValid {
		t.Fatalf("the revocation: %+v", st)
	}

	// A name Pebble cannot reach fails with its problem.
	st = wait(e.order("default.nowhere.1", "pebble", "nowhere.invalid"))
	if st.State != StateFailed || st.Problem == "" {
		t.Fatalf("an order for a name that does not exist: %+v", st)
	}
	t.Logf("a name that does not exist: %s", st.Problem)
}
