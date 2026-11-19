// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"crypto/x509"
	"encoding/hex"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	xacme "golang.org/x/crypto/acme"

	"github.com/bacanoo-io/norca-platform/provider/dns"
)

func leafOf(t *testing.T, st OrderStatus) *x509.Certificate {
	t.Helper()
	certs, err := parsePEM(st.Chain)
	if err != nil || len(certs) == 0 {
		t.Fatalf("the chain of the order: %v, %d certificates", err, len(certs))
	}
	return certs[0]
}

// An issuer of the operator gets an account, whose key is a secret of the
// operator, and a status with the heartbeat and the roots; an issuer of
// another controller is left alone.
func TestIssuer_AccountAndStatus(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	e.cluster.put(IssuerKind, "other", map[string]any{"external": map[string]any{"controller": "somebody-else"}}, nil)
	e.cluster.put(IssuerKind, "internal", map[string]any{"builtin": map[string]any{}}, nil)
	e.pass()

	st := e.issuerStatus("ca")
	if st.Heartbeat != e.now.Format(time.RFC3339) || st.Account != testDirectory+"/acct/1" ||
		st.Directory != testDirectory || st.Problem != "" || st.Controller != DefaultController {
		t.Fatalf("issuer status: %+v", st)
	}
	if roots, err := parsePEM(st.Roots); err != nil || len(roots) != 1 || !roots[0].Equal(e.ca.root) {
		t.Fatalf("the roots of the status are not the configured root: %v", err)
	}
	account := e.secrets.items["norca-acme/accounts/ca"]
	if _, err := decodeKey(account["key"]); err != nil || account["directory"] != testDirectory {
		t.Fatalf("the account secret: %v %v", err, account["directory"])
	}
	if e.cluster.statusWrites["CertificateIssuer/other"]+e.cluster.statusWrites["CertificateIssuer/internal"] != 0 {
		t.Fatal("the status of an issuer of somebody else was written")
	}

	// Nothing is written again until the heartbeat is due, and the account
	// is opened once.
	e.advance(10 * time.Second)
	e.pass()
	if n := e.cluster.statusWrites["CertificateIssuer/ca"]; n != 1 {
		t.Fatalf("%d status writes within the heartbeat interval", n)
	}
	e.advance(30 * time.Second)
	e.pass()
	if st := e.issuerStatus("ca"); st.Heartbeat != e.now.Format(time.RFC3339) {
		t.Fatalf("the heartbeat was not renewed: %+v", st)
	}
	if e.ca.accounts != 1 {
		t.Fatalf("%d accounts opened", e.ca.accounts)
	}

	// After a restart the account of the stored key is used.
	e.op = e.newOperator()
	e.pass()
	if len(e.secrets.paths("norca-acme/accounts/")) != 1 {
		t.Fatal("a restart made another account secret")
	}
}

// An account key that another instance of the operator (a rolling update
// runs two) stores between this instance's read of the account secret and
// its write is not overwritten: a key for the issuer's directory is used,
// and a key for another directory fails this pass and is replaced on the
// next one, after a fresh read (review finding ATM-10).
func TestIssuer_AccountKeyStoredMeanwhile(t *testing.T) {
	tests := []struct {
		name      string
		directory string // of the key the other instance stores
	}{
		{name: "same directory", directory: testDirectory},
		{name: "other directory", directory: "https://other.test/directory"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.issuer("ca", nil)
			theirs, err := newAccountKey()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := encodeKey(theirs)
			if err != nil {
				t.Fatal(err)
			}
			const path = "norca-acme/accounts/ca"
			e.secrets.writeBeforeNextWrite(path, map[string]string{"key": encoded, "directory": tt.directory})
			e.pass()

			if stored := e.secrets.items[path]; stored["key"] != encoded || stored["directory"] != tt.directory {
				t.Fatalf("the key of the other instance was overwritten (directory %q)", stored["directory"])
			}
			if tt.directory == testDirectory {
				if st := e.issuerStatus("ca"); st.Problem != "" {
					t.Fatalf("the issuer has a problem: %s", st.Problem)
				}
				if !theirs.Equal(e.ca.lastKey) {
					t.Fatal("the account was not opened with the stored key")
				}
				return
			}
			if st := e.issuerStatus("ca"); st.Problem == "" || e.ca.accounts != 0 {
				t.Fatalf("a key for another directory was used: problem %q, %d accounts", st.Problem, e.ca.accounts)
			}

			// The next pass reads the key of the other directory and replaces
			// it with a key of its own.
			e.pass()
			if st := e.issuerStatus("ca"); st.Problem != "" {
				t.Fatalf("the issuer has a problem after the next pass: %s", st.Problem)
			}
			stored := e.secrets.items[path]
			ours, err := decodeKey(stored["key"])
			if err != nil || stored["directory"] != testDirectory || stored["key"] == encoded {
				t.Fatalf("the account secret after the next pass: %v, directory %q", err, stored["directory"])
			}
			if !ours.Equal(e.ca.lastKey) {
				t.Fatal("the account was not opened with the stored key")
			}
		})
	}
}

// An issuer that cannot work has a problem and no heartbeat, so the
// servers order nothing from it.
func TestIssuer_Problems(t *testing.T) {
	e := newEnv(t)
	e.issuer("plain-http", map[string]any{"directory": "http://ca.test/directory"})
	e.issuer("unreachable", nil)
	e.ca.failNext("connect", errors.New("connection refused"))
	e.pass()

	if st := e.issuerStatus("plain-http"); st.Heartbeat != "" || !strings.Contains(st.Problem, "must be an https URL") {
		t.Fatalf("an issuer with an http directory: %+v", st)
	}
	if st := e.issuerStatus("unreachable"); st.Heartbeat != "" || !strings.Contains(st.Problem, "connection refused") {
		t.Fatalf("an issuer whose certificate authority is away: %+v", st)
	}
	// It recovers by itself.
	e.pass()
	if st := e.issuerStatus("unreachable"); st.Heartbeat == "" || st.Problem != "" {
		t.Fatalf("the issuer after the certificate authority came back: %+v", st)
	}

	// An external account binding without its secret is a problem too.
	e.issuer("eab", map[string]any{"eab_secret": "norca-acme/eab/ca"})
	e.pass()
	if st := e.issuerStatus("eab"); !strings.Contains(st.Problem, "kid and hmac_key") {
		t.Fatalf("an issuer without its binding: %+v", st)
	}
	e.secrets.items["norca-acme/eab/ca"] = map[string]string{"kid": "kid-1", "hmac_key": "c2VjcmV0"}
	e.pass()
	if st := e.issuerStatus("eab"); st.Problem != "" || st.Heartbeat == "" {
		t.Fatalf("an issuer with its binding: %+v", st)
	}
}

// An order for a name without a DNS zone is proven with HTTP-01: the
// operator answers the challenge, has the request signed, declares the
// root in the status of the issuer and writes the chain into the order.
func TestOrder_HTTP01(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", map[string]any{"roots": e.ca.rootPEM()})
	order := e.order("default.web.1", "ca", "www.example.org")

	e.pass()
	st := e.orderStatus(order)
	if st.State != StateProcessing || st.Progress == nil || st.Progress.OrderURL != "https://ca.test/order/1" {
		t.Fatalf("after the first pass: %+v", st)
	}
	st = e.passes(order, 5)
	if st.State != StateValid || st.Problem != "" || st.Progress != nil {
		t.Fatalf("the order: %+v", st)
	}
	leaf := leafOf(t, st)
	if !slices.Equal(leaf.DNSNames, []string{"www.example.org"}) {
		t.Fatalf("the certificate is for %v", leaf.DNSNames)
	}
	if certs, _ := parsePEM(st.Chain); len(certs) != 2 || !certs[1].Equal(e.ca.intermediate) {
		t.Fatal("the chain is not the leaf and the intermediate")
	}
	if e.ca.placed != 1 {
		t.Fatalf("%d orders placed with the certificate authority", e.ca.placed)
	}

	// The answer of the challenge is gone, and the certificate is kept for
	// a revocation until it expires.
	if e.op.responder.has("tok-1-0") {
		t.Fatal("the HTTP-01 answer was not removed")
	}
	issued := "norca-acme/issued/ca/" + hex.EncodeToString(leaf.SerialNumber.Bytes())
	if kept, _ := parsePEM(e.secrets.items[issued]["cert"]); len(kept) != 1 || !kept[0].Equal(leaf) {
		t.Fatalf("the issued certificate is not kept at %s", issued)
	}
	if !e.secrets.expires[issued].Equal(leaf.NotAfter) {
		t.Fatalf("the kept certificate expires at %v, the certificate at %v", e.secrets.expires[issued], leaf.NotAfter)
	}

	// A fulfilled order is not touched again.
	writes := e.cluster.statusWrites["CertificateOrder/"+order]
	e.pass()
	if e.cluster.statusWrites["CertificateOrder/"+order] != writes {
		t.Fatal("a valid order was written again")
	}
}

// A name and its wildcard in a DNS zone are proven with DNS-01: two values
// at one challenge name, accepted only when the name servers show them,
// and removed when the order is done.
func TestOrder_DNS01Wildcard(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	order := e.order("default.wild.1", "ca", "example.com", "*.example.com")
	name := dns.ChallengeName("example.com")

	// The records are presented, but the name servers do not show them yet:
	// the certificate authority is not told to look.
	e.prop.set(false, nil)
	e.pass()
	e.pass()
	e.pass()
	st := e.orderStatus(order)
	if got := e.zone.Values(name); len(got) != 2 {
		t.Fatalf("the challenge records in the zone: %q", got)
	}
	if st.State != StateProcessing || len(st.Progress.Presented) != 2 || st.Progress.Presented[0].Zone != "example.com" {
		t.Fatalf("while waiting for the name servers: %+v %+v", st, st.Progress)
	}
	if az, _ := e.ca.GetAuthorization(t.Context(), "https://ca.test/order/1/authz/0"); az.Challenges[1].Status != xacme.StatusPending {
		t.Fatal("a challenge was accepted before its record was visible")
	}

	e.prop.set(true, nil)
	st = e.passes(order, 5)
	if st.State != StateValid {
		t.Fatalf("the order: %+v", st)
	}
	leaf := leafOf(t, st)
	if !slices.Equal(leaf.DNSNames, []string{"example.com", "*.example.com"}) {
		t.Fatalf("the certificate is for %v", leaf.DNSNames)
	}
	if got := e.zone.Values(name); len(got) != 0 {
		t.Fatalf("challenge records left in the zone: %q", got)
	}
}

// A record that never shows up at the name servers fails the attempt, and
// the order after its attempts.
func TestOrder_DNS01NeverVisible(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	order := e.order("default.wild.1", "ca", "*.example.com")
	e.prop.set(false, nil)

	e.pass() // the order is placed
	e.pass() // the record is presented, the wait begins
	e.advance(3 * time.Minute)
	e.pass()
	st := e.orderStatus(order)
	if st.State != StateProcessing || st.Progress.Attempts != 1 || !strings.Contains(st.Progress.LastError, "did not appear at the name servers") {
		t.Fatalf("after the propagation timeout: %+v %+v", st, st.Progress)
	}
	// Nothing happens before the retry time.
	e.pass()
	if got := e.orderStatus(order); got.Progress.Attempts != 1 {
		t.Fatalf("an attempt before its time: %+v", got.Progress)
	}
	for range 2 {
		e.advance(11 * time.Minute)
		e.pass()
		e.advance(3 * time.Minute)
		e.pass()
	}
	st = e.orderStatus(order)
	if st.State != StateFailed || !strings.Contains(st.Problem, "given up after 3 attempts") {
		t.Fatalf("after the last attempt: %+v", st)
	}
	if got := e.zone.Values(dns.ChallengeName("example.com")); len(got) != 0 {
		t.Fatalf("challenge records left in the zone after the failure: %q", got)
	}
}

// What cannot be proven fails the order with a reason the certificate
// shows: a wildcard without a zone, a zone whose provider is away, a proof
// the certificate authority does not find.
func TestOrder_Unprovable(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	e.issuer("no-http", map[string]any{"http01": false})

	wild := e.order("default.wild.1", "ca", "*.example.org")
	st := e.passes(wild, 4)
	if st.State != StateFailed || !strings.Contains(st.Problem, "needs the DNS-01 challenge and no DNS zone") {
		t.Fatalf("a wildcard without a zone: %+v", st)
	}

	plain := e.order("default.plain.1", "no-http", "www.example.org")
	st = e.passes(plain, 4)
	if st.State != StateFailed || !strings.Contains(st.Problem, "no challenge can be used for www.example.org") {
		t.Fatalf("a name without a zone and without HTTP-01: %+v", st)
	}

	// The provider of the zone stopped its heartbeat: a wildcard waits for
	// it, a name that can be proven over HTTP is proven there.
	e.zoneStopped = true
	e.advance(10 * time.Minute)
	stale := e.order("default.stale.1", "ca", "*.example.com")
	st = e.passes(stale, 3)
	if st.State != StateProcessing || st.Progress.Attempts != 1 || !strings.Contains(st.Progress.LastError, "reports no heartbeat: zone example.com") {
		t.Fatalf("a wildcard whose zone has no heartbeat: %+v %+v", st, st.Progress)
	}
	fallback := e.order("default.fallback.1", "ca", "www.example.com")
	if st = e.passes(fallback, 6); st.State != StateValid {
		t.Fatalf("a name of a zone without a heartbeat, over HTTP: %+v", st)
	}
	e.zoneStopped = false
	e.advance(2 * time.Minute)
	if st = e.passes(stale, 6); st.State != StateValid {
		t.Fatalf("the wildcard after the provider of the zone came back: %+v", st)
	}

	// The certificate authority does not find the proof.
	e.ca.checkHTTP = func(string, string, string) bool { return false }
	refused := e.order("default.refused.1", "ca", "www.example.org")
	st = e.passes(refused, 6)
	if st.State != StateFailed || !strings.Contains(st.Problem, "the proof was not found") {
		t.Fatalf("a proof the certificate authority refused: %+v", st)
	}
}

// A failure of the certificate authority that passes is tried again with a
// growing delay; a request it refuses fails the order at once.
func TestOrder_Errors(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)

	limited := &xacme.Error{StatusCode: http.StatusTooManyRequests, ProblemType: "urn:ietf:params:acme:error:rateLimited", Detail: "too many new orders"}
	e.ca.failNext("order", limited, errors.New("connection reset"))
	order := e.order("default.web.1", "ca", "www.example.org")
	e.pass()
	st := e.orderStatus(order)
	if st.State != StateProcessing || st.Progress.Attempts != 1 || !strings.Contains(st.Progress.LastError, "rateLimited: too many new orders") ||
		st.Progress.RetryAt != e.now.Add(time.Minute).Format(time.RFC3339) {
		t.Fatalf("after a rate limit: %+v %+v", st, st.Progress)
	}
	e.advance(61 * time.Second)
	e.pass()
	if st = e.orderStatus(order); st.Progress.Attempts != 2 || st.Progress.RetryAt != e.now.Add(2*time.Minute).Format(time.RFC3339) {
		t.Fatalf("after the second failure: %+v", st.Progress)
	}
	e.advance(121 * time.Second)
	if st = e.passes(order, 5); st.State != StateValid {
		t.Fatalf("the order after the failures passed: %+v", st)
	}

	rejected := &xacme.Error{StatusCode: http.StatusBadRequest, ProblemType: "urn:ietf:params:acme:error:rejectedIdentifier", Detail: "the name is on a block list"}
	e.ca.failNext("order", rejected)
	blocked := e.order("default.blocked.1", "ca", "blocked.example.org")
	e.pass()
	st = e.orderStatus(blocked)
	if st.State != StateFailed || st.Problem != "placing the order: rejectedIdentifier: the name is on a block list" {
		t.Fatalf("a refused order: %+v", st)
	}

	// An order without a request, and one of an issuer the operator does
	// not have, are not its work.
	e.cluster.put(OrderKind, "default.empty.1", map[string]any{"issuer": "ca", "certificate": "default/empty", "dns_names": []string{"a.example.org"}}, nil)
	e.cluster.put(OrderKind, "default.foreign.1", map[string]any{"issuer": "unknown", "certificate": "default/foreign"}, nil)
	e.pass()
	if st = e.orderStatus("default.empty.1"); st.State != StateFailed || !strings.Contains(st.Problem, "no signing request") {
		t.Fatalf("an order without a request: %+v", st)
	}
	if e.cluster.statusWrites["CertificateOrder/default.foreign.1"] != 0 {
		t.Fatal("an order of another issuer was written")
	}
}

// A chain that does not lead to the roots of the issuer is not handed on.
func TestOrder_UntrustedChain(t *testing.T) {
	e := newEnv(t)
	other := newFakeCA(t)
	e.issuer("ca", map[string]any{"roots": other.rootPEM()})
	order := e.order("default.web.1", "ca", "www.example.org")
	st := e.passes(order, 6)
	if st.State != StateFailed || st.Chain != "" || !strings.Contains(st.Problem, "is not trusted") {
		t.Fatalf("a chain of an unknown root: %+v", st)
	}
	if roots, _ := parsePEM(e.issuerStatus("ca").Roots); len(roots) != 1 || !roots[0].Equal(other.root) {
		t.Fatal("the root of the refused chain was declared")
	}
}

// The operator after a restart continues the order of the certificate
// authority it finds in the status, and answers its challenge again.
func TestOrder_Restart(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	// The proof is not found until the test says so, so the order stays
	// open across the restart.
	accept := errors.New("the certificate authority is busy")
	e.ca.failNext("accept", accept)
	order := e.order("default.web.1", "ca", "www.example.org")
	e.pass()
	e.pass()
	if st := e.orderStatus(order); st.State != StateProcessing || st.Progress.Attempts != 1 {
		t.Fatalf("before the restart: %+v %+v", st, st.Progress)
	}

	e.op = e.newOperator()
	if e.op.responder.has("tok-1-0") {
		t.Fatal("the new operator answers a challenge it has not seen")
	}
	e.advance(2 * time.Minute)
	st := e.passes(order, 5)
	if st.State != StateValid {
		t.Fatalf("the order after the restart: %+v", st)
	}
	if e.ca.placed != 1 || e.ca.accounts != 2 {
		t.Fatalf("%d orders placed, %d connections", e.ca.placed, e.ca.accounts)
	}
	if len(e.secrets.paths("norca-acme/accounts/")) != 1 {
		t.Fatal("the restart made another account key")
	}
}

// The suggestion of the certificate authority for the renewal is passed on
// when it is within the life of the certificate.
func TestOrder_RenewalInfo(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	e.ca.renewAt = time.Now().Add(6 * time.Hour).UTC().Truncate(time.Second)
	st := e.passes(e.order("default.a.1", "ca", "a.example.org"), 6)
	if st.State != StateValid || st.RenewAfter != e.ca.renewAt.Format(time.RFC3339) {
		t.Fatalf("with a suggestion: %+v", st)
	}
	e.ca.renewAt = time.Now().Add(48 * time.Hour)
	st = e.passes(e.order("default.b.1", "ca", "b.example.org"), 6)
	if st.State != StateValid || st.RenewAfter != "" {
		t.Fatalf("with a suggestion after the certificate expires: %+v", st)
	}
}

// A revocation order revokes the certificate the operator issued with that
// serial number; an unknown serial fails it.
func TestOrder_Revoke(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	st := e.passes(e.order("default.web.1", "ca", "www.example.org"), 6)
	leaf := leafOf(t, st)
	serial := hex.EncodeToString(leaf.SerialNumber.Bytes())

	e.cluster.put(OrderKind, "default.web.revoke-1", map[string]any{"issuer": "ca", "certificate": "default/web", "revoke": serial}, map[string]any{"state": StatePending})
	e.ca.failNext("revoke", errors.New("timeout"))
	e.pass()
	if got := e.orderStatus("default.web.revoke-1"); got.State != StateProcessing || got.Progress.Attempts != 1 {
		t.Fatalf("a revocation that failed once: %+v", got)
	}
	e.advance(2 * time.Minute)
	e.pass()
	if got := e.orderStatus("default.web.revoke-1"); got.State != StateValid || got.Chain != "" {
		t.Fatalf("the revocation: %+v", got)
	}
	if len(e.ca.revoked) != 1 || !slices.Equal(e.ca.revoked[0], leaf.Raw) {
		t.Fatal("the certificate authority was not asked to revoke the certificate")
	}
	if e.ca.reasons[0] != xacme.CRLReasonUnspecified {
		t.Fatalf("a revocation without a reason: reason %d", e.ca.reasons[0])
	}

	e.cluster.put(OrderKind, "default.web.revoke-2", map[string]any{"issuer": "ca", "certificate": "default/web", "revoke": "0badc0de"}, nil)
	e.pass()
	if got := e.orderStatus("default.web.revoke-2"); got.State != StateFailed || !strings.Contains(got.Problem, "is not one this controller issued") {
		t.Fatalf("a revocation of an unknown serial: %+v", got)
	}
}

// The reason the servers give a revocation goes to the certificate authority
// (RFC 8555, section 7.6): cessationOfOperation when a namespace may no
// longer ask for the names of its certificate (review finding ATM-21). A
// reason this controller does not know revokes without one.
func TestOrder_RevokeReason(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   xacme.CRLReasonCode
	}{
		{name: "cessation of operation", reason: "cessationOfOperation", want: xacme.CRLReasonCessationOfOperation},
		{name: "superseded", reason: "superseded", want: xacme.CRLReasonSuperseded},
		{name: "unknown", reason: "somethingElse", want: xacme.CRLReasonUnspecified},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.issuer("ca", nil)
			leaf := leafOf(t, e.passes(e.order("default.web.1", "ca", "www.example.org"), 6))
			serial := hex.EncodeToString(leaf.SerialNumber.Bytes())

			e.cluster.put(OrderKind, "default.web.revoke-1", map[string]any{
				"issuer": "ca", "certificate": "default/web", "revoke": serial, "reason": tt.reason,
			}, map[string]any{"state": StatePending})
			e.pass()
			if got := e.orderStatus("default.web.revoke-1"); got.State != StateValid {
				t.Fatalf("the revocation: %+v", got)
			}
			if len(e.ca.reasons) != 1 || e.ca.reasons[0] != tt.want {
				t.Fatalf("revoked with the reasons %v, want %d", e.ca.reasons, tt.want)
			}
		})
	}
}

// A certificate authority that refuses the reason of a revocation
// (badRevocationReason, RFC 8555 section 7.6) is asked once more without
// one: the revocation matters more than its reason.
func TestOrder_RevokeBadReason(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	leaf := leafOf(t, e.passes(e.order("default.web.1", "ca", "www.example.org"), 6))
	serial := hex.EncodeToString(leaf.SerialNumber.Bytes())

	e.ca.failNext("revoke", &xacme.Error{StatusCode: http.StatusBadRequest,
		ProblemType: "urn:ietf:params:acme:error:badRevocationReason", Detail: "reason not allowed"})
	e.cluster.put(OrderKind, "default.web.revoke-1", map[string]any{
		"issuer": "ca", "certificate": "default/web", "revoke": serial, "reason": "cessationOfOperation",
	}, map[string]any{"state": StatePending})
	e.pass()
	if got := e.orderStatus("default.web.revoke-1"); got.State != StateValid {
		t.Fatalf("the revocation: %+v", got)
	}
	if len(e.ca.reasons) != 1 || e.ca.reasons[0] != xacme.CRLReasonUnspecified || !slices.Equal(e.ca.revoked[0], leaf.Raw) {
		t.Fatalf("revoked with the reasons %v, want once without a reason", e.ca.reasons)
	}

	// Without a reason there is nothing to drop: the refusal fails the order.
	e.ca.failNext("revoke", &xacme.Error{StatusCode: http.StatusBadRequest,
		ProblemType: "urn:ietf:params:acme:error:badRevocationReason", Detail: "reason not allowed"})
	e.cluster.put(OrderKind, "default.web.revoke-2", map[string]any{
		"issuer": "ca", "certificate": "default/web", "revoke": serial,
	}, map[string]any{"state": StatePending})
	e.pass()
	if got := e.orderStatus("default.web.revoke-2"); got.State != StateFailed || !strings.Contains(got.Problem, "badRevocationReason") {
		t.Fatalf("a refused revocation without a reason: %+v", got)
	}
}

// What is held for an order that the servers deleted is dropped.
func TestOrder_DeletedWhileOpen(t *testing.T) {
	e := newEnv(t)
	e.issuer("ca", nil)
	e.ca.failNext("accept", errors.New("busy"))
	order := e.order("default.web.1", "ca", "www.example.org")
	e.pass()
	e.pass()
	if !e.op.responder.has("tok-1-0") {
		t.Fatal("the challenge of the open order is not answered")
	}
	e.cluster.remove(OrderKind, order)
	e.pass()
	if e.op.responder.has("tok-1-0") {
		t.Fatal("the answer of a deleted order is still served")
	}
}

// The responder answers the challenges it was given, as text, and nothing
// else.
func TestResponder(t *testing.T) {
	r := newResponder()
	r.present("tok", "tok.thumbprint")
	get := func(method, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		return rec
	}
	if rec := get(http.MethodGet, ChallengePath+"tok"); rec.Code != http.StatusOK || rec.Body.String() != "tok.thumbprint" ||
		rec.Header().Get("Content-Type") != "text/plain" {
		t.Fatalf("a challenge: %d %q", rec.Code, rec.Body.String())
	}
	for _, path := range []string{ChallengePath + "other", ChallengePath, ChallengePath + "tok/x", "/", "/.well-known/other/tok"} {
		if rec := get(http.MethodGet, path); rec.Code != http.StatusNotFound {
			t.Errorf("%s: %d", path, rec.Code)
		}
	}
	if rec := get(http.MethodPost, ChallengePath+"tok"); rec.Code != http.StatusNotFound {
		t.Errorf("POST: %d", rec.Code)
	}
	if rec := get(http.MethodGet, "/healthz"); rec.Code != http.StatusNoContent {
		t.Errorf("/healthz: %d", rec.Code)
	}
	r.cleanUp("tok")
	if rec := get(http.MethodGet, ChallengePath+"tok"); rec.Code != http.StatusNotFound {
		t.Errorf("after the clean up: %d", rec.Code)
	}
}

// The identifier of a certificate for the renewal information, with the
// example of RFC 9773, section 4.1.
func TestRenewalInfoID(t *testing.T) {
	aki, _ := hex.DecodeString("69885b6b87464041e1b37b847ba0ae2cde01c8d4")
	serial, _ := new(big.Int).SetString("87654321", 16)
	id, ok := renewalInfoID(&x509.Certificate{AuthorityKeyId: aki, SerialNumber: serial})
	if !ok || id != "aYhba4dGQEHhs3uEe6CuLN4ByNQ.AIdlQyE" {
		t.Fatalf("the identifier: %q %v", id, ok)
	}
	if _, ok := renewalInfoID(&x509.Certificate{SerialNumber: serial}); ok {
		t.Fatal("a certificate without an authority key identifier has an identifier")
	}

	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	if at, ok := timeInWindow(start, start.Add(48*time.Hour), 0.5); !ok || !at.Equal(start.Add(24*time.Hour)) {
		t.Fatalf("the middle of the window: %v %v", at, ok)
	}
	if _, ok := timeInWindow(start, start.Add(-time.Hour), 0.5); ok {
		t.Fatal("a window that ends before it starts")
	}
}

// What a failure of the certificate authority means for an order.
func TestProblemOf(t *testing.T) {
	cases := []struct {
		err       error
		text      string
		permanent bool
	}{
		{&xacme.Error{StatusCode: 403, ProblemType: "urn:ietf:params:acme:error:unauthorized", Detail: "no"}, "unauthorized: no", true},
		{&xacme.Error{StatusCode: 429, ProblemType: "urn:ietf:params:acme:error:rateLimited", Detail: "slow down"}, "rateLimited: slow down", false},
		{&xacme.Error{StatusCode: 400, ProblemType: "urn:ietf:params:acme:error:badNonce", Detail: "nonce"}, "badNonce: nonce", false},
		{&xacme.Error{StatusCode: 503, Detail: "maintenance"}, "maintenance", false},
		{errors.New("connection refused"), "connection refused", false},
	}
	for _, tc := range cases {
		text, permanent := problemOf(tc.err)
		if text != tc.text || permanent != tc.permanent {
			t.Errorf("%v: %q %v, want %q %v", tc.err, text, permanent, tc.text, tc.permanent)
		}
	}
}

// What the configuration of an issuer may not be.
func TestIssuerConfig(t *testing.T) {
	off := false
	cases := map[string]IssuerConfig{
		"no directory":  {},
		"http":          {Directory: "http://ca.example/dir"},
		"email":         {Directory: "https://ca.example/dir", Email: "nobody"},
		"no challenges": {Directory: "https://ca.example/dir", HTTP01: &off, DNS01: &DNS01Config{Disabled: true}},
	}
	for name, cfg := range cases {
		if err := cfg.validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	ok := IssuerConfig{Directory: "https://ca.example/dir", Email: "ops@example.com", HTTP01: &off}
	if err := ok.validate(); err != nil {
		t.Errorf("a valid configuration: %v", err)
	}
}

// What the flags may not be.
func TestSettings(t *testing.T) {
	if err := Run(t.Context(), []string{"-interval", "0s"}, "test"); err == nil {
		t.Error("-interval 0 accepted")
	}
	if err := Run(t.Context(), []string{"-secret-prefix", "/abs/"}, "test"); err == nil {
		t.Error("an absolute -secret-prefix accepted")
	}
	if err := Run(t.Context(), []string{"-max-attempts", "0"}, "test"); err == nil {
		t.Error("-max-attempts 0 accepted")
	}
	if err := Run(t.Context(), []string{"-version"}, "test"); err != nil {
		t.Errorf("-version: %v", err)
	}
}
