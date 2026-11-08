// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pki "github.com/bacanoo-io/norca-pki"
	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
	"github.com/bacanoo-io/norca/norca/controller/fake"
)

type staticAuthority struct{ a *join.Authority }

func (s staticAuthority) Authority(context.Context) (*join.Authority, error) { return s.a, nil }

type event struct{ group, reason, message string }

type env struct {
	t       *testing.T
	backend *fake.Backend
	store   *join.Store
	tmpl    *bootstrap.Template
	root    *pki.CertificateAuthority
	srv     *httptest.Server
	server  *join.Server
	mu      sync.Mutex
	events  []event
}

func newEnv(t *testing.T) *env {
	t.Helper()
	items, root := bootstraptest.Items(t)
	tmpl, err := bootstrap.Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, backend: fake.New(), tmpl: tmpl, root: root}
	e.store = &join.Store{Vars: e.backend, Namespace: "default", Region: "eu"}
	e.server = &join.Server{Store: e.store, Authority: staticAuthority{tmpl.JoinAuthority()}, Region: "eu",
		Validity: time.Hour,
		Event: func(_ context.Context, group, reason, message string) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.events = append(e.events, event{group, reason, message})
		}}
	// The listener is wrapped here: StartTLS would put a certificate of its
	// own into Certificates, which a client without SNI (an IP address) gets
	// instead of GetCertificate.
	e.srv = httptest.NewUnstartedServer(e.server.Handler())
	e.srv.Listener = tls.NewListener(e.srv.Listener, e.server.TLSConfig())
	e.srv.Start()
	e.srv.URL = strings.Replace(e.srv.URL, "http://", "https://", 1)
	t.Cleanup(e.srv.Close)
	e.server.URLs = func(context.Context) ([]string, error) { return []string{e.srv.URL}, nil }
	return e
}

// node is the configuration of norca-join of an instance, with a token.
func (e *env) node(instance string) (*join.Client, string) {
	e.t.Helper()
	token, err := e.store.Issue(context.Background(), "workers", instance, 10*time.Minute)
	if err != nil {
		e.t.Fatal(err)
	}
	dir := e.t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), e.tmpl.CAPEM, 0o644); err != nil {
		e.t.Fatal(err)
	}
	conf := &join.Config{Instance: instance, Region: "eu", URLs: []string{e.srv.URL},
		Endpoint: join.EndpointURI(e.tmpl.TrustDomain, "eu").String(), Token: token,
		CAFile: filepath.Join(dir, "ca.pem"), CertFile: filepath.Join(dir, "client.pem"),
		KeyFile: filepath.Join(dir, "client-key.pem"), StateFile: filepath.Join(dir, "state.json")}
	path := filepath.Join(dir, join.ConfigFileName)
	data, _ := json.Marshal(conf)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		e.t.Fatal(err)
	}
	return &join.Client{Config: conf}, path
}

func (e *env) has(reason, text string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ev := range e.events {
		if ev.reason == reason && ev.group == "workers" && strings.Contains(ev.message, text) {
			return true
		}
	}
	return false
}

// The certificate a node gets: for its own key, with the names of a node
// and of its instance, verifying for client auth against the cluster CA
// through the node CA in the cert file, as the agents verify it.
func checkNodeCert(t *testing.T, e *env, c *join.Client, instance string) *x509.Certificate {
	t.Helper()
	pair, leaf, err := c.Current()
	if err != nil {
		t.Fatal(err)
	}
	if len(pair.Certificate) != 2 {
		t.Fatalf("the cert file holds %d certificates, want the leaf and the node CA", len(pair.Certificate))
	}
	inter, _ := x509.ParseCertificate(pair.Certificate[1])
	roots := x509.NewCertPool()
	roots.AddCert(e.root.Certificate)
	ints := x509.NewCertPool()
	ints.AddCert(inter)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: ints,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Fatalf("the node certificate does not verify: %v", err)
	}
	if got, err := join.InstanceOf(leaf, "eu"); err != nil || got != instance {
		t.Fatalf("the instance of the certificate is %q (%v), want %s", got, err, instance)
	}
	if len(leaf.URIs) != 1 || !strings.HasSuffix(leaf.URIs[0].String(), "/region/eu/node/"+instance) {
		t.Fatalf("the SPIFFE ID of the node is %v", leaf.URIs)
	}
	if left := time.Until(leaf.NotAfter); left < 50*time.Minute || left > time.Hour {
		t.Fatalf("a certificate of one hour ends in %s", left)
	}
	return leaf
}

func TestJoinAndRenew(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c, path := e.node("workers-1-1")

	if err := c.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	first := checkNodeCert(t, e, c, "workers-1-1")
	if !e.has("CertificateIssued", "workers-1-1 joined with its own key") {
		t.Fatalf("no CertificateIssued event: %v", e.events)
	}
	// The used token leaves the configuration file.
	if conf, err := join.LoadConfig(path); err != nil || conf.Token != "" {
		t.Fatalf("the token stays in the configuration: %v %+v", err, conf)
	}
	// The key never left the node: it is the one norca-join wrote.
	if info, err := os.Stat(c.Config.KeyFile); err != nil || info.Size() == 0 {
		t.Fatalf("no key on the node: %v", err)
	}

	// A renewal with the current certificate gives a new one, for a new key.
	keyBefore, _ := os.ReadFile(c.Config.KeyFile)
	if err := c.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	second := checkNodeCert(t, e, c, "workers-1-1")
	keyAfter, _ := os.ReadFile(c.Config.KeyFile)
	if second.SerialNumber.Cmp(first.SerialNumber) == 0 || string(keyBefore) == string(keyAfter) {
		t.Fatal("the renewal kept the certificate or the key")
	}
	if pub, ok := second.PublicKey.(*ecdsa.PublicKey); !ok || pub.Equal(first.PublicKey) {
		t.Fatal("the renewed certificate is for the key of the one before")
	}
	if !e.has("CertificateRenewed", "workers-1-1 renewed") {
		t.Fatalf("no CertificateRenewed event: %v", e.events)
	}
	// The endpoints the operator named are kept for the next renewal.
	if st, err := os.ReadFile(c.Config.StateFile); err != nil || !strings.Contains(string(st), e.srv.URL) {
		t.Fatalf("the state has no endpoints: %s %v", st, err)
	}
	if at := c.RenewAt(second); at.Before(time.Now().Add(35*time.Minute)) || at.After(time.Now().Add(41*time.Minute)) {
		t.Fatalf("a certificate of one hour renews at %s", time.Until(at))
	}
}

func TestJoinRefusals(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// A used token is void.
	c, path := e.node("workers-1-1")
	token := c.Config.Token
	if err := c.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	// The token is checked before anything else of the request: a used one
	// is refused whatever the request carries.
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	body := `{"instance":"workers-1-1","token":"` + token + `","csr":"junk"}`
	res, err := hc.Post(e.srv.URL+"/v1/join", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("a used token with a junk request: %d", res.StatusCode)
	}
	c.Config.Token = token
	var refused *join.RefusedError
	if err := c.Join(ctx, ""); !errors.As(err, &refused) || refused.Status != http.StatusForbidden {
		t.Fatalf("a used token: %v", err)
	}

	// A wrong token, and a token of another instance.
	c2, _ := e.node("workers-1-2")
	c2.Config.Token = "wrong"
	if err := c2.Join(ctx, ""); !errors.As(err, &refused) {
		t.Fatalf("a wrong token: %v", err)
	}
	c3, _ := e.node("workers-1-3")
	c3.Config.Instance = "workers-1-4"
	if err := c3.Join(ctx, ""); !errors.As(err, &refused) {
		t.Fatalf("an instance without a record: %v", err)
	}

	// An expired token.
	c5, _ := e.node("workers-1-5")
	e.store.Now = func() time.Time { return time.Now().Add(time.Hour) }
	if err := c5.Join(ctx, ""); !errors.As(err, &refused) {
		t.Fatalf("an expired token: %v", err)
	}
	e.store.Now = nil
}

// Only the latest certificate of an instance renews: a copy of an older one
// (a stolen bundle, a disk image) is refused, and the refusal is in the log
// of the group.
func TestRenewOnlyTheLatest(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c, path := e.node("workers-1-1")
	if err := c.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	stolenCert, _ := os.ReadFile(c.Config.CertFile)
	stolenKey, _ := os.ReadFile(c.Config.KeyFile)
	if err := c.Renew(ctx); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	thief := &join.Client{Config: &join.Config{Instance: "workers-1-1", Region: "eu", URLs: []string{e.srv.URL},
		Endpoint: c.Config.Endpoint, CAFile: c.Config.CAFile, CertFile: filepath.Join(dir, "c.pem"),
		KeyFile: filepath.Join(dir, "k.pem"), StateFile: filepath.Join(dir, "s.json")}}
	_ = os.WriteFile(thief.Config.CertFile, stolenCert, 0o644)
	_ = os.WriteFile(thief.Config.KeyFile, stolenKey, 0o600)
	var refused *join.RefusedError
	if err := thief.Renew(ctx); !errors.As(err, &refused) || refused.Status != http.StatusForbidden {
		t.Fatalf("an older certificate renews: %v", err)
	}
	if !e.has("RenewalRefused", "is not the latest of workers-1-1") {
		t.Fatalf("no RenewalRefused event: %v", e.events)
	}

	// Without a client certificate there is no renewal.
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	res, err := hc.Post(e.srv.URL+"/v1/renew", "application/json", strings.NewReader(`{"csr":""}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a renewal without a certificate: %d", res.StatusCode)
	}
}

// A renewal rotates the key: a request for the key of the current
// certificate is refused, and the refusal is in the log of the group.
func TestRenewWithTheSameKeyIsRefused(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c, path := e.node("workers-1-1")
	if err := c.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	pair, leaf, err := c.Current()
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "workers-1-1"}}, pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(join.RenewRequest{CSR: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))})
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, Certificates: []tls.Certificate{*pair}}}} // #nosec G402 -- a test client of the endpoint
	res, err := hc.Post(e.srv.URL+"/v1/renew", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var got join.ErrorResponse
	_ = json.NewDecoder(res.Body).Decode(&got)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusForbidden || !strings.Contains(got.Error, "a renewal needs a new key") {
		t.Fatalf("a renewal with the same key: %d %+v", res.StatusCode, got)
	}
	if !e.has("RenewalRefused", "for the key of its current certificate") {
		t.Fatalf("no RenewalRefused event: %v", e.events)
	}
	// Nothing was recorded: the current certificate still renews, with a
	// new key.
	if err := c.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if _, now, _ := c.Current(); now.SerialNumber.Cmp(leaf.SerialNumber) == 0 {
		t.Fatal("no new certificate")
	}
}

// With CheckAddress the join and the renewals come from an address the
// provider reports for the instance; a first join waits until the operator
// recorded what the provider reports.
func TestAddressBinding(t *testing.T) {
	e := newEnv(t)
	e.server.CheckAddress = true
	ctx := context.Background()
	c, path := e.node("workers-1-1")
	var refused *join.RefusedError

	// Not recorded yet: the endpoint answers 503, the node tries again (not
	// a refusal), and the token stays valid.
	err := c.Join(ctx, path)
	if err == nil || errors.As(err, &refused) || !strings.Contains(err.Error(), "has not recorded the addresses of workers-1-1 yet") {
		t.Fatalf("a join before the addresses are known: %v", err)
	}

	// From an address that is not the instance's: refused, in the log of
	// the group; the token is still not used.
	if _, err := e.store.Seen(ctx, "workers", "workers-1-1", []string{"10.0.0.9"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Join(ctx, path); !errors.As(err, &refused) || refused.Status != http.StatusForbidden ||
		!strings.Contains(refused.Message, "from 127.0.0.1, which is not an address of the instance (10.0.0.9)") {
		t.Fatalf("a join from another address: %v", err)
	}
	if !e.has("JoinAddressRefused", "a join of workers-1-1 from 127.0.0.1") {
		t.Fatalf("no JoinAddressRefused event: %v", e.events)
	}

	// From an address of the instance (IPv4 mapped into IPv6 is the same).
	if _, err := e.store.Seen(ctx, "workers", "workers-1-1", []string{"10.0.0.9", "::ffff:127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	rec := e.backend.GetVariable("default", "nodescaler/join/eu/workers-1-1")
	if rec.Items["addresses"] != "10.0.0.9,127.0.0.1" || rec.Items["seen"] == "" || rec.Items["serial"] == "" {
		t.Fatalf("the record after the join: %v", rec.Items)
	}

	// A provider that stops reporting addresses does not clear them.
	if wrote, err := e.store.Seen(ctx, "workers", "workers-1-1", nil); err != nil || wrote {
		t.Fatalf("an empty list changed the record: %v %v", wrote, err)
	}
	if err := c.Renew(ctx); err != nil {
		t.Fatal(err)
	}

	// The renewals are bound too: a new list replaces the old one.
	if _, err := e.store.Seen(ctx, "workers", "workers-1-1", []string{"10.0.0.9"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Renew(ctx); !errors.As(err, &refused) || refused.Status != http.StatusForbidden {
		t.Fatalf("a renewal from another address: %v", err)
	}
	if !e.has("JoinAddressRefused", "a renewal of workers-1-1 from 127.0.0.1") {
		t.Fatalf("no JoinAddressRefused event of the renewal: %v", e.events)
	}

	// A provider that reports no address for a running instance: the
	// operator records that it saw it, and the join is not bound.
	c2, path2 := e.node("workers-1-2")
	if _, err := e.store.Seen(ctx, "workers", "workers-1-2", nil); err != nil {
		t.Fatal(err)
	}
	if err := c2.Join(ctx, path2); err != nil {
		t.Fatalf("a join of an instance without addresses: %v", err)
	}

	// Without the check nothing waits for the operator.
	e.server.CheckAddress = false
	c3, path3 := e.node("workers-1-3")
	if err := c3.Join(ctx, path3); err != nil {
		t.Fatalf("a join without the check: %v", err)
	}
}

// A change of the record by the operator (its addresses) between the check
// of a token or a certificate and the write of the new serial is not a use:
// the join and the renewal still go through, a second use still fails.
func TestOperatorWritesDoNotRefuse(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c, _ := e.node("workers-1-1")
	rec, _, err := e.store.Redeem(ctx, "workers-1-1", c.Config.Token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Seen(ctx, "workers", "workers-1-1", []string{"10.0.0.9"}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Joined(ctx, rec, "aa"); err != nil {
		t.Fatalf("a join after a write of the operator: %v", err)
	}
	if err := e.store.Joined(ctx, rec, "bb"); !errors.Is(err, join.ErrRefused) {
		t.Fatalf("the token was used twice: %v", err)
	}
	rec, _, err = e.store.Renewing(ctx, "workers-1-1", "aa")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Seen(ctx, "workers", "workers-1-1", []string{"10.0.0.8"}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Renewed(ctx, rec, "cc"); err != nil {
		t.Fatalf("a renewal after a write of the operator: %v", err)
	}
	if err := e.store.Renewed(ctx, rec, "dd"); !errors.Is(err, join.ErrRefused) {
		t.Fatalf("a certificate renewed twice: %v", err)
	}
	got := e.backend.GetVariable("default", "nodescaler/join/eu/workers-1-1").Items
	if got["serial"] != "cc" || got["addresses"] != "10.0.0.8" || got["seen"] == "" || got["joined"] == "" ||
		got["group"] != "workers" || got["token_sha256"] != "" {
		t.Fatalf("the record: %v", got)
	}
}

// A node trusts only an endpoint with the certificate of the join endpoint:
// a server with a node certificate (any node holds one) is refused before
// the token is sent.
func TestNodeRefusesAnEndpointWithANodeCertificate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	victim, path := e.node("workers-1-1")
	attacker, apath := e.node("workers-1-2")
	if err := attacker.Join(ctx, apath); err != nil {
		t.Fatal(err)
	}
	pair, _, err := attacker.Current()
	if err != nil {
		t.Fatal(err)
	}
	var got sync.Map
	fakeSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store("request", true)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	fakeSrv.TLS = &tls.Config{Certificates: []tls.Certificate{*pair}}
	fakeSrv.StartTLS()
	defer fakeSrv.Close()

	victim.Config.URLs = []string{fakeSrv.URL}
	if err := victim.Join(ctx, path); err == nil || !strings.Contains(err.Error(), "is not for") {
		t.Fatalf("a node joined at an endpoint with a node certificate: %v", err)
	}
	if _, ok := got.Load("request"); ok {
		t.Fatal("the token reached a server with a node certificate")
	}
	// The real endpoint still takes the token.
	victim.Config.URLs = []string{fakeSrv.URL, e.srv.URL}
	if err := victim.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
}

// The loop of norca-join run: it joins, starts the agent once, and renews at
// two thirds of the life of each certificate, with a reload after each
// renewal (the agent keeps running: no second start).
func TestRunnerJoinsStartsAndRenews(t *testing.T) {
	e := newEnv(t)
	e.server.Validity = 3 * time.Second
	c, path := e.node("workers-1-1")
	var mu sync.Mutex
	starts, reloads := 0, 0
	var serials []string
	r := &join.Runner{Client: c, ConfigPath: path, Retry: 100 * time.Millisecond,
		Start: func(context.Context) error { mu.Lock(); starts++; mu.Unlock(); return nil },
		Reload: func() error {
			_, leaf, err := c.Current()
			mu.Lock()
			defer mu.Unlock()
			reloads++
			if err == nil {
				serials = append(serials, leaf.SerialNumber.String())
			}
			return nil
		}}
	ctx, cancel := context.WithTimeout(context.Background(), 5500*time.Millisecond)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// Renewals at about 2s and 4s.
	if starts != 1 || reloads < 2 {
		t.Fatalf("starts %d, reloads %d: want 1 start and at least 2 reloads", starts, reloads)
	}
	if serials[0] == serials[1] {
		t.Fatal("a reload without a new certificate")
	}
	if _, _, err := c.Current(); err != nil {
		t.Fatalf("the node has no valid certificate after the renewals: %v", err)
	}
}

// A node whose token is refused does not retry for ever: the instance is
// replaced after the join deadline.
func TestRunnerStopsOnARefusedJoin(t *testing.T) {
	e := newEnv(t)
	c, path := e.node("workers-1-1")
	c.Config.Token = "wrong"
	r := &join.Runner{Client: c, ConfigPath: path, Start: func(context.Context) error {
		t.Fatal("the agent started without a certificate")
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var refused *join.RefusedError
	if err := r.Run(ctx); !errors.As(err, &refused) {
		t.Fatalf("a refused join: %v", err)
	}
}

// Of two uses of one token at the same time, one wins.
func TestOneTokenOneJoin(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	c, _ := e.node("workers-1-1")
	token := c.Config.Token
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			dir := t.TempDir()
			cc := &join.Client{Config: &join.Config{Instance: "workers-1-1", Region: "eu", URLs: c.Config.URLs,
				Endpoint: c.Config.Endpoint, Token: token, CAFile: c.Config.CAFile, CertFile: filepath.Join(dir, "c.pem"),
				KeyFile: filepath.Join(dir, "k.pem"), StateFile: filepath.Join(dir, "s.json")}}
			results <- cc.Join(ctx, "")
		})
	}
	wg.Wait()
	close(results)
	ok := 0
	for err := range results {
		if err == nil {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("%d joins with one token, want 1", ok)
	}
}

func TestPrune(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	joined, path := e.node("workers-1-1")
	if err := joined.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	e.node("workers-1-2") // a fresh token of an instance being created
	if _, err := e.store.Issue(ctx, "other", "other-1-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	// A group whose instances start like those of "workers".
	if _, err := e.store.Issue(ctx, "workers-x", "workers-x-1-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	e.node("workers-1-3")

	// Live: none of them listed by the provider yet.
	if err := e.store.Prune(ctx, "workers", "workers", map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	exists := func(inst string) bool {
		return e.backend.GetVariable("default", "nodescaler/join/eu/"+inst) != nil
	}
	if exists("workers-1-1") {
		t.Fatal("the record of a joined instance that is gone stays")
	}
	if !exists("workers-1-2") || !exists("workers-1-3") || !exists("other-1-1") {
		t.Fatal("a record with a valid token, or of another group, was deleted")
	}

	// Once their tokens expired, the unused records go; another group's not.
	e.store.Now = func() time.Time { return time.Now().Add(time.Hour) }
	if err := e.store.Prune(ctx, "workers", "workers", map[string]bool{"workers-1-3": true}); err != nil {
		t.Fatal(err)
	}
	if exists("workers-1-2") || !exists("workers-1-3") || !exists("other-1-1") || !exists("workers-x-1-1") {
		t.Fatal("the expired record stays, or a live one or one of another group went")
	}
}

type failingAuthority struct{}

func (failingAuthority) Authority(context.Context) (*join.Authority, error) {
	return nil, errors.New("the bootstrap Variable is not readable")
}

func TestHealth(t *testing.T) {
	e := newEnv(t)
	// As the service check of the job asks: over TLS, without verifying the
	// certificate of the endpoint and without a client certificate.
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} // #nosec G402 -- as tls_skip_verify does
	resp, err := hc.Get(e.srv.URL + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	var got join.HealthResponse
	_ = json.NewDecoder(resp.Body).Decode(&got)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || got.Status != "ok" {
		t.Fatalf("health: %d %+v", resp.StatusCode, got)
	}
	// Only GET.
	resp, err = hc.Post(e.srv.URL+"/v1/health", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /v1/health: %d", resp.StatusCode)
	}

	// Without a node CA the endpoint cannot serve: 503 with the reason.
	srv := &join.Server{Store: e.store, Authority: failingAuthority{}, Region: "eu"}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/health", nil))
	got = join.HealthResponse{}
	_ = json.NewDecoder(rec.Body).Decode(&got)
	if rec.Code != http.StatusServiceUnavailable || got.Status != "unavailable" || !strings.Contains(got.Error, "not readable") {
		t.Fatalf("health without a node CA: %d %+v", rec.Code, got)
	}
}
