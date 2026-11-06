// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
