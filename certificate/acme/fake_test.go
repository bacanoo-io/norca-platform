// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	xacme "golang.org/x/crypto/acme"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/providers/mock"
	"github.com/bacanoo-io/norca/norca/api"
)

// fakeCluster keeps objects and Variables in memory.
type fakeCluster struct {
	mu      sync.Mutex
	objects map[string]*api.CustomObject // "<kind>/<name>"
	vars    map[string]map[string]string // path -> items
	index   uint64
	// statusWrites counts the status writes per object.
	statusWrites map[string]int
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{objects: map[string]*api.CustomObject{}, vars: map[string]map[string]string{}, statusWrites: map[string]int{}}
}

func (c *fakeCluster) put(kind kindRef, name string, spec, status map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.index++
	c.objects[kind.Kind+"/"+name] = &api.CustomObject{
		Group: kind.Group, Kind: kind.Kind, Name: name, Spec: spec, Status: status, ModifyIndex: c.index,
	}
}

func (c *fakeCluster) remove(kind kindRef, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.objects, kind.Kind+"/"+name)
}

func (c *fakeCluster) status(kind kindRef, name string, out any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o := c.objects[kind.Kind+"/"+name]; o != nil {
		_ = decode(o.Status, out)
	}
}

func (c *fakeCluster) ListObjects(_ context.Context, kind kindRef) ([]*api.CustomObject, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*api.CustomObject
	for _, o := range c.objects {
		if o.Kind == kind.Kind {
			cp := *o
			out = append(out, &cp)
		}
	}
	slices.SortFunc(out, func(a, b *api.CustomObject) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func (c *fakeCluster) UpdateStatus(_ context.Context, kind kindRef, name string, status map[string]any, modifyIndex uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	o := c.objects[kind.Kind+"/"+name]
	if o == nil {
		return errGone
	}
	if modifyIndex != 0 && modifyIndex != o.ModifyIndex {
		return errConflict
	}
	c.index++
	o.Status, o.ModifyIndex = status, c.index
	c.statusWrites[kind.Kind+"/"+name]++
	return nil
}

func (c *fakeCluster) ReadVariable(_ context.Context, _, path string) (*api.Variable, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	items, ok := c.vars[path]
	if !ok {
		return nil, fmt.Errorf("variable %s not found", path)
	}
	return &api.Variable{Path: path, Items: items}, nil
}

// fakeSecrets keeps secrets in memory.
type fakeSecrets struct {
	mu      sync.Mutex
	items   map[string]map[string]string
	expires map[string]time.Time
}

func newFakeSecrets() *fakeSecrets {
	return &fakeSecrets{items: map[string]map[string]string{}, expires: map[string]time.Time{}}
}

func (s *fakeSecrets) Read(_ context.Context, path string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items[path], nil
}

func (s *fakeSecrets) Write(_ context.Context, path string, items map[string]string, expiresAt *time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[path] = items
	if expiresAt != nil {
		s.expires[path] = *expiresAt
	}
	return nil
}

func (s *fakeSecrets) paths(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for p := range s.items {
		if strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	slices.Sort(out)
	return out
}

// fakeCA is a certificate authority with a root and an intermediate that
// speaks the part of ACME the operator uses. A challenge is checked when
// it is accepted, with the functions of the test.
type fakeCA struct {
	t *testing.T

	root, intermediate *x509.Certificate
	intermediateKey    *ecdsa.PrivateKey

	mu       sync.Mutex
	accounts int
	orders   map[string]*fakeOrder
	authzs   map[string]*xacme.Authorization
	placed   int
	revoked  [][]byte

	// checkHTTP and checkDNS say whether the proof of a name can be seen.
	checkHTTP func(name, token, want string) bool
	checkDNS  func(name, want string) bool

	// fail makes the next calls of an operation fail with the error.
	fail map[string][]error

	// renewAt is the suggestion of the renewal information; zero: none.
	renewAt time.Time
}

type fakeOrder struct {
	names  []string
	authzs []string
	status string
	chain  [][]byte
}

func newFakeCA(t *testing.T) *fakeCA {
	t.Helper()
	rootKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	rootTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fake root"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTmpl, rootTmpl, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := x509.ParseCertificate(rootDER)
	interKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	interTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "fake intermediate"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	interDER, err := x509.CreateCertificate(rand.Reader, interTmpl, root, &interKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	inter, _ := x509.ParseCertificate(interDER)
	return &fakeCA{
		t: t, root: root, intermediate: inter, intermediateKey: interKey,
		orders: map[string]*fakeOrder{}, authzs: map[string]*xacme.Authorization{}, fail: map[string][]error{},
		checkHTTP: func(string, string, string) bool { return true },
		checkDNS:  func(string, string) bool { return true },
	}
}

func (ca *fakeCA) rootPEM() string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.root.Raw}))
}

// connect is the connectFunc of the fake.
func (ca *fakeCA) connect(_ context.Context, req accountRequest) (authority, string, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if err := ca.next("connect"); err != nil {
		return nil, "", err
	}
	ca.accounts++
	return ca, fmt.Sprintf("%s/acct/%d", req.Directory, ca.accounts), nil
}

// next returns the error the test queued for an operation. The caller
// holds the lock.
func (ca *fakeCA) next(op string) error {
	if len(ca.fail[op]) == 0 {
		return nil
	}
	err := ca.fail[op][0]
	ca.fail[op] = ca.fail[op][1:]
	return err
}

func (ca *fakeCA) failNext(op string, errs ...error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.fail[op] = append(ca.fail[op], errs...)
}

func (ca *fakeCA) AuthorizeOrder(_ context.Context, names []string) (*xacme.Order, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if err := ca.next("order"); err != nil {
		return nil, err
	}
	ca.placed++
	url := fmt.Sprintf("https://ca.test/order/%d", ca.placed)
	o := &fakeOrder{names: names, status: xacme.StatusPending}
	for i, name := range names {
		az := &xacme.Authorization{
			URI: fmt.Sprintf("%s/authz/%d", url, i), Status: xacme.StatusPending,
			Identifier: xacme.AuthzID{Type: "dns", Value: strings.TrimPrefix(name, "*.")},
			Wildcard:   strings.HasPrefix(name, "*."),
		}
		token := fmt.Sprintf("tok-%d-%d", ca.placed, i)
		if !az.Wildcard {
			az.Challenges = append(az.Challenges, &xacme.Challenge{Type: "http-01", URI: az.URI + "/http", Token: token, Status: xacme.StatusPending})
		}
		az.Challenges = append(az.Challenges, &xacme.Challenge{Type: "dns-01", URI: az.URI + "/dns", Token: token, Status: xacme.StatusPending})
		ca.authzs[az.URI] = az
		o.authzs = append(o.authzs, az.URI)
	}
	ca.orders[url] = o
	return ca.orderLocked(url), nil
}

func (ca *fakeCA) orderLocked(url string) *xacme.Order {
	o := ca.orders[url]
	return &xacme.Order{URI: url, Status: o.status, AuthzURLs: o.authzs, FinalizeURL: url + "/finalize", CertURL: url + "/cert"}
}

func (ca *fakeCA) GetOrder(_ context.Context, url string) (*xacme.Order, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if err := ca.next("getorder"); err != nil {
		return nil, err
	}
	if ca.orders[url] == nil {
		return nil, &xacme.Error{StatusCode: http.StatusNotFound, ProblemType: "urn:ietf:params:acme:error:malformed", Detail: "no such order"}
	}
	return ca.orderLocked(url), nil
}

func (ca *fakeCA) GetAuthorization(_ context.Context, url string) (*xacme.Authorization, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	az := *ca.authzs[url]
	az.Challenges = nil
	for _, c := range ca.authzs[url].Challenges {
		cp := *c
		az.Challenges = append(az.Challenges, &cp)
	}
	return &az, nil
}

func (ca *fakeCA) HTTP01Response(token string) (string, error) { return token + ".thumbprint", nil }
func (ca *fakeCA) DNS01Record(token string) (string, error)    { return "dns-" + token, nil }

func (ca *fakeCA) Accept(_ context.Context, chal *xacme.Challenge) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if err := ca.next("accept"); err != nil {
		return err
	}
	for url, o := range ca.orders {
		for _, authzURL := range o.authzs {
			az := ca.authzs[authzURL]
			for _, c := range az.Challenges {
				if c.URI != chal.URI {
					continue
				}
				ok := false
				switch c.Type {
				case "http-01":
					ok = ca.checkHTTP(az.Identifier.Value, c.Token, c.Token+".thumbprint")
				case "dns-01":
					ok = ca.checkDNS(az.Identifier.Value, "dns-"+c.Token)
				}
				if ok {
					c.Status, az.Status = xacme.StatusValid, xacme.StatusValid
				} else {
					c.Status, az.Status = xacme.StatusInvalid, xacme.StatusInvalid
					c.Error = &xacme.Error{StatusCode: http.StatusForbidden, ProblemType: "urn:ietf:params:acme:error:unauthorized", Detail: "the proof was not found"}
					o.status = xacme.StatusInvalid
				}
			}
		}
		if o.status == xacme.StatusPending && !slices.ContainsFunc(o.authzs, func(u string) bool { return ca.authzs[u].Status != xacme.StatusValid }) {
			o.status = xacme.StatusReady
		}
		_ = url
	}
	return nil
}

func (ca *fakeCA) Finalize(_ context.Context, finalizeURL string, csrDER []byte) ([][]byte, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if err := ca.next("finalize"); err != nil {
		return nil, err
	}
	o := ca.orders[strings.TrimSuffix(finalizeURL, "/finalize")]
	if o == nil || o.status != xacme.StatusReady {
		return nil, &xacme.Error{StatusCode: http.StatusForbidden, ProblemType: "urn:ietf:params:acme:error:orderNotReady", Detail: "the order is not ready"}
	}
	csr, err := x509.ParseCertificateRequest(csrDER)
	if err != nil {
		return nil, err
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial, DNSNames: csr.DNSNames,
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(12 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leaf, err := x509.CreateCertificate(rand.Reader, tmpl, ca.intermediate, csr.PublicKey, ca.intermediateKey)
	if err != nil {
		return nil, err
	}
	o.chain = [][]byte{leaf, ca.intermediate.Raw}
	o.status = xacme.StatusValid
	return o.chain, nil
}

func (ca *fakeCA) FetchCert(_ context.Context, certURL string) ([][]byte, error) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	o := ca.orders[strings.TrimSuffix(certURL, "/cert")]
	if o == nil || o.chain == nil {
		return nil, errors.New("no certificate")
	}
	return o.chain, nil
}

func (ca *fakeCA) Revoke(_ context.Context, cert []byte) error {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if err := ca.next("revoke"); err != nil {
		return err
	}
	ca.revoked = append(ca.revoked, cert)
	return nil
}

func (ca *fakeCA) RenewalInfo(context.Context, *x509.Certificate, *x509.Certificate) (time.Time, bool) {
	return ca.renewAt, !ca.renewAt.IsZero()
}

// fakePropagation says what the test wants about the name servers.
type fakePropagation struct {
	mu    sync.Mutex
	seen  bool
	err   error
	asked int
}

func (p *fakePropagation) set(visible bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen, p.err = visible, err
}

func (p *fakePropagation) visible(context.Context, string, string, string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked++
	return p.seen, p.err
}

// env is an operator with its fakes.
type env struct {
	t       *testing.T
	op      *Operator
	cluster *fakeCluster
	secrets *fakeSecrets
	ca      *fakeCA
	prop    *fakePropagation
	now     time.Time

	// zone is the DNS provider of the zone example.com, served over the
	// contract; zoneStopped stops its heartbeat.
	zone        *mock.DNS
	zoneStopped bool
}

const testDirectory = "https://ca.test/directory"

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{
		t: t, cluster: newFakeCluster(), secrets: newFakeSecrets(), ca: newFakeCA(t),
		prop: &fakePropagation{seen: true}, now: time.Now().UTC().Truncate(time.Second),
	}
	e.ca.checkHTTP = func(_, token, want string) bool {
		rec := httptest.NewRecorder()
		e.op.responder.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, ChallengePath+token, nil))
		return rec.Code == http.StatusOK && rec.Body.String() == want
	}
	e.ca.checkDNS = func(name, want string) bool {
		return slices.Contains(e.zone.Values(dns.ChallengeName(name)), want)
	}

	// The zone example.com of a provider, with a token in a Variable.
	z, err := provider.Default.NewZone("example.com", mock.TypeID, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	e.zone = z.Provider.(*mock.DNS)
	srv := httptest.NewServer(dns.NewHandler(z, "zone-token", nil))
	t.Cleanup(srv.Close)
	e.cluster.put(ZoneKind, "example.com", map[string]any{
		"zone": "example.com", "provider": "lab", "endpoint": srv.URL, "tokenVariable": "providers/lab",
	}, map[string]any{"heartbeat": e.now.Format(time.RFC3339)})
	e.cluster.vars["providers/lab"] = map[string]string{"token": "zone-token"}

	e.op = e.newOperator()
	return e
}

// newOperator builds an operator on the fakes of the environment: a second
// one is the operator after a restart.
func (e *env) newOperator() *Operator {
	return &Operator{
		Name: DefaultController, Version: "test",
		cluster: e.cluster, secrets: e.secrets, connect: e.ca.connect,
		zones: &zones{
			cluster: e.cluster, ttl: 3 * time.Minute, tokenNamespace: "default", allowHTTP: true,
			client: &http.Client{Timeout: 5 * time.Second}, now: e.clock, cacheFor: 0,
		},
		propagation: e.prop, responder: newResponder(),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)), now: e.clock,
		secretPrefix: "norca-acme/", heartbeatEvery: 30 * time.Second,
		retryBase: time.Minute, retryMax: 10 * time.Minute, maxAttempts: 3,
		propagationTimeout: 2 * time.Minute,
		accounts:           map[string]*account{}, tokens: map[string][]string{}, waiting: map[string]time.Time{},
	}
}

func (e *env) clock() time.Time { return e.now }

// advance moves the clock, and the heartbeat of the zone with it unless
// its provider was stopped.
func (e *env) advance(d time.Duration) {
	e.now = e.now.Add(d)
	if e.zoneStopped {
		return
	}
	e.cluster.mu.Lock()
	defer e.cluster.mu.Unlock()
	if z := e.cluster.objects[ZoneKind.Kind+"/example.com"]; z != nil {
		z.Status = map[string]any{"heartbeat": e.now.Format(time.RFC3339)}
	}
}

// issuer writes an issuer of the operator; config is merged into the
// default configuration.
func (e *env) issuer(name string, config map[string]any) {
	cfg := map[string]any{"directory": testDirectory, "roots": e.ca.rootPEM()}
	for k, v := range config {
		cfg[k] = v
	}
	e.cluster.put(IssuerKind, name, map[string]any{
		"external": map[string]any{"controller": DefaultController, "config": cfg},
	}, nil)
}

// order writes the order of a certificate for the names and returns its
// name.
func (e *env) order(name, issuer string, names ...string) string {
	e.t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{DNSNames: names}, key)
	if err != nil {
		e.t.Fatal(err)
	}
	e.cluster.put(OrderKind, name, map[string]any{
		"issuer": issuer, "certificate": "default/" + name,
		"csr":       string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})),
		"dns_names": names,
	}, map[string]any{"state": StatePending})
	return name
}

func (e *env) pass() {
	e.t.Helper()
	if err := e.op.Pass(context.Background()); err != nil {
		e.t.Fatalf("pass: %v", err)
	}
}

// passes runs passes until the order is valid or failed, at most n.
func (e *env) passes(order string, n int) OrderStatus {
	e.t.Helper()
	var st OrderStatus
	for range n {
		e.pass()
		st = OrderStatus{}
		e.cluster.status(OrderKind, order, &st)
		if st.State == StateValid || st.State == StateFailed {
			break
		}
	}
	return st
}

func (e *env) orderStatus(order string) OrderStatus {
	var st OrderStatus
	e.cluster.status(OrderKind, order, &st)
	return st
}

func (e *env) issuerStatus(name string) IssuerStatus {
	var st IssuerStatus
	e.cluster.status(IssuerKind, name, &st)
	return st
}
