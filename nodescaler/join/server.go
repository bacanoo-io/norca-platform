// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	pki "github.com/bacanoo-io/norca-pki"
	"github.com/bacanoo-io/norca/norca/api"
)

// Authority is what the endpoint signs with: the cluster CA (the roots of
// the node certificates) and the node CA of the region.
type Authority struct {
	CAPEM       []byte
	NodeCA      *pki.CertificateAuthority
	TrustDomain *url.URL
}

// AuthoritySource gives the authority; it is read again when the node CA
// changes.
type AuthoritySource interface {
	Authority(ctx context.Context) (*Authority, error)
}

// DefaultValidity is the validity of the certificate of a node that renews
// itself; it renews at two thirds.
const DefaultValidity = 7 * 24 * time.Hour

// Server is the join endpoint of the operator: POST /v1/join (a one-time
// token and a certificate request), POST /v1/renew (a certificate request
// with the current certificate as the client certificate) and GET /v1/health
// (for the service check of the operator's job). Every instance of the
// operator serves it: the records are shared (Store).
type Server struct {
	Store     *Store
	Authority AuthoritySource
	Region    string
	// Validity of a node certificate (DefaultValidity if zero).
	Validity time.Duration
	// URLs gives the join endpoints of now, returned to the nodes for
	// their renewals; nil or an error returns none.
	URLs func(ctx context.Context) ([]string, error)
	// Event records an event in the log of a node group; nil records none.
	Event func(ctx context.Context, group, reason, message string)
	// CheckAddress binds a join and a renewal to the addresses the provider
	// reports for the instance (its record, Store.Seen): a request from
	// another source address is refused. A record without addresses is not
	// checked, but a first join waits until the operator has recorded what
	// the provider reports. Off where a proxy, a load balancer or NAT in
	// front of the endpoint changes the source address.
	CheckAddress bool
	Log          *slog.Logger
	Now          func() time.Time

	mu       sync.Mutex
	auth     *Authority
	authRead time.Time
	cert     *tls.Certificate
	certCA   string
	certEnd  time.Time
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func (s *Server) validity() time.Duration {
	if s.Validity > 0 {
		return s.Validity
	}
	return DefaultValidity
}

// authority returns the authority, read at most every 30 seconds.
func (s *Server) authority(ctx context.Context) (*Authority, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.auth != nil && s.now().Sub(s.authRead) < 30*time.Second {
		return s.auth, nil
	}
	a, err := s.Authority.Authority(ctx)
	if err != nil {
		if s.auth != nil {
			s.log().Warn("reading the node CA failed, the last one is used", "error", err)
			return s.auth, nil
		}
		return nil, err
	}
	s.auth, s.authRead = a, s.now()
	return a, nil
}

// TLSConfig is the TLS of the endpoint. A client certificate is asked for
// but not required (a joining node has none); /v1/renew verifies it.
func (s *Server) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		ClientAuth:     tls.RequestClientCert,
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) { return s.certificate(hello.Context()) },
	}
}

// certificate is the certificate of the endpoint, issued again by the node
// CA at two thirds of its life (one day) or when the node CA changed.
func (s *Server) certificate(ctx context.Context) (*tls.Certificate, error) {
	a, err := s.authority(ctx)
	if err != nil {
		return nil, err
	}
	caSerial := a.NodeCA.Certificate.SerialNumber.String()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if s.cert != nil && s.certCA == caSerial && now.Before(s.certEnd) {
		return s.cert, nil
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	id := pki.Identity{CN: "client." + s.Region + ".norca", SPIFFE: EndpointURI(a.TrustDomain, s.Region)}
	const life = 24 * time.Hour
	b, err := a.NodeCA.IssueForKey(id, &key.PublicKey, life)
	if err != nil {
		return nil, fmt.Errorf("issuing the certificate of the join endpoint: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	pair, err := tls.X509KeyPair(append(b.CertPEM, a.NodeCA.CertPEM...),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		return nil, err
	}
	s.cert, s.certCA, s.certEnd = &pair, caSerial, now.Add(life*2/3)
	return s.cert, nil
}

// Handler serves the endpoint.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/join", s.handleJoin)
	mux.HandleFunc("POST /v1/renew", s.handleRenew)
	mux.HandleFunc("GET /v1/health", s.handleHealth)
	return mux
}

// HealthResponse is the answer of GET /v1/health.
type HealthResponse struct {
	Status string `json:"status"`          // "ok" or "unavailable"
	Error  string `json:"error,omitempty"` // why it is unavailable
}

// handleHealth answers 200 while the endpoint can serve a join: the node CA
// is readable and the certificate of the endpoint is issued; else 503. It
// needs no client certificate and reads no record.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if _, err := s.certificate(r.Context()); err != nil {
		s.log().Warn("the join endpoint is unavailable", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, HealthResponse{Status: "unavailable", Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, HealthResponse{Status: "ok"})
}

const maxBody = 64 << 10

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req JoinRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil {
		s.fail(w, http.StatusBadRequest, "join", "", fmt.Errorf("the request: %w", err))
		return
	}
	ctx := r.Context()
	// The token first: a request without a valid one gets no further.
	rec, group, err := s.Store.Redeem(ctx, req.Instance, req.Token)
	if err != nil {
		s.fail(w, statusOf(err), "join", req.Instance, err)
		return
	}
	if status, err := s.checkAddress(ctx, r, rec, group, req.Instance, "join"); err != nil {
		s.fail(w, status, "join", req.Instance, err)
		return
	}
	pub, err := pki.ParseCSR([]byte(req.CSR))
	if err != nil {
		s.fail(w, http.StatusBadRequest, "join", req.Instance, err)
		return
	}
	resp, serial, err := s.issue(ctx, req.Instance, pub)
	if err != nil {
		s.fail(w, http.StatusInternalServerError, "join", req.Instance, err)
		return
	}
	if err := s.Store.Joined(ctx, rec, serial); err != nil {
		s.fail(w, statusOf(err), "join", req.Instance, err)
		return
	}
	s.log().Info("a node joined with its own key", "instance", req.Instance, "group", group, "serial", serial,
		"key_store", reportedKeyStore(req.KeyStore))
	s.event(ctx, group, "CertificateIssued", fmt.Sprintf("%s joined with its own key%s: certificate %s, valid until %s",
		req.Instance, keyStoreNote(req.KeyStore), serial, resp.NotAfter.UTC().Format(time.RFC3339)))
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		s.fail(w, http.StatusUnauthorized, "renew", "", errors.New("a renewal needs the current certificate of the node"))
		return
	}
	a, err := s.authority(ctx)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, "renew", "", err)
		return
	}
	leaf, err := verifyNode(a, r.TLS.PeerCertificates, s.now())
	if err != nil {
		s.fail(w, http.StatusUnauthorized, "renew", "", err)
		return
	}
	instance, err := InstanceOf(leaf, s.Region)
	if err != nil {
		s.fail(w, http.StatusForbidden, "renew", "", err)
		return
	}
	var req RenewRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil {
		s.fail(w, http.StatusBadRequest, "renew", instance, fmt.Errorf("the request: %w", err))
		return
	}
	rec, group, err := s.Store.Renewing(ctx, instance, leaf.SerialNumber.Text(16))
	if err != nil {
		if errors.Is(err, ErrRefused) {
			// The group of a refused renewal is in its record, if any: a
			// refusal there may be a stolen certificate.
			if v, rerr := s.Store.read(ctx, instance); rerr == nil {
				s.event(ctx, v.Items[itemGroup], "RenewalRefused", fmt.Sprintf("%s: %v", instance, err))
			}
		}
		s.fail(w, statusOf(err), "renew", instance, err)
		return
	}
	if status, err := s.checkAddress(ctx, r, rec, group, instance, "renewal"); err != nil {
		s.fail(w, status, "renew", instance, err)
		return
	}
	pub, err := pki.ParseCSR([]byte(req.CSR))
	if err != nil {
		s.fail(w, http.StatusBadRequest, "renew", instance, err)
		return
	}
	// A renewal rotates the key: a request for the key of the current
	// certificate would keep a key that may have leaked valid for ever.
	if k, ok := leaf.PublicKey.(interface{ Equal(crypto.PublicKey) bool }); ok && k.Equal(pub) {
		err := fmt.Errorf("%w: the request of %s is for the key of its current certificate; a renewal needs a new key", ErrRefused, instance)
		s.event(ctx, group, "RenewalRefused", err.Error())
		s.fail(w, http.StatusForbidden, "renew", instance, err)
		return
	}
	resp, serial, err := s.issue(ctx, instance, pub)
	if err != nil {
		s.fail(w, http.StatusInternalServerError, "renew", instance, err)
		return
	}
	if err := s.Store.Renewed(ctx, rec, serial); err != nil {
		s.fail(w, statusOf(err), "renew", instance, err)
		return
	}
	s.log().Info("a node renewed its certificate", "instance", instance, "group", group, "serial", serial,
		"key_store", reportedKeyStore(req.KeyStore))
	s.event(ctx, group, "CertificateRenewed", fmt.Sprintf("%s renewed its certificate%s: %s, valid until %s",
		instance, keyStoreNote(req.KeyStore), serial, resp.NotAfter.UTC().Format(time.RFC3339)))
	writeJSON(w, http.StatusOK, resp)
}

// reportedKeyStore is the key store a node reports, file or tpm; a node
// that reports none (an older norca-join) keeps its key in a file.
func reportedKeyStore(s string) string {
	switch s {
	case "", KeyStoreFile:
		return KeyStoreFile
	case KeyStoreTPM:
		return KeyStoreTPM
	default:
		return "unknown"
	}
}

// keyStoreNote is the note of an event on a key the node reports to keep
// in its TPM.
func keyStoreNote(s string) string {
	if s == KeyStoreTPM {
		return " (key in its TPM)"
	}
	return ""
}

// issue signs the key of an instance with the identity of a node: the names
// of a node certificate, the name of the instance and, with SPIFFE, its ID.
func (s *Server) issue(ctx context.Context, instance string, pub any) (*Response, string, error) {
	a, err := s.authority(ctx)
	if err != nil {
		return nil, "", err
	}
	id := pki.ClientIdentity(s.Region)
	id.DNS = append(id.DNS, NodeDNSName(instance, s.Region))
	if a.TrustDomain != nil {
		spiffe, err := pki.SPIFFEID(a.TrustDomain, "region", s.Region, "node", instance)
		if err != nil {
			return nil, "", err
		}
		id = id.WithSPIFFE(spiffe)
	}
	b, err := a.NodeCA.IssueForKey(id, pub, s.validity())
	if err != nil {
		return nil, "", err
	}
	block, _ := pem.Decode(b.CertPEM)
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, "", err
	}
	resp := &Response{
		Certificate: string(b.CertPEM) + string(a.NodeCA.CertPEM),
		CA:          string(a.CAPEM),
		NotAfter:    leaf.NotAfter,
	}
	if s.URLs != nil {
		if urls, err := s.URLs(ctx); err == nil {
			resp.URLs = urls
		} else {
			s.log().Warn("listing the join endpoints failed", "error", err)
		}
	}
	return resp, leaf.SerialNumber.Text(16), nil
}

// verifyNode verifies the client certificate of a renewal against the
// cluster CA (with the intermediates the node sent) for client auth.
func verifyNode(a *Authority, chain []*x509.Certificate, now time.Time) (*x509.Certificate, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(a.CAPEM) {
		return nil, errors.New("the cluster CA is not PEM")
	}
	ints := x509.NewCertPool()
	for _, c := range chain[1:] {
		ints.AddCert(c)
	}
	if _, err := chain[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: ints, CurrentTime: now,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return nil, fmt.Errorf("the certificate of the node does not verify: %w", err)
	}
	return chain[0], nil
}

// errNotSeen is a first join before the operator recorded what the provider
// reports for the instance: the node tries again (503).
var errNotSeen = errors.New("not recorded yet")

// checkAddress checks the source address of a request (the peer of the
// connection; no header is trusted) against the addresses of the record of
// the instance, while CheckAddress is on. A request from another address is
// refused (403) and in the log of the group: it may be a stolen token or
// key. A record without addresses is not checked; only a first join waits
// (503) until the operator has recorded what the provider reports.
func (s *Server) checkAddress(ctx context.Context, r *http.Request, rec *api.Variable, group, instance, what string) (int, error) {
	if !s.CheckAddress {
		return 0, nil
	}
	addrs, seen := Addresses(rec)
	if len(addrs) == 0 {
		if what == "join" && !seen {
			return http.StatusServiceUnavailable, fmt.Errorf("%w: the operator has not recorded the addresses of %s yet; try again", errNotSeen, instance)
		}
		s.log().Debug("the provider reports no address of the instance: the source of the request is not checked",
			"instance", instance, "request", what)
		return 0, nil
	}
	src := sourceAddress(r)
	if slices.Contains(addrs, src) {
		return 0, nil
	}
	err := fmt.Errorf("%w: a %s of %s from %s, which is not an address of the instance (%s)",
		ErrRefused, what, instance, src, strings.Join(addrs, ", "))
	s.event(ctx, group, "JoinAddressRefused", err.Error()+"; a stolen token or key?")
	return http.StatusForbidden, err
}

// sourceAddress is the IP address of the peer of a request, an IPv4 address
// mapped into IPv6 as IPv4.
func sourceAddress(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Unmap().WithZone("").String()
	}
	return host
}

func (s *Server) event(ctx context.Context, group, reason, message string) {
	if s.Event != nil && group != "" {
		s.Event(ctx, group, reason, message)
	}
}

func (s *Server) fail(w http.ResponseWriter, status int, what, instance string, err error) {
	s.log().Warn("a "+what+" was refused", "instance", instance, "status", status, "error", err)
	msg := err.Error()
	if status >= 500 && !errors.Is(err, errNotSeen) {
		msg = "internal error"
	}
	writeJSON(w, status, ErrorResponse{Error: msg})
}

func statusOf(err error) int {
	if errors.Is(err, ErrRefused) {
		return http.StatusForbidden
	}
	return http.StatusInternalServerError
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
