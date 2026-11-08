// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/tpmkey"
)

// State is what norca-join keeps between its runs: the endpoints the
// operator last named, and when the certificate was issued and ends.
type State struct {
	URLs     []string  `json:"urls,omitempty"`
	Issued   time.Time `json:"issued"`
	NotAfter time.Time `json:"not_after"`
}

// Client is norca-join on a node.
type Client struct {
	Config *Config
	Log    *slog.Logger
	Now    func() time.Time
	// Timeout of one request (default 30s).
	Timeout time.Duration

	// OpenTPM opens the TPM of the key store tpm (default the device
	// /dev/tpmrm0, on Linux only); TPMPresent tells the key store auto
	// whether there is one (default: /dev/tpmrm0 is a device). Tests
	// replace them.
	OpenTPM    tpmkey.Opener
	TPMPresent func() bool
}

func (c *Client) openTPM() tpmkey.Opener {
	if c.OpenTPM != nil {
		return c.OpenTPM
	}
	return tpmkey.DeviceOpener("")
}

// keyStore is where the next key is made, KeyStoreFile or KeyStoreTPM: the
// key store of the configuration on this machine.
func (c *Client) keyStore() (string, error) {
	supported := tpmkey.Supported || c.OpenTPM != nil
	switch c.Config.KeyStore {
	case "", KeyStoreFile:
		return KeyStoreFile, nil
	case KeyStoreTPM:
		if !supported {
			return "", fmt.Errorf("key_store tpm: %w", tpmkey.ErrUnsupported)
		}
		return KeyStoreTPM, nil
	case KeyStoreAuto:
		present := c.TPMPresent
		if present == nil {
			present = func() bool { return tpmkey.Present("") }
		}
		if supported && present() {
			return KeyStoreTPM, nil
		}
		return KeyStoreFile, nil
	default:
		return "", fmt.Errorf("unknown key_store %q", c.Config.KeyStore)
	}
}

func (c *Client) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Client) log() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.Default()
}

// ErrNoToken is a node without a certificate and without a token: it cannot
// join.
var ErrNoToken = errors.New("no certificate and no join token")

// Current returns the certificate of the node if it is there and still
// valid. A key of the TPM (a TSS2 key file) opens the TPM at its first
// signature.
func (c *Client) Current() (*tls.Certificate, *x509.Certificate, error) {
	certPEM, err := os.ReadFile(c.Config.CertFile)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := os.ReadFile(c.Config.KeyFile)
	if err != nil {
		return nil, nil, err
	}
	var pair tls.Certificate
	if tpmkey.IsPEM(keyPEM) {
		pair, err = tpmkey.X509KeyPair(certPEM, keyPEM, c.openTPM())
	} else {
		pair, err = tls.X509KeyPair(certPEM, keyPEM)
	}
	if err != nil {
		return nil, nil, err
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, nil, err
	}
	if !c.now().Before(leaf.NotAfter) {
		return nil, nil, fmt.Errorf("the certificate expired at %s", leaf.NotAfter.Format(time.RFC3339))
	}
	return &pair, leaf, nil
}

// RenewAt is when the certificate is renewed: at two thirds of its life,
// counted from when it was issued (the state), or from its NotBefore.
func (c *Client) RenewAt(leaf *x509.Certificate) time.Time {
	issued := leaf.NotBefore
	if st, err := c.loadState(); err == nil && st.NotAfter.Equal(leaf.NotAfter) && !st.Issued.IsZero() {
		issued = st.Issued
	}
	return issued.Add(leaf.NotAfter.Sub(issued) * 2 / 3)
}

// Join makes the key of the node, sends a certificate request with the
// one-time token, and writes the certificate and the key. The token is
// removed from the configuration file once it is used, where the file can
// be written.
func (c *Client) Join(ctx context.Context, configPath string) error {
	if c.Config.Token == "" {
		return ErrNoToken
	}
	key, csr, err := c.newKeyAndCSR()
	if err != nil {
		return err
	}
	defer key.close()
	body, _ := json.Marshal(JoinRequest{Instance: c.Config.Instance, Token: c.Config.Token, CSR: string(csr), KeyStore: key.store})
	resp, err := c.post(ctx, "/v1/join", body, nil)
	if err != nil {
		return err
	}
	if err := c.install(key, resp); err != nil {
		return err
	}
	c.forgetToken(configPath)
	c.log().Info("joined with a key of the node", "instance", c.Config.Instance, "key_store", key.store, "not_after", resp.NotAfter)
	return nil
}

// Renew sends a certificate request for a new key with the current
// certificate and writes the new pair.
func (c *Client) Renew(ctx context.Context) error {
	current, _, err := c.Current()
	if err != nil {
		return fmt.Errorf("the current certificate: %w", err)
	}
	if closer, ok := current.PrivateKey.(io.Closer); ok {
		defer closer.Close()
	}
	key, csr, err := c.newKeyAndCSR()
	if err != nil {
		return err
	}
	defer key.close()
	body, _ := json.Marshal(RenewRequest{CSR: string(csr), KeyStore: key.store})
	resp, err := c.post(ctx, "/v1/renew", body, current)
	if err != nil {
		return err
	}
	if err := c.install(key, resp); err != nil {
		return err
	}
	c.log().Info("renewed the certificate", "instance", c.Config.Instance, "key_store", key.store, "not_after", resp.NotAfter)
	return nil
}

// urls are the endpoints to try: those the operator named last, then those
// of the bootstrap.
func (c *Client) urls() []string {
	var out []string
	seen := map[string]bool{}
	if st, err := c.loadState(); err == nil {
		for _, u := range st.URLs {
			if !seen[u] {
				seen[u] = true
				out = append(out, u)
			}
		}
	}
	for _, u := range c.Config.URLs {
		if !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	return out
}

// RefusedError is an answer of the endpoint that refuses the request: no
// other endpoint would accept it.
type RefusedError struct {
	Status  int
	Message string
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("refused by the join endpoint (%d): %s", e.Status, e.Message)
}

// post sends the request to the endpoints in turn until one answers.
func (c *Client) post(ctx context.Context, path string, body []byte, cert *tls.Certificate) (*Response, error) {
	caPEM, err := os.ReadFile(c.Config.CAFile)
	if err != nil {
		return nil, fmt.Errorf("the cluster CA: %w", err)
	}
	tlsConf, err := EndpointTLS(caPEM, c.Config.Endpoint, cert)
	if err != nil {
		return nil, err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	hc := &http.Client{Timeout: timeout, Transport: &http.Transport{TLSClientConfig: tlsConf}}
	defer hc.CloseIdleConnections()

	var errs []error
	for _, base := range c.urls() {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := hc.Do(req)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		data, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			var e ErrorResponse
			_ = json.Unmarshal(data, &e)
			if res.StatusCode >= 400 && res.StatusCode < 500 {
				return nil, &RefusedError{Status: res.StatusCode, Message: e.Error}
			}
			errs = append(errs, fmt.Errorf("%s: %d %s", base, res.StatusCode, e.Error))
			continue
		}
		var out Response
		if err := json.Unmarshal(data, &out); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", base, err))
			continue
		}
		return &out, nil
	}
	return nil, fmt.Errorf("no join endpoint answered: %w", errors.Join(errs...))
}

// EndpointTLS is the TLS of a node toward the join endpoint: the chain of
// the endpoint must verify against the cluster CA, and its certificate must
// carry the URI endpoint (a node certificate never does). cert is the client
// certificate of a renewal.
func EndpointTLS(caPEM []byte, endpoint string, cert *tls.Certificate) (*tls.Config, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("the cluster CA is not PEM")
	}
	conf := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// The name is checked below: the endpoint has a URI, not a host
		// name, so the standard verification is replaced, not skipped.
		InsecureSkipVerify: true, //nolint:gosec // G402: VerifyConnection verifies the chain and the URI

		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("the join endpoint sent no certificate")
			}
			ints := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				ints.AddCert(c)
			}
			leaf := cs.PeerCertificates[0]
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: ints,
				KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
				return fmt.Errorf("the certificate of the join endpoint: %w", err)
			}
			for _, u := range leaf.URIs {
				if u.String() == endpoint {
					return nil
				}
			}
			return fmt.Errorf("the certificate of the join endpoint is not for %s", endpoint)
		},
	}
	if cert != nil {
		conf.Certificates = []tls.Certificate{*cert}
	}
	return conf, nil
}

// install writes the key and the certificate (each by a rename, so the
// agent never reads half a file), the cluster CA if it changed, and the
// state.
func (c *Client) install(key *nodeKey, resp *Response) error {
	block, _ := pem.Decode([]byte(resp.Certificate))
	if block == nil {
		return errors.New("the answer has no certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	if pub, ok := leaf.PublicKey.(*ecdsa.PublicKey); !ok || !pub.Equal(key.signer.Public()) {
		return errors.New("the certificate is not for the key of the request")
	}
	// The key first: until the certificate follows, the agent keeps the
	// pair it loaded, and it loads again only on SIGHUP.
	if err := writeAtomic(c.Config.KeyFile, key.pem, 0o600); err != nil {
		return err
	}
	if err := writeAtomic(c.Config.CertFile, []byte(resp.Certificate), 0o644); err != nil {
		return err
	}
	if resp.CA != "" {
		if old, err := os.ReadFile(c.Config.CAFile); err != nil || !bytes.Equal(old, []byte(resp.CA)) {
			if err := writeAtomic(c.Config.CAFile, []byte(resp.CA), 0o644); err != nil {
				return err
			}
		}
	}
	st := State{URLs: resp.URLs, Issued: c.now(), NotAfter: leaf.NotAfter}
	if len(st.URLs) == 0 {
		if old, err := c.loadState(); err == nil {
			st.URLs = old.URLs
		}
	}
	data, _ := json.MarshalIndent(st, "", "  ")
	return writeAtomic(c.Config.StateFile, data, 0o600)
}

func (c *Client) loadState() (*State, error) {
	data, err := os.ReadFile(c.Config.StateFile)
	if err != nil {
		return nil, err
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// forgetToken removes the used token from the configuration file. A file
// that can not be written (a read-only mount) keeps it: the token is void
// once used.
func (c *Client) forgetToken(path string) {
	c.Config.Token = ""
	if path == "" {
		return
	}
	data, err := json.MarshalIndent(c.Config, "", "  ")
	if err != nil {
		return
	}
	if err := writeAtomic(path, data, 0o600); err != nil {
		c.log().Debug("the used token stays in the configuration file", "error", err)
	}
}

// nodeKey is a new key of the node: its signer, its key file and where it
// is kept.
type nodeKey struct {
	signer crypto.Signer
	pem    []byte
	store  string
}

// close closes the TPM of a key of the TPM.
func (k *nodeKey) close() {
	if closer, ok := k.signer.(io.Closer); ok {
		_ = closer.Close()
	}
}

// newKey makes a new ECDSA P-256 key in the key store: in the TPM (a TSS2
// key file) or in memory (an EC PRIVATE KEY file). A TPM that the key store
// auto found but that can not make the key leaves it to a file.
func (c *Client) newKey() (*nodeKey, error) {
	store, err := c.keyStore()
	if err != nil {
		return nil, err
	}
	if store == KeyStoreTPM {
		signer, keyPEM, err := tpmkey.Generate(c.openTPM())
		if err == nil {
			return &nodeKey{signer: signer, pem: keyPEM, store: KeyStoreTPM}, nil
		}
		if c.Config.KeyStore == KeyStoreTPM {
			return nil, fmt.Errorf("the key in the TPM: %w", err)
		}
		c.log().Warn("the TPM can not make the key: it is kept in a file", "error", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return &nodeKey{signer: key, pem: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), store: KeyStoreFile}, nil
}

// newKeyAndCSR makes a new key and its certificate request, signed with it.
func (c *Client) newKeyAndCSR() (*nodeKey, []byte, error) {
	key, err := c.newKey()
	if err != nil {
		return nil, nil, err
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: c.Config.Instance},
	}, key.signer)
	if err != nil {
		key.close()
		return nil, nil, err
	}
	return key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}), nil
}

// writeAtomic writes a file through a temporary file in its directory and a
// rename.
func writeAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Chmod(perm); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
