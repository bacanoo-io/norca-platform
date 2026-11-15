// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"strings"
	"time"

	xacme "golang.org/x/crypto/acme"
)

// authority is what the operator asks of the certificate authority of one
// issuer, with the account of that issuer. The ACME client of
// golang.org/x/crypto implements it (acmeAuthority); tests use a fake.
type authority interface {
	// AuthorizeOrder places an order for the DNS names.
	AuthorizeOrder(ctx context.Context, names []string) (*xacme.Order, error)

	// GetOrder and GetAuthorization read the state of an order and of one
	// of its authorizations.
	GetOrder(ctx context.Context, url string) (*xacme.Order, error)
	GetAuthorization(ctx context.Context, url string) (*xacme.Authorization, error)

	// HTTP01Response is the body the HTTP-01 challenge with the token is
	// answered with, DNS01Record the value of its DNS-01 record.
	HTTP01Response(token string) (string, error)
	DNS01Record(token string) (string, error)

	// Accept tells the certificate authority that a challenge is ready to
	// be checked.
	Accept(ctx context.Context, chal *xacme.Challenge) error

	// Finalize has the signing request (DER) of an order that is ready
	// signed and returns the chain (DER), the leaf first.
	Finalize(ctx context.Context, finalizeURL string, csr []byte) ([][]byte, error)

	// FetchCert returns the chain of an order that is valid.
	FetchCert(ctx context.Context, certURL string) ([][]byte, error)

	// Revoke revokes a certificate (DER) the account issued.
	Revoke(ctx context.Context, cert []byte) error

	// RenewalInfo returns when the certificate authority suggests to renew
	// the certificate (RFC 9773); ok is false if it says nothing.
	RenewalInfo(ctx context.Context, leaf, issuer *x509.Certificate) (at time.Time, ok bool)
}

// accountRequest is what is needed to open or find the account of an
// issuer.
type accountRequest struct {
	Directory string
	Email     string
	Key       crypto.Signer

	// EAB is the external account binding; nil without one.
	EAB *xacme.ExternalAccountBinding
}

// connectFunc opens the account of an issuer at its certificate authority:
// it registers the key, or finds the account the key has already. It
// returns the authority and the URL of the account.
type connectFunc func(ctx context.Context, req accountRequest) (authority, string, error)

// acmeAuthority is an authority on the ACME client.
type acmeAuthority struct {
	client *xacme.Client
	http   *http.Client

	// renewalInfo is the renewalInfo URL of the directory; empty if the
	// certificate authority has none.
	renewalInfo string
}

// connectACME returns the connectFunc of the ACME client with the HTTP
// client (nil: the default one) and the user agent.
func connectACME(hc *http.Client, userAgent string) connectFunc {
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	return func(ctx context.Context, req accountRequest) (authority, string, error) {
		client := &xacme.Client{Key: req.Key, DirectoryURL: req.Directory, HTTPClient: hc, UserAgent: userAgent}
		acct := &xacme.Account{ExternalAccountBinding: req.EAB}
		if req.Email != "" {
			acct.Contact = []string{"mailto:" + req.Email}
		}
		// An issuer of the operator stands for the agreement to the terms
		// of its certificate authority: whoever writes the issuer object
		// has read them.
		got, err := client.Register(ctx, acct, xacme.AcceptTOS)
		if errors.Is(err, xacme.ErrAccountAlreadyExists) {
			got, err = client.GetReg(ctx, "")
		}
		if err != nil {
			return nil, "", fmt.Errorf("account at %s: %w", req.Directory, err)
		}
		a := &acmeAuthority{client: client, http: hc}
		a.renewalInfo = directoryRenewalInfo(ctx, hc, req.Directory)
		return a, got.URI, nil
	}
}

// directoryRenewalInfo reads the renewalInfo URL of a directory. The ACME
// client does not know the field.
func directoryRenewalInfo(ctx context.Context, hc *http.Client, directory string) string {
	var doc struct {
		RenewalInfo string `json:"renewalInfo"`
	}
	if err := getJSON(ctx, hc, directory, &doc); err != nil {
		return ""
	}
	return strings.TrimRight(doc.RenewalInfo, "/")
}

func getJSON(ctx context.Context, hc *http.Client, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s answered %d", url, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}

func (a *acmeAuthority) AuthorizeOrder(ctx context.Context, names []string) (*xacme.Order, error) {
	return a.client.AuthorizeOrder(ctx, xacme.DomainIDs(names...))
}

func (a *acmeAuthority) GetOrder(ctx context.Context, url string) (*xacme.Order, error) {
	return a.client.GetOrder(ctx, url)
}

func (a *acmeAuthority) GetAuthorization(ctx context.Context, url string) (*xacme.Authorization, error) {
	return a.client.GetAuthorization(ctx, url)
}

func (a *acmeAuthority) HTTP01Response(token string) (string, error) {
	return a.client.HTTP01ChallengeResponse(token)
}

func (a *acmeAuthority) DNS01Record(token string) (string, error) {
	return a.client.DNS01ChallengeRecord(token)
}

func (a *acmeAuthority) Accept(ctx context.Context, chal *xacme.Challenge) error {
	_, err := a.client.Accept(ctx, chal)
	return err
}

func (a *acmeAuthority) Finalize(ctx context.Context, finalizeURL string, csr []byte) ([][]byte, error) {
	der, _, err := a.client.CreateOrderCert(ctx, finalizeURL, csr, true)
	return der, err
}

func (a *acmeAuthority) FetchCert(ctx context.Context, certURL string) ([][]byte, error) {
	return a.client.FetchCert(ctx, certURL, true)
}

func (a *acmeAuthority) Revoke(ctx context.Context, cert []byte) error {
	return a.client.RevokeCert(ctx, nil, cert, xacme.CRLReasonUnspecified)
}

// RenewalInfo asks the renewalInfo resource of the certificate authority
// for the window in which the certificate should be renewed, and picks a
// time in it: the certificate authority spreads its load by the window.
func (a *acmeAuthority) RenewalInfo(ctx context.Context, leaf, _ *x509.Certificate) (time.Time, bool) {
	if a.renewalInfo == "" {
		return time.Time{}, false
	}
	id, ok := renewalInfoID(leaf)
	if !ok {
		return time.Time{}, false
	}
	var doc struct {
		SuggestedWindow struct {
			Start time.Time `json:"start"`
			End   time.Time `json:"end"`
		} `json:"suggestedWindow"`
	}
	if err := getJSON(ctx, a.http, a.renewalInfo+"/"+id, &doc); err != nil {
		return time.Time{}, false
	}
	return timeInWindow(doc.SuggestedWindow.Start, doc.SuggestedWindow.End, mrand.Float64())
}

// renewalInfoID is the identifier of a certificate for the renewalInfo
// resource (RFC 9773, section 4.1): the key identifier of the authority
// that signed it and its serial number, each base64url without padding,
// joined by a dot.
func renewalInfoID(leaf *x509.Certificate) (string, bool) {
	if len(leaf.AuthorityKeyId) == 0 || leaf.SerialNumber == nil {
		return "", false
	}
	serial := leaf.SerialNumber.Bytes()
	// The serial number is written as the content of its DER INTEGER: a
	// leading zero keeps a number with the high bit set positive.
	if len(serial) == 0 || serial[0]&0x80 != 0 {
		serial = append([]byte{0}, serial...)
	}
	enc := base64.RawURLEncoding
	return enc.EncodeToString(leaf.AuthorityKeyId) + "." + enc.EncodeToString(serial), true
}

// timeInWindow returns the time at the share (0 to 1) of the window; ok is
// false for a window that is not one.
func timeInWindow(start, end time.Time, share float64) (time.Time, bool) {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return time.Time{}, false
	}
	return start.Add(time.Duration(share * float64(end.Sub(start)))).UTC().Truncate(time.Second), true
}

// newAccountKey makes the key of an account: ECDSA P-256, which every ACME
// certificate authority accepts.
func newAccountKey() (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
}

func encodeKey(key *ecdsa.PrivateKey) (string, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), nil
}

func decodeKey(text string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(text))
	if block == nil {
		return nil, errors.New("the account key is not PEM")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("the account key: %w", err)
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("the account key is not an ECDSA key")
	}
	return ec, nil
}

// chainPEM writes a chain of DER certificates as PEM, the leaf first.
func chainPEM(der [][]byte) string {
	var b strings.Builder
	for _, cert := range der {
		_ = pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: cert})
	}
	return b.String()
}

// parsePEM reads the certificates of a PEM text.
func parsePEM(text string) ([]*x509.Certificate, error) {
	var out []*x509.Certificate
	rest := []byte(text)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return out, nil
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		out = append(out, cert)
	}
}

// problemOf writes an error of the certificate authority for the status of
// an order, and says whether the same request would fail again.
func problemOf(err error) (text string, permanent bool) {
	var acmeErr *xacme.Error
	if errors.As(err, &acmeErr) {
		text = acmeErr.Detail
		if text == "" {
			text = acmeErr.ProblemType
		}
		if acmeErr.ProblemType != "" && acmeErr.Detail != "" {
			text = strings.TrimPrefix(acmeErr.ProblemType, "urn:ietf:params:acme:error:") + ": " + acmeErr.Detail
		}
		// A request the certificate authority refuses is refused again;
		// its own failures and its rate limits pass.
		rateLimited := acmeErr.StatusCode == http.StatusTooManyRequests ||
			strings.HasSuffix(acmeErr.ProblemType, ":rateLimited")
		badNonce := strings.HasSuffix(acmeErr.ProblemType, ":badNonce")
		permanent = acmeErr.StatusCode >= 400 && acmeErr.StatusCode < 500 && !rateLimited && !badNonce
		return text, permanent
	}
	var orderErr *xacme.OrderError
	if errors.As(err, &orderErr) {
		return fmt.Sprintf("the order of the certificate authority is %s", orderErr.Status), true
	}
	var authzErr *xacme.AuthorizationError
	if errors.As(err, &authzErr) {
		return authzErr.Error(), true
	}
	return err.Error(), false
}
