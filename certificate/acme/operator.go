// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	xacme "golang.org/x/crypto/acme"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

type kindRef = controller.Kind

// The errors of a status write the operator goes on after.
var (
	errConflict = errors.New("the object changed")
	errGone     = errors.New("the object is gone")
)

// secretStore keeps the secrets of the operator: the keys of its accounts
// and the certificates it issued, which a revocation needs.
type secretStore interface {
	// Read returns the items of a secret; nil if it does not exist.
	Read(ctx context.Context, path string) (map[string]string, error)

	// Write writes a new version of a secret. With expiresAt the secret
	// expires then.
	Write(ctx context.Context, path string, items map[string]string, expiresAt *time.Time) error
}

// The paths of the secrets of the operator, below its prefix.
const (
	secretAccounts = "accounts/"
	secretIssued   = "issued/"
)

// Operator is the controller of the ACME issuers.
type Operator struct {
	// Name is the controller name issuers name the operator by.
	Name    string
	Version string

	cluster cluster
	secrets secretStore
	connect connectFunc
	zones   *zones

	// propagation checks that a DNS record can be seen before the
	// certificate authority is told to look; nil does not check.
	propagation propagation

	responder *responder

	// systemRoots verifies the chains of issuers without configured roots;
	// nil is the pool of the system.
	systemRoots *x509.CertPool

	logger *slog.Logger
	now    func() time.Time

	// secretPrefix is the path prefix of the secrets of the operator.
	secretPrefix string

	// heartbeatEvery is how often the status of an issuer is written when
	// nothing else changed.
	heartbeatEvery time.Duration

	// An order is tried again after retryBase, doubled with every failed
	// attempt up to retryMax, and given up after maxAttempts.
	retryBase   time.Duration
	retryMax    time.Duration
	maxAttempts int

	// propagationTimeout is how long a DNS record may take to be seen.
	propagationTimeout time.Duration

	mu       sync.Mutex
	accounts map[string]*account
	// tokens are the HTTP-01 tokens answered for an order.
	tokens map[string][]string
	// waiting is since when an order waits for its DNS records.
	waiting map[string]time.Time
}

// account is the account of an issuer at its certificate authority.
type account struct {
	directory string
	email     string
	eab       string
	authority authority
	url       string

	// learned are the roots the chains of the issuer ended in (PEM).
	learned []string
}

func (o *Operator) log() *slog.Logger {
	if o.logger != nil {
		return o.logger
	}
	return slog.Default()
}

// issuer is an issuer of the operator in one pass.
type issuer struct {
	obj     *api.CustomObject
	config  IssuerConfig
	status  IssuerStatus
	account *account
}

// Pass does the work of the operator once: it keeps the accounts and the
// status of its issuers, and moves every open order of them a step. It
// returns an error only when the objects cannot be listed.
func (o *Operator) Pass(ctx context.Context) error {
	issuerObjs, err := o.cluster.ListObjects(ctx, IssuerKind)
	if err != nil {
		return fmt.Errorf("listing the issuers: %w", err)
	}
	issuers := map[string]*issuer{}
	for _, obj := range issuerObjs {
		if obj.DeletionTime != 0 {
			continue
		}
		cfg, mine, cfgErr := issuerOf(obj, o.Name)
		if !mine {
			continue
		}
		is := &issuer{obj: obj, config: cfg}
		_ = decode(obj.Status, &is.status)
		if cfgErr == nil {
			is.account, cfgErr = o.account(ctx, obj.Name, cfg)
		}
		if cfgErr != nil {
			o.log().Warn("the issuer is not available", "issuer", obj.Name, "error", cfgErr)
			o.writeIssuerStatus(ctx, is, cfgErr.Error(), false)
			continue
		}
		o.writeIssuerStatus(ctx, is, "", false)
		issuers[obj.Name] = is
	}

	orders, err := o.cluster.ListObjects(ctx, OrderKind)
	if err != nil {
		return fmt.Errorf("listing the orders: %w", err)
	}
	open := map[string]bool{}
	for _, obj := range orders {
		var spec OrderSpec
		if err := decode(obj.Spec, &spec); err != nil {
			continue
		}
		is := issuers[spec.Issuer]
		if is == nil || obj.DeletionTime != 0 {
			continue
		}
		open[obj.Name] = true
		o.order(ctx, is, obj, &spec)
	}
	o.forget(open)
	return nil
}

// forget drops what the operator holds in memory for orders that are gone.
func (o *Operator) forget(open map[string]bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for name, tokens := range o.tokens {
		if open[name] {
			continue
		}
		for _, token := range tokens {
			o.responder.cleanUp(token)
		}
		delete(o.tokens, name)
	}
	for name := range o.waiting {
		if !open[name] {
			delete(o.waiting, name)
		}
	}
}

// account returns the account of an issuer: the one of this process, or the
// one of the key in the secrets of the operator, or a new one.
func (o *Operator) account(ctx context.Context, name string, cfg IssuerConfig) (*account, error) {
	o.mu.Lock()
	cached := o.accounts[name]
	o.mu.Unlock()
	if cached != nil && cached.directory == cfg.Directory && cached.email == cfg.Email && cached.eab == cfg.EABSecret {
		return cached, nil
	}

	path := o.secretPrefix + secretAccounts + name
	items, err := o.secrets.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("reading the account secret %s: %w", path, err)
	}
	req := accountRequest{Directory: cfg.Directory, Email: cfg.Email}
	fresh := false
	if items["key"] != "" && items["directory"] == cfg.Directory {
		if req.Key, err = decodeKey(items["key"]); err != nil {
			return nil, fmt.Errorf("the account secret %s: %w", path, err)
		}
	} else {
		// No account yet, or the issuer was moved to another certificate
		// authority: a key of its own for the new account.
		key, err := newAccountKey()
		if err != nil {
			return nil, err
		}
		req.Key, fresh = key, true
		encoded, err := encodeKey(key)
		if err != nil {
			return nil, err
		}
		// The key is stored before the account is opened: a key that was
		// registered and then lost could not be used again.
		if err := o.secrets.Write(ctx, path, map[string]string{"key": encoded, "directory": cfg.Directory}, nil); err != nil {
			return nil, fmt.Errorf("writing the account secret %s: %w", path, err)
		}
	}
	if cfg.EABSecret != "" {
		eab, err := o.secrets.Read(ctx, cfg.EABSecret)
		if err != nil {
			return nil, fmt.Errorf("reading the external account binding %s: %w", cfg.EABSecret, err)
		}
		hmacKey, decErr := base64.RawURLEncoding.DecodeString(strings.TrimRight(strings.TrimSpace(eab["hmac_key"]), "="))
		if eab["kid"] == "" || decErr != nil || len(hmacKey) == 0 {
			return nil, fmt.Errorf("the external account binding %s needs the items kid and hmac_key (base64url)", cfg.EABSecret)
		}
		req.EAB = &xacme.ExternalAccountBinding{KID: strings.TrimSpace(eab["kid"]), Key: hmacKey}
	}
	auth, url, err := o.connect(ctx, req)
	if err != nil {
		return nil, err
	}
	if fresh {
		o.log().Info("account opened", "issuer", name, "directory", cfg.Directory, "account", url)
	}
	acct := &account{directory: cfg.Directory, email: cfg.Email, eab: cfg.EABSecret, authority: auth, url: url}
	o.mu.Lock()
	o.accounts[name] = acct
	o.mu.Unlock()
	return acct, nil
}

// roots returns the roots of an issuer for its status: the configured
// ones, those the status has already and those its chains ended in.
func (is *issuer) roots() string {
	var out []string
	seen := map[string]bool{}
	add := func(text string) {
		certs, err := parsePEM(text)
		if err != nil {
			return
		}
		for _, cert := range certs {
			block := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
			if !seen[block] {
				seen[block] = true
				out = append(out, block)
			}
		}
	}
	add(is.config.Roots)
	if is.status.Directory == is.config.Directory {
		// The roots learned from an earlier certificate authority of the
		// issuer are not carried over.
		add(is.status.Roots)
	}
	if is.account != nil {
		for _, root := range is.account.learned {
			add(root)
		}
	}
	return strings.Join(out, "")
}

// writeIssuerStatus writes the status of an issuer: with a problem it has
// no heartbeat, so the servers order nothing from it. Without force it is
// written when something changed or the heartbeat is due. It reports
// whether the status the servers see is the one wanted.
func (o *Operator) writeIssuerStatus(ctx context.Context, is *issuer, problem string, force bool) bool {
	now := o.now().UTC()
	want := IssuerStatus{
		Roots:      is.roots(),
		Directory:  is.config.Directory,
		Problem:    problem,
		Controller: o.Name,
		Version:    o.Version,
	}
	if is.account != nil {
		want.Account = is.account.url
	}
	if problem == "" {
		want.Heartbeat = now.Format(time.RFC3339)
	}
	same := want
	same.Heartbeat = is.status.Heartbeat
	if !force && same == is.status {
		last, err := time.Parse(time.RFC3339, is.status.Heartbeat)
		if problem != "" || (err == nil && now.Sub(last) < o.heartbeatEvery) {
			return true
		}
	}
	status, err := encode(want)
	if err == nil {
		err = o.cluster.UpdateStatus(ctx, IssuerKind, is.obj.Name, status, 0)
	}
	if err != nil {
		o.log().Warn("writing the status of the issuer failed", "issuer", is.obj.Name, "error", err)
		return false
	}
	is.status = want
	return true
}

// errWait says that an order needs another pass and nothing is wrong.
var errWait = errors.New("not done yet")

// permanentError is a failure that the same order meets again.
type permanentError struct{ msg string }

func (e *permanentError) Error() string { return e.msg }

func permanent(format string, args ...any) error {
	return &permanentError{msg: fmt.Sprintf(format, args...)}
}

// order moves an order one step and writes its status.
func (o *Operator) order(ctx context.Context, is *issuer, obj *api.CustomObject, spec *OrderSpec) {
	var st OrderStatus
	_ = decode(obj.Status, &st)
	if st.State == StateValid || st.State == StateFailed {
		return
	}
	if st.Progress == nil {
		st.Progress = &Progress{}
	}
	if o.now().Before(st.Progress.retryAt()) {
		return
	}
	before, _ := encode(st)

	var err error
	if spec.Revoke != "" && spec.CSR == "" {
		err = o.revoke(ctx, is, spec)
	} else {
		err = o.issue(ctx, is, obj.Name, spec, &st)
	}

	log := o.log().With("order", obj.Name, "issuer", is.obj.Name, "certificate", spec.Certificate)
	var perm *permanentError
	switch {
	case err == nil:
		o.cleanUp(ctx, obj.Name, st.Progress)
		st.State, st.Problem, st.Progress = StateValid, "", nil
		log.Info("order fulfilled")
	case errors.Is(err, errWait):
		st.State = StateProcessing
	case errors.As(err, &perm):
		o.cleanUp(ctx, obj.Name, st.Progress)
		st.State, st.Problem, st.Chain, st.Progress = StateFailed, perm.msg, "", nil
		log.Warn("order failed", "problem", perm.msg)
	default:
		pr := st.Progress
		pr.Attempts++
		pr.LastError = err.Error()
		if pr.Attempts >= o.maxAttempts {
			o.cleanUp(ctx, obj.Name, pr)
			st.State, st.Chain, st.Progress = StateFailed, "", nil
			st.Problem = fmt.Sprintf("given up after %d attempts: %v", pr.Attempts, err)
			log.Warn("order failed", "problem", st.Problem)
			break
		}
		delay := o.retryBase << (pr.Attempts - 1)
		if delay > o.retryMax || delay <= 0 {
			delay = o.retryMax
		}
		pr.RetryAt = o.now().Add(delay).UTC().Format(time.RFC3339)
		st.State = StateProcessing
		log.Warn("order attempt failed, trying again", "attempt", pr.Attempts, "retry_in", delay.String(), "error", err)
	}

	after, encErr := encode(st)
	if encErr != nil || fmt.Sprint(before) == fmt.Sprint(after) {
		return
	}
	switch err := o.cluster.UpdateStatus(ctx, OrderKind, obj.Name, after, obj.ModifyIndex); {
	case err == nil, errors.Is(err, errGone):
	case errors.Is(err, errConflict):
		// Somebody wrote the order meanwhile: the next pass sees it.
	default:
		log.Warn("writing the status of the order failed", "error", err)
	}
}

// cleanUp removes what was presented for an order: the DNS records and the
// HTTP-01 answers. A record that cannot be removed stays in the zone; it
// proves nothing once its challenge is over.
func (o *Operator) cleanUp(ctx context.Context, order string, pr *Progress) {
	if pr != nil {
		for _, rec := range pr.Presented {
			if err := o.zones.cleanUp(ctx, rec); err != nil {
				o.log().Warn("removing a challenge record failed", "order", order, "name", rec.Name, "error", err)
			}
		}
		pr.Presented = nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, token := range o.tokens[order] {
		o.responder.cleanUp(token)
	}
	delete(o.tokens, order)
	delete(o.waiting, order)
}

// issue moves the issuance of an order forward. It returns nil when the
// chain is in the status, errWait when the certificate authority or a name
// server needs time, a permanentError when the order cannot be fulfilled
// and any other error for a failure worth another attempt.
func (o *Operator) issue(ctx context.Context, is *issuer, orderName string, spec *OrderSpec, st *OrderStatus) error {
	block, _ := pem.Decode([]byte(spec.CSR))
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return permanent("the order has no signing request")
	}
	if len(spec.DNSNames) == 0 {
		return permanent("the order names no DNS name")
	}
	auth := is.account.authority
	pr := st.Progress

	if pr.OrderURL == "" {
		order, err := auth.AuthorizeOrder(ctx, spec.DNSNames)
		if err != nil {
			return o.caError("placing the order", err)
		}
		pr.OrderURL = order.URI
		// The URL is written before anything else is done with the order,
		// so a restart continues it instead of placing a second one.
		return errWait
	}

	order, err := auth.GetOrder(ctx, pr.OrderURL)
	if err != nil {
		return o.caError("reading the order", err)
	}
	var der [][]byte
	switch order.Status {
	case xacme.StatusPending:
		if err := o.authorize(ctx, is, orderName, order, pr); err != nil {
			return err
		}
		return errWait
	case xacme.StatusProcessing:
		return errWait
	case xacme.StatusReady:
		if der, err = auth.Finalize(ctx, order.FinalizeURL, block.Bytes); err != nil {
			return o.caError("finalizing the order", err)
		}
	case xacme.StatusValid:
		if der, err = auth.FetchCert(ctx, order.CertURL); err != nil {
			return o.caError("fetching the certificate", err)
		}
	default:
		return permanent("the certificate authority ended the order: %s", o.orderProblem(ctx, auth, order))
	}
	return o.complete(ctx, is, st, der)
}

// orderProblem says why the certificate authority ended an order: its own
// error, or that of the authorization that failed.
func (o *Operator) orderProblem(ctx context.Context, auth authority, order *xacme.Order) string {
	if order.Error != nil {
		text, _ := problemOf(order.Error)
		return text
	}
	for _, authzURL := range order.AuthzURLs {
		az, err := auth.GetAuthorization(ctx, authzURL)
		if err != nil || az.Status == xacme.StatusValid || az.Status == xacme.StatusPending {
			continue
		}
		return fmt.Sprintf("the control of %s was not proven: %s", az.Identifier.Value, authorizationProblem(az))
	}
	return "the order is " + order.Status
}

// caError turns an error of the certificate authority into the error of a
// pass.
func (o *Operator) caError(what string, err error) error {
	text, perm := problemOf(err)
	if perm {
		return permanent("%s: %s", what, text)
	}
	return fmt.Errorf("%s: %s", what, text)
}

// authorize works on the authorizations of an order that is pending: for
// each name that is not proven yet it presents a challenge and, once the
// proof can be seen, tells the certificate authority to check it.
func (o *Operator) authorize(ctx context.Context, is *issuer, orderName string, order *xacme.Order, pr *Progress) error {
	auth := is.account.authority
	for _, authzURL := range order.AuthzURLs {
		az, err := auth.GetAuthorization(ctx, authzURL)
		if err != nil {
			return o.caError("reading an authorization", err)
		}
		name := az.Identifier.Value
		switch az.Status {
		case xacme.StatusValid:
			continue
		case xacme.StatusPending, xacme.StatusProcessing:
		default:
			return permanent("the control of %s was not proven: %s", name, authorizationProblem(az))
		}

		chal, zone, err := o.choose(ctx, is, az)
		if err != nil {
			return err
		}
		switch chal.Type {
		case "http-01":
			body, err := auth.HTTP01Response(chal.Token)
			if err != nil {
				return permanent("the HTTP-01 answer for %s: %v", name, err)
			}
			o.responder.present(chal.Token, body)
			o.mu.Lock()
			if !slices.Contains(o.tokens[orderName], chal.Token) {
				o.tokens[orderName] = append(o.tokens[orderName], chal.Token)
			}
			o.mu.Unlock()
		case "dns-01":
			value, err := auth.DNS01Record(chal.Token)
			if err != nil {
				return permanent("the DNS-01 record for %s: %v", name, err)
			}
			rec := PresentedRecord{Zone: zone.Name, Name: dns.ChallengeName(name), Value: value}
			if !slices.Contains(pr.Presented, rec) {
				if err := o.zones.present(ctx, rec); err != nil {
					return fmt.Errorf("presenting the challenge record of %s: %w", name, err)
				}
				pr.Presented = append(pr.Presented, rec)
			}
			if chal.Status == xacme.StatusPending {
				seen, err := o.visible(ctx, orderName, rec)
				if err != nil {
					return err
				}
				if !seen {
					continue
				}
			}
		}
		if chal.Status == xacme.StatusPending {
			if err := auth.Accept(ctx, chal); err != nil {
				return o.caError("accepting the challenge of "+name, err)
			}
		}
	}
	return nil
}

// visible reports whether a challenge record can be seen at the name
// servers of its zone. A record that does not appear within the timeout is
// a failure of the attempt.
func (o *Operator) visible(ctx context.Context, orderName string, rec PresentedRecord) (bool, error) {
	if o.propagation == nil {
		return true, nil
	}
	seen, err := o.propagation.visible(ctx, rec.Zone, rec.Name, rec.Value)
	if seen {
		return true, nil
	}
	o.mu.Lock()
	since, waiting := o.waiting[orderName]
	if !waiting {
		since = o.now()
		o.waiting[orderName] = since
	}
	o.mu.Unlock()
	if o.now().Sub(since) < o.propagationTimeout {
		return false, nil
	}
	o.mu.Lock()
	delete(o.waiting, orderName)
	o.mu.Unlock()
	if err != nil {
		return false, fmt.Errorf("the challenge record %s cannot be checked: %w", rec.Name, err)
	}
	return false, fmt.Errorf("the challenge record %s did not appear at the name servers of %s within %s", rec.Name, rec.Zone, o.propagationTimeout)
}

// choose picks the challenge of an authorization: DNS-01 when a zone of
// the issuer holds the name (the only one for a wildcard), HTTP-01
// otherwise. A challenge the certificate authority is checking already is
// kept.
func (o *Operator) choose(ctx context.Context, is *issuer, az *xacme.Authorization) (*xacme.Challenge, *zoneInfo, error) {
	name := az.Identifier.Value
	byType := map[string]*xacme.Challenge{}
	for _, c := range az.Challenges {
		byType[c.Type] = c
	}
	var zone *zoneInfo
	if d := is.config.DNS01; (d == nil || !d.Disabled) && byType["dns-01"] != nil {
		var allowed []string
		if d != nil {
			allowed = d.Zones
		}
		var err error
		zone, err = o.zones.find(ctx, name, allowed)
		http01 := byType["http-01"]
		switch {
		case err == nil:
		case errors.Is(err, errZoneAway) && http01 != nil && !az.Wildcard && is.config.http01():
			// The name can be proven over HTTP while the zone is away.
			zone = nil
		default:
			return nil, nil, err
		}
	}
	http01 := byType["http-01"]
	if http01 != nil && (az.Wildcard || !is.config.http01()) {
		http01 = nil
	}
	// A challenge that is being checked decides.
	if c := byType["dns-01"]; c != nil && c.Status == xacme.StatusProcessing && zone != nil {
		return c, zone, nil
	}
	if http01 != nil && http01.Status == xacme.StatusProcessing {
		return http01, nil, nil
	}
	switch {
	case zone != nil:
		return byType["dns-01"], zone, nil
	case http01 != nil:
		return http01, nil, nil
	case az.Wildcard:
		return nil, nil, permanent("the wildcard %s needs the DNS-01 challenge and no DNS zone of the issuer holds it", "*."+name)
	}
	return nil, nil, permanent("no challenge can be used for %s: no DNS zone of the issuer holds it and HTTP-01 is not offered or turned off", name)
}

// authorizationProblem says why an authorization failed.
func authorizationProblem(az *xacme.Authorization) string {
	for _, c := range az.Challenges {
		if c.Error != nil {
			text, _ := problemOf(c.Error)
			return fmt.Sprintf("%s: %s", c.Type, text)
		}
	}
	return "the authorization is " + az.Status
}

// complete takes the chain of the certificate authority: it finds the root
// the chain ends in and declares it in the status of the issuer, keeps the
// certificate for a later revocation, and puts the chain into the status
// of the order.
func (o *Operator) complete(ctx context.Context, is *issuer, st *OrderStatus, der [][]byte) error {
	if len(der) == 0 {
		return permanent("the certificate authority returned no certificate")
	}
	certs := make([]*x509.Certificate, 0, len(der))
	for _, raw := range der {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return permanent("the certificate authority returned a certificate that cannot be read: %v", err)
		}
		certs = append(certs, cert)
	}
	leaf := certs[0]

	root, err := o.rootOf(is, certs)
	if err != nil {
		return permanent("the chain of the certificate authority is not trusted: %v", err)
	}
	rootPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root.Raw}))
	o.mu.Lock()
	if !slices.Contains(is.account.learned, rootPEM) {
		is.account.learned = append(is.account.learned, rootPEM)
	}
	o.mu.Unlock()
	// The servers check the chain against the roots of the issuer: they
	// must be there before the chain is.
	if !o.writeIssuerStatus(ctx, is, "", true) {
		return errors.New("the root of the chain could not be declared in the status of the issuer")
	}

	serial := hex.EncodeToString(leaf.SerialNumber.Bytes())
	expires := leaf.NotAfter
	path := o.secretPrefix + secretIssued + is.obj.Name + "/" + serial
	leafPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leaf.Raw}))
	if err := o.secrets.Write(ctx, path, map[string]string{"cert": leafPEM}, &expires); err != nil {
		return fmt.Errorf("keeping the certificate for a revocation (%s): %w", path, err)
	}

	st.Chain = chainPEM(der)
	st.RenewAfter = ""
	if len(certs) > 1 {
		if at, ok := is.account.authority.RenewalInfo(ctx, leaf, certs[1]); ok && at.After(leaf.NotBefore) && at.Before(leaf.NotAfter) {
			st.RenewAfter = at.UTC().Format(time.RFC3339)
		}
	}
	return nil
}

// rootOf verifies a chain against the roots of the issuer (the configured
// ones, or those of the system) and returns the root it ends in.
func (o *Operator) rootOf(is *issuer, certs []*x509.Certificate) (*x509.Certificate, error) {
	roots := o.systemRoots
	if is.config.Roots != "" {
		configured, err := parsePEM(is.config.Roots)
		if err != nil || len(configured) == 0 {
			return nil, errors.New("the roots of the issuer configuration hold no certificate")
		}
		roots = x509.NewCertPool()
		for _, c := range configured {
			roots.AddCert(c)
		}
	} else if roots == nil {
		var err error
		if roots, err = x509.SystemCertPool(); err != nil {
			return nil, fmt.Errorf("the roots of the system: %w", err)
		}
	}
	intermediates := x509.NewCertPool()
	for _, c := range certs[1:] {
		intermediates.AddCert(c)
	}
	chains, err := certs[0].Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		CurrentTime:   o.now(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return nil, err
	}
	chain := chains[0]
	return chain[len(chain)-1], nil
}

// revoke revokes the certificate with the serial number of a revocation
// order. The operator kept the certificates it issued.
func (o *Operator) revoke(ctx context.Context, is *issuer, spec *OrderSpec) error {
	path := o.secretPrefix + secretIssued + is.obj.Name + "/" + strings.ToLower(spec.Revoke)
	items, err := o.secrets.Read(ctx, path)
	if err != nil {
		return fmt.Errorf("reading the certificate to revoke (%s): %w", path, err)
	}
	block, _ := pem.Decode([]byte(items["cert"]))
	if block == nil {
		return permanent("the certificate with the serial %s is not one this controller issued for the issuer %s", spec.Revoke, is.obj.Name)
	}
	if err := is.account.authority.Revoke(ctx, block.Bytes); err != nil {
		text, perm := problemOf(err)
		if strings.Contains(text, "alreadyRevoked") {
			return nil
		}
		if perm {
			return permanent("revoking: %s", text)
		}
		return fmt.Errorf("revoking: %s", text)
	}
	return nil
}
