// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca/norca/controller"
)

// ClassKind is the cluster-scoped object type that registers a load balancer
// provider: which class it handles and where the operator calls it (see
// loadbalancerclass.kind.hcl). Providers are external applications; they
// write their own LoadBalancerClass object when they start and report a
// heartbeat in its status.
var ClassKind = controller.Kind{Group: "norca.io", Kind: "LoadBalancerClass"}

// ClassSpec is the registration of a provider.
type ClassSpec struct {
	// Endpoint is the base URL of the provider API (https).
	Endpoint string `json:"endpoint"`

	// TokenVariable is the path of the Variable whose item "token" is sent
	// as the bearer token of the calls. The token itself never is part of
	// the object.
	TokenVariable string `json:"tokenVariable,omitempty"`

	Capabilities ClassCapabilities `json:"capabilities,omitzero"`

	// Parameters are provider specific settings, passed on with every call.
	Parameters map[string]any `json:"parameters,omitempty"`
}

// ClassCapabilities is what a provider supports.
type ClassCapabilities struct {
	// Internal is true if the provider can create load balancers that are
	// not reachable from the internet.
	Internal bool `json:"internal,omitempty"`

	// Ports limits the frontend ports ("1-65535", "80,443", "8000-8100,443").
	// Empty means any port.
	Ports string `json:"ports,omitempty"`
}

// ClassStatus is reported by the provider, as in the status of a
// NodeProvider object.
type ClassStatus struct {
	// Heartbeat (RFC 3339) is when the provider last said it is alive. A
	// class without a heartbeat is a static registration and always
	// available; one with a heartbeat older than the class TTL is not.
	Heartbeat string `json:"heartbeat,omitempty"`

	// Version of the provider.
	Version string `json:"version,omitempty"`
}

// classInfo is a registered provider.
type classInfo struct {
	Name   string
	Spec   ClassSpec
	Status ClassStatus
}

// Validate checks the registration.
func (c *classInfo) validate(allowHTTP bool) error {
	u, err := url.Parse(c.Spec.Endpoint)
	if err != nil || u.Host == "" {
		return fmt.Errorf("invalid endpoint %q", c.Spec.Endpoint)
	}
	if u.Scheme != "https" && !(allowHTTP && u.Scheme == "http") {
		return fmt.Errorf("endpoint %q must be an https URL", c.Spec.Endpoint)
	}
	if _, err := lb.ParsePorts(c.Spec.Capabilities.Ports); err != nil {
		return fmt.Errorf("capabilities.ports: %w", err)
	}
	return nil
}

// available reports whether the heartbeat of the provider is recent.
func (c *classInfo) available(now time.Time, ttl time.Duration) (bool, string) {
	if c.Status.Heartbeat == "" {
		return true, ""
	}
	t, err := time.Parse(time.RFC3339, c.Status.Heartbeat)
	if err != nil {
		return false, fmt.Sprintf("invalid heartbeat %q", c.Status.Heartbeat)
	}
	if age := now.Sub(t); age > ttl {
		return false, fmt.Sprintf("no heartbeat for %s (last at %s)", age.Round(time.Second), c.Status.Heartbeat)
	}
	return true, ""
}

// unsupported returns why the provider cannot implement the spec, or "".
func (c *classInfo) unsupported(spec Spec) string {
	if spec.Internal && !c.Spec.Capabilities.Internal {
		return fmt.Sprintf("class %q cannot create internal load balancers", c.Name)
	}
	if ranges, _ := lb.ParsePorts(c.Spec.Capabilities.Ports); !lb.PortAllowed(ranges, spec.Port) {
		return fmt.Sprintf("class %q does not support port %d (supported: %s)", c.Name, spec.Port, c.Spec.Capabilities.Ports)
	}
	return ""
}

// classRegistry reads the LoadBalancerClass objects. The list is cached for
// a few seconds, so a burst of reconciles makes one call.
type classRegistry struct {
	objs controller.ObjectsAPI
	vars controller.VariablesAPI

	// ttl is how old a heartbeat may be.
	ttl time.Duration

	// tokenNamespace is the namespace of the token Variables.
	tokenNamespace string

	// allowHTTP permits http endpoints (development and tests).
	allowHTTP bool

	// cacheFor is how long a listing is reused.
	cacheFor time.Duration
	now      func() time.Time
	client   *http.Client

	mu     sync.Mutex
	cached map[string]*classInfo
	at     time.Time
}

func newClassRegistry(b controller.Backend, tokenNamespace string, ttl time.Duration) *classRegistry {
	if tokenNamespace == "" {
		tokenNamespace = "default"
	}
	return &classRegistry{
		objs:           b,
		vars:           b,
		ttl:            ttl,
		tokenNamespace: tokenNamespace,
		cacheFor:       3 * time.Second,
		now:            time.Now,
		client:         &http.Client{Timeout: 30 * time.Second},
	}
}

// setCAFile makes the calls to the providers trust the certificates of the
// PEM file in addition to the system roots, for providers with a certificate
// of a private CA (the CA of the cluster, for example). A file without a
// certificate is an error.
//
// The certificate of a provider is verified against the system roots first and
// then against the file. The verification is done here, not by adding the
// file to the system pool: on Windows and macOS the platform verifier ignores
// what is added to that pool.
func (g *classRegistry) setCAFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("class CA file: %w", err)
	}
	extra := x509.NewCertPool()
	if !extra.AppendCertsFromPEM(raw) {
		return fmt.Errorf("class CA file %s: no certificate found", path)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Verification is done in VerifyConnection, with the system roots and
		// the extra CA; the handshake itself does not verify.
		InsecureSkipVerify: true, //nolint:gosec // verified by VerifyConnection below
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("the provider presented no certificate")
			}
			intermediates := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			opts := x509.VerifyOptions{DNSName: cs.ServerName, Intermediates: intermediates}
			leaf := cs.PeerCertificates[0]
			if _, err := leaf.Verify(opts); err == nil {
				return nil // a certificate the system trusts
			}
			opts.Roots = extra
			_, err := leaf.Verify(opts)
			return err
		},
	}
	g.client = &http.Client{Timeout: g.client.Timeout, Transport: transport}
	return nil
}

func (g *classRegistry) list(ctx context.Context) (map[string]*classInfo, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cached != nil && g.now().Sub(g.at) < g.cacheFor {
		return g.cached, nil
	}
	objs, _, err := g.objs.ListObjects(ctx, ClassKind, "*", 0)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*classInfo, len(objs))
	for _, o := range objs {
		info := &classInfo{Name: o.Name}
		if b, err := json.Marshal(o.Spec); err == nil {
			_ = json.Unmarshal(b, &info.Spec)
		}
		if b, err := json.Marshal(o.Status); err == nil {
			_ = json.Unmarshal(b, &info.Status)
		}
		out[o.Name] = info
	}
	g.cached, g.at = out, g.now()
	return out, nil
}

// lookup returns the registered class, or nil.
func (g *classRegistry) lookup(ctx context.Context, name string) (*classInfo, error) {
	all, err := g.list(ctx)
	if err != nil {
		return nil, err
	}
	return all[name], nil
}

// names returns the registered class names, sorted.
func (g *classRegistry) names(ctx context.Context) []string {
	all, err := g.list(ctx)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(all))
	for name := range all {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// remoteProvider calls a registered provider over the load balancer
// contract (package provider/lb):
//
//	PUT    {endpoint}/v1/loadbalancers/{namespace}/{name}   create or update
//	DELETE {endpoint}/v1/loadbalancers/{namespace}/{name}   remove
//
// PUT is idempotent and returns the state of the load balancer.
type remoteProvider struct {
	info *classInfo
	reg  *classRegistry
}

func (g *classRegistry) provider(info *classInfo) *remoteProvider {
	return &remoteProvider{info: info, reg: g}
}

func (p *remoteProvider) Class() string { return p.info.Name }

func (p *remoteProvider) token(ctx context.Context) (string, error) {
	path := p.info.Spec.TokenVariable
	if path == "" {
		return "", nil
	}
	v, err := p.reg.vars.ReadVariable(ctx, p.reg.tokenNamespace, path)
	if err != nil {
		return "", fmt.Errorf("reading the token Variable %q of class %q: %w", path, p.info.Name, err)
	}
	return v.Items["token"], nil
}

// client returns the client of the class, after checking its registration.
func (p *remoteProvider) client(ctx context.Context) (*lb.Client, error) {
	if err := p.info.validate(p.reg.allowHTTP); err != nil {
		return nil, fmt.Errorf("class %q: %w", p.info.Name, err)
	}
	token, err := p.token(ctx)
	if err != nil {
		return nil, err
	}
	return &lb.Client{Endpoint: p.info.Spec.Endpoint, Token: token, HTTP: p.reg.client}, nil
}

// classError names the class in an error of the client: "class "x"
// answered 502: ..." for an answer, "calling class "x": ..." for a failed
// call.
func (p *remoteProvider) classError(err error) error {
	if _, ok := errors.AsType[*lb.ResponseError](err); ok {
		return fmt.Errorf("class %q %w", p.info.Name, err)
	}
	return fmt.Errorf("calling class %q: %w", p.info.Name, err)
}

func (p *remoteProvider) Ensure(ctx context.Context, obj *controller.Object, spec Spec, targets []Target) (Result, error) {
	c, err := p.client(ctx)
	if err != nil {
		return Result{}, err
	}
	req := lb.EnsureRequest{
		Name:       obj.Key.Name,
		Namespace:  obj.Key.Namespace,
		Service:    spec.Service,
		Port:       spec.Port,
		Internal:   spec.Internal,
		Targets:    make([]lb.WireTarget, len(targets)),
		Parameters: p.info.Spec.Parameters,
	}
	for i, t := range targets {
		req.Targets[i] = lb.WireTarget{Address: t.Address, Port: t.Port, AllocID: t.AllocID}
	}
	out, err := c.Ensure(ctx, lb.Ref{Namespace: obj.Key.Namespace, Name: obj.Key.Name}, req)
	if err != nil {
		return Result{}, p.classError(err)
	}
	return Result{Addresses: out.Addresses, Ready: out.Ready, Message: out.Message, Resource: out.Resource}, nil
}

func (p *remoteProvider) Delete(ctx context.Context, key controller.ObjectKey) error {
	c, err := p.client(ctx)
	if err != nil {
		return err
	}
	if err := c.Delete(ctx, lb.Ref{Namespace: key.Namespace, Name: key.Name}); err != nil {
		return p.classError(err)
	}
	return nil
}

var errClassNotRegistered = errors.New("class is not registered")
