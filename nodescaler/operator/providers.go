// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	provider "github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca/norca/controller"
)

// registeredProvider is a NodeProvider object.
type registeredProvider struct {
	Name          string
	Endpoint      string
	TokenVariable string
	Heartbeat     time.Time // zero: no heartbeat
	Groups        map[string]bool
}

// providers reads the NodeProvider objects and calls the providers.
type providers struct {
	objs controller.ObjectsAPI
	vars controller.VariablesAPI

	// ttl is how old a heartbeat may be. A provider without a heartbeat is a
	// static registration and always available.
	ttl time.Duration

	// tokenNamespace is the namespace of the token Variables.
	tokenNamespace string

	// allowHTTP permits http endpoints (development and tests).
	allowHTTP bool

	http *http.Client
	now  func() time.Time

	// cacheFor is how long a listing is reused.
	cacheFor time.Duration
	mu       sync.Mutex
	cached   map[string]*registeredProvider
	at       time.Time
}

func (p *providers) list(ctx context.Context) (map[string]*registeredProvider, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cached != nil && p.now().Sub(p.at) < p.cacheFor {
		return p.cached, nil
	}
	objs, _, err := p.objs.ListObjects(ctx, NodeProviderKind, "*", 0)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*registeredProvider, len(objs))
	for _, o := range objs {
		reg := &registeredProvider{Name: o.Name, Groups: map[string]bool{}}
		reg.Endpoint, _ = o.Spec["endpoint"].(string)
		reg.TokenVariable, _ = o.Spec["token_variable"].(string)
		if groups, ok := o.Spec["groups"].([]any); ok {
			for _, g := range groups {
				if m, ok := g.(map[string]any); ok {
					if name, ok := m["name"].(string); ok {
						reg.Groups[name] = true
					}
				}
			}
		}
		if hb, ok := o.Status["heartbeat"].(string); ok {
			reg.Heartbeat, _ = time.Parse(time.RFC3339, hb)
		}
		out[o.Name] = reg
	}
	p.cached, p.at = out, p.now()
	return out, nil
}

// errUnavailable is a provider that is not registered or not alive; the
// reason is the reason of the condition ProviderAvailable.
type errUnavailable struct {
	reason, message string
}

func (e *errUnavailable) Error() string { return e.message }

// client returns the client of the named provider, checked for being
// registered, alive and offering the group.
func (p *providers) client(ctx context.Context, name, group string) (*provider.Client, error) {
	all, err := p.list(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing the providers: %w", err)
	}
	reg, ok := all[name]
	if !ok {
		return nil, &errUnavailable{"UnknownProvider", fmt.Sprintf("no NodeProvider %q is registered", name)}
	}
	if err := p.available(reg); err != nil {
		return nil, err
	}
	if len(reg.Groups) > 0 && !reg.Groups[group] {
		return nil, &errUnavailable{"UnknownGroup", fmt.Sprintf("provider %q does not offer the group %q", name, group)}
	}
	return p.clientOf(ctx, reg)
}

// available checks that a provider is alive: a heartbeat within the TTL,
// or none (a static registration).
func (p *providers) available(reg *registeredProvider) error {
	if !reg.Heartbeat.IsZero() && p.now().Sub(reg.Heartbeat) > p.ttl {
		return &errUnavailable{"ProviderUnavailable",
			fmt.Sprintf("the heartbeat of provider %q is %s old", reg.Name, p.now().Sub(reg.Heartbeat).Round(time.Second))}
	}
	return nil
}

// clientOf returns the client of a registered provider: an https endpoint
// (http only when allowed) and its token.
func (p *providers) clientOf(ctx context.Context, reg *registeredProvider) (*provider.Client, error) {
	u, err := url.Parse(reg.Endpoint)
	if err != nil || (u.Scheme != "https" && (u.Scheme != "http" || !p.allowHTTP)) {
		return nil, &errUnavailable{"InvalidProvider",
			fmt.Sprintf("the endpoint %q of provider %q is not an https URL", reg.Endpoint, reg.Name)}
	}
	token, err := p.token(ctx, reg)
	if err != nil {
		return nil, err
	}
	return &provider.Client{Endpoint: reg.Endpoint, Token: token, HTTP: p.http}, nil
}

// token reads the item "token" of the Variable the provider names.
func (p *providers) token(ctx context.Context, reg *registeredProvider) (string, error) {
	if reg.TokenVariable == "" {
		return "", nil
	}
	v, err := p.vars.ReadVariable(ctx, p.tokenNamespace, reg.TokenVariable)
	if errors.Is(err, controller.ErrNotFound) {
		return "", &errUnavailable{"InvalidProvider",
			fmt.Sprintf("the token Variable %q of provider %q does not exist", reg.TokenVariable, reg.Name)}
	}
	if err != nil {
		return "", fmt.Errorf("reading the token Variable %q of provider %q: %w", reg.TokenVariable, reg.Name, err)
	}
	return v.Items["token"], nil
}
