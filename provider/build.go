// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"strings"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca-platform/provider/params"
	"github.com/bacanoo-io/norca-platform/provider/registrar"
)

// Serve narrows what a process serves of its configuration (the -serve
// flag): it never adds a section the configuration does not have.
type Serve string

// The values of Serve.
const (
	ServeAll  Serve = "all"
	ServeNode Serve = "node"
	ServeLB   Serve = "lb"
)

// ParseServe reads the -serve flag; empty is ServeAll.
func ParseServe(s string) (Serve, error) {
	switch v := Serve(s); v {
	case "":
		return ServeAll, nil
	case ServeAll, ServeNode, ServeLB:
		return v, nil
	}
	return "", fmt.Errorf("serve %q: want node, lb or all", s)
}

func (s Serve) node() bool { return s == ServeAll || s == ServeNode || s == "" }
func (s Serve) lb() bool   { return s == ServeAll || s == ServeLB || s == "" }

// Runtime is a built configuration: the node providers and the classes this
// process serves.
type Runtime struct {
	Nodes   []*NodeProvider
	Classes []*ServedClass
}

// NodeProvider is a served node provider.
type NodeProvider struct {
	Name     string
	Type     string
	Provider node.Provider

	// Endpoint is the URL the operator calls (in the NodeProvider object).
	Endpoint string

	// Token is the bearer token of the calls; empty with no_auth.
	Token         string
	TokenVariable string

	Groups []registrar.GroupSpec

	// Parameters are published in the NodeProvider object.
	Parameters map[string]any
}

// ServedClass is a served load balancer class.
type ServedClass struct {
	*lb.Class

	// ProviderName is the provider of the configuration the class belongs
	// to.
	ProviderName string

	// Endpoint is the URL the operator calls (in the LoadBalancerClass
	// object).
	Endpoint string

	// Token is the bearer token of the calls; empty with no_auth.
	Token         string
	TokenVariable string
}

// Registrations returns the objects the runtime registers.
func (rt *Runtime) Registrations() []registrar.Registration {
	var out []registrar.Registration
	for _, n := range rt.Nodes {
		out = append(out, registrar.NodeProvider(n.Name, n.Endpoint, n.TokenVariable, n.Groups, n.Parameters))
	}
	for _, c := range rt.Classes {
		out = append(out, registrar.LoadBalancerClass(c.Name, c.Endpoint, c.TokenVariable, c.Caps.Internal, c.Caps.Ports, c.Published))
	}
	return out
}

// Close releases the providers that hold resources.
func (rt *Runtime) Close() {
	for _, c := range rt.Classes {
		_ = c.Close()
	}
	for _, n := range rt.Nodes {
		if cl, ok := n.Provider.(io.Closer); ok {
			_ = cl.Close()
		}
	}
}

// Check checks the configuration against the types: every type exists and
// implements the sections the configuration asks of it.
func (f *File) Check(reg *Registry) error {
	for _, p := range f.Providers {
		t, err := reg.lookup(p.Type)
		if err != nil {
			return fmt.Errorf("provider %q: %w", p.Name, err)
		}
		if p.Node != nil && t.Node == nil {
			return fmt.Errorf("provider %q: type %s has no node provider", p.Name, t.ID)
		}
		if p.LoadBalancer != nil && t.LB == nil {
			return fmt.Errorf("provider %q: type %s has no load balancer provider", p.Name, t.ID)
		}
	}
	return nil
}

// Build checks the configuration against the types (Check) and creates the
// providers of the sections serve keeps. A configuration that leaves
// nothing to serve is an error.
func (f *File) Build(reg *Registry, serve Serve, log *slog.Logger) (*Runtime, error) {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if err := f.Check(reg); err != nil {
		return nil, err
	}
	base := strings.TrimRight(f.AdvertiseURL, "/")
	rt := &Runtime{}
	fail := func(err error) (*Runtime, error) {
		rt.Close()
		return nil, err
	}
	for _, p := range f.Providers {
		t, _ := reg.Lookup(p.Type)
		token, err := p.token()
		if err != nil {
			return fail(fmt.Errorf("provider %q: %w", p.Name, err))
		}
		if p.Node != nil && serve.node() {
			n, err := buildNode(t, p, token, base, log)
			if err != nil {
				return fail(fmt.Errorf("provider %q: %w", p.Name, err))
			}
			rt.Nodes = append(rt.Nodes, n)
		}
		if p.LoadBalancer != nil && serve.lb() {
			for _, cc := range p.LoadBalancer.Classes {
				c, err := newClass(p.Name, t, cc.Class, p.Params, cc.Params, log)
				if err != nil {
					return fail(fmt.Errorf("provider %q: %w", p.Name, err))
				}
				sc := &ServedClass{Class: c, ProviderName: p.Name, Endpoint: cc.Endpoint, Token: token, TokenVariable: p.TokenVariable}
				rt.Classes = append(rt.Classes, sc)
				if cc.Internal != nil {
					if *cc.Internal && !c.Caps.Internal {
						return fail(fmt.Errorf("provider %q: class %q: type %s cannot create internal load balancers with these parameters", p.Name, cc.Class, t.ID))
					}
					c.Caps.Internal = *cc.Internal
				}
				c.Caps.Ports = cc.Ports
				if sc.Endpoint == "" {
					sc.Endpoint = base + "/classes/" + cc.Class
				}
			}
		}
	}
	if len(rt.Nodes) == 0 && len(rt.Classes) == 0 {
		return nil, fmt.Errorf("nothing to serve: the configuration has no %s section", map[Serve]string{ServeNode: "node", ServeLB: "loadbalancer"}[serve])
	}
	return rt, nil
}

// token resolves the token of a provider.
func (p ProviderConfig) token() (string, error) {
	switch {
	case p.Token != "":
		t, err := params.ResolveSecret(p.Token)
		if err != nil {
			return "", fmt.Errorf("token: %w", err)
		}
		return t, nil
	case p.TokenFile != "":
		data, err := os.ReadFile(p.TokenFile)
		if err != nil {
			return "", fmt.Errorf("reading the token: %w", err)
		}
		t := strings.TrimSpace(string(data))
		if t == "" {
			return "", errors.New("the token file is empty")
		}
		return t, nil
	}
	return "", nil
}

func buildNode(t Type, p ProviderConfig, token, base string, log *slog.Logger) (*NodeProvider, error) {
	shared, err := params.Resolve(t.Params, p.Params)
	if err != nil {
		return nil, fmt.Errorf("params: %w", err)
	}
	section, err := params.Resolve(t.Node.Params, p.Node.Params)
	if err != nil {
		return nil, fmt.Errorf("node params: %w", err)
	}
	endpoint := p.Node.Endpoint
	if endpoint == "" {
		endpoint = base + "/providers/" + p.Name
	}
	n := &NodeProvider{
		Name: p.Name, Type: t.ID, Endpoint: endpoint, Token: token, TokenVariable: p.TokenVariable,
		Parameters: map[string]any{"type": t.ID},
	}
	maps.Copy(n.Parameters, params.Published(t.Params, shared))
	maps.Copy(n.Parameters, params.Published(t.Node.Params, section))
	cfg := NodeConfig{Name: p.Name, Values: params.Merge(shared, section), Endpoint: endpoint, Logger: log.With("provider", p.Name)}
	for _, g := range p.Node.Groups {
		gv, err := params.Resolve(t.Node.GroupParams, g.Params)
		if err != nil {
			return nil, fmt.Errorf("group %q: %w", g.Name, err)
		}
		cfg.Groups = append(cfg.Groups, NodeGroup{Name: g.Name, Max: g.Max, Values: gv})
		n.Groups = append(n.Groups, registrar.GroupSpec{Name: g.Name, Max: g.Max, Parameters: params.Published(t.Node.GroupParams, gv)})
	}
	if n.Provider, err = t.Node.New(cfg); err != nil {
		return nil, err
	}
	return n, nil
}
