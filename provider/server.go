// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca-platform/provider/params"
	"github.com/bacanoo-io/norca-platform/provider/wire"
)

// NewHandler serves a runtime on one HTTP server:
//
//	/providers/{name}/v1/groups/...           the node contract of a provider (package node)
//	/classes/{class}/v1/loadbalancers/...     the load balancer contract of a class (package lb)
//	/zones/{zone}/v1/txt/...                  the DNS contract of a zone (package dns)
//	GET /healthz                              the health check
//	GET /v1/types                             the linked types, their capabilities and parameters
//	GET /v1/providers                         the served node providers and classes
//	GET /v1/classes                           the served classes
//	GET /v1/zones                             the served DNS zones
//
// Every provider checks its own token, for its node contract, its classes
// and its zones alike; failures have the shape of package wire.
func NewHandler(rt *Runtime, types []Type, log *slog.Logger) http.Handler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		wire.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/types", func(w http.ResponseWriter, _ *http.Request) {
		wire.WriteJSON(w, http.StatusOK, describeTypes(types))
	})
	mux.HandleFunc("GET /v1/providers", func(w http.ResponseWriter, _ *http.Request) {
		wire.WriteJSON(w, http.StatusOK, describeProviders(rt))
	})
	mux.HandleFunc("GET /v1/classes", func(w http.ResponseWriter, _ *http.Request) {
		wire.WriteJSON(w, http.StatusOK, describeClasses(rt.Classes))
	})
	for _, n := range rt.Nodes {
		prefix := "/providers/" + n.Name
		mux.Handle(prefix+"/", http.StripPrefix(prefix, node.NewHandler(n.Provider, n.Token, log.With("provider", n.Name))))
	}
	for _, c := range rt.Classes {
		prefix := "/classes/" + c.Name
		mux.Handle(prefix+"/", http.StripPrefix(prefix, lb.NewHandler(c.Class, c.Token, log)))
	}
	mux.HandleFunc("GET /v1/zones", func(w http.ResponseWriter, _ *http.Request) {
		wire.WriteJSON(w, http.StatusOK, describeZones(rt.Zones))
	})
	for _, z := range rt.Zones {
		prefix := "/zones/" + z.Name
		mux.Handle(prefix+"/", http.StripPrefix(prefix, dns.NewHandler(z.Zone, z.Token, log)))
	}
	// What no route takes: an unknown provider or class, in the shape of
	// the contracts.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		msg := "not found"
		switch {
		case strings.HasPrefix(r.URL.Path, "/providers/"):
			msg = "unknown provider"
		case strings.HasPrefix(r.URL.Path, "/classes/"):
			msg = "unknown class"
		case strings.HasPrefix(r.URL.Path, "/zones/"):
			msg = "unknown zone"
		}
		wire.WriteError(w, http.StatusNotFound, wire.ReasonInvalid, msg)
	})
	return mux
}

// TypeInfo describes a type on /v1/types.
type TypeInfo struct {
	ID           string        `json:"id"`
	Description  string        `json:"description"`
	Capabilities []string      `json:"capabilities"`
	Parameters   []params.Info `json:"parameters"`
	Node         *NodeInfo     `json:"node,omitempty"`
	LoadBalancer *LBInfo       `json:"loadbalancer,omitempty"`
	DNS          *DNSInfo      `json:"dns,omitempty"`
}

// DNSInfo describes the DNS provider of a type.
type DNSInfo struct {
	ZoneParameters []params.Info `json:"zone_parameters"`
}

// NodeInfo describes the node provider of a type.
type NodeInfo struct {
	Parameters      []params.Info `json:"parameters"`
	GroupParameters []params.Info `json:"group_parameters"`
}

// LBInfo describes the load balancer provider of a type.
type LBInfo struct {
	ClassParameters []params.Info `json:"class_parameters"`
}

// describeTypes describes the types.
func describeTypes(types []Type) []TypeInfo {
	out := make([]TypeInfo, 0, len(types))
	for _, t := range types {
		info := TypeInfo{ID: t.ID, Description: t.Description, Capabilities: t.Capabilities(), Parameters: params.Infos(t.Params)}
		if t.Node != nil {
			info.Node = &NodeInfo{Parameters: params.Infos(t.Node.Params), GroupParameters: params.Infos(t.Node.GroupParams)}
		}
		if t.LB != nil {
			info.LoadBalancer = &LBInfo{ClassParameters: params.Infos(t.LB.Params)}
		}
		if t.DNS != nil {
			info.DNS = &DNSInfo{ZoneParameters: params.Infos(t.DNS.Params)}
		}
		out = append(out, info)
	}
	return out
}

type providerInfo struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	Node         *nodeInfo   `json:"node,omitempty"`
	TokenChecked bool        `json:"tokenChecked"`
	Classes      []classInfo `json:"classes,omitempty"`
	Zones        []zoneInfo  `json:"zones,omitempty"`
}

type zoneInfo struct {
	Zone         string         `json:"zone"`
	Provider     string         `json:"provider"`
	Type         string         `json:"type"`
	Endpoint     string         `json:"endpoint"`
	Parameters   map[string]any `json:"parameters"`
	TokenChecked bool           `json:"tokenChecked"`
}

func describeZones(zones []*ServedZone) []zoneInfo {
	out := make([]zoneInfo, 0, len(zones))
	for _, z := range zones {
		out = append(out, zoneInfo{
			Zone: z.Name, Provider: z.ProviderName, Type: z.Type, Endpoint: z.Endpoint,
			Parameters: z.Published, TokenChecked: z.Token != "",
		})
	}
	slices.SortFunc(out, func(a, b zoneInfo) int { return strings.Compare(a.Zone, b.Zone) })
	return out
}

type nodeInfo struct {
	Endpoint   string         `json:"endpoint"`
	Groups     []groupInfo    `json:"groups"`
	Parameters map[string]any `json:"parameters"`
}

type groupInfo struct {
	Name       string         `json:"name"`
	Max        int            `json:"max,omitempty"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

type classInfo struct {
	Class        string         `json:"class"`
	Provider     string         `json:"provider"`
	Type         string         `json:"type"`
	Endpoint     string         `json:"endpoint"`
	Internal     bool           `json:"internal"`
	Ports        string         `json:"ports,omitempty"`
	Parameters   map[string]any `json:"parameters"`
	TokenChecked bool           `json:"tokenChecked"`
}

func describeClasses(classes []*ServedClass) []classInfo {
	out := make([]classInfo, 0, len(classes))
	for _, c := range classes {
		out = append(out, classInfo{
			Class: c.Name, Provider: c.ProviderName, Type: c.Type, Endpoint: c.Endpoint, Internal: c.Caps.Internal,
			Ports: c.Caps.Ports, Parameters: c.Published, TokenChecked: c.Token != "",
		})
	}
	slices.SortFunc(out, func(a, b classInfo) int { return strings.Compare(a.Class, b.Class) })
	return out
}

func describeProviders(rt *Runtime) []providerInfo {
	byName := map[string]*providerInfo{}
	var order []string
	get := func(name, typ string, token bool) *providerInfo {
		if p, ok := byName[name]; ok {
			return p
		}
		p := &providerInfo{Name: name, Type: typ, TokenChecked: token}
		byName[name] = p
		order = append(order, name)
		return p
	}
	for _, n := range rt.Nodes {
		p := get(n.Name, n.Type, n.Token != "")
		ni := &nodeInfo{Endpoint: n.Endpoint, Groups: []groupInfo{}, Parameters: n.Parameters}
		for _, g := range n.Groups {
			ni.Groups = append(ni.Groups, groupInfo{Name: g.Name, Max: g.Max, Parameters: g.Parameters})
		}
		p.Node = ni
	}
	for _, c := range describeClasses(rt.Classes) {
		p := get(c.Provider, c.Type, c.TokenChecked)
		p.Classes = append(p.Classes, c)
	}
	for _, z := range describeZones(rt.Zones) {
		p := get(z.Provider, z.Type, z.TokenChecked)
		p.Zones = append(p.Zones, z)
	}
	slices.Sort(order)
	out := make([]providerInfo, 0, len(order))
	for _, name := range order {
		out = append(out, *byName[name])
	}
	return out
}
