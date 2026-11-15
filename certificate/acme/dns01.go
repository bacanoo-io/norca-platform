// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	mdns "github.com/miekg/dns"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca/norca/api"
)

// zoneSpec is the registration of a DNS zone (a norca.io/DNSZone object of
// norca-provider).
type zoneSpec struct {
	Zone          string `json:"zone"`
	Provider      string `json:"provider"`
	Endpoint      string `json:"endpoint"`
	TokenVariable string `json:"tokenVariable"`
}

// zoneInfo is a registered zone.
type zoneInfo struct {
	Name      string
	Spec      zoneSpec
	Heartbeat string
}

// available reports whether the provider of the zone is alive: a zone
// without a heartbeat is a static registration and always available.
func (z *zoneInfo) available(now time.Time, ttl time.Duration) bool {
	if z.Heartbeat == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339, z.Heartbeat)
	return err == nil && now.Sub(t) <= ttl
}

// zones reads the DNSZone objects and calls their providers. The list is
// cached for a few seconds, so the names of one order make one call.
type zones struct {
	cluster cluster

	// ttl is how old the heartbeat of a zone may be.
	ttl time.Duration

	// tokenNamespace is the namespace of the token Variables.
	tokenNamespace string

	// allowHTTP permits http endpoints (development and tests).
	allowHTTP bool

	client   *http.Client
	now      func() time.Time
	cacheFor time.Duration

	mu     sync.Mutex
	cached []*zoneInfo
	at     time.Time
}

func (z *zones) list(ctx context.Context) ([]*zoneInfo, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	if z.cached != nil && z.now().Sub(z.at) < z.cacheFor {
		return z.cached, nil
	}
	objs, err := z.cluster.ListObjects(ctx, ZoneKind)
	if err != nil {
		return nil, fmt.Errorf("listing the DNS zones: %w", err)
	}
	out := make([]*zoneInfo, 0, len(objs))
	for _, o := range objs {
		info := &zoneInfo{Name: dns.Normalize(o.Name)}
		if err := decode(o.Spec, &info.Spec); err != nil {
			continue
		}
		if zone := dns.Normalize(info.Spec.Zone); zone != "" {
			info.Name = zone
		}
		var status struct {
			Heartbeat string `json:"heartbeat"`
		}
		_ = decode(o.Status, &status)
		info.Heartbeat = status.Heartbeat
		out = append(out, info)
	}
	z.cached, z.at = out, z.now()
	return out, nil
}

// errZoneAway says that the zone of a name is registered and its provider
// is not alive: the name can be proven when the provider is back.
var errZoneAway = errors.New("the provider of the DNS zone reports no heartbeat")

// find returns the zone a DNS name is proven in: the longest zone that
// holds the name and is one of the allowed zones (none: every zone). It
// returns nil if there is none, and errZoneAway if the provider of that
// zone is not alive.
func (z *zones) find(ctx context.Context, domain string, allowed []string) (*zoneInfo, error) {
	all, err := z.list(ctx)
	if err != nil {
		return nil, err
	}
	name := strings.TrimPrefix(dns.Normalize(domain), "*.")
	var best *zoneInfo
	for _, zone := range all {
		if !dns.InZone(name, zone.Name) {
			continue
		}
		if len(allowed) > 0 && !slices.ContainsFunc(allowed, func(a string) bool { return dns.Normalize(a) == zone.Name }) {
			continue
		}
		if best == nil || len(zone.Name) > len(best.Name) {
			best = zone
		}
	}
	if best != nil && !best.available(z.now(), z.ttl) {
		return nil, fmt.Errorf("%w: zone %s, last heartbeat %q", errZoneAway, best.Name, best.Heartbeat)
	}
	return best, nil
}

// byName returns a registered zone.
func (z *zones) byName(ctx context.Context, name string) (*zoneInfo, error) {
	all, err := z.list(ctx)
	if err != nil {
		return nil, err
	}
	for _, zone := range all {
		if zone.Name == name {
			return zone, nil
		}
	}
	return nil, fmt.Errorf("the DNS zone %s is not registered", name)
}

// provider returns the client of the provider of a zone, with its token.
func (z *zones) provider(ctx context.Context, zone *zoneInfo) (*dns.Client, error) {
	u, err := url.Parse(zone.Spec.Endpoint)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("zone %s: invalid endpoint %q", zone.Name, zone.Spec.Endpoint)
	}
	if u.Scheme != "https" && !(z.allowHTTP && u.Scheme == "http") {
		return nil, fmt.Errorf("zone %s: the endpoint %q must be an https URL", zone.Name, zone.Spec.Endpoint)
	}
	client := &dns.Client{Endpoint: zone.Spec.Endpoint, HTTP: z.client}
	if zone.Spec.TokenVariable != "" {
		v, err := z.cluster.ReadVariable(ctx, z.tokenNamespace, zone.Spec.TokenVariable)
		if err != nil {
			return nil, fmt.Errorf("zone %s: reading the token Variable %s: %w", zone.Name, zone.Spec.TokenVariable, err)
		}
		if client.Token = strings.TrimSpace(v.Items["token"]); client.Token == "" {
			return nil, fmt.Errorf("zone %s: the Variable %s has no item token", zone.Name, zone.Spec.TokenVariable)
		}
	}
	return client, nil
}

// present adds a challenge record through the provider of its zone.
func (z *zones) present(ctx context.Context, rec PresentedRecord) error {
	zone, err := z.byName(ctx, rec.Zone)
	if err != nil {
		return err
	}
	client, err := z.provider(ctx, zone)
	if err != nil {
		return err
	}
	if err := client.Present(ctx, dns.Record{Name: rec.Name, Value: rec.Value}); err != nil {
		return fmt.Errorf("zone %s: %w", rec.Zone, err)
	}
	return nil
}

// cleanUp removes a challenge record.
func (z *zones) cleanUp(ctx context.Context, rec PresentedRecord) error {
	zone, err := z.byName(ctx, rec.Zone)
	if err != nil {
		return err
	}
	client, err := z.provider(ctx, zone)
	if err != nil {
		return err
	}
	if err := client.CleanUp(ctx, dns.Record{Name: rec.Name, Value: rec.Value}); err != nil {
		return fmt.Errorf("zone %s: %w", rec.Zone, err)
	}
	return nil
}

// propagation says whether a challenge record can be seen where the
// certificate authority will look for it.
type propagation interface {
	// visible reports whether every name server of the zone answers the
	// name with the value.
	visible(ctx context.Context, zone, name, value string) (bool, error)
}

// nameServers checks the authoritative name servers of a zone, which it
// finds through a recursive resolver.
type nameServers struct {
	// resolver is the address (host:port) of the recursive resolver.
	resolver string
	timeout  time.Duration
}

// newNameServers returns the check with the resolver; empty takes the
// first one of /etc/resolv.conf.
func newNameServers(resolver string) (*nameServers, error) {
	if resolver == "" {
		conf, err := mdns.ClientConfigFromFile("/etc/resolv.conf")
		if err != nil || len(conf.Servers) == 0 {
			return nil, errors.New("no DNS resolver: /etc/resolv.conf names none, set -dns-resolver")
		}
		resolver = net.JoinHostPort(conf.Servers[0], conf.Port)
	} else if _, _, err := net.SplitHostPort(resolver); err != nil {
		resolver = net.JoinHostPort(strings.Trim(resolver, "[]"), "53")
	}
	return &nameServers{resolver: resolver, timeout: 5 * time.Second}, nil
}

func (n *nameServers) query(ctx context.Context, server, name string, qtype uint16, recurse bool) (*mdns.Msg, error) {
	msg := new(mdns.Msg)
	msg.SetQuestion(mdns.Fqdn(name), qtype)
	msg.RecursionDesired = recurse
	client := &mdns.Client{Timeout: n.timeout}
	reply, _, err := client.ExchangeContext(ctx, msg, server)
	if err == nil && reply.Truncated {
		client.Net = "tcp"
		reply, _, err = client.ExchangeContext(ctx, msg, server)
	}
	return reply, err
}

func (n *nameServers) visible(ctx context.Context, zone, name, value string) (bool, error) {
	reply, err := n.query(ctx, n.resolver, zone, mdns.TypeNS, true)
	if err != nil {
		return false, fmt.Errorf("the name servers of %s: %w", zone, err)
	}
	var servers []string
	for _, rr := range reply.Answer {
		if ns, ok := rr.(*mdns.NS); ok {
			servers = append(servers, ns.Ns)
		}
	}
	if len(servers) == 0 {
		return false, fmt.Errorf("the resolver %s knows no name server of %s", n.resolver, zone)
	}
	for _, server := range servers {
		addrs, err := n.addresses(ctx, server)
		if err != nil {
			return false, err
		}
		answer, err := n.query(ctx, net.JoinHostPort(addrs[0], "53"), name, mdns.TypeTXT, false)
		if err != nil {
			return false, fmt.Errorf("the name server %s: %w", server, err)
		}
		found := slices.ContainsFunc(answer.Answer, func(rr mdns.RR) bool {
			txt, ok := rr.(*mdns.TXT)
			return ok && strings.Join(txt.Txt, "") == value
		})
		if !found {
			return false, nil
		}
	}
	return true, nil
}

// addresses resolves a name server through the resolver.
func (n *nameServers) addresses(ctx context.Context, host string) ([]string, error) {
	var out []string
	for _, qtype := range []uint16{mdns.TypeA, mdns.TypeAAAA} {
		reply, err := n.query(ctx, n.resolver, host, qtype, true)
		if err != nil {
			continue
		}
		for _, rr := range reply.Answer {
			switch a := rr.(type) {
			case *mdns.A:
				out = append(out, a.A.String())
			case *mdns.AAAA:
				out = append(out, a.AAAA.String())
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("the name server %s has no address at the resolver %s", host, n.resolver)
	}
	return out, nil
}

// cluster is the part of the Norca API the operator uses for objects and
// Variables.
type cluster interface {
	// ListObjects lists the objects of a kind in every namespace.
	ListObjects(ctx context.Context, kind kindRef) ([]*api.CustomObject, error)

	// UpdateStatus replaces the status of a cluster scoped object, if it
	// still has the modify index; a conflict is errConflict, a missing
	// object errGone.
	UpdateStatus(ctx context.Context, kind kindRef, name string, status map[string]any, modifyIndex uint64) error

	// ReadVariable reads a Variable.
	ReadVariable(ctx context.Context, namespace, path string) (*api.Variable, error)
}
