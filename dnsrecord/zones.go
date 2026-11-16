// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca/norca/controller"
)

// The registered zones, read from the DNSZone objects of norca-provider. The
// lookup is the one norca-acme does for its challenges, with the records
// switch of the zone.

// zoneSpec is the spec of a DNSZone object.
type zoneSpec struct {
	Zone          string `json:"zone"`
	Provider      string `json:"provider"`
	Endpoint      string `json:"endpoint"`
	TokenVariable string `json:"tokenVariable"`
	Records       bool   `json:"records"`
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

// The problems of a zone, for the conditions.
var (
	errNoZone          = errors.New("no registered DNS zone holds the name")
	errRecordsDisabled = errors.New("the zone does not let norca-dns write records")
	errZoneAway        = errors.New("the provider of the DNS zone reports no heartbeat")
)

// zones reads the registered zones and calls their providers.
type zones struct {
	backend controller.Backend

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
	objs, _, err := z.backend.ListObjects(ctx, ZoneKind, "*", 0)
	if err != nil {
		return nil, fmt.Errorf("listing the DNS zones: %w", err)
	}
	out := make([]*zoneInfo, 0, len(objs))
	for _, o := range objs {
		info := &zoneInfo{Name: dns.Normalize(o.Name)}
		if decode(o.Spec, &info.Spec) != nil {
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

// find returns the zone of a name: the longest registered zone that holds
// it. Its error says why the zone can't be written now.
func (z *zones) find(ctx context.Context, name string) (*zoneInfo, error) {
	all, err := z.list(ctx)
	if err != nil {
		return nil, err
	}
	var best *zoneInfo
	for _, zone := range all {
		if dns.InZone(name, zone.Name) && (best == nil || len(zone.Name) > len(best.Name)) {
			best = zone
		}
	}
	switch {
	case best == nil:
		return nil, errNoZone
	case !best.Spec.Records:
		return best, fmt.Errorf("%w: zone %s (records = true in the configuration of norca-provider)", errRecordsDisabled, best.Name)
	case !best.available(z.now(), z.ttl):
		return best, fmt.Errorf("%w: zone %s, last heartbeat %q", errZoneAway, best.Name, best.Heartbeat)
	}
	return best, nil
}

// byName returns a registered zone, or nil.
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
	return nil, nil
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
		v, err := z.backend.ReadVariable(ctx, z.tokenNamespace, zone.Spec.TokenVariable)
		if err != nil {
			return nil, fmt.Errorf("zone %s: reading the token Variable %s: %w", zone.Name, zone.Spec.TokenVariable, err)
		}
		if client.Token = strings.TrimSpace(v.Items["token"]); client.Token == "" {
			return nil, fmt.Errorf("zone %s: the Variable %s has no item token", zone.Name, zone.Spec.TokenVariable)
		}
	}
	return client, nil
}

// decode decodes a part of an object.
func decode(in map[string]any, out any) error {
	if len(in) == 0 {
		return nil
	}
	buf, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, out)
}
