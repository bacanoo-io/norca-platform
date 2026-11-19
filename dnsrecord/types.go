// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package dnsrecord is norca-dns, the controller of DNSRecord objects: DNS
// names that follow what runs in the cluster. It keeps the record set of
// each object (a name, a type, its values) in a zone of norca-provider,
// through the records contract of package provider/dns.
//
// What a namespace may name is its allow list, the namespace metadata
// norca.io/dns-suffixes (written by a Tenant or by a management token). The
// operator writes only zones whose DNSZone object says records = true, and
// changes only the record sets it wrote: a name that has records of the type
// it did not write is left alone (Conflict) unless the object says adopt,
// also when they have the values the object asks for.
// One cluster writes a zone; the zone carries no marker of what is whose.
package dnsrecord

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca/norca/controller"
)

// The kinds the operator works with.
var (
	// Kind is the kind of the records (namespaced, registered with the
	// norca-dns pack: dnsrecord.kind.hcl).
	Kind = controller.Kind{Group: "norca.io", Kind: "DNSRecord"}

	// ZoneKind is the kind of the zones of norca-provider (cluster scoped).
	ZoneKind = controller.Kind{Group: "norca.io", Kind: "DNSZone"}

	// LoadBalancerKind is the kind of the load balancers of the
	// LoadBalancer operator, a source of addresses.
	LoadBalancerKind = controller.Kind{Group: "norca.io", Kind: "LoadBalancer"}
)

// Finalizer keeps a record until the operator has removed its record set
// from the zone.
const Finalizer = "norca.io/dns-records"

// MetaDNSSuffixes is the namespace metadata with the DNS suffixes the
// namespace may write names under, comma separated. A suffix allows itself
// and every name below it.
const MetaDNSSuffixes = "norca.io/dns-suffixes"

// ConditionConflict is true while another record or the zone itself holds
// the name and type.
const ConditionConflict = "Conflict"

// The deletion policies of a record: what its deletion does with the record
// set the operator holds for it.
const (
	// DeletionDelete removes the record set; the record stays until that
	// succeeds. The default.
	DeletionDelete = "delete"

	// DeletionRetain leaves the record set in the zone: the record goes at
	// once. It is the way out of a deletion that can't remove the set.
	DeletionRetain = "retain"
)

// Spec is the spec of a DNSRecord object.
type Spec struct {
	// Name is the fully qualified name of the record set.
	Name string `json:"name"`

	// Type is one of dns.RecordTypes.
	Type string `json:"type"`

	// TTL is the time to live in seconds; 0 is dns.DefaultRecordTTL.
	TTL int `json:"ttl,omitempty"`

	// Values are the values, unless they come from Source.
	Values []string `json:"values,omitempty"`

	// Source takes the values from an object of the cluster.
	Source *Source `json:"source,omitempty"`

	// Adopt takes over a record set of the name and type that the operator
	// did not write.
	Adopt bool `json:"adopt,omitempty"`

	// DeletionPolicy is DeletionDelete (also when empty) or DeletionRetain.
	DeletionPolicy string `json:"deletion_policy,omitempty"`
}

// Source takes the values of a record set from an object of the cluster.
type Source struct {
	// LoadBalancer is a LoadBalancer object of the namespace: its frontend
	// addresses (status.addresses) are the values (A: its IPv4 addresses,
	// AAAA: its IPv6 addresses, CNAME: its first host name).
	LoadBalancer string `json:"load_balancer,omitempty"`
}

// Status is the status of a DNSRecord object.
type Status struct {
	controller.CommonStatus

	// Zone, Name, Type, TTL and Values are the record set the operator
	// wrote last, or found as it wanted it.
	Zone   string   `json:"zone,omitempty"`
	Name   string   `json:"name,omitempty"`
	Type   string   `json:"type,omitempty"`
	TTL    int      `json:"ttl,omitempty"`
	Values []string `json:"values,omitempty"`

	// Owned says the record set at Name and Type is the operator's: it
	// wrote or adopted it. Only an owned set is changed or removed.
	Owned bool `json:"owned,omitempty"`

	// Pending is the record set the operator is about to write, recorded
	// before the write: a set found in the zone exactly as it says is the
	// record's, also when the status write after the zone write was lost.
	// It is cleared when the status records the set as owned, or when the
	// write was not done.
	Pending *PendingSet `json:"pending,omitempty"`

	// WrittenAt is when the operator last wrote the set (RFC 3339).
	WrittenAt string `json:"writtenAt,omitempty"`

	// Drifts counts the changes made outside the operator that it set
	// back; LastDrift is the time of the last one.
	Drifts    int    `json:"drifts,omitempty"`
	LastDrift string `json:"lastDrift,omitempty"`
}

// PendingSet is a record set the operator is about to write, or holds.
type PendingSet struct {
	Zone   string   `json:"zone"`
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	TTL    int      `json:"ttl"`
	Values []string `json:"values"`
}

// normalize checks the spec and writes its name and type as the contract
// does.
func (s *Spec) normalize() error {
	s.Name = dns.Normalize(s.Name)
	s.Type = strings.ToUpper(strings.TrimSpace(s.Type))
	if !dns.ValidName(s.Name) || strings.HasPrefix(s.Name, "*.") {
		return fmt.Errorf("name %q is not a DNS name", s.Name)
	}
	if !slices.Contains(dns.RecordTypes, s.Type) {
		return fmt.Errorf("type %q is not one of %s", s.Type, strings.Join(dns.RecordTypes, ", "))
	}
	if s.TTL < 0 || s.TTL > dns.MaxRecordTTL {
		return fmt.Errorf("ttl must be 0 (the default) to %d seconds", dns.MaxRecordTTL)
	}
	if s.DeletionPolicy != "" && s.DeletionPolicy != DeletionDelete && s.DeletionPolicy != DeletionRetain {
		return fmt.Errorf("deletion_policy %q is not %q or %q", s.DeletionPolicy, DeletionDelete, DeletionRetain)
	}
	hasSource := s.Source != nil && s.Source.LoadBalancer != ""
	switch {
	case hasSource && len(s.Values) > 0:
		return errors.New("values and source exclude each other")
	case !hasSource && len(s.Values) == 0:
		return errors.New("values or source is needed")
	case hasSource && s.Type == dns.TypeTXT:
		return errors.New("a TXT record takes its values from values, not from a source")
	}
	return nil
}

// allowed reports whether a name is in the allow list of a namespace.
func allowed(name string, meta map[string]string) bool {
	for suffix := range strings.SplitSeq(meta[MetaDNSSuffixes], ",") {
		suffix = dns.Normalize(strings.TrimPrefix(strings.TrimSpace(suffix), "*."))
		if suffix != "" && dns.InZone(name, suffix) {
			return true
		}
	}
	return false
}

// loadBalancerValues returns the values a load balancer gives a record type
// from its frontend addresses ("ip:port", "host:port" or without a port).
func loadBalancerValues(recordType string, addresses []string) []string {
	var out []string
	for _, a := range addresses {
		host := a
		if h, _, err := net.SplitHostPort(a); err == nil {
			host = h
		}
		ip := net.ParseIP(host)
		switch {
		case recordType == dns.TypeA && ip != nil && ip.To4() != nil:
			out = append(out, ip.To4().String())
		case recordType == dns.TypeAAAA && ip != nil && ip.To4() == nil:
			out = append(out, ip.String())
		case recordType == dns.TypeCNAME && ip == nil && dns.ValidName(dns.Normalize(host)):
			return []string{dns.Normalize(host)}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}
