// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package dns is the contract between an operator that proves the control
// of a DNS name (norca-acme, for the DNS-01 challenge of ACME) and a DNS
// provider: an application outside the core that can write TXT records in
// a zone. The operator calls the provider over HTTP:
//
//	PUT    {endpoint}/v1/txt/{name}   add a value to the TXT records of the name (idempotent)
//	DELETE {endpoint}/v1/txt/{name}   remove that value
//
// The endpoint is the one of the DNSZone object of the zone; norca-provider
// serves every zone at {advertise_url}/zones/{zone}. A platform implements
// Provider; NewHandler serves one zone and Client is the operator's side.
// Failures have the shape of package wire, and the operator sends a bearer
// token, as in the node and the load balancer contract.
//
// The contract is deliberately narrow. A name is the name of a challenge,
// "_acme-challenge.<name in the zone>": nothing else can be written, so the
// token of a zone cannot change what the zone serves, unless the zone is
// configured with records = true: then the same token also covers the
// records contract (records.go), which writes the A, AAAA, CNAME and TXT
// record sets of the names of the zone. A name holds several values at
// once: a certificate for example.com and *.example.com has two challenges
// with the same name.
package dns

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/bacanoo-io/norca-platform/provider/params"
)

// ChallengeLabel is the first label of every name of the contract.
const ChallengeLabel = "_acme-challenge"

// The TTL of a record when a request names none, and its bounds.
const (
	DefaultTTL = 60
	MinTTL     = 1
	MaxTTL     = 3600
)

// Record is a value of the TXT records of a name.
type Record struct {
	// Name is the fully qualified name of the record, lower case, without
	// the dot at the end: "_acme-challenge.www.example.com".
	Name string

	// Value is the text of the record, as it is: the provider quotes it for
	// its platform.
	Value string

	// TTL is the time to live of the record in seconds.
	TTL int
}

// Provider is an instance of a type: it writes the TXT records of one zone.
// All methods must be safe for concurrent use; calls for the same name are
// serialized by the handler.
type Provider interface {
	// Present makes the value one of the TXT records of the name, next to
	// the values that are there already. It must be idempotent, and it
	// returns when the platform has accepted the change: the operator waits
	// for the name servers itself. An error wrapping ErrInvalid means the
	// request cannot be satisfied (the operator does not retry it with the
	// same input); any other error is a failure to try again.
	Present(ctx context.Context, rec Record) error

	// CleanUp removes the value from the TXT records of the name and leaves
	// the other values. Removing what is not there is not an error.
	CleanUp(ctx context.Context, rec Record) error
}

// Closer is implemented by providers that hold resources.
type Closer interface {
	Close() error
}

// ErrInvalid marks a request a provider cannot satisfy.
var ErrInvalid = errors.New("invalid request")

// Invalid returns an error wrapping ErrInvalid.
func Invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Config is the input of the constructor of a type.
type Config struct {
	// Provider is the name of the provider (the entry of the
	// configuration) the zone belongs to.
	Provider string

	// Zone is the name of the zone.
	Zone string

	// Values are the resolved parameters: the shared ones of the type and
	// those of the zone.
	Values params.Values

	// Logger is the logger for the instance; may be nil.
	Logger *slog.Logger
}

// Log returns the configured logger, or slog.Default() if none was set.
func (c Config) Log() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// Zone is a configured zone: a provider instance with its parameters.
type Zone struct {
	// Name is the name of the zone, lower case, without the dot at the end.
	Name string

	// Type is the ID of the type that serves the zone.
	Type string

	Provider Provider

	// Values are the parameters of the zone as configured (shared and zone
	// parameters).
	Values params.Values

	// Published are the parameters written to the DNSZone object.
	Published map[string]any

	// Records serves the records contract for the zone (records.go): the
	// configuration of the zone allows it and the provider is a
	// RecordProvider.
	Records bool
}

// Close releases the provider if it holds resources.
func (z *Zone) Close() error {
	if cl, ok := z.Provider.(Closer); ok {
		return cl.Close()
	}
	return nil
}

var validLabel = regexp.MustCompile(`^[a-z0-9_]([a-z0-9_-]{0,61}[a-z0-9_])?$`)

// ValidName reports whether name is a DNS name as the contract writes them:
// lower case labels of letters, digits, hyphens and underscores, no dot at
// the end, at most 253 characters.
func ValidName(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if !validLabel.MatchString(label) {
			return false
		}
	}
	return true
}

// ValidZone reports whether name can be the name of a zone: a valid name
// without underscores.
func ValidZone(name string) bool {
	return ValidName(name) && !strings.Contains(name, "_")
}

// Normalize writes a name as the contract does: lower case, without the dot
// at the end.
func Normalize(name string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(name), "."))
}

// InZone reports whether a name is the zone itself or a name below it. Both
// are normalized names.
func InZone(name, zone string) bool {
	return name == zone || strings.HasSuffix(name, "."+zone)
}

// ChallengeName returns the name of the challenge record of a DNS name: a
// wildcard is proven at the name it stands under.
func ChallengeName(domain string) string {
	return ChallengeLabel + "." + strings.TrimPrefix(Normalize(domain), "*.")
}

// CheckRecord checks a record against the zone: the name is a challenge
// name of the zone, the value is a text of one string, the TTL is in its
// bounds. It returns the record with its defaults.
func CheckRecord(zone string, rec Record) (Record, error) {
	if !ValidName(rec.Name) {
		return rec, fmt.Errorf("invalid name %q", rec.Name)
	}
	rest, ok := strings.CutPrefix(rec.Name, ChallengeLabel+".")
	if !ok {
		return rec, fmt.Errorf("the name %q is not a challenge name (%s.<name>)", rec.Name, ChallengeLabel)
	}
	if !InZone(rest, zone) {
		return rec, fmt.Errorf("the name %q is not in the zone %s", rec.Name, zone)
	}
	if rec.Value == "" || len(rec.Value) > 255 {
		return rec, fmt.Errorf("the value must have 1 to 255 characters")
	}
	for _, r := range rec.Value {
		if r < 0x20 || r > 0x7e || r == '"' || r == '\\' {
			return rec, fmt.Errorf("the value must be printable ASCII without quotes and backslashes")
		}
	}
	if rec.TTL == 0 {
		rec.TTL = DefaultTTL
	}
	if rec.TTL < MinTTL || rec.TTL > MaxTTL {
		return rec, fmt.Errorf("the TTL must be %d to %d seconds", MinTTL, MaxTTL)
	}
	return rec, nil
}

// The bodies of the HTTP contract.
type (
	// RecordRequest is the body of a PUT and of a DELETE.
	RecordRequest struct {
		Value string `json:"value"`
		TTL   int    `json:"ttl,omitempty"`
	}
)
