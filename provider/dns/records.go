// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dns

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/bacanoo-io/norca-platform/provider/wire"
)

// The records of a zone: the second right of the contract, for norca-dns.
// A zone serves them only when its configuration says records = true and
// its type can write them (RecordProvider):
//
//	GET    {endpoint}/v1/records/{name}          the record sets of the name
//	PUT    {endpoint}/v1/records/{name}/{type}   set the values of the name and type (idempotent)
//	DELETE {endpoint}/v1/records/{name}/{type}   remove the record set
//
// A record set is every value of one name and type. The challenge names of
// the TXT contract are not written through here: they belong to norca-acme.

// The record types of the contract.
const (
	TypeA     = "A"
	TypeAAAA  = "AAAA"
	TypeCNAME = "CNAME"
	TypeTXT   = "TXT"
)

// RecordTypes are the types a record set can have.
var RecordTypes = []string{TypeA, TypeAAAA, TypeCNAME, TypeTXT}

// The TTL of a record set when a request names none, its bounds, and the
// most values one set may hold.
const (
	DefaultRecordTTL = 300
	MaxRecordTTL     = 86400
	MaxRecordValues  = 32
)

// RecordSet is every value of one name and type.
type RecordSet struct {
	// Name is the fully qualified name, normalized.
	Name string `json:"name"`

	// Type is one of RecordTypes.
	Type string `json:"type"`

	// TTL is the time to live in seconds.
	TTL int `json:"ttl"`

	// Values are the values: addresses for A and AAAA, one name for CNAME
	// (normalized, without the dot at the end), texts for TXT.
	Values []string `json:"values"`
}

// RecordProvider is implemented by the providers that can write records.
// All methods must be safe for concurrent use; calls for the same name are
// serialized by the handler.
type RecordProvider interface {
	// Records returns the record sets of a name of the types of the
	// contract, as the platform holds them now; none is an empty list.
	Records(ctx context.Context, name string) ([]RecordSet, error)

	// SetRecords makes the values of the set the only values of its name
	// and type, at once if the platform can (no moment without a record).
	// It must be idempotent. An error wrapping ErrInvalid means the request
	// can't be satisfied.
	SetRecords(ctx context.Context, set RecordSet) error

	// DeleteRecords removes every value of the name and type. Removing what
	// is not there is not an error.
	DeleteRecords(ctx context.Context, name, recordType string) error
}

// CheckRecordName checks the name of a record set against the zone: a name
// of the zone, not a challenge name of the TXT contract.
func CheckRecordName(zone, name string) error {
	if !ValidName(name) {
		return fmt.Errorf("invalid name %q", name)
	}
	if !InZone(name, zone) {
		return fmt.Errorf("the name %q is not in the zone %s", name, zone)
	}
	if name == ChallengeLabel+"."+zone || strings.HasPrefix(name, ChallengeLabel+".") {
		return fmt.Errorf("the name %q is a challenge name: those are written through the TXT contract", name)
	}
	return nil
}

// CheckRecordSet checks a record set against the zone and returns it with
// its defaults and its values normalized and sorted.
func CheckRecordSet(zone string, set RecordSet) (RecordSet, error) {
	set.Name = Normalize(set.Name)
	set.Type = strings.ToUpper(strings.TrimSpace(set.Type))
	if err := CheckRecordName(zone, set.Name); err != nil {
		return set, err
	}
	if !slices.Contains(RecordTypes, set.Type) {
		return set, fmt.Errorf("unknown record type %q (one of %s)", set.Type, strings.Join(RecordTypes, ", "))
	}
	if set.TTL == 0 {
		set.TTL = DefaultRecordTTL
	}
	if set.TTL < MinTTL || set.TTL > MaxRecordTTL {
		return set, fmt.Errorf("the TTL must be %d to %d seconds", MinTTL, MaxRecordTTL)
	}
	if len(set.Values) == 0 || len(set.Values) > MaxRecordValues {
		return set, fmt.Errorf("a record set has 1 to %d values", MaxRecordValues)
	}
	if set.Type == TypeCNAME {
		if len(set.Values) != 1 {
			return set, fmt.Errorf("a CNAME has one value")
		}
		if set.Name == zone {
			return set, fmt.Errorf("the apex of a zone can't be a CNAME")
		}
	}
	values := make([]string, 0, len(set.Values))
	for _, v := range set.Values {
		nv, err := normalizeValue(set.Type, v)
		if err != nil {
			return set, err
		}
		if !slices.Contains(values, nv) {
			values = append(values, nv)
		}
	}
	slices.Sort(values)
	set.Values = values
	return set, nil
}

// normalizeValue checks one value of a type and writes it as the contract
// compares it.
func normalizeValue(recordType, v string) (string, error) {
	v = strings.TrimSpace(v)
	switch recordType {
	case TypeA:
		ip := net.ParseIP(v)
		if ip == nil || ip.To4() == nil {
			return "", fmt.Errorf("%q is not an IPv4 address", v)
		}
		return ip.To4().String(), nil
	case TypeAAAA:
		ip := net.ParseIP(v)
		if ip == nil || ip.To4() != nil {
			return "", fmt.Errorf("%q is not an IPv6 address", v)
		}
		return ip.String(), nil
	case TypeCNAME:
		n := Normalize(v)
		if !ValidName(n) {
			return "", fmt.Errorf("%q is not a DNS name", v)
		}
		return n, nil
	default:
		if v == "" || len(v) > 255 {
			return "", fmt.Errorf("a TXT value has 1 to 255 characters")
		}
		for _, r := range v {
			if r < 0x20 || r > 0x7e || r == '"' || r == '\\' {
				return "", fmt.Errorf("a TXT value is printable ASCII without quotes and backslashes")
			}
		}
		return v, nil
	}
}

// SameRecords reports whether two record sets have the same values and TTL.
// Both are checked sets (normalized and sorted).
func SameRecords(a, b RecordSet) bool {
	return a.TTL == b.TTL && slices.Equal(a.Values, b.Values)
}

// RecordSetRequest is the body of a PUT of a record set.
type RecordSetRequest struct {
	Values []string `json:"values"`
	TTL    int      `json:"ttl,omitempty"`
}

// Records returns the record sets of a name.
func (c *Client) Records(ctx context.Context, name string) ([]RecordSet, error) {
	var out []RecordSet
	err := c.call(ctx, http.MethodGet, "/v1/records/"+url.PathEscape(name), nil, &out)
	return out, err
}

// SetRecords sets the values of a name and type.
func (c *Client) SetRecords(ctx context.Context, set RecordSet) error {
	body := RecordSetRequest{Values: set.Values, TTL: set.TTL}
	return c.call(ctx, http.MethodPut, "/v1/records/"+url.PathEscape(set.Name)+"/"+url.PathEscape(set.Type), body, nil)
}

// DeleteRecords removes the record set of a name and type.
func (c *Client) DeleteRecords(ctx context.Context, name, recordType string) error {
	return c.call(ctx, http.MethodDelete, "/v1/records/"+url.PathEscape(name)+"/"+url.PathEscape(recordType), nil, nil)
}

// call sends one request of the records contract.
func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.Endpoint, "/")+path, body) //nolint:gosec // G704: the endpoint of the registered zone
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	hc := c.HTTP
	if hc == nil {
		hc = defaultHTTP
	}
	resp, err := hc.Do(req) //nolint:gosec // G704: the endpoint of the registered zone
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		e := &ResponseError{Status: resp.StatusCode, Message: snippet(data)}
		var we wire.Error
		if json.Unmarshal(data, &we) == nil && we.Message != "" {
			e.Reason, e.Message = we.Reason, we.Message
		}
		return e
	}
	if out != nil && len(bytes.TrimSpace(data)) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}
