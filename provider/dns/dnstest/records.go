// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnstest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/dns"
)

// RecordsConfig describes a provider of the records contract under test.
type RecordsConfig struct {
	// New returns the provider of the zone under test; it must be a
	// dns.RecordProvider.
	New func(t *testing.T) dns.Provider

	// Zone is the name of the zone the provider serves.
	Zone string

	// Lookup returns the values of a name and type as the platform holds
	// them now, in any order. Without it the checks only see what the
	// calls answer.
	Lookup func(t *testing.T, name, recordType string) []string
}

// countedProvider counts the calls that reach a provider.
type countedProvider struct {
	dns.Provider
	records dns.RecordProvider
	calls   atomic.Int64
}

func (c *countedProvider) Present(ctx context.Context, rec dns.Record) error {
	c.calls.Add(1)
	return c.Provider.Present(ctx, rec)
}

func (c *countedProvider) CleanUp(ctx context.Context, rec dns.Record) error {
	c.calls.Add(1)
	return c.Provider.CleanUp(ctx, rec)
}

func (c *countedProvider) Records(ctx context.Context, name string) ([]dns.RecordSet, error) {
	c.calls.Add(1)
	return c.records.Records(ctx, name)
}

func (c *countedProvider) SetRecords(ctx context.Context, set dns.RecordSet) error {
	c.calls.Add(1)
	return c.records.SetRecords(ctx, set)
}

func (c *countedProvider) DeleteRecords(ctx context.Context, name, recordType string) error {
	c.calls.Add(1)
	return c.records.DeleteRecords(ctx, name, recordType)
}

// Close implements dns.Closer.
func (c *countedProvider) Close() error {
	if cl, ok := c.Provider.(dns.Closer); ok {
		return cl.Close()
	}
	return nil
}

// countedConditional is a countedProvider of a provider that can make a
// write depend on the record set: the wrapper keeps that ability, and only
// that of the provider.
type countedConditional struct {
	*countedProvider
	cond dns.ConditionalRecordProvider
}

func (c *countedConditional) SetRecordsIf(ctx context.Context, set dns.RecordSet, expect dns.Expect) error {
	c.calls.Add(1)
	return c.cond.SetRecordsIf(ctx, set, expect)
}

func (c *countedConditional) DeleteRecordsIf(ctx context.Context, name, recordType string, expect dns.Expect) error {
	c.calls.Add(1)
	return c.cond.DeleteRecordsIf(ctx, name, recordType, expect)
}

// RunRecords checks the records contract the way norca-dns relies on it:
//
//   - a record set is set, read back and set again without a change;
//   - setting it again with other values replaces them all, and the TTL;
//   - A, AAAA, CNAME and TXT records;
//   - a deletion removes the set, and deleting again is not an error;
//   - names outside the zone, challenge names and invalid values are
//     refused before the provider is called;
//   - the token is required;
//   - a zone without records = true refuses the contract;
//   - a conditional write (a dns.ConditionalRecordProvider) creates only a
//     set that is not there, replaces and deletes only a set with the
//     expected values, and answers 412 otherwise; a provider that can't
//     check a condition refuses it (501) and writes nothing.
func RunRecords(t *testing.T, cfg RecordsConfig) {
	t.Helper()
	if !dns.ValidZone(cfg.Zone) {
		t.Fatalf("dnstest: invalid zone %q", cfg.Zone)
	}
	// calls counts the calls that reached the provider of the last client;
	// conditional says whether it is a dns.ConditionalRecordProvider.
	var (
		calls       *atomic.Int64
		conditional bool
	)
	newClient := func(t *testing.T, token string, records bool) *dns.Client {
		t.Helper()
		p := cfg.New(t)
		rp, ok := p.(dns.RecordProvider)
		if !ok {
			t.Fatalf("dnstest: the provider is not a dns.RecordProvider")
		}
		counted := &countedProvider{Provider: p, records: rp}
		calls = &counted.calls
		var served dns.Provider = counted
		var cp dns.ConditionalRecordProvider
		if cp, conditional = p.(dns.ConditionalRecordProvider); conditional {
			served = &countedConditional{countedProvider: counted, cond: cp}
		}
		z := &dns.Zone{Name: cfg.Zone, Type: "test", Provider: served, Records: records}
		srv := httptest.NewServer(dns.NewHandler(z, token, nil))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { _ = z.Close() })
		return &dns.Client{Endpoint: srv.URL, Token: token}
	}
	ctx := func(t *testing.T) context.Context {
		c, cancel := context.WithTimeout(context.Background(), time.Minute)
		t.Cleanup(cancel)
		return c
	}
	held := func(t *testing.T, name, recordType string, want ...string) {
		t.Helper()
		if cfg.Lookup == nil {
			return
		}
		got := slices.Clone(cfg.Lookup(t, name, recordType))
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("the %s records of %s are %q, want %q", recordType, name, got, want)
		}
	}
	read := func(t *testing.T, c *dns.Client, name, recordType string) *dns.RecordSet {
		t.Helper()
		sets, err := c.Records(ctx(t), name)
		if err != nil {
			t.Fatalf("records of %s: %v", name, err)
		}
		for i := range sets {
			if sets[i].Type == recordType {
				slices.Sort(sets[i].Values)
				return &sets[i]
			}
		}
		return nil
	}
	invalid := func(t *testing.T, what string, err error) {
		t.Helper()
		var re *dns.ResponseError
		if !errors.As(err, &re) || !re.Invalid() {
			t.Fatalf("%s: %v", what, err)
		}
	}

	name := "records." + cfg.Zone

	t.Run("set, read and set again", func(t *testing.T) {
		c := newClient(t, "", true)
		t.Cleanup(func() { _ = c.DeleteRecords(context.Background(), name, dns.TypeA) })
		set := dns.RecordSet{Name: name, Type: dns.TypeA, TTL: 120, Values: []string{"192.0.2.10", "192.0.2.11"}}
		for i := range 2 {
			if err := c.SetRecords(ctx(t), set); err != nil {
				t.Fatalf("set %d: %v", i+1, err)
			}
		}
		got := read(t, c, name, dns.TypeA)
		if got == nil || !slices.Equal(got.Values, set.Values) || got.TTL != 120 {
			t.Fatalf("read back %+v, want %+v", got, set)
		}
		held(t, name, dns.TypeA, set.Values...)

		set.Values, set.TTL = []string{"192.0.2.12"}, 60
		if err := c.SetRecords(ctx(t), set); err != nil {
			t.Fatalf("replace: %v", err)
		}
		got = read(t, c, name, dns.TypeA)
		if got == nil || !slices.Equal(got.Values, set.Values) || got.TTL != 60 {
			t.Fatalf("after the replacement %+v, want %+v", got, set)
		}
		held(t, name, dns.TypeA, "192.0.2.12")
	})

	t.Run("the types", func(t *testing.T) {
		c := newClient(t, "", true)
		alias := "alias." + cfg.Zone
		t.Cleanup(func() {
			for _, rt := range []string{dns.TypeAAAA, dns.TypeTXT} {
				_ = c.DeleteRecords(context.Background(), name, rt)
			}
			_ = c.DeleteRecords(context.Background(), alias, dns.TypeCNAME)
		})
		for _, set := range []dns.RecordSet{
			{Name: name, Type: dns.TypeAAAA, Values: []string{"2001:db8::10"}},
			{Name: name, Type: dns.TypeTXT, Values: []string{"v=lab1 hello"}},
			{Name: alias, Type: dns.TypeCNAME, Values: []string{name}},
		} {
			if err := c.SetRecords(ctx(t), set); err != nil {
				t.Fatalf("set %s %s: %v", set.Name, set.Type, err)
			}
			got := read(t, c, set.Name, set.Type)
			if got == nil || !slices.Equal(got.Values, set.Values) || got.TTL != dns.DefaultRecordTTL {
				t.Fatalf("read back %+v, want %+v with the default TTL", got, set)
			}
			held(t, set.Name, set.Type, set.Values...)
		}
	})

	t.Run("delete", func(t *testing.T) {
		c := newClient(t, "", true)
		set := dns.RecordSet{Name: name, Type: dns.TypeA, Values: []string{"192.0.2.20"}}
		if err := c.SetRecords(ctx(t), set); err != nil {
			t.Fatalf("set: %v", err)
		}
		for i := range 2 {
			if err := c.DeleteRecords(ctx(t), name, dns.TypeA); err != nil {
				t.Fatalf("delete %d: %v", i+1, err)
			}
		}
		if got := read(t, c, name, dns.TypeA); got != nil {
			t.Fatalf("after the deletion: %+v", got)
		}
		held(t, name, dns.TypeA)
		if err := c.DeleteRecords(ctx(t), "never-set."+cfg.Zone, dns.TypeTXT); err != nil {
			t.Fatalf("delete of what was never set: %v", err)
		}
	})

	t.Run("conditional writes", func(t *testing.T) {
		c := newClient(t, "", true)
		t.Cleanup(func() { _ = c.DeleteRecords(context.Background(), name, dns.TypeA) })
		failed := func(t *testing.T, what string, err error) {
			t.Helper()
			var re *dns.ResponseError
			if !errors.As(err, &re) || !re.PreconditionFailed() {
				t.Fatalf("%s: %v, want a 412 with the reason %s", what, err, dns.ReasonPreconditionFailed)
			}
		}
		set := dns.RecordSet{Name: name, Type: dns.TypeA, TTL: 120, Values: []string{"192.0.2.30", "192.0.2.31"}}
		absent := dns.Expect{Absent: true}

		if !conditional {
			// A provider that can't check a condition refuses it and
			// writes nothing.
			err := c.SetRecordsIf(ctx(t), set, absent)
			var re *dns.ResponseError
			if !errors.As(err, &re) || !re.ConditionsUnsupported() {
				t.Fatalf("a conditional write to a provider that can't check it: %v, want a 501 with the reason %s",
					err, dns.ReasonConditionsUnsupported)
			}
			if got := read(t, c, name, dns.TypeA); got != nil {
				t.Fatalf("the refused write was done: %+v", got)
			}
			return
		}

		// Created only where there is no set.
		if err := c.SetRecordsIf(ctx(t), set, absent); err != nil {
			t.Fatalf("create: %v", err)
		}
		held(t, name, dns.TypeA, set.Values...)
		other := set
		other.Values = []string{"192.0.2.39"}
		failed(t, "creating a set that is there", c.SetRecordsIf(ctx(t), other, absent))
		held(t, name, dns.TypeA, set.Values...)

		// Replaced only over the expected values, in any order.
		failed(t, "replacing other values", c.SetRecordsIf(ctx(t), other, dns.Expect{Values: []string{"192.0.2.30"}}))
		failed(t, "replacing more values", c.SetRecordsIf(ctx(t), other,
			dns.Expect{Values: []string{"192.0.2.30", "192.0.2.31", "192.0.2.32"}}))
		held(t, name, dns.TypeA, set.Values...)
		if err := c.SetRecordsIf(ctx(t), other, dns.Expect{Values: []string{"192.0.2.31", "192.0.2.30"}}); err != nil {
			t.Fatalf("replace: %v", err)
		}
		got := read(t, c, name, dns.TypeA)
		if got == nil || !slices.Equal(got.Values, other.Values) {
			t.Fatalf("after the replacement %+v, want %+v", got, other)
		}
		held(t, name, dns.TypeA, other.Values...)

		// Deleted only with the expected values.
		failed(t, "deleting other values", c.DeleteRecordsIf(ctx(t), name, dns.TypeA, dns.Expect{Values: set.Values}))
		held(t, name, dns.TypeA, other.Values...)
		if err := c.DeleteRecordsIf(ctx(t), name, dns.TypeA, dns.Expect{Values: other.Values}); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if got := read(t, c, name, dns.TypeA); got != nil {
			t.Fatalf("after the deletion: %+v", got)
		}
		held(t, name, dns.TypeA)
		failed(t, "deleting a set that is not there", c.DeleteRecordsIf(ctx(t), name, dns.TypeA, dns.Expect{Values: other.Values}))
		failed(t, "replacing a set that is not there", c.SetRecordsIf(ctx(t), other, dns.Expect{Values: other.Values}))
		held(t, name, dns.TypeA)
	})

	t.Run("refused before the provider", func(t *testing.T) {
		c := newClient(t, "", true)
		invalid(t, "a name of another zone",
			c.SetRecords(ctx(t), dns.RecordSet{Name: "www.other-zone.example", Type: dns.TypeA, Values: []string{"192.0.2.1"}}))
		invalid(t, "a challenge name",
			c.SetRecords(ctx(t), dns.RecordSet{Name: dns.ChallengeName("www." + cfg.Zone), Type: dns.TypeTXT, Values: []string{"x"}}))
		invalid(t, "an IPv6 address in an A record",
			c.SetRecords(ctx(t), dns.RecordSet{Name: name, Type: dns.TypeA, Values: []string{"2001:db8::1"}}))
		invalid(t, "two CNAME values",
			c.SetRecords(ctx(t), dns.RecordSet{Name: name, Type: dns.TypeCNAME, Values: []string{"a." + cfg.Zone, "b." + cfg.Zone}}))
		invalid(t, "a CNAME at the apex",
			c.SetRecords(ctx(t), dns.RecordSet{Name: cfg.Zone, Type: dns.TypeCNAME, Values: []string{"a.other.example"}}))
		invalid(t, "an unknown type",
			c.SetRecords(ctx(t), dns.RecordSet{Name: name, Type: "MX", Values: []string{"10 mail." + cfg.Zone}}))
		invalid(t, "no value",
			c.SetRecords(ctx(t), dns.RecordSet{Name: name, Type: dns.TypeA}))
		_, err := c.Records(ctx(t), "www.other-zone.example")
		invalid(t, "reading a name of another zone", err)
		invalid(t, "deleting a name of another zone", c.DeleteRecords(ctx(t), "www.other-zone.example", dns.TypeA))
		invalid(t, "deleting a challenge name", c.DeleteRecords(ctx(t), dns.ChallengeName("www."+cfg.Zone), dns.TypeTXT))
		invalid(t, "deleting an unknown type", c.DeleteRecords(ctx(t), name, "MX"))
		a := dns.RecordSet{Name: name, Type: dns.TypeA, Values: []string{"192.0.2.1"}}
		invalid(t, "expecting an absent set and values",
			c.SetRecordsIf(ctx(t), a, dns.Expect{Absent: true, Values: []string{"192.0.2.1"}}))
		invalid(t, "expecting nothing", c.SetRecordsIf(ctx(t), a, dns.Expect{}))
		invalid(t, "expecting an IPv6 address in an A record", c.SetRecordsIf(ctx(t), a, dns.Expect{Values: []string{"2001:db8::1"}}))
		invalid(t, "a deletion expecting an absent set", c.DeleteRecordsIf(ctx(t), name, dns.TypeA, dns.Expect{Absent: true}))
		if n := calls.Load(); n != 0 {
			t.Fatalf("%d of the refused requests reached the provider", n)
		}
	})

	t.Run("token", func(t *testing.T) {
		c := newClient(t, "s3cret", true)
		c.Token = "wrong"
		_, err := c.Records(ctx(t), name)
		var re *dns.ResponseError
		if !errors.As(err, &re) || re.Status != http.StatusUnauthorized {
			t.Fatalf("a wrong token: %v", err)
		}
		if n := calls.Load(); n != 0 {
			t.Fatalf("%d requests with a wrong token reached the provider", n)
		}
	})

	t.Run("records not enabled", func(t *testing.T) {
		c := newClient(t, "", false)
		_, err := c.Records(ctx(t), name)
		var re *dns.ResponseError
		if !errors.As(err, &re) || !re.RecordsDisabled() {
			t.Fatalf("a zone without records = true: %v, want a 403 with the reason %s", err, dns.ReasonRecordsDisabled)
		}
		if n := calls.Load(); n != 0 {
			t.Fatalf("%d requests of a zone without records = true reached the provider", n)
		}
	})
}
