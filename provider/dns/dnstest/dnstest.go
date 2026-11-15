// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package dnstest checks that a DNS provider keeps the contract of package
// dns, the way norca-acme relies on it. Run serves the provider with
// dns.NewHandler and calls it with dns.Client, over HTTP, so the checks see
// what the operator sees:
//
//   - a value is presented, and presenting it again changes nothing;
//   - a name holds two values at once (a name and its wildcard are proven
//     at the same challenge name);
//   - cleaning up removes one value and leaves the other;
//   - cleaning up again, or a value that was never there, is not an error;
//   - the token is required;
//   - a name outside the zone, and a name that is not a challenge name, is
//     refused before the provider is called.
//
// A provider for a real platform runs it against a test zone or a fake of
// the platform's API; the mock of this repository runs it in its tests.
package dnstest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/dns"
)

// Config describes the provider under test.
type Config struct {
	// New returns the provider of the zone under test.
	New func(t *testing.T) dns.Provider

	// Zone is the name of the zone the provider serves.
	Zone string

	// Lookup returns the values of the TXT records of a name as the
	// platform holds them now, in any order. Without it the checks only see
	// what the calls answer.
	Lookup func(t *testing.T, name string) []string
}

// Run runs the checks.
func Run(t *testing.T, cfg Config) {
	t.Helper()
	if !dns.ValidZone(cfg.Zone) {
		t.Fatalf("dnstest: invalid zone %q", cfg.Zone)
	}

	newClient := func(t *testing.T, token string) *dns.Client {
		t.Helper()
		z := &dns.Zone{Name: cfg.Zone, Type: "test", Provider: cfg.New(t)}
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
	held := func(t *testing.T, name string, want ...string) {
		t.Helper()
		if cfg.Lookup == nil {
			return
		}
		got := slices.Clone(cfg.Lookup(t, name))
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("the TXT records of %s are %q, want %q", name, got, want)
		}
	}
	invalid := func(t *testing.T, what string, err error) {
		t.Helper()
		var re *dns.ResponseError
		if !errors.As(err, &re) || !re.Invalid() {
			t.Fatalf("%s: %v", what, err)
		}
	}

	name := dns.ChallengeName("conformance." + cfg.Zone)
	first := dns.Record{Name: name, Value: "conformance-value-1"}
	second := dns.Record{Name: name, Value: "conformance-value-2"}

	t.Run("present is idempotent", func(t *testing.T) {
		c := newClient(t, "")
		t.Cleanup(func() { _ = c.CleanUp(context.Background(), first) })
		if err := c.Present(ctx(t), first); err != nil {
			t.Fatalf("present: %v", err)
		}
		if err := c.Present(ctx(t), first); err != nil {
			t.Fatalf("present again: %v", err)
		}
		held(t, name, first.Value)
	})

	t.Run("two values of one name", func(t *testing.T) {
		c := newClient(t, "")
		t.Cleanup(func() {
			_ = c.CleanUp(context.Background(), first)
			_ = c.CleanUp(context.Background(), second)
		})
		if err := c.Present(ctx(t), first); err != nil {
			t.Fatalf("present: %v", err)
		}
		if err := c.Present(ctx(t), second); err != nil {
			t.Fatalf("present a second value: %v", err)
		}
		held(t, name, first.Value, second.Value)

		if err := c.CleanUp(ctx(t), first); err != nil {
			t.Fatalf("clean up: %v", err)
		}
		held(t, name, second.Value)
		if err := c.CleanUp(ctx(t), second); err != nil {
			t.Fatalf("clean up the second value: %v", err)
		}
		held(t, name)
	})

	t.Run("clean up of what is not there", func(t *testing.T) {
		c := newClient(t, "")
		if err := c.Present(ctx(t), first); err != nil {
			t.Fatalf("present: %v", err)
		}
		if err := c.CleanUp(ctx(t), first); err != nil {
			t.Fatalf("clean up: %v", err)
		}
		if err := c.CleanUp(ctx(t), first); err != nil {
			t.Fatalf("clean up again: %v", err)
		}
		never := dns.Record{Name: dns.ChallengeName("never-presented." + cfg.Zone), Value: "never"}
		if err := c.CleanUp(ctx(t), never); err != nil {
			t.Fatalf("clean up of a value that was never presented: %v", err)
		}
		held(t, name)
	})

	t.Run("the zone itself and a wildcard", func(t *testing.T) {
		c := newClient(t, "")
		apex := dns.Record{Name: dns.ChallengeName("*." + cfg.Zone), Value: "conformance-apex"}
		if apex.Name != dns.ChallengeLabel+"."+cfg.Zone {
			t.Fatalf("the challenge name of the wildcard of the zone is %s", apex.Name)
		}
		if err := c.Present(ctx(t), apex); err != nil {
			t.Fatalf("present at the zone itself: %v", err)
		}
		held(t, apex.Name, apex.Value)
		if err := c.CleanUp(ctx(t), apex); err != nil {
			t.Fatalf("clean up at the zone itself: %v", err)
		}
	})

	t.Run("token", func(t *testing.T) {
		c := newClient(t, "s3cret")
		c.Token = "wrong"
		err := c.Present(ctx(t), first)
		var re *dns.ResponseError
		if !errors.As(err, &re) || re.Status != http.StatusUnauthorized {
			t.Fatalf("a wrong token: %v", err)
		}
		c.Token = "s3cret"
		if err := c.Present(ctx(t), first); err != nil {
			t.Fatalf("the right token: %v", err)
		}
		_ = c.CleanUp(ctx(t), first)
	})

	t.Run("names the zone does not serve", func(t *testing.T) {
		c := newClient(t, "")
		invalid(t, "a name of another zone",
			c.Present(ctx(t), dns.Record{Name: dns.ChallengeLabel + ".www.other-zone.example", Value: "x"}))
		invalid(t, "a name that only ends like the zone",
			c.Present(ctx(t), dns.Record{Name: dns.ChallengeLabel + ".not" + cfg.Zone, Value: "x"}))
		invalid(t, "a name that is not a challenge name",
			c.Present(ctx(t), dns.Record{Name: "www." + cfg.Zone, Value: "x"}))
		invalid(t, "a challenge label below the first",
			c.Present(ctx(t), dns.Record{Name: "www." + dns.ChallengeLabel + "." + cfg.Zone, Value: "x"}))
		invalid(t, "an empty value",
			c.Present(ctx(t), dns.Record{Name: name, Value: ""}))
		invalid(t, "a value with a quote",
			c.Present(ctx(t), dns.Record{Name: name, Value: `a"b`}))
		invalid(t, "a clean up outside the zone",
			c.CleanUp(ctx(t), dns.Record{Name: dns.ChallengeLabel + ".www.other-zone.example", Value: "x"}))
	})
}
