// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package mock_test

import (
	"context"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/dns/dnstest"
	"github.com/bacanoo-io/norca-platform/providers/mock"
)

// The DNS provider of the mock keeps the contract.
func TestContract_MockDNS(t *testing.T) {
	var current *mock.DNS
	dnstest.Run(t, dnstest.Config{
		Zone: "example.com",
		New: func(t *testing.T) dns.Provider {
			z, err := provider.Default.NewZone("example.com", mock.TypeID, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			current = z.Provider.(*mock.DNS)
			return current
		},
		Lookup: func(_ *testing.T, name string) []string { return current.Values(name) },
	})
}

// A mock zone that is set to fail fails every call with an error to retry.
func TestMockDNS_Fail(t *testing.T) {
	z, err := provider.Default.NewZone("example.com", mock.TypeID, nil, map[string]any{"fail": true})
	if err != nil {
		t.Fatal(err)
	}
	err = z.Provider.Present(context.Background(), dns.Record{Name: "_acme-challenge.example.com", Value: "v"})
	if err == nil {
		t.Fatal("a zone set to fail presented a record")
	}
}

// The DNS provider of the mock keeps the records contract.
func TestRecordsContract_MockDNS(t *testing.T) {
	var current *mock.DNS
	dnstest.RunRecords(t, dnstest.RecordsConfig{
		Zone: "example.com",
		New: func(t *testing.T) dns.Provider {
			z, err := provider.Default.NewZone("example.com", mock.TypeID, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			current = z.Provider.(*mock.DNS)
			return current
		},
		Lookup: func(_ *testing.T, name, recordType string) []string { return current.RecordValues(name, recordType) },
	})
}

// A CNAME stands alone at its name.
func TestMockDNS_CNAMEAlone(t *testing.T) {
	z, err := provider.Default.NewZone("example.com", mock.TypeID, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := z.Provider.(*mock.DNS)
	ctx := context.Background()
	if err := p.SetRecords(ctx, dns.RecordSet{Name: "www.example.com", Type: dns.TypeA, TTL: 60, Values: []string{"192.0.2.1"}}); err != nil {
		t.Fatal(err)
	}
	if err := p.SetRecords(ctx, dns.RecordSet{Name: "www.example.com", Type: dns.TypeCNAME, TTL: 60, Values: []string{"web.example.com"}}); err == nil {
		t.Fatal("a CNAME was set next to an A record")
	}
	if err := p.SetRecords(ctx, dns.RecordSet{Name: "alias.example.com", Type: dns.TypeCNAME, TTL: 60, Values: []string{"web.example.com"}}); err != nil {
		t.Fatal(err)
	}
	if err := p.SetRecords(ctx, dns.RecordSet{Name: "alias.example.com", Type: dns.TypeTXT, TTL: 60, Values: []string{"x"}}); err == nil {
		t.Fatal("a TXT record was set next to a CNAME")
	}
}
