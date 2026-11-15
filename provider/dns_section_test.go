// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package provider_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/registrar"
	"github.com/bacanoo-io/norca-platform/providers/mock"
)

const withZones = `{
  "advertise_url": "https://provider.service.norca:8443",
  "providers": [{
    "name": "lab", "type": "mock", "token": "env:PROVIDER_TEST_TOKEN", "token_variable": "providers/lab",
    "loadbalancer": {"classes": [{"class": "mock-public"}]},
    "dns": {"zones": [
      {"zone": "Example.COM.", "params": {"latency": "1ms"}},
      {"zone": "internal.example.org", "endpoint": "http://10.0.0.5:18443/zones/internal.example.org"}
    ]}
  }]
}`

// A dns section builds a served zone per zone, registered as DNSZone
// objects with the endpoint and the token Variable of the provider.
func TestBuild_DNSSection(t *testing.T) {
	t.Setenv("PROVIDER_TEST_TOKEN", "s3cret")
	rt, err := parse(t, withZones).Build(testRegistry(t), provider.ServeAll, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if len(rt.Classes) != 1 || len(rt.Zones) != 2 {
		t.Fatalf("%d classes, %d zones", len(rt.Classes), len(rt.Zones))
	}
	first, second := rt.Zones[0], rt.Zones[1]
	if first.Name != "example.com" || first.Endpoint != "https://provider.service.norca:8443/zones/example.com" ||
		first.Token != "s3cret" || first.ProviderName != "lab" || first.Type != mock.TypeID {
		t.Fatalf("first zone: %+v %+v", first, first.Zone)
	}
	if second.Endpoint != "http://10.0.0.5:18443/zones/internal.example.org" || second.TokenVariable != "providers/lab" {
		t.Fatalf("second zone: %+v", second)
	}

	regs := rt.Registrations()
	if len(regs) != 3 || regs[1].Kind != registrar.KindDNSZone || regs[1].Name != "example.com" {
		t.Fatalf("registrations: %+v", regs)
	}
	spec := regs[1].Spec
	if spec["zone"] != "example.com" || spec["provider"] != "lab" || spec["tokenVariable"] != "providers/lab" ||
		spec["endpoint"] != "https://provider.service.norca:8443/zones/example.com" {
		t.Fatalf("DNSZone spec: %#v", spec)
	}

	// -serve dns keeps the zones only; -serve lb leaves them out.
	onlyDNS, err := parse(t, withZones).Build(testRegistry(t), provider.ServeDNS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer onlyDNS.Close()
	if len(onlyDNS.Classes) != 0 || len(onlyDNS.Zones) != 2 {
		t.Fatalf("-serve dns: %d classes, %d zones", len(onlyDNS.Classes), len(onlyDNS.Zones))
	}
	onlyLB, err := parse(t, withZones).Build(testRegistry(t), provider.ServeLB, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer onlyLB.Close()
	if len(onlyLB.Classes) != 1 || len(onlyLB.Zones) != 0 {
		t.Fatalf("-serve lb: %d classes, %d zones", len(onlyLB.Classes), len(onlyLB.Zones))
	}
}

// The zones are served below /zones/<zone> with the token of their
// provider, and listed on /v1/zones.
func TestHandler_Zones(t *testing.T) {
	t.Setenv("PROVIDER_TEST_TOKEN", "s3cret")
	rt, err := parse(t, withZones).Build(testRegistry(t), provider.ServeAll, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	srv := httptest.NewServer(provider.NewHandler(rt, provider.Default.Types(), nil))
	defer srv.Close()

	rec := dns.Record{Name: "_acme-challenge.www.example.com", Value: "proof"}
	client := &dns.Client{Endpoint: srv.URL + "/zones/example.com", Token: "s3cret"}
	if err := client.Present(context.Background(), rec); err != nil {
		t.Fatalf("present: %v", err)
	}
	if got := rt.Zones[0].Provider.(*mock.DNS).Values(rec.Name); len(got) != 1 || got[0] != "proof" {
		t.Fatalf("the records of the mock: %q", got)
	}
	if err := client.CleanUp(context.Background(), rec); err != nil {
		t.Fatalf("clean up: %v", err)
	}

	// The token of the provider is required, the zone must be served, and
	// a zone does not write the names of another one.
	noToken := &dns.Client{Endpoint: client.Endpoint}
	if err := noToken.Present(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("without the token: %v", err)
	}
	unknown := &dns.Client{Endpoint: srv.URL + "/zones/unknown.example", Token: "s3cret"}
	if err := unknown.Present(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "unknown zone") {
		t.Fatalf("an unknown zone: %v", err)
	}
	other := &dns.Client{Endpoint: srv.URL + "/zones/internal.example.org", Token: "s3cret"}
	if err := other.Present(context.Background(), rec); err == nil || !strings.Contains(err.Error(), "not in the zone") {
		t.Fatalf("a name of another zone: %v", err)
	}

	resp, err := http.Get(srv.URL + "/v1/zones")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var zones []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&zones); err != nil {
		t.Fatal(err)
	}
	if len(zones) != 2 || zones[0]["zone"] != "example.com" || zones[0]["tokenChecked"] != true || zones[0]["type"] != "mock" {
		t.Fatalf("/v1/zones: %v", zones)
	}
}

// What a dns section may not be.
func TestParse_DNSSectionErrors(t *testing.T) {
	cases := map[string][2]string{
		"no zones": {`"dns":{"zones":[]}`, "at least one zone"},
		"invalid":  {`"dns":{"zones":[{"zone":"not a zone"}]}`, "invalid zone name"},
		"twice":    {`"dns":{"zones":[{"zone":"example.com"},{"zone":"EXAMPLE.com."}]}`, "configured twice"},
		"endpoint": {`"dns":{"zones":[{"zone":"example.com","endpoint":"ftp://x"}]}`, "must be an http or https URL"},
	}
	for name, tc := range cases {
		content := `{"advertise_url":"https://x","providers":[{"name":"p","type":"mock","no_auth":true,` + tc[0] + `}]}`
		if _, err := provider.Parse([]byte(content), os.LookupEnv); err == nil || !strings.Contains(err.Error(), tc[1]) {
			t.Errorf("%s: %v", name, err)
		}
	}

	// A type without a DNS provider cannot serve a zone.
	f := parse(t, `{"advertise_url":"https://x","providers":[{"name":"p","type":"lbonly","no_auth":true,
	  "dns":{"zones":[{"zone":"example.com"}]}}]}`)
	if _, err := f.Build(testRegistry(t), provider.ServeAll, nil); err == nil || !strings.Contains(err.Error(), "has no dns provider") {
		t.Fatalf("a zone of a type without a dns provider: %v", err)
	}
}
