// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package mock

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/dns"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

var zoneParams = []params.Param{
	{Name: "latency", Type: params.ParamDuration, Default: time.Duration(0),
		Description: "time every call takes (simulates a slow DNS API)"},
	{Name: "fail", Type: params.ParamBool, Default: false,
		Description: "whether every call fails (simulates an outage of the DNS API)"},
}

// DNS is the DNS provider of a mock zone: it keeps the TXT records in
// memory and serves them to nobody.
type DNS struct {
	zone    string
	latency time.Duration
	fail    bool

	mu      sync.Mutex
	records map[string][]string

	// sets are the record sets of the records contract, by name and type.
	sets map[string]map[string]dns.RecordSet
}

// NewDNS creates the provider of a zone.
func NewDNS(cfg dns.Config) (dns.Provider, error) {
	return &DNS{
		zone:    cfg.Zone,
		latency: cfg.Values.Duration("latency"),
		fail:    cfg.Values.Bool("fail"),
		records: map[string][]string{},
		sets:    map[string]map[string]dns.RecordSet{},
	}, nil
}

func (p *DNS) call(ctx context.Context) error {
	if p.latency > 0 {
		select {
		case <-time.After(p.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.fail {
		return errMockFails
	}
	return nil
}

type mockError string

func (e mockError) Error() string { return string(e) }

const errMockFails = mockError("the mock is set to fail")

// Present implements dns.Provider.
func (p *DNS) Present(ctx context.Context, rec dns.Record) error {
	if err := p.call(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !slices.Contains(p.records[rec.Name], rec.Value) {
		p.records[rec.Name] = append(p.records[rec.Name], rec.Value)
	}
	return nil
}

// CleanUp implements dns.Provider.
func (p *DNS) CleanUp(ctx context.Context, rec dns.Record) error {
	if err := p.call(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	values := slices.DeleteFunc(p.records[rec.Name], func(v string) bool { return v == rec.Value })
	if len(values) == 0 {
		delete(p.records, rec.Name)
		return nil
	}
	p.records[rec.Name] = values
	return nil
}

// Values returns the values of the TXT records of a name.
func (p *DNS) Values(name string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.records[name])
}

// Records implements dns.RecordProvider.
func (p *DNS) Records(ctx context.Context, name string) ([]dns.RecordSet, error) {
	if err := p.call(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []dns.RecordSet{}
	for _, t := range dns.RecordTypes {
		if set, ok := p.sets[name][t]; ok {
			set.Values = slices.Clone(set.Values)
			out = append(out, set)
		}
	}
	return out, nil
}

// SetRecords implements dns.RecordProvider.
func (p *DNS) SetRecords(ctx context.Context, set dns.RecordSet) error {
	if err := p.call(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	types := p.sets[set.Name]
	// A CNAME stands alone at its name, as DNS asks.
	switch {
	case set.Type == dns.TypeCNAME && len(types) > 0 && types[dns.TypeCNAME].Name == "":
		return dns.Invalid("the name %s has other records: a CNAME stands alone", set.Name)
	case set.Type != dns.TypeCNAME && types[dns.TypeCNAME].Name != "":
		return dns.Invalid("the name %s is a CNAME: it can't have other records", set.Name)
	}
	if types == nil {
		types = map[string]dns.RecordSet{}
		p.sets[set.Name] = types
	}
	set.Values = slices.Clone(set.Values)
	types[set.Type] = set
	return nil
}

// DeleteRecords implements dns.RecordProvider.
func (p *DNS) DeleteRecords(ctx context.Context, name, recordType string) error {
	if err := p.call(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sets[name], recordType)
	if len(p.sets[name]) == 0 {
		delete(p.sets, name)
	}
	return nil
}

// RecordValues returns the values of a record set of the records contract.
func (p *DNS) RecordValues(name, recordType string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.sets[name][recordType].Values)
}
