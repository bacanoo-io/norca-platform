// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca/norca/api"
)

// JoinIssuer gives the instances of a request a one-time token instead of a
// certificate (package join), and drops the records that are no longer
// needed.
type JoinIssuer interface {
	// Ticket records the token of an instance of the group, valid for ttl,
	// and names the join endpoints.
	Ticket(ctx context.Context, group, instance string, ttl time.Duration) (bootstrap.JoinTicket, error)
	// Prune drops the records of the group whose token expired unused and
	// those of instances that are gone; live are the instances the
	// provider lists, prefix is the start of the names of the group's
	// instances.
	Prune(ctx context.Context, group, prefix string, live map[string]bool) error
	// Forget drops every record of a group whose instances are gone with
	// it (a deletion with the policy drain or terminate).
	Forget(ctx context.Context, group, prefix string) error
	// Seen records what the provider reports for an instance of the group:
	// its addresses, which the join endpoint binds the join and the
	// renewals of the instance to.
	Seen(ctx context.Context, group, instance string, addresses []string) error
}

// joinSlack is added to the join deadline of a group for the validity of a
// token: the instance is deleted at the deadline anyway.
const joinSlack = 5 * time.Minute

// StoreJoin is the JoinIssuer of the operator: the records in a join.Store,
// the endpoints from URLs.
type StoreJoin struct {
	Store *join.Store
	URLs  func(ctx context.Context) ([]string, error)

	mu sync.Mutex
	// seen are the addresses last recorded per group and instance: a
	// record is read again only when they change or a token is issued.
	seen map[string]string
}

// Ticket implements JoinIssuer.
func (s *StoreJoin) Ticket(ctx context.Context, group, instance string, ttl time.Duration) (bootstrap.JoinTicket, error) {
	urls, err := s.URLs(ctx)
	if err != nil {
		return bootstrap.JoinTicket{}, fmt.Errorf("the join endpoints: %w", err)
	}
	if len(urls) == 0 {
		return bootstrap.JoinTicket{}, errors.New("no join endpoint is known: the operator's join service has no instance and no -join-url is set")
	}
	// A new token is a new record: what the provider reports is recorded
	// again.
	s.mu.Lock()
	delete(s.seen, group+"/"+instance)
	s.mu.Unlock()
	token, err := s.Store.Issue(ctx, group, instance, ttl)
	if err != nil {
		return bootstrap.JoinTicket{}, err
	}
	return bootstrap.JoinTicket{Token: token, URLs: urls}, nil
}

// Prune implements JoinIssuer.
func (s *StoreJoin) Prune(ctx context.Context, group, prefix string, live map[string]bool) error {
	return s.Store.Prune(ctx, group, prefix, live)
}

// Seen implements JoinIssuer.
func (s *StoreJoin) Seen(ctx context.Context, group, instance string, addresses []string) error {
	key, list := group+"/"+instance, strings.Join(join.NormalizeAddresses(addresses), ",")
	s.mu.Lock()
	last, ok := s.seen[key]
	s.mu.Unlock()
	if ok && (list == last || list == "") {
		return nil
	}
	if _, err := s.Store.Seen(ctx, group, instance, addresses); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = map[string]string{}
	}
	if list != "" || !ok {
		s.seen[key] = list
	}
	return nil
}

// Forget implements JoinIssuer.
func (s *StoreJoin) Forget(ctx context.Context, group, prefix string) error {
	return s.Store.Forget(ctx, group, prefix)
}

// TemplateAuthority gives the join endpoint the authority of the bootstrap
// template: the cluster CA and the node CA of the region.
type TemplateAuthority struct {
	Bootstrap BootstrapSource
}

// Authority implements join.AuthoritySource.
func (t *TemplateAuthority) Authority(ctx context.Context) (*join.Authority, error) {
	tmpl, err := t.Bootstrap.Template(ctx)
	if err != nil {
		return nil, err
	}
	return tmpl.JoinAuthority(), nil
}

// ServiceURLs names the join endpoints after the registrations of a service
// in the native catalog (the operator's pack registers its join port):
// https://<address>:<port> of every instance, sorted.
func ServiceURLs(client *api.Client, service, namespace string) func(ctx context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		regs, _, err := client.Services().Get(service, (&api.QueryOptions{Namespace: namespace}).WithContext(ctx))
		if err != nil {
			return nil, err
		}
		var urls []string
		for _, r := range regs {
			if r.Address == "" || r.Port == 0 {
				continue
			}
			urls = append(urls, "https://"+net.JoinHostPort(r.Address, strconv.Itoa(r.Port)))
		}
		slices.Sort(urls)
		return slices.Compact(urls), nil
	}
}

// recordSeen records in the join records what the provider reports for the
// instances: their addresses, once it reports some or the instance runs (a
// provider that reports none for a running instance has none to report).
// The join endpoint waits for it before a first join while it checks the
// addresses.
func (r *Reconciler) recordSeen(ctx context.Context, group string, insts []provider.Instance) {
	if r.cfg.Join == nil {
		return
	}
	for _, inst := range insts {
		if inst.Name == "" || (inst.State != provider.StateRunning && (inst.State != provider.StateCreating || len(inst.Addresses) == 0)) {
			continue
		}
		if err := r.cfg.Join.Seen(ctx, group, inst.Name, inst.Addresses); err != nil {
			r.log.Warn("recording the addresses of an instance failed", "group", group, "instance", inst.Name, "error", err)
		}
	}
}

// instancePrefix is a start every name of an instance of the group has. A
// long group is cut shorter for a longer request number (instanceName), so
// it is at most 39 characters, which a suffix of 24 leaves; the group of a
// record is checked after it.
func instancePrefix(group string) string {
	name := instanceName(group, 0, 0)
	base := name[:len(name)-len("-0-0")]
	if len(base) > 39 {
		base = base[:39]
	}
	return base
}
