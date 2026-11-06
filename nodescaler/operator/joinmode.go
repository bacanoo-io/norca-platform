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
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
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
}

// joinSlack is added to the join deadline of a group for the validity of a
// token: the instance is deleted at the deadline anyway.
const joinSlack = 5 * time.Minute

// StoreJoin is the JoinIssuer of the operator: the records in a join.Store,
// the endpoints from URLs.
type StoreJoin struct {
	Store *join.Store
	URLs  func(ctx context.Context) ([]string, error)
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
