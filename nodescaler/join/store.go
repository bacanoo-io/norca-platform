// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// The items of the record of an instance.
const (
	itemGroup   = "group"
	itemToken   = "token_sha256"
	itemExpires = "expires"
	itemSerial  = "serial"
	itemJoined  = "joined"
	itemRenewed = "renewed"
	// The addresses the provider reports for the instance (comma separated)
	// and when the operator first recorded what the provider reports: the
	// join endpoint binds the join and the renewals to these addresses.
	itemAddresses = "addresses"
	itemSeen      = "seen"
)

// ErrRefused is a join or a renewal the record does not allow: an unknown
// instance, a wrong, used or expired token, a certificate that is not the
// latest of its instance.
var ErrRefused = errors.New("refused")

// Store keeps the record of every instance that joins with its own key, a
// Variable nodescaler/join/<region>/<instance>: first the hash of its
// one-time token and its expiry, after the join the serial of the latest
// certificate of the instance. Only the operator reads and writes these
// Variables (the policy of its pack).
//
// The writes are check-and-set: of two uses of one token, or two renewals
// with one certificate, one wins, at whichever instance of the operator
// they arrive.
type Store struct {
	Vars      controller.VariablesAPI
	Namespace string
	Region    string
	Now       func() time.Time
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Store) prefix() string { return "nodescaler/join/" + s.Region + "/" }

func (s *Store) path(instance string) string { return s.prefix() + instance }

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Issue makes the one-time token of an instance of the group, valid for
// ttl, and records its hash. An instance asked for again (after a restart
// of the operator) gets a new token; the old one is void.
func (s *Store) Issue(ctx context.Context, group, instance string, ttl time.Duration) (string, error) {
	if !ValidInstance(instance) {
		return "", fmt.Errorf("the instance %q is not a host name", instance)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	v := &api.Variable{Namespace: s.Namespace, Path: s.path(instance), Items: api.VariableItems{
		itemGroup: group, itemToken: hashToken(token), itemExpires: s.now().Add(ttl).UTC().Format(time.RFC3339),
	}}
	old, err := s.Vars.ReadVariable(ctx, s.Namespace, v.Path)
	switch {
	case err == nil:
		v.ModifyIndex = old.ModifyIndex
	case !errors.Is(err, controller.ErrNotFound):
		return "", fmt.Errorf("reading the join record of %s: %w", instance, err)
	}
	if _, err := s.Vars.WriteVariable(ctx, v); err != nil {
		return "", fmt.Errorf("writing the join record of %s: %w", instance, err)
	}
	return token, nil
}

// Redeem checks the token of an instance: it returns the record, to be
// passed to Joined, and the group of the instance.
func (s *Store) Redeem(ctx context.Context, instance, token string) (*api.Variable, string, error) {
	v, err := s.read(ctx, instance)
	if err != nil {
		return nil, "", err
	}
	want := v.Items[itemToken]
	if want == "" {
		return nil, "", fmt.Errorf("%w: the token of %s was used", ErrRefused, instance)
	}
	if subtle.ConstantTimeCompare([]byte(hashToken(token)), []byte(want)) != 1 {
		return nil, "", fmt.Errorf("%w: a wrong token for %s", ErrRefused, instance)
	}
	if exp, err := time.Parse(time.RFC3339, v.Items[itemExpires]); err != nil || !s.now().Before(exp) {
		return nil, "", fmt.Errorf("%w: the token of %s expired", ErrRefused, instance)
	}
	return v, v.Items[itemGroup], nil
}

// Joined records the first certificate of a redeemed instance; the token is
// void from now. It fails if the token was used meanwhile; a change of the
// record by the operator (its addresses) is not a use.
func (s *Store) Joined(ctx context.Context, v *api.Variable, serial string) error {
	token := v.Items[itemToken]
	return s.updateRetry(ctx, v, func(cur *api.Variable) bool { return cur.Items[itemToken] == token },
		func(items api.VariableItems) {
			delete(items, itemToken)
			delete(items, itemExpires)
			items[itemSerial] = serial
			items[itemJoined] = s.now().UTC().Format(time.RFC3339)
		})
}

// Renewing checks that a certificate of an instance is its latest: it
// returns the record, to be passed to Renewed, and the group.
func (s *Store) Renewing(ctx context.Context, instance, serial string) (*api.Variable, string, error) {
	v, err := s.read(ctx, instance)
	if err != nil {
		return nil, "", err
	}
	if v.Items[itemSerial] == "" {
		return nil, "", fmt.Errorf("%w: %s has not joined", ErrRefused, instance)
	}
	if v.Items[itemSerial] != serial {
		return nil, "", fmt.Errorf("%w: the certificate %s is not the latest of %s", ErrRefused, serial, instance)
	}
	return v, v.Items[itemGroup], nil
}

// Renewed records the new certificate of an instance. It fails if another
// renewal with the same certificate won; a change of the record by the
// operator (its addresses) is not one.
func (s *Store) Renewed(ctx context.Context, v *api.Variable, serial string) error {
	old := v.Items[itemSerial]
	return s.updateRetry(ctx, v, func(cur *api.Variable) bool { return cur.Items[itemSerial] == old },
		func(items api.VariableItems) {
			items[itemSerial] = serial
			items[itemRenewed] = s.now().UTC().Format(time.RFC3339)
		})
}

// Addresses returns the addresses recorded for an instance and whether the
// operator recorded what the provider reports for it (seen).
func Addresses(v *api.Variable) (addresses []string, seen bool) {
	if a := v.Items[itemAddresses]; a != "" {
		addresses = strings.Split(a, ",")
	}
	return addresses, v.Items[itemSeen] != ""
}

// NormalizeAddresses returns the IP addresses of a list, sorted and once
// each, IPv4 mapped into IPv6 as IPv4; what is not an IP address is left out.
func NormalizeAddresses(list []string) []string {
	var out []string
	for _, a := range list {
		ip, err := netip.ParseAddr(strings.TrimSpace(a))
		if err != nil {
			continue
		}
		out = append(out, ip.Unmap().WithZone("").String())
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Seen records what the provider reports for an instance of the group: its
// addresses, and that the operator saw it (the join endpoint waits for it
// before a first join while it checks the addresses). It writes only on a
// change. Addresses that were known are never cleared: a provider that stops
// reporting them does not open the check; a new non-empty list replaces
// them. An instance without a record, or with the record of another group,
// is left alone. It reports whether it wrote.
func (s *Store) Seen(ctx context.Context, group, instance string, addresses []string) (bool, error) {
	if !ValidInstance(instance) {
		return false, nil
	}
	v, err := s.Vars.ReadVariable(ctx, s.Namespace, s.path(instance))
	if errors.Is(err, controller.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading the join record of %s: %w", instance, err)
	}
	if v.Items[itemGroup] != group {
		return false, nil
	}
	items := maps.Clone(v.Items)
	if list := NormalizeAddresses(addresses); len(list) > 0 {
		items[itemAddresses] = strings.Join(list, ",")
	}
	if items[itemSeen] == "" {
		items[itemSeen] = s.now().UTC().Format(time.RFC3339)
	}
	if maps.Equal(items, v.Items) {
		return false, nil
	}
	if err := s.update(ctx, v, items); err != nil {
		return false, err
	}
	return true, nil
}

// Prune deletes the records of a group that are no longer needed: a token
// that expired unused, and an instance that joined and is gone. live are
// the instances the provider lists for the group; only the records of
// instances whose names start with namePrefix are read. The instances of a
// request are recorded before the request, so an instance that joined is
// listed until it is deleted.
func (s *Store) Prune(ctx context.Context, group, namePrefix string, live map[string]bool) error {
	list, _, err := s.Vars.ListVariables(ctx, s.Namespace, s.prefix()+namePrefix, 0)
	if err != nil {
		return err
	}
	var errs []error
	for _, meta := range list {
		instance := strings.TrimPrefix(meta.Path, s.prefix())
		if live[instance] {
			continue
		}
		v, err := s.Vars.ReadVariable(ctx, s.Namespace, meta.Path)
		if errors.Is(err, controller.ErrNotFound) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if v.Items[itemGroup] != group {
			continue
		}
		joined := v.Items[itemSerial] != ""
		exp, err := time.Parse(time.RFC3339, v.Items[itemExpires])
		expired := err != nil || !s.now().Before(exp)
		if joined || expired {
			if err := s.Vars.DeleteVariable(ctx, s.Namespace, meta.Path, v.ModifyIndex); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// Forget deletes every record of a group, used tokens or not: the group is
// gone with its instances. Only the records of instances whose names start
// with namePrefix are read.
func (s *Store) Forget(ctx context.Context, group, namePrefix string) error {
	list, _, err := s.Vars.ListVariables(ctx, s.Namespace, s.prefix()+namePrefix, 0)
	if err != nil {
		return err
	}
	var errs []error
	for _, meta := range list {
		v, err := s.Vars.ReadVariable(ctx, s.Namespace, meta.Path)
		if errors.Is(err, controller.ErrNotFound) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if v.Items[itemGroup] == group {
			if err := s.Vars.DeleteVariable(ctx, s.Namespace, meta.Path, v.ModifyIndex); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (s *Store) read(ctx context.Context, instance string) (*api.Variable, error) {
	if !ValidInstance(instance) {
		return nil, fmt.Errorf("%w: the instance %q is not a host name", ErrRefused, instance)
	}
	v, err := s.Vars.ReadVariable(ctx, s.Namespace, s.path(instance))
	if errors.Is(err, controller.ErrNotFound) {
		return nil, fmt.Errorf("%w: no record of %s", ErrRefused, instance)
	}
	if err != nil {
		return nil, fmt.Errorf("reading the join record of %s: %w", instance, err)
	}
	return v, nil
}

// updateRetry changes the items of a record (change works on a copy). When
// the record changed since it was read, it is read again and, if still
// holds for it, changed again, once: a change by the operator (Seen) does
// not refuse a join or a renewal.
func (s *Store) updateRetry(ctx context.Context, v *api.Variable, still func(*api.Variable) bool, change func(api.VariableItems)) error {
	for attempt := 0; ; attempt++ {
		items := maps.Clone(v.Items)
		change(items)
		err := s.update(ctx, v, items)
		if err == nil || attempt > 0 || !errors.Is(err, ErrRefused) {
			return err
		}
		cur, rerr := s.Vars.ReadVariable(ctx, s.Namespace, v.Path)
		if rerr != nil || !still(cur) {
			return err
		}
		v = cur
	}
}

func (s *Store) update(ctx context.Context, v *api.Variable, items api.VariableItems) error {
	next := &api.Variable{Namespace: v.Namespace, Path: v.Path, ModifyIndex: v.ModifyIndex, Items: items}
	if _, err := s.Vars.WriteVariable(ctx, next); err != nil {
		if errors.Is(err, controller.ErrConflict) {
			return fmt.Errorf("%w: the record of %s changed meanwhile", ErrRefused, strings.TrimPrefix(v.Path, s.prefix()))
		}
		return err
	}
	return nil
}
