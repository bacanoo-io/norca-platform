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
	// The request of the operator the token was issued for, and the tokens
	// issued for the same request before (see Issue): entries
	// <hash>@<expiry>, comma separated, the oldest first. A record without
	// them has no request and no earlier token.
	itemRequest  = "request"
	itemPrevious = "previous_tokens"
)

// maxPrevious is how many earlier tokens of a request a record keeps beside
// the latest. The operator makes a request again each time the grace time of
// the listing passes (two minutes by default) and after each failed create,
// and a token lives for the join deadline and five minutes (twenty minutes
// by default): the number covers the repeats of the listing within the life
// of a token. Beyond it the oldest is dropped, so a record stays small.
const maxPrevious = 16

// ErrRefused is a join or a renewal the record does not allow: an unknown
// instance, a wrong, used or expired token, a certificate that is not the
// latest of its instance.
var ErrRefused = errors.New("refused")

// ErrJoined is an issue for an instance of a request that has joined: it
// needs no token, and its record is left as it is.
var ErrJoined = errors.New("the instance has joined")

// ErrOtherGroup is an issue for a name whose record belongs to another
// group, and that a machine of that group can still use (it joined, or one
// of its tokens is valid): the record is left as it is.
var ErrOtherGroup = errors.New("the instance belongs to another group")

// Store keeps the record of every instance that joins with its own key, a
// Variable nodescaler/join/<region>/<instance>: first the hash of its
// one-time token and its expiry (and of the earlier tokens of the same
// request), after the join the serial of the latest certificate of the
// instance. Only the operator reads and writes these Variables (the policy
// of its pack).
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

// heldToken is a token of a record: its hash and when it expires.
type heldToken struct {
	hash    string
	expires time.Time
}

// parseToken reads a hash and its expiry; one that does not parse is no
// token (it cannot be used).
func parseToken(hash, expires string) (heldToken, bool) {
	exp, err := time.Parse(time.RFC3339, expires)
	if hash == "" || err != nil {
		return heldToken{}, false
	}
	return heldToken{hash: hash, expires: exp}, true
}

// previousOf returns the earlier tokens of the request of a record, the
// oldest first; none for a record without the item.
func previousOf(v *api.Variable) []heldToken {
	var out []heldToken
	for entry := range strings.SplitSeq(v.Items[itemPrevious], ",") {
		hash, expires, _ := strings.Cut(entry, "@")
		if t, ok := parseToken(hash, expires); ok {
			out = append(out, t)
		}
	}
	return out
}

// tokensOf returns the tokens of a record: the earlier ones of its request,
// the oldest first, then the latest. The record of an instance that joined
// has none.
func tokensOf(v *api.Variable) []heldToken {
	out := previousOf(v)
	if t, ok := parseToken(v.Items[itemToken], v.Items[itemExpires]); ok {
		out = append(out, t)
	}
	return out
}

// holds returns the token of a record with the hash, if it has one. Every
// token of the record is compared, each in constant time.
func holds(v *api.Variable, hash string) (heldToken, bool) {
	var found heldToken
	ok := false
	for _, t := range tokensOf(v) {
		if subtle.ConstantTimeCompare([]byte(hash), []byte(t.hash)) == 1 && !ok {
			found, ok = t, true
		}
	}
	return found, ok
}

// inUse reports a record a machine can still use: its instance joined (its
// certificate renews), or a token of it is valid.
func inUse(v *api.Variable, now time.Time) bool {
	return v.Items[itemSerial] != "" ||
		slices.ContainsFunc(tokensOf(v), func(t heldToken) bool { return now.Before(t.expires) })
}

// Issue makes the one-time token of an instance of the group, valid for
// ttl, and records its hash with the request of the operator it is for (its
// request ID; empty for none).
//
// A request the operator makes again (after a lost answer, or when the
// listing of the provider did not show its instances in time) is answered by
// the provider with the instances it made the first time, which carry the
// tokens of that time. So an issue for the same instance, group and request
// keeps the tokens issued for it before, each until its own expiry, beside
// the new one (at most maxPrevious of them; the addresses of the record stay
// too). Any one of them joins the instance, once: the join voids them all.
// And when the instance has joined, nothing is issued and the record, with
// the serial of its certificate, is left as it is: ErrJoined.
//
// An issue for another request or no request replaces the record: every
// token issued for the name before is void.
//
// The record of a name belongs to its group. An issue by another group does
// not replace it while a machine of that group can use it (it joined, or one
// of its tokens is valid): ErrOtherGroup, and the record is left as it is
// (review finding CON-6l). The operator gives the instances of two groups
// different names, so this is a second guard. A record of another group that
// no machine can use any more is replaced, as Prune would drop it.
func (s *Store) Issue(ctx context.Context, group, request, instance string, ttl time.Duration) (string, error) {
	if !ValidInstance(instance) {
		return "", fmt.Errorf("the instance %q is not a host name", instance)
	}
	path := s.path(instance)
	old, err := s.Vars.ReadVariable(ctx, s.Namespace, path)
	switch {
	case errors.Is(err, controller.ErrNotFound):
		old = nil
	case err != nil:
		return "", fmt.Errorf("reading the join record of %s: %w", instance, err)
	}
	again := old != nil && request != "" && old.Items[itemGroup] == group && old.Items[itemRequest] == request
	if again && old.Items[itemSerial] != "" {
		return "", fmt.Errorf("%w: %s", ErrJoined, instance)
	}
	now := s.now()
	if old != nil && old.Items[itemGroup] != group && inUse(old, now) {
		return "", fmt.Errorf("%w: %s is an instance of group %q, not of %q", ErrOtherGroup, instance, old.Items[itemGroup], group)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	v := &api.Variable{Namespace: s.Namespace, Path: path, Items: api.VariableItems{
		itemGroup: group, itemToken: hashToken(token), itemExpires: now.Add(ttl).UTC().Format(time.RFC3339),
	}}
	if request != "" {
		v.Items[itemRequest] = request
	}
	if old != nil {
		v.ModifyIndex = old.ModifyIndex
	}
	if again {
		// The tokens of the request so far stay until their own expiry.
		var keep []string
		for _, t := range tokensOf(old) {
			if now.Before(t.expires) {
				keep = append(keep, t.hash+"@"+t.expires.UTC().Format(time.RFC3339))
			}
		}
		if len(keep) > maxPrevious {
			keep = keep[len(keep)-maxPrevious:]
		}
		if len(keep) > 0 {
			v.Items[itemPrevious] = strings.Join(keep, ",")
		}
		for _, item := range []string{itemAddresses, itemSeen} {
			if val := old.Items[item]; val != "" {
				v.Items[item] = val
			}
		}
	}
	if _, err := s.Vars.WriteVariable(ctx, v); err != nil {
		return "", fmt.Errorf("writing the join record of %s: %w", instance, err)
	}
	return token, nil
}

// Redeem checks the token of an instance against the tokens of its record
// (the latest and the earlier ones of the same request, each with its own
// expiry): it returns the record, to be passed to Joined, and the group of
// the instance.
func (s *Store) Redeem(ctx context.Context, instance, token string) (*api.Variable, string, error) {
	v, err := s.read(ctx, instance)
	if err != nil {
		return nil, "", err
	}
	if v.Items[itemToken] == "" || v.Items[itemSerial] != "" {
		return nil, "", fmt.Errorf("%w: the token of %s was used", ErrRefused, instance)
	}
	held, ok := holds(v, hashToken(token))
	if !ok {
		return nil, "", fmt.Errorf("%w: a wrong token for %s", ErrRefused, instance)
	}
	if !s.now().Before(held.expires) {
		return nil, "", fmt.Errorf("%w: the token of %s expired", ErrRefused, instance)
	}
	return v, v.Items[itemGroup], nil
}

// Joined records the first certificate of an instance redeemed with the
// token; every token of the instance is void from now. It fails if a token
// of the instance was used meanwhile; a change of the record by the operator
// (its addresses, a token for the same request) is not a use.
func (s *Store) Joined(ctx context.Context, v *api.Variable, token, serial string) error {
	hash := hashToken(token)
	still := func(cur *api.Variable) bool {
		_, ok := holds(cur, hash)
		return ok && cur.Items[itemSerial] == ""
	}
	return s.updateRetry(ctx, v, still, func(items api.VariableItems) {
		delete(items, itemToken)
		delete(items, itemExpires)
		delete(items, itemPrevious)
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
// the instances of the group: those the provider lists, and those of the
// requests its listing has not all shown (a machine can join before the
// listing shows it; review finding CON-6i). Only the records of instances
// whose names start with namePrefix are read.
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
		// Expired: no token of the record can be used any more.
		now := s.now()
		expired := !slices.ContainsFunc(tokensOf(v), func(t heldToken) bool { return now.Before(t.expires) })
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
