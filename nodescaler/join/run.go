// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join

import (
	"context"
	"errors"
	"time"
)

// Runner is the loop of norca-join run: join when the node has no valid
// certificate, start the agent once, and renew the certificate at two
// thirds of its life, telling the agent to reload it.
type Runner struct {
	Client     *Client
	ConfigPath string

	// Start starts the agent, once the node has a certificate. Reload makes
	// it load the renewed certificate (SIGHUP: the agent keeps its
	// connections).
	Start  func(ctx context.Context) error
	Reload func() error

	// Retry is the pause after a failed join or renewal that may succeed
	// later (an endpoint that does not answer); it doubles up to
	// MaxRetry (defaults 5s and 5m).
	Retry, MaxRetry time.Duration
	// After is time.After; tests replace it.
	After func(d time.Duration) <-chan time.Time
}

func (r *Runner) after(d time.Duration) <-chan time.Time {
	if r.After != nil {
		return r.After(d)
	}
	return time.After(d)
}

// Run runs until ctx ends. It returns an error that retrying does not
// mend: a refused join (a used or expired token), a node without a
// certificate and a token, or an agent that would not start.
func (r *Runner) Run(ctx context.Context) error {
	c := r.Client
	retry, maxRetry := r.Retry, r.MaxRetry
	if retry <= 0 {
		retry = 5 * time.Second
	}
	if maxRetry <= 0 {
		maxRetry = 5 * time.Minute
	}

	// The certificate: the one on the node, or a join.
	pause := retry
	for {
		if _, _, err := c.Current(); err == nil {
			break
		}
		err := c.Join(ctx, r.ConfigPath)
		if err == nil {
			break
		}
		if _, refused := errors.AsType[*RefusedError](err); refused || errors.Is(err, ErrNoToken) {
			return err
		}
		c.log().Warn("the join failed, retrying", "error", err, "in", pause)
		select {
		case <-ctx.Done():
			return nil
		case <-r.after(pause):
		}
		pause = min(pause*2, maxRetry)
	}
	if r.Start != nil {
		if err := r.Start(ctx); err != nil {
			return err
		}
	}

	// The renewals.
	for {
		_, leaf, err := c.Current()
		wait := time.Duration(0)
		if err == nil {
			wait = c.RenewAt(leaf).Sub(c.now())
		}
		if wait > 0 {
			c.log().Info("the next renewal", "at", c.now().Add(wait).UTC().Format(time.RFC3339))
			select {
			case <-ctx.Done():
				return nil
			case <-r.after(wait):
			}
		}
		pause := retry
		for {
			err := c.Renew(ctx)
			if err == nil {
				break
			}
			if _, refused := errors.AsType[*RefusedError](err); refused {
				// Not the latest certificate of the instance, or an
				// instance without a record: the operator has to act. The
				// node keeps working until its certificate expires.
				c.log().Error("the renewal was refused", "error", err)
				pause = maxRetry
			} else {
				c.log().Warn("the renewal failed, retrying", "error", err, "in", pause)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-r.after(pause):
			}
			pause = min(pause*2, maxRetry)
		}
		if r.Reload != nil {
			if err := r.Reload(); err != nil {
				c.log().Error("the agent did not reload the renewed certificate", "error", err)
			}
		}
	}
}
