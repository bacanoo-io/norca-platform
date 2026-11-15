// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dns

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/wire"
)

// Client calls the provider of a zone; it is the operator's side of the
// contract.
type Client struct {
	// Endpoint is the base URL of the zone (the endpoint of its DNSZone
	// object).
	Endpoint string

	// Token is sent as a bearer token; empty sends none.
	Token string

	// HTTP is the client of the calls; nil uses one with a 30 second
	// timeout.
	HTTP *http.Client
}

var defaultHTTP = &http.Client{Timeout: 30 * time.Second}

// ResponseError is an answer of the provider that is not a success.
type ResponseError struct {
	Status int

	// Reason and Message are those of the error body (package wire); a
	// body of another shape is the message.
	Reason  string
	Message string
}

func (e *ResponseError) Error() string {
	return fmt.Sprintf("answered %d: %s", e.Status, e.Message)
}

// Invalid reports whether the provider refused the request as one it
// cannot satisfy (the same request fails again).
func (e *ResponseError) Invalid() bool {
	return e.Reason == wire.ReasonInvalid || e.Status == http.StatusUnprocessableEntity
}

// Present adds the value to the TXT records of the name. A TTL of 0 leaves
// the time to live to the provider.
func (c *Client) Present(ctx context.Context, rec Record) error {
	return c.do(ctx, http.MethodPut, rec)
}

// CleanUp removes the value from the TXT records of the name.
func (c *Client) CleanUp(ctx context.Context, rec Record) error {
	return c.do(ctx, http.MethodDelete, rec)
}

func (c *Client) do(ctx context.Context, method string, rec Record) error {
	body, err := json.Marshal(RecordRequest{Value: rec.Value, TTL: rec.TTL})
	if err != nil {
		return err
	}
	target := strings.TrimRight(c.Endpoint, "/") + "/v1/txt/" + url.PathEscape(rec.Name)
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	hc := c.HTTP
	if hc == nil {
		hc = defaultHTTP
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		e := &ResponseError{Status: resp.StatusCode, Message: snippet(data)}
		var we wire.Error
		if json.Unmarshal(data, &we) == nil && we.Message != "" {
			e.Reason, e.Message = we.Reason, we.Message
		}
		return e
	}
	return nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}
