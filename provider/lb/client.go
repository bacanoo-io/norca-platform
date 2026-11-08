// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package lb

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

// Client calls the provider of a class; it is the operator's side of the
// contract.
type Client struct {
	// Endpoint is the base URL of the class (the endpoint of its
	// LoadBalancerClass object).
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

// Ensure creates or updates a load balancer and returns its state.
func (c *Client) Ensure(ctx context.Context, ref Ref, req EnsureRequest) (*EnsureResponse, error) {
	if req.Name == "" {
		req.Name = ref.Name
	}
	if req.Namespace == "" {
		req.Namespace = ref.Namespace
	}
	if req.Targets == nil {
		req.Targets = []WireTarget{}
	}
	data, err := c.do(ctx, http.MethodPut, ref, req)
	if err != nil {
		return nil, err
	}
	var out EnsureResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("invalid answer: %w", err)
	}
	return &out, nil
}

// Delete removes a load balancer; one the provider does not know (404) is
// not an error.
func (c *Client) Delete(ctx context.Context, ref Ref) error {
	_, err := c.do(ctx, http.MethodDelete, ref, nil)
	if e, ok := err.(*ResponseError); ok && e.Status == http.StatusNotFound { //nolint:errorlint // do returns it unwrapped
		return nil
	}
	return err
}

func (c *Client) url(ref Ref) string {
	return strings.TrimRight(c.Endpoint, "/") + "/v1/loadbalancers/" +
		url.PathEscape(ref.Namespace) + "/" + url.PathEscape(ref.Name)
}

func (c *Client) do(ctx context.Context, method string, ref Ref, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	// The endpoint is the one a registered LoadBalancerClass names: calling
	// it is the purpose of the client.
	req, err := http.NewRequestWithContext(ctx, method, c.url(ref), rd) //nolint:gosec // G704: see above
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	hc := c.HTTP
	if hc == nil {
		hc = defaultHTTP
	}
	resp, err := hc.Do(req) //nolint:gosec // G704: the endpoint of the registered class
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		e := &ResponseError{Status: resp.StatusCode, Message: snippet(data)}
		var we wire.Error
		if json.Unmarshal(data, &we) == nil && we.Message != "" {
			e.Reason, e.Message = we.Reason, we.Message
		}
		return nil, e
	}
	return data, nil
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}
