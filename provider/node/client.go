// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package node

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
)

// Client calls a provider; it is the operator's side of the contract.
type Client struct {
	// Endpoint is the base URL of the provider (the endpoint of its
	// NodeProvider object).
	Endpoint string

	// Token is sent as a bearer token; empty sends none.
	Token string

	// HTTP is the client of the calls; nil uses one with a 30 second
	// timeout.
	HTTP *http.Client
}

var defaultHTTP = &http.Client{Timeout: 30 * time.Second}

// Group returns the instances of a group and the capacity of a machine of
// the kind the parameters name (sent as the query).
func (c *Client) Group(ctx context.Context, group string, params map[string]any) (*Group, error) {
	q := url.Values{}
	for k, v := range params {
		q.Set(k, fmt.Sprint(v))
	}
	path := "/v1/groups/" + url.PathEscape(group)
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	var g Group
	if err := c.do(ctx, http.MethodGet, path, nil, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

// Create asks for the instances of a request.
func (c *Client) Create(ctx context.Context, group string, req CreateRequest) ([]Instance, error) {
	var resp CreateResponse
	if err := c.do(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(group)+"/instances", req, &resp); err != nil {
		return nil, err
	}
	return resp.Instances, nil
}

// Delete deletes an instance.
func (c *Client) Delete(ctx context.Context, group, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/groups/"+url.PathEscape(group)+"/instances/"+url.PathEscape(id), nil, nil)
}

// do makes a call. A 404 is ErrUnknownGroup; another failure with a reason
// in the body is an *Error.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(data)
	}
	// The endpoint is the one a registered NodeProvider names: calling it is
	// the purpose of the client.
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.Endpoint, "/")+path, rd) //nolint:gosec // G704: see above
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	hc := c.HTTP
	if hc == nil {
		hc = defaultHTTP
	}
	resp, err := hc.Do(req) //nolint:gosec // G704: the endpoint of the registered provider
	if err != nil {
		return fmt.Errorf("calling the provider: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxRequestBody))
	if err != nil {
		return fmt.Errorf("reading the answer of the provider: %w", err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w: %s", ErrUnknownGroup, strings.TrimSpace(string(data)))
	}
	if resp.StatusCode >= 300 {
		var e Error
		if json.Unmarshal(data, &e) == nil && e.Reason != "" {
			return &e
		}
		return &Error{Reason: ReasonError, Message: fmt.Sprintf("the provider answered %s: %s", resp.Status, strings.TrimSpace(string(data)))}
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("the answer of the provider is not valid: %w", err)
	}
	return nil
}
