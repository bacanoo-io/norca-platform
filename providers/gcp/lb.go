// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package gcp implements a load balancer provider backed by the Google Compute
// Engine REST API v1 (/compute/v1/projects/{project}/regions/{region}/...).
//
// Each LoadBalancer object maps to four regional Compute Engine resources with
// the same deterministic RFC1035 name:
//   - a Regional TCP HealthCheck (`regionHealthChecks`),
//   - a Regional NetworkEndpointGroup (`regionNetworkEndpointGroups` of type
//     `NON_GCP_PRIVATE_IP_PORT`),
//   - a Regional BackendService (`regionBackendServices`), and
//   - a Regional ForwardingRule (`forwardingRules`).
//
// Both `EXTERNAL` and `INTERNAL` passthrough TCP load balancers are supported,
// and `network_tier` (`PREMIUM` or `STANDARD`) can be adjusted dynamically via
// the LoadBalancerClass object.
//
// Type ID: "gcp".
package gcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// TypeID is the ID of the type.
const TypeID = "gcp"

const defaultEndpoint = "https://compute.googleapis.com"

var classParams = []params.Param{
	{Name: "network", Type: params.ParamString, Default: "default",
		Description: "VPC network name or URL used for network endpoint groups and internal load balancers"},
	{Name: "subnetwork", Type: params.ParamString, Default: "",
		Description: "optional subnetwork name or URL for internal forwarding rules"},
	{Name: "network_tier", Type: params.ParamString, Default: "PREMIUM", Dynamic: true,
		Enum:        []string{"PREMIUM", "STANDARD"},
		Description: "network tier for external forwarding rules"},
	{Name: "name_prefix", Type: params.ParamString, Default: "norca-",
		Description: "prefix added to Compute Engine resource names"},
}

// Provider manages Google Cloud regional TCP load balancers for one LoadBalancerClass.
type Provider struct {
	class      string
	project    string
	region     string
	token      string
	network    string
	subnetwork string
	namePrefix string
	endpoint   string
	caps       lb.Capabilities
	log        *slog.Logger
	client     *http.Client
}

// NewLB creates a Google Cloud Compute Engine provider instance.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	project := strings.TrimSpace(cfg.Values.String("project"))
	if project == "" {
		return nil, fmt.Errorf("project must not be empty")
	}
	region := strings.TrimSpace(cfg.Values.String("region"))
	if region == "" {
		return nil, fmt.Errorf("region must not be empty")
	}
	token := strings.TrimSpace(cfg.Values.String("token"))
	if token == "" {
		return nil, fmt.Errorf("token must not be empty")
	}
	network := strings.TrimSpace(cfg.Values.String("network"))
	if network == "" {
		network = "default"
	}
	subnetwork := strings.TrimSpace(cfg.Values.String("subnetwork"))
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Values.String("endpoint")), "/")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint URL %q", endpoint)
	}

	return &Provider{
		class:      cfg.Class,
		project:    project,
		region:     region,
		token:      token,
		network:    network,
		subnetwork: subnetwork,
		namePrefix: cfg.Values.String("name_prefix"),
		endpoint:   endpoint,
		caps: lb.Capabilities{
			Internal: true,
		},
		log:    cfg.Log().With("provider", TypeID, "class", cfg.Class, "project", project, "region", region),
		client: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// Capabilities implements lb.Provider.
func (p *Provider) Capabilities() lb.Capabilities { return p.caps }

// Ensure implements lb.Provider.
func (p *Provider) Ensure(ctx context.Context, req lb.Request) (lb.State, error) {
	if req.Port <= 0 || req.Port > 65535 {
		return lb.State{}, lb.Invalid("invalid frontend port %d", req.Port)
	}
	dstPort, endpoints, err := normalizeEndpoints(req.Port, req.Targets)
	if err != nil {
		return lb.State{}, err
	}

	name := p.resourceName(req.Ref)
	scheme := "EXTERNAL"
	if req.Internal {
		scheme = "INTERNAL"
	}
	tier := strings.TrimSpace(req.Params.String("network_tier"))
	if tier == "" {
		tier = "PREMIUM"
	}

	hcLink, err := p.ensureHealthCheck(ctx, name, dstPort)
	if err != nil {
		return lb.State{}, err
	}

	negLink, err := p.ensureNEG(ctx, name, dstPort, endpoints)
	if err != nil {
		return lb.State{}, err
	}

	bsLink, err := p.ensureBackendService(ctx, name, scheme, hcLink, negLink)
	if err != nil {
		return lb.State{}, err
	}

	fr, err := p.ensureForwardingRule(ctx, name, scheme, tier, req.Port, bsLink)
	if err != nil {
		return lb.State{}, err
	}

	state := lb.State{
		Resource: fmt.Sprintf("gcp/%s/%s/%s", p.project, p.region, name),
	}
	if fr.IPAddress != "" {
		state.Addresses = []string{net.JoinHostPort(fr.IPAddress, strconv.Itoa(req.Port))}
	}
	switch {
	case len(endpoints) == 0:
		state.Message = "no targets"
	case len(state.Addresses) == 0:
		state.Message = "waiting for forwarding rule IP address"
	default:
		state.Ready = true
	}
	return state, nil
}

// Delete implements lb.Provider.
func (p *Provider) Delete(ctx context.Context, ref lb.Ref) error {
	name := p.resourceName(ref)
	base := p.regionBase()
	for _, resourcePath := range []string{
		base + "/forwardingRules/" + url.PathEscape(name),
		base + "/backendServices/" + url.PathEscape(name),
		base + "/networkEndpointGroups/" + url.PathEscape(name),
		base + "/healthChecks/" + url.PathEscape(name),
	} {
		if err := p.doJSON(ctx, http.MethodDelete, resourcePath, nil, nil, true); err != nil {
			return err
		}
	}
	return nil
}

type gcpEndpoint struct {
	IPAddress string `json:"ipAddress"`
	Port      int    `json:"port"`
}

func normalizeEndpoints(frontendPort int, targets []lb.Target) (int, []gcpEndpoint, error) {
	dstPort := frontendPort
	seen := make(map[gcpEndpoint]bool, len(targets))
	out := make([]gcpEndpoint, 0, len(targets))
	for i, t := range targets {
		if t.Port <= 0 || t.Port > 65535 {
			return 0, nil, lb.Invalid("invalid target port %d", t.Port)
		}
		if i == 0 {
			dstPort = t.Port
		}
		ip := strings.TrimSpace(t.Address)
		if net.ParseIP(ip) == nil {
			return 0, nil, lb.Invalid("target address %q is not a valid IP address", ip)
		}
		ep := gcpEndpoint{IPAddress: ip, Port: t.Port}
		if !seen[ep] {
			seen[ep] = true
			out = append(out, ep)
		}
	}
	slices.SortFunc(out, func(a, b gcpEndpoint) int {
		if c := strings.Compare(a.IPAddress, b.IPAddress); c != 0 {
			return c
		}
		return a.Port - b.Port
	})
	return dstPort, out, nil
}

// resourceName produces a valid RFC1035 Compute Engine resource name (<= 63 chars,
// starting with a lowercase letter).
func (p *Provider) resourceName(ref lb.Ref) string {
	raw := strings.ToLower(p.namePrefix + ref.Namespace + "-" + ref.Name)
	var b strings.Builder
	prevHyphen := false
	for _, r := range raw {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			prevHyphen = false
		default:
			if b.Len() > 0 && !prevHyphen {
				b.WriteByte('-')
				prevHyphen = true
			}
		}
	}
	s := strings.Trim(b.String(), "-")
	if s == "" || (s[0] < 'a' || s[0] > 'z') {
		s = "lb-" + s
	}
	if len(s) > 63 {
		sum := sha256.Sum256([]byte(ref.String()))
		suffix := hex.EncodeToString(sum[:])[:6]
		s = strings.TrimRight(s[:56], "-") + "-" + suffix
	}
	return s
}

func (p *Provider) regionBase() string {
	return fmt.Sprintf("/compute/v1/projects/%s/regions/%s", url.PathEscape(p.project), url.PathEscape(p.region))
}

func (p *Provider) networkURL() string {
	if strings.Contains(p.network, "/") {
		return p.network
	}
	return fmt.Sprintf("projects/%s/global/networks/%s", p.project, p.network)
}

func (p *Provider) subnetworkURL() string {
	if p.subnetwork == "" || strings.Contains(p.subnetwork, "/") {
		return p.subnetwork
	}
	return fmt.Sprintf("projects/%s/regions/%s/subnetworks/%s", p.project, p.region, p.subnetwork)
}

// Compute Engine REST v1 wire structures.

type gcpHealthCheck struct {
	Name           string `json:"name"`
	SelfLink       string `json:"selfLink,omitempty"`
	Type           string `json:"type"`
	TCPHealthCheck *struct {
		Port int `json:"port"`
	} `json:"tcpHealthCheck,omitempty"`
}

type gcpNEG struct {
	Name                string `json:"name"`
	SelfLink            string `json:"selfLink,omitempty"`
	NetworkEndpointType string `json:"networkEndpointType"`
	Network             string `json:"network,omitempty"`
	DefaultPort         int    `json:"defaultPort,omitempty"`
}

type gcpListEndpointsResponse struct {
	Items []struct {
		NetworkEndpoint gcpEndpoint `json:"networkEndpoint"`
	} `json:"items"`
}

type gcpBackendService struct {
	Name                string       `json:"name"`
	SelfLink            string       `json:"selfLink,omitempty"`
	Protocol            string       `json:"protocol"`
	LoadBalancingScheme string       `json:"loadBalancingScheme"`
	HealthChecks        []string     `json:"healthChecks"`
	Backends            []gcpBackend `json:"backends,omitempty"`
	Network             string       `json:"network,omitempty"`
}

type gcpBackend struct {
	Group string `json:"group"`
}

type gcpForwardingRule struct {
	Name                string   `json:"name"`
	SelfLink            string   `json:"selfLink,omitempty"`
	IPAddress           string   `json:"IPAddress,omitempty"`
	IPProtocol          string   `json:"IPProtocol"`
	Ports               []string `json:"ports"`
	LoadBalancingScheme string   `json:"loadBalancingScheme"`
	BackendService      string   `json:"backendService"`
	NetworkTier         string   `json:"networkTier,omitempty"`
	Network             string   `json:"network,omitempty"`
	Subnetwork          string   `json:"subnetwork,omitempty"`
}

type gcpErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (p *Provider) ensureHealthCheck(ctx context.Context, name string, port int) (string, error) {
	base := p.regionBase() + "/healthChecks"
	itemPath := base + "/" + url.PathEscape(name)
	selfLink := p.endpoint + itemPath

	var existing gcpHealthCheck
	found, err := p.getOptional(ctx, itemPath, &existing)
	if err != nil {
		return "", err
	}
	body := map[string]any{
		"name":           name,
		"type":           "TCP",
		"tcpHealthCheck": map[string]any{"port": port},
	}
	if !found {
		if err := p.doJSON(ctx, http.MethodPost, base, body, nil, false); err != nil {
			return "", err
		}
		return selfLink, nil
	}
	if existing.TCPHealthCheck == nil || existing.TCPHealthCheck.Port != port {
		if err := p.doJSON(ctx, http.MethodPut, itemPath, body, nil, false); err != nil {
			return "", err
		}
	}
	if existing.SelfLink != "" {
		return existing.SelfLink, nil
	}
	return selfLink, nil
}

func (p *Provider) ensureNEG(ctx context.Context, name string, dstPort int, desired []gcpEndpoint) (string, error) {
	base := p.regionBase() + "/networkEndpointGroups"
	itemPath := base + "/" + url.PathEscape(name)
	selfLink := p.endpoint + itemPath

	var existing gcpNEG
	found, err := p.getOptional(ctx, itemPath, &existing)
	if err != nil {
		return "", err
	}
	if !found {
		body := gcpNEG{
			Name:                name,
			NetworkEndpointType: "NON_GCP_PRIVATE_IP_PORT",
			Network:             p.networkURL(),
			DefaultPort:         dstPort,
		}
		if err := p.doJSON(ctx, http.MethodPost, base, body, nil, false); err != nil {
			return "", err
		}
	} else if existing.SelfLink != "" {
		selfLink = existing.SelfLink
	}

	var listResp gcpListEndpointsResponse
	if err := p.doJSON(ctx, http.MethodPost, itemPath+"/listNetworkEndpoints", map[string]any{}, &listResp, false); err != nil {
		return "", err
	}

	currentSet := make(map[gcpEndpoint]bool, len(listResp.Items))
	for _, it := range listResp.Items {
		currentSet[it.NetworkEndpoint] = true
	}
	desiredSet := make(map[gcpEndpoint]bool, len(desired))
	var toAttach []gcpEndpoint
	for _, ep := range desired {
		desiredSet[ep] = true
		if !currentSet[ep] {
			toAttach = append(toAttach, ep)
		}
	}
	var toDetach []gcpEndpoint
	for ep := range currentSet {
		if !desiredSet[ep] {
			toDetach = append(toDetach, ep)
		}
	}

	if len(toAttach) > 0 {
		if err := p.doJSON(ctx, http.MethodPost, itemPath+"/attachNetworkEndpoints", map[string]any{
			"networkEndpoints": toAttach,
		}, nil, false); err != nil {
			return "", err
		}
	}
	if len(toDetach) > 0 {
		if err := p.doJSON(ctx, http.MethodPost, itemPath+"/detachNetworkEndpoints", map[string]any{
			"networkEndpoints": toDetach,
		}, nil, false); err != nil {
			return "", err
		}
	}
	return selfLink, nil
}

func (p *Provider) ensureBackendService(ctx context.Context, name, scheme, hcLink, negLink string) (string, error) {
	base := p.regionBase() + "/backendServices"
	itemPath := base + "/" + url.PathEscape(name)
	selfLink := p.endpoint + itemPath

	var existing gcpBackendService
	found, err := p.getOptional(ctx, itemPath, &existing)
	if err != nil {
		return "", err
	}
	if found && existing.LoadBalancingScheme != "" && existing.LoadBalancingScheme != scheme {
		// LoadBalancingScheme is immutable in Compute Engine; delete ForwardingRule & BackendService first.
		_ = p.doJSON(ctx, http.MethodDelete, p.regionBase()+"/forwardingRules/"+url.PathEscape(name), nil, nil, true)
		if err := p.doJSON(ctx, http.MethodDelete, itemPath, nil, nil, true); err != nil {
			return "", err
		}
		found = false
	}

	desired := gcpBackendService{
		Name:                name,
		Protocol:            "TCP",
		LoadBalancingScheme: scheme,
		HealthChecks:        []string{hcLink},
		Backends:            []gcpBackend{{Group: negLink}},
	}
	if scheme == "INTERNAL" {
		desired.Network = p.networkURL()
	}

	if !found {
		if err := p.doJSON(ctx, http.MethodPost, base, desired, nil, false); err != nil {
			return "", err
		}
		return selfLink, nil
	}
	if len(existing.Backends) != 1 || existing.Backends[0].Group != negLink || len(existing.HealthChecks) != 1 || existing.HealthChecks[0] != hcLink {
		if err := p.doJSON(ctx, http.MethodPut, itemPath, desired, nil, false); err != nil {
			return "", err
		}
	}
	if existing.SelfLink != "" {
		return existing.SelfLink, nil
	}
	return selfLink, nil
}

func (p *Provider) ensureForwardingRule(ctx context.Context, name, scheme, tier string, port int, bsLink string) (*gcpForwardingRule, error) {
	base := p.regionBase() + "/forwardingRules"
	itemPath := base + "/" + url.PathEscape(name)
	portStr := strconv.Itoa(port)

	var existing gcpForwardingRule
	found, err := p.getOptional(ctx, itemPath, &existing)
	if err != nil {
		return nil, err
	}
	if found {
		samePort := len(existing.Ports) == 1 && existing.Ports[0] == portStr
		sameScheme := existing.LoadBalancingScheme == "" || existing.LoadBalancingScheme == scheme
		sameTier := scheme == "INTERNAL" || existing.NetworkTier == "" || existing.NetworkTier == tier
		if samePort && sameScheme && sameTier {
			return &existing, nil
		}
		if err := p.doJSON(ctx, http.MethodDelete, itemPath, nil, nil, true); err != nil {
			return nil, err
		}
	}

	desired := gcpForwardingRule{
		Name:                name,
		IPProtocol:          "TCP",
		Ports:               []string{portStr},
		LoadBalancingScheme: scheme,
		BackendService:      bsLink,
	}
	if scheme == "INTERNAL" {
		desired.Network = p.networkURL()
		if sub := p.subnetworkURL(); sub != "" {
			desired.Subnetwork = sub
		}
	} else {
		desired.NetworkTier = tier
	}

	if err := p.doJSON(ctx, http.MethodPost, base, desired, nil, false); err != nil {
		return nil, err
	}
	if _, err := p.getOptional(ctx, itemPath, &existing); err != nil {
		return nil, err
	}
	return &existing, nil
}

func (p *Provider) getOptional(ctx context.Context, path string, out any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+path, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Accept", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("gcp GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, parseGCPError(http.MethodGet, path, resp.StatusCode, raw)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return false, fmt.Errorf("decoding gcp response: %w", err)
		}
	}
	return true, nil
}

func (p *Provider) doJSON(ctx context.Context, method, path string, reqBody, respBody any, ignoreNotFound bool) error {
	var bodyReader io.Reader
	if reqBody != nil {
		data, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("encoding request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, p.endpoint+path, bodyReader)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("gcp %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("reading gcp response: %w", err)
	}
	if ignoreNotFound && resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseGCPError(method, path, resp.StatusCode, raw)
	}
	if respBody != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("decoding gcp response: %w", err)
		}
	}
	return nil
}

func parseGCPError(method, path string, status int, raw []byte) error {
	var apiErr gcpErrorResponse
	if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error.Message != "" {
		if status == http.StatusBadRequest {
			return lb.Invalid("gcp %s %s: %s", method, path, apiErr.Error.Message)
		}
		return fmt.Errorf("gcp %s %s: status %d: %s", method, path, status, apiErr.Error.Message)
	}
	return fmt.Errorf("gcp %s %s: status %d: %s", method, path, status, strings.TrimSpace(string(raw)))
}
