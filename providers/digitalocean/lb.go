// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package digitalocean implements a load balancer provider backed by the
// DigitalOcean REST API v2 (/v2/load_balancers and /v2/droplets).
//
// Each LoadBalancer object maps to one DigitalOcean Load Balancer named
// "<name_prefix><namespace>-<name>" in the configured region (and optional
// VPC). Target IP addresses are resolved to DigitalOcean Droplet IDs via
// GET /v2/droplets, and the load balancer's forwarding rules, TCP health check,
// droplet_ids, algorithm, and size_unit are reconciled idempotently.
//
// Type ID: "digitalocean".
package digitalocean

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
const TypeID = "digitalocean"

const defaultEndpoint = "https://api.digitalocean.com/v2"

var classParams = []params.Param{
	{Name: "region", Type: params.ParamString, Default: "fra1",
		Description: "DigitalOcean region slug (e.g. fra1, ams3, lon1, nyc1, nyc3, sfo3)"},
	{Name: "size_unit", Type: params.ParamInt, Default: 1, Dynamic: true,
		Description: "number of load balancer nodes (size_unit >= 1)"},
	{Name: "algorithm", Type: params.ParamString, Default: "round_robin", Dynamic: true,
		Enum:        []string{"round_robin", "least_connections"},
		Description: "load balancing algorithm"},
	{Name: "vpc_uuid", Type: params.ParamString, Default: "",
		Description: "DigitalOcean VPC UUID in which load balancers are placed (required for internal load balancers)"},
	{Name: "tag", Type: params.ParamString, Default: "",
		Description: "optional DigitalOcean Droplet tag; when empty, target IPs are resolved to droplet_ids"},
	{Name: "enable_proxy_protocol", Type: params.ParamBool, Default: false, Dynamic: true,
		Description: "enable PROXY protocol on the DigitalOcean load balancer"},
	{Name: "name_prefix", Type: params.ParamString, Default: "norca-",
		Description: "prefix added to DigitalOcean load balancer names"},
}

// Provider manages DigitalOcean load balancers for one LoadBalancerClass.
type Provider struct {
	class      string
	token      string
	region     string
	vpcUUID    string
	tag        string
	namePrefix string
	endpoint   string
	caps       lb.Capabilities
	log        *slog.Logger
	client     *http.Client
}

// NewLB creates a DigitalOcean provider instance.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	token := strings.TrimSpace(cfg.Values.String("token"))
	if token == "" {
		return nil, fmt.Errorf("token must not be empty")
	}
	region := strings.TrimSpace(cfg.Values.String("region"))
	if region == "" {
		return nil, fmt.Errorf("region must not be empty")
	}
	if cfg.Values.Int("size_unit") <= 0 {
		return nil, fmt.Errorf("size_unit must be >= 1")
	}
	vpcUUID := strings.TrimSpace(cfg.Values.String("vpc_uuid"))
	tag := strings.TrimSpace(cfg.Values.String("tag"))
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
		token:      token,
		region:     region,
		vpcUUID:    vpcUUID,
		tag:        tag,
		namePrefix: cfg.Values.String("name_prefix"),
		endpoint:   endpoint,
		caps: lb.Capabilities{
			Internal: vpcUUID != "",
		},
		log:    cfg.Log().With("provider", TypeID, "class", cfg.Class, "region", region),
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
	if req.Internal && p.vpcUUID == "" {
		return lb.State{}, lb.Invalid("internal load balancer requires vpc_uuid")
	}
	dstPort, targetIPs, err := extractTargets(req.Port, req.Targets)
	if err != nil {
		return lb.State{}, err
	}

	sizeUnit := req.Params.Int("size_unit")
	if sizeUnit <= 0 {
		sizeUnit = 1
	}
	alg := strings.TrimSpace(req.Params.String("algorithm"))
	if alg == "" {
		alg = "round_robin"
	}
	proxyProto := req.Params.Bool("enable_proxy_protocol")

	var dropletIDs []int
	if p.tag == "" && len(targetIPs) > 0 {
		dropletIDs, err = p.resolveDropletIDs(ctx, targetIPs)
		if err != nil {
			return lb.State{}, err
		}
	}

	lbName := p.resourceName(req.Ref)
	curLB, err := p.findByName(ctx, lbName)
	if err != nil {
		return lb.State{}, err
	}

	spec := p.buildSpec(req, lbName, sizeUnit, alg, proxyProto, dstPort, dropletIDs)
	if curLB == nil {
		var resp doSingleLBResponse
		if err := p.doJSON(ctx, http.MethodPost, "/load_balancers", spec, &resp, false); err != nil {
			return lb.State{}, err
		}
		curLB = &resp.LoadBalancer
		p.log.Info("created DigitalOcean load balancer", "name", lbName, "id", curLB.ID)
	} else if needsUpdate(curLB, spec) {
		var resp doSingleLBResponse
		path := "/load_balancers/" + url.PathEscape(curLB.ID)
		if err := p.doJSON(ctx, http.MethodPut, path, spec, &resp, false); err != nil {
			return lb.State{}, err
		}
		curLB = &resp.LoadBalancer
	}

	state := lb.State{
		Resource: "digitalocean/lb/" + curLB.ID,
	}
	if curLB.IP != "" {
		state.Addresses = []string{net.JoinHostPort(curLB.IP, strconv.Itoa(req.Port))}
	}
	switch {
	case curLB.Status != "" && curLB.Status != "active":
		state.Message = fmt.Sprintf("load balancer status is %s", curLB.Status)
	case len(req.Targets) == 0 && p.tag == "":
		state.Message = "no targets"
	case len(state.Addresses) == 0:
		state.Message = "waiting for load balancer IP assignment"
	default:
		state.Ready = true
	}
	return state, nil
}

// Delete implements lb.Provider.
func (p *Provider) Delete(ctx context.Context, ref lb.Ref) error {
	lbName := p.resourceName(ref)
	curLB, err := p.findByName(ctx, lbName)
	if err != nil {
		return err
	}
	if curLB == nil {
		return nil
	}
	return p.doJSON(ctx, http.MethodDelete, "/load_balancers/"+url.PathEscape(curLB.ID), nil, nil, true)
}

func extractTargets(frontendPort int, targets []lb.Target) (int, []string, error) {
	dstPort := frontendPort
	seen := make(map[string]bool, len(targets))
	ips := make([]string, 0, len(targets))
	for i, t := range targets {
		if t.Port <= 0 || t.Port > 65535 {
			return 0, nil, lb.Invalid("invalid target port %d", t.Port)
		}
		if i == 0 {
			dstPort = t.Port
		} else if t.Port != dstPort {
			return 0, nil, lb.Invalid("digitalocean requires all targets of a load balancer to use the same port (got %d and %d)", dstPort, t.Port)
		}
		addr := strings.TrimSpace(t.Address)
		if addr == "" {
			return 0, nil, lb.Invalid("target address must not be empty")
		}
		if !seen[addr] {
			seen[addr] = true
			ips = append(ips, addr)
		}
	}
	slices.Sort(ips)
	return dstPort, ips, nil
}

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
	if s == "" {
		s = "lb"
	}
	if len(s) > 63 {
		sum := sha256.Sum256([]byte(ref.String()))
		suffix := hex.EncodeToString(sum[:])[:6]
		s = strings.TrimRight(s[:56], "-") + "-" + suffix
	}
	return s
}

// DigitalOcean API v2 wire types (matching github.com/digitalocean/godo).

type doLoadBalancer struct {
	ID                  string             `json:"id,omitempty"`
	Name                string             `json:"name"`
	IP                  string             `json:"ip,omitempty"`
	SizeUnit            int                `json:"size_unit,omitempty"`
	Algorithm           string             `json:"algorithm,omitempty"`
	Status              string             `json:"status,omitempty"`
	ForwardingRules     []doForwardingRule `json:"forwarding_rules"`
	HealthCheck         doHealthCheck      `json:"health_check"`
	Region              any                `json:"region,omitempty"`
	VPCUUID             string             `json:"vpc_uuid,omitempty"`
	Type                string             `json:"type,omitempty"`
	DropletIDs          []int              `json:"droplet_ids,omitempty"`
	Tag                 string             `json:"tag,omitempty"`
	EnableProxyProtocol bool               `json:"enable_proxy_protocol,omitempty"`
}

type doForwardingRule struct {
	EntryProtocol  string `json:"entry_protocol"`
	EntryPort      int    `json:"entry_port"`
	TargetProtocol string `json:"target_protocol"`
	TargetPort     int    `json:"target_port"`
}

type doHealthCheck struct {
	Protocol               string `json:"protocol"`
	Port                   int    `json:"port"`
	CheckIntervalSeconds   int    `json:"check_interval_seconds"`
	ResponseTimeoutSeconds int    `json:"response_timeout_seconds"`
	UnhealthyThreshold     int    `json:"unhealthy_threshold"`
	HealthyThreshold       int    `json:"healthy_threshold"`
}

type doListLBsResponse struct {
	LoadBalancers []doLoadBalancer `json:"load_balancers"`
}

type doSingleLBResponse struct {
	LoadBalancer doLoadBalancer `json:"load_balancer"`
}

type doDroplet struct {
	ID       int `json:"id"`
	Networks struct {
		V4 []struct {
			IPAddress string `json:"ip_address"`
			Type      string `json:"type"`
		} `json:"v4"`
		V6 []struct {
			IPAddress string `json:"ip_address"`
			Type      string `json:"type"`
		} `json:"v6"`
	} `json:"networks"`
}

type doListDropletsResponse struct {
	Droplets []doDroplet `json:"droplets"`
}

type doErrorResponse struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

func (p *Provider) buildSpec(
	req lb.Request,
	name string,
	sizeUnit int,
	alg string,
	proxyProto bool,
	dstPort int,
	dropletIDs []int,
) doLoadBalancer {
	lbType := "REGIONAL"
	if req.Internal {
		lbType = "REGIONAL_NETWORK"
	}
	spec := doLoadBalancer{
		Name:                name,
		Region:              p.region,
		SizeUnit:            sizeUnit,
		Algorithm:           alg,
		VPCUUID:             p.vpcUUID,
		Type:                lbType,
		EnableProxyProtocol: proxyProto,
		ForwardingRules: []doForwardingRule{{
			EntryProtocol:  "tcp",
			EntryPort:      req.Port,
			TargetProtocol: "tcp",
			TargetPort:     dstPort,
		}},
		HealthCheck: doHealthCheck{
			Protocol:               "tcp",
			Port:                   dstPort,
			CheckIntervalSeconds:   10,
			ResponseTimeoutSeconds: 5,
			UnhealthyThreshold:     3,
			HealthyThreshold:       5,
		},
	}
	if p.tag != "" {
		spec.Tag = p.tag
	} else {
		spec.DropletIDs = dropletIDs
	}
	return spec
}

func needsUpdate(cur *doLoadBalancer, want doLoadBalancer) bool {
	if cur.SizeUnit != 0 && cur.SizeUnit != want.SizeUnit {
		return true
	}
	if cur.Algorithm != "" && cur.Algorithm != want.Algorithm {
		return true
	}
	if cur.EnableProxyProtocol != want.EnableProxyProtocol {
		return true
	}
	if len(cur.ForwardingRules) != 1 || cur.ForwardingRules[0] != want.ForwardingRules[0] {
		return true
	}
	if cur.HealthCheck.Port != want.HealthCheck.Port || cur.HealthCheck.Protocol != "tcp" {
		return true
	}
	if want.Tag != "" {
		return cur.Tag != want.Tag
	}
	curIDs := slices.Clone(cur.DropletIDs)
	slices.Sort(curIDs)
	return !slices.Equal(curIDs, want.DropletIDs)
}

func (p *Provider) findByName(ctx context.Context, name string) (*doLoadBalancer, error) {
	var resp doListLBsResponse
	if err := p.doJSON(ctx, http.MethodGet, "/load_balancers?per_page=200", nil, &resp, false); err != nil {
		return nil, err
	}
	for i := range resp.LoadBalancers {
		if resp.LoadBalancers[i].Name == name {
			return &resp.LoadBalancers[i], nil
		}
	}
	return nil, nil
}

func (p *Provider) resolveDropletIDs(ctx context.Context, targetIPs []string) ([]int, error) {
	var resp doListDropletsResponse
	if err := p.doJSON(ctx, http.MethodGet, "/droplets?per_page=200", nil, &resp, false); err != nil {
		return nil, err
	}
	ipToID := make(map[string]int, len(resp.Droplets)*2)
	for _, d := range resp.Droplets {
		for _, n := range d.Networks.V4 {
			if n.IPAddress != "" {
				ipToID[n.IPAddress] = d.ID
			}
		}
		for _, n := range d.Networks.V6 {
			if n.IPAddress != "" {
				ipToID[n.IPAddress] = d.ID
			}
		}
	}

	seen := make(map[int]bool, len(targetIPs))
	ids := make([]int, 0, len(targetIPs))
	for _, ip := range targetIPs {
		id, ok := ipToID[ip]
		if !ok {
			return nil, fmt.Errorf("digitalocean droplet with IP %s not found", ip)
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
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
		return fmt.Errorf("digitalocean %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("reading digitalocean response: %w", err)
	}

	if ignoreNotFound && resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr doErrorResponse
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Message != "" {
			if resp.StatusCode == http.StatusUnprocessableEntity || apiErr.ID == "unprocessable_entity" {
				return lb.Invalid("digitalocean %s %s: %s", method, path, apiErr.Message)
			}
			return fmt.Errorf("digitalocean %s %s: status %d (%s): %s", method, path, resp.StatusCode, apiErr.ID, apiErr.Message)
		}
		return fmt.Errorf("digitalocean %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if respBody != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("decoding digitalocean response: %w", err)
		}
	}
	return nil
}
