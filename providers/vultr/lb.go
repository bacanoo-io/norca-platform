// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package vultr implements a load balancer provider backed by the Vultr REST
// API v2 (/v2/load-balancers and /v2/instances).
//
// Each LoadBalancer object maps to one Vultr Load Balancer labelled
// "<name_prefix><namespace>-<name>" in the configured region (and optional
// VPC). Target IP addresses are resolved to Vultr Instance IDs via
// GET /v2/instances, and the load balancer's forwarding rules, TCP health
// check, instances list, balancing algorithm, and PROXY protocol flag are
// reconciled idempotently via PATCH /v2/load-balancers/{id}.
//
// Type ID: "vultr".
package vultr

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
const TypeID = "vultr"

const defaultEndpoint = "https://api.vultr.com/v2"

var classParams = []params.Param{
	{Name: "region", Type: params.ParamString, Default: "fra",
		Description: "Vultr region ID (e.g. fra, ams, lhr, ewr, ord, lax)"},
	{Name: "algorithm", Type: params.ParamString, Default: "roundrobin", Dynamic: true,
		Enum:        []string{"roundrobin", "leastconn"},
		Description: "load balancing algorithm"},
	{Name: "vpc", Type: params.ParamString, Default: "",
		Description: "Vultr VPC ID to attach load balancers to (enables internal load balancers)"},
	{Name: "proxy_protocol", Type: params.ParamBool, Default: false, Dynamic: true,
		Description: "enable PROXY protocol on the Vultr load balancer"},
	{Name: "name_prefix", Type: params.ParamString, Default: "norca-",
		Description: "prefix added to Vultr load balancer labels"},
}

// Provider manages Vultr load balancers for one LoadBalancerClass.
type Provider struct {
	class      string
	apiKey     string
	region     string
	vpc        string
	namePrefix string
	endpoint   string
	caps       lb.Capabilities
	log        *slog.Logger
	client     *http.Client
}

// NewLB creates a Vultr provider instance.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	apiKey := strings.TrimSpace(cfg.Values.String("api_key"))
	if apiKey == "" {
		return nil, fmt.Errorf("api_key must not be empty")
	}
	region := strings.TrimSpace(cfg.Values.String("region"))
	if region == "" {
		return nil, fmt.Errorf("region must not be empty")
	}
	vpc := strings.TrimSpace(cfg.Values.String("vpc"))
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
		apiKey:     apiKey,
		region:     region,
		vpc:        vpc,
		namePrefix: cfg.Values.String("name_prefix"),
		endpoint:   endpoint,
		caps: lb.Capabilities{
			Internal: vpc != "",
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
	if req.Internal && p.vpc == "" {
		return lb.State{}, lb.Invalid("internal load balancer requires vpc")
	}
	dstPort, targetIPs, err := extractTargets(req.Port, req.Targets)
	if err != nil {
		return lb.State{}, err
	}

	alg := strings.TrimSpace(req.Params.String("algorithm"))
	if alg == "" {
		alg = "roundrobin"
	}
	proxyProto := req.Params.Bool("proxy_protocol")

	var instanceIDs []string
	if len(targetIPs) > 0 {
		instanceIDs, err = p.resolveInstanceIDs(ctx, targetIPs)
		if err != nil {
			return lb.State{}, err
		}
	}

	label := p.resourceName(req.Ref)
	curLB, err := p.findByLabel(ctx, label)
	if err != nil {
		return lb.State{}, err
	}

	spec := p.buildSpec(label, alg, proxyProto, req.Port, dstPort, instanceIDs)
	if curLB == nil {
		var resp vultrSingleLBResponse
		if err := p.doJSON(ctx, http.MethodPost, "/load-balancers", spec, &resp, false); err != nil {
			return lb.State{}, err
		}
		curLB = &resp.LoadBalancer
		p.log.Info("created Vultr load balancer", "label", label, "id", curLB.ID)
	} else if needsUpdate(curLB, spec) {
		path := "/load-balancers/" + url.PathEscape(curLB.ID)
		if err := p.doJSON(ctx, http.MethodPatch, path, spec, nil, false); err != nil {
			return lb.State{}, err
		}
		refreshed, err := p.getByID(ctx, curLB.ID)
		if err != nil {
			return lb.State{}, err
		}
		if refreshed != nil {
			curLB = refreshed
		}
	}

	state := lb.State{
		Resource: "vultr/lb/" + curLB.ID,
	}
	if curLB.IP != "" {
		state.Addresses = []string{net.JoinHostPort(curLB.IP, strconv.Itoa(req.Port))}
	}
	switch {
	case curLB.Status != "" && curLB.Status != "active":
		state.Message = fmt.Sprintf("load balancer status is %s", curLB.Status)
	case len(req.Targets) == 0:
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
	label := p.resourceName(ref)
	curLB, err := p.findByLabel(ctx, label)
	if err != nil {
		return err
	}
	if curLB == nil {
		return nil
	}
	return p.doJSON(ctx, http.MethodDelete, "/load-balancers/"+url.PathEscape(curLB.ID), nil, nil, true)
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
			return 0, nil, lb.Invalid("vultr requires all targets of a load balancer to use the same port (got %d and %d)", dstPort, t.Port)
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

// Vultr API v2 wire types (matching github.com/vultr/govultr/v3).

type vultrLoadBalancer struct {
	ID                 string                `json:"id,omitempty"`
	Label              string                `json:"label"`
	Region             string                `json:"region,omitempty"`
	Status             string                `json:"status,omitempty"`
	IP                 string                `json:"ip,omitempty"`
	IPv6               string                `json:"ipv6,omitempty"`
	BalancingAlgorithm string                `json:"balancing_algorithm,omitempty"`
	ProxyProtocol      bool                  `json:"proxy_protocol,omitempty"`
	VPC                string                `json:"vpc,omitempty"`
	HealthCheck        vultrHealthCheck      `json:"health_check"`
	ForwardingRules    []vultrForwardingRule `json:"forwarding_rules"`
	Instances          []string              `json:"instances"`
}

type vultrForwardingRule struct {
	RuleID           string `json:"id,omitempty"`
	FrontendProtocol string `json:"frontend_protocol"`
	FrontendPort     int    `json:"frontend_port"`
	BackendProtocol  string `json:"backend_protocol"`
	BackendPort      int    `json:"backend_port"`
}

type vultrHealthCheck struct {
	Protocol           string `json:"protocol"`
	Port               int    `json:"port"`
	CheckInterval      int    `json:"check_interval"`
	ResponseTimeout    int    `json:"response_timeout"`
	UnhealthyThreshold int    `json:"unhealthy_threshold"`
	HealthyThreshold   int    `json:"healthy_threshold"`
}

type vultrListLBsResponse struct {
	LoadBalancers []vultrLoadBalancer `json:"load_balancers"`
}

type vultrSingleLBResponse struct {
	LoadBalancer vultrLoadBalancer `json:"load_balancer"`
}

type vultrInstance struct {
	ID         string `json:"id"`
	MainIP     string `json:"main_ip"`
	InternalIP string `json:"internal_ip"`
	V6MainIP   string `json:"v6_main_ip"`
}

type vultrListInstancesResponse struct {
	Instances []vultrInstance `json:"instances"`
}

type vultrErrorResponse struct {
	Error  string `json:"error"`
	Status int    `json:"status"`
}

func (p *Provider) buildSpec(label, alg string, proxyProto bool, frontendPort, dstPort int, instanceIDs []string) vultrLoadBalancer {
	if instanceIDs == nil {
		instanceIDs = []string{}
	}
	return vultrLoadBalancer{
		Label:              label,
		Region:             p.region,
		BalancingAlgorithm: alg,
		ProxyProtocol:      proxyProto,
		VPC:                p.vpc,
		ForwardingRules: []vultrForwardingRule{{
			FrontendProtocol: "tcp",
			FrontendPort:     frontendPort,
			BackendProtocol:  "tcp",
			BackendPort:      dstPort,
		}},
		HealthCheck: vultrHealthCheck{
			Protocol:           "tcp",
			Port:               dstPort,
			CheckInterval:      15,
			ResponseTimeout:    5,
			UnhealthyThreshold: 3,
			HealthyThreshold:   2,
		},
		Instances: instanceIDs,
	}
}

func needsUpdate(cur *vultrLoadBalancer, want vultrLoadBalancer) bool {
	if cur.BalancingAlgorithm != "" && cur.BalancingAlgorithm != want.BalancingAlgorithm {
		return true
	}
	if cur.ProxyProtocol != want.ProxyProtocol {
		return true
	}
	if len(cur.ForwardingRules) != 1 {
		return true
	}
	r := cur.ForwardingRules[0]
	w := want.ForwardingRules[0]
	if r.FrontendPort != w.FrontendPort || r.BackendPort != w.BackendPort || r.FrontendProtocol != "tcp" || r.BackendProtocol != "tcp" {
		return true
	}
	if cur.HealthCheck.Port != want.HealthCheck.Port || cur.HealthCheck.Protocol != "tcp" {
		return true
	}
	curInst := slices.Clone(cur.Instances)
	slices.Sort(curInst)
	return !slices.Equal(curInst, want.Instances)
}

func (p *Provider) findByLabel(ctx context.Context, label string) (*vultrLoadBalancer, error) {
	var resp vultrListLBsResponse
	if err := p.doJSON(ctx, http.MethodGet, "/load-balancers?per_page=100", nil, &resp, false); err != nil {
		return nil, err
	}
	for i := range resp.LoadBalancers {
		if resp.LoadBalancers[i].Label == label {
			return &resp.LoadBalancers[i], nil
		}
	}
	return nil, nil
}

func (p *Provider) getByID(ctx context.Context, id string) (*vultrLoadBalancer, error) {
	var resp vultrSingleLBResponse
	if err := p.doJSON(ctx, http.MethodGet, "/load-balancers/"+url.PathEscape(id), nil, &resp, false); err != nil {
		return nil, err
	}
	return &resp.LoadBalancer, nil
}

func (p *Provider) resolveInstanceIDs(ctx context.Context, targetIPs []string) ([]string, error) {
	var resp vultrListInstancesResponse
	if err := p.doJSON(ctx, http.MethodGet, "/instances?per_page=100", nil, &resp, false); err != nil {
		return nil, err
	}
	ipToID := make(map[string]string, len(resp.Instances)*2)
	for _, inst := range resp.Instances {
		for _, ip := range []string{inst.MainIP, inst.InternalIP, inst.V6MainIP} {
			if ip != "" {
				ipToID[ip] = inst.ID
			}
		}
	}

	seen := make(map[string]bool, len(targetIPs))
	ids := make([]string, 0, len(targetIPs))
	for _, ip := range targetIPs {
		id, ok := ipToID[ip]
		if !ok {
			return nil, fmt.Errorf("vultr instance with IP %s not found", ip)
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
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("vultr %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("reading vultr response: %w", err)
	}

	if ignoreNotFound && resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr vultrErrorResponse
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error != "" {
			if resp.StatusCode == http.StatusBadRequest {
				return lb.Invalid("vultr %s %s: %s", method, path, apiErr.Error)
			}
			return fmt.Errorf("vultr %s %s: status %d: %s", method, path, resp.StatusCode, apiErr.Error)
		}
		return fmt.Errorf("vultr %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if respBody != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("decoding vultr response: %w", err)
		}
	}
	return nil
}
