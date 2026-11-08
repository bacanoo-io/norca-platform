// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package hcloud implements a load balancer provider backed by the Hetzner
// Cloud Load Balancers REST API v1.
//
// Each LoadBalancer object maps to one Hetzner Cloud Load Balancer named
// "<name_prefix><namespace>-<name>" and labelled with its Norca reference and
// class. Both public and private (VPC network) load balancers are supported,
// and the load balancer type ("lb11", "lb21", "lb31") and balancing algorithm
// ("round_robin", "least_connections") can be changed dynamically via the
// LoadBalancerClass object.
//
// Type ID: "hcloud".
package hcloud

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
const TypeID = "hcloud"

const defaultEndpoint = "https://api.hetzner.cloud/v1"

var classParams = []params.Param{
	{Name: "location", Type: params.ParamString, Default: "fsn1",
		Description: "Hetzner Cloud location name (e.g. fsn1, nbg1, hel1, ash, hil)"},
	{Name: "network_zone", Type: params.ParamString, Default: "",
		Description: "Hetzner Cloud network zone (e.g. eu-central); takes precedence over location when non-empty"},
	{Name: "type", Type: params.ParamString, Default: "lb11", Dynamic: true,
		Description: "Hetzner Cloud load balancer type (e.g. lb11, lb21, lb31)"},
	{Name: "algorithm", Type: params.ParamString, Default: "round_robin", Dynamic: true,
		Enum:        []string{"round_robin", "least_connections"},
		Description: "load balancing algorithm"},
	{Name: "network_id", Type: params.ParamInt, Default: 0,
		Description: "Hetzner Cloud network ID to attach load balancers to (required for internal load balancers)"},
	{Name: "use_private_ip", Type: params.ParamBool, Default: false,
		Description: "route traffic to targets over the attached Hetzner Cloud private network"},
	{Name: "name_prefix", Type: params.ParamString, Default: "norca-",
		Description: "prefix added to Hetzner Cloud load balancer names"},
}

// Provider manages Hetzner Cloud load balancers for one LoadBalancerClass.
type Provider struct {
	class        string
	token        string
	location     string
	networkZone  string
	networkID    int
	usePrivateIP bool
	namePrefix   string
	endpoint     string
	caps         lb.Capabilities
	log          *slog.Logger
	client       *http.Client
}

// NewLB creates a Hetzner Cloud provider instance.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	token := strings.TrimSpace(cfg.Values.String("token"))
	if token == "" {
		return nil, fmt.Errorf("token must not be empty")
	}
	location := strings.TrimSpace(cfg.Values.String("location"))
	networkZone := strings.TrimSpace(cfg.Values.String("network_zone"))
	if location == "" && networkZone == "" {
		return nil, fmt.Errorf("either location or network_zone must be set")
	}
	networkID := cfg.Values.Int("network_id")
	if networkID < 0 {
		return nil, fmt.Errorf("network_id must be >= 0")
	}
	usePrivateIP := cfg.Values.Bool("use_private_ip")
	if usePrivateIP && networkID == 0 {
		return nil, fmt.Errorf("use_private_ip requires network_id > 0")
	}
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Values.String("endpoint")), "/")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint URL %q", endpoint)
	}

	return &Provider{
		class:        cfg.Class,
		token:        token,
		location:     location,
		networkZone:  networkZone,
		networkID:    networkID,
		usePrivateIP: usePrivateIP,
		namePrefix:   cfg.Values.String("name_prefix"),
		endpoint:     endpoint,
		caps: lb.Capabilities{
			Internal: networkID > 0,
		},
		log:    cfg.Log().With("provider", TypeID, "class", cfg.Class),
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
	if req.Internal && p.networkID == 0 {
		return lb.State{}, lb.Invalid("internal load balancer requires network_id > 0")
	}
	dstPort, targetIPs, err := extractTargets(req.Port, req.Targets)
	if err != nil {
		return lb.State{}, err
	}

	lbType := strings.TrimSpace(req.Params.String("type"))
	if lbType == "" {
		lbType = "lb11"
	}
	alg := strings.TrimSpace(req.Params.String("algorithm"))
	if alg == "" {
		alg = "round_robin"
	}

	lbName := p.resourceName(req.Ref)
	curLB, err := p.findByName(ctx, lbName)
	if err != nil {
		return lb.State{}, err
	}

	if curLB == nil {
		curLB, err = p.createLoadBalancer(ctx, req, lbName, lbType, alg, dstPort, targetIPs)
		if err != nil {
			return lb.State{}, err
		}
	} else {
		if err := p.reconcileLoadBalancer(ctx, curLB, req, lbType, alg, dstPort, targetIPs); err != nil {
			return lb.State{}, err
		}
		// Refresh state after mutations.
		refreshed, err := p.getByID(ctx, curLB.ID)
		if err != nil {
			return lb.State{}, err
		}
		if refreshed != nil {
			curLB = refreshed
		}
	}

	addrs := p.extractAddresses(curLB, req.Port, req.Internal)
	state := lb.State{
		Addresses: addrs,
		Resource:  fmt.Sprintf("hcloud/lb/%d", curLB.ID),
	}
	switch {
	case len(req.Targets) == 0:
		state.Message = "no targets"
	case len(addrs) == 0:
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
	path := fmt.Sprintf("/load_balancers/%d", curLB.ID)
	return p.doJSON(ctx, http.MethodDelete, path, nil, nil, true)
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
			return 0, nil, lb.Invalid("hcloud requires all targets of a load balancer to use the same port (got %d and %d)", dstPort, t.Port)
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

func (p *Provider) extractAddresses(curLB *hcLoadBalancer, port int, internal bool) []string {
	portStr := strconv.Itoa(port)
	var addrs []string
	if internal {
		for _, pn := range curLB.PrivateNet {
			if (p.networkID == 0 || pn.Network == p.networkID) && pn.IP != "" {
				addrs = append(addrs, net.JoinHostPort(pn.IP, portStr))
			}
		}
		if len(addrs) == 0 && len(curLB.PrivateNet) > 0 && curLB.PrivateNet[0].IP != "" {
			addrs = append(addrs, net.JoinHostPort(curLB.PrivateNet[0].IP, portStr))
		}
		return addrs
	}
	if curLB.PublicNet.IPv4.IP != "" {
		addrs = append(addrs, net.JoinHostPort(curLB.PublicNet.IPv4.IP, portStr))
	}
	return addrs
}

// Hetzner Cloud API wire types (matching hcloud-go/hcloud/schema/load_balancer.go).

type hcLoadBalancer struct {
	ID               int64             `json:"id"`
	Name             string            `json:"name"`
	PublicNet        hcPublicNet       `json:"public_net"`
	PrivateNet       []hcPrivateNet    `json:"private_net"`
	LoadBalancerType hcLBType          `json:"load_balancer_type"`
	Algorithm        hcAlgorithm       `json:"algorithm"`
	Services         []hcService       `json:"services"`
	Targets          []hcTarget        `json:"targets"`
	Labels           map[string]string `json:"labels"`
}

type hcPublicNet struct {
	Enabled bool       `json:"enabled"`
	IPv4    hcPublicIP `json:"ipv4"`
	IPv6    hcPublicIP `json:"ipv6"`
}

type hcPublicIP struct {
	IP string `json:"ip"`
}

type hcPrivateNet struct {
	Network int    `json:"network"`
	IP      string `json:"ip"`
}

type hcLBType struct {
	Name string `json:"name"`
}

type hcAlgorithm struct {
	Type string `json:"type"`
}

type hcService struct {
	Protocol        string        `json:"protocol"`
	ListenPort      int           `json:"listen_port"`
	DestinationPort int           `json:"destination_port"`
	Proxyprotocol   bool          `json:"proxyprotocol"`
	HealthCheck     hcHealthCheck `json:"health_check"`
}

type hcHealthCheck struct {
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
	Interval int    `json:"interval"`
	Timeout  int    `json:"timeout"`
	Retries  int    `json:"retries"`
}

type hcTarget struct {
	Type         string      `json:"type"`
	IP           *hcTargetIP `json:"ip,omitempty"`
	UsePrivateIP bool        `json:"use_private_ip,omitempty"`
}

type hcTargetIP struct {
	IP string `json:"ip"`
}

type hcListResponse struct {
	LoadBalancers []hcLoadBalancer `json:"load_balancers"`
}

type hcSingleResponse struct {
	LoadBalancer hcLoadBalancer `json:"load_balancer"`
}

type hcErrorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func desiredService(listenPort, dstPort int) hcService {
	return hcService{
		Protocol:        "tcp",
		ListenPort:      listenPort,
		DestinationPort: dstPort,
		Proxyprotocol:   false,
		HealthCheck: hcHealthCheck{
			Protocol: "tcp",
			Port:     dstPort,
			Interval: 15,
			Timeout:  10,
			Retries:  3,
		},
	}
}

func (p *Provider) findByName(ctx context.Context, name string) (*hcLoadBalancer, error) {
	path := "/load_balancers?name=" + url.QueryEscape(name)
	var resp hcListResponse
	if err := p.doJSON(ctx, http.MethodGet, path, nil, &resp, false); err != nil {
		return nil, err
	}
	for i := range resp.LoadBalancers {
		if resp.LoadBalancers[i].Name == name {
			return &resp.LoadBalancers[i], nil
		}
	}
	return nil, nil
}

func (p *Provider) getByID(ctx context.Context, id int64) (*hcLoadBalancer, error) {
	path := fmt.Sprintf("/load_balancers/%d", id)
	var resp hcSingleResponse
	if err := p.doJSON(ctx, http.MethodGet, path, nil, &resp, false); err != nil {
		return nil, err
	}
	return &resp.LoadBalancer, nil
}

func (p *Provider) createLoadBalancer(
	ctx context.Context,
	req lb.Request,
	lbName, lbType, alg string,
	dstPort int,
	targetIPs []string,
) (*hcLoadBalancer, error) {
	targets := make([]hcTarget, len(targetIPs))
	for i, ip := range targetIPs {
		targets[i] = hcTarget{
			Type:         "ip",
			IP:           &hcTargetIP{IP: ip},
			UsePrivateIP: p.usePrivateIP,
		}
	}
	publicIface := !req.Internal
	body := map[string]any{
		"name":               lbName,
		"load_balancer_type": lbType,
		"algorithm":          hcAlgorithm{Type: alg},
		"public_interface":   publicIface,
		"services":           []hcService{desiredService(req.Port, dstPort)},
		"targets":            targets,
		"labels": map[string]string{
			"norca.io/namespace": req.Namespace,
			"norca.io/name":      req.Name,
			"norca.io/class":     p.class,
		},
	}
	if p.networkZone != "" {
		body["network_zone"] = p.networkZone
	} else {
		body["location"] = p.location
	}
	if p.networkID > 0 {
		body["network"] = p.networkID
	}

	var resp hcSingleResponse
	if err := p.doJSON(ctx, http.MethodPost, "/load_balancers", body, &resp, false); err != nil {
		return nil, err
	}
	p.log.Info("created Hetzner Cloud load balancer", "name", lbName, "id", resp.LoadBalancer.ID)
	return &resp.LoadBalancer, nil
}

func (p *Provider) reconcileLoadBalancer(
	ctx context.Context,
	curLB *hcLoadBalancer,
	req lb.Request,
	lbType, alg string,
	dstPort int,
	targetIPs []string,
) error {
	base := fmt.Sprintf("/load_balancers/%d/actions", curLB.ID)

	// 1. Reconcile load balancer type.
	if curLB.LoadBalancerType.Name != "" && !strings.EqualFold(curLB.LoadBalancerType.Name, lbType) {
		if err := p.doJSON(ctx, http.MethodPost, base+"/change_type", map[string]any{
			"load_balancer_type": lbType,
		}, nil, false); err != nil {
			return fmt.Errorf("changing load balancer type: %w", err)
		}
	}

	// 2. Reconcile balancing algorithm.
	if curLB.Algorithm.Type != "" && curLB.Algorithm.Type != alg {
		if err := p.doJSON(ctx, http.MethodPost, base+"/change_algorithm", map[string]any{
			"type": alg,
		}, nil, false); err != nil {
			return fmt.Errorf("changing algorithm: %w", err)
		}
	}

	// 3. Reconcile network attachment if configured.
	if p.networkID > 0 && !hasNetwork(curLB.PrivateNet, p.networkID) {
		if err := p.doJSON(ctx, http.MethodPost, base+"/attach_to_network", map[string]any{
			"network": p.networkID,
		}, nil, false); err != nil {
			return fmt.Errorf("attaching to network %d: %w", p.networkID, err)
		}
	}

	// 4. Reconcile public interface state.
	wantPublic := !req.Internal
	if curLB.PublicNet.Enabled != wantPublic {
		action := "/enable_public_interface"
		if !wantPublic {
			action = "/disable_public_interface"
		}
		if err := p.doJSON(ctx, http.MethodPost, base+action, map[string]any{}, nil, false); err != nil {
			return fmt.Errorf("updating public interface: %w", err)
		}
	}

	// 5. Reconcile services (remove stale listen ports, add or update desired port).
	foundPort := false
	for _, svc := range curLB.Services {
		if svc.ListenPort != req.Port {
			if err := p.doJSON(ctx, http.MethodPost, base+"/delete_service", map[string]any{
				"listen_port": svc.ListenPort,
			}, nil, false); err != nil {
				return fmt.Errorf("deleting stale service on port %d: %w", svc.ListenPort, err)
			}
			continue
		}
		foundPort = true
		if svc.DestinationPort != dstPort || svc.Protocol != "tcp" || svc.HealthCheck.Port != dstPort {
			if err := p.doJSON(ctx, http.MethodPost, base+"/update_service", desiredService(req.Port, dstPort), nil, false); err != nil {
				return fmt.Errorf("updating service on port %d: %w", req.Port, err)
			}
		}
	}
	if !foundPort {
		if err := p.doJSON(ctx, http.MethodPost, base+"/add_service", desiredService(req.Port, dstPort), nil, false); err != nil {
			return fmt.Errorf("adding service on port %d: %w", req.Port, err)
		}
	}

	// 6. Reconcile IP targets.
	currentIPs := make(map[string]bool, len(curLB.Targets))
	for _, t := range curLB.Targets {
		if t.Type == "ip" && t.IP != nil && t.IP.IP != "" {
			currentIPs[t.IP.IP] = true
		}
	}
	desiredSet := make(map[string]bool, len(targetIPs))
	for _, ip := range targetIPs {
		desiredSet[ip] = true
		if !currentIPs[ip] {
			if err := p.doJSON(ctx, http.MethodPost, base+"/add_target", hcTarget{
				Type:         "ip",
				IP:           &hcTargetIP{IP: ip},
				UsePrivateIP: p.usePrivateIP,
			}, nil, false); err != nil {
				return fmt.Errorf("adding target %s: %w", ip, err)
			}
		}
	}
	for ip := range currentIPs {
		if !desiredSet[ip] {
			if err := p.doJSON(ctx, http.MethodPost, base+"/remove_target", hcTarget{
				Type: "ip",
				IP:   &hcTargetIP{IP: ip},
			}, nil, false); err != nil {
				return fmt.Errorf("removing target %s: %w", ip, err)
			}
		}
	}

	return nil
}

func hasNetwork(nets []hcPrivateNet, networkID int) bool {
	for _, n := range nets {
		if n.Network == networkID {
			return true
		}
	}
	return false
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
		return fmt.Errorf("hcloud %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("reading hcloud response: %w", err)
	}

	if ignoreNotFound && resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr hcErrorResponse
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Error.Message != "" {
			if apiErr.Error.Code == "invalid_input" || apiErr.Error.Code == "uniqueness_error" {
				return lb.Invalid("hcloud %s %s (%s): %s", method, path, apiErr.Error.Code, apiErr.Error.Message)
			}
			return fmt.Errorf("hcloud %s %s: status %d (%s): %s", method, path, resp.StatusCode, apiErr.Error.Code, apiErr.Error.Message)
		}
		return fmt.Errorf("hcloud %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if respBody != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("decoding hcloud response: %w", err)
		}
	}
	return nil
}
