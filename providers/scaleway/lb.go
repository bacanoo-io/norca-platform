// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package scaleway implements a load balancer provider backed by the Scaleway
// Load Balancers Zoned REST API v1 (/lb/v1/zones/{zone}/...).
//
// Each LoadBalancer object maps to one Scaleway Load Balancer, one TCP Backend
// with a TCP health check, and one Frontend in the configured availability
// zone. Both flexible public IPs and Private Network attachments are
// supported, and the offer type (e.g. "LB-S", "LB-GP-M") and load balancing
// algorithm ("roundrobin", "leastconn", "first") can be updated dynamically via
// the LoadBalancerClass object.
//
// Type ID: "scaleway".
package scaleway

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
const TypeID = "scaleway"

const defaultEndpoint = "https://api.scaleway.com"

var classParams = []params.Param{
	{Name: "zone", Type: params.ParamString, Default: "fr-par-1",
		Description: "Scaleway availability zone (e.g. fr-par-1, fr-par-2, nl-ams-1, pl-waw-1)"},
	{Name: "type", Type: params.ParamString, Default: "LB-S", Dynamic: true,
		Description: "Scaleway load balancer commercial offer type (e.g. LB-S, LB-GP-M, LB-GP-L)"},
	{Name: "algorithm", Type: params.ParamString, Default: "roundrobin", Dynamic: true,
		Enum:        []string{"roundrobin", "leastconn", "first"},
		Description: "backend forwarding algorithm"},
	{Name: "private_network_id", Type: params.ParamString, Default: "",
		Description: "Scaleway Private Network ID to attach load balancers to (required for internal load balancers)"},
	{Name: "name_prefix", Type: params.ParamString, Default: "norca-",
		Description: "prefix added to Scaleway load balancer, backend, and frontend names"},
}

// Provider manages Scaleway load balancers for one LoadBalancerClass.
type Provider struct {
	class            string
	secretKey        string
	projectID        string
	zone             string
	privateNetworkID string
	namePrefix       string
	endpoint         string
	caps             lb.Capabilities
	log              *slog.Logger
	client           *http.Client
}

// NewLB creates a Scaleway provider instance.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	secretKey := strings.TrimSpace(cfg.Values.String("secret_key"))
	if secretKey == "" {
		return nil, fmt.Errorf("secret_key must not be empty")
	}
	projectID := strings.TrimSpace(cfg.Values.String("project_id"))
	if projectID == "" {
		return nil, fmt.Errorf("project_id must not be empty")
	}
	zone := strings.TrimSpace(cfg.Values.String("zone"))
	if zone == "" {
		return nil, fmt.Errorf("zone must not be empty")
	}
	privNetID := strings.TrimSpace(cfg.Values.String("private_network_id"))
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Values.String("endpoint")), "/")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint URL %q", endpoint)
	}

	return &Provider{
		class:            cfg.Class,
		secretKey:        secretKey,
		projectID:        projectID,
		zone:             zone,
		privateNetworkID: privNetID,
		namePrefix:       cfg.Values.String("name_prefix"),
		endpoint:         endpoint,
		caps: lb.Capabilities{
			Internal: privNetID != "",
		},
		log:    cfg.Log().With("provider", TypeID, "class", cfg.Class, "zone", zone),
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
	if req.Internal && p.privateNetworkID == "" {
		return lb.State{}, lb.Invalid("internal load balancer requires private_network_id")
	}
	dstPort, targetIPs, err := extractTargets(req.Port, req.Targets)
	if err != nil {
		return lb.State{}, err
	}

	lbType := strings.TrimSpace(req.Params.String("type"))
	if lbType == "" {
		lbType = "LB-S"
	}
	alg := strings.TrimSpace(req.Params.String("algorithm"))
	if alg == "" {
		alg = "roundrobin"
	}

	name := p.resourceName(req.Ref)
	curLB, err := p.findLB(ctx, name)
	if err != nil {
		return lb.State{}, err
	}

	if curLB == nil {
		curLB, err = p.createLB(ctx, req, name, lbType)
		if err != nil {
			return lb.State{}, err
		}
	} else if curLB.Type != "" && !strings.EqualFold(curLB.Type, lbType) {
		curLB, err = p.migrateLB(ctx, curLB.ID, lbType)
		if err != nil {
			return lb.State{}, err
		}
	}

	if p.privateNetworkID != "" {
		if err := p.ensurePrivateNetwork(ctx, curLB.ID); err != nil {
			return lb.State{}, err
		}
	}

	backend, err := p.ensureBackend(ctx, curLB.ID, name, dstPort, alg, targetIPs)
	if err != nil {
		return lb.State{}, err
	}

	if err := p.ensureFrontend(ctx, curLB.ID, name, req.Port, backend.ID); err != nil {
		return lb.State{}, err
	}

	// Refresh LB state to capture any newly allocated IPs or Private Network addresses.
	refreshed, err := p.getLB(ctx, curLB.ID)
	if err != nil {
		return lb.State{}, err
	}
	if refreshed != nil {
		curLB = refreshed
	}

	addrs := p.extractAddresses(ctx, curLB, req.Port, req.Internal)
	state := lb.State{
		Addresses: addrs,
		Resource:  fmt.Sprintf("scaleway/%s/lb/%s", p.zone, curLB.ID),
	}
	switch {
	case curLB.Status != "" && curLB.Status != "ready":
		state.Message = fmt.Sprintf("load balancer status is %s", curLB.Status)
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
	name := p.resourceName(ref)
	curLB, err := p.findLB(ctx, name)
	if err != nil {
		return err
	}
	if curLB == nil {
		return nil
	}
	path := fmt.Sprintf("/lb/v1/zones/%s/lbs/%s?release_ip=true", url.PathEscape(p.zone), url.PathEscape(curLB.ID))
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
			return 0, nil, lb.Invalid("scaleway requires all targets of a load balancer to use the same port (got %d and %d)", dstPort, t.Port)
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

func (p *Provider) zoneBase() string {
	return "/lb/v1/zones/" + url.PathEscape(p.zone)
}

// Scaleway LB API v1 wire types (matching scaleway-sdk-go/api/lb/v1/lb_sdk.go).

type scwLB struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Status string   `json:"status"`
	Type   string   `json:"type"`
	IP     []scwIP  `json:"ip"`
	Tags   []string `json:"tags"`
}

type scwIP struct {
	ID        string `json:"id"`
	IPAddress string `json:"ip_address"`
}

type scwListLBsResponse struct {
	LBs []scwLB `json:"lbs"`
}

type scwBackend struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	ForwardProtocol      string          `json:"forward_protocol"`
	ForwardPort          int             `json:"forward_port"`
	ForwardPortAlgorithm string          `json:"forward_port_algorithm"`
	Pool                 []string        `json:"pool"`
	HealthCheck          *scwHealthCheck `json:"health_check,omitempty"`
}

type scwHealthCheck struct {
	Port            int            `json:"port"`
	CheckDelay      string         `json:"check_delay"`
	CheckTimeout    string         `json:"check_timeout"`
	CheckMaxRetries int            `json:"check_max_retries"`
	TCPConfig       map[string]any `json:"tcp_config,omitempty"`
}

type scwListBackendsResponse struct {
	Backends []scwBackend `json:"backends"`
}

type scwFrontend struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	InboundPort int         `json:"inbound_port"`
	Backend     *scwBackend `json:"backend,omitempty"`
}

type scwListFrontendsResponse struct {
	Frontends []scwFrontend `json:"frontends"`
}

type scwPrivateNetwork struct {
	PrivateNetworkID string   `json:"private_network_id"`
	Status           string   `json:"status"`
	IPAMIDs          []string `json:"ipam_ids,omitempty"`
	DHCPConfig       *struct {
		IPAddress string `json:"ip_address,omitempty"`
	} `json:"dhcp_config,omitempty"`
	StaticConfig *struct {
		IPAddress []string `json:"ip_address,omitempty"`
	} `json:"static_config,omitempty"`
}

type scwListPrivateNetworksResponse struct {
	PrivateNetwork []scwPrivateNetwork `json:"private_network"`
}

type scwErrorResponse struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (p *Provider) findLB(ctx context.Context, name string) (*scwLB, error) {
	q := url.Values{}
	q.Set("name", name)
	q.Set("project_id", p.projectID)
	var resp scwListLBsResponse
	if err := p.doJSON(ctx, http.MethodGet, p.zoneBase()+"/lbs?"+q.Encode(), nil, &resp, false); err != nil {
		return nil, err
	}
	for i := range resp.LBs {
		if resp.LBs[i].Name == name {
			return &resp.LBs[i], nil
		}
	}
	return nil, nil
}

func (p *Provider) getLB(ctx context.Context, lbID string) (*scwLB, error) {
	var curLB scwLB
	if err := p.doJSON(ctx, http.MethodGet, p.zoneBase()+"/lbs/"+url.PathEscape(lbID), nil, &curLB, false); err != nil {
		return nil, err
	}
	return &curLB, nil
}

func (p *Provider) createLB(ctx context.Context, req lb.Request, name, lbType string) (*scwLB, error) {
	assignFlexIP := !req.Internal
	body := map[string]any{
		"project_id":         p.projectID,
		"name":               name,
		"type":               lbType,
		"assign_flexible_ip": assignFlexIP,
		"tags": []string{
			"norca-namespace=" + req.Namespace,
			"norca-name=" + req.Name,
			"norca-class=" + p.class,
		},
	}
	var curLB scwLB
	if err := p.doJSON(ctx, http.MethodPost, p.zoneBase()+"/lbs", body, &curLB, false); err != nil {
		return nil, err
	}
	p.log.Info("created Scaleway load balancer", "name", name, "id", curLB.ID)
	return &curLB, nil
}

func (p *Provider) migrateLB(ctx context.Context, lbID, lbType string) (*scwLB, error) {
	var curLB scwLB
	path := p.zoneBase() + "/lbs/" + url.PathEscape(lbID) + "/migrate"
	if err := p.doJSON(ctx, http.MethodPost, path, map[string]any{"type": lbType}, &curLB, false); err != nil {
		return nil, fmt.Errorf("migrating load balancer type: %w", err)
	}
	return &curLB, nil
}

func (p *Provider) ensurePrivateNetwork(ctx context.Context, lbID string) error {
	base := p.zoneBase() + "/lbs/" + url.PathEscape(lbID)
	var list scwListPrivateNetworksResponse
	if err := p.doJSON(ctx, http.MethodGet, base+"/private-networks", nil, &list, false); err != nil {
		return err
	}
	for _, pn := range list.PrivateNetwork {
		if pn.PrivateNetworkID == p.privateNetworkID {
			return nil
		}
	}
	body := map[string]any{
		"private_network_id": p.privateNetworkID,
		"ipam_config":        map[string]any{},
	}
	return p.doJSON(ctx, http.MethodPost, base+"/attach-private-network", body, nil, false)
}

func (p *Provider) ensureBackend(ctx context.Context, lbID, name string, dstPort int, alg string, targetIPs []string) (*scwBackend, error) {
	var list scwListBackendsResponse
	if err := p.doJSON(ctx, http.MethodGet, p.zoneBase()+"/lbs/"+url.PathEscape(lbID)+"/backends", nil, &list, false); err != nil {
		return nil, err
	}
	var existing *scwBackend
	for i := range list.Backends {
		if list.Backends[i].Name == name {
			existing = &list.Backends[i]
			break
		}
	}
	if existing == nil && len(list.Backends) > 0 {
		existing = &list.Backends[0]
	}

	hc := scwHealthCheck{
		Port:            dstPort,
		CheckDelay:      "10s",
		CheckTimeout:    "5s",
		CheckMaxRetries: 3,
		TCPConfig:       map[string]any{},
	}

	if existing == nil {
		body := map[string]any{
			"name":                   name,
			"forward_protocol":       "tcp",
			"forward_port":           dstPort,
			"forward_port_algorithm": alg,
			"server_ip":              targetIPs,
			"health_check":           hc,
		}
		var created scwBackend
		if err := p.doJSON(ctx, http.MethodPost, p.zoneBase()+"/lbs/"+url.PathEscape(lbID)+"/backends", body, &created, false); err != nil {
			return nil, err
		}
		return &created, nil
	}

	backendPath := p.zoneBase() + "/backends/" + url.PathEscape(existing.ID)
	if existing.ForwardPort != dstPort || existing.ForwardPortAlgorithm != alg || existing.ForwardProtocol != "tcp" {
		body := map[string]any{
			"name":                   existing.Name,
			"forward_protocol":       "tcp",
			"forward_port":           dstPort,
			"forward_port_algorithm": alg,
		}
		if err := p.doJSON(ctx, http.MethodPut, backendPath, body, existing, false); err != nil {
			return nil, err
		}
		if err := p.doJSON(ctx, http.MethodPut, backendPath+"/healthcheck", hc, nil, false); err != nil {
			return nil, err
		}
	}

	curPool := slices.Clone(existing.Pool)
	slices.Sort(curPool)
	if !slices.Equal(curPool, targetIPs) {
		body := map[string]any{"server_ip": targetIPs}
		if err := p.doJSON(ctx, http.MethodPut, backendPath+"/servers", body, existing, false); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

func (p *Provider) ensureFrontend(ctx context.Context, lbID, name string, inboundPort int, backendID string) error {
	var list scwListFrontendsResponse
	if err := p.doJSON(ctx, http.MethodGet, p.zoneBase()+"/lbs/"+url.PathEscape(lbID)+"/frontends", nil, &list, false); err != nil {
		return err
	}
	var existing *scwFrontend
	for i := range list.Frontends {
		if list.Frontends[i].Name == name {
			existing = &list.Frontends[i]
			break
		}
	}
	if existing == nil && len(list.Frontends) > 0 {
		existing = &list.Frontends[0]
	}

	body := map[string]any{
		"name":         name,
		"inbound_port": inboundPort,
		"backend_id":   backendID,
	}
	if existing == nil {
		return p.doJSON(ctx, http.MethodPost, p.zoneBase()+"/lbs/"+url.PathEscape(lbID)+"/frontends", body, nil, false)
	}
	curBackendID := ""
	if existing.Backend != nil {
		curBackendID = existing.Backend.ID
	}
	if existing.InboundPort != inboundPort || (curBackendID != "" && curBackendID != backendID) {
		return p.doJSON(ctx, http.MethodPut, p.zoneBase()+"/frontends/"+url.PathEscape(existing.ID), body, nil, false)
	}
	return nil
}

func (p *Provider) extractAddresses(ctx context.Context, curLB *scwLB, port int, internal bool) []string {
	portStr := strconv.Itoa(port)
	var addrs []string
	for _, ip := range curLB.IP {
		if ip.IPAddress != "" {
			addrs = append(addrs, net.JoinHostPort(ip.IPAddress, portStr))
		}
	}
	if len(addrs) == 0 && internal && p.privateNetworkID != "" {
		base := p.zoneBase() + "/lbs/" + url.PathEscape(curLB.ID)
		var list scwListPrivateNetworksResponse
		if p.doJSON(ctx, http.MethodGet, base+"/private-networks", nil, &list, false) == nil {
			for _, pn := range list.PrivateNetwork {
				if pn.DHCPConfig != nil && pn.DHCPConfig.IPAddress != "" {
					addrs = append(addrs, net.JoinHostPort(pn.DHCPConfig.IPAddress, portStr))
				}
				if pn.StaticConfig != nil {
					for _, sip := range pn.StaticConfig.IPAddress {
						if sip != "" {
							addrs = append(addrs, net.JoinHostPort(sip, portStr))
						}
					}
				}
			}
		}
	}
	return addrs
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
	req.Header.Set("X-Auth-Token", p.secretKey)
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("scaleway %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("reading scaleway response: %w", err)
	}

	if ignoreNotFound && resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr scwErrorResponse
		if json.Unmarshal(raw, &apiErr) == nil && apiErr.Message != "" {
			if apiErr.Type == "invalid_arguments" || resp.StatusCode == http.StatusBadRequest {
				return lb.Invalid("scaleway %s %s: %s", method, path, apiErr.Message)
			}
			return fmt.Errorf("scaleway %s %s: status %d (%s): %s", method, path, resp.StatusCode, apiErr.Type, apiErr.Message)
		}
		return fmt.Errorf("scaleway %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if respBody != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("decoding scaleway response: %w", err)
		}
	}
	return nil
}
