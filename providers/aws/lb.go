// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package aws implements a load balancer provider backed by AWS Elastic Load
// Balancing v2 (Network Load Balancer, API version 2015-12-01).
//
// Each LoadBalancer object maps to an AWS Network Load Balancer (`Type=network`),
// a TCP IP-type Target Group (`TargetType=ip`), and a TCP Listener in the
// configured VPC and subnets. Requests are signed with AWS Signature Version 4
// (`elasticloadbalancing` service) using the standard library.
//
// Type ID: "aws".
package aws

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
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
const TypeID = "aws"

const (
	apiVersion  = "2015-12-01"
	serviceName = "elasticloadbalancing"
)

var classParams = []params.Param{
	{Name: "vpc_id", Type: params.ParamString, Required: true,
		Description: "AWS VPC ID for target groups (e.g. vpc-0123456789abcdef0)"},
	{Name: "subnets", Type: params.ParamList, Required: true,
		Description: "subnet IDs for internet-facing (and default internal) load balancers"},
	{Name: "internal_subnets", Type: params.ParamList,
		Description: "optional subnet IDs specifically for internal load balancers; defaults to subnets"},
	{Name: "security_groups", Type: params.ParamList,
		Description: "optional security group IDs attached to the Network Load Balancer"},
	{Name: "cross_zone", Type: params.ParamBool, Default: false, Dynamic: true,
		Description: "enable cross-zone load balancing (load_balancing.cross_zone.enabled)"},
	{Name: "name_prefix", Type: params.ParamString, Default: "norca-",
		Description: "prefix added to AWS load balancer and target group names"},
}

// Provider manages AWS Network Load Balancers for one LoadBalancerClass.
type Provider struct {
	class           string
	region          string
	vpcID           string
	subnets         []string
	internalSubnets []string
	securityGroups  []string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	namePrefix      string
	endpoint        string
	caps            lb.Capabilities
	log             *slog.Logger
	client          *http.Client
	now             func() time.Time
}

// NewLB creates an AWS ELBv2 provider instance.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	region := strings.TrimSpace(cfg.Values.String("region"))
	if region == "" {
		return nil, fmt.Errorf("region must not be empty")
	}
	vpcID := strings.TrimSpace(cfg.Values.String("vpc_id"))
	if vpcID == "" {
		return nil, fmt.Errorf("vpc_id must not be empty")
	}
	subnets := cleanList(cfg.Values.List("subnets"))
	if len(subnets) == 0 {
		return nil, fmt.Errorf("subnets must contain at least one subnet ID")
	}
	internalSubnets := cleanList(cfg.Values.List("internal_subnets"))
	if len(internalSubnets) == 0 {
		internalSubnets = subnets
	}
	accessKey := strings.TrimSpace(cfg.Values.String("access_key_id"))
	if accessKey == "" {
		return nil, fmt.Errorf("access_key_id must not be empty")
	}
	secretKey := strings.TrimSpace(cfg.Values.String("secret_access_key"))
	if secretKey == "" {
		return nil, fmt.Errorf("secret_access_key must not be empty")
	}

	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Values.String("endpoint")), "/")
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://elasticloadbalancing.%s.amazonaws.com", region)
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint URL %q", endpoint)
	}

	return &Provider{
		class:           cfg.Class,
		region:          region,
		vpcID:           vpcID,
		subnets:         subnets,
		internalSubnets: internalSubnets,
		securityGroups:  cleanList(cfg.Values.List("security_groups")),
		accessKeyID:     accessKey,
		secretAccessKey: secretKey,
		sessionToken:    strings.TrimSpace(cfg.Values.String("session_token")),
		namePrefix:      cfg.Values.String("name_prefix"),
		endpoint:        endpoint,
		caps: lb.Capabilities{
			Internal: true,
		},
		log:    cfg.Log().With("provider", TypeID, "class", cfg.Class, "region", region),
		client: &http.Client{Timeout: 30 * time.Second},
		now:    time.Now,
	}, nil
}

func cleanList(in []string) []string {
	var out []string
	for _, s := range in {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// Capabilities implements lb.Provider.
func (p *Provider) Capabilities() lb.Capabilities { return p.caps }

// Ensure implements lb.Provider.
func (p *Provider) Ensure(ctx context.Context, req lb.Request) (lb.State, error) {
	if req.Port <= 0 || req.Port > 65535 {
		return lb.State{}, lb.Invalid("invalid frontend port %d", req.Port)
	}
	dstPort, targets, err := normalizeTargets(req.Port, req.Targets)
	if err != nil {
		return lb.State{}, err
	}

	name := p.resourceName(req.Ref)
	scheme := "internet-facing"
	if req.Internal {
		scheme = "internal"
	}

	curLB, err := p.describeLoadBalancer(ctx, name)
	if err != nil {
		return lb.State{}, err
	}
	if curLB != nil && curLB.Scheme != "" && curLB.Scheme != scheme {
		// AWS NLB scheme is immutable; recreate if Internal changed.
		if err := p.deleteAll(ctx, name, curLB.ARN); err != nil {
			return lb.State{}, err
		}
		curLB = nil
	}
	if curLB == nil {
		curLB, err = p.createLoadBalancer(ctx, req, name, scheme)
		if err != nil {
			return lb.State{}, err
		}
	}

	if err := p.reconcileCrossZone(ctx, curLB.ARN, req.Params.Bool("cross_zone")); err != nil {
		return lb.State{}, err
	}

	tgARN, err := p.ensureTargetGroup(ctx, req, name, dstPort)
	if err != nil {
		return lb.State{}, err
	}

	if err := p.reconcileTargets(ctx, tgARN, targets); err != nil {
		return lb.State{}, err
	}

	if err := p.ensureListener(ctx, curLB.ARN, tgARN, req.Port); err != nil {
		return lb.State{}, err
	}

	state := lb.State{
		Resource: curLB.ARN,
	}
	if curLB.DNSName != "" {
		state.Addresses = []string{net.JoinHostPort(curLB.DNSName, strconv.Itoa(req.Port))}
	}
	switch {
	case curLB.State != "" && curLB.State != "active":
		state.Message = fmt.Sprintf("load balancer state is %s", curLB.State)
	case len(targets) == 0:
		state.Message = "no targets"
	case len(state.Addresses) == 0:
		state.Message = "waiting for DNS name assignment"
	default:
		state.Ready = true
	}
	return state, nil
}

// Delete implements lb.Provider.
func (p *Provider) Delete(ctx context.Context, ref lb.Ref) error {
	name := p.resourceName(ref)
	curLB, err := p.describeLoadBalancer(ctx, name)
	if err != nil {
		return err
	}
	lbARN := ""
	if curLB != nil {
		lbARN = curLB.ARN
	}
	return p.deleteAll(ctx, name, lbARN)
}

func (p *Provider) deleteAll(ctx context.Context, name, lbARN string) error {
	if lbARN != "" {
		listeners, err := p.describeListeners(ctx, lbARN)
		if err != nil {
			return err
		}
		for _, l := range listeners {
			q := url.Values{"ListenerArn": {l.ARN}}
			if err := p.call(ctx, "DeleteListener", q, nil); err != nil && !isAWSError(err, "ListenerNotFound") {
				return err
			}
		}
		q := url.Values{"LoadBalancerArn": {lbARN}}
		if err := p.call(ctx, "DeleteLoadBalancer", q, nil); err != nil && !isAWSError(err, "LoadBalancerNotFound") {
			return err
		}
	}
	tg, err := p.describeTargetGroup(ctx, name)
	if err != nil {
		return err
	}
	if tg != nil {
		q := url.Values{"TargetGroupArn": {tg.ARN}}
		if err := p.call(ctx, "DeleteTargetGroup", q, nil); err != nil && !isAWSError(err, "TargetGroupNotFound") {
			return err
		}
	}
	return nil
}

type targetEndpoint struct {
	IP   string
	Port int
}

func normalizeTargets(frontendPort int, raw []lb.Target) (int, []targetEndpoint, error) {
	dstPort := frontendPort
	seen := make(map[targetEndpoint]bool, len(raw))
	out := make([]targetEndpoint, 0, len(raw))
	for i, t := range raw {
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
		ep := targetEndpoint{IP: ip, Port: t.Port}
		if !seen[ep] {
			seen[ep] = true
			out = append(out, ep)
		}
	}
	slices.SortFunc(out, func(a, b targetEndpoint) int {
		if c := strings.Compare(a.IP, b.IP); c != 0 {
			return c
		}
		return a.Port - b.Port
	})
	return dstPort, out, nil
}

// resourceName returns a deterministic AWS-valid ELBv2 name (<= 32 chars,
// alphanumeric + hyphens, not starting with "internal-" or a hyphen).
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
	base := strings.Trim(b.String(), "-")
	if after, ok := strings.CutPrefix(base, "internal-"); ok {
		base = "lb-" + after
	}
	if base == "" {
		base = "lb"
	}
	sum := sha256.Sum256([]byte(p.class + "/" + ref.String()))
	suffix := hex.EncodeToString(sum[:])[:6]
	if len(base) > 25 {
		base = strings.TrimRight(base[:25], "-")
	}
	return base + "-" + suffix
}

// XML response structures for ELBv2 Query API (2015-12-01).

type xmlLoadBalancer struct {
	ARN     string `xml:"LoadBalancerArn"`
	DNSName string `xml:"DNSName"`
	Name    string `xml:"LoadBalancerName"`
	Scheme  string `xml:"Scheme"`
	State   string `xml:"State>Code"`
}

type xmlDescribeLoadBalancersResponse struct {
	LoadBalancers []xmlLoadBalancer `xml:"DescribeLoadBalancersResult>LoadBalancers>member"`
}

type xmlCreateLoadBalancerResponse struct {
	LoadBalancers []xmlLoadBalancer `xml:"CreateLoadBalancerResult>LoadBalancers>member"`
}

type xmlTargetGroup struct {
	ARN      string `xml:"TargetGroupArn"`
	Name     string `xml:"TargetGroupName"`
	Port     int    `xml:"Port"`
	Protocol string `xml:"Protocol"`
}

type xmlDescribeTargetGroupsResponse struct {
	TargetGroups []xmlTargetGroup `xml:"DescribeTargetGroupsResult>TargetGroups>member"`
}

type xmlCreateTargetGroupResponse struct {
	TargetGroups []xmlTargetGroup `xml:"CreateTargetGroupResult>TargetGroups>member"`
}

type xmlTargetHealthDescription struct {
	ID   string `xml:"Target>Id"`
	Port int    `xml:"Target>Port"`
}

type xmlDescribeTargetHealthResponse struct {
	Descriptions []xmlTargetHealthDescription `xml:"DescribeTargetHealthResult>TargetHealthDescriptions>member"`
}

type xmlListener struct {
	ARN            string   `xml:"ListenerArn"`
	Port           int      `xml:"Port"`
	Protocol       string   `xml:"Protocol"`
	TargetGroupARN []string `xml:"DefaultActions>member>TargetGroupArn"`
}

type xmlDescribeListenersResponse struct {
	Listeners []xmlListener `xml:"DescribeListenersResult>Listeners>member"`
}

type xmlErrorResponse struct {
	Code    string `xml:"Error>Code"`
	Message string `xml:"Error>Message"`
}

type awsAPIError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e *awsAPIError) Error() string {
	return fmt.Sprintf("aws elbv2 (%s): %s", e.Code, e.Message)
}

func isAWSError(err error, code string) bool {
	var apiErr *awsAPIError
	return errors.As(err, &apiErr) && apiErr.Code == code
}

func (p *Provider) describeLoadBalancer(ctx context.Context, name string) (*xmlLoadBalancer, error) {
	q := url.Values{"Names.member.1": {name}}
	var resp xmlDescribeLoadBalancersResponse
	if err := p.call(ctx, "DescribeLoadBalancers", q, &resp); err != nil {
		if isAWSError(err, "LoadBalancerNotFound") {
			return nil, nil
		}
		return nil, err
	}
	if len(resp.LoadBalancers) == 0 {
		return nil, nil
	}
	return &resp.LoadBalancers[0], nil
}

func (p *Provider) createLoadBalancer(ctx context.Context, req lb.Request, name, scheme string) (*xmlLoadBalancer, error) {
	subnets := p.subnets
	if req.Internal {
		subnets = p.internalSubnets
	}
	q := url.Values{
		"Name":   {name},
		"Type":   {"network"},
		"Scheme": {scheme},
	}
	for i, s := range subnets {
		q.Set(fmt.Sprintf("Subnets.member.%d", i+1), s)
	}
	for i, sg := range p.securityGroups {
		q.Set(fmt.Sprintf("SecurityGroups.member.%d", i+1), sg)
	}
	tags := [][2]string{
		{"norca.io/namespace", req.Namespace},
		{"norca.io/name", req.Name},
		{"norca.io/class", p.class},
	}
	for i, kv := range tags {
		q.Set(fmt.Sprintf("Tags.member.%d.Key", i+1), kv[0])
		q.Set(fmt.Sprintf("Tags.member.%d.Value", i+1), kv[1])
	}

	var resp xmlCreateLoadBalancerResponse
	if err := p.call(ctx, "CreateLoadBalancer", q, &resp); err != nil {
		return nil, err
	}
	if len(resp.LoadBalancers) == 0 {
		return nil, fmt.Errorf("aws CreateLoadBalancer returned no load balancer")
	}
	p.log.Info("created AWS Network Load Balancer", "name", name, "arn", resp.LoadBalancers[0].ARN)
	return &resp.LoadBalancers[0], nil
}

func (p *Provider) reconcileCrossZone(ctx context.Context, lbARN string, enabled bool) error {
	q := url.Values{
		"LoadBalancerArn":           {lbARN},
		"Attributes.member.1.Key":   {"load_balancing.cross_zone.enabled"},
		"Attributes.member.1.Value": {strconv.FormatBool(enabled)},
	}
	return p.call(ctx, "ModifyLoadBalancerAttributes", q, nil)
}

func (p *Provider) describeTargetGroup(ctx context.Context, name string) (*xmlTargetGroup, error) {
	q := url.Values{"Names.member.1": {name}}
	var resp xmlDescribeTargetGroupsResponse
	if err := p.call(ctx, "DescribeTargetGroups", q, &resp); err != nil {
		if isAWSError(err, "TargetGroupNotFound") {
			return nil, nil
		}
		return nil, err
	}
	if len(resp.TargetGroups) == 0 {
		return nil, nil
	}
	return &resp.TargetGroups[0], nil
}

func (p *Provider) ensureTargetGroup(ctx context.Context, req lb.Request, name string, dstPort int) (string, error) {
	tg, err := p.describeTargetGroup(ctx, name)
	if err != nil {
		return "", err
	}
	if tg != nil {
		return tg.ARN, nil
	}
	q := url.Values{
		"Name":                {name},
		"Protocol":            {"TCP"},
		"Port":                {strconv.Itoa(dstPort)},
		"VpcId":               {p.vpcID},
		"TargetType":          {"ip"},
		"HealthCheckProtocol": {"TCP"},
		"Tags.member.1.Key":   {"norca.io/namespace"},
		"Tags.member.1.Value": {req.Namespace},
		"Tags.member.2.Key":   {"norca.io/name"},
		"Tags.member.2.Value": {req.Name},
		"Tags.member.3.Key":   {"norca.io/class"},
		"Tags.member.3.Value": {p.class},
	}
	var resp xmlCreateTargetGroupResponse
	if err := p.call(ctx, "CreateTargetGroup", q, &resp); err != nil {
		return "", err
	}
	if len(resp.TargetGroups) == 0 {
		return "", fmt.Errorf("aws CreateTargetGroup returned no target group")
	}
	return resp.TargetGroups[0].ARN, nil
}

func (p *Provider) reconcileTargets(ctx context.Context, tgARN string, desired []targetEndpoint) error {
	var healthResp xmlDescribeTargetHealthResponse
	if err := p.call(ctx, "DescribeTargetHealth", url.Values{"TargetGroupArn": {tgARN}}, &healthResp); err != nil {
		return err
	}
	currentSet := make(map[targetEndpoint]bool, len(healthResp.Descriptions))
	for _, d := range healthResp.Descriptions {
		currentSet[targetEndpoint{IP: d.ID, Port: d.Port}] = true
	}
	desiredSet := make(map[targetEndpoint]bool, len(desired))
	var toAdd []targetEndpoint
	for _, ep := range desired {
		desiredSet[ep] = true
		if !currentSet[ep] {
			toAdd = append(toAdd, ep)
		}
	}
	var toRemove []targetEndpoint
	for ep := range currentSet {
		if !desiredSet[ep] {
			toRemove = append(toRemove, ep)
		}
	}

	if len(toAdd) > 0 {
		q := url.Values{"TargetGroupArn": {tgARN}}
		for i, ep := range toAdd {
			q.Set(fmt.Sprintf("Targets.member.%d.Id", i+1), ep.IP)
			q.Set(fmt.Sprintf("Targets.member.%d.Port", i+1), strconv.Itoa(ep.Port))
		}
		if err := p.call(ctx, "RegisterTargets", q, nil); err != nil {
			return err
		}
	}
	if len(toRemove) > 0 {
		q := url.Values{"TargetGroupArn": {tgARN}}
		for i, ep := range toRemove {
			q.Set(fmt.Sprintf("Targets.member.%d.Id", i+1), ep.IP)
			q.Set(fmt.Sprintf("Targets.member.%d.Port", i+1), strconv.Itoa(ep.Port))
		}
		if err := p.call(ctx, "DeregisterTargets", q, nil); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) describeListeners(ctx context.Context, lbARN string) ([]xmlListener, error) {
	var resp xmlDescribeListenersResponse
	if err := p.call(ctx, "DescribeListeners", url.Values{"LoadBalancerArn": {lbARN}}, &resp); err != nil {
		if isAWSError(err, "LoadBalancerNotFound") {
			return nil, nil
		}
		return nil, err
	}
	return resp.Listeners, nil
}

func (p *Provider) ensureListener(ctx context.Context, lbARN, tgARN string, port int) error {
	listeners, err := p.describeListeners(ctx, lbARN)
	if err != nil {
		return err
	}
	found := false
	for _, l := range listeners {
		if l.Port != port {
			_ = p.call(ctx, "DeleteListener", url.Values{"ListenerArn": {l.ARN}}, nil)
			continue
		}
		found = true
		if !slices.Contains(l.TargetGroupARN, tgARN) {
			q := url.Values{
				"ListenerArn":                            {l.ARN},
				"Port":                                   {strconv.Itoa(port)},
				"Protocol":                               {"TCP"},
				"DefaultActions.member.1.Type":           {"forward"},
				"DefaultActions.member.1.TargetGroupArn": {tgARN},
			}
			if err := p.call(ctx, "ModifyListener", q, nil); err != nil {
				return err
			}
		}
	}
	if !found {
		q := url.Values{
			"LoadBalancerArn":                        {lbARN},
			"Port":                                   {strconv.Itoa(port)},
			"Protocol":                               {"TCP"},
			"DefaultActions.member.1.Type":           {"forward"},
			"DefaultActions.member.1.TargetGroupArn": {tgARN},
		}
		return p.call(ctx, "CreateListener", q, nil)
	}
	return nil
}

// call executes a signed ELBv2 Query API POST request and decodes the XML response.
func (p *Provider) call(ctx context.Context, action string, params url.Values, out any) error {
	if params == nil {
		params = url.Values{}
	}
	params.Set("Action", action)
	params.Set("Version", apiVersion)
	body := params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.endpoint+"/", strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("building aws request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	p.signRequest(req, []byte(body))

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("aws elbv2 %s: %w", action, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("reading aws elbv2 response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp xmlErrorResponse
		if xml.Unmarshal(raw, &errResp) == nil && errResp.Code != "" {
			if errResp.Code == "ValidationError" || errResp.Code == "InvalidConfigurationRequest" || errResp.Code == "SubnetNotFound" {
				return lb.Invalid("aws elbv2 %s (%s): %s", action, errResp.Code, errResp.Message)
			}
			return &awsAPIError{StatusCode: resp.StatusCode, Code: errResp.Code, Message: errResp.Message}
		}
		return fmt.Errorf("aws elbv2 %s: status %d: %s", action, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	if out != nil && len(raw) > 0 {
		if err := xml.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decoding aws elbv2 %s XML: %w", action, err)
		}
	}
	return nil
}

// signRequest signs req using AWS Signature Version 4 (HMAC-SHA256).
func (p *Provider) signRequest(req *http.Request, payload []byte) {
	now := p.now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256Hex(payload)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if p.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", p.sessionToken)
	}

	host := req.URL.Host
	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	signedHeaders := "content-type;host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := fmt.Sprintf("content-type:%s\nhost:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
		req.Header.Get("Content-Type"), host, payloadHash, amzDate)
	if p.sessionToken != "" {
		signedHeaders += ";x-amz-security-token"
		canonicalHeaders += fmt.Sprintf("x-amz-security-token:%s\n", p.sessionToken)
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, p.region, serviceName)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+p.secretAccessKey), dateStamp)
	kRegion := hmacSHA256(kDate, p.region)
	kService := hmacSHA256(kRegion, serviceName)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		p.accessKeyID, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", authHeader)
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
