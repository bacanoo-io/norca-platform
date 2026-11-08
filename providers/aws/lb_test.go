// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package aws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/lb"
)

type fakeELBv2Server struct {
	mu          sync.Mutex
	lbs         map[string]*xmlLoadBalancer
	tgs         map[string]*xmlTargetGroup
	targets     map[string][]targetEndpoint
	listeners   map[string][]xmlListener
	crossZone   map[string]string
	lastAuthHdr string
}

func newFakeELBv2(t *testing.T) (*fakeELBv2Server, *httptest.Server) {
	t.Helper()
	f := &fakeELBv2Server{
		lbs:       map[string]*xmlLoadBalancer{},
		tgs:       map[string]*xmlTargetGroup{},
		targets:   map[string][]targetEndpoint{},
		listeners: map[string][]xmlListener{},
		crossZone: map[string]string{},
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeELBv2Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=") {
		w.WriteHeader(http.StatusForbidden)
		_, _ = fmt.Fprint(w, `<ErrorResponse><Error><Code>InvalidClientTokenId</Code><Message>missing SigV4</Message></Error></ErrorResponse>`)
		return
	}

	raw, _ := io.ReadAll(r.Body)
	vals, _ := url.ParseQuery(string(raw))
	action := vals.Get("Action")

	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastAuthHdr = auth

	w.Header().Set("Content-Type", "text/xml")

	switch action {
	case "DescribeLoadBalancers":
		name := vals.Get("Names.member.1")
		curLB, ok := f.lbs[name]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `<ErrorResponse><Error><Code>LoadBalancerNotFound</Code><Message>not found</Message></Error></ErrorResponse>`)
			return
		}
		_, _ = fmt.Fprintf(w, `<DescribeLoadBalancersResponse><DescribeLoadBalancersResult><LoadBalancers><member><LoadBalancerArn>%s</LoadBalancerArn><DNSName>%s</DNSName><LoadBalancerName>%s</LoadBalancerName><Scheme>%s</Scheme><State><Code>%s</Code></State></member></LoadBalancers></DescribeLoadBalancersResult></DescribeLoadBalancersResponse>`,
			curLB.ARN, curLB.DNSName, curLB.Name, curLB.Scheme, curLB.State)

	case "CreateLoadBalancer":
		name := vals.Get("Name")
		scheme := vals.Get("Scheme")
		curLB := &xmlLoadBalancer{
			ARN:     "arn:aws:elasticloadbalancing:eu-central-1:123456789012:loadbalancer/net/" + name + "/1111",
			DNSName: name + "-1111.elb.eu-central-1.amazonaws.com",
			Name:    name,
			Scheme:  scheme,
			State:   "active",
		}
		f.lbs[name] = curLB
		_, _ = fmt.Fprintf(w, `<CreateLoadBalancerResponse><CreateLoadBalancerResult><LoadBalancers><member><LoadBalancerArn>%s</LoadBalancerArn><DNSName>%s</DNSName><LoadBalancerName>%s</LoadBalancerName><Scheme>%s</Scheme><State><Code>%s</Code></State></member></LoadBalancers></CreateLoadBalancerResult></CreateLoadBalancerResponse>`,
			curLB.ARN, curLB.DNSName, curLB.Name, curLB.Scheme, curLB.State)

	case "ModifyLoadBalancerAttributes":
		arn := vals.Get("LoadBalancerArn")
		f.crossZone[arn] = vals.Get("Attributes.member.1.Value")
		_, _ = fmt.Fprint(w, `<ModifyLoadBalancerAttributesResponse/>`)

	case "DeleteLoadBalancer":
		arn := vals.Get("LoadBalancerArn")
		for k, curLB := range f.lbs {
			if curLB.ARN == arn {
				delete(f.lbs, k)
			}
		}
		delete(f.listeners, arn)
		_, _ = fmt.Fprint(w, `<DeleteLoadBalancerResponse/>`)

	case "DescribeTargetGroups":
		name := vals.Get("Names.member.1")
		tg, ok := f.tgs[name]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `<ErrorResponse><Error><Code>TargetGroupNotFound</Code><Message>not found</Message></Error></ErrorResponse>`)
			return
		}
		_, _ = fmt.Fprintf(w, `<DescribeTargetGroupsResponse><DescribeTargetGroupsResult><TargetGroups><member><TargetGroupArn>%s</TargetGroupArn><TargetGroupName>%s</TargetGroupName><Port>%d</Port><Protocol>%s</Protocol></member></TargetGroups></DescribeTargetGroupsResult></DescribeTargetGroupsResponse>`,
			tg.ARN, tg.Name, tg.Port, tg.Protocol)

	case "CreateTargetGroup":
		name := vals.Get("Name")
		port, _ := strconv.Atoi(vals.Get("Port"))
		tg := &xmlTargetGroup{
			ARN:      "arn:aws:elasticloadbalancing:eu-central-1:123456789012:targetgroup/" + name + "/2222",
			Name:     name,
			Port:     port,
			Protocol: vals.Get("Protocol"),
		}
		f.tgs[name] = tg
		_, _ = fmt.Fprintf(w, `<CreateTargetGroupResponse><CreateTargetGroupResult><TargetGroups><member><TargetGroupArn>%s</TargetGroupArn><TargetGroupName>%s</TargetGroupName><Port>%d</Port><Protocol>%s</Protocol></member></TargetGroups></CreateTargetGroupResult></CreateTargetGroupResponse>`,
			tg.ARN, tg.Name, tg.Port, tg.Protocol)

	case "DeleteTargetGroup":
		arn := vals.Get("TargetGroupArn")
		for k, tg := range f.tgs {
			if tg.ARN == arn {
				delete(f.tgs, k)
			}
		}
		delete(f.targets, arn)
		_, _ = fmt.Fprint(w, `<DeleteTargetGroupResponse/>`)

	case "DescribeTargetHealth":
		arn := vals.Get("TargetGroupArn")
		var members strings.Builder
		for _, ep := range f.targets[arn] {
			_, _ = fmt.Fprintf(&members, `<member><Target><Id>%s</Id><Port>%d</Port></Target></member>`, ep.IP, ep.Port)
		}
		_, _ = fmt.Fprintf(w, `<DescribeTargetHealthResponse><DescribeTargetHealthResult><TargetHealthDescriptions>%s</TargetHealthDescriptions></DescribeTargetHealthResult></DescribeTargetHealthResponse>`, members.String())

	case "RegisterTargets":
		arn := vals.Get("TargetGroupArn")
		for i := 1; ; i++ {
			ip := vals.Get(fmt.Sprintf("Targets.member.%d.Id", i))
			if ip == "" {
				break
			}
			port, _ := strconv.Atoi(vals.Get(fmt.Sprintf("Targets.member.%d.Port", i)))
			f.targets[arn] = append(f.targets[arn], targetEndpoint{IP: ip, Port: port})
		}
		_, _ = fmt.Fprint(w, `<RegisterTargetsResponse/>`)

	case "DeregisterTargets":
		arn := vals.Get("TargetGroupArn")
		for i := 1; ; i++ {
			ip := vals.Get(fmt.Sprintf("Targets.member.%d.Id", i))
			if ip == "" {
				break
			}
			port, _ := strconv.Atoi(vals.Get(fmt.Sprintf("Targets.member.%d.Port", i)))
			f.targets[arn] = slices.DeleteFunc(f.targets[arn], func(ep targetEndpoint) bool {
				return ep.IP == ip && ep.Port == port
			})
		}
		_, _ = fmt.Fprint(w, `<DeregisterTargetsResponse/>`)

	case "DescribeListeners":
		arn := vals.Get("LoadBalancerArn")
		var members strings.Builder
		for _, l := range f.listeners[arn] {
			tgARN := ""
			if len(l.TargetGroupARN) > 0 {
				tgARN = l.TargetGroupARN[0]
			}
			_, _ = fmt.Fprintf(&members, `<member><ListenerArn>%s</ListenerArn><Port>%d</Port><Protocol>%s</Protocol><DefaultActions><member><TargetGroupArn>%s</TargetGroupArn></member></DefaultActions></member>`,
				l.ARN, l.Port, l.Protocol, tgARN)
		}
		_, _ = fmt.Fprintf(w, `<DescribeListenersResponse><DescribeListenersResult><Listeners>%s</Listeners></DescribeListenersResult></DescribeListenersResponse>`, members.String())

	case "CreateListener":
		lbARN := vals.Get("LoadBalancerArn")
		port, _ := strconv.Atoi(vals.Get("Port"))
		tgARN := vals.Get("DefaultActions.member.1.TargetGroupArn")
		l := xmlListener{
			ARN:            fmt.Sprintf("%s:listener/%d", lbARN, port),
			Port:           port,
			Protocol:       vals.Get("Protocol"),
			TargetGroupARN: []string{tgARN},
		}
		f.listeners[lbARN] = append(f.listeners[lbARN], l)
		_, _ = fmt.Fprint(w, `<CreateListenerResponse/>`)

	case "DeleteListener":
		lARN := vals.Get("ListenerArn")
		for lbARN, list := range f.listeners {
			f.listeners[lbARN] = slices.DeleteFunc(list, func(l xmlListener) bool {
				return l.ARN == lARN
			})
		}
		_, _ = fmt.Fprint(w, `<DeleteListenerResponse/>`)

	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func TestAWSLifecycle(t *testing.T) {
	fake, srv := newFakeELBv2(t)

	inst, err := provider.Default.Instantiate("aws-nlb", TypeID, map[string]any{
		"region":            "eu-central-1",
		"vpc_id":            "vpc-12345",
		"subnets":           []string{"subnet-a", "subnet-b"},
		"access_key_id":     "AKIAIOSFODNN7EXAMPLE",
		"secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"endpoint":          srv.URL,
	})
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	if !inst.Provider.Capabilities().Internal {
		t.Fatalf("expected Internal capability for AWS provider")
	}

	ctx := context.Background()
	ref := lb.Ref{Namespace: "prod", Name: "orders"}

	// 1. Create internet-facing NLB.
	params1, _ := inst.Effective(nil)
	state, err := inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "orders",
		Port:    80,
		Targets: []lb.Target{
			{Address: "10.0.1.10", Port: 8080},
			{Address: "10.0.1.11", Port: 8080},
		},
		Params: params1,
	})
	if err != nil {
		t.Fatalf("Ensure create: %v", err)
	}
	if !state.Ready || !strings.HasSuffix(state.Addresses[0], ":80") {
		t.Fatalf("unexpected state: %+v", state)
	}

	// 2. Reconcile targets, port, and dynamic cross_zone=true.
	params2, err := inst.Effective(map[string]any{"cross_zone": true})
	if err != nil {
		t.Fatalf("Effective: %v", err)
	}
	state, err = inst.Provider.Ensure(ctx, lb.Request{
		Ref:     ref,
		Service: "orders",
		Port:    443,
		Targets: []lb.Target{
			{Address: "10.0.1.11", Port: 8080},
			{Address: "10.0.1.12", Port: 9090},
		},
		Params: params2,
	})
	if err != nil {
		t.Fatalf("Ensure update: %v", err)
	}
	if !state.Ready || !strings.HasSuffix(state.Addresses[0], ":443") {
		t.Fatalf("unexpected updated state: %+v", state)
	}

	fake.mu.Lock()
	if fake.crossZone[state.Resource] != "true" {
		t.Fatalf("expected cross_zone=true on %s, got %q", state.Resource, fake.crossZone[state.Resource])
	}
	if len(fake.listeners[state.Resource]) != 1 || fake.listeners[state.Resource][0].Port != 443 {
		t.Fatalf("unexpected listeners: %+v", fake.listeners[state.Resource])
	}
	fake.mu.Unlock()

	// 3. Delete is idempotent.
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := inst.Provider.Delete(ctx, ref); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
}
