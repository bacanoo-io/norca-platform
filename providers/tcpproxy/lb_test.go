// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package tcpproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// backend answers every connection with its name and closes.
func backend(t *testing.T, name string) (addr string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = fmt.Fprintf(c, "%s\n", name)
			}()
		}
	}()
	a := ln.Addr().(*net.TCPAddr)
	return a.IP.String(), a.Port
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func newProvider(t *testing.T) *Provider {
	t.Helper()
	values, err := params.Resolve(classParams, map[string]any{"advertise_address": "127.0.0.1", "bind_address": "127.0.0.1", "dial_timeout": "500ms"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewLB(lb.Config{Class: "tcp", Values: values})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.(*Provider).Close() })
	return p.(*Provider)
}

func ensure(t *testing.T, p *Provider, name string, port int, targets ...string) lb.State {
	t.Helper()
	req := lb.Request{
		Ref: lb.Ref{Namespace: "default", Name: name}, Service: "web", Port: port,
		Params: params.Values{"dial_timeout": 500 * time.Millisecond},
	}
	for _, tg := range targets {
		host, ps, _ := strings.Cut(tg, ":")
		n, _ := strconv.Atoi(ps)
		req.Targets = append(req.Targets, lb.Target{Address: host, Port: n})
	}
	state, err := p.Ensure(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// ask connects to the load balancer and returns what the backend said.
func ask(port int) (string, error) {
	c, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	line, err := bufio.NewReader(c).ReadString('\n')
	return strings.TrimSpace(line), err
}

func TestRoundRobinAndTargetChange(t *testing.T) {
	p := newProvider(t)
	a1, ap := backend(t, "a")
	b1, bp := backend(t, "b")
	port := freePort(t)

	state := ensure(t, p, "web", port, fmt.Sprintf("%s:%d", a1, ap), fmt.Sprintf("%s:%d", b1, bp))
	if !state.Ready || state.Addresses[0] != net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) {
		t.Fatalf("state: %+v", state)
	}
	seen := map[string]int{}
	for range 8 {
		got, err := ask(port)
		if err != nil {
			t.Fatal(err)
		}
		seen[got]++
	}
	if seen["a"] != 4 || seen["b"] != 4 {
		t.Fatalf("round robin: %v", seen)
	}

	// A target change applies to the running listener.
	ensure(t, p, "web", port, fmt.Sprintf("%s:%d", b1, bp))
	for range 4 {
		if got, err := ask(port); err != nil || got != "b" {
			t.Fatalf("after the change: %q, %v", got, err)
		}
	}

	// No targets: not ready, and connections are closed.
	state = ensure(t, p, "web", port)
	if state.Ready || state.Message == "" {
		t.Fatalf("no targets: %+v", state)
	}
	if got, err := ask(port); err == nil && got != "" {
		t.Fatalf("a load balancer without targets answered %q", got)
	}
}

func TestFailover(t *testing.T) {
	p := newProvider(t)
	a1, ap := backend(t, "a")
	dead := freePort(t) // nothing listens there
	port := freePort(t)
	ensure(t, p, "web", port, fmt.Sprintf("127.0.0.1:%d", dead), fmt.Sprintf("%s:%d", a1, ap))
	for i := range 4 {
		if got, err := ask(port); err != nil || got != "a" {
			t.Fatalf("connection %d: %q, %v", i, got, err)
		}
	}
}

func TestPortChangeDeleteAndConflict(t *testing.T) {
	p := newProvider(t)
	a1, ap := backend(t, "a")
	target := fmt.Sprintf("%s:%d", a1, ap)
	first, second := freePort(t), freePort(t)

	ensure(t, p, "web", first, target)
	if got, err := ask(first); err != nil || got != "a" {
		t.Fatalf("first port: %q, %v", got, err)
	}

	// Another load balancer cannot take the port.
	_, err := p.Ensure(context.Background(), lb.Request{
		Ref: lb.Ref{Namespace: "default", Name: "other"}, Port: first,
		Params: params.Values{"dial_timeout": time.Second},
	})
	if !errors.Is(err, lb.ErrInvalid) {
		t.Fatalf("a taken port: %v", err)
	}

	// Moving to another port frees the first one.
	ensure(t, p, "web", second, target)
	if _, err := ask(first); err == nil {
		t.Fatal("the old port still listens")
	}
	if got, err := ask(second); err != nil || got != "a" {
		t.Fatalf("second port: %q, %v", got, err)
	}

	if err := p.Delete(context.Background(), lb.Ref{Namespace: "default", Name: "web"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ask(second); err == nil {
		t.Fatal("a deleted load balancer still listens")
	}
	if err := p.Delete(context.Background(), lb.Ref{Namespace: "default", Name: "web"}); err != nil {
		t.Fatalf("deleting twice: %v", err)
	}
	if got := p.Targets(lb.Ref{Namespace: "default", Name: "web"}); got != nil {
		t.Fatalf("targets of a deleted load balancer: %v", got)
	}
}

func TestNewValidates(t *testing.T) {
	values, _ := params.Resolve(classParams, map[string]any{"advertise_address": "lb.example.com"})
	if _, err := NewLB(lb.Config{Class: "x", Values: values}); err == nil {
		t.Fatal("a host name is not an advertise_address")
	}
}
