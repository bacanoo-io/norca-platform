// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package nginx is a provider type that runs a load balancer as nginx stream
// (TCP) servers on the machine the provider runs on. It writes one file per
// load balancer into a directory that the nginx configuration includes in its
// "stream" context, and reloads nginx when something changed:
//
//	stream {
//	    include /etc/nginx/norca-lb/*.conf;
//	}
//
// It is also a model of a provider with real side effects: the state is the
// directory, so a restarted provider continues where it was; a change that
// nginx rejects is rolled back; two load balancers cannot take the same
// listening port.
//
// Type ID: "nginx".
package nginx

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// TypeID is the ID of the type.
const TypeID = "nginx"

var classParams = []params.Param{
	{Name: "config_dir", Type: params.ParamString, Required: true,
		Description: "directory for the generated files; nginx includes *.conf of it in its stream context"},
	{Name: "advertise_address", Type: params.ParamString, Required: true,
		Description: "address clients use to reach the load balancers (the address of this machine)"},
	{Name: "listen_address", Type: params.ParamString, Default: "0.0.0.0",
		Description: "address nginx listens on for load balancers that are not internal"},
	{Name: "internal_listen_address", Type: params.ParamString, Default: "",
		Description: "address nginx listens on for internal load balancers; empty means the class has none"},
	{Name: "reload_command", Type: params.ParamList, Default: []string{"nginx", "-s", "reload"},
		Description: "command that makes nginx read its configuration; empty means no reload"},
}

// Runner runs the reload command. It is replaced in tests.
type Runner func(ctx context.Context, argv []string) error

func execRunner(ctx context.Context, argv []string) error {
	out, err := exec.CommandContext(ctx, argv[0], argv[1:]...).CombinedOutput() //nolint:gosec // G204: operator-configured reload_command by design
	if err != nil {
		return fmt.Errorf("%s: %w: %s", strings.Join(argv, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Provider is the nginx instance.
type Provider struct {
	class         string
	dir           string
	advertise     string
	listen        string
	internalListn string
	caps          lb.Capabilities
	reload        []string

	// Run runs the reload command.
	Run Runner

	mu sync.Mutex
	// listeners maps "listen:port" to the load balancer that owns it.
	listeners map[string]lb.Ref
	// files maps a load balancer to its listener.
	files map[lb.Ref]string
}

// NewLB creates an instance and reads the load balancers already in the
// directory.
func NewLB(cfg lb.Config) (lb.Provider, error) {
	v := cfg.Values
	for _, name := range []string{"advertise_address", "listen_address", "internal_listen_address"} {
		if ip := v.String(name); ip != "" && net.ParseIP(ip) == nil {
			return nil, fmt.Errorf("%s %q is not an IP address", name, ip)
		}
	}
	dir := v.String("config_dir")
	//nolint:gosec // G301: nginx worker processes need read and execute access to the stream include directory
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("config_dir: %w", err)
	}
	p := &Provider{
		class:         cfg.Class,
		dir:           dir,
		advertise:     v.String("advertise_address"),
		listen:        v.String("listen_address"),
		internalListn: v.String("internal_listen_address"),
		caps:          lb.Capabilities{Internal: v.String("internal_listen_address") != ""},
		reload:        v.List("reload_command"),
		Run:           execRunner,
		listeners:     map[string]lb.Ref{},
		files:         map[lb.Ref]string{},
	}
	if err := p.scan(); err != nil {
		return nil, err
	}
	return p, nil
}

// Capabilities implements lb.Provider.
func (p *Provider) Capabilities() lb.Capabilities { return p.caps }

const header = "# Managed by norca-loadbalancer. Do not edit.\n"

// fileFor returns the path of the file of a load balancer.
func (p *Provider) fileFor(ref lb.Ref) string {
	return filepath.Join(p.dir, ref.Namespace+"."+ref.Name+".conf")
}

// scan reads the files written by an earlier run, so the listeners they hold
// are known.
func (p *Provider) scan() error {
	files, err := filepath.Glob(filepath.Join(p.dir, "*.conf"))
	if err != nil {
		return err
	}
	for _, f := range files {
		raw, err := os.ReadFile(f) //nolint:gosec // G304: scanning config_dir files managed by this provider
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(bytes.NewReader(raw))
		for sc.Scan() {
			rest, ok := strings.CutPrefix(sc.Text(), "# norca-lb: ")
			if !ok {
				continue
			}
			var ref lb.Ref
			var listener string
			for kv := range strings.FieldsSeq(rest) {
				k, val, _ := strings.Cut(kv, "=")
				switch k {
				case "namespace":
					ref.Namespace = val
				case "name":
					ref.Name = val
				case "listener":
					listener = val
				}
			}
			if ref.Name != "" && listener != "" {
				p.listeners[listener] = ref
				p.files[ref] = listener
			}
		}
	}
	return nil
}

// upstreamName is a stable nginx identifier of a load balancer.
func upstreamName(ref lb.Ref) string {
	sum := sha256.Sum256([]byte(ref.String()))
	return "lb_" + hex.EncodeToString(sum[:6])
}

// render returns the file of a load balancer.
func render(ref lb.Ref, listener string, req lb.Request) string {
	var b strings.Builder
	b.WriteString(header)
	fmt.Fprintf(&b, "# norca-lb: namespace=%s name=%s listener=%s\n", ref.Namespace, ref.Name, listener)
	fmt.Fprintf(&b, "# service %s\n", sanitizeComment(req.Service))
	up := upstreamName(ref)
	fmt.Fprintf(&b, "upstream %s {\n", up)
	targets := make([]string, 0, len(req.Targets))
	for _, t := range req.Targets {
		targets = append(targets, net.JoinHostPort(t.Address, strconv.Itoa(t.Port)))
	}
	sort.Strings(targets)
	if len(targets) == 0 {
		// An upstream needs a server; this one is down, so connections fail.
		b.WriteString("    server 127.0.0.1:9 down;\n")
	}
	for _, t := range targets {
		fmt.Fprintf(&b, "    server %s;\n", t)
	}
	b.WriteString("}\n")
	fmt.Fprintf(&b, "server {\n    listen %s;\n    proxy_pass %s;\n}\n", listener, up)
	return b.String()
}

func sanitizeComment(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' {
			return ' '
		}
		return r
	}, s)
}

// Ensure implements lb.Provider.
func (p *Provider) Ensure(ctx context.Context, req lb.Request) (lb.State, error) {
	listenAddr := p.listen
	if req.Internal {
		if p.internalListn == "" {
			return lb.State{}, lb.Invalid("this class has no internal_listen_address")
		}
		listenAddr = p.internalListn
	}
	listener := net.JoinHostPort(listenAddr, strconv.Itoa(req.Port))
	advertised := net.JoinHostPort(p.advertise, strconv.Itoa(req.Port))
	if req.Internal {
		advertised = net.JoinHostPort(p.internalListn, strconv.Itoa(req.Port))
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if owner, taken := p.listeners[listener]; taken && owner != req.Ref {
		return lb.State{}, lb.Invalid("port %d on %s is used by load balancer %s", req.Port, listenAddr, owner)
	}

	path := p.fileFor(req.Ref)
	content := render(req.Ref, listener, req)
	old, readErr := os.ReadFile(path) //nolint:gosec // G304: path is built from validated namespace and name in config_dir
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return lb.State{}, readErr
	}
	changed := string(old) != content
	if changed {
		if err := writeAtomic(path, []byte(content)); err != nil {
			return lb.State{}, err
		}
		if err := p.reloadLocked(ctx); err != nil {
			// nginx rejected the file: go back to what worked.
			if readErr == nil {
				_ = writeAtomic(path, old)
			} else {
				_ = os.Remove(path)
			}
			_ = p.reloadLocked(ctx)
			return lb.State{}, fmt.Errorf("nginx did not accept the configuration: %w", err)
		}
	}
	if prev, ok := p.files[req.Ref]; ok && prev != listener {
		delete(p.listeners, prev)
	}
	p.listeners[listener] = req.Ref
	p.files[req.Ref] = listener

	state := lb.State{Addresses: []string{advertised}, Resource: path}
	if len(req.Targets) == 0 {
		state.Message = "no targets"
	} else {
		state.Ready = true
	}
	return state, nil
}

// Delete implements lb.Provider.
func (p *Provider) Delete(ctx context.Context, ref lb.Ref) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	path := p.fileFor(ref)
	old, err := os.ReadFile(path) //nolint:gosec // G304: path is built from validated namespace and name in config_dir
	if errors.Is(err, os.ErrNotExist) {
		delete(p.listeners, p.files[ref])
		delete(p.files, ref)
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	if err := p.reloadLocked(ctx); err != nil {
		_ = writeAtomic(path, old)
		return fmt.Errorf("nginx did not accept the configuration: %w", err)
	}
	delete(p.listeners, p.files[ref])
	delete(p.files, ref)
	return nil
}

func (p *Provider) reloadLocked(ctx context.Context) error {
	if len(p.reload) == 0 {
		return nil
	}
	return p.Run(ctx, p.reload)
}

// writeAtomic replaces a file so nginx never reads a half-written one.
func writeAtomic(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".norca-lb-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}
	if _, err := f.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := f.Chmod(0o644); err != nil {
		cleanup()
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
