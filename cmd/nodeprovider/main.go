// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Command nodeprovider is a node provider: it serves the provider contract
// of the node scaling operator over HTTP and registers itself in Norca as a
// NodeProvider object with a heartbeat. The type "docker" runs each instance
// as a container on this host with the agent of its bootstrap (the lab); the
// type "mock" keeps the instances
// in memory and creates no machine, for trials and tests.
//
//	nodeprovider -name mock -listen :8480 -endpoint http://10.0.0.5:8480 \
//	  -group dc1:4000:8192:40000:10 -token-file /secrets/token
//
// The Norca API is read from the environment (NORCA_ADDR, NORCA_TOKEN); as a
// job, NORCA_ADDR=unix://${NORCA_SECRETS_DIR}/api.sock with the workload
// identity.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider/docker"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider/mock"
	"github.com/bacanoo-io/norca-platform/nodescaler/registrar"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

// groupFlags collects the -group flags.
type groupFlags []string

func (g *groupFlags) String() string     { return strings.Join(*g, ",") }
func (g *groupFlags) Set(v string) error { *g = append(*g, v); return nil }

// parseGroup reads "name:cpu:memory:disk[:max]".
func parseGroup(s string) (string, mock.GroupConfig, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 && len(parts) != 5 {
		return "", mock.GroupConfig{}, fmt.Errorf("group %q: want name:cpu:memory:disk[:max]", s)
	}
	var nums [4]int64
	for i, p := range parts[1:] {
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil || n < 0 {
			return "", mock.GroupConfig{}, fmt.Errorf("group %q: %q is not a number", s, p)
		}
		nums[i] = n
	}
	if parts[0] == "" || nums[0] == 0 || nums[1] == 0 {
		return "", mock.GroupConfig{}, fmt.Errorf("group %q: a name, cpu and memory are required", s)
	}
	return parts[0], mock.GroupConfig{
		Capacity: provider.Capacity{CPU: nums[0], Memory: nums[1], Disk: nums[2]},
		Max:      int(nums[3]),
	}, nil
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "nodeprovider:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("nodeprovider", flag.ContinueOnError)
	var (
		groups        groupFlags
		name          = fs.String("name", "", "name of the NodeProvider object (required)")
		typ           = fs.String("type", "mock", "the provider type: mock or docker")
		dockerImage   = fs.String("docker-image", "debian:bookworm-slim", "docker: the image of the containers")
		dockerBinary  = fs.String("docker-binary", "/usr/local/bin/norca", "docker: the norca binary of the host, mounted into the containers")
		dockerJoin    = fs.String("docker-join-binary", "/usr/local/bin/norca-join", "docker: norca-join of the host, mounted into the containers of nodes that join with a key of their own")
		dockerState   = fs.String("docker-state-dir", "/var/lib/norca-nodeprovider", "docker: where the files of the nodes are written")
		dockerNetwork = fs.String("docker-network", "bridge", "docker: the network of the containers")
		listen        = fs.String("listen", ":8480", "address to serve the provider contract on")
		endpoint      = fs.String("endpoint", "", "the URL the operator calls (default http://<listen>)")
		tokenFile     = fs.String("token-file", "", "file with the bearer token the operator must send")
		tokenVariable = fs.String("token-variable", "", "path of the Variable that holds the token, for the operator")
		heartbeat     = fs.Duration("heartbeat", 30*time.Second, "period of the heartbeat")
		register      = fs.Bool("register", true, "register the NodeProvider object in Norca")
		showVersion   = fs.Bool("version", false, "print the version and exit")
	)
	fs.Var(&groups, "group", "a group, name:cpu:memory:disk[:max] (CPU in MHz, memory and disk in MB); repeatable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Println(version)
		return nil
	}
	if *name == "" {
		return errors.New("-name is required")
	}
	if *typ != "mock" && *typ != "docker" {
		return fmt.Errorf("unknown type %q (linked types: mock, docker)", *typ)
	}
	if len(groups) == 0 {
		return errors.New("at least one -group is required")
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	configs := map[string]mock.GroupConfig{}
	var specs []registrar.GroupSpec
	for _, g := range groups {
		gname, cfg, err := parseGroup(g)
		if err != nil {
			return err
		}
		if _, dup := configs[gname]; dup {
			return fmt.Errorf("group %q is given twice", gname)
		}
		configs[gname] = cfg
		specs = append(specs, registrar.GroupSpec{Name: gname, Max: cfg.Max, Parameters: map[string]any{
			"cpu": cfg.Capacity.CPU, "memory": cfg.Capacity.Memory, "disk": cfg.Capacity.Disk,
		}})
	}
	token := ""
	if *tokenFile != "" {
		data, err := os.ReadFile(*tokenFile)
		if err != nil {
			return fmt.Errorf("reading the token: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	if *endpoint == "" {
		host := *listen
		if strings.HasPrefix(host, ":") {
			host = "127.0.0.1" + host
		}
		*endpoint = "http://" + host
	}

	var impl provider.Provider = mock.New(configs)
	if *typ == "docker" {
		groups := map[string]docker.GroupConfig{}
		for name, cfg := range configs {
			groups[name] = docker.GroupConfig{Capacity: cfg.Capacity, Max: cfg.Max}
		}
		d, err := docker.New(docker.Config{Groups: groups, Image: *dockerImage, Binary: *dockerBinary,
			JoinBinary: *dockerJoin, StateDir: *dockerState, Network: *dockerNetwork})
		if err != nil {
			return err
		}
		impl = d
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              *listen,
		Handler:           provider.NewHandler(impl, token, log),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("serving the provider contract", "address", *listen, "type", *typ, "groups", len(configs))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()

	if *register {
		store, err := registrar.NewAPIStore(nil)
		if err != nil {
			return err
		}
		reg := &registrar.Registrar{
			Store: store,
			Registration: registrar.Registration{
				Name: *name, Endpoint: *endpoint, TokenVariable: *tokenVariable, Groups: specs,
				Parameters: map[string]any{"type": *typ},
			},
			Interval: *heartbeat, Version: version, DeregisterOnExit: true, Logger: log,
		}
		go func() { _ = reg.Run(ctx) }()
	}

	select {
	case <-ctx.Done():
	case err := <-errc:
		if err != nil {
			return err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}
