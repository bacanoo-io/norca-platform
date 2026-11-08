// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Command norca-provider connects Norca to the platforms it runs on: it
// serves the node providers of the node scaling operator and the load
// balancer classes of the LoadBalancer operator named in its configuration
// file, on one HTTP server, and registers them in Norca (NodeProvider and
// LoadBalancerClass objects) with a heartbeat.
//
//	norca-provider -config providers.json            run
//	norca-provider -config providers.json -serve lb  serve only the classes of the file
//	norca-provider types                             list the linked types and their parameters
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
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/params"
	"github.com/bacanoo-io/norca-platform/provider/registrar"
	_ "github.com/bacanoo-io/norca-platform/providers/all"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "types" {
		printTypes(os.Stdout, provider.Default)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "norca-provider:", err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("norca-provider", flag.ContinueOnError)
	path := fs.String("config", "providers.json", "configuration file")
	serveFlag := fs.String("serve", "all", "what to serve of the configuration: node, lb or all")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn or error")
	register := fs.Bool("register", true, "register the providers and classes in Norca (NORCA_ADDR, NORCA_TOKEN)")
	deregister := fs.Bool("deregister-on-exit", true, "remove the registrations on exit")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Fprintln(stdout, version)
		return nil
	}
	serve, err := provider.ParseServe(*serveFlag)
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("log level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	cfg, err := provider.Load(*path)
	if err != nil {
		return err
	}
	rt, err := cfg.Build(provider.Default, serve, logger)
	if err != nil {
		return err
	}
	defer rt.Close()

	ctx, cancelRun := context.WithCancel(parent)
	defer cancelRun()

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           provider.NewHandler(rt, provider.Default.Types(), logger),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 2)
	go func() {
		logger.Info("serving", "address", cfg.Listen, "node_providers", len(rt.Nodes), "classes", len(rt.Classes), "tls", cfg.TLS != nil)
		var err error
		if cfg.TLS != nil {
			err = srv.ListenAndServeTLS(cfg.TLS.Cert, cfg.TLS.Key)
		} else {
			err = srv.ListenAndServe()
		}
		if !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	var wg sync.WaitGroup
	if *register {
		store, err := registrar.NewAPIStore(nil)
		if err != nil {
			return fmt.Errorf("Norca API: %w", err)
		}
		reg := &registrar.Registrar{
			Store:            store,
			Registrations:    rt.Registrations(),
			Interval:         cfg.Interval(),
			Version:          version,
			DeregisterOnExit: *deregister,
			Logger:           logger,
		}
		wg.Go(func() {
			if err := reg.Run(ctx); err != nil {
				errc <- err
			}
		})
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errc:
	}
	cancelRun()

	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	srvErr := srv.Shutdown(shutdown)
	wg.Wait()

	if runErr != nil {
		return runErr
	}
	return srvErr
}

// printTypes lists the types with their capabilities and parameters.
func printTypes(w io.Writer, reg *provider.Registry) {
	for _, t := range reg.Types() {
		fmt.Fprintf(w, "%s - %s [%s]\n", t.ID, t.Description, strings.Join(t.Capabilities(), ", "))
		printParams(w, "params", t.Params)
		if t.Node != nil {
			printParams(w, "node.params", t.Node.Params)
			printParams(w, "node.groups[].params", t.Node.GroupParams)
		}
		if t.LB != nil {
			printParams(w, "loadbalancer.classes[].params", t.LB.Params)
		}
		fmt.Fprintln(w)
	}
}

func printParams(w io.Writer, section string, ps []params.Param) {
	if len(ps) == 0 {
		return
	}
	fmt.Fprintf(w, "  %s:\n", section)
	for _, p := range ps {
		var flags []string
		if p.Required {
			flags = append(flags, "required")
		}
		if p.Secret {
			flags = append(flags, "secret")
		}
		if p.Dynamic {
			flags = append(flags, "dynamic")
		}
		def := ""
		if p.Default != nil && p.Default != "" {
			def = fmt.Sprintf(" (default %v)", p.Default)
		}
		extra := ""
		if len(flags) > 0 {
			extra = " [" + strings.Join(flags, ", ") + "]"
		}
		fmt.Fprintf(w, "    %-24s %-8s%s%s  %s\n", p.Name, p.Type, def, extra, p.Description)
	}
}
