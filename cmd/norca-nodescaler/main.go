// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Command norca-nodescaler is the node scaling operator: it makes the
// machines of every NodeGroup match its scale, through the group's node
// provider (a registered NodeProvider). It runs as a job of the cluster,
// outside the pools it scales, with two instances and leader election.
//
// The Norca API is read from the environment (NORCA_ADDR, NORCA_TOKEN); as a
// job, NORCA_ADDR=unix://${NORCA_SECRETS_DIR}/api.sock with the workload
// identity. The token needs read and write-status (and write-events) on the
// objects of norca.io/NodeGroup, read on norca.io/NodeProvider, node read
// and write (a foreign node is made ineligible),
// and read on the Variables with the tokens of the providers and on the
// bootstrap Variable (nodescaler/bootstrap/<region>), which holds the key of
// the node CA and must be readable by the operator only.
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
	"syscall"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/operator"
	"github.com/bacanoo-io/norca/norca/controller"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "norca-nodescaler:", err)
		os.Exit(1)
	}
}

type flags struct {
	region         string
	providerTTL    time.Duration
	allowHTTP      bool
	tokenNamespace string
	workers        int
	resync         time.Duration
	leaderElection bool
	leaseNamespace string
	metricsAddr    string
	logLevel       string
	showVersion    bool
	orphanSweep    time.Duration
	orphanAfter    time.Duration
	deleteOrphans  bool
	bootstrap      bool
	bootstrapVar   string
	bootstrapNS    string
	networkVar     string
}

func parseFlags(args []string) (*flags, error) {
	var f flags
	fs := flag.NewFlagSet("norca-nodescaler", flag.ContinueOnError)
	fs.StringVar(&f.region, "region", envOr("NORCA_REGION", "global"), "region of the cluster: a label of the instances and part of the request IDs")
	fs.DurationVar(&f.providerTTL, "provider-ttl", 3*time.Minute, "how old the heartbeat of a NodeProvider may be")
	fs.BoolVar(&f.allowHTTP, "provider-allow-http", false, "accept http endpoints of providers (development only)")
	fs.StringVar(&f.tokenNamespace, "token-namespace", "default", "namespace of the Variables with the tokens of the providers")
	fs.IntVar(&f.workers, "workers", 2, "concurrent reconciles")
	fs.DurationVar(&f.resync, "resync", 5*time.Minute, "period of full resynchronization")
	fs.BoolVar(&f.leaderElection, "leader-election", true, "reconcile only on the elected leader")
	fs.StringVar(&f.leaseNamespace, "lease-namespace", "default", "namespace of the lease Variable")
	fs.StringVar(&f.metricsAddr, "metrics-addr", "", "address of the /metrics endpoint (disabled if empty)")
	fs.StringVar(&f.logLevel, "log-level", "info", "log level: debug, info, warn or error")
	fs.BoolVar(&f.showVersion, "version", false, "print the version and exit")
	fs.DurationVar(&f.orphanSweep, "orphan-sweep", 5*time.Minute, "period of the sweep of orphaned instances")
	fs.DurationVar(&f.orphanAfter, "orphan-after", 15*time.Minute, "age after which an orphaned instance without a node is deleted")
	fs.BoolVar(&f.deleteOrphans, "delete-orphans", false, "delete orphaned instances that run a node too (otherwise they are only reported)")
	fs.BoolVar(&f.bootstrap, "bootstrap", true, "send each instance a bootstrap with the certificate and the configuration of its node")
	fs.StringVar(&f.bootstrapVar, "bootstrap-variable", "", "Variable with the bootstrap template of the region (default nodescaler/bootstrap/<region>)")
	fs.StringVar(&f.bootstrapNS, "bootstrap-namespace", "default", "namespace of the bootstrap Variable")
	fs.StringVar(&f.networkVar, "network-variable", "", "Variable with the cluster ranges of the region, in the namespace of the bootstrap (default nodescaler/network/<region>)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if f.region == "" {
		return nil, errors.New("-region must not be empty")
	}
	if f.workers < 1 {
		return nil, errors.New("-workers must be at least 1")
	}
	if f.bootstrapVar == "" {
		f.bootstrapVar = "nodescaler/bootstrap/" + f.region
	}
	if f.networkVar == "" {
		f.networkVar = "nodescaler/network/" + f.region
	}
	return &f, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run(ctx context.Context, args []string) error {
	f, err := parseFlags(args)
	if err != nil {
		return err
	}
	if f.showVersion {
		fmt.Println(version)
		return nil
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(f.logLevel)); err != nil {
		return fmt.Errorf("-log-level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	client, err := controller.NewAPIClient()
	if err != nil {
		return err
	}
	backend := controller.NewBackend(client)
	metrics := controller.NewTextMetrics()
	if f.metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics)
		srv := &http.Server{Addr: f.metricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics endpoint failed", "error", err)
			}
		}()
		go func() {
			<-ctx.Done()
			_ = srv.Close()
		}()
	}

	var boot operator.BootstrapSource
	if f.bootstrap {
		boot = &operator.VariableBootstrap{Vars: backend, Namespace: f.bootstrapNS, Path: f.bootstrapVar, Region: f.region}
	}
	rec := operator.NewReconciler(backend, &operator.APINodes{Client: client}, operator.Config{
		Bootstrap:      boot,
		Network:        &operator.VariableNetwork{Vars: backend, Namespace: f.bootstrapNS, Path: f.networkVar},
		Region:         f.region,
		ProviderTTL:    f.providerTTL,
		TokenNamespace: f.tokenNamespace,
		AllowHTTP:      f.allowHTTP,
		OrphanAfter:    f.orphanAfter,
		DeleteOrphans:  f.deleteOrphans,
		Logger:         logger,
	})
	var election *controller.LeaderElectionConfig
	if f.leaderElection {
		election = &controller.LeaderElectionConfig{Namespace: f.leaseNamespace, ReleaseOnCancel: true}
	}
	ctrl, err := controller.New(backend, rec, controller.Options{
		Name:    "nodescaler",
		Kind:    operator.NodeGroupKind,
		Storage: controller.StorageNative,
		// Node groups are cluster-scoped.
		Namespace:      "*",
		Workers:        f.workers,
		ResyncPeriod:   f.resync,
		LeaderElection: election,
		Sources:        []controller.Source{&operator.OrphanSweep{Reconciler: rec, Interval: f.orphanSweep}},
		Metrics:        metrics,
		Logger:         logger,
	})
	if err != nil {
		return err
	}
	logger.Info("starting the node scaling operator", "version", version, "region", f.region)
	if err := ctrl.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
