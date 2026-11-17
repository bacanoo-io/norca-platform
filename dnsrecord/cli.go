// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package dnsrecord

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// Settings are the flags of norca-dns.
type Settings struct {
	Namespace      string
	Workers        int
	Resync         time.Duration
	Requeue        time.Duration
	LeaderElection bool
	LeaseNamespace string
	MetricsAddr    string

	ZoneTokenNamespace string
	ZoneTTL            time.Duration
	ZoneAllowHTTP      bool
	ZoneCAFile         string

	LogLevel string
}

func (s *Settings) register(fs *flag.FlagSet) {
	fs.StringVar(&s.Namespace, "namespace", "*", `namespace of the DNSRecord objects ("*": every namespace)`)
	fs.IntVar(&s.Workers, "workers", 2, "records reconciled at once")
	fs.DurationVar(&s.Resync, "resync", 10*time.Minute, "time between two full looks at every record")
	fs.DurationVar(&s.Requeue, "requeue", 5*time.Minute, "time after which a record is compared with its zone again (finds changes made in the zone by hand)")
	fs.BoolVar(&s.LeaderElection, "leader-election", false, "run several instances, one of them active")
	fs.StringVar(&s.LeaseNamespace, "lease-namespace", "default", "namespace of the leader election lease")
	fs.StringVar(&s.MetricsAddr, "metrics-addr", "", "address of the Prometheus metrics endpoint (empty: off)")
	fs.StringVar(&s.ZoneTokenNamespace, "zone-token-namespace", "default", "namespace of the Variables holding the tokens of the DNS zones")
	fs.DurationVar(&s.ZoneTTL, "zone-ttl", 3*time.Minute, "how old the heartbeat of a DNS zone may be")
	fs.BoolVar(&s.ZoneAllowHTTP, "zone-allow-http", false, "accept http endpoints of DNS zones (development only)")
	fs.StringVar(&s.ZoneCAFile, "zone-ca-file", "", "PEM file with the CA that signed the https certificates of the DNS providers, in addition to the system roots")
	fs.StringVar(&s.LogLevel, "log-level", "info", "log level: debug, info, warn or error")
}

func (s *Settings) validate() error {
	switch {
	case s.Namespace == "":
		return errors.New("-namespace must not be empty")
	case s.Workers < 1:
		return errors.New("-workers must be at least 1")
	case s.Resync <= 0 || s.Requeue <= 0 || s.ZoneTTL <= 0:
		return errors.New("-resync, -requeue and -zone-ttl must be positive")
	}
	return nil
}

// Run runs norca-dns with the arguments until ctx ends. version is the
// version of the binary.
func Run(ctx context.Context, args []string, version string) error {
	var s Settings
	fs := flag.NewFlagSet("norca-dns", flag.ContinueOnError)
	s.register(fs)
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showVersion {
		fmt.Println(version)
		return nil
	}
	if err := s.validate(); err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(s.LogLevel)); err != nil {
		return fmt.Errorf("log level: %w", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	client, err := controller.NewAPIClient()
	if err != nil {
		return fmt.Errorf("Norca API: %w", err)
	}
	hc, err := httpClient(s.ZoneCAFile)
	if err != nil {
		return err
	}
	backend := controller.NewBackend(client)
	metrics := controller.NewTextMetrics()
	if s.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics)
		srv := &http.Server{Addr: s.MetricsAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
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

	r := &reconciler{
		cluster: &apiCluster{client: client},
		zones: &zones{
			backend:        backend,
			ttl:            s.ZoneTTL,
			tokenNamespace: s.ZoneTokenNamespace,
			allowHTTP:      s.ZoneAllowHTTP,
			client:         hc,
			now:            time.Now,
			cacheFor:       15 * time.Second,
		},
		logger:         logger,
		now:            time.Now,
		namespace:      s.Namespace,
		requeueReady:   s.Requeue,
		requeueWaiting: time.Minute,
	}
	opts := controller.Options{
		Name:         "norca-dns",
		Kind:         Kind,
		Storage:      controller.StorageNative,
		Namespace:    s.Namespace,
		Workers:      s.Workers,
		ResyncPeriod: s.Resync,
		Sources:      []controller.Source{loadBalancerSource(), zoneSource(r.zones)},
		Metrics:      metrics,
		Logger:       logger,
	}
	if s.LeaderElection {
		opts.LeaderElection = &controller.LeaderElectionConfig{Namespace: s.LeaseNamespace, ReleaseOnCancel: true}
	}
	c, err := controller.New(backend, r, opts)
	if err != nil {
		return err
	}
	r.ctrl = c
	logger.Info("norca-dns started", "version", version, "namespace", s.Namespace)
	return c.Run(ctx)
}

// apiCluster reads the namespaces through the Norca API.
type apiCluster struct {
	client *api.Client
}

func (c *apiCluster) NamespaceMeta(ctx context.Context, namespace string) (map[string]string, error) {
	ns, _, err := c.client.Namespaces().Info(namespace, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		var re api.UnexpectedResponseError
		if errors.As(err, &re) && re.StatusCode() == http.StatusNotFound {
			return nil, fmt.Errorf("namespace %s: %w", namespace, controller.ErrNotFound)
		}
		return nil, err
	}
	return ns.Meta, nil
}

// httpClient returns the client of the calls to the DNS providers: it trusts
// the certificates of the PEM file in addition to the roots of the system.
func httpClient(caFile string) (*http.Client, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	if caFile == "" {
		return client, nil
	}
	raw, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("zone CA file: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("zone CA file %s: no certificate found", caFile)
	}
	client.Transport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	return client, nil
}
