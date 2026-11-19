// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

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
	"strings"
	"time"

	"github.com/bacanoo-io/norca/norca/api"
	"github.com/bacanoo-io/norca/norca/controller"
)

// systemNamespace is the operations namespace of the Norca operators, the
// default namespace of the operator's own data: its secrets (the account
// keys, the certificates it issued) and the tokens of the DNS zones.
const systemNamespace = "norca-system"

// Settings are the flags of norca-acme.
type Settings struct {
	Controller      string
	Interval        time.Duration
	HTTP01Listen    string
	SecretNamespace string
	SecretPrefix    string

	ZoneTokenNamespace string
	ZoneTTL            time.Duration
	ZoneAllowHTTP      bool
	ZoneCAFile         string

	DNSResolver           string
	DNSPropagationCheck   bool
	DNSPropagationTimeout time.Duration

	ACMECAFile string

	RetryBase   time.Duration
	RetryMax    time.Duration
	MaxAttempts int

	LogLevel string
}

func (s *Settings) register(fs *flag.FlagSet) {
	fs.StringVar(&s.Controller, "controller", DefaultController, "name issuers name this operator by (spec.external.controller)")
	fs.DurationVar(&s.Interval, "interval", 5*time.Second, "time between two passes over the issuers and the orders")
	fs.StringVar(&s.HTTP01Listen, "http01-listen", ":8089", "address the HTTP-01 challenges are answered on (empty: not served)")
	fs.StringVar(&s.SecretNamespace, "secret-namespace", systemNamespace, "namespace of the secrets of the operator")
	fs.StringVar(&s.SecretPrefix, "secret-prefix", "norca-acme/", "path prefix of the secrets of the operator (accounts/, issued/)")
	fs.StringVar(&s.ZoneTokenNamespace, "zone-token-namespace", systemNamespace, "namespace of the Variables holding the tokens of the DNS zones")
	fs.DurationVar(&s.ZoneTTL, "zone-ttl", 3*time.Minute, "how old the heartbeat of a DNS zone may be")
	fs.BoolVar(&s.ZoneAllowHTTP, "zone-allow-http", false, "accept http endpoints of DNS zones (development only)")
	fs.StringVar(&s.ZoneCAFile, "zone-ca-file", "", "PEM file with the CA that signed the https certificates of the DNS providers, in addition to the system roots")
	fs.StringVar(&s.DNSResolver, "dns-resolver", "", "recursive resolver (host:port) the name servers of a zone are found with (default: the first of /etc/resolv.conf)")
	fs.BoolVar(&s.DNSPropagationCheck, "dns-propagation-check", true, "wait until every name server of the zone answers a challenge record before the certificate authority is told to look")
	fs.DurationVar(&s.DNSPropagationTimeout, "dns-propagation-timeout", 5*time.Minute, "how long a challenge record may take to appear at the name servers")
	fs.StringVar(&s.ACMECAFile, "acme-ca-file", "", "PEM file with the CA that signed the https certificate of the ACME directory, in addition to the system roots (a private or test certificate authority)")
	fs.DurationVar(&s.RetryBase, "retry-base", time.Minute, "delay after the first failed attempt of an order; doubled with every further one")
	fs.DurationVar(&s.RetryMax, "retry-max", 2*time.Hour, "longest delay between two attempts of an order")
	fs.IntVar(&s.MaxAttempts, "max-attempts", 8, "attempts after which an order is failed; the servers then place a new one later")
	fs.StringVar(&s.LogLevel, "log-level", "info", "log level: debug, info, warn or error")
}

func (s *Settings) validate() error {
	switch {
	case s.Controller == "":
		return errors.New("-controller must not be empty")
	case s.Interval <= 0:
		return errors.New("-interval must be positive")
	case s.SecretPrefix == "" || !strings.HasSuffix(s.SecretPrefix, "/") || strings.HasPrefix(s.SecretPrefix, "/"):
		return errors.New(`-secret-prefix must be a relative path that ends with "/"`)
	case s.RetryBase <= 0 || s.RetryMax < s.RetryBase:
		return errors.New("-retry-base must be positive and -retry-max not below it")
	case s.MaxAttempts < 1:
		return errors.New("-max-attempts must be at least 1")
	case s.ZoneTTL <= 0 || s.DNSPropagationTimeout <= 0:
		return errors.New("-zone-ttl and -dns-propagation-timeout must be positive")
	}
	return nil
}

// Run runs norca-acme with the arguments until ctx ends. version is the
// version of the binary.
func Run(ctx context.Context, args []string, version string) error {
	var s Settings
	fs := flag.NewFlagSet("norca-acme", flag.ContinueOnError)
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
	op, err := New(s, version, client, logger)
	if err != nil {
		return err
	}

	errc := make(chan error, 1)
	if s.HTTP01Listen != "" {
		srv := &http.Server{
			Addr:              s.HTTP01Listen,
			Handler:           op.responder,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
		}
		go func() {
			logger.Info("answering HTTP-01 challenges", "address", s.HTTP01Listen)
			if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("the HTTP-01 server: %w", err)
			}
		}()
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		}()
	}

	logger.Info("norca-acme started", "controller", s.Controller, "version", version, "interval", s.Interval.String())
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		if err := op.Pass(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("pass failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-errc:
			return err
		case <-ticker.C:
		}
	}
}

// New builds the operator on the Norca API.
func New(s Settings, version string, client *api.Client, logger *slog.Logger) (*Operator, error) {
	acmeHTTP, err := httpClient(s.ACMECAFile, "ACME")
	if err != nil {
		return nil, err
	}
	zoneHTTP, err := httpClient(s.ZoneCAFile, "DNS zone")
	if err != nil {
		return nil, err
	}
	backend := &apiCluster{backend: controller.NewBackend(client)}
	op := &Operator{
		Name:    s.Controller,
		Version: version,
		cluster: backend,
		secrets: &apiSecrets{client: client, namespace: s.SecretNamespace},
		connect: connectACME(acmeHTTP, "norca-acme/"+version),
		zones: &zones{
			cluster:        backend,
			ttl:            s.ZoneTTL,
			tokenNamespace: s.ZoneTokenNamespace,
			allowHTTP:      s.ZoneAllowHTTP,
			client:         zoneHTTP,
			now:            time.Now,
			cacheFor:       3 * time.Second,
		},
		responder:          newResponder(),
		logger:             logger,
		now:                time.Now,
		secretPrefix:       s.SecretPrefix,
		heartbeatEvery:     30 * time.Second,
		retryBase:          s.RetryBase,
		retryMax:           s.RetryMax,
		maxAttempts:        s.MaxAttempts,
		propagationTimeout: s.DNSPropagationTimeout,
		accounts:           map[string]*account{},
		tokens:             map[string][]string{},
		waiting:            map[string]time.Time{},
	}
	if s.DNSPropagationCheck {
		check, err := newNameServers(s.DNSResolver)
		if err != nil {
			// Without a resolver the HTTP-01 challenge still works; an
			// order that needs DNS-01 says what is missing.
			logger.Warn("the name servers of DNS zones cannot be checked", "error", err)
			op.propagation = failingPropagation{err: err}
		} else {
			op.propagation = check
		}
	}
	return op, nil
}

// failingPropagation is the check of an operator without a resolver.
type failingPropagation struct{ err error }

func (f failingPropagation) visible(context.Context, string, string, string) (bool, error) {
	return false, f.err
}

// httpClient returns an HTTP client that trusts the certificates of the
// PEM file in addition to the roots of the system.
func httpClient(caFile, what string) (*http.Client, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	if caFile == "" {
		return client, nil
	}
	raw, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("%s CA file: %w", what, err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(raw) {
		return nil, fmt.Errorf("%s CA file %s: no certificate found", what, caFile)
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

// apiCluster is the cluster on the Norca API.
type apiCluster struct {
	backend controller.Backend
}

func (c *apiCluster) ListObjects(ctx context.Context, kind kindRef) ([]*api.CustomObject, error) {
	objs, _, err := c.backend.ListObjects(ctx, kind, "*", 0)
	return objs, err
}

func (c *apiCluster) UpdateStatus(ctx context.Context, kind kindRef, name string, status map[string]any, modifyIndex uint64) error {
	_, err := c.backend.UpdateObjectStatus(ctx, kind, "", name, status, modifyIndex)
	switch {
	case errors.Is(err, controller.ErrConflict):
		return fmt.Errorf("%w: %w", errConflict, err)
	case errors.Is(err, controller.ErrNotFound):
		return fmt.Errorf("%w: %w", errGone, err)
	}
	return err
}

func (c *apiCluster) ReadVariable(ctx context.Context, namespace, path string) (*api.Variable, error) {
	return c.backend.ReadVariable(ctx, namespace, path)
}

// apiSecrets are the secrets of the operator in the secret store of Norca.
type apiSecrets struct {
	client    *api.Client
	namespace string
}

func (s *apiSecrets) Read(ctx context.Context, path string) (map[string]string, uint64, error) {
	q := (&api.QueryOptions{Namespace: s.namespace}).WithContext(ctx)
	value, _, err := s.client.Secrets().Read(path, 0, q)
	if errors.Is(err, api.ErrSecretNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var index uint64
	if value.Secret != nil {
		index = value.Secret.ModifyIndex
	}
	return value.Items, index, nil
}

func (s *apiSecrets) Write(ctx context.Context, path string, items map[string]string, expiresAt *time.Time) error {
	w := (&api.WriteOptions{Namespace: s.namespace}).WithContext(ctx)
	write := &api.SecretWrite{Items: items}
	if expiresAt != nil {
		write.Meta = &api.SecretMeta{ExpiresAt: expiresAt}
	}
	_, _, err := s.client.Secrets().Write(path, write, w)
	return err
}

func (s *apiSecrets) CheckedWrite(ctx context.Context, path string, checkIndex uint64, items map[string]string) error {
	w := (&api.WriteOptions{Namespace: s.namespace}).WithContext(ctx)
	_, _, err := s.client.Secrets().CheckedWrite(path, checkIndex, &api.SecretWrite{Items: items}, w)
	var respErr api.UnexpectedResponseError
	if errors.As(err, &respErr) && respErr.StatusCode() == http.StatusConflict {
		return fmt.Errorf("%w: %w", errSecretConflict, err)
	}
	return err
}
