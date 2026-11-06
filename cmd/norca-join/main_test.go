// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

//go:build linux

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/nodescaler/join"
	"github.com/bacanoo-io/norca/norca/controller/fake"
)

type staticAuthority struct{ a *join.Authority }

func (s staticAuthority) Authority(context.Context) (*join.Authority, error) { return s.a, nil }

// run with an agent command: norca-join joins, starts the agent as its
// child, renews the certificate (3 seconds, at two thirds) and sends the
// agent SIGHUP after each renewal; it ends with the agent and its exit code.
// The agent here is a shell that counts the SIGHUPs and ends after the
// second with the code 7.
func TestRunSupervisesTheAgent(t *testing.T) {
	items, _ := bootstraptest.Items(t)
	tmpl, err := bootstrap.Parse("eu", items)
	if err != nil {
		t.Fatal(err)
	}
	backend := fake.New()
	store := &join.Store{Vars: backend, Namespace: "default", Region: "eu"}
	srv := &join.Server{Store: store, Authority: staticAuthority{tmpl.JoinAuthority()}, Region: "eu",
		Validity: 3 * time.Second}
	hs := httptest.NewUnstartedServer(srv.Handler())
	hs.Listener = tls.NewListener(hs.Listener, srv.TLSConfig())
	hs.Start()
	defer hs.Close()
	url := strings.Replace(hs.URL, "http://", "https://", 1)

	token, err := store.Issue(context.Background(), "workers", "workers-1-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), tmpl.CAPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	conf := join.Config{Instance: "workers-1-1", Region: "eu", URLs: []string{url},
		Endpoint: join.EndpointURI(tmpl.TrustDomain, "eu").String(), Token: token,
		CAFile: filepath.Join(dir, "ca.pem"), CertFile: filepath.Join(dir, "client.pem"),
		KeyFile: filepath.Join(dir, "client-key.pem"), StateFile: filepath.Join(dir, "state.json")}
	confPath := filepath.Join(dir, join.ConfigFileName)
	data, _ := json.Marshal(conf)
	if err := os.WriteFile(confPath, data, 0o600); err != nil {
		t.Fatal(err)
	}

	hups := filepath.Join(dir, "hups")
	agent := `n=0; trap 'n=$((n+1)); echo $n >> ` + hups + `; [ $n -ge 2 ] && exit 7' HUP
test -s ` + conf.CertFile + ` || exit 3
while :; do sleep 0.1; done`
	done := make(chan struct{})
	var code int
	var runErr error
	go func() {
		defer close(done)
		code, runErr = run([]string{"run", "-config", confPath, "--", "sh", "-c", agent})
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("norca-join did not end with the agent")
	}
	if runErr != nil || code != 7 {
		t.Fatalf("norca-join ended with %d, %v: want the code 7 of the agent", code, runErr)
	}
	got, _ := os.ReadFile(hups)
	if strings.TrimSpace(string(got)) != "1\n2" {
		t.Fatalf("the agent got the SIGHUPs %q", got)
	}
	// The token was used and is gone from the configuration.
	if c, err := join.LoadConfig(confPath); err != nil || c.Token != "" {
		t.Fatalf("the configuration after the join: %+v %v", c, err)
	}
}

func TestRunRefusesBadArguments(t *testing.T) {
	if code, err := run(nil); code != 2 || err == nil {
		t.Fatalf("no mode: %d %v", code, err)
	}
	if code, err := run([]string{"fly", "-config", "/nonexistent"}); err == nil {
		t.Fatalf("an unknown mode: %d %v", code, err)
	}
}
