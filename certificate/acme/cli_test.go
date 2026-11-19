// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package acme

import (
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bacanoo-io/norca/norca/api"
)

// The operator's own data (its secrets, the tokens of the DNS zones) is in
// the operations namespace, as the norca-acme pack runs it.
func TestSettings_Defaults(t *testing.T) {
	var s Settings
	fs := flag.NewFlagSet("norca-acme", flag.ContinueOnError)
	s.register(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if s.SecretNamespace != "norca-system" || s.ZoneTokenNamespace != "norca-system" {
		t.Fatalf("namespaces: secrets %q, zone tokens %q", s.SecretNamespace, s.ZoneTokenNamespace)
	}
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
}

// The secrets of the operator in the secret store: a read returns the
// modify index of the secret, and a checked write sends it as the cas
// parameter; the conflict answer of the servers is errSecretConflict.
func TestAPISecrets_CheckedWrite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/secret/norca-acme/accounts/ca" || r.URL.Query().Get("namespace") != "ops" {
			http.Error(w, "unexpected request "+r.URL.String(), http.StatusBadRequest)
			return
		}
		switch {
		case r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`{"Secret":{"ModifyIndex":7},"Version":1,"Items":{"key":"k"}}`))
		case r.Method == http.MethodPut && r.URL.Query().Get("cas") == "7":
			_, _ = w.Write([]byte(`{"Secret":{"ModifyIndex":8}}`))
		case r.Method == http.MethodPut:
			http.Error(w, "secret was modified concurrently (check index mismatch)", http.StatusConflict)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()
	client, err := api.NewClient(&api.Config{Address: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	s := &apiSecrets{client: client, namespace: "ops"}
	ctx := t.Context()

	items, index, err := s.Read(ctx, "norca-acme/accounts/ca")
	if err != nil || index != 7 || items["key"] != "k" {
		t.Fatalf("read: %v, index %d, items %v", err, index, items)
	}
	if err := s.CheckedWrite(ctx, "norca-acme/accounts/ca", 7, map[string]string{"key": "new"}); err != nil {
		t.Fatalf("a write at the read index: %v", err)
	}
	if err := s.CheckedWrite(ctx, "norca-acme/accounts/ca", 0, map[string]string{"key": "new"}); !errors.Is(err, errSecretConflict) {
		t.Fatalf("a write at another index: %v", err)
	}
}
