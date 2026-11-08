// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package join_test

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bacanoo-io/norca-platform/nodescaler/join"
	"github.com/bacanoo-io/norca-platform/nodescaler/tpmkey"
	"github.com/bacanoo-io/norca-platform/nodescaler/tpmkey/tpmtest"
	"github.com/google/go-tpm/tpm2/transport"
)

// With the key store tpm the key is made in the TPM, the key file is a
// TSS2 key file, and each renewal makes a new key in the TPM, signing the
// request and the client certificate of the renewal with the TPM.
func TestJoinAndRenewWithTPM(t *testing.T) {
	open := tpmtest.Start(t)
	e := newEnv(t)
	ctx := context.Background()
	c, path := e.node("workers-1-1")
	c.Config.KeyStore = join.KeyStoreTPM
	c.OpenTPM = open

	if err := c.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	keyPEM, _ := os.ReadFile(c.Config.KeyFile)
	if !strings.HasPrefix(string(keyPEM), "-----BEGIN TSS2 PRIVATE KEY-----") {
		t.Fatalf("the key file is not a TSS2 key file:\n%s", keyPEM)
	}
	leaf := checkNodeCert(t, e, c, "workers-1-1")
	if !e.has("CertificateIssued", "workers-1-1 joined with its own key (key in its TPM)") {
		t.Fatalf("no CertificateIssued event of a key in the TPM: %v", e.events)
	}

	// Renewals, more than the TPM has object slots: no key stays loaded.
	for i := range 4 {
		if err := c.Renew(ctx); err != nil {
			t.Fatalf("renewal %d: %v", i+1, err)
		}
		next := checkNodeCert(t, e, c, "workers-1-1")
		if pub := next.PublicKey.(*ecdsa.PublicKey); pub.Equal(leaf.PublicKey) {
			t.Fatalf("renewal %d kept the key", i+1)
		}
		keyAfter, _ := os.ReadFile(c.Config.KeyFile)
		if !tpmkey.IsPEM(keyAfter) || string(keyAfter) == string(keyPEM) {
			t.Fatalf("renewal %d: the key file is not a new TSS2 key file", i+1)
		}
		leaf, keyPEM = next, keyAfter
	}
	if !e.has("CertificateRenewed", "workers-1-1 renewed its certificate (key in its TPM)") {
		t.Fatalf("no CertificateRenewed event of a key in the TPM: %v", e.events)
	}
}

// A node that turns from a file to the TPM renews with its key file and
// gets a key of the TPM.
func TestRenewFromFileToTPM(t *testing.T) {
	open := tpmtest.Start(t)
	e := newEnv(t)
	ctx := context.Background()
	c, path := e.node("workers-1-1")
	if err := c.Join(ctx, path); err != nil {
		t.Fatal(err)
	}
	c.Config.KeyStore = join.KeyStoreAuto
	c.OpenTPM = open
	c.TPMPresent = func() bool { return true }
	if err := c.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if keyPEM, _ := os.ReadFile(c.Config.KeyFile); !tpmkey.IsPEM(keyPEM) {
		t.Fatalf("auto with a TPM made no TPM key:\n%s", keyPEM)
	}
	checkNodeCert(t, e, c, "workers-1-1")
}

// The key store auto keeps the key in a file on a machine without a TPM.
func TestKeyStoreAutoWithoutTPM(t *testing.T) {
	e := newEnv(t)
	c, path := e.node("workers-1-1")
	c.Config.KeyStore = join.KeyStoreAuto
	c.TPMPresent = func() bool { return false }
	if err := c.Join(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	keyPEM, _ := os.ReadFile(c.Config.KeyFile)
	if !strings.HasPrefix(string(keyPEM), "-----BEGIN EC PRIVATE KEY-----") {
		t.Fatalf("auto without a TPM: the key file is\n%s", keyPEM)
	}
	if e.has("CertificateIssued", "TPM") {
		t.Fatalf("a key in a file was reported in the TPM: %v", e.events)
	}
}

// The key store auto keeps the key in a file when the TPM it found can
// not make it.
func TestKeyStoreAutoWithAnUnusableTPM(t *testing.T) {
	e := newEnv(t)
	c, path := e.node("workers-1-1")
	c.Config.KeyStore = join.KeyStoreAuto
	c.TPMPresent = func() bool { return true }
	c.OpenTPM = func() (transport.TPMCloser, error) { return nil, errors.New("permission denied") }
	if err := c.Join(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	if keyPEM, _ := os.ReadFile(c.Config.KeyFile); tpmkey.IsPEM(keyPEM) {
		t.Fatal("a TPM key without a TPM")
	}
}

// The key store tpm fails without a TPM, before the token is used.
func TestKeyStoreTPMWithoutTPM(t *testing.T) {
	e := newEnv(t)
	c, path := e.node("workers-1-1")
	c.Config.KeyStore = join.KeyStoreTPM
	c.OpenTPM = tpmkey.DeviceOpener(filepath.Join(t.TempDir(), "tpmrm0"))
	if err := c.Join(context.Background(), path); err == nil {
		t.Fatal("a join with the key store tpm and no TPM")
	}
	if conf, err := join.LoadConfig(path); err != nil || conf.Token == "" {
		t.Fatalf("the token was used: %v %+v", err, conf)
	}
	if _, err := os.Stat(c.Config.KeyFile); !os.IsNotExist(err) {
		t.Fatalf("a key file was written: %v", err)
	}
}

// Where TPM keys are not supported (not Linux), tpm is an error and auto a
// file.
func TestKeyStoreUnsupported(t *testing.T) {
	if tpmkey.Supported {
		t.Skip("TPM keys are supported here")
	}
	e := newEnv(t)
	c, path := e.node("workers-1-1")
	c.Config.KeyStore = join.KeyStoreTPM
	if err := c.Join(context.Background(), path); !errors.Is(err, tpmkey.ErrUnsupported) {
		t.Fatalf("tpm on an unsupported system: %v", err)
	}
	c.Config.KeyStore = join.KeyStoreAuto
	if err := c.Join(context.Background(), path); err != nil {
		t.Fatal(err)
	}
}

func TestConfigKeyStore(t *testing.T) {
	conf := join.Config{Instance: "a-1", Region: "eu", URLs: []string{"https://x:1"}, Endpoint: "spiffe://x/y",
		CAFile: "a", CertFile: "b", KeyFile: "c", StateFile: "d"}
	for _, ks := range []string{"", "file", "tpm", "auto"} {
		conf.KeyStore = ks
		if err := conf.Check(); err != nil {
			t.Fatalf("%q: %v", ks, err)
		}
	}
	conf.KeyStore = "hsm"
	if err := conf.Check(); err == nil {
		t.Fatal("the key store hsm was accepted")
	}
}
