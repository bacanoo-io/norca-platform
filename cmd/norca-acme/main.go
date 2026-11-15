// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Command norca-acme is the controller of the certificate issuers of Norca
// that sign through an ACME certificate authority (package certificate/acme):
// it keeps their accounts, fulfils the CertificateOrder objects the servers
// write for them, and answers the HTTP-01 challenges.
//
//	norca-acme [flags]
//
// The Norca API is read from the environment (NORCA_ADDR, NORCA_TOKEN); as a
// job, NORCA_ADDR=unix://${NORCA_SECRETS_DIR}/api.sock with the workload
// identity.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bacanoo-io/norca-platform/certificate/acme"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := acme.Run(ctx, os.Args[1:], version); err != nil {
		fmt.Fprintln(os.Stderr, "norca-acme:", err)
		os.Exit(1)
	}
}
