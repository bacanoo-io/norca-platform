// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Command norca-dns is the controller of the DNSRecord objects of Norca
// (package dnsrecord): it keeps their record sets in the DNS zones that
// norca-provider serves with records = true.
//
//	norca-dns [flags]
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

	"github.com/bacanoo-io/norca-platform/dnsrecord"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := dnsrecord.Run(ctx, os.Args[1:], version); err != nil {
		fmt.Fprintln(os.Stderr, "norca-dns:", err)
		os.Exit(1)
	}
}
