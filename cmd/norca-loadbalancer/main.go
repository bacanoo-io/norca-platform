// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Command norca-loadbalancer is the LoadBalancer operator of Norca (package
// loadbalancer/operator): it keeps a load balancer for every LoadBalancer
// object, with the built-in "nomad" class or a provider registered with a
// LoadBalancerClass object.
//
//	norca-loadbalancer [controller] [flags]   run the controller (default)
//	norca-loadbalancer proxy -listen :8080 -targets local/targets
//	norca-loadbalancer webhook -tls-cert cert.pem -tls-key key.pem
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bacanoo-io/norca-platform/loadbalancer/operator"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := operator.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "norca-loadbalancer:", err)
		os.Exit(1)
	}
}
