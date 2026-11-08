// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

// Package all links every platform into the binary. A platform registers
// its type in init(); to add one, give it a directory under providers/ and a
// blank import here. A build with fewer platforms imports only the ones it
// wants.
package all

import (
	_ "github.com/bacanoo-io/norca-platform/providers/aws"
	_ "github.com/bacanoo-io/norca-platform/providers/digitalocean"
	_ "github.com/bacanoo-io/norca-platform/providers/docker"
	_ "github.com/bacanoo-io/norca-platform/providers/gcp"
	_ "github.com/bacanoo-io/norca-platform/providers/hcloud"
	_ "github.com/bacanoo-io/norca-platform/providers/mock"
	_ "github.com/bacanoo-io/norca-platform/providers/nginx"
	_ "github.com/bacanoo-io/norca-platform/providers/scaleway"
	_ "github.com/bacanoo-io/norca-platform/providers/tcpproxy"
	_ "github.com/bacanoo-io/norca-platform/providers/vultr"
)
