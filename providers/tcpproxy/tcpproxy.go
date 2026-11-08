// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package tcpproxy

import (
	"github.com/bacanoo-io/norca-platform/provider"
)

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "TCP proxy in this process",
		LB: &provider.LBSpec{
			Params: classParams,
			New:    NewLB,
		},
	})
}
