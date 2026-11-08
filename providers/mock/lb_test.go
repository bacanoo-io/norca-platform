// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package mock_test

import (
	"testing"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/lb"
	"github.com/bacanoo-io/norca-platform/provider/lb/lbtest"
	"github.com/bacanoo-io/norca-platform/providers/mock"
)

// The load balancer provider of the mock keeps the contract.
func TestContract_MockLB(t *testing.T) {
	class, err := provider.Default.NewClass("mock", mock.TypeID, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	lbtest.Run(t, lbtest.Config{
		New: func(t *testing.T) lb.Provider {
			c, err := provider.Default.NewClass("mock", mock.TypeID, nil, map[string]any{"address": "203.0.113.7"})
			if err != nil {
				t.Fatal(err)
			}
			return c.Provider
		},
		Params: class.Values,
	})
}
