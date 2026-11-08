// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package mock_test

import (
	"testing"

	"github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca-platform/provider/node/providertest"
	"github.com/bacanoo-io/norca-platform/providers/mock"
)

// The mock provider keeps the contract: the checks every provider runs.
func TestContract_Mock(t *testing.T) {
	providertest.Run(t, providertest.Config{
		New: func(*testing.T) node.Provider {
			return mock.New(map[string]mock.GroupConfig{
				"fsn1": {Capacity: node.Capacity{CPU: 4000, Memory: 8192, Disk: 40000}, Max: 3},
				"nbg1": {Capacity: node.Capacity{CPU: 2000, Memory: 4096, Disk: 20000}},
			})
		},
		// The mock keeps its instances in memory: the platform of a restarted
		// mock is the same object.
		Restart:    func(_ *testing.T, p node.Provider) node.Provider { return p },
		Group:      "nbg1",
		Params:     map[string]any{"size": "cx22"},
		Capacity:   &node.Capacity{CPU: 2000, Memory: 4096, Disk: 20000},
		QuotaGroup: "fsn1",
		QuotaMax:   3,
		Addresses:  true,
	})
}
