// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package docker

import (
	"testing"

	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap"
	"github.com/bacanoo-io/norca-platform/nodescaler/bootstrap/bootstraptest"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider"
	"github.com/bacanoo-io/norca-platform/nodescaler/provider/providertest"
)

// The docker provider keeps the contract. Its platform is the fake docker
// command line: a restarted provider is a new Provider on the same docker
// and the same state directory, as after a restart of the application.
func TestContract_Docker(t *testing.T) {
	providertest.Run(t, providertest.Config{
		New: func(t *testing.T) provider.Provider {
			p, _, _ := newTestProvider(t)
			return p
		},
		Restart: func(t *testing.T, p provider.Provider) provider.Provider {
			old := p.(*Provider)
			again, err := New(old.cfg)
			if err != nil {
				t.Fatal(err)
			}
			return again
		},
		Group:      "dc1",
		Capacity:   &provider.Capacity{CPU: 2000, Memory: 2048, Disk: 10000},
		QuotaGroup: "dc1",
		QuotaMax:   3,
		Addresses:  true,
		// The docker provider writes the files of the bootstrap: it needs a
		// real one.
		Bootstrap: func(t *testing.T, name string) *provider.Bootstrap {
			items, _ := bootstraptest.Items(t)
			tmpl, err := bootstrap.Parse("eu", items)
			if err != nil {
				t.Fatal(err)
			}
			r, err := tmpl.Render(bootstrap.Node{Name: name, Datacenter: "dc1", Pool: "workers"})
			if err != nil {
				t.Fatal(err)
			}
			return &provider.Bootstrap{UserData: r.UserData}
		},
	})
}
