// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package operator

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/bacanoo-io/norca/norca/api"
)

// withNetwork makes the reconciler read the network Variable of the region;
// ranges "" leaves the Variable out.
func (e *env) withNetwork(ranges string) {
	e.r.cfg.Network = &VariableNetwork{Vars: e.backend, Namespace: "default", Path: "nodescaler/network/eu"}
	if ranges != "" {
		e.backend.PutVariable(&api.Variable{Namespace: "default", Path: "nodescaler/network/eu",
			Items: map[string]string{"cluster_cidrs": ranges, "api_cidrs": ""}})
	}
}

func TestNetwork_Covered(t *testing.T) {
	for name, tc := range map[string]struct {
		ranges, network, status, reason string
	}{
		"inside":           {"10.0.0.0/16, 192.168.1.0/24", "10.0.4.0/24", "True", "Covered"},
		"the same":         {"10.0.4.0/24", "10.0.4.0/24", "True", "Covered"},
		"an address":       {"10.0.4.7", "10.0.4.7/32", "True", "Covered"},
		"wider":            {"10.0.4.0/24", "10.0.0.0/16", "False", "NotCovered"},
		"outside":          {"10.0.0.0/16", "172.16.0.0/24", "False", "NotCovered"},
		"bad entries skip": {"nonsense, 10.0.0.0/8", "10.9.0.0/16", "True", "Covered"},
		"no Variable":      {"", "10.0.4.0/24", "Unknown", "NetworkUnchecked"},
		"no network":       {"10.0.0.0/16", "", "Unknown", "NoNetwork"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, "")
			e.withNetwork(tc.ranges)
			extra := map[string]any{}
			if tc.network != "" {
				extra["network"] = tc.network
			}
			e.putGroup(1, extra)
			e.reconcile()
			c := conditionOf(e.status(), CondNetworkCovered)
			if c["status"] != tc.status || c["reason"] != tc.reason {
				t.Fatalf("condition: %+v", c)
			}
			// The instances are asked for anyway: the condition only warns.
			if n := len(e.instances()); n != 1 {
				t.Fatalf("instances: %d", n)
			}
		})
	}
}

func TestNetwork_NotCoveredIsAnEventOnce(t *testing.T) {
	e := newEnv(t, "")
	e.withNetwork("10.0.0.0/16")
	e.putGroup(1, map[string]any{"network": "172.16.0.0/24"})
	e.reconcile()
	e.reconcile()
	if n := e.eventCount("NetworkNotCovered"); n != 1 {
		t.Fatalf("NetworkNotCovered events: %d", n)
	}
	c := conditionOf(e.status(), CondNetworkCovered)
	if !strings.Contains(c["message"].(string), "10.0.0.0/16") {
		t.Fatalf("message: %v", c["message"])
	}
	// Without a source the check is left out.
	e.r.cfg.Network = nil
	e.reconcile()
	if c := conditionOf(e.status(), CondNetworkCovered); c["reason"] != "NetworkUnchecked" {
		t.Fatalf("condition: %+v", c)
	}
}

func TestCovers(t *testing.T) {
	ranges := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/16"), netip.MustParsePrefix("fd00::/64")}
	for network, want := range map[string]bool{
		"10.0.1.0/24": true, "10.0.0.0/16": true, "10.0.0.0/15": false, "10.1.0.0/24": false,
		"fd00::/80": true, "fd01::/80": false,
	} {
		if got := covers(ranges, netip.MustParsePrefix(network)); got != want {
			t.Errorf("%s: %t, want %t", network, got, want)
		}
	}
}
