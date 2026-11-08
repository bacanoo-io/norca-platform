// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package docker

import (
	"fmt"
	"net/netip"
	"net/url"
	"strings"

	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/node"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// TypeID is the ID of the type.
const TypeID = "docker"

// nodeParams are the parameters of the node section.
var nodeParams = []params.Param{
	{Name: "image", Type: params.ParamString, Default: "debian:bookworm-slim",
		Description: "the image of the containers; it needs what the agent needs (glibc)"},
	{Name: "binary", Type: params.ParamString, Default: "/usr/local/bin/norca",
		Description: "the norca binary of the host, mounted into the containers"},
	{Name: "join_binary", Type: params.ParamString, Default: "/usr/local/bin/norca-join",
		Description: "norca-join of the host, mounted into the containers of nodes that join with a key of their own"},
	{Name: "state_dir", Type: params.ParamString, Default: "/var/lib/norca-provider",
		Description: "where the files of the nodes are written, one directory per instance"},
	{Name: "network", Type: params.ParamString, Default: "bridge",
		Description: "the docker network of the containers"},
	{Name: "host_addresses", Type: params.ParamList,
		Description: "the addresses the traffic of the containers to other machines comes from, after the masquerade of the docker network, " +
			"reported as addresses of every instance (the join endpoint binds the nodes to them); " +
			"default: the host of the endpoint of the provider if it is an IP address that is not a loopback one"},
}

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "containers on the host of the provider, each running a Norca agent; for the lab and for trials",
		Node: &provider.NodeSpec{
			Params:      nodeParams,
			GroupParams: provider.CapacityParams,
			New:         newNode,
		},
	})
}

func newNode(cfg provider.NodeConfig) (node.Provider, error) {
	groups := map[string]GroupConfig{}
	for _, g := range cfg.Groups {
		c, err := provider.GroupCapacity(g)
		if err != nil {
			return nil, err
		}
		groups[g.Name] = GroupConfig{Capacity: c, Max: g.Max}
	}
	hostAddrs, err := HostAddresses(cfg.Values.List("host_addresses"), cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	if len(hostAddrs) == 0 {
		cfg.Log().Warn("no address of the host is known (host_addresses): the instances report only the addresses of their containers, " +
			"and a node's traffic to another machine comes from an address the join endpoint does not know")
	}
	v := cfg.Values
	return New(Config{Groups: groups, Image: v.String("image"), Binary: v.String("binary"),
		JoinBinary: v.String("join_binary"), StateDir: v.String("state_dir"), Network: v.String("network"),
		HostAddresses: hostAddrs})
}

// HostAddresses reads the host_addresses parameter: IP addresses. Without
// any, it is the host of the endpoint if that is an IP address and not a
// loopback or unspecified one (the provider then serves on the address of
// the host the containers' traffic leaves from), else none.
func HostAddresses(configured []string, endpoint string) ([]string, error) {
	var out []string
	for _, a := range configured {
		if a = strings.TrimSpace(a); a == "" {
			continue
		}
		ip, err := netip.ParseAddr(a)
		if err != nil {
			return nil, fmt.Errorf("host_addresses: %q is not an IP address", a)
		}
		out = append(out, ip.Unmap().String())
	}
	if len(out) > 0 {
		return out, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, nil
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !ip.IsLoopback() && !ip.IsUnspecified() {
		return []string{ip.Unmap().String()}, nil
	}
	return nil, nil
}
