// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package digitalocean

import (
	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// sharedParams are the parameters of the platform its node and load balancer
// providers share: the credentials and the API.
var sharedParams = []params.Param{
	{Name: "token", Type: params.ParamString, Required: true, Secret: true,
		Description: "DigitalOcean API personal access token (supports env:NAME or file:/path)"},
	{Name: "endpoint", Type: params.ParamString, Default: defaultEndpoint,
		Description: "DigitalOcean API v2 base URL"},
}

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "DigitalOcean",
		Params:      sharedParams,
		LB: &provider.LBSpec{
			Params: classParams,
			New:    NewLB,
		},
	})
}
