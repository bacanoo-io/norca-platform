// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package scaleway

import (
	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// sharedParams are the parameters of the platform its node and load balancer
// providers share: the credentials and the API.
var sharedParams = []params.Param{
	{Name: "secret_key", Type: params.ParamString, Required: true, Secret: true,
		Description: "Scaleway API secret key (supports env:NAME or file:/path)"},
	{Name: "project_id", Type: params.ParamString, Required: true,
		Description: "Scaleway project UUID in which load balancers are created"},
	{Name: "endpoint", Type: params.ParamString, Default: defaultEndpoint,
		Description: "Scaleway API base URL"},
}

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "Scaleway",
		Params:      sharedParams,
		LB: &provider.LBSpec{
			Params: classParams,
			New:    NewLB,
		},
	})
}
