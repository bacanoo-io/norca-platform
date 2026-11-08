// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package aws

import (
	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// sharedParams are the parameters of the platform its node and load balancer
// providers share: the credentials and the API.
var sharedParams = []params.Param{
	{Name: "region", Type: params.ParamString, Required: true,
		Description: "AWS region (e.g. eu-central-1, us-east-1)"},
	{Name: "access_key_id", Type: params.ParamString, Required: true, Secret: true,
		Description: "AWS access key ID (supports env:NAME or file:/path)"},
	{Name: "secret_access_key", Type: params.ParamString, Required: true, Secret: true,
		Description: "AWS secret access key (supports env:NAME or file:/path)"},
	{Name: "session_token", Type: params.ParamString, Default: "", Secret: true,
		Description: "optional AWS temporary session token (supports env:NAME or file:/path)"},
	{Name: "endpoint", Type: params.ParamString, Default: "",
		Description: "custom ELBv2 endpoint URL; defaults to https://elasticloadbalancing.<region>.amazonaws.com"},
}

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "Amazon Web Services",
		Params:      sharedParams,
		LB: &provider.LBSpec{
			Params: classParams,
			New:    NewLB,
		},
	})
}
