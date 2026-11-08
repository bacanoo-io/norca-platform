// SPDX-FileCopyrightText: 2026 Norca contributors
// SPDX-License-Identifier: MPL-2.0

package gcp

import (
	"github.com/bacanoo-io/norca-platform/provider"
	"github.com/bacanoo-io/norca-platform/provider/params"
)

// sharedParams are the parameters of the platform its node and load balancer
// providers share: the credentials and the API.
var sharedParams = []params.Param{
	{Name: "project", Type: params.ParamString, Required: true,
		Description: "Google Cloud project ID"},
	{Name: "region", Type: params.ParamString, Required: true,
		Description: "Google Cloud region (e.g. europe-west1, us-central1)"},
	{Name: "token", Type: params.ParamString, Required: true, Secret: true,
		Description: "OAuth2 Bearer access token for Google Compute Engine API (supports env:NAME or file:/path)"},
	{Name: "endpoint", Type: params.ParamString, Default: defaultEndpoint,
		Description: "Google Compute Engine API base URL"},
}

func init() {
	provider.Register(provider.Type{
		ID:          TypeID,
		Description: "Google Cloud",
		Params:      sharedParams,
		LB: &provider.LBSpec{
			Params: classParams,
			New:    NewLB,
		},
	})
}
