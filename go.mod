module github.com/bacanoo-io/norca-platform

go 1.27.3

// The Norca API client and the controller SDK of the checkout next to this
// repository, as in norca-loadbalancer.
replace (
	github.com/bacanoo-io/norca/norca/api => ../norca/norca/api
	github.com/bacanoo-io/norca/norca/controller => ../norca/norca/controller
)

require (
	github.com/bacanoo-io/norca-pki v0.1.1-0.20261105182009-9426845506a7
	github.com/bacanoo-io/norca/norca/api v0.0.0-20230103221135-ce00d683f9be
	github.com/bacanoo-io/norca/norca/controller v0.0.0-00010101000000-000000000000
)

require (
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/cronexpr v1.1.2 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	golang.org/x/exp v0.0.0-20230728194245-b0cb94b80691 // indirect
)
