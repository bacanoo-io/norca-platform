<!--
SPDX-FileCopyrightText: 2026 Norca contributors
SPDX-License-Identifier: MPL-2.0
-->

# norca-platform

The platform operators of Norca and the providers they call, in one
repository. An operator is an application outside the core that turns
objects of the operator framework into things of a platform; a provider is
the application that knows one platform (a cloud, the VMs of a lab) and
keeps its credentials.

- **Node scaling** (`nodescaler/`): the core decides how many nodes a
  `NodeGroup` should have (`spec.replicas`); the operator `norca-nodescaler`
  creates and deletes the machines through a **node provider**.
- **Load balancers** will move here from `norca-loadbalancer`. Nothing here
  assumes it is the only operator of the repository.

## Layout

```
nodescaler/provider/        the contract between the node scaling operator and a node provider:
                            the types, the HTTP server a provider uses, the client the operator uses
nodescaler/provider/mock/   a node provider in memory, for trials and tests
nodescaler/registrar/       the NodeProvider object of a provider and its heartbeat
cmd/nodeprovider/           a node provider program (type mock)
```

The operator itself (`nodescaler/operator/`, `cmd/norca-nodescaler/`) comes
next.

## The node provider contract

A provider registers itself with a `norca.io/NodeProvider` object (its
endpoint, the Variable with its token, the groups it offers) and keeps a
heartbeat in the status. The operator calls it:

```
GET    {endpoint}/v1/groups/{group}                 the instances and the capacity of one node
POST   {endpoint}/v1/groups/{group}/instances       create instances (idempotent by request_id)
DELETE {endpoint}/v1/groups/{group}/instances/{id}  delete one instance
```

with a bearer token. A request that cannot be served fails with a reason:
`quota`, `no_capacity`, `invalid` or `error`. See the package documentation
of `nodescaler/provider`.

```shell
nodeprovider -name mock -listen :8480 -endpoint http://10.0.0.5:8480 \
  -group dc1:4000:8192:40000:10 -token-file /secrets/token
```

## Development

The Norca API client comes from the checkout next to this repository
(`replace` in `go.mod`), as in `norca-loadbalancer`: keep `norca` and
`norca-platform` side by side.

```shell
make test      # the tests
make lint      # golangci-lint v2 with .golangci.yml
make build     # bin/nodeprovider
```

Commits are signed off (`git commit -s`).
