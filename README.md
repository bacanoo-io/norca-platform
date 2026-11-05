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
nodescaler/operator/         the operator: reconciles the NodeGroup objects with their providers
cmd/nodeprovider/           a node provider program (type mock)
cmd/norca-nodescaler/       the operator program
```

## The operator

`norca-nodescaler` reconciles every `norca.io/NodeGroup` with a `provider`:
it asks the provider for the instances the scale (`spec.replicas`) is
missing, in a request that is idempotent by its ID (`<region>/<group>/<n>`),
matches the nodes that join with their instances by the meta key
`norca.provider_id`, and writes the status of the group:

| Field | Meaning |
| --- | --- |
| `replicas` | Ready nodes of the group's instances. |
| `joining` | Live instances whose node has not joined yet. |
| `failed` | Instances the provider reports as failed. |
| `capacity`, `allocatable` | The size of a machine from the provider, less `spec.node.reserved`. |
| `nodes` | One entry per instance: `instance`, `state`, `request`, `created`, and `node`, `ready` once it joined. |
| `lastRequest` | The number of the last request. |
| `conditions` | `ProviderAvailable` (`UnknownProvider`, `ProviderUnavailable`, `UnknownGroup`, `InvalidProvider`) and `Provisioned` (`Quota`, `NoCapacity`, `Invalid`, `ProviderError`). |

The status is shared with the autoscaler (`status.decision`): the operator
changes only its own fields and writes with the index it read. This version
adds instances; the join deadline, orphans, scale down and the bootstrap of
a machine come next.

```shell
norca-nodescaler -region eu -provider-ttl 3m -token-namespace default
```

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
