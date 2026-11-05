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
nodescaler/provider/docker/ a node provider whose machines are containers on its host, for the lab
nodescaler/registrar/       the NodeProvider object of a provider and its heartbeat
nodescaler/operator/        the operator: reconciles the NodeGroup objects with their providers
nodescaler/bootstrap/       the bootstrap of a machine: the node's certificate and agent configuration, as cloud-init
cmd/nodeprovider/           a node provider program (types mock and docker)
cmd/norca-nodescaler/       the operator program
```

## The operator

`norca-nodescaler` reconciles every `norca.io/NodeGroup` with a `provider`:
it asks the provider for the instances the scale (`spec.replicas`) is
missing, in a request that is idempotent by its ID (`<region>/<group>/<n>`).
Each instance has a name of the operator (`<group>-<n>-<i>`) and a
bootstrap of its own; the node it runs registers with that name in the meta
key `norca.instance`, which is how nodes are matched with their instances.
The operator writes the status of the group:

| Field | Meaning |
| --- | --- |
| `replicas` | Ready nodes of the group's instances. |
| `joining` | Live instances whose node has not joined yet, without the slow ones (they no longer count as capacity). |
| `failed` | Instances the provider reports as failed. |
| `capacity`, `allocatable` | The size of a machine from the provider, less `spec.node.reserved`. |
| `nodes` | One entry per instance: `instance`, `state`, `request`, `created`, `node` and `ready` once it joined, `slow` while it joins after `join.slow_after`. |
| `lastRequest` | The number of the last request. |
| `join` | The join failures in a row (`failures`, `lastFailure`) and the template (`revision`) and `resumed` generation they count for. |
| `conditions` | `ProviderAvailable` (`UnknownProvider`, `ProviderUnavailable`, `UnknownGroup`, `InvalidProvider`), `Provisioned` (`Quota`, `NoCapacity`, `Invalid`, `ProviderError`, `JoinFailing`) and `JoinFailing` (`TooManyFailures`). |

**The join of the nodes.** An instance whose node has not joined by
`join.deadline` (15 minutes) is deleted, and so is one the provider reports
as failed: both are join failures (events `JoinTimeout`, `InstanceFailed`).
After `join.max_failures` (3) in a row the group stops asking for instances
(`JoinFailing`) until the template (`spec.node`, the provider, its group)
changes or `join.resume_generation` is raised; a node that joins
(`NodeJoined`) ends the row. A node still joining after `join.slow_after`
(5 minutes) no longer counts in `joining` (`JoinSlow`), so the autoscaler may
ask for another one.

**The bootstrap of a machine.** The deployer writes the static parts of a
region into the Variable `nodescaler/bootstrap/<region>` (namespace
`default`; it holds the key of the node CA, so only the operator may read
it):

| Item | Meaning |
| --- | --- |
| `ca` | The cluster CA (PEM), the `ca_file` of the agents. |
| `node_ca`, `node_ca_key` | The node CA of the region and its key (`norca-deployer pki issue-node-ca`). |
| `agent_config` | The agent configuration, a Go template with `.Name`, `.Region`, `.Datacenter`, `.Pool`, `.Class`, `.Meta`, `.CAFile`, `.CertFile`, `.KeyFile` and the functions `hcl` (a quoted string) and `hclmap` (a map as an HCL object). |
| `trust_domain` | The SPIFFE trust domain; the node gets `spiffe://<domain>/region/<region>/node/<name>`. |
| `validity` | The validity of a node certificate (`60d`). |
| `tls_dir`, `config_file`, `service` | Where the files go and the unit of the agent (`/etc/norca/tls`, `/etc/norca/norca.hcl`, `norca`). |

For each instance the operator issues a certificate with the node CA (the
file holds the leaf and the node CA), renders the agent configuration with
the node's name, datacenter, pool, class and meta (`spec.node.meta` and the
marks `norca.created_by`, `norca.node_group`, `norca.instance`,
`norca.request`, `norca.created_at`), and sends a cloud-init document that
writes the files and starts the agent. Every certificate is an event
(`CertificateIssued`: serial and expiry). Without the Variable no instance is
asked for (`Provisioned` False, `NoBootstrap`); `-bootstrap=false` sends only
the names, for a provider that brings its own machines.

**Orphans.** Every `-orphan-sweep` (5 minutes) the leader looks at the groups
of the available providers: an instance of this region whose node group is
gone, or no longer names that provider and group, is deleted after
`-orphan-after` (15 minutes) if it has no node; one with a node is left alone
and logged.

The status is shared with the autoscaler (`status.decision`): the operator
changes only its own fields and writes with the index it read. Scale down
comes next.

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

with a bearer token. A create request lists the instances, each with its
name and its bootstrap (`user_data`); the provider gives the machine that
name. A request that cannot be served fails with a reason:
`quota`, `no_capacity`, `invalid` or `error`. See the package documentation
of `nodescaler/provider`.

```shell
nodeprovider -name mock -listen :8480 -endpoint http://10.0.0.5:8480 \
  -group dc1:4000:8192:40000:10 -token-file /secrets/token
```

**The docker provider** (`-type docker`) runs each instance as a container on
the host it runs on: it reads the files back from the cloud-init document of
the instance, writes them under `-docker-state-dir` and mounts them read-only
at their paths, mounts the norca binary of the host (`-docker-binary`), and
runs `norca agent -config <the .hcl file>` in `-docker-image` (a privileged
container with the host's cgroups, named after the instance). The containers
carry the labels of the request, so a restart of the provider loses nothing.
It is for the lab and for trials, not for production.

```shell
nodeprovider -name lab -type docker -listen :8480 -endpoint http://10.0.0.5:8480   -group dc2:2000:1024:10000:3 -docker-state-dir /var/lib/norca-nodeprovider
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
