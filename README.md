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
nodescaler/join/            the hardened node identity: the join endpoint of the operator and norca-join on the node
cmd/nodeprovider/           a node provider program (types mock and docker)
cmd/norca-nodescaler/       the operator program
cmd/norca-join/             the program on a node that makes its key, joins with a one-time token and renews
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
| `conditions` | `ProviderAvailable` (`UnknownProvider`, `ProviderUnavailable`, `UnknownGroup`, `InvalidProvider`), `Provisioned` (`Quota`, `NoCapacity`, `Invalid`, `ProviderError`, `JoinFailing`), `JoinFailing` (`TooManyFailures`), `ForeignNode` and `NetworkCovered` (`Covered`, `NotCovered`, `NetworkUnchecked`, `NoNetwork`). |
| `strayNodes` | Nodes of the group without an instance: `node`, `name`, `instance`, `kind` (`gone`: down, what is left of a deleted instance; `foreign`: ready). |

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
| `join_binary` | Where `norca-join` is on the machines, for the join below (`/usr/local/bin/norca-join`). |
| `install_script` | Optional: a bash script that installs the `norca` binary, the CNI plugins, `norca-join`, the directories and the unit of the agent (`norca-deployer node-scaling install-script`, written with `node_scaling.machine.install: boot`). The bootstrap writes it to `/usr/local/sbin/norca-node-install.sh` (0700) and runs it before it starts the agent or `norca-join`. Without it the image of the machine has all of that. The docker provider ignores it: it mounts the binaries itself. |

For each instance the operator issues a certificate with the node CA (the
file holds the leaf and the node CA), renders the agent configuration with
the node's name, datacenter, pool, class and meta (`spec.node.meta` and the
marks `norca.created_by`, `norca.node_group`, `norca.instance`,
`norca.request`, `norca.created_at`), and sends a cloud-init document that
writes the files and starts the agent. Every certificate is an event
(`CertificateIssued`: serial and expiry). Without the Variable no instance is
asked for (`Provisioned` False, `NoBootstrap`); `-bootstrap=false` sends only
the names, for a provider that brings its own machines.

**The join: the key stays on the node.** With `-join-listen` the bootstrap
carries no key and no certificate: a **one-time token**, the join endpoints,
the cluster CA, the agent configuration and the unit of `norca-join`.
`norca-join` makes the key on the machine, sends a certificate request with
the token to the **join endpoint** of the operator (every instance of the
operator serves it), and gets a certificate of `-join-validity` (7 days);
it starts the agent, renews the certificate at two thirds of its life with
the current one as its client certificate, and sends the agent SIGHUP: the
agent loads the new pair and keeps its connections. The node carries the
meta `norca.identity = "join"` and no `norca.cert_expires`, so it is not
replaced for its age (an explicit `rolling.max_node_age` still applies).

- The endpoint proves itself with a certificate of the node CA whose only
  name is the URI `spiffe://<trust domain>/region/<region>/nodescaler/join`
  (`spiffe://no-spiffe.invalid/...` without SPIFFE): no node certificate
  carries it, so a node, which holds a node certificate, cannot pose as the
  endpoint. `norca-join` checks the chain against the cluster CA and that
  URI before it sends the token.
- A node certificate names its instance (`<instance>.client.<region>.norca`).
  The record of each instance is a Variable `nodescaler/join/<region>/<instance>`
  (in the namespace of the bootstrap): first the hash of its token and when
  it expires (the join deadline of the group and five minutes), after the
  join the serial of its latest certificate. The writes are check-and-set,
  so a token is used once, and **only the latest certificate of an instance
  renews it**: an older copy (a stolen disk) is refused (`RenewalRefused`,
  a warning in the log of the group). The records of instances that are gone
  and of tokens that expired unused are deleted at the next reconcile, and
  all records of a group when it is deleted with its instances (`drain`,
  `terminate`); orphaned instances keep theirs, so their nodes go on
  renewing.
- A renewal rotates the key: the endpoint refuses a request for the key of
  the current certificate (`RenewalRefused`), so a key that leaked does not
  stay valid through the renewals.
- **The join and the renewals are bound to the addresses of the instance.**
  The operator writes the addresses the provider reports for an instance
  into its record (`addresses`, and `seen` once it recorded what the
  provider reports), from the answer of the create and from later lists,
  only on a change; addresses once known are never cleared, a new non-empty
  list replaces them. The endpoint accepts a join or a renewal only from one
  of them (the peer address of the connection; no header is trusted); from
  another it answers 403 and records `JoinAddressRefused`, a warning in the
  log of the group: a stolen token or key. The token is issued before the
  instance exists, so a first join waits (503, `norca-join` tries again)
  until the operator has recorded the instance: its addresses, or that it
  runs without any. A provider that reports no addresses has no binding.
  Where a proxy, a load balancer or NAT in front of the endpoint changes the
  source address, turn it off with `-join-check-address=false`.
- The endpoints are `-join-url` (repeatable: a stable name or a load
  balancer) or, without it, the instances of the service `-join-service`
  (`nodescaler-join`) in the native catalog. Each answer gives the node the
  endpoints of the moment for its next renewal; a node whose endpoints all
  moved away between two renewals cannot renew, so give a stable
  `-join-url` where the operator moves often.
- Without an endpoint no instance is asked for (`Provisioned` False,
  `NoBootstrap`). Every join and renewal is an event of the group
  (`CertificateIssued`, `CertificateRenewed`).

```shell
norca-nodescaler -region eu -join-listen :8490 -join-validity 168h
norca-join run -config /etc/norca/norca-join.json                       # the unit of the bootstrap
norca-join run -config /etc/norca/norca-join.json -- norca agent -config /etc/norca/norca.hcl   # in a container
```

**Nodes without an instance.** A node with the marks of the group whose
instance the provider does not have is reported: down, it is what is left of
a deleted instance (`NodeWithoutInstance`); ready, it runs without a machine
the operator asked for, with a bootstrap that left through the user data for
example, and is foreign (`ForeignNode`): the operator makes it ineligible
for new work. So is a second ready node with the name of an instance that
has one (the first that registered is the node of the instance; a machine
that came up again leaves its old node down and is not foreign). Nodes
without the marks are left alone. This detects; it does not prevent.

**The network of a group.** The firewall of the servers must let the nodes
in: the range of a group (`spec.network`) must be inside the cluster ranges
of the region, which the deployer publishes in the Variable
`nodescaler/network/<region>` (`-network-variable`). The condition
`NetworkCovered` says whether it is (an event `NetworkNotCovered` when it
turns false); without the Variable it is `NetworkUnchecked`. It only warns:
the instances are asked for anyway.

**Orphans.** Every `-orphan-sweep` (5 minutes) the leader looks at the groups
of the available providers: an instance of this region whose node group is
gone, or no longer names that provider and group, is deleted after
`-orphan-after` (15 minutes) if it has no node; one with a node is left alone
and logged. With `-metrics-addr`, `norca_nodescaler_orphaned_instances` (the
orphans the last sweep left) and `norca_nodescaler_orphans_deleted_total`
count them by provider.

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
`quota`, `no_capacity`, `invalid` or `error`. An instance may report its
`addresses`, every IP address its node's traffic to the operator can come
from (its own, a NAT in front of it), best already in the answer of the
create: the join endpoint binds the join and the renewals of the node to
them; without addresses there is no binding. See the package documentation
of `nodescaler/provider`.

A provider implements `provider.Provider` (`Group`, `Create`, `Delete`),
serves it with `provider.NewHandler` and registers it with
`registrar.Registrar`; `cmd/nodeprovider` puts the three together.
**`nodescaler/provider/providertest`** checks a provider against the
contract over HTTP, as the operator calls it: the capacity of a group
without instances, a create that is idempotent by its request ID, the names,
labels and request IDs of the instances, invalid requests, the quota reason,
the delete, a restart that loses nothing, and the token. The mock and the
docker provider run it in their tests; a provider for a cloud runs it
against a test account:

```go
func TestContract(t *testing.T) {
	providertest.Run(t, providertest.Config{
		New:        func(t *testing.T) provider.Provider { return newProvider(t) },
		Restart:    func(t *testing.T, _ provider.Provider) provider.Provider { return newProvider(t) },
		Group:      "fsn1",
		Params:     map[string]any{"size": "cx22"},
		QuotaGroup: "fsn1-small", QuotaMax: 2,
		Settle:     time.Minute, // the lists of the platform lag behind its writes
	})
}
```

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
A node that joins with a key of its own runs `norca-join` of the host
(`-docker-join-binary`) with the agent as its child. An instance reports the
addresses of its container and those of the host (`-docker-host-address`,
comma separated; default the host of `-endpoint` when it is an IP address
that is not a loopback one): a container reaches its own host from its
bridge address and other machines from the host's, after the masquerade of
the bridge. Toward the other machines the containers of one host share its
address, so the address binding of the join tells hosts apart, not
containers. It is for the lab and for trials, not for production.

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
make build     # bin/nodeprovider, bin/norca-nodescaler, bin/norca-join
```

Commits are signed off (`git commit -s`).
