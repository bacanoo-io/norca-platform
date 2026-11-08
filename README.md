<!--
SPDX-FileCopyrightText: 2026 Norca contributors
SPDX-License-Identifier: MPL-2.0
-->

# norca-platform

The platform operators of Norca and the provider application they call, in
one repository. An operator is an application outside the core that turns
objects of the operator framework into things of a platform; the provider
application, `norca-provider`, knows the platforms (a cloud, the VMs of a
lab) and keeps their credentials. **One platform is one directory under
`providers/` and one type of `norca-provider`**, serving node scaling and
load balancers with the same credentials.

- **Node scaling** (`nodescaler/`): the core decides how many nodes a
  `NodeGroup` should have (`spec.replicas`); the operator `norca-nodescaler`
  creates and deletes the machines through a **node provider**.
- **Load balancers** (`loadbalancer/`): a service asks for a load balancer
  with `expose { load_balancer { class = "..." } }`; the operator
  `norca-loadbalancer` keeps a `norca.io/LoadBalancer` object for it and has
  the load balancer made by its built-in `nomad` class or by the provider of
  a **load balancer class**.

## Layout

```
provider/                 the framework of norca-provider: the type registry, the configuration file,
                          the build of the providers, one HTTP server for both contracts
provider/params/          the typed parameters of a type (required, secret, dynamic, published)
provider/wire/            what both contracts share: the error shape and the bearer token
provider/node/            the node contract: types, handler, client of the operator; providertest checks a provider
provider/lb/              the load balancer contract: types, handler, client of the operator; lbtest checks a provider
provider/registrar/       the NodeProvider and LoadBalancerClass objects of the providers and their heartbeat
providers/<platform>/     one directory per platform: aws, gcp, hcloud, digitalocean, vultr, scaleway,
                          nginx, tcpproxy (load balancers), docker (nodes), mock (both); all links them
nodescaler/operator/      the node scaling operator: reconciles the NodeGroup objects with their providers
nodescaler/bootstrap/     the bootstrap of a machine: the node's certificate and agent configuration, as cloud-init
nodescaler/join/          the hardened node identity: the join endpoint of the operator and norca-join on the node
loadbalancer/operator/    the LoadBalancer operator, with its kinds (loadbalancer.kind.hcl,
                          loadbalancerclass.kind.hcl), their schemas, its ACL policy and a job file
cmd/norca-provider/       the provider application
cmd/norca-nodescaler/     the node scaling operator
cmd/norca-join/           the program on a node that makes its key, joins with a one-time token and renews
cmd/norca-loadbalancer/   the LoadBalancer operator (and the proxy of its nomad class)
```

## The node scaling operator

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

## The provider application

`norca-provider` serves the providers its configuration file names, on one
HTTP server, and registers them in Norca: a `norca.io/NodeProvider` object
for a node provider, a `norca.io/LoadBalancerClass` object for each load
balancer class, both with the same status (`heartbeat`, RFC 3339, and
`version`), updated every `heartbeat_interval` (30 seconds), registered
again when an object disappears and removed on exit
(`-deregister-on-exit`). `-register=false` serves without touching Norca.

**Types.** A type is a platform. It declares the parameters its two sides
share (the credentials, the API of the platform) and implements a node
provider, a load balancer provider or both:

```
$ norca-provider types
docker - containers on the host of the provider, each running a Norca agent; for the lab and for trials [node]
  node.params:
    image                    string   (default debian:bookworm-slim)  the image of the containers ...
  node.groups[].params:
    cpu                      int      [required]  CPU of a machine of the group, in MHz
...
hcloud - Hetzner Cloud [loadbalancer]
  params:
    token                    string   [required, secret]  Hetzner Cloud API token (supports env:NAME or file:/path)
  loadbalancer.classes[].params:
    location                 string   (default fsn1)  Hetzner Cloud location name ...
    type                     string   (default lb11) [dynamic]  Hetzner Cloud load balancer type ...
```

Every parameter has a name, a type (`string`, `int`, `bool`, `duration`,
`list`), a description and optionally a default, `required`, an enum,
`secret` (never published; a value may be `env:NAME` or `file:/path`) and
`dynamic` (class parameters only: the operator sends the `parameters` of the
`LoadBalancerClass` object with every call and they override the configured
ones, so they change without a restart). The parameters that are not secret
are published: those of a group in the groups of the `NodeProvider`, those of
a class (and the shared ones) in `spec.parameters` of the
`LoadBalancerClass`. Unknown parameters and wrong types are errors at start.

**Configuration.** `providers.example.json` shows a full file:

```json
{
  "listen": ":8443",
  "tls": { "cert": "tls.crt", "key": "tls.key" },
  "advertise_url": "https://norca-provider.service.norca:8443",
  "heartbeat_interval": "30s",
  "providers": [{
    "name": "hetzner-eu", "type": "hcloud",
    "params": { "token": "${HCLOUD_TOKEN}" },
    "token_file": "/secrets/token", "token_variable": "providers/hetzner-eu",
    "node": { "groups": [{ "name": "fsn1", "max": 10, "params": {} }] },
    "loadbalancer": { "classes": [{ "class": "hcloud", "params": { "location": "fsn1" } }] }
  }]
}
```

- **What runs is what the file says**: only the sections a provider has
  (`node`, `loadbalancer`) are started, registered and served. A section the
  type does not implement is an error at start (`type nginx has no node
  provider`). `-serve node|lb|all` (default `all`) narrows what a process
  serves of its file; it never adds a section.
- **Endpoints.** The node provider of `hetzner-eu` is served at
  `advertise_url` + `/providers/hetzner-eu` (the endpoint of its
  `NodeProvider`), a class at `advertise_url` + `/classes/<class>`. `endpoint`
  in the node section or in a class replaces it, behind a port mapping or a
  proxy: the registered URL is not the address the process listens on.
- **The token** of a provider (`token`, `env:NAME` and `file:/path`
  resolved, or `token_file`) is required of every call to its node provider
  and its classes, as `Authorization: Bearer`; `token_variable` is the
  Variable in Norca (item `token`) the operators read to send it. Without a
  token the provider needs `no_auth` (a trusted network only).
- A class may limit itself: `internal: false` offers only public load
  balancers, `internal: true` requires a type that can make internal ones,
  `ports` (`"80,443,8000-8100"`) limits the frontend ports; the server checks
  both before the provider is called.
- **`${NAME}` and `${NAME:-default}`** in any string are replaced with the
  environment (an unset variable without a default is an error), so a file
  can be written for a job whose address and port are known when it is
  placed: `"listen": "0.0.0.0:${NORCA_PORT_api}"`. Inside a Norca job file the
  dollar sign is doubled in the `template` block.
- The LoadBalancer operator accepts `https` endpoints, `http` only with
  `-class-allow-http`, and trusts a private CA with `-class-ca-file`.

The server also answers `GET /healthz`, `GET /v1/types` (the linked types,
their capabilities and parameters), `GET /v1/providers` and `GET /v1/classes`
(what it serves; tokens are never listed).

**Running it.** The Norca API is read from the environment (`NORCA_ADDR`,
`NORCA_TOKEN`). As a Norca job, `NORCA_ADDR=unix://${NORCA_SECRETS_DIR}/api.sock`
with `identity { env = true }` uses the task API socket and the workload
identity; as a container in bridge mode, `NORCA_ADDR` is a server and
`NORCA_CACERT` the CA of the cluster. The ACL policy of the job needs
`read`, `write` and `write-status` on the kinds it registers
(`norca.io/NodeProvider`, `norca.io/LoadBalancerClass`) in the namespace
`"*"`, and nothing else. The `norca-provider` pack of the registry is a
complete job; the `Dockerfile` builds a `FROM scratch` image (the docker
platform needs the docker CLI and runs on the host).

```shell
norca var put providers/hetzner-eu token="$PROVIDER_TOKEN"
norca-provider -config providers.json
```

## The node provider contract

A node provider is registered with a `norca.io/NodeProvider` object (its
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
of `provider/node`.

**`provider/node/providertest`** checks a node provider against the
contract over HTTP, as the operator calls it: the capacity of a group
without instances, a create that is idempotent by its request ID, the names,
labels and request IDs of the instances, invalid requests, the quota reason,
the delete, a restart that loses nothing, and the token. The mock and the
docker provider run it in their tests; a provider for a cloud runs it
against a test account:

```go
func TestContract(t *testing.T) {
	providertest.Run(t, providertest.Config{
		New:        func(t *testing.T) node.Provider { return newProvider(t) },
		Restart:    func(t *testing.T, _ node.Provider) node.Provider { return newProvider(t) },
		Group:      "fsn1",
		Params:     map[string]any{"size": "cx22"},
		QuotaGroup: "fsn1-small", QuotaMax: 2,
		Settle:     time.Minute, // the lists of the platform lag behind its writes
	})
}
```

**The docker platform** runs each instance as a container on the host it
runs on: it reads the files back from the cloud-init document of the
instance, writes them under `state_dir` and mounts them read-only at their
paths, mounts the norca binary of the host (`binary`), and runs
`norca agent -config <the .hcl file>` in `image` (a privileged container
with the host's cgroups, named after the instance). The containers carry the
labels of the request, so a restart of the provider loses nothing. A node
that joins with a key of its own runs `norca-join` of the host
(`join_binary`) with the agent as its child. An instance reports the
addresses of its container and those of the host (`host_addresses`; default
the host of the endpoint of the provider when it is an IP address that is
not a loopback one): a container reaches its own host from its bridge
address and other machines from the host's, after the masquerade of the
bridge. Toward the other machines the containers of one host share its
address, so the address binding of the join tells hosts apart, not
containers. It is for the lab and for trials, not for production.

```json
{"name": "lab", "type": "docker", "token_file": "/secrets/token", "token_variable": "nodescaler/providers/lab",
 "node": {"params": {"state_dir": "/var/lib/norca-provider"},
          "groups": [{"name": "dc2", "max": 3, "params": {"cpu": 2000, "memory": 1024, "disk": 10000}}]}}
```

## The load balancer contract

A class is registered with a `norca.io/LoadBalancerClass` object (its
endpoint, the Variable with its token, its capabilities `internal` and
`ports`, its published parameters) and keeps a heartbeat in the status. The
operator calls it:

```
PUT    {endpoint}/v1/loadbalancers/{namespace}/{name}   create or update (idempotent), returns the state
DELETE {endpoint}/v1/loadbalancers/{namespace}/{name}   remove
```

with a bearer token. `PUT` carries the whole desired state (service, port,
internal, the healthy targets, the parameters of the object) and returns
`ready`, `addresses`, `resource` and `message`. A failure has the shape of
the node contract (`{"reason": "invalid", "error": "..."}`): 422 for a
request the provider cannot satisfy (the operator does not retry it with the
same input), 502 for a failure of the platform. See the package
documentation of `provider/lb`; **`provider/lb/lbtest`** checks a provider
the way `providertest` does for nodes.

The load balancer platforms:

- **mock**: keeps the load balancers in memory. `ready` and `latency` are
  dynamic, so editing the `LoadBalancerClass` object makes the mock report an
  outage or a slow cloud.
- **tcpproxy**: a TCP listener per load balancer inside the provider; each
  connection goes to the next target (round robin) and to the next one when a
  target does not answer. After a restart the load balancers are back when
  the operator calls again. The firewall must allow their ports.
- **nginx**: writes one file per load balancer into `config_dir`, to be
  included in the `stream` context, and runs `reload_command` when something
  changed. A configuration that nginx rejects is rolled back.
- **hcloud**, **scaleway**, **aws**, **gcp**, **digitalocean**, **vultr**: the
  load balancers of the cloud through its REST API (no SDK), public and
  private ones where the cloud has them, with dynamic parameters for the type
  or size and the algorithm.

## Adding a platform

1. Create `providers/<id>/`, implement `node.Provider` (`Group`, `Create`,
   `Delete`) and/or `lb.Provider` (`Capabilities`, `Ensure`, `Delete`).
2. Declare the parameters and register the type in `init()`:

   ```go
   var shared = []params.Param{
       {Name: "token", Type: params.ParamString, Required: true, Secret: true},
   }
   var classParams = []params.Param{
       {Name: "location", Type: params.ParamString, Default: "fsn1"},
       {Name: "type", Type: params.ParamString, Default: "lb11", Dynamic: true},
   }

   func init() {
       provider.Register(provider.Type{
           ID: "hcloud", Description: "Hetzner Cloud", Params: shared,
           Node: &provider.NodeSpec{GroupParams: groupParams, New: newNode}, // nil without node provider
           LB:   &provider.LBSpec{Params: classParams, New: NewLB},          // nil without load balancers
       })
   }
   ```

3. Add a blank import to `providers/all/all.go`.

A constructor gets the resolved shared parameters together with those of its
section (`provider.NodeConfig`: the node parameters and the groups with
their parameters and max; `lb.Config`: the class parameters). `Ensure`
receives the whole desired state each time and must be idempotent; `Delete`
must accept what does not exist. An error wrapping `lb.ErrInvalid`
(`lb.Invalid(...)`) means the request cannot be satisfied. The server
serializes the calls for one load balancer. A platform with no state of its
own should find its resources by a name derived from the request (the
instance name, the namespace and name of the load balancer) or by a tag, so
it survives a restart. Run `providertest` and `lbtest` in its tests.

## The LoadBalancer operator

`norca-loadbalancer` (`loadbalancer/operator`) reconciles every
`norca.io/LoadBalancer` object: it follows the healthy instances of the
service and has the load balancer made by the class of the object, the
built-in `nomad` class (a proxy job of the same binary, `norca-loadbalancer
proxy`) or a registered `LoadBalancerClass`. A class without a registration
is reported as `UnknownClass`, one whose heartbeat is older than
`-class-ttl` (three minutes) as `ProviderUnavailable`. It is installed as
the `loadbalancer-operator` pack of the registry; `loadbalancer.norca.hcl`
and `loadbalancer-policy.hcl` run it by hand, after
`norca object-kind apply loadbalancer.kind.hcl` and
`loadbalancerclass.kind.hcl`.

```shell
norca-loadbalancer -objects native -class-ca-file /secrets/ca.pem
```

## Development

The Norca API client and the controller SDK come from the checkout next to
this repository (`replace` in `go.mod`): keep `norca` and `norca-platform`
side by side.

```shell
make test      # the tests
make lint      # golangci-lint v2 with .golangci.yml
make build     # bin/norca-provider, bin/norca-nodescaler, bin/norca-join, bin/norca-loadbalancer
```

The tests need no cloud, no nginx and no Norca. The end-to-end test of the
LoadBalancer operator (`loadbalancer/operator/e2e_test.go`) installs the
`loadbalancer-operator` pack on a dev agent: it needs a `norca` binary
(`NORCA_BINARY` or on the `PATH`) and a `norca-registry` checkout
(`NORCA_REGISTRY_DIR`, or next to this repository), and is skipped without
them. The whole chain on real machines is in the lab (`norca-lab`, cases
`nodescaler`, `loadbalancer`, `loadbalancer-docker`).

Commits are signed off (`git commit -s`).
