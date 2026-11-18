# SPDX-FileCopyrightText: 2026 Norca contributors
# SPDX-License-Identifier: MPL-2.0

# ACL policy of norca-dns. Bind it to the job of the operator, so the operator
# runs with its workload identity and needs no static token:
#
#   norca acl policy apply -namespace default -job norca-dns \
#     norca-dns norca-dns-policy.hcl
#
# The records are namespaced: the rules below are for every namespace ("*").
# An operator for one namespace (-namespace) needs write-status, write-events
# and manage-finalizers there only, but it still reads the records and the
# metadata of every namespace: a name is one for the whole cluster, and the
# record that holds it can be in any namespace. Without that read it can't
# see such a record, and writes over or removes its record set.
namespace "*" {
  # The allow list of a namespace is its metadata (norca.io/dns-suffixes).
  capabilities = ["read-job"]

  # The records: the status is the operator's, the finalizer keeps a record
  # until its record set is removed, the events say what it did.
  object "norca.io/DNSRecord" {
    capabilities = ["read", "write-status", "write-events", "manage-finalizers"]
  }

  # The load balancers whose addresses a record takes (source.load_balancer).
  object "norca.io/LoadBalancer" {
    capabilities = ["read"]
  }

  # The DNS zones norca-provider registers (cluster scoped).
  object "norca.io/DNSZone" {
    capabilities = ["read"]
  }
}

namespace "default" {
  # A block of a namespace by name takes the place of the "*" block above for
  # that namespace: without a capability of its own here the operator could
  # not read the metadata of this namespace, and no record of it would be
  # written. Any capability lets the namespace be read; list-jobs is the
  # narrowest.
  capabilities = ["list-jobs"]

  variables {
    # The tokens the operator sends to the DNS providers are Variables of
    # the namespace -zone-token-namespace, at the paths the DNSZone objects
    # name (the token_variable of a provider of norca-provider).
    path "providers/*" {
      capabilities = ["read"]
    }

    # With -leader-election: the lease of the instances.
    path "controller-lease/norca-dns*" {
      capabilities = ["list", "read", "write"]
    }
  }
}
