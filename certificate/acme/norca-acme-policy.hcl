# SPDX-FileCopyrightText: 2026 Norca contributors
# SPDX-License-Identifier: MPL-2.0

# ACL policy of norca-acme. Bind it to the job of the operator, so the
# operator runs with its workload identity and needs no static token:
#
#   norca acl policy apply -namespace norca-system -job norca-acme \
#     norca-acme norca-acme-policy.hcl
#
# The job runs in the operations namespace norca-system, which also holds the
# secrets of the operator and the tokens of the DNS zones (-secret-namespace
# and -zone-token-namespace). The namespace must exist:
#
#   norca namespace apply -description "Norca operators" norca-system
#
# The operator never reads a secret of a namespace: the keys of the
# certificates stay with the servers.

# The kinds are cluster scoped, so their rules are in the namespace "*".
namespace "*" {
  # The issuers: the operator reads all of them and writes the status of
  # those that name it as their controller (the heartbeat, the roots).
  object "norca.io/CertificateIssuer" {
    capabilities = ["read", "write-status"]
  }

  # The orders the servers write: the operator reads the signing request
  # and writes the status (the state, the chain, the problem).
  object "norca.io/CertificateOrder" {
    capabilities = ["read", "write-status"]
  }

  # The DNS zones norca-provider registers, for the DNS-01 challenge.
  object "norca.io/DNSZone" {
    capabilities = ["read"]
  }
}

# The secrets of the operator, in the namespace -secret-namespace below
# -secret-prefix: the keys of its accounts (accounts/<issuer>), the
# certificates it issued until they expire (issued/<issuer>/<serial>, for a
# revocation), and the external account bindings an administrator puts
# there (eab/..., read only by the operator).
namespace "norca-system" {
  secret "norca-acme/*" {
    capabilities = ["read-secret", "write-secret"]
  }

  # The tokens the operator sends to the DNS providers are Variables of the
  # namespace -zone-token-namespace, at the paths the DNSZone objects name
  # (the token_variable of a provider of norca-provider).
  variables {
    path "providers/*" {
      capabilities = ["read"]
    }
  }
}
