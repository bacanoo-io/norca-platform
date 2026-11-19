# SPDX-FileCopyrightText: 2026 Norca contributors
# SPDX-License-Identifier: MPL-2.0

# ACL policy of the LoadBalancer operator. Bind it to the controller job, so
# the controller runs with its workload identity and needs no static token:
#
#   norca acl policy apply -namespace norca-system -job loadbalancer-controller \
#     loadbalancer-controller loadbalancer-policy.hcl
#
# The controller job runs in the operations namespace norca-system, which also
# holds its leases and the tokens of the registered classes (-lease-namespace
# and -class-token-namespace). The namespace must exist:
#
#   norca namespace apply -description "Norca operators" norca-system
#
# The LoadBalancer objects it serves are in the namespace "default"
# (-namespace). For an operator watching several namespaces (-namespace "*"),
# repeat the "default" block for each of them, or move its rules into the
# "*" block below; a block of a namespace by name takes the place of the "*"
# block for that namespace, so the "norca-system" block then repeats them.

# The namespace of the LoadBalancer objects and of their proxy jobs.
namespace "default" {
  # Read services, allocations and jobs; register and purge the proxy jobs
  # (lb-*) the operator owns.
  capabilities = ["list-jobs", "read-job", "submit-job"]

  variables {
    # The LoadBalancer objects: spec written by users, status by the
    # controller, objects of marked services created and deleted by it.
    path "crd/norca-io/LoadBalancer/*" {
      capabilities = ["list", "read", "write", "destroy"]
    }

    # The target lists of the proxy jobs (the jobs' own Variables).
    path "nomad/jobs/lb-*" {
      capabilities = ["list", "read", "write", "destroy"]
    }
  }

  # With -objects=native: the native LoadBalancer objects. "write" is
  # needed only for the objects created from marked services (discovery);
  # the status is written with "write-status", the event log with
  # "write-events", and the cleanup finalizer needs "manage-finalizers".
  # The crd/... Variables path above is then unused.
  object "norca.io/LoadBalancer" {
    capabilities = ["read", "write", "write-status", "write-events", "manage-finalizers"]
  }
}

# The namespace of the controller job: its own Variables.
namespace "norca-system" {
  variables {
    # Leader election leases of the two controllers of the operator.
    path "controller-lease/loadbalancer*" {
      capabilities = ["list", "read", "write"]
    }

    # With registered providers (LoadBalancerClass): the tokens the operator
    # sends to them, at the paths the classes name (the token_variable of a
    # provider of norca-provider).
    path "providers/*" {
      capabilities = ["read"]
    }
  }
}

# The nodes of the instances: an instance on a node marked as leaving (the
# meta norca.lb.leaving, set by the node scaling operator before a drain) is
# not a target, and node events reconcile the load balancers.
node {
  policy = "read"
}

# With -objects=native and registered providers (LoadBalancerClass): the kind
# is cluster-scoped, so its rules are in the namespace "*". The operator only
# reads the registrations; the providers write their own (see the policy of a
# provider below).
namespace "*" {
  object "norca.io/LoadBalancerClass" {
    capabilities = ["read"]
  }
}

# A provider application needs only its own registration and heartbeat, for
# example (bind it to the job of the provider):
#
#   namespace "*" {
#     object "norca.io/LoadBalancerClass" {
#       capabilities = ["read", "write", "write-status"]
#     }
#   }
