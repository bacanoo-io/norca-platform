# SPDX-FileCopyrightText: 2026 Norca contributors
# SPDX-License-Identifier: MPL-2.0

# The DNSZone object kind: the registry of the DNS zones a provider can write
# TXT records in, for the DNS-01 challenges of norca-acme. Register it
# (operator write is required) with:
#
#   norca object-kind apply dnszone.kind.hcl
#
# A provider is an external application that implements the DNS contract
# (package provider/dns of norca-platform; norca-provider serves it). When it
# starts, it registers the zones of its configuration, for example:
#
#   kind = "norca.io/DNSZone"
#   name = "example.com"
#   spec {
#     zone          = "example.com"
#     provider      = "dns-eu"
#     endpoint      = "https://provider.service.eu.norca:8443/zones/example.com"
#     tokenVariable = "providers/dns-eu"
#   }
#
# and reports a heartbeat in the status (status.heartbeat, RFC 3339) while
# it runs. A zone whose provider stopped reporting is not used for new
# challenges.
kind "DNSZone" {
  group       = "norca.io"
  description = "A DNS zone a provider writes challenge records in, and where the operator calls it"
  namespaced  = false
  schema      = file("dnszone.schema.json")

  columns {
    column "Provider" { path = "spec.provider" }
    column "Endpoint" { path = "spec.endpoint" }
    column "Last seen" { path = "status.heartbeat" }
  }
}
