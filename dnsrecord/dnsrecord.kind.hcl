# SPDX-FileCopyrightText: 2026 Norca contributors
# SPDX-License-Identifier: MPL-2.0

# The DNSRecord object kind: a DNS name that follows what runs in the cluster,
# kept by norca-dns in a zone of norca-provider. Register it (operator write is
# required) with:
#
#   norca object-kind apply dnsrecord.kind.hcl
#
# and create records with "norca object apply", for example:
#
#   kind      = "norca.io/DNSRecord"
#   name      = "shop"
#   namespace = "team-a"
#   spec {
#     name   = "shop.team-a.example.com"
#     type   = "A"
#     values = ["203.0.113.10"]
#   }
#
# The namespace may write names under its allow list only (the namespace
# metadata norca.io/dns-suffixes), and the zone of the name must let norca-dns
# write records (records = true in the configuration of norca-provider).
kind "DNSRecord" {
  group       = "norca.io"
  description = "A DNS record set kept by norca-dns in a zone of norca-provider"
  namespaced  = true
  schema      = file("dnsrecord.schema.json")

  rule {
    expression = "has(self.spec.values) != has(self.spec.source)"
    message    = "a record takes its values from values or from source, one of them"
  }

  columns {
    column "Name" { path = "spec.name" }
    column "Type" { path = "spec.type" }
    column "Values" { path = "status.values" }
    column "Zone" { path = "status.zone" }
    column "Deletion" { path = "spec.deletion_policy" }
    column "Ready" { path = "status.conditions[?type=='Ready'].status" }
  }
}
