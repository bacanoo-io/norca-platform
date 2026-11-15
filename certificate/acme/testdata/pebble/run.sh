#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Norca contributors
# SPDX-License-Identifier: MPL-2.0
#
# Runs TestPebble: the operator against Pebble (the test certificate
# authority of the ACME protocol) and a BIND name server that takes dynamic
# updates, all in containers on a network of their own.
#
# Usage: certificate/acme/testdata/pebble/run.sh
# Needs docker and, next to this repository, the norca checkout (go.mod
# takes the API from ../norca). Environment:
#   GO_IMAGE      image with the Go toolchain (default golang:1.27-bookworm)
#   PEBBLE_IMAGE  default ghcr.io/letsencrypt/pebble:latest
#   BIND_IMAGE    default internetsystemsconsortium/bind9:9.20
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../../.." && pwd)"
norca="$(cd "$repo/../norca" && pwd)"
go_image="${GO_IMAGE:-golang:1.27-bookworm}"
pebble_image="${PEBBLE_IMAGE:-ghcr.io/letsencrypt/pebble:latest}"
bind_image="${BIND_IMAGE:-internetsystemsconsortium/bind9:9.20}"
net=norca-acme-test
tsig="c2VjcmV0LXNlY3JldC1zZWNyZXQtc2VjcmV0LXNlY3JldA=="

# On Windows (Git Bash) paths of volumes must not be rewritten.
export MSYS_NO_PATHCONV=1
vol() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else echo "$1"; fi; }

cleanup() {
  docker rm -f norca-acme-test-ns norca-acme-test-pebble >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
  rm -rf "$work"
}
work="$(mktemp -d)"
trap cleanup EXIT
cleanup
mkdir -p "$work"

docker network create --subnet 10.30.50.0/24 "$net" >/dev/null

# The name server: the zone files are copied to where named may write its
# journal.
docker run -d --name norca-acme-test-ns --network "$net" --ip 10.30.50.3 \
  -v "$(vol "$here")":/src:ro --entrypoint sh "$bind_image" \
  -c 'cp /src/db.* /var/lib/bind/ && chown bind /var/lib/bind/db.* && exec /usr/sbin/named -u bind -g -c /src/named.conf' >/dev/null

# Pebble resolves the names at the name server and checks at once.
docker run -d --name norca-acme-test-pebble --network "$net" --ip 10.30.50.4 --hostname pebble \
  -e PEBBLE_VA_NOSLEEP=1 "$pebble_image" \
  -config test/config/pebble-config.json -dnsserver 10.30.50.3:53 >/dev/null

# The CA of the https certificate of Pebble is in its image.
docker cp norca-acme-test-pebble:/test/certs/pebble.minica.pem "$(vol "$work")/pebble-ca.pem" >/dev/null
sleep 3

docker run --rm --network "$net" --ip 10.30.50.5 \
  -v "$(vol "$repo")":/src/platform -v "$(vol "$norca")":/src/norca:ro \
  -v "$(vol "$work/pebble-ca.pem")":/pebble-ca.pem:ro \
  ${GO_VOLUMES:-} -w /src/platform -e GOFLAGS=-buildvcs=false ${GO_ENV:-} \
  -e PEBBLE_DIRECTORY=https://pebble:14000/dir \
  -e PEBBLE_MANAGEMENT=https://pebble:15000 \
  -e PEBBLE_CA_FILE=/pebble-ca.pem \
  -e PEBBLE_DNS=10.30.50.3:53 \
  -e PEBBLE_TSIG_SECRET="$tsig" \
  -e PEBBLE_HTTP_NAME=www.http.lab \
  -e PEBBLE_DNS_ZONE=dns.lab \
  "$go_image" go test -tags pebble -count=1 -run TestPebble -v ./certificate/acme/
