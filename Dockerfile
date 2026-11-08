# SPDX-FileCopyrightText: 2026 Norca contributors
# SPDX-License-Identifier: MPL-2.0
#
# norca-provider as an image: a static binary, nothing else (the docker
# platform needs the docker CLI and is run on the host instead).
#
#   CGO_ENABLED=0 GOOS=linux go build -o norca-provider ./cmd/norca-provider
#   docker build -t norca/provider .
FROM scratch
COPY norca-provider /norca-provider
ENTRYPOINT ["/norca-provider"]
