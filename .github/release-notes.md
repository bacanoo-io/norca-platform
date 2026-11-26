## norca-platform {{VERSION}}

<!-- Draft release: add the highlights and the changes of this release here before publishing. -->

### norca-join

`norca-join` gives a scaled node its identity: it makes the key on the machine, joins with the
one-time token of its bootstrap, starts the agent and renews the certificate. Each binary is
static (built with `CGO_ENABLED=0`): the same file runs on glibc distributions (Debian, Ubuntu,
RHEL) and on Alpine Linux.

| Platform | Binary |
| --- | --- |
| Linux x86-64 | `norca-join_{{VERSION}}_linux_amd64` |
| Linux ARM64 | `norca-join_{{VERSION}}_linux_arm64` |

Checksums: `norca-platform_{{VERSION}}_SHA256SUMS`. Software bill of materials (CycloneDX JSON):
`norca-join_{{VERSION}}_linux_<arch>.cdx.json`.

norca-deployer installs this release on the machines of the node scaling operator with
`node_scaling.machine.join_download: {source: official, version: v{{VERSION}}}`: the install
script downloads the binary of the architecture of the machines and checks it against the
digest in the checksums.

### Source code and license

The source code of this release is available under the Mozilla Public License 2.0 at
{{TAG_URL}}. It was built with the Norca API client and controller SDK of the norca commit
`{{NORCA_COMMIT}}` ({{NORCA_URL}}).
