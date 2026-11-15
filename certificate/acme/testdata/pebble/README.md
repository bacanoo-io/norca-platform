<!--
SPDX-FileCopyrightText: 2026 Norca contributors
SPDX-License-Identifier: MPL-2.0
-->

# The operator against Pebble

`TestPebble` (build tag `pebble`) runs the operator against
[Pebble](https://github.com/letsencrypt/pebble), the test certificate
authority of the ACME protocol, and a BIND name server. The unit tests use
a certificate authority of their own; this one speaks the real protocol
with the real client:

- an account is opened and the roots of the issuer are declared;
- a name is proven with **HTTP-01** (Pebble asks the test on port 5002);
- a name and its wildcard are proven with **DNS-01**: the records are
  written with dynamic updates signed with TSIG (the `rfc2136` type, over
  the DNS contract), waited for at the name server, and removed;
- the renewal information of the certificate authority is read;
- a certificate is revoked;
- an order for a name that does not exist fails with the problem of the
  certificate authority.

```shell
certificate/acme/testdata/pebble/run.sh
```

It starts three containers on a network of their own (10.30.50.0/24): the
name server (`named.conf`, the zones `http.lab` and `dns.lab`), Pebble with
the name server as its resolver, and the Go toolchain with the test. The
norca checkout must be next to this repository. `GO_VOLUMES` and `GO_ENV`
pass more arguments to the last container, for a module cache:

```shell
GO_VOLUMES="-v gomod:/go/pkg/mod" GO_ENV="-e GOPROXY=off" certificate/acme/testdata/pebble/run.sh
```

The TSIG secret of `named.conf` is a test value.
