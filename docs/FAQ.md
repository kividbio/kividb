# FAQ

- [How KiviDB compares](#how-kividb-compares)
- [Licensing](#licensing)

## How KiviDB compares

| | KiviDB | Redis 8 | Valkey 8 | Dragonfly | KeyDB |
|---|---|---|---|---|---|
| License | AGPL-3.0 | RSALv2, SSPLv1 or AGPL-3.0 | BSD-3-Clause | BSL 1.1 | BSD-3-Clause |
| Language | Rust | C | C | C++ | C++ |
| Commands run on | every core (sharded) | one thread (I/O threads) | one thread (I/O threads) | every core (shared-nothing) | several threads |
| Full-text and vector search (`FT.*`) | built in | built in (Query Engine) | separate module | built in | no |
| Lua scripting | `lua` build | yes | yes | yes | yes |

As of October 2026. Something out of date or wrong? Please [open an issue](https://github.com/kividbio/kividb/issues/new/choose).

[PERFORMANCE.md](PERFORMANCE.md) has the benchmarks; [COMPATIBILITY.md](COMPATIBILITY.md)
lists the differences from Redis.

## Licensing

KiviDB is licensed under the [GNU Affero General Public License, version 3](../LICENSE)
(AGPL-3.0-only); the [README](../README.md#license) has the license terms in full.

- **Can I use KiviDB as the database of my closed-source or commercial application?** Yes.
  Your application connects to KiviDB over the network; it is not a derivative of KiviDB,
  and the AGPL places no obligation on it.
- **Can I run KiviDB as a service for my own users or customers?** Yes. If you run it
  unmodified, you have nothing to publish. If you modify KiviDB and offer that modified
  version over a network, you must offer its source to the users of that service.
- **Do client libraries or my configuration count as modifications?** No. Redis clients,
  configuration files, and scripts and functions you send to KiviDB are not part of KiviDB.
- **My organization does not allow AGPL software.** The commercial license is for that
  case: support@kividb.io.
