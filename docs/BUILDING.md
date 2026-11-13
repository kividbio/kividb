# Building KiviDB

- [Builds](#builds)
- [Building from source](#building-from-source)

## Builds

KiviDB comes in four variants, chosen by Cargo features at build time:

| Variant | Cargo features | Adds |
|---------|----------------|------|
| `default` | none | the core server, without Lua or TLS |
| `lua` | `lua` | Lua scripting (`EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`, `FCALL`, …) |
| `tls` | `tls` | TLS listener, and TLS between cluster nodes and replicas |
| `full` | `lua,tls` | both |

In a build without Lua, the scripting commands return an error. A build without TLS refuses
to start if `--tls-port` is given, rather than serving plaintext.

Lua scripting is an optional Cargo feature: `EVAL`, `SCRIPT`, `FUNCTION`, `FCALL` and related
commands are available in builds with `--features lua` (see
[Scripting commands (Lua)](../COMMANDS.md#scripting-commands-lua)). TLS is an optional Cargo
feature too (`--features tls`).

Docker images, release tarballs and `cargo install` offer every variant; the apt and dnf
packages and Homebrew offer the default one (`kividb`) and, from 1.3.0, the full one
(`kividb-full`). See [INSTALL.md](INSTALL.md).

## Building from source

Requires a **stable Rust** toolchain ([`rust-toolchain.toml`](../rust-toolchain.toml) selects
`stable` with `rustfmt` and `clippy`). The crate uses the **2024 edition**; the oldest
toolchain it is checked against is **Rust 1.89**, the version the [`Dockerfile`](../Dockerfile)
builds with.

```bash
git clone https://github.com/kividbio/kividb.git
cd kividb

cargo build --release                      # default variant
cargo build --release --features lua       # with Lua scripting
cargo build --release --features tls       # with TLS
cargo build --release --features lua,tls   # full

./target/release/kividb
```

On Linux the server uses io_uring when the kernel allows it and falls back to Tokio
otherwise (`--io-model auto`, the default).

To build the Docker image yourself, `docker build -t kividb .` builds the default variant;
`docker build --build-arg KIVIDB_FEATURES=lua,tls -t kividb:full .` builds another one.

[CONTRIBUTING.md](../CONTRIBUTING.md) covers building and testing for contributors.
