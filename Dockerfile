# syntax=docker/dockerfile:1

# ── Stage 1: build ────────────────────────────────────────────────────────────
# Pinned to the same Debian release as the runtime stage below (bookworm) —
# `rust:1.89-slim` floats to whatever Debian release is current upstream
# (currently trixie/glibc 2.40+), which produces a binary that fails to
# start on the older bookworm-slim runtime ("GLIBC_2.38 not found") for any
# build that pulls in a dependency touching a newer glibc symbol version
# (observed with the `tls` feature's dependency chain).
FROM rust:1.89-slim-bookworm AS builder

WORKDIR /build

# Which Cargo features to build with — set per image variant via
# `docker build --build-arg KIVIDB_FEATURES=tls` (see .github/workflows/release.yml).
# Must be declared before both RUN cargo build steps below, including the
# dummy dependency-cache build, so the cached deps layer matches the feature
# set actually used by the real build.
ARG KIVIDB_FEATURES=""

# Cache dependencies separately from source — Docker layer cache trick.
# Copy only the manifest files first, build a dummy main, then replace with
# real source. This means `cargo build --release` is only re-run when
# Cargo.toml/Cargo.lock change, not on every source edit.
COPY Cargo.toml Cargo.lock ./
# On arm64, atomics as single instructions (LSE) and not as calls that choose
# at run time: it needs ARMv8.1, which every arm64 server has (a Raspberry
# Pi 4 has not). The same flags in both builds, or the second rebuilds the
# dependencies the first cached.
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && if [ "$(uname -m)" = aarch64 ]; then export RUSTFLAGS="-C target-feature=+lse"; fi \
    && cargo build --release ${KIVIDB_FEATURES:+--features "${KIVIDB_FEATURES}"} \
    && rm -f target/release/kividb target/release/deps/kividb*

# Now copy real source and build for real
COPY src ./src
RUN if [ "$(uname -m)" = aarch64 ]; then export RUSTFLAGS="-C target-feature=+lse"; fi \
    && cargo build --release ${KIVIDB_FEATURES:+--features "${KIVIDB_FEATURES}"}

# The runtime image has no shell to make the home directory with.
RUN mkdir -p /rootfs/home/kividb

# The one library the binary needs that the runtime image does not have:
# libgcc_s, for unwinding. Kept at its multiarch path (amd64 or arm64), where
# glibc's loader looks.
RUN mkdir -p /rootfs/libgcc \
    && cp --parents /usr/lib/*-linux-gnu/libgcc_s.so.1 /rootfs/libgcc/

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# Distroless, without OpenSSL: glibc, CA certificates and time zones, and
# libgcc_s copied in from the builder -- what the binary links against (libc,
# libm, libgcc_s) -- and nothing else: no shell, no package manager, no perl or
# util-linux. Up to 1.1.0 the runtime was debian:bookworm-slim, whose ~90
# packages carried every CVE a scanner reported on the image (4 critical and
# 60 high, none of them in code kividb runs). Then distroless/cc, which ships
# libssl3: the binary never loads it (TLS is rustls, built in), but scanners
# reported its CVEs on the image all the same (12 in October 2026). Same Debian
# release as the builder, so the same glibc.
#
# It has no redis-cli either. To debug a container, connect from another one
# (`docker run --rm -it --network container:<name> redis redis-cli -p 6380`,
# or `kubectl debug` in Kubernetes).
#
# Pinned by digest; Dependabot proposes each new one (.github/dependabot.yml).
FROM gcr.io/distroless/base-nossl-debian12@sha256:e35893f8ca1cabfbd60394af94899c20930c4615355adf97c5da660a51b61bca

COPY --from=builder /rootfs/libgcc/ /

# The same non-root user as before 1.1.1, UID and GID 1000, so data written by
# an older image (a bind mount, a named volume) stays writable.
COPY --from=builder --chown=1000:1000 /rootfs/home/kividb /home/kividb
ENV HOME=/home/kividb
USER 1000:1000
WORKDIR /home/kividb

COPY --from=builder /build/target/release/kividb /usr/local/bin/kividb

# KiviDB is licensed under the GNU Affero General Public License v3 (AGPL-3.0-only),
# as in the release tarballs and packages.
COPY LICENSE NOTICE /usr/share/doc/kividb/
LABEL org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.vendor="KIVIDB PRIVATE LIMITED" \
      org.opencontainers.image.source="https://github.com/kividbio/kividb"

EXPOSE 6380

ENTRYPOINT ["kividb"]
# A container is reached through its network, never from loopback: protected
# mode would turn every client away. As in Redis's image, it is off by default
# here; arguments of your own replace this, and with them it is on unless you
# set a password or a bind address.
CMD ["--protected-mode", "no"]