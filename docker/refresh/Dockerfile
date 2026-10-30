# Refreshes a released image: its kividb, on the newest build of the runtime
# base. The binary, its home directory and its licence files are the released
# image's, byte for byte; the base, and libgcc_s with it, are today's. Used by
# .github/workflows/refresh-images.yml; BASE is a per-arch released image by
# digest.
#
# Distroless has no shell and no package manager, so the Debian updates
# cannot be applied on top of the released image (`apt-get upgrade`, as this
# did while the runtime was debian:bookworm-slim, up to 1.1.0): the released
# files are moved onto a new base instead. Keep what is set below the same as
# the runtime stage of the top-level Dockerfile.
ARG BASE
# Not pinned: the newest build is what a refresh is for. The workflow's Trivy
# gate stops it before any tag moves if that build carries a critical or high
# CVE with a fix.
ARG RUNTIME=gcr.io/distroless/base-nossl-debian12

FROM ${BASE} AS released

# libgcc_s, the one library the binary needs that the runtime base does not
# have, from the same Debian release, at its multiarch path.
FROM debian:bookworm-slim AS libgcc
RUN mkdir -p /rootfs/libgcc \
    && cp --parents /usr/lib/*-linux-gnu/libgcc_s.so.1 /rootfs/libgcc/

FROM ${RUNTIME}
COPY --from=libgcc /rootfs/libgcc/ /
COPY --from=released --chown=1000:1000 /home/kividb /home/kividb
ENV HOME=/home/kividb
USER 1000:1000
WORKDIR /home/kividb
COPY --from=released /usr/local/bin/kividb /usr/local/bin/kividb
COPY --from=released /usr/share/doc/kividb/ /usr/share/doc/kividb/
LABEL org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.vendor="KIVIDB PRIVATE LIMITED" \
      org.opencontainers.image.source="https://github.com/kividbio/kividb"
EXPOSE 6380
ENTRYPOINT ["kividb"]
CMD ["--protected-mode", "no"]
