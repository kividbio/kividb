#!/bin/bash
# Tests the `kividb` and `kividb-full` packages against each other (#732):
# install, swap, swap back and upgrade, in clean containers running systemd,
# from a local apt or dnf repository. Also renders the Homebrew formulas.
#
#   packaging/test/swap-test.sh DEFAULT_BINARY FULL_BINARY [DISTRO...]
#
# DEFAULT_BINARY  kividb built with no features (the `kividb` package)
# FULL_BINARY     kividb built with --features lua,tls (`kividb-full`)
# DISTRO          debian:12 ubuntu:24.04 rockylinux:9 amazonlinux:2023 (the
#                 default: all four)
#
# The binaries must run on glibc 2.34, as the release's do: build them in
# amazonlinux:2023 (.github/workflows/release.yml). Run from the
# repository's root on an x86_64 Linux machine with Docker; it runs
# containers --privileged, for systemd. nfpm is used from PATH, or from its
# Docker image. Each distribution's log is in target/pkgtest/<distro>.log.
#
# Four versions of each package are built: the version in Cargo.toml and
# three more with the patch number raised, each with one more comment line
# in the default kividb.conf, so that an upgrade or a swap meets an edited
# config and a new default. packaging/test/in-container.sh says what is
# checked at each step.
set -euo pipefail

default_bin="$(realpath "${1:?usage: swap-test.sh DEFAULT_BINARY FULL_BINARY [DISTRO...]}")"
full_bin="$(realpath "${2:?usage: swap-test.sh DEFAULT_BINARY FULL_BINARY [DISTRO...]}")"
shift 2
distros=("$@")
[ "${#distros[@]}" -gt 0 ] || distros=(debian:12 ubuntu:24.04 rockylinux:9 amazonlinux:2023)

[ -f packaging/nfpm.yaml ] || { echo "run from the repository's root" >&2; exit 1; }
NFPM_IMAGE="${NFPM_IMAGE:-goreleaser/nfpm:v2.41.0}"
work="target/pkgtest"
rm -rf "$work"
mkdir -p "$work/bin" "$work/pkgs"
cp "$default_bin" "$work/bin/kividb-default"
cp "$full_bin" "$work/bin/kividb-full"

"$work/bin/kividb-default" --version | grep -q "(default build" ||
    { echo "$default_bin is not the default build: $("$work/bin/kividb-default" --version)" >&2; exit 1; }
"$work/bin/kividb-full" --version | grep -q "(full build" ||
    { echo "$full_bin is not the full build: $("$work/bin/kividb-full" --version)" >&2; exit 1; }

nfpm() {
    if type -P nfpm >/dev/null; then
        command nfpm "$@"
    else
        docker run --rm -u "$(id -u):$(id -g)" -v "$PWD:/src" -w /src "$NFPM_IMAGE" "$@"
    fi
}

# ── Versions ─────────────────────────────────────────────────────────────
base="$(awk '/^\[package\]/{p=1;next} /^\[/{p=0} p&&/^version *=/{gsub(/[" ]/,"");sub(/^version=/,"");print;exit}' Cargo.toml)"
base="${base%%-*}"
IFS=. read -r major minor patch <<<"$base"
versions=()
for i in 0 1 2 3; do versions+=("$major.$minor.$((patch + i))"); done
echo "versions: ${versions[*]}"

# ── Packages ─────────────────────────────────────────────────────────────
for i in 0 1 2 3; do
    v="${versions[$i]}"
    mkdir -p "$work/pkgs/$v"
    conf="$work/kividb.conf.$v"
    cp packaging/kividb.conf "$conf"
    for j in $(seq 1 "$i"); do echo "# swap test: default config of version $j" >> "$conf"; done
    for variant in default full; do
        sed "s|^  - src: ./packaging/kividb.conf\$|  - src: ./$conf|" \
            <(bash packaging/nfpm-config.sh "$variant" "$v" amd64 "./$work/bin/kividb-$variant") \
            > "$work/nfpm-$variant-$v.yaml"
        grep -q "src: ./$conf" "$work/nfpm-$variant-$v.yaml"
        name=kividb
        [ "$variant" = full ] && name=kividb-full
        nfpm pkg -f "$work/nfpm-$variant-$v.yaml" --packager deb --target "$work/pkgs/$v/${name}_${v}_amd64.deb" >/dev/null
        nfpm pkg -f "$work/nfpm-$variant-$v.yaml" --packager rpm --target "$work/pkgs/$v/${name}-${v}-1.x86_64.rpm" >/dev/null
    done
done
ls -l "$work"/pkgs/*/

# What each package says about the other, as built for the release.
echo "== kividb-full_${versions[0]}_amd64.deb"
docker run --rm -v "$PWD/$work/pkgs:/p:ro" debian:12 \
    dpkg-deb -f "/p/${versions[0]}/kividb-full_${versions[0]}_amd64.deb" Package Version Provides Conflicts Replaces Depends
echo "== kividb_${versions[0]}_amd64.deb"
docker run --rm -v "$PWD/$work/pkgs:/p:ro" debian:12 \
    dpkg-deb -f "/p/${versions[0]}/kividb_${versions[0]}_amd64.deb" Package Version Provides Conflicts Replaces Depends

# ── Homebrew formulas ────────────────────────────────────────────────────
a="$(printf a%.0s $(seq 64))"
b="$(printf b%.0s $(seq 64))"
for variant in default full; do
    sh packaging/homebrew/formula.sh "$variant" 9.8.7 kividbio/kividb "$a" "$b" > "$work/formula-$variant.rb"
done
check_formula() { # check_formula FILE CLASS PREFIX OTHER
    local f="$1"
    grep -qx "class $2 < Formula" "$f"
    grep -qx "      url \"https://github.com/kividbio/kividb/releases/download/v9.8.7/$3-darwin-aarch64.tar.gz\"" "$f"
    grep -qx "      url \"https://github.com/kividbio/kividb/releases/download/v9.8.7/$3-darwin-x86_64.tar.gz\"" "$f"
    grep -qx "      sha256 \"$a\"" "$f"
    grep -qx "      sha256 \"$b\"" "$f"
    grep -q "^  conflicts_with \"$4\"" "$f"
    grep -qx '  version "9.8.7"' "$f"
}
check_formula "$work/formula-default.rb" Kividb kividb kividb-full
check_formula "$work/formula-full.rb" KividbFull kividb-full kividb
if sh packaging/homebrew/formula.sh full 9.8.7 kividbio/kividb "" "$b" >/dev/null 2>&1; then
    echo "formula.sh took an empty checksum" >&2
    exit 1
fi
docker run --rm -v "$PWD/$work:/w:ro" ruby:3.3-slim sh -c 'ruby -c /w/formula-default.rb && ruby -c /w/formula-full.rb'
echo "ok: the Homebrew formulas render, with the release's urls and checksums"

# ── The distributions ────────────────────────────────────────────────────
failed=()
for distro in "${distros[@]}"; do
    tag="pkgtest-$(echo "$distro" | tr ':.' '--')"
    case "$distro" in
        debian:* | ubuntu:*)
            family=deb
            dockerfile="FROM $distro
RUN apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends systemd systemd-sysv apt-utils procps >/dev/null
CMD [\"/lib/systemd/systemd\"]"
            ;;
        rockylinux:* | amazonlinux:* | fedora:* | almalinux:*)
            family=rpm
            dockerfile="FROM $distro
RUN dnf install -y -q systemd createrepo_c procps-ng findutils >/dev/null && dnf clean all
CMD [\"/usr/lib/systemd/systemd\"]"
            ;;
        *) echo "unknown distribution $distro" >&2; exit 1 ;;
    esac
    echo "== $distro: building $tag"
    printf '%s\n' "$dockerfile" | docker build -q -t "$tag" - >/dev/null
    name="$tag-$$"
    docker run -d --name "$name" --privileged --cgroupns=host \
        -v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /run/lock \
        -v "$PWD/$work/pkgs:/pkgs:ro" -v "$PWD/packaging/test:/pkgtest:ro" \
        "$tag" >/dev/null
    log="$work/$(echo "$distro" | tr ':' '-').log"
    if docker exec "$name" bash /pkgtest/in-container.sh "$family" "${versions[@]}" > "$log" 2>&1; then
        echo "== $distro: PASS ($(grep -c '^ok:' "$log") checks)"
    else
        echo "== $distro: FAIL; the end of $log:"
        tail -n 40 "$log"
        failed+=("$distro")
    fi
    docker rm -f "$name" >/dev/null
done

if [ "${#failed[@]}" -gt 0 ]; then
    echo "failed: ${failed[*]}"
    exit 1
fi
echo "all passed: ${distros[*]}"
