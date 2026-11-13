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
# Four versions of each package are built: the version in Cargo.toml (or
# 1.3.0, if that is older) and three more with the patch number raised, each
# with one more comment line in the default kividb.conf, so that an upgrade
# or a swap meets an edited config and a new default.
# packaging/test/in-container.sh says what is checked at each step, and
# packaging/test/from-1.2.sh what is checked from a released 1.2.x package.
#
# Environment:
#   OLD_RELEASE  the release whose kividb package from-1.2.sh starts from
#                (default v1.2.2; "none" to skip that part), downloaded from
#                github.com/kividbio/kividb
#   SHIPPED_DIR  a directory with a release's own kividb_amd64.deb,
#                kividb-full_amd64.deb, kividb.x86_64.rpm and
#                kividb-full.x86_64.rpm, with SHIPPED_VERSION: their fields
#                are checked, and they are installed and swapped once, in
#                debian:12 and rockylinux:9 (the release workflow sets it)
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
# Above any 1.2.x, so that from-1.2.sh upgrades from the released package.
if [ "$(printf '%s\n' "$base" 1.3.0 | sort -V | head -n 1)" != 1.3.0 ]; then base=1.3.0; fi
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

# ── What each package says about the other ───────────────────────────────
retry() { # retry COMMAND...: up to three times, for the network
    local t
    for t in 1 2 3; do
        "$@" && return 0
        echo "attempt $t failed: $*" >&2
        sleep $((t * 10))
    done
    return 1
}
# check_fields DIR DEB_DEFAULT DEB_FULL RPM_DEFAULT RPM_FULL VERSION
check_fields() {
    # nfpm writes a pre-release 1.3.0-rc1 as 1.3.0~rc1 (nfpm-config.sh).
    local dir="$1" v="${6/-/\~}" got want
    got="$(retry docker run --rm -v "$dir:/p:ro" debian:12 sh -c \
        "dpkg-deb -f /p/$2 Package Provides Conflicts Replaces; echo ---; dpkg-deb -f /p/$3 Package Provides Conflicts Replaces")"
    want="Package: kividb
Conflicts: kividb-full
Replaces: kividb-full
---
Package: kividb-full
Provides: kividb (= $v)
Conflicts: kividb
Replaces: kividb"
    if [ "$got" != "$want" ]; then
        printf 'deb fields of %s and %s:\n%s\nwanted:\n%s\n' "$2" "$3" "$got" "$want" >&2
        return 1
    fi
    got="$(retry docker run --rm -v "$dir:/p:ro" rockylinux:9 sh -c "
        for f in /p/$4 /p/$5; do
            echo \"name: \$(rpm -qp --nosignature --qf '%{NAME}' \$f)\"
            echo \"conflicts:\$(rpm -qp --nosignature --conflicts \$f | sed 's/^/ /' | tr -d '\\n')\"
            echo \"obsoletes:\$(rpm -qp --nosignature --obsoletes \$f | sed 's/^/ /' | tr -d '\\n')\"
            rpm -qp --nosignature --provides \$f | grep -qx 'kividb = $v' && echo 'provides: kividb = $v' || true
        done")"
    want="name: kividb
conflicts: kividb-full
obsoletes:
name: kividb-full
conflicts: kividb
obsoletes:
provides: kividb = $v"
    # No Obsoletes: with one, dnf would trade one package for the other on
    # every upgrade.
    if [ "$got" != "$want" ]; then
        printf 'rpm fields of %s and %s:\n%s\nwanted:\n%s\n' "$4" "$5" "$got" "$want" >&2
        return 1
    fi
    echo "ok: the Provides, Conflicts, Replaces (deb) and Obsoletes (rpm, none) of $2, $3, $4 and $5"
}
v0="${versions[0]}"
check_fields "$PWD/$work/pkgs/$v0" "kividb_${v0}_amd64.deb" "kividb-full_${v0}_amd64.deb" \
    "kividb-${v0}-1.x86_64.rpm" "kividb-full-${v0}-1.x86_64.rpm" "$v0"
if [ -n "${SHIPPED_DIR:-}" ]; then
    shipped="$(realpath "$SHIPPED_DIR")"
    check_fields "$shipped" kividb_amd64.deb kividb-full_amd64.deb kividb.x86_64.rpm kividb-full.x86_64.rpm \
        "${SHIPPED_VERSION:?set SHIPPED_VERSION with SHIPPED_DIR}"
fi

# ── The released 1.2.x package from-1.2.sh starts from ───────────────────
OLD_RELEASE="${OLD_RELEASE:-v1.2.2}"
if [ "$OLD_RELEASE" != none ]; then
    mkdir -p "$work/old"
    for f in kividb_amd64.deb kividb.x86_64.rpm; do
        retry curl -fsSL -o "$work/old/$f" "https://github.com/kividbio/kividb/releases/download/$OLD_RELEASE/$f"
    done
fi

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
retry docker run --rm -v "$PWD/$work:/w:ro" ruby:3.3-slim sh -c 'ruby -c /w/formula-default.rb && ruby -c /w/formula-full.rb'
echo "ok: the Homebrew formulas render, with the release's urls and checksums"

# ── The distributions ────────────────────────────────────────────────────
# run_in TAG LOG SCRIPT ARG...: SCRIPT in a new container of TAG, once
# systemd is up. Prints PASS or FAIL; fails with the script.
run_in() {
    local tag="$1" log="$2" name t state
    shift 2
    name="$tag-$$-$RANDOM"
    docker run -d --name "$name" --privileged --cgroupns=host \
        -v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /run/lock \
        -v "$PWD/$work/pkgs:/pkgs:ro" -v "$PWD/$work/old:/old:ro" \
        -v "$PWD/packaging/test:/pkgtest:ro" ${shipped:+-v "$shipped:/shipped:ro"} \
        "$tag" >/dev/null
    # "degraded": a unit a container cannot run failed, which is no matter.
    state="$(docker exec "$name" timeout 120 systemctl is-system-running --wait 2>/dev/null || true)"
    case "$state" in
        running | degraded) ;;
        *) echo "systemd in $tag is '$state'" > "$log" ;;
    esac
    local rc=0
    docker exec "$name" bash "$@" >> "$log" 2>&1 || rc=$?
    docker rm -f "$name" >/dev/null
    if [ "$rc" = 0 ]; then
        echo "   PASS: $(basename "$log") ($(grep -c '^ok:' "$log") checks)"
    else
        echo "   FAIL: $(basename "$log"), the end of it:"
        tail -n 40 "$log"
    fi
    return "$rc"
}

mkdir -p "$work/old"
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
    if ! printf '%s\n' "$dockerfile" | retry docker build -q -t "$tag" - >/dev/null; then
        echo "   FAIL: cannot build $tag"
        failed+=("$distro")
        continue
    fi
    base_log="$work/$(echo "$distro" | tr ':' '-')"
    run_in "$tag" "$base_log.log" /pkgtest/in-container.sh "$family" "${versions[@]}" || failed+=("$distro")
    if [ "$OLD_RELEASE" != none ]; then
        old=/old/kividb_amd64.deb
        [ "$family" = rpm ] && old=/old/kividb.x86_64.rpm
        run_in "$tag" "$base_log-from-$OLD_RELEASE.log" /pkgtest/from-1.2.sh "$family" "${versions[3]}" "$old" ||
            failed+=("$distro from $OLD_RELEASE")
    fi
    if [ -n "${shipped:-}" ] && { [ "$distro" = debian:12 ] || [ "$distro" = rockylinux:9 ]; }; then
        run_in "$tag" "$base_log-shipped.log" /pkgtest/shipped.sh "$family" || failed+=("$distro shipped")
    fi
done

if [ "${#failed[@]}" -gt 0 ]; then
    echo "failed: ${failed[*]}"
    exit 1
fi
echo "all passed: ${distros[*]}"
