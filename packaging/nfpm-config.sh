#!/bin/sh
# Writes the nfpm configuration of one package to stdout, from the template
# packaging/nfpm.yaml:
#
#   nfpm-config.sh VARIANT VERSION ARCH [BINARY]
#
# VARIANT  default (the `kividb` package: no Lua, no TLS) or full (the
#          `kividb-full` package: Lua and TLS, built with --features lua,tls)
# VERSION  the package version, e.g. 1.3.0
# ARCH     amd64 or arm64 (nfpm names it as rpm wants for an .rpm)
# BINARY   the kividb binary to package (default ./target/release/kividb)
#
# Run from the repository's root, where the template's paths start. The two
# packages install the same files and conflict with each other: `kividb-full`
# also provides `kividb`, so anything that needs a KiviDB server takes either.
# See docs/INSTALL.md, "Switching between kividb and kividb-full".
set -eu

variant="${1:?usage: nfpm-config.sh default|full VERSION ARCH [BINARY]}"
version="${2:?usage: nfpm-config.sh default|full VERSION ARCH [BINARY]}"
arch="${3:?usage: nfpm-config.sh default|full VERSION ARCH [BINARY]}"
binary="${4:-./target/release/kividb}"
template="packaging/nfpm.yaml"

case "$version" in '' | *[!0-9A-Za-z.+~-]*) echo "bad version: $version" >&2; exit 1 ;; esac
case "$arch" in amd64 | arm64) ;; *) echo "bad arch: $arch" >&2; exit 1 ;; esac
case "$binary" in *'|'* | *'&'* | *'\'*) echo "bad binary path: $binary" >&2; exit 1 ;; esac
# The version as nfpm writes it into the packages: a pre-release such as
# 1.3.0-rc1 becomes 1.3.0~rc1, which sorts before 1.3.0. What kividb-full
# provides must be the same.
pkg_version="$(printf '%s' "$version" | sed 's/-/~/')"

common() {
    sed -e "s/^version:.*/version: ${version}/" \
        -e "s/^arch:.*/arch: ${arch}/" \
        -e "s|^  - src: ./target/release/kividb\$|  - src: ${binary}|" \
        "$template"
}

case "$variant" in
    default)
        out="$(common)"
        ;;
    full)
        # The name, the description, the package it conflicts with and
        # replaces, and what it provides: the rest is the same.
        out="$(common | sed \
            -e 's/^name: kividb$/name: kividb-full/' \
            -e 's/^description: "\(.*\)"$/description: "\1 The full build, with Lua scripting (EVAL, FUNCTION) and TLS."/' \
            -e 's/^\( *- \)kividb-full$/\1kividb/' |
            awk -v v="$pkg_version" '
                { print }
                $0 == "  deb:" { print "    provides:"; print "      - kividb (= " v ")" }
                $0 == "  rpm:" { print "    provides:"; print "      - kividb = " v }
            ')"
        ;;
    *)
        echo "variant must be default or full, not $variant" >&2
        exit 1
        ;;
esac

# Every edit must have found its line: a template changed under this script
# would otherwise give a package that installs beside the other one.
check() { # check PATTERN COUNT
    n="$(printf '%s\n' "$out" | grep -c -x -e "$1" || true)"
    if [ "$n" != "$2" ]; then
        echo "nfpm-config.sh: expected $2 line(s) matching '$1' in the $variant configuration, found $n" >&2
        exit 1
    fi
}
check "version: ${version}" 1
check "arch: ${arch}" 1
check "  - src: ${binary}" 1
check "    posttrans: ./packaging/scripts/posttrans.sh" 1
if [ "$variant" = full ]; then
    check "name: kividb-full" 1
    check " *- kividb" 2
    check " *- kividb-full" 0
    check "      - kividb (= ${pkg_version})" 1
    check "      - kividb = ${pkg_version}" 1
else
    check "name: kividb" 1
    check " *- kividb-full" 2
fi

printf '%s\n' "$out"
