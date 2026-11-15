#!/bin/bash
# Run by swap-test.sh inside a container running systemd, with SHIPPED_DIR
# mounted at /shipped: the release's own packages, installed and swapped
# once, as a user given the files would.
#
#   shipped.sh deb|rpm
set -euo pipefail

family="$1"
# shellcheck source=packaging/test/lib.sh
source "$(dirname "$0")/lib.sh"
# Only the files given: no local repository here.
rm -f /etc/yum.repos.d/local.repo /etc/apt/sources.list.d/local.list

expect() { # expect VARIANT
    systemctl -q is-enabled kividb || fail "kividb.service is not enabled"
    systemctl -q is-active kividb || fail "kividb.service is not active"
    wait_up
    [ "$(info_field kividb_variant)" = "$1" ] || fail "INFO kividb_variant: $(info_field kividb_variant)"
    kividb --version | grep -q "($1 build" || fail "kividb --version: $(kividb --version)"
    ok "$(kividb --version): running and enabled"
}

step "the release's kividb, then its kividb-full"
if [ "$family" = deb ]; then
    "${APT_PLAIN[@]}" install /shipped/kividb_amd64.deb </dev/null
    expect default
    resp SET s v-s | grep -qx '+OK' || fail "SET s"
    "${APT_PLAIN[@]}" install /shipped/kividb-full_amd64.deb </dev/null
else
    dnf -y -q install /shipped/kividb.x86_64.rpm
    expect default
    resp SET s v-s | grep -qx '+OK' || fail "SET s"
    dnf -y -q swap kividb /shipped/kividb-full.x86_64.rpm
fi
[ -z "$(installed_version kividb)" ] || fail "kividb is still installed"
expect full
check_data s
resp EVAL "return 1" 0 | grep -qx ':1' || fail "EVAL: $(resp EVAL 'return 1' 0 | tr '\n' ' ')"
ok "EVAL works"

echo
echo "PASSED"
