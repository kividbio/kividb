#!/bin/bash
# Run by swap-test.sh inside a container running systemd: from a released
# kividb 1.2.x package, whose maintainer scripts predate kividb-full, to the
# new packages.
#
#   from-1.2.sh deb|rpm VERSION OLD_PACKAGE
#
# VERSION's packages are in /pkgs/VERSION/; OLD_PACKAGE is the released
# kividb .deb or .rpm (1.2.2). Each case starts from a clean install of it:
#
#   1  running and enabled, swap to kividb-full: running and enabled
#   2  stopped and disabled, swap to kividb-full: with dnf, stopped and
#      disabled; with apt, enabled and started (1.2.x's prerm disables the
#      unit before the new package can see it: docs/INSTALL.md says so)
#   3  running and enabled, upgrade to the new kividb: running and enabled
#   4  stopped and disabled, upgrade to the new kividb: stopped and disabled
#   5  the new kividb-full swapped back to 1.2.x, which docs/INSTALL.md says
#      not to do: what it says happens (dnf: stopped; apt: running), and the
#      note left behind is dropped by the next upgrade
set -euo pipefail

family="$1"
VN="$2"
OLD="$3"
# shellcheck source=packaging/test/lib.sh
source "$(dirname "$0")/lib.sh"

publish "$VN"

clean() {
    if [ "$family" = deb ]; then
        env DEBIAN_FRONTEND=noninteractive apt-get -y -q purge kividb kividb-full </dev/null >/dev/null 2>&1 || true
    else
        dnf -y -q remove kividb kividb-full >/dev/null 2>&1 || true
    fi
    rm -rf /var/lib/kividb /etc/kividb /run/kividb-unit-state /run/kividb-enable-after-install
}
install_old() {
    clean
    if [ "$family" = deb ]; then
        "${APT_PLAIN[@]}" install "$OLD" </dev/null >/dev/null
    else
        dnf -y -q install "$OLD"
    fi
    systemctl -q is-active kividb || fail "the old package did not start the unit"
    wait_up
    resp SET old v-old | grep -qx '+OK' || fail "SET old"
    ok "installed $(kividb --version), wrote a key"
}
to_full() {
    if [ "$family" = deb ]; then
        "${APT_PLAIN[@]}" install kividb-full </dev/null
    else
        dnf -y -q swap kividb kividb-full
    fi
}
to_new_kividb() {
    if [ "$family" = deb ]; then
        "${APT_PLAIN[@]}" install kividb </dev/null
    else
        dnf -y -q upgrade kividb
    fi
}
expect_running() { # expect_running VARIANT
    systemctl -q is-enabled kividb || fail "kividb.service is not enabled"
    systemctl -q is-active kividb || fail "kividb.service is not active"
    wait_up
    [ "$(info_field kividb_variant)" = "$1" ] || fail "INFO kividb_variant: $(info_field kividb_variant)"
    check_data old
    ok "running and enabled, $1 build, data there"
}
expect_stopped() { # expect_stopped VARIANT
    ! systemctl -q is-active kividb || fail "the unit was stopped, and is running"
    ! systemctl -q is-enabled kividb || fail "the unit was disabled, and is enabled"
    ok "still stopped and disabled"
    systemctl enable --now kividb >/dev/null 2>&1
    expect_running "$1"
}

step "1: $OLD running, swap to kividb-full $VN"
install_old
to_full
expect_running full

step "2: $OLD stopped and disabled, swap to kividb-full $VN"
install_old
systemctl disable --now kividb >/dev/null 2>&1
to_full
if [ "$family" = deb ]; then
    # 1.2.x's prerm disables the unit and apt then deletes it, before the
    # new package's preinst runs: nothing is left to say it was disabled.
    expect_running full
    ok "(apt, from 1.2.x: enabled and started, as docs/INSTALL.md says)"
else
    expect_stopped full
fi

step "3: $OLD running, upgrade to kividb $VN"
install_old
to_new_kividb
[ "$(installed_version kividb)" = "$VN" ] || fail "kividb $VN is not installed"
expect_running default

step "4: $OLD stopped and disabled, upgrade to kividb $VN"
install_old
systemctl disable --now kividb >/dev/null 2>&1
to_new_kividb
[ "$(installed_version kividb)" = "$VN" ] || fail "kividb $VN is not installed"
expect_stopped default

step "5: the new kividb-full, swapped back to $OLD (not supported: docs/INSTALL.md)"
clean
install_pkg kividb-full
systemctl -q is-active kividb || fail "kividb-full did not start"
if [ "$family" = deb ]; then
    "${APT_PLAIN[@]}" install "$OLD" </dev/null
    # 1.2.x's preinst finds no unit (apt removed kividb-full's) and enables it.
    systemctl -q is-active kividb || fail "after a swap back to 1.2.x with apt, the unit is not running"
    ok "apt, back to 1.2.x: running, as docs/INSTALL.md says"
else
    dnf -y -q swap kividb-full "$OLD"
    ! systemctl -q is-active kividb || fail "after dnf swap back to 1.2.x, the unit runs: fix docs/INSTALL.md"
    ok "dnf, back to 1.2.x: stopped, as docs/INSTALL.md says"
    # The note kividb-full's %preun left is stale; the next upgrade drops it.
    dnf -y -q upgrade kividb
    [ ! -e /run/kividb-unit-state ] || fail "the upgrade left a stale /run/kividb-unit-state"
    ok "the upgrade to $VN dropped the stale note"
fi

echo
echo "PASSED"
