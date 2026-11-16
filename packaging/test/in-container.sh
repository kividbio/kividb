#!/bin/bash
# Run by swap-test.sh inside a container running systemd:
#
#   in-container.sh deb|rpm V0 V1 V2 V3
#
# /pkgs/<version>/ holds that version's kividb and kividb-full packages; a
# version is added to the local repository (/repo) when a step says so, as a
# release adds it to packages.kividb.io. The steps:
#
#   A  install kividb V0; set a password and appendonly in the config; write
#   B  swap to kividb-full (V0): config kept, data there, restarted on the
#      new binary, EVAL works, FUNCTION LOAD stores a library
#   C  swap back to kividb: config kept, data there, running; FCALL refused;
#      the library kept through the default build's own saves
#   D  V1 published: the upgrade stays on kividb; the edited config is kept
#      and the new default lands beside it
#   E  V2 published: swap to kividb-full V2; the library is there again
#   F  V3 published: the upgrade stays on kividb-full
#   G  swap back to kividb V3
#   H  stop and disable the unit, swap to kividb-full: it stays so
#   I  swap to kividb, FUNCTION FLUSH, swap back: the library is gone
set -euo pipefail

family="$1"
V0="$2" V1="$3" V2="$4" V3="$5"
# shellcheck source=packaging/test/lib.sh
source "$(dirname "$0")/lib.sh"

# ── A ────────────────────────────────────────────────────────────────────
step "A: install kividb $V0"
publish "$V0"
install_pkg kividb
check default "$V0"
resp SET a v-a | grep -qx '+OK' || fail "SET a"
printf 'requirepass %s\nappendonly yes\n' "$PASS" >> "$CONF"
systemctl restart kividb
AUTH="$PASS"
wait_up
resp SET k1 v-k1 | grep -qx '+OK' || fail "SET k1"
resp EVAL "return 1" 0 | grep -q '^-ERR this build of KiviDB has no Lua' || fail "EVAL on the default build"
ok "EVAL is refused on the default build"
check default "$V0"
check_data a k1

# ── B ────────────────────────────────────────────────────────────────────
step "B: swap to kividb-full $V0"
before="$(run_id)"
swap_to kividb-full
check full "$V0"
check_restarted "$before"
check_data a k1
resp EVAL "return 1" 0 | grep -qx ':1' || fail "EVAL: $(resp EVAL 'return 1' 0 | tr '\n' ' ')"
ok "EVAL works"
resp FUNCTION LOAD "$LIB" | grep -qx swaplib || fail "FUNCTION LOAD: $(resp FUNCTION LOAD "$LIB" | tr '\n' ' ')"
ok "FUNCTION LOAD stored swaplib"
check_function_runs
resp SET k2 v-k2 | grep -qx '+OK' || fail "SET k2"
if [ "$family" = deb ]; then
    # The package swapped out is left with its config (apt's "rc"); purging
    # it must not take the config the other package now uses.
    dpkg-query -W -f='${db:Status-Abbrev}\n' kividb | grep -q '^rc' ||
        fail "kividb is not left as rc: $(dpkg-query -W -f='${db:Status-Abbrev}' kividb)"
    ok "dpkg -l shows the removed kividb as rc"
    dpkg --purge kividb
    [ -f "$CONF" ] || fail "purging kividb removed $CONF, which kividb-full uses"
    ok "purging the removed kividb leaves the config"
fi

# ── C ────────────────────────────────────────────────────────────────────
step "C: swap back to kividb $V0, with a library stored"
before="$(run_id)"
swap_to kividb
check default "$V0"
check_restarted "$before"
check_data a k1 k2
resp FCALL swapfn 0 | grep -q '^-ERR this build of KiviDB has no Lua' || fail "FCALL on the default build"
ok "FCALL is refused on the default build, and the server runs"
journalctl -u kividb --no-pager | grep -q "function libraries swaplib are not loaded" ||
    fail "no warning about the libraries in the journal"
ok "the journal says swaplib is kept but not loaded"
# The default build's own snapshot, then a start from it and from the AOF.
resp SAVE | grep -qx '+OK' || fail "SAVE"
resp SET k3 v-k3 | grep -qx '+OK' || fail "SET k3"
systemctl restart kividb
wait_up
check_data a k1 k2 k3

# ── D ────────────────────────────────────────────────────────────────────
step "D: kividb $V1 published; the upgrade stays on kividb"
publish "$V1"
before="$(run_id)"
upgrade_all
check default "$V1"
check_restarted "$before"
check_new_default_beside 1
check_data a k1 k2 k3

# ── E ────────────────────────────────────────────────────────────────────
step "E: $V2 published; swap to kividb-full $V2"
publish "$V2"
before="$(run_id)"
swap_to kividb-full new-default
check full "$V2"
check_restarted "$before"
check_new_default_beside 2
check_data a k1 k2 k3
check_function_runs
ok "the library stored on kividb-full came back through kividb"

# ── F ────────────────────────────────────────────────────────────────────
step "F: $V3 published; the upgrade stays on kividb-full"
publish "$V3"
before="$(run_id)"
upgrade_all
check full "$V3"
check_restarted "$before"
check_new_default_beside 3
check_data a k1 k2 k3
check_function_runs

# ── G ────────────────────────────────────────────────────────────────────
step "G: swap back to kividb $V3"
before="$(run_id)"
swap_to kividb
check default "$V3"
check_restarted "$before"
check_data a k1 k2 k3

# ── H ────────────────────────────────────────────────────────────────────
step "H: a unit stopped and disabled stays so through a swap"
systemctl disable --now kividb >/dev/null 2>&1
swap_to kividb-full
[ "$(installed_version kividb-full)" = "$V3" ] || fail "kividb-full $V3 is not installed"
! systemctl -q is-active kividb || fail "the swap started a unit that was stopped"
! systemctl -q is-enabled kividb || fail "the swap enabled a unit that was disabled"
ok "still stopped and disabled after the swap"
systemctl enable --now kividb >/dev/null 2>&1
check full "$V3"
check_data a k1 k2 k3
check_function_runs

# -- I ---------------------------------------------------------------------
step "I: FUNCTION FLUSH on kividb drops the libraries it kept"
swap_to kividb
check default "$V3"
resp FUNCTION FLUSH | grep -qx '+OK' || fail "FUNCTION FLUSH: $(resp FUNCTION FLUSH | tr '\n' ' ')"
ok "FUNCTION FLUSH answers OK on the default build"
systemctl restart kividb
wait_up
swap_to kividb-full
check full "$V3"
resp FUNCTION LIST | grep -qx '[*]0' || fail "FUNCTION LIST: $(resp FUNCTION LIST | tr '\n' ' ')"
ok "kividb-full has no libraries after the flush on kividb"
check_data a k1 k2 k3

echo
echo "PASSED"
