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
set -euo pipefail
export LC_ALL=C

family="$1"
V0="$2" V1="$3" V2="$4" V3="$5"
PASS="swap-test-pass"
AUTH=""
CONF=/etc/kividb/kividb.conf

ok() { echo "ok: $*"; }
fail() {
    echo "FAIL: $*"
    echo "--- systemctl status"
    systemctl status kividb --no-pager -l 2>&1 | tail -n 15 || true
    echo "--- journal"
    journalctl -u kividb --no-pager 2>&1 | tail -n 30 || true
    echo "--- $CONF"
    cat "$CONF" 2>&1 || true
    exit 1
}
step() { echo; echo "=== $*"; }

# ── A RESP client over bash's /dev/tcp ───────────────────────────────────
enc() {
    printf '*%d\r\n' "$#"
    local a
    for a in "$@"; do printf '$%d\r\n%s\r\n' "${#a}" "$a"; done
}
# resp ARG...: one command on a connection of its own (after AUTH when the
# password is set), and QUIT. Prints every reply line, \r removed.
resp() {
    exec 3<>/dev/tcp/127.0.0.1/6380 || return 1
    { if [ -n "$AUTH" ]; then enc AUTH "$AUTH"; fi; enc "$@"; enc QUIT; } >&3
    timeout 10 cat <&3 | tr -d '\r'
    exec 3>&- 3<&-
}
info_field() { resp INFO server | sed -n "s/^$1://p"; }
wait_up() {
    local i
    for i in $(seq 1 60); do
        if resp PING 2>/dev/null | grep -qx '+PONG'; then return 0; fi
        sleep 0.5
    done
    fail "the server does not answer PING"
}

# ── The local repository ─────────────────────────────────────────────────
mkdir -p /repo
if [ "$family" = deb ]; then
    echo "deb [trusted=yes] file:/repo ./" > /etc/apt/sources.list.d/local.list
    ext=deb
else
    printf '[local]\nname=local\nbaseurl=file:///repo\ngpgcheck=0\nenabled=1\nmetadata_expire=0\n' \
        > /etc/yum.repos.d/local.repo
    ext=rpm
fi
publish() { # publish VERSION
    cp /pkgs/"$1"/*."$ext" /repo/
    if [ "$family" = deb ]; then
        (cd /repo && apt-ftparchive packages . > Packages)
        apt-get update -q >/dev/null
    else
        createrepo_c -q /repo
        dnf -q makecache --repo local >/dev/null
    fi
    ok "published $1"
}

# apt's answers to an edited config whose default changed: keep it, and put
# the new default beside it (.dpkg-dist), with no prompt. A person at a
# terminal is asked instead. Only the steps where the default changes use
# them: a swap with the same default must not ask at all.
APT_PLAIN=(env DEBIAN_FRONTEND=noninteractive apt-get -y -q)
APT=("${APT_PLAIN[@]}" -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold)

install_pkg() { # install_pkg NAME[=VERSION]: the first install
    if [ "$family" = deb ]; then "${APT_PLAIN[@]}" install "$1" </dev/null; else dnf -y -q install "$1"; fi
}
# swap_to NAME [new-default]: the commands docs/INSTALL.md gives
swap_to() {
    local other=kividb-full
    [ "$1" = kividb-full ] && other=kividb
    if [ "$family" = deb ] && [ "${2:-}" = new-default ]; then
        "${APT[@]}" install "$1" </dev/null
    elif [ "$family" = deb ]; then
        "${APT_PLAIN[@]}" install "$1" </dev/null
    else
        dnf -y -q swap "$other" "$1"
    fi
}
upgrade_all() {
    if [ "$family" = deb ]; then "${APT[@]}" upgrade </dev/null; else dnf -y -q upgrade; fi
}

installed_version() { # installed_version NAME: empty when not installed
    if [ "$family" = deb ]; then
        dpkg-query -W -f='${db:Status-Abbrev} ${Version}\n' "$1" 2>/dev/null |
            awk '$1 == "ii" { print $2 }'
    else
        rpm -q --qf '%{VERSION}\n' "$1" 2>/dev/null | grep -v 'not installed' || true
    fi
}

# check VARIANT VERSION: the state every step ends in
check() {
    local variant="$1" version="$2" name=kividb other=kividb-full
    if [ "$variant" = full ]; then name=kividb-full; other=kividb; fi
    [ "$(installed_version "$name")" = "$version" ] ||
        fail "$name $version is not installed (installed: '$(installed_version "$name")')"
    [ -z "$(installed_version "$other")" ] || fail "$other is installed too"
    ok "$name $version installed, $other not"
    kividb --version | grep -q "(${variant} build" || fail "kividb --version: $(kividb --version)"
    ok "kividb --version: $(kividb --version)"
    systemctl -q is-enabled kividb || fail "kividb.service is not enabled"
    systemctl -q is-active kividb || fail "kividb.service is not active"
    wait_up
    local pid exe
    pid="$(systemctl show -p MainPID --value kividb)"
    exe="$(readlink "/proc/$pid/exe")"
    [ "$exe" = /usr/bin/kividb ] || fail "the server runs '$exe', not /usr/bin/kividb (an old binary?)"
    ok "kividb.service active and enabled, running /usr/bin/kividb (pid $pid)"
    [ "$(info_field kividb_variant)" = "$variant" ] || fail "INFO kividb_variant: $(info_field kividb_variant)"
    ok "INFO server: kividb_variant:$variant"
    echo "/etc/kividb: $(ls -A /etc/kividb | tr '\n' ' ')"
    if [ -n "$AUTH" ]; then
        grep -qx "requirepass $PASS" "$CONF" || fail "the edited config was not kept"
        grep -qx "appendonly yes" "$CONF" || fail "the edited config was not kept"
        ok "the edited config is kept"
        [ "$(stat -c '%U:%G %a' "$CONF")" = "kividb:kividb 640" ] ||
            fail "$CONF is $(stat -c '%U:%G %a' "$CONF")"
        ok "$CONF is kividb:kividb 640"
    fi
}
check_data() { # check_data KEY...: each holds "v-<key>"
    local k
    for k in "$@"; do
        resp GET "$k" | grep -qx "v-$k" || fail "GET $k: $(resp GET "$k" | tr '\n' ' ')"
    done
    ok "data there: $*"
}
run_id() { info_field run_id; }
check_restarted() { # check_restarted OLD_RUN_ID
    [ "$(run_id)" != "$1" ] || fail "the server was not restarted"
    ok "restarted (run_id changed)"
}
check_new_default_beside() {
    if [ "$family" = deb ]; then
        [ -f "$CONF.dpkg-dist" ] || fail "no $CONF.dpkg-dist: $(ls /etc/kividb)"
        grep -q "swap test: default config of version $1" "$CONF.dpkg-dist" || fail "$CONF.dpkg-dist is not the new default"
        rm -f "$CONF.dpkg-dist"
    else
        [ -f "$CONF.rpmnew" ] || fail "no $CONF.rpmnew: $(ls /etc/kividb)"
        grep -q "swap test: default config of version $1" "$CONF.rpmnew" || fail "$CONF.rpmnew is not the new default"
        rm -f "$CONF.rpmnew"
    fi
    ok "the new default config is beside the edited one"
}
LIB='#!lua name=swaplib
redis.register_function("swapfn", function(keys, args) return "from-swaplib" end)'
check_function_runs() {
    resp FCALL swapfn 0 | grep -qx from-swaplib || fail "FCALL swapfn: $(resp FCALL swapfn 0 | tr '\n' ' ')"
    ok "FCALL runs the stored library"
}

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

echo
echo "PASSED"
