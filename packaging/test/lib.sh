#!/bin/bash
# Helpers for in-container.sh and from-1.2.sh, sourced inside a container
# running systemd. Set `family` (deb or rpm) before sourcing it.
export LC_ALL=C

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
# password is set), and QUIT. Prints the command's reply lines, \r removed:
# not AUTH's +OK, nor QUIT's.
resp() {
    exec 3<>/dev/tcp/127.0.0.1/6380 || return 1
    { if [ -n "$AUTH" ]; then enc AUTH "$AUTH"; fi; enc "$@"; enc QUIT; } >&3
    timeout 10 cat <&3 | tr -d '\r' | { if [ -n "$AUTH" ]; then sed 1d; else cat; fi; } | sed '$d'
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
