#!/bin/sh
# rpm only: runs once the whole transaction is done. In `dnf swap kividb
# kividb-full` (or back), the package going away stops the unit in its
# %preun, which runs after this package's %post; preremove.sh noted how the
# unit was, and it is put back here, on this package's binary. The same
# script for `kividb` and `kividb-full`; postinstall.sh has a copy of
# restore_unit_state.
set -e

restore_unit_state() {
    state="$(cat /run/kividb-unit-state 2>/dev/null || true)"
    rm -f /run/kividb-unit-state /run/kividb-enable-after-install
    case "$state" in
        *enabled=yes*) systemctl enable kividb.service >/dev/null 2>&1 || true ;;
    esac
    case "$state" in
        *active=yes*)
            systemctl restart kividb.service >/dev/null 2>&1 ||
                echo "kividb: installed but did not start; see: journalctl -u kividb" >&2
            ;;
    esac
}

if [ -d /run/systemd/system ] && [ -e /run/kividb-unit-state ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    restore_unit_state
fi

exit 0
