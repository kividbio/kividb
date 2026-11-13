#!/bin/sh
# The same script for `kividb` and `kividb-full`, which install the same
# unit; see docs/INSTALL.md, "Switching between kividb and kividb-full".
set -e

# The unit as it was before the other package was removed for this one
# (preremove.sh wrote the state): enabled again if it was, and started on
# this package's binary if it was running. posttrans.sh has a copy.
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

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true

    # apt removes the other package (its prerm) before this one is
    # configured, so the state is already here: a swap, or a reinstall after
    # `apt remove`.
    if [ -e /run/kividb-unit-state ]; then
        restore_unit_state
        exit 0
    fi

    # No unit before this package (see preinstall): enable and start it,
    # whatever the package manager calls this.
    if [ -e /run/kividb-enable-after-install ]; then
        rm -f /run/kividb-enable-after-install
        systemctl enable --now kividb.service >/dev/null 2>&1 ||
            echo "kividb: installed but did not start; see: journalctl -u kividb" >&2
        exit 0
    fi

    case "$1" in
        configure)
            # deb: $2 is the version being upgraded from, and empty on a first
            # install.
            if [ -n "$2" ]; then
                systemctl try-restart kividb.service >/dev/null 2>&1 ||
                    echo "kividb: upgraded but did not restart; see: journalctl -u kividb" >&2
            else
                systemctl enable --now kividb.service >/dev/null 2>&1 ||
                    echo "kividb: installed but did not start; see: journalctl -u kividb" >&2
            fi
            ;;
        2)
            # rpm: 2 means an upgrade, 1 a first install.
            systemctl try-restart kividb.service >/dev/null 2>&1 ||
                echo "kividb: upgraded but did not restart; see: journalctl -u kividb" >&2
            ;;
        1)
            # rpm: a first install of this package, and the unit was already
            # there (preinstall left no note): the other package is still
            # installed, and `dnf swap` removes it in this transaction. Its
            # %preun stops the unit after this script; posttrans.sh starts
            # it again, on this binary.
            ;;
        *)
            systemctl enable --now kividb.service >/dev/null 2>&1 ||
                echo "kividb: installed but did not start; see: journalctl -u kividb" >&2
            ;;
    esac
fi

exit 0
