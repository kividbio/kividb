#!/bin/sh
# The same script for `kividb` and `kividb-full`, which install the same
# unit; see docs/INSTALL.md, "Switching between kividb and kividb-full".
set -e

# The unit as it was before the other package was removed for this one
# (preremove.sh or preinstall.sh wrote the note): enabled again if it was,
# and started on this package's binary if it was running. posttrans.sh has
# a copy.
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

enable_now() {
    rm -f /run/kividb-enable-after-install
    systemctl enable --now kividb.service >/dev/null 2>&1 ||
        echo "kividb: installed but did not start; see: journalctl -u kividb" >&2
}

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true

    case "$1" in
        configure)
            # deb. apt removes the other package (its prerm writes the note)
            # before this one is configured: a swap, or a reinstall after
            # `apt remove`.
            if [ -e /run/kividb-unit-state ]; then
                restore_unit_state
            # No unit before this package (see preinstall): a first install,
            # or one after `apt remove`, whatever dpkg calls it.
            elif [ -e /run/kividb-enable-after-install ]; then
                enable_now
            # $2 is the version being upgraded from, and empty on a first
            # install.
            elif [ -n "$2" ]; then
                systemctl try-restart kividb.service >/dev/null 2>&1 ||
                    echo "kividb: upgraded but did not restart; see: journalctl -u kividb" >&2
            else
                enable_now
            fi
            ;;
        1)
            # rpm, a first install of this package. With a note, the other
            # package is being swapped out in this transaction (preinstall
            # wrote it): its %preun stops the unit after this script, and
            # posttrans.sh puts the unit back as it was, on this binary.
            if [ ! -e /run/kividb-unit-state ]; then
                enable_now
            fi
            ;;
        2)
            # rpm, an upgrade. From a package with no unit (1.1.0 and
            # before), the unit is new: enable and start it.
            if [ -e /run/kividb-enable-after-install ]; then
                enable_now
            else
                systemctl try-restart kividb.service >/dev/null 2>&1 ||
                    echo "kividb: upgraded but did not restart; see: journalctl -u kividb" >&2
            fi
            ;;
        *)
            enable_now
            ;;
    esac
fi

exit 0
