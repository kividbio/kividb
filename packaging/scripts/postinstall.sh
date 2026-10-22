#!/bin/sh
set -e

if [ -d /run/systemd/system ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true

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
        *)
            systemctl enable --now kividb.service >/dev/null 2>&1 ||
                echo "kividb: installed but did not start; see: journalctl -u kividb" >&2
            ;;
    esac
fi

exit 0
