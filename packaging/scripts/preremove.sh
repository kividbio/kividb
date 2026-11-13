#!/bin/sh
# Stop the unit only when the package is going away, never on upgrade.
set -e

case "$1" in
    0|remove|purge)
        if [ -d /run/systemd/system ]; then
            # How the unit was, for the package that installs it next. When
            # `kividb` and `kividb-full` are swapped for each other (both
            # install kividb.service), the one going away stops the unit
            # here, and the one coming in starts it again as it was, on its
            # own binary: postinstall.sh with apt, posttrans.sh with dnf,
            # which runs this %preun after the new package's %post. In /run,
            # so a removal with nothing after it leaves nothing past the
            # next boot.
            enabled=no
            active=no
            if systemctl -q is-enabled kividb.service >/dev/null 2>&1; then enabled=yes; fi
            if systemctl -q is-active kividb.service >/dev/null 2>&1; then active=yes; fi
            echo "enabled=$enabled active=$active" > /run/kividb-unit-state || true
            systemctl stop kividb.service >/dev/null 2>&1 || true
            systemctl disable kividb.service >/dev/null 2>&1 || true
        fi
        ;;
esac

# The kividb user and /var/lib/kividb survive a removal: the data is not the
# package's to delete.
exit 0
