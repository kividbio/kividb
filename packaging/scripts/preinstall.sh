#!/bin/sh
# The service user has to exist before the package manager unpacks
# /var/lib/kividb with this owner, so it is created here and not in postinstall.
set -e

if ! getent group kividb >/dev/null; then
    groupadd --system kividb
fi

if ! getent passwd kividb >/dev/null; then
    # /usr/sbin/nologin is where Debian and RHEL both keep it (RHEL's /sbin is
    # a symlink). adduser(8) is Debian-only, so this uses useradd(8).
    useradd --system --gid kividb --home-dir /var/lib/kividb \
            --no-create-home --shell /usr/sbin/nologin --comment "KiviDB" kividb
fi

# Whether the unit is to be enabled and started once the package is in:
# when this package's unit is not on disk yet. That is a first install, an
# upgrade from a package that had no unit (1.1.0 and before were the binary
# only), and on Debian an install after `apt remove` (dpkg then reports it as
# an upgrade from the removed version, whose unit was disabled and deleted).
# An upgrade from a package with the unit only restarts it, so a unit the
# administrator disabled stays disabled. The note is read by postinstall.
if [ -d /run/systemd/system ] && [ ! -e /usr/lib/systemd/system/kividb.service ] &&
    [ ! -e /lib/systemd/system/kividb.service ]; then
    touch /run/kividb-enable-after-install
fi

# How the unit is, for the swap between `kividb` and `kividb-full` (see
# preremove.sh, which writes the same note, and posttrans.sh, which reads
# it). With dnf, this package's %pre runs while the other package is still
# installed, and the other's %preun runs only after this one's %post: a
# %preun of 1.3.0 or later writes the note again, but that of 1.2.x stops
# and disables the unit and writes nothing, so it is written here first.
# Not for deb, where the other package's prerm has already stopped the unit
# when this runs. An upgrade is no swap: a note left from an earlier one
# (a swap back to 1.2.x, which reads none) is stale, and dropped.
if [ -d /run/systemd/system ]; then
    case "$1" in
        1)
            if [ -e /usr/lib/systemd/system/kividb.service ] || [ -e /lib/systemd/system/kividb.service ]; then
                enabled=no
                active=no
                if systemctl -q is-enabled kividb.service >/dev/null 2>&1; then enabled=yes; fi
                if systemctl -q is-active kividb.service >/dev/null 2>&1; then active=yes; fi
                echo "enabled=$enabled active=$active" > /run/kividb-unit-state || true
            fi
            ;;
        2 | upgrade)
            rm -f /run/kividb-unit-state
            ;;
    esac
fi

exit 0
