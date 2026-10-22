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

exit 0
