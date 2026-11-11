#!/bin/bash
# As run on the test VM: clones and venvs under ~/fw (see the setup notes in the docs page).
# SHUTDOWN NOSAVE on KiviDB 1.2.0 (port 7011) and Redis 8.10.2 (port 7012), raw and through redis-py.
P=~/fw/litellm/.venv/bin/python
mkdir -p /tmp/s1 /tmp/s2
st() {
  (cd /tmp/s1; ~/fw/bin/kividb-v1.2.0 --port 7011 --protected-mode no >/tmp/s1.log 2>&1 &)
  (cd /tmp/s2; ~/fw/bin/redis-server-8.10.2 --port 7012 --save "" >/tmp/s2.log 2>&1 &)
  sleep 2
}
st
$P ~/fw/repro_shutdown.py raw 7011 "SHUTDOWN NOSAVE"
$P ~/fw/repro_shutdown.py raw 7012 "SHUTDOWN NOSAVE"
sleep 1
st
$P ~/fw/repro_shutdown.py py 7011
$P ~/fw/repro_shutdown.py py 7012
sleep 1
st
$P ~/fw/repro_shutdown.py raw 7011 "SHUTDOWN ABORT"
$P ~/fw/repro_shutdown.py raw 7012 "SHUTDOWN ABORT"
$P ~/fw/repro_shutdown.py raw 7011 "SHUTDOWN"
$P ~/fw/repro_shutdown.py raw 7012 "SHUTDOWN"
sleep 1
pgrep -af "port 701[12]" || echo "both exited"
