"""cli.py PORT PASSWORD CMD... [;; CMD...]: the commands, on one connection,
their replies on stdout (an array one element a line), exit 1 on an error
reply or no connection. run.sh's redis-cli, so that the runner needs none.
On one connection, `SET k v ;; WAIT 1 5000` waits for that SET."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "benchmarks", "ci"))
from resp import Conn, Err  # noqa: E402

port, password, *args = sys.argv[1:]
cmds = [[]]
for a in args:
    if a == ";;":
        cmds.append([])
    else:
        cmds[-1].append(a)
try:
    # WAIT takes up to its own timeout (run.sh gives 5 s).
    waits = any(cmd and cmd[0].upper() == "WAIT" for cmd in cmds)
    c = Conn(int(port), timeout=7.0 if waits else 2.0)
    if password:
        c.cmd("AUTH", password)
    replies = [c.cmd(*cmd) for cmd in cmds]
except OSError as e:
    sys.exit(f"127.0.0.1:{port}: {e}")


def show(v):
    if isinstance(v, list):
        for x in v:
            show(x)
    elif v is not None:
        print(v)


for reply in replies:
    show(reply)
sys.exit(1 if any(isinstance(r, Err) for r in replies) else 0)
