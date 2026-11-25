"""cli.py PORT PASSWORD CMD...: one command, its reply on stdout (an array one
element a line), exit 1 on an error reply or no connection. run.sh's
redis-cli, so that the runner needs none."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "benchmarks", "ci"))
from resp import Conn, Err  # noqa: E402

port, password, *cmd = sys.argv[1:]
try:
    c = Conn(int(port), timeout=2.0)
    if password:
        c.cmd("AUTH", password)
    reply = c.cmd(*cmd)
except OSError as e:
    sys.exit(f"127.0.0.1:{port}: {e}")


def show(v):
    if isinstance(v, list):
        for x in v:
            show(x)
    elif v is not None:
        print(v)


show(reply)
sys.exit(1 if isinstance(reply, Err) else 0)
