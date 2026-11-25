"""redis-py runner: run.py's contract (host port protocol cases.json)."""

import json
import sys

import redis

host, port, protocol, cases = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
r = redis.Redis(host=host, port=port, protocol=protocol, single_connection_client=True, socket_timeout=10)
r.ping()


def norm(v):
    if isinstance(v, bytes):
        return v.decode("utf-8", "backslashreplace")
    if isinstance(v, dict):
        return [[norm(k), norm(x)] for k, x in v.items()]
    if isinstance(v, (list, tuple)):
        return [norm(x) for x in v]
    if isinstance(v, set):
        return sorted((norm(x) for x in v), key=json.dumps)
    return v


print(json.dumps({"client": "redis-py", "version": redis.__version__}))
for case in json.load(open(cases)):
    try:
        out = {"reply": norm(r.execute_command(*case["cmd"]))}
    except redis.ResponseError as e:
        out = {"error": str(e).split(" ")[0]}
    print(json.dumps({"case": case["name"], **out}, ensure_ascii=False))
