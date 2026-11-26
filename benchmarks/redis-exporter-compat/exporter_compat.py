#!/usr/bin/env python3
"""redis_exporter against a server: what it reports, and whether it reads it right.

usage: exporter_compat.py --port PORT [--metrics-url URL] [--junitxml OUT.xml]
                          [--exporter-log-command CMD] [--write-expected FILE]

ci.sh starts the exporter (oliver006/redis_exporter, the version the KiviDB operator runs as its
sidecar) against 127.0.0.1:PORT. This puts a known workload on the server, the same on Redis and
on KiviDB, then reads the exporter's /metrics and checks:

- Scrape: the exporter reaches the server (redis_up 1) and reports no scrape error, and its log
  has no error or warning.
- Value: metrics that read what the workload did, so a field the exporter reads but misparses
  shows. 11 match the workload's exact numbers: keys and expiring keys per database, SET, GET and
  failed INCR calls, ERR errors, hits and misses, blocked clients, the role. Four check only that
  a value is there or above zero: connected clients, time in SET, used memory, SET latency
  percentiles.
- Metric: each metric name in expected-metrics.txt is reported. That file is what the exporter
  reports for Redis 8.10.3 under this workload (--write-expected writes it). A metric listed in
  known-gaps.txt that KiviDB does not report is a skip, not a fail: KiviDB's INFO has no field
  for it yet.
- Known gaps: listed: each name in known-gaps.txt is in expected-metrics.txt, so a stale entry
  fails (the check is in kividb-passes.txt).
- Known gaps: up to date: no metric listed in known-gaps.txt is reported by KiviDB. When KiviDB
  gains one, remove it from the list (the check is in kividb-passes.txt, so the job fails until
  then).

Standard library only.
"""

import argparse
import os
import re
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "ci"))
from resp import Conn  # noqa: E402

# The workload: what each Value check expects.
KEYS_DB0 = 100
EXPIRING_DB0 = 10
KEYS_DB1 = 7
GET_HITS = 50
GET_MISSES = 20
INCR_ERRORS = 5

LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def parse(text):
    """{metric name: [(labels dict, value)]} from Prometheus's text format."""
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = LINE.match(line)
        if not m:
            continue
        name, labels, value = m.groups()
        try:
            v = float(value)
        except ValueError:
            continue
        out.setdefault(name, []).append((dict(LABEL.findall(labels or "")), v))
    return out


def listed(name):
    with open(os.path.join(HERE, name), encoding="utf-8") as f:
        return [ln.split("#")[0].strip() for ln in f if ln.split("#")[0].strip()]


def workload(port):
    """Runs the workload and returns the connections it holds open: a client blocked in BLPOP and
    one subscribed to a channel."""
    c = Conn(port)
    c.cmd("FLUSHALL")
    c.cmd("CONFIG", "RESETSTAT")
    for i in range(KEYS_DB0):
        if i < EXPIRING_DB0:
            c.cmd("SET", f"key:{i}", "v", "EX", "100000")
        else:
            c.cmd("SET", f"key:{i}", "v")
    for i in range(GET_HITS):
        c.cmd("GET", f"key:{i}")
    for i in range(GET_MISSES):
        c.cmd("GET", f"missing:{i}")
    for _ in range(INCR_ERRORS):
        c.cmd("INCR", "key:50")  # "v" is not an integer: ERR
    c.cmd("SELECT", "1")
    for i in range(KEYS_DB1):
        c.cmd("SET", f"db1:{i}", "v")
    c.cmd("SELECT", "0")

    blocked = Conn(port)
    blocked.send("BLPOP", "nothing-here", "0")
    subscriber = Conn(port)
    subscriber.send("SUBSCRIBE", "exporter-compat")
    subscriber.read()
    # The blocked client is counted once the server has read its BLPOP.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        info = c.cmd("INFO", "clients")
        if re.search(r"^blocked_clients:1\b", info, re.M):
            break
        time.sleep(0.05)
    return c, blocked, subscriber


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--metrics-url", default=os.environ.get("EXPORTER_URL", "http://127.0.0.1:19121/metrics"))
    ap.add_argument("--junitxml")
    ap.add_argument("--exporter-log-command")
    ap.add_argument("--write-expected")
    args = ap.parse_args()

    started = time.monotonic()
    rows = []

    def check(name, ok, detail="", skip=False):
        rows.append({
            "check": name,
            "status": "SKIP" if skip else ("PASS" if ok else "FAIL"),
            "detail": detail,
            "ms": 0,
        })

    # The exporter takes a moment to listen.
    text = None
    for _ in range(60):
        try:
            with urllib.request.urlopen(args.metrics_url, timeout=5) as r:
                text = r.read().decode()
            break
        except OSError:
            time.sleep(0.5)
    if text is None:
        sys.exit(f"no answer from {args.metrics_url}")

    held = workload(args.port)
    is_kividb = "kividb_version:" in held[0].cmd("INFO", "server")
    with urllib.request.urlopen(args.metrics_url, timeout=10) as r:
        text = r.read().decode()
    metrics = parse(text)

    if args.write_expected:
        names = sorted(n for n in metrics if n.startswith("redis_") and not n.startswith("redis_exporter_"))
        with open(args.write_expected, "w", encoding="utf-8") as f:
            f.write("# What redis_exporter reports for Redis under exporter_compat.py's workload\n")
            f.write("# (exporter_compat.py --write-expected). One metric name a line.\n")
            f.writelines(n + "\n" for n in names)
        print(f"wrote {len(names)} names to {args.write_expected}")
        return

    def values(name, **labels):
        return [v for lab, v in metrics.get(name, []) if all(lab.get(k) == w for k, w in labels.items())]

    def value_check(what, name, expected, group="Value", **labels):
        got = values(name, **labels)
        sel = ",".join(f'{k}="{v}"' for k, v in labels.items())
        shown = f"{name}{{{sel}}}" if sel else name
        if not got:
            check(f"{group}: {what}", False, f"{shown} not reported")
        elif callable(expected):
            check(f"{group}: {what}", expected(got[0]), f"{shown} is {got[0]:g}")
        else:
            check(f"{group}: {what}", got[0] == expected, f"{shown} is {got[0]:g}, expected {expected:g}")

    # Scrape
    value_check("the exporter reaches the server", "redis_up", 1, group="Scrape")
    err = metrics.get("redis_exporter_last_scrape_error", [({}, 1)])[0]
    check("Scrape: no scrape error", err[1] == 0, f"redis_exporter_last_scrape_error {err}")
    if args.exporter_log_command:
        log = subprocess.run(args.exporter_log_command, shell=True, capture_output=True, text=True)
        bad = [ln for ln in (log.stdout + log.stderr).splitlines() if re.search(r"level=(error|warn)", ln)]
        check("Scrape: no errors or warnings in the exporter's log", not bad, "\n".join(bad[:5]))

    # Value
    value_check("keys in db0", "redis_db_keys", KEYS_DB0, db="db0")
    value_check("expiring keys in db0", "redis_db_keys_expiring", EXPIRING_DB0, db="db0")
    value_check("keys in db1", "redis_db_keys", KEYS_DB1, db="db1")
    value_check("SET calls", "redis_commands_total", KEYS_DB0 + KEYS_DB1, cmd="set")
    value_check("GET calls", "redis_commands_total", GET_HITS + GET_MISSES, cmd="get")
    value_check("failed INCR calls", "redis_commands_failed_calls_total", INCR_ERRORS, cmd="incr")
    value_check("ERR errors", "redis_errors_total", INCR_ERRORS, err="ERR")
    value_check("keyspace hits", "redis_keyspace_hits_total", GET_HITS)
    value_check("keyspace misses", "redis_keyspace_misses_total", GET_MISSES)
    value_check("blocked clients", "redis_blocked_clients", 1)
    value_check("connected clients", "redis_connected_clients", lambda v: v >= 3)
    value_check("time spent in SET", "redis_commands_duration_seconds_total", lambda v: v > 0, cmd="set")
    value_check("memory used", "redis_memory_used_bytes", lambda v: v > 0)
    value_check("role", "redis_instance_info", 1, role="master", redis_mode="standalone")
    lat = [lab for lab, _ in metrics.get("redis_latency_percentiles_usec", []) if lab.get("cmd") == "set"]
    check("Value: SET latency percentiles", bool(lat), f"{len(lat)} series for cmd=\"set\"")

    # Metric
    gaps = set(listed("known-gaps.txt"))
    expected = listed("expected-metrics.txt")
    stale = sorted(gaps - set(expected))
    check("Known gaps: listed", not stale,
          "not in expected-metrics.txt, remove from known-gaps.txt: " + ", ".join(stale))
    for name in expected:
        present = name in metrics
        check(f"Metric: {name}", present, "" if present else "not reported",
              skip=not present and is_kividb and name in gaps)
    if is_kividb:
        exported = sorted(g for g in gaps if g in metrics)
        check("Known gaps: up to date", not exported,
              "reported now, remove from known-gaps.txt: " + ", ".join(exported))

    for c in held:
        c.close()

    failed = [r for r in rows if r["status"] == "FAIL"]
    for r in failed:
        print(f"FAIL {r['check']}: {r['detail']}")
    print(f"{len(rows)} checks, {len(failed)} failed, {sum(r['status'] == 'SKIP' for r in rows)} skipped")
    if args.junitxml:
        from compat_junit import write_junit
        write_junit(args.junitxml, "redis-exporter", rows, time.monotonic() - started)


if __name__ == "__main__":
    main()
