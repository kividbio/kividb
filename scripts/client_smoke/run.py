#!/usr/bin/env python3
"""The client smoke suite: the same commands through nine Redis clients, against Redis and KiviDB.

usage: run.py --redis HOST:PORT --kividb HOST:PORT [--clients a,b,...] [--work DIR] [--out DIR]

cases.json is the suite: commands run in order on one connection, starting with FLUSHALL. Each
client has a runner (redis_py.py, node/ioredis.mjs, ...) that sends every case through the
client's own way of sending a command, and prints one JSON line per case: the reply as the client
gives it back, turned into JSON, or the error's code. Each runs over RESP2 and RESP3 (ioredis, Jedis,
Lettuce and the rest all speak both), against Redis and then against KiviDB.

A client's lines against KiviDB are compared with its lines against Redis, case by case, so there is
no expected reply to keep: a difference is KiviDB answering otherwise than Redis, as that client
sees it. A case whose reply has no order (a set, a hash) says "sort" in cases.json, and its items are
sorted before the comparison. Where the client fails on Redis's reply and reads KiviDB's (valkey-glide
reading Redis 8's RESP3 search replies, which KiviDB sends in the shape glide reads: see
search-resp3-reply), it is reported, not counted as a failure.

Each runner's toolchain must be on the PATH: python3, node and npm, a JDK and Maven, Go, the .NET SDK,
Cargo. --clients picks some. The matrix goes to stdout, and to $GITHUB_STEP_SUMMARY when set; the
runners' output to --out. Exits 1 when a client answers otherwise on KiviDB than on Redis, or could
not be set up or run.
"""

import argparse
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def clients(work):
    """name: (protocols, setup shell command or None, the runner's command)."""
    py = os.path.join(work, "py", "bin", "python")
    java = ["java", "-cp", f"{HERE}/java/target/classes{os.pathsep}{HERE}/java/target/lib/*"]
    return {
        "redis-py": ((2, 3), f'python3 -m venv "{work}/py" && "{work}/py/bin/pip" install -q -r requirements.txt',
                     [py, "redis_py.py"]),
        "valkey-glide": ((2, 3), None, [py, "valkey_glide.py"]),  # in the venv redis-py set up
        "ioredis": ((2, 3), "npm ci --silent --no-audit --no-fund --prefix node", ["node", "node/ioredis.mjs"]),
        "node-redis": ((2, 3), None, ["node", "node/node_redis.mjs"]),  # installed with ioredis
        "Jedis": ((2, 3), "mvn -q -B -f java/pom.xml package", java + ["JedisSmoke"]),
        "Lettuce": ((2, 3), None, java + ["LettuceSmoke"]),  # built with Jedis
        "go-redis": ((2, 3), f'cd go && go build -o "{work}/go-redis" .', [f"{work}/go-redis"]),
        "StackExchange.Redis": ((2, 3), f'dotnet build -c Release -v quiet -o "{work}/dotnet" dotnet',
                                ["dotnet", f"{work}/dotnet/SmokeSE.dll"]),
        "redis-rs": ((2, 3), f'cargo build -q --release --manifest-path rust/Cargo.toml --target-dir "{work}/rust"',
                     [f"{work}/rust/release/client-smoke-redis-rs"]),
    }


# The runners each client's setup builds; a client whose setup is shared names the one that does it.
SETUP_BY = {"valkey-glide": "redis-py", "node-redis": "ioredis", "Lettuce": "Jedis"}


def sort_reply(reply, how):
    """A reply with no order, sorted: "items" sorts the elements, "pairs" the [key, value] pairs of
    a map (a flat [k, v, k, v] list is paired first)."""
    if not isinstance(reply, list):
        return reply
    if how == "pairs":
        if reply and not all(isinstance(p, list) and len(p) == 2 for p in reply):
            reply = [reply[i:i + 2] for i in range(0, len(reply), 2)]
        return sorted(reply, key=json.dumps)
    if how == "items":
        return sorted(reply, key=json.dumps)
    return reply


def run_one(cmd, host, port, protocol, out_path):
    """Runs a runner; returns (header, {case: result}) or (None, error text)."""
    try:
        p = subprocess.run(cmd + [host, str(port), str(protocol), os.path.join(HERE, "cases.json")],
                           cwd=HERE, capture_output=True, text=True, timeout=180)
    except subprocess.TimeoutExpired:
        return None, "timed out after 180 s"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(p.stdout)
        if p.stderr:
            f.write("\n# stderr\n" + p.stderr)
    lines = [json.loads(ln) for ln in p.stdout.splitlines() if ln.startswith("{")]
    if p.returncode != 0 or not lines:
        return None, (p.stderr.strip().splitlines() or ["no output"])[-1][:300]
    return lines[0], {ln["case"]: ln for ln in lines[1:]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", required=True)
    ap.add_argument("--kividb", required=True)
    ap.add_argument("--clients", default="")
    ap.add_argument("--work", default=os.path.join(HERE, ".work"))
    ap.add_argument("--out", default="client-smoke-results")
    args = ap.parse_args()
    work = os.path.abspath(args.work)
    out = os.path.abspath(args.out)
    os.makedirs(work, exist_ok=True)
    os.makedirs(out, exist_ok=True)
    servers = {"Redis": args.redis.rsplit(":", 1), "KiviDB": args.kividb.rsplit(":", 1)}
    cases = json.load(open(os.path.join(HERE, "cases.json"), encoding="utf-8"))
    sort = {c["name"]: c.get("sort") for c in cases}

    table = clients(work)
    chosen = [c for c in table if not args.clients or c in args.clients.split(",")]
    setup_failed = {}
    for name in chosen:
        setup = table[name][1]
        owner = SETUP_BY.get(name)
        if owner and owner in setup_failed:
            setup_failed[name] = setup_failed[owner]
            continue
        if owner and owner not in chosen:
            setup = table[owner][1]
        if setup:
            print(f"== setting up {name}", flush=True)
            p = subprocess.run(setup, shell=True, cwd=HERE, capture_output=True, text=True)
            if p.returncode != 0:
                setup_failed[name] = (p.stderr or p.stdout).strip().splitlines()[-1:] or ["failed"]

    rows, details, failed = [], [], False
    for name in chosen:
        protocols, _, cmd = table[name]
        if name in setup_failed:
            rows.append((name, "?", {p: f"❌ setup failed: {setup_failed[name][0][:120]}" for p in protocols}))
            failed = True
            continue
        version, cells = "?", {}
        for protocol in protocols:
            got = {}
            for server, (host, port) in servers.items():
                print(f"== {name} RESP{protocol} against {server}", flush=True)
                path = os.path.join(out, f"{name}-resp{protocol}-{server.lower()}.jsonl")
                got[server] = run_one(cmd, host, port, protocol, path)
            (rh, rres), (kh, kres) = got["Redis"], got["KiviDB"]
            if kh is None:
                cells[protocol] = f"❌ did not run: {kres}" if rh else f"❌ did not run on either: {rres}"
                failed = True
                continue
            if rh is None:
                cells[protocol] = f"❌ did not run against Redis: {rres}"
                failed = True
                continue
            version = kh.get("version", "?")
            same, better, differ = 0, [], []
            for c in cases:
                r, k = rres.get(c["name"]), kres.get(c["name"])
                if r is None or k is None:
                    differ.append((c["name"], r, k))
                    continue
                if "reply" in r:
                    r = {**r, "reply": sort_reply(r["reply"], sort[c["name"]])}
                if "reply" in k:
                    k = {**k, "reply": sort_reply(k["reply"], sort[c["name"]])}
                if r == k:
                    same += 1
                elif str(r.get("error", "")).startswith("client") and "reply" in k:
                    better.append(c["name"])
                else:
                    differ.append((c["name"], r, k))
            cell = f"{'❌' if differ else '✅'} {same}/{len(cases)} the same"
            if better:
                cell += f"; {len(better)} read on KiviDB only"
                details.append(f"- **{name}, RESP{protocol}**: fails on Redis's reply and reads KiviDB's: "
                               + ", ".join(f"`{b}`" for b in better))
            for case, r, k in differ:
                details.append(f"- **{name}, RESP{protocol}**, `{case}`: Redis `{json.dumps(r, ensure_ascii=False)[:200]}`, "
                               f"KiviDB `{json.dumps(k, ensure_ascii=False)[:200]}`")
            failed = failed or bool(differ)
            cells[protocol] = cell
        rows.append((name, version, cells))

    lines = [f"### Client smoke suite: {len(cases)} cases, KiviDB against Redis", "",
             "| Client | Version | RESP2 | RESP3 |", "|---|---|---|---|"]
    for name, version, cells in rows:
        lines.append(f"| {name} | {version} | {cells.get(2, '–')} | {cells.get(3, '–')} |")
    if details:
        lines += ["", *details]
    report = "\n".join(lines) + "\n"
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(report)
    with open(os.path.join(out, "summary.md"), "w", encoding="utf-8") as f:
        f.write(report)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
