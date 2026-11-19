#!/usr/bin/env python3
"""#900: Centrifuge's history scripts (two and three keys without a hash tag)
and PUBLISH to many channels nobody subscribes to.

Centrifuge (the engine of Centrifugo) keeps a channel's history in a list
and its offset/epoch in a hash, and reads and appends them with Lua scripts
whose keys carry no hash tag (`<prefix>.list.<ch>`, `<prefix>.list.meta.<ch>`),
so each script runs alone (`scripts_run_alone_no_tag`) and nearly all are
handed to the script-runner thread. Its broker publishes a message without
history as a plain PUBLISH to `<prefix>.client.<ch>`, which in its benchmarks
no one subscribes to; the broker's own pub/sub connections subscribe to other
channels.

On 2026-11-18 centrifuge v0.40.1's own benchmarks showed master 10-17 %
slower than v1.2.2 on every script row and 28 % slower on Publish_ManyCh at
PARALLELISM 16. It was not the code: v1.2.2 was the release binary, built by
rustc 1.100.0, and master was built on the VM image by rustc 1.99.1. Built
by the same rustc, master was level with or ahead of v1.2.2 on every row
(the numbers are in #921). These rows keep it so; env.json records each
binary's rustc, and compare.py refuses a candidate built by an older one.

The scripts are centrifuge v0.40.1's `broker_history_list.lua` and
`broker_history_add_list.lua` (MIT, github.com/centrifugal/centrifuge), sent
with the arguments its broker sends in BenchmarkRedisHistory_1Ch and
BenchmarkRedisPublish_History_1Ch (lists, no idempotency key, no delta):
the meta key's TTL is the node's default HistoryMetaTTL, 30 days
(2,592,000 s), so both scripts EXPIRE it; the history read asks for every
entry (right bound -1); a publish keeps 100 entries for 100 s.

Configurations, one server each, KiviDB `--threads 2` (Redis: defaults):

  scripts   (lua: v1.2.2's full build and --candidate-full) 16 connections
            of redis-benchmark (--threads 2) call one script on one channel:
              history_calls_per_s   EVALSHA broker_history_list.lua, 2 keys:
                                    HMGET meta, EXPIRE meta, LRANGE 0 -1 of
                                    the 4 entries written first
              add_calls_per_s       EVALSHA broker_history_add_list.lua,
                                    3 keys: HGET meta, HINCRBY meta, EXPIRE
                                    meta, LPUSH, LTRIM 0 99, EXPIRE list,
                                    PUBLISH
  publish   4 idle subscribers on other channels; PUBLISH of a 16-byte
            message to one of 1,000 channels:
              publish_p16_per_s     one connection, pipeline 16
              publish_8x16_per_s    8 connections, pipeline 16 each

Every rate is calls a second counted by the server (INFO commandstats) over
an 8 s window after 2 s of warm-up: the median of 3 rounds, a fresh
redis-benchmark each round. redis-benchmark does not stop at an error reply
(a NOSCRIPT or WRONGTYPE storm would count as throughput), so each round
checks the server's own error counts: `total_error_replies` (INFO stats) and
the `failed_calls` and `rejected_calls` of every command must not move, and
after a publish round the history list holds exactly 100 entries (4 after a
read round). Anything else fails the suite.

Gates: 10 % against the recorded value, and at most 1.05x worse than v1.2.2
measured in the same session (the rows spread 1-3 % between runs of one
binary). publish_8x16_per_s is held to 1.08x FOR NOW: it is the row where
master is behind v1.2.2 with both built by the same rustc (1.44-1.51M
against 1.53-1.58M a second in #921, both workers at 100 % CPU, although a
PUBLISH costs 3,525 instructions on master against 3,778 on v1.2.2 with one
thread). The gap is tracked by the fix on branch
perf/unpipelined-commands-vs-1-2-2; tighten this row to 1.05x in the PR that
lands it. Redis is shown, not gated.

Before its measured rounds each configuration checks that the error counts
work on this server: a deliberate error reply (EVALSHA of an unknown script)
must move them, or the suite fails rather than measure blind. (KiviDB 1.2.2,
master and Redis 8.10.2 all report total_error_replies and per-command
failed_calls; this guards against a server that does not.)
"""
import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import (Suite, Config, THROUGHPUT, vs_v1_2_2, median)  # noqa: E402
from nodes import redis_tool  # noqa: E402

S = Suite("25-centrifuge-scripts-publish", [
    Config("scripts", threads=2, lua=True),
    Config("publish", threads=2),
])
ROUNDS = 3
WARMUP_S = 2.0
WINDOW_S = 8.0
CHANNELS = 1000
# Temporary: tracked by the fix on branch perf/unpipelined-commands-vs-1-2-2;
# tighten to 1.05 in the PR that lands it.
PUBLISH_8X16_MAX_X = 1.08

HISTORY_LIST = """local list_key = KEYS[1]
local meta_key = KEYS[2]
local include_publications = ARGV[1]
local list_right_bound = ARGV[2]
local meta_expire = ARGV[3]
local new_epoch_if_empty = ARGV[4]

local stream_meta = redis.call("hmget", meta_key, "e", "s")
local current_epoch, top_offset = stream_meta[1], stream_meta[2]

if current_epoch == false then
    current_epoch = new_epoch_if_empty
    top_offset = 0
    redis.call("hset", meta_key, "e", current_epoch)
end

if top_offset == false then
    top_offset = 0
end

if meta_expire ~= '0' then
    redis.call("expire", meta_key, meta_expire)
end

local pubs = nil
if include_publications ~= "0" then
    pubs = redis.call("lrange", list_key, 0, list_right_bound)
end

return { top_offset, current_epoch, pubs }
"""

HISTORY_ADD_LIST = """local list_key = KEYS[1]
local meta_key = KEYS[2]
local result_key = KEYS[3]
local message_payload = ARGV[1]
local ltrim_right_bound = ARGV[2]
local list_ttl = ARGV[3]
local channel = ARGV[4]
local meta_expire = ARGV[5]
local new_epoch_if_empty = ARGV[6]
local publish_command = ARGV[7]
local result_key_expire = ARGV[8]
local use_delta = ARGV[9]

if result_key_expire ~= '' then
    local cached_result = redis.call("hmget", result_key, "e", "s")
    local result_epoch, result_offset = cached_result[1], cached_result[2]
    if result_epoch ~= false then
        return { result_offset, result_epoch, "1" }
    end
end

local current_epoch = redis.call("hget", meta_key, "e")
if current_epoch == false then
    current_epoch = new_epoch_if_empty
    redis.call("hset", meta_key, "e", current_epoch)
end

local top_offset = redis.call("hincrby", meta_key, "s", 1)

if meta_expire ~= '0' then
    redis.call("expire", meta_key, meta_expire)
end

local prev_message_payload = ""
if use_delta == "1" then
    prev_message_payload = redis.call("lindex", list_key, 0) or ""
end

local payload = "__" .. "p1:" .. top_offset .. ":" .. current_epoch .. "__" .. message_payload
redis.call("lpush", list_key, payload)
redis.call("ltrim", list_key, 0, ltrim_right_bound)
redis.call("expire", list_key, list_ttl)

if channel ~= '' then
    if use_delta == "1" then
        payload = "__" ..
        "d1:" ..
        top_offset ..
        ":" ..
        current_epoch ..
        ":" .. #prev_message_payload .. ":" .. prev_message_payload .. ":" .. #message_payload .. ":" .. message_payload
    end
    redis.call(publish_command, channel, payload)
end

if result_key_expire ~= '' then
    redis.call("hset", result_key, "e", current_epoch, "s", top_offset)
    redis.call("expire", result_key, result_key_expire)
end

return { top_offset, current_epoch, "0" }
"""

LIST_KEY, META_KEY = "cf.list.ch", "cf.list.meta.ch"
# The idempotency result key: named, never touched (result_key_expire is '').
RESULT_KEY = "cf.result.ch."
PAYLOAD = '{"bench": true}'
META_TTL = "2592000"   # centrifuge's default HistoryMetaTTL, 30 days
EPOCH = "abcd"
# BenchmarkRedisHistory_1Ch: 4 publishes with HistorySize 4, HistoryTTL 300 s.
SEED_SIZE, SEED_TTL = 4, 300
# BenchmarkRedisPublish_History_1Ch: HistorySize 100, HistoryTTL 100 s.
HISTORY_SIZE, HISTORY_TTL = 100, 100


def add_args(size, ttl, payload=PAYLOAD):
    """broker_history_add_list.lua's ARGV as centrifuge's publish sends them
    (no idempotency key: result_key_expire ''; no delta: use_delta '';
    version '0' and version epoch '', which the list script ignores)."""
    return [payload, str(size - 1), str(ttl), "cf.client.ch", META_TTL, EPOCH, "publish", "", "", "0", ""]


def history_args():
    """broker_history_list.lua's ARGV for History(ch, WithLimit(-1))."""
    return ["1", "-1", META_TTL, EPOCH]


def calls(c, command):
    return c.info("commandstats").get("cmdstat_" + command, {}).get("calls", 0)


def error_counts(info_all):
    """The server's error replies so far: total_error_replies, and the failed
    and rejected calls of every command. From INFO (all sections, parsed)."""
    errors = int(info_all.get("total_error_replies", 0) or 0)
    for k, v in info_all.items():
        if k.startswith("cmdstat_") and isinstance(v, dict):
            errors += int(v.get("failed_calls", 0) or 0) + int(v.get("rejected_calls", 0) or 0)
    return errors


def errors_during(before, after):
    """Error replies between two INFO snapshots; a round must have none."""
    return error_counts(after) - error_counts(before)


def rate(ctx, command, bench_args):
    """Calls a second of `command` over the window, while redis-benchmark runs.
    Raises if the server sent any error reply during the round."""
    c = ctx.client()
    cmd = [redis_tool("redis-benchmark"), "-p", str(ctx.port), "-q", "-n", "1000000000"] + bench_args
    before = c.info("all")
    rb = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        time.sleep(WARMUP_S)
        a, t0 = calls(c, command), time.perf_counter()
        time.sleep(WINDOW_S)
        b, t1 = calls(c, command), time.perf_counter()
    finally:
        # SIGTERM: redis-benchmark --threads does not stop on SIGINT.
        rb.terminate()
        try:
            _, err = rb.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            rb.kill()
            _, err = rb.communicate()
    errors = errors_during(before, c.info("all"))
    if errors or b <= a:
        raise RuntimeError("redis-benchmark %s: %d calls, %d error replies; %s" % (
            " ".join(bench_args[:8]), b - a, errors, (err or "")[-300:]))
    return (b - a) / (t1 - t0)


def subscribers(port, n):
    socks = []
    for i in range(n):
        s = socket.create_connection(("127.0.0.1", port))
        s.sendall(b"SUBSCRIBE pfx.control pfx.shard.%d\r\n" % i)
        ack = b""
        while ack.count(b"subscribe") < 2:
            ack += s.recv(4096)
        socks.append(s)
    return socks


def expect_len(c, n, after):
    got = c.llen(LIST_KEY)
    if got != n:
        raise RuntimeError("after %s the history list holds %d entries, not %d" % (after, got, n))


def errors_are_counted(c):
    """Refuse to measure on a server whose error counts do not move: send one
    error reply and see it counted (the error check of rate() relies on it)."""
    before = c.info("all")
    try:
        c.execute_command("EVALSHA", "0" * 40, 0)
    except Exception as e:  # redis.ResponseError (not imported: the self-test runs without redis-py)
        if type(e).__name__ != "ResponseError":
            raise
    else:
        raise RuntimeError("EVALSHA of an unknown script did not fail")
    if errors_during(before, c.info("all")) < 1:
        raise RuntimeError("this server does not count error replies (total_error_replies, failed_calls): "
                           "the suite's error check would see nothing")


def run_scripts(ctx):
    c = ctx.client()
    errors_are_counted(c)
    hist = c.script_load(HISTORY_LIST)
    add = c.script_load(HISTORY_ADD_LIST)
    bench = ["--threads", "2", "-c", "16", "-P", "1", "EVALSHA"]
    rows = {"history_calls_per_s": [], "add_calls_per_s": []}
    for _ in range(ROUNDS):
        c.flushall()
        for _ in range(SEED_SIZE):
            c.evalsha(add, 3, LIST_KEY, META_KEY, RESULT_KEY, *add_args(SEED_SIZE, SEED_TTL, "{}"))
        expect_len(c, SEED_SIZE, "the seed")
        rows["history_calls_per_s"].append(rate(ctx, "evalsha", bench + [
            hist, "2", LIST_KEY, META_KEY] + history_args()))
        expect_len(c, SEED_SIZE, "a read round")
        rows["add_calls_per_s"].append(rate(ctx, "evalsha", bench + [
            add, "3", LIST_KEY, META_KEY, RESULT_KEY] + add_args(HISTORY_SIZE, HISTORY_TTL)))
        expect_len(c, HISTORY_SIZE, "a publish round")
    ctx.raw["samples"] = rows
    for metric, xs in rows.items():
        ctx.emit(metric, median(xs), "calls/s", "higher", gate=[THROUGHPUT, vs_v1_2_2(1.05)], samples=xs)


def run_publish(ctx):
    c = ctx.client()
    errors_are_counted(c)
    socks = subscribers(ctx.port, 4)
    rows = {"publish_p16_per_s": [], "publish_8x16_per_s": []}
    channel = "pfx.client.channel__rand_int__"
    try:
        for _ in range(ROUNDS):
            rows["publish_p16_per_s"].append(rate(ctx, "publish", [
                "-c", "1", "-P", "16", "-r", str(CHANNELS), "PUBLISH", channel, PAYLOAD]))
            rows["publish_8x16_per_s"].append(rate(ctx, "publish", [
                "--threads", "2", "-c", "8", "-P", "16", "-r", str(CHANNELS), "PUBLISH", channel, PAYLOAD]))
        ctx.raw["control_subscribers"] = int(c.pubsub_numsub("pfx.control")[0][1])
    finally:
        for s in socks:
            s.close()
    ctx.raw["samples"] = rows
    for metric, xs in rows.items():
        limit = PUBLISH_8X16_MAX_X if metric == "publish_8x16_per_s" else 1.05
        ctx.emit(metric, median(xs), "calls/s", "higher", gate=[THROUGHPUT, vs_v1_2_2(limit)], samples=xs)


@S.run
def run(ctx):
    if ctx.config.name == "scripts":
        run_scripts(ctx)
    else:
        run_publish(ctx)


if __name__ == "__main__":
    sys.exit(S.main())
