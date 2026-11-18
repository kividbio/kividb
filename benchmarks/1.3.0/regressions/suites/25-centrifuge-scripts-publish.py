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
(the numbers are in the #900 PR). These rows keep it so, and env.json now
records each binary's rustc (compare.py warns when the candidate's differs).

The scripts are centrifuge v0.40.1's `broker_history_list.lua` and
`broker_history_add_list.lua` (MIT, github.com/centrifugal/centrifuge), with
the arguments its benchmarks pass.

Configurations, one server each, KiviDB `--threads 2` (Redis: defaults):

  scripts   (lua: v1.2.2's full build and --candidate-full) 16 connections
            of redis-benchmark (--threads 2) call one script on one channel:
              history_calls_per_s   EVALSHA broker_history_list.lua, 2 keys
                                    (LRANGE of 4 entries, HMGET, EXPIRE)
              add_calls_per_s       EVALSHA broker_history_add_list.lua,
                                    3 keys (HGET, HINCRBY, EXPIRE, LPUSH,
                                    LTRIM, EXPIRE, PUBLISH)
  publish   4 idle subscribers on other channels; PUBLISH of a 16-byte
            message to one of 1,000 channels:
              publish_p16_per_s     one connection, pipeline 16
              publish_8x16_per_s    8 connections, pipeline 16 each

Every rate is calls a second counted by the server (INFO commandstats) over
an 8 s window after 2 s of warm-up: the median of 3 rounds, a fresh
redis-benchmark each round.

Gates: 10 % against the recorded value, and at most 1.05x worse than v1.2.2
measured in the same session (the rows spread 1-2 % between runs of one
binary). Redis is shown: it runs these scripts about twice as fast (#900).
"""
import os
import signal
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
PAYLOAD = '{"bench": true}'


def calls(c, command):
    return c.info("commandstats").get("cmdstat_" + command, {}).get("calls", 0)


def rate(ctx, command, bench_args):
    """Calls a second of `command` over the window, while redis-benchmark runs."""
    c = ctx.client()
    cmd = [redis_tool("redis-benchmark"), "-p", str(ctx.port), "-q", "-n", "1000000000"] + bench_args
    rb = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        time.sleep(WARMUP_S)
        a, t0 = calls(c, command), time.perf_counter()
        time.sleep(WINDOW_S)
        b, t1 = calls(c, command), time.perf_counter()
    finally:
        rb.send_signal(signal.SIGINT)
        try:
            _, err = rb.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            rb.kill()
            _, err = rb.communicate()
    if "ERR" in (err or "") or b <= a:
        raise RuntimeError("redis-benchmark %s: %s calls; %s" % (" ".join(bench_args[:8]), b - a, (err or "")[-300:]))
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


def run_scripts(ctx):
    c = ctx.client()
    hist = c.script_load(HISTORY_LIST)
    add = c.script_load(HISTORY_ADD_LIST)
    rows = {"history_calls_per_s": [], "add_calls_per_s": []}
    for _ in range(ROUNDS):
        c.flushall()
        # Four entries, as BenchmarkRedisHistory_1Ch publishes before it reads.
        for _ in range(4):
            c.evalsha(add, 3, LIST_KEY, META_KEY, "", "{}", 3, 300, "", 0, "abcd", "publish", "", 0)
        rows["history_calls_per_s"].append(rate(ctx, "evalsha", [
            "--threads", "2", "-c", "16", "-P", "1",
            "EVALSHA", hist, "2", LIST_KEY, META_KEY, "1", "-1", "0", "abcd"]))
        # A history of 100, as BenchmarkRedisPublish_History_1Ch keeps.
        rows["add_calls_per_s"].append(rate(ctx, "evalsha", [
            "--threads", "2", "-c", "16", "-P", "1",
            "EVALSHA", add, "3", LIST_KEY, META_KEY, "", PAYLOAD, "99", "100", "cf.client.ch", "0", "abcd",
            "publish", "", "0"]))
    ctx.raw["samples"] = rows
    for metric, xs in rows.items():
        ctx.emit(metric, median(xs), "calls/s", "higher", gate=[THROUGHPUT, vs_v1_2_2(1.05)], samples=xs)


def run_publish(ctx):
    c = ctx.client()
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
        ctx.emit(metric, median(xs), "calls/s", "higher", gate=[THROUGHPUT, vs_v1_2_2(1.05)], samples=xs)


@S.run
def run(ctx):
    if ctx.config.name == "scripts":
        run_scripts(ctx)
    else:
        run_publish(ctx)


if __name__ == "__main__":
    sys.exit(S.main())
