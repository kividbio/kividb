#!/usr/bin/env python3
"""#900: Centrifuge v0.40.1 history scripts, kept from regressing.

The scripts below are copied from Centrifuge v0.40.1.  Both history formats
are driven from 16 connections: one hot channel and 16 independent channels.
It measures list reads/appends, stream appends, and the stream script's
XRANGE/XREVRANGE COUNT arms.  Sixteen idle SUBSCRIBE connections remain live.

``threads4`` explicitly asks for four KiviDB workers.  ``default`` deliberately
omits --threads because #900 also asks to preserve the current default policy.
On an 8-vCPU Linux VM the server is pinned to four CPUs and the clients to the
other four.  Record it on that same machine with Redis 8.10.2; master today is
the regression baseline and Redis is shown in the same session.
"""
import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, THROUGHPUT, median  # noqa: E402
from nodes import redis_tool  # noqa: E402

CLIENTS, ROUNDS = 16, 2
WARMUP_S, WINDOW_S = 1.0, 5.0
SEED_SIZE = 4
HISTORY_SIZE, HISTORY_TTL = 100, 100
META_TTL, EPOCH = "2592000", "issue900"
PAYLOAD = '{"bench": true}'


def parse_cpus(text):
    """``0-3,6`` -> ``[0, 1, 2, 3, 6]``."""
    cpus = []
    for part in text.split(","):
        if "-" in part:
            lo, hi = part.split("-", 1)
            cpus.extend(range(int(lo), int(hi) + 1))
        elif part.strip():
            cpus.append(int(part))
    return cpus


def cpu_split():
    if not hasattr(os, "sched_getaffinity"):
        return None, None
    server, client = os.environ.get("REGRESS_SERVER_CPUS"), os.environ.get("REGRESS_CLIENT_CPUS")
    if server and client:
        return parse_cpus(server), parse_cpus(client)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 2:
        return None, None
    middle = len(cpus) // 2
    return cpus[:middle], cpus[middle:]


SERVER_CPUS, CLIENT_CPUS = cpu_split()
S = Suite("38-centrifuge-history", [
    Config("threads4", threads=4, lua=True),
    # #900 explicitly asks for both --threads 4 and the current default.
    Config("default", threads=0, lua=True),
], min_cpus=8)


def pin_server(pid):
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), SERVER_CPUS)


# Centrifuge v0.40.1: internal/redis_lua/broker_history_list.lua.
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

# Centrifuge v0.40.1: internal/redis_lua/broker_history_add_list.lua.
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
        payload = "__" .. "d1:" .. top_offset .. ":" .. current_epoch .. ":" .. #prev_message_payload .. ":" .. prev_message_payload .. ":" .. #message_payload .. ":" .. message_payload
    end
    redis.call(publish_command, channel, payload)
end
if result_key_expire ~= '' then
    redis.call("hset", result_key, "e", current_epoch, "s", top_offset)
    redis.call("expire", result_key, result_key_expire)
end
return { top_offset, current_epoch, "0" }
"""

# Centrifuge v0.40.1: internal/redis_lua/broker_history_stream.lua.
HISTORY_STREAM = """local stream_key = KEYS[1]
local meta_key = KEYS[2]
local include_publications = ARGV[1]
local since_offset = ARGV[2]
local limit = ARGV[3]
local reverse = ARGV[4]
local meta_expire = ARGV[5]
local new_epoch_if_empty = ARGV[6]
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
    if limit ~= "0" then
        if reverse == "0" then
            pubs = redis.call("xrange", stream_key, since_offset, "+", "COUNT", limit)
        else
            local get_offset = top_offset
            if since_offset ~= "0" then
                get_offset = since_offset
            end
            pubs = redis.call("xrevrange", stream_key, get_offset, "-", "COUNT", limit)
        end
    else
        if reverse == "0" then
            pubs = redis.call("xrange", stream_key, since_offset, "+")
        else
            local get_offset = top_offset
            if since_offset ~= "0" then
                get_offset = since_offset
            end
            pubs = redis.call("xrevrange", stream_key, get_offset, "-")
        end
    end
end
return { top_offset, current_epoch, pubs }
"""

# Centrifuge v0.40.1: broker_history_add_stream_fn.lua and its caller.
HISTORY_ADD_STREAM = """local function add_history(
    stream_key, meta_key, result_key,
    message_payload, stream_size, stream_ttl, channel, meta_expire,
    new_epoch_if_empty, publish_command, result_key_expire, use_delta,
    version, version_epoch
)
    if result_key_expire ~= '' then
        local cached_result = redis.call("hmget", result_key, "e", "s")
        local result_epoch, result_offset = cached_result[1], cached_result[2]
        if result_epoch ~= false then
            return { result_offset, result_epoch, "1", "0" }
        end
    end
    local current_epoch = redis.call("hget", meta_key, "e")
    if current_epoch == false then
        current_epoch = new_epoch_if_empty
        redis.call("hset", meta_key, "e", current_epoch)
    end
    if version ~= "0" then
        local prev_version_values = redis.call("hmget", meta_key, "v", "ve", "s")
        local prev_version = prev_version_values[1]
        local prev_version_epoch = prev_version_values[2]
        local current_offset = prev_version_values[3]
        if prev_version then
            if (version_epoch == "" or version_epoch == prev_version_epoch) and (tonumber(prev_version) >= tonumber(version)) then
                local offset_num = 0
                if current_offset then
                    offset_num = tonumber(current_offset)
                end
                return { offset_num, current_epoch, "0", "1" }
            end
        end
        redis.call("hset", meta_key, "v", version, "ve", version_epoch)
    end
    local top_offset = redis.call("hincrby", meta_key, "s", 1)
    if meta_expire ~= '0' then
        redis.call("expire", meta_key, meta_expire)
    end
    local prev_message_payload = ""
    if use_delta == "1" and top_offset ~= 1 then
        local prev_entries = redis.call("xrevrange", stream_key, "+", "-", "COUNT", 1)
        if #prev_entries > 0 then
            prev_message_payload = prev_entries[1][2]["d"]
            local fields_and_values = prev_entries[1][2]
            for i = 1, #fields_and_values, 2 do
                local field = fields_and_values[i]
                local value = fields_and_values[i + 1]
                if field == "d" then
                    prev_message_payload = value
                    break
                end
            end
        end
    end
    if top_offset == 1 then
        redis.call("del", stream_key)
        prev_message_payload = ""
    end
    redis.call("xadd", stream_key, "MAXLEN", stream_size, top_offset, "d", message_payload)
    redis.call("expire", stream_key, stream_ttl)
    if channel ~= '' then
        local payload
        if use_delta == "1" then
            payload = "__" .. "d1:" .. top_offset .. ":" .. current_epoch .. ":" .. #prev_message_payload .. ":" .. prev_message_payload .. ":" .. #message_payload .. ":" .. message_payload
        else
            payload = "__" .. "p1:" .. top_offset .. ":" .. current_epoch .. "__" .. message_payload
        end
        redis.call(publish_command, channel, payload)
    end
    if result_key_expire ~= '' then
        redis.call("hset", result_key, "e", current_epoch, "s", top_offset)
        redis.call("expire", result_key, result_key_expire)
    end
    return { top_offset, current_epoch, "0", "0" }
end
return add_history(
    KEYS[1], KEYS[2], KEYS[3],
    ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5],
    ARGV[6], ARGV[7], ARGV[8], ARGV[9], ARGV[10], ARGV[11]
)
"""


def keys(kind, channel):
    """Centrifuge-like untagged data, metadata and idempotency keys."""
    return ("cf38.%s.%s" % (kind, channel), "cf38.%s.meta.%s" % (kind, channel),
            "cf38.%s.result.%s" % (kind, channel))


def add_args(kind, channel, size=HISTORY_SIZE, ttl=HISTORY_TTL):
    """ARGV values from a normal Centrifuge broker publish (no delta/cache)."""
    trimmed = size - 1 if kind == "list" else size
    return [PAYLOAD, str(trimmed), str(ttl), "cf38.client.%s" % channel,
            META_TTL, EPOCH, "publish", "", "", "0", ""]


def read_args(kind, reverse=False):
    if kind == "list":
        return ["1", "-1", META_TTL, EPOCH]
    return ["1", "0", str(SEED_SIZE), "1" if reverse else "0", META_TTL, EPOCH]


def channels(mode):
    return (["one"] * CLIENTS if mode == "one"
            else ["many-%02d" % number for number in range(CLIENTS)])


def calls(client):
    return client.info("commandstats").get("cmdstat_evalsha", {}).get("calls", 0)


def error_counts(all_info):
    errors = int(all_info.get("total_error_replies", 0) or 0)
    for name, values in all_info.items():
        if name.startswith("cmdstat_") and isinstance(values, dict):
            errors += int(values.get("failed_calls", 0) or 0) + int(values.get("rejected_calls", 0) or 0)
    return errors


def errors_are_counted(client):
    before = client.info("all")
    try:
        client.execute_command("EVALSHA", "0" * 40, 0)
    except Exception as exc:  # redis.ResponseError; no redis import keeps the self-test portable.
        if not any(t.__name__ == "ResponseError" for t in type(exc).__mro__):
            raise
    else:
        raise RuntimeError("EVALSHA of an unknown script did not fail")
    if error_counts(client.info("all")) <= error_counts(before):
        raise RuntimeError("server does not count error replies; refusing to measure blind")


def subscribers(port):
    socks = []
    for number in range(CLIENTS):
        sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        sock.sendall(b"SUBSCRIBE cf38.control.%d\r\n" % number)
        reply = b""
        while b"subscribe" not in reply:
            block = sock.recv(4096)
            if not block:
                raise RuntimeError("server closed a SUBSCRIBE connection")
            reply += block
        socks.append(sock)
    return socks


def command(sha, key_names, args):
    return ["EVALSHA", sha, str(len(key_names))] + list(key_names) + list(args)


def rate(ctx, commands):
    """Run CLIENTS unpipelined redis-benchmark clients and return EVALSHA/s."""
    if len(commands) != CLIENTS:
        raise ValueError("expected %d benchmark commands, got %d" % (CLIENTS, len(commands)))
    client, procs = ctx.client(), []
    before = client.info("all")
    try:
        for bench in commands:
            argv = [redis_tool("redis-benchmark"), "-p", str(ctx.port), "-q", "-n", "1000000000",
                    "-c", "1", "-P", "1"] + bench
            procs.append(subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True))
        time.sleep(WARMUP_S)
        start, t0 = calls(client), time.perf_counter()
        time.sleep(WINDOW_S)
        end, t1 = calls(client), time.perf_counter()
    finally:
        stderr = []
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                _, err = proc.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                _, err = proc.communicate()
            if err:
                stderr.append(err[-200:])
    reply_errors = error_counts(client.info("all")) - error_counts(before)
    if reply_errors or end <= start:
        raise RuntimeError("history load made %d calls with %d error replies: %s" % (
            end - start, reply_errors, " ".join(stderr)[-500:]))
    return (end - start) / (t1 - t0)


def seed(client, kind, add_sha, channel):
    data, meta, result = keys(kind, channel)
    for _ in range(SEED_SIZE):
        client.evalsha(add_sha, 3, data, meta, result, *add_args(kind, channel, SEED_SIZE, 300))


def history_len(client, kind, channel):
    data, _, _ = keys(kind, channel)
    return client.llen(data) if kind == "list" else client.xlen(data)


def expect_history(client, kind, channel_names, expected, when):
    for channel in sorted(set(channel_names)):
        actual = history_len(client, kind, channel)
        if actual != expected:
            raise RuntimeError("%s %s has %d entries after %s, not %d" % (
                kind, channel, actual, when, expected))


def commands_for(case, mode, shas):
    kind, operation = case
    commands = []
    for channel in channels(mode):
        data, meta, result = keys(kind, channel)
        if operation == "list_read":
            commands.append(command(shas["list_read"], (data, meta), read_args("list")))
        elif operation == "list_add":
            commands.append(command(shas["list_add"], (data, meta, result), add_args("list", channel)))
        elif operation == "xrange":
            commands.append(command(shas["stream_read"], (data, meta), read_args("stream")))
        elif operation == "xrevrange":
            commands.append(command(shas["stream_read"], (data, meta), read_args("stream", reverse=True)))
        elif operation == "stream_add":
            commands.append(command(shas["stream_add"], (data, meta, result), add_args("stream", channel)))
        else:
            raise ValueError("unknown workload %r" % (operation,))
    return commands


CASES = (("list", "list_read"), ("list", "list_add"), ("stream", "xrange"),
         ("stream", "xrevrange"), ("stream", "stream_add"))


def setup_case(client, case, channel_names, shas):
    client.flushall()
    kind, _ = case
    add_sha = shas["list_add"] if kind == "list" else shas["stream_add"]
    for channel in sorted(set(channel_names)):
        seed(client, kind, add_sha, channel)
    expect_history(client, kind, channel_names, SEED_SIZE, "the seed")


@S.run
def run(ctx):
    client = ctx.client()
    if SERVER_CPUS and ctx.pid:
        pin_server(ctx.pid)
        os.sched_setaffinity(0, CLIENT_CPUS)  # redis-benchmark children inherit this.
    ctx.raw["cpus"] = {"pinned": bool(SERVER_CPUS and ctx.pid), "server": SERVER_CPUS,
                       "client": CLIENT_CPUS, "os_cpus": os.cpu_count(), "threads": ctx.config.threads}
    errors_are_counted(client)
    shas = {"list_read": client.script_load(HISTORY_LIST), "list_add": client.script_load(HISTORY_ADD_LIST),
            "stream_read": client.script_load(HISTORY_STREAM), "stream_add": client.script_load(HISTORY_ADD_STREAM)}
    socks = subscribers(ctx.port)
    try:
        subscribed = client.pubsub_numsub(*["cf38.control.%d" % n for n in range(CLIENTS)])
        if sum(int(row[1]) for row in subscribed) != CLIENTS:
            raise RuntimeError("expected %d live SUBSCRIBE connections, got %r" % (CLIENTS, subscribed))
        rows = {}
        for case in CASES:
            for mode in ("one", "many"):
                name, names = "%s_%s" % (case[1], mode), channels(mode)
                setup_case(client, case, names, shas)
                samples = [rate(ctx, commands_for(case, mode, shas)) for _ in range(ROUNDS)]
                expected = HISTORY_SIZE if case[1].endswith("add") else SEED_SIZE
                expect_history(client, case[0], names, expected, name)
                rows[name + "_per_s"] = samples
    finally:
        for sock in socks:
            sock.close()
    ctx.raw["samples"] = rows
    for metric, samples in rows.items():
        # THROUGHPUT records current master; the same-session Redis value is shown.
        ctx.emit(metric, median(samples), "calls/s", "higher", gate=[THROUGHPUT], samples=samples)


if __name__ == "__main__":
    sys.exit(S.main())
