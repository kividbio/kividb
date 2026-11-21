#!/usr/bin/env python3
"""#900: centrifuge's history scripts against Redis, the way its Redis broker
runs them: a few pipelined connections, each script's 2-3 keys without a hash
tag (so each runs alone), on a server with several workers.

Centrifuge (the engine of Centrifugo) keeps a channel's history in a list or
a stream, and its offset and epoch in a hash, and reads and appends them with
the Lua scripts below. Its client, rueidis, sends every command of a broker
over 4 pipelined connections. With 4 connections on 4 workers, nearly every
script meets another one running and is queued for the thread running that
one, and its connection has nothing else in flight until the reply comes
back. centrifuge v0.40.1's own benchmarks (BenchmarkRedis*/rd_single, server
on 4 CPUs with --threads 4, the Go client on 4 others) ran 1.6-1.8x Redis
8.10.2's ns/op on the list scripts on master a987989e (#900).

Scripts, verbatim from centrifuge v0.40.1 (MIT, github.com/centrifugal/centrifuge,
internal/redis_lua): broker_history_list.lua, broker_history_stream.lua,
broker_history_add_list.lua, and broker_history_add_stream_fn.lua joined with
broker_history_add_stream_call.lua by a newline (as the broker joins them).
Each is first sent with EVAL and then by its SHA-1 (rueidis's EVALSHA, NOSCRIPT,
EVAL), with the arguments the broker sends in its benchmarks (no idempotency
key, no delta, the node's default 30-day meta TTL):

  history_list     History_1Ch, lists: HMGET meta, EXPIRE meta, LRANGE 0 -1 of
                   the 4 entries written first (4 publishes, HistorySize 4)
  history_stream   History_1Ch, streams: HMGET meta, EXPIRE meta, XRANGE of
                   the 4 entries written first
  publish_list     Publish_History_1Ch, lists: HGET, HINCRBY, EXPIRE meta,
                   LPUSH, LTRIM 0 99, EXPIRE list, PUBLISH (nobody subscribes)
  publish_stream   Publish_History_1Ch, streams: HGET, HINCRBY, EXPIRE meta,
                   XADD MAXLEN 100, EXPIRE stream, PUBLISH
  publish_list_many, publish_stream_many
                   Pub_History_ManyCh: the same over 1,024 channels, each
                   connection going through them in turn

Load: CONNS connections, each `redis-cli --pipe` of a file of BATCH calls, sent
again and again until the round ends (the server answers a connection's
commands in order, so how deep the pipeline is past one does not change what
it does). Each round starts from an empty server and its seed; the metric is
calls a second counted by the server (INFO commandstats) over WINDOW_S after
WARMUP_S, the median of ROUNDS rounds. A round with any error reply
(redis-cli's count, `total_error_replies`, a command's failed or rejected
calls) fails the suite, as does a history of the wrong length afterwards (4
after a read round, 100 after a one-channel publish round).

CPU: as suite 42, the server's threads pinned to the first half of the CPUs
this process may use and the load to the other half; KiviDB with as many
threads as the server has CPUs (`--threads 4` on a Standard_D8s_v5). Needs 8
vCPUs (`min_cpus=8`) and the `lua,tls` builds (`lua=True`).

Gates (the candidate): calls a second at least 1/MAX_X[row] of Redis's in the
same session (`vs_redis`), THROUGHPUT against the recorded candidate, and 60 %
of the gain over v1.2.2 measured in the same session (KEEP_IMPROVEMENT). See
README, "Recorded run, suite 45", for the numbers and centrifuge's own
benchmarks beside them.
"""
import os
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from suite import Suite, Config, KEEP_IMPROVEMENT, THROUGHPUT, vs_redis, median  # noqa: E402
from nodes import redis_tool  # noqa: E402

CONNS = 4          # rueidis: 2^PipelineMultiplex (2) pipelined connections
BATCH = 20_000     # calls in one connection's file
ROUNDS = 3
WARMUP_S = 2.0
WINDOW_S = 6.0
CHANNELS = 1024    # benchmarkNumDifferentChannels

# The candidate's calls a second at least 1/MAX_X of Redis's (time a call at
# most MAX_X times Redis's). Recorded (Standard_D8s_v5): 1.35x, 1.00x, 1.46x,
# 1.19x, 1.60x, 1.14x; each limit is about 15 % above that. Redis answers a
# deep pipeline of each connection in one turn of its loop, so it is further
# ahead here than in centrifuge's own benchmarks (README, "Recorded run, suite
# 45"). These are the rows to tighten when the scripts that run alone get one
# thread of their own (docs/PERFORMANCE.md, "Centrifuge's history scripts").
MAX_X = {
    "history_list": 1.55,
    "history_stream": 1.15,
    "publish_list": 1.70,
    "publish_stream": 1.35,
    "publish_list_many": 1.85,
    "publish_stream_many": 1.30,
}

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

ADD_LIST = """local list_key = KEYS[1]
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

ADD_STREAM_FN = """-- add_history appends one publication to a channel's history stream and
-- publishes it, returning { offset, epoch, from_cache, skipped }.
--
-- The logic is a function with one definition, and the caller supplies the
-- keys and arguments explicitly - nothing here reads KEYS or ARGV.
--
-- Writing, publishing and remembering happen here in that order, and none of
-- them is left to the caller. What the stream holds is what went out, and what
-- went out is what the result key remembers - so a call which fails partway
-- leaves nothing half-done behind it. Remember a publication before publishing
-- it and a failure in between leaves a result claiming something nobody
-- received; remember it later and a retry carrying the same idempotency key
-- cannot tell it already happened, and writes it a second time.
local function add_history(
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
                -- Suppressed: return current offset without incrementing
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
            -- Loop through the fields and values to find the field "d"
            for i = 1, #fields_and_values, 2 do
                local field = fields_and_values[i]
                local value = fields_and_values[i + 1]
                if field == "d" then
                    prev_message_payload = value
                    break -- Stop the loop once we find the field "d"
                end
            end
        end
    end

    if top_offset == 1 then
        -- If a new epoch starts (thus top_offset is 1), try to delete the existing stream, this may
        -- be important when the meta key is evicted by Redis LRU/LFU strategies. So we are emulating
        -- an eviction of stream key here.
        redis.call("del", stream_key)
        prev_message_payload = ""
    end

    redis.call("xadd", stream_key, "MAXLEN", stream_size, top_offset, "d", message_payload)
    redis.call("expire", stream_key, stream_ttl)

    if channel ~= '' then
        local payload
        if use_delta == "1" then
            payload = "__" ..
            "d1:" ..
            top_offset ..
            ":" ..
            current_epoch ..
            ":" .. #prev_message_payload .. ":" .. prev_message_payload .. ":" .. #message_payload .. ":" .. message_payload
        else
            payload = "__" .. "p1:" .. top_offset .. ":" .. current_epoch .. "__" .. message_payload
        end
        redis.call(publish_command, channel, payload)
    end

    -- The remembered result is written after the publication, never before:
    -- the other way round, a failure between the two would leave a result
    -- claiming a publication nobody received, and a retry would be answered
    -- from it.
    if result_key_expire ~= '' then
        redis.call("hset", result_key, "e", current_epoch, "s", top_offset)
        redis.call("expire", result_key, result_key_expire)
    end

    return { top_offset, current_epoch, "0", "0" }
end
"""

ADD_STREAM_CALL = """-- Caller for one publication. The logic lives in
-- broker_history_add_stream_fn.lua, which is prepended to this by the Go side.
return add_history(
    KEYS[1], KEYS[2], KEYS[3],
    ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5],
    ARGV[6], ARGV[7], ARGV[8], ARGV[9],
    ARGV[10], ARGV[11]
)
"""

ADD_STREAM = ADD_STREAM_FN + "\n" + ADD_STREAM_CALL

PREFIX = "cfb"
# The protobuf of a Publication carrying {"bench": true}, as the benchmarks send.
PAYLOAD = b'\x12\x0f{"bench": true}'
META_TTL = "2592000"   # centrifuge's default HistoryMetaTTL, 30 days
EPOCH = "AbCd"


def resp(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


def keys(kind, ch):
    if kind == "list":
        return PREFIX + ".list." + ch, PREFIX + ".list.meta." + ch
    return PREFIX + ".stream." + ch, PREFIX + ".stream.meta." + ch


def add_args(kind, ch, size, ttl):
    """KEYS and ARGV of a publish with history, as the broker sends them: a
    list's right bound is the size less one, a stream's MAXLEN the size; the
    list script ignores the last two arguments."""
    history, meta = keys(kind, ch)
    bound = str(size - 1) if kind == "list" else str(size)
    return [history, meta, PREFIX + ".result." + ch + ".", PAYLOAD, bound, str(ttl),
            PREFIX + ".client." + ch, META_TTL, EPOCH, "publish", "", "", "0", ""]


def history_args(kind):
    """History(ch, WithLimit(-1), WithSince(nil))."""
    history, meta = keys(kind, "channel")
    if kind == "list":
        return [history, meta, "1", "-1", META_TTL, EPOCH]
    return [history, meta, "1", "0", "0", "0", META_TTL, EPOCH]


# name -> (script, kind, number of keys, seed, calls(i) -> args)
WORKLOADS = {
    "history_list": (HISTORY_LIST, "list", 2, True, lambda i: history_args("list")),
    "history_stream": (HISTORY_STREAM, "stream", 2, True, lambda i: history_args("stream")),
    "publish_list": (ADD_LIST, "list", 3, False, lambda i: add_args("list", "channel", 100, 100)),
    "publish_stream": (ADD_STREAM, "stream", 3, False,
                       lambda i: add_args("stream", "channel", 100, 100)),
    "publish_list_many": (ADD_LIST, "list", 3, False,
                          lambda i: add_args("list", "channel%d" % (i % CHANNELS), 100, 100)),
    "publish_stream_many": (ADD_STREAM, "stream", 3, False,
                            lambda i: add_args("stream", "channel%d" % (i % CHANNELS), 100, 100)),
}


def parse_cpus(text):
    cpus = []
    for part in text.split(","):
        if "-" in part:
            lo, hi = part.split("-")
            cpus.extend(range(int(lo), int(hi) + 1))
        elif part.strip():
            cpus.append(int(part))
    return cpus


def cpu_split():
    """(server CPUs, client CPUs), or (None, None) where nothing is pinned."""
    if not hasattr(os, "sched_getaffinity"):
        return None, None
    server, client = os.environ.get("REGRESS_SERVER_CPUS"), os.environ.get("REGRESS_CLIENT_CPUS")
    if server and client:
        return parse_cpus(server), parse_cpus(client)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 2:
        return None, None
    half = len(cpus) // 2
    return cpus[:half], cpus[half:]


SERVER_CPUS, CLIENT_CPUS = cpu_split()
_THREADS = len(SERVER_CPUS) if SERVER_CPUS else 0
S = Suite("45-centrifuge-history-vs-redis", [Config("scripts", threads=_THREADS, lua=True)],
          min_cpus=8)


def pin_server(pid):
    for tid in os.listdir("/proc/%d/task" % pid):
        os.sched_setaffinity(int(tid), SERVER_CPUS)


def calls(c, command):
    return c.info("commandstats").get("cmdstat_" + command, {}).get("calls", 0)


def error_counts(info_all):
    errors = int(info_all.get("total_error_replies", 0) or 0)
    for k, v in info_all.items():
        if k.startswith("cmdstat_") and isinstance(v, dict):
            errors += int(v.get("failed_calls", 0) or 0) + int(v.get("rejected_calls", 0) or 0)
    return errors


class Load:
    """CONNS connections, each sending its file through `redis-cli --pipe`
    again and again until stopped."""

    def __init__(self, port, files):
        self.port, self.files = port, files
        self.stop = threading.Event()
        self.errors, self.threads = [], []

    def _one(self, path):
        pin = (lambda: os.sched_setaffinity(0, CLIENT_CPUS)) if CLIENT_CPUS else None
        while not self.stop.is_set():
            with open(path, "rb") as f:
                p = subprocess.run([redis_tool("redis-cli"), "-p", str(self.port), "--pipe"],
                                   stdin=f, capture_output=True, text=True, preexec_fn=pin)
            last = (p.stdout.strip().splitlines() or [""])[-1]
            if p.returncode != 0 or not last.startswith("errors: 0,"):
                self.errors.append(last or p.stderr[-300:])
                return

    def __enter__(self):
        for path in self.files:
            t = threading.Thread(target=self._one, args=(path,), daemon=True)
            t.start()
            self.threads.append(t)
        return self

    def __exit__(self, *exc):
        self.stop.set()
        for t in self.threads:
            t.join(timeout=120)


def history_len(c, kind, ch="channel"):
    history, _ = keys(kind, ch)
    return c.llen(history) if kind == "list" else c.xlen(history)


def one_round(ctx, c, name, files):
    script, kind, numkeys, seed, _ = WORKLOADS[name]
    c.flushall()
    # As rueidis loads a script: EVAL once (after a NOSCRIPT), then EVALSHA.
    add = ADD_LIST if kind == "list" else ADD_STREAM
    if seed:
        for _ in range(4):  # 4 publishes with HistorySize 4, HistoryTTL 300 s
            c.eval(add, 3, *add_args(kind, "channel", 4, 300))
        c.eval(script, numkeys, *WORKLOADS[name][4](0))
    else:
        c.eval(script, numkeys, *WORKLOADS[name][4](0))
    before = c.info("all")
    with Load(ctx.port, files) as load:
        time.sleep(WARMUP_S)
        a, t0 = calls(c, "evalsha"), time.perf_counter()
        time.sleep(WINDOW_S)
        b, t1 = calls(c, "evalsha"), time.perf_counter()
    errors = error_counts(c.info("all")) - error_counts(before)
    if load.errors or errors or b <= a:
        raise RuntimeError("%s: %d calls, %d error replies, redis-cli %r" % (
            name, b - a, errors, load.errors[:2]))
    want = 4 if seed else (100 if not name.endswith("_many") else None)
    if want is not None and history_len(c, kind) != want:
        raise RuntimeError("after a %s round the history holds %d entries, not %d" % (
            name, history_len(c, kind), want))
    return (b - a) / (t1 - t0)


@S.run
def run(ctx):
    c = ctx.client()
    pinned = bool(SERVER_CPUS and ctx.pid)
    if pinned:
        pin_server(ctx.pid)
    ctx.raw["cpus"] = {"pinned": pinned, "server": SERVER_CPUS, "client": CLIENT_CPUS,
                       "os_cpus": os.cpu_count()}
    ctx.raw["samples"], ctx.raw["script_counts"] = {}, {}
    with tempfile.TemporaryDirectory() as d:
        for name, (script, _, numkeys, _, args) in WORKLOADS.items():
            sha = c.script_load(script)
            files = []
            for k in range(CONNS):
                path = os.path.join(d, "%s-%d.resp" % (name, k))
                with open(path, "wb") as f:
                    # Each connection its own place in the channels' round.
                    f.write(b"".join(resp("EVALSHA", sha, str(numkeys), *args(k * BATCH + i))
                                     for i in range(BATCH)))
                files.append(path)
            c.script_flush()
            before = {k: int(v) for k, v in c.info("stats").items() if k.startswith("scripts_")}
            xs = [one_round(ctx, c, name, files) for _ in range(ROUNDS)]
            after = {k: int(v) for k, v in c.info("stats").items() if k.startswith("scripts_")}
            if ctx.kind == "kividb":
                ctx.raw["script_counts"][name] = {k: after[k] - before.get(k, 0) for k in after}
            ctx.raw["samples"][name] = xs
            ctx.emit("%s_calls_per_s" % name, median(xs), "calls/s", "higher",
                     gate=[THROUGHPUT, vs_redis(MAX_X[name]), KEEP_IMPROVEMENT], samples=xs)


if __name__ == "__main__":
    sys.exit(S.main())
