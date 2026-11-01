"""
Shared helpers, fixtures, and assertion utilities for KiviDB e2e tests.

By default the test suite spins up redis-stack-server on KIVI_PORT (6380).
Set KIVI_BINARY to override the server binary.
Set KIVI_EXTERNAL=1 to skip server management (use a pre-running instance).
"""
from __future__ import annotations

import os
import shutil
import time
import struct
import math
import threading
import subprocess
import socket
import redis
import pytest
from typing import Any, Generator

# ── Connection config ──────────────────────────────────────────────────────────
KIVI_HOST = os.environ.get("KIVI_HOST", "127.0.0.1")
KIVI_PORT = int(os.environ.get("KIVI_PORT", "6380"))


def _find_server_binary() -> str:
    """
    Resolve the server binary to use for tests.

    Priority:
      1. KIVI_BINARY env var (explicit override)
      2. redis-stack-server in PATH  (preferred: gives full module support)
      3. redis-server in PATH        (fallback: core commands only)
      4. kividb release binary relative to repo root
    """
    explicit = os.environ.get("KIVI_BINARY")
    if explicit:
        return explicit
    for candidate in ("redis-stack-server", "redis-server"):
        found = shutil.which(candidate)
        if found:
            return found
    _repo_root = os.path.normpath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
    )
    return os.path.join(_repo_root, "target", "release", "kividb")


KIVI_BINARY = _find_server_binary()

# True when the resolved binary is a standard Redis server (not kividb).
_IS_REDIS = any(name in os.path.basename(KIVI_BINARY)
                for name in ("redis-stack-server", "redis-server"))


# ── Low-level wait helper ──────────────────────────────────────────────────────

def wait_for_ready(host: str, port: int, timeout: float = 60.0) -> bool:
    """Poll until the server is ready and not loading its dataset."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            c = redis.Redis(host=host, port=port, socket_connect_timeout=1,
                            socket_timeout=2)
            # DBSIZE fails with BusyLoadingError during dataset load; PING does not.
            c.dbsize()
            c.close()
            return True
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.5)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
                OSError):
            time.sleep(0.2)
        except Exception:
            time.sleep(0.2)
    return False


def wait_for_port(host: str, port: int, timeout: float = 30.0) -> bool:
    """Alias kept for backwards compatibility — delegates to wait_for_ready."""
    return wait_for_ready(host, port, timeout)


# ── Ports for servers a test starts itself ─────────────────────────────────────
#
# Ports come from 10000-29999, below every OS's ephemeral range (Linux
# 32768-60999, Windows and macOS 49152-65535). A port from that range can be
# taken as the source port of any outgoing connection in the moment between
# picking it and the server binding it, which is also why the Rust tests'
# `free_port()` stays out of it.
#
# And no port is handed out twice in one process. These used to come from
# `bind(("127.0.0.1", 0))`, which on Linux returns the same port to two calls
# in a row about once in 17,000 pairs. The second server of the pair then
# failed to bind and exited, the test found the first one answering on the
# port and took it for the second: a "replica" told `REPLICAOF` its own
# address, which never syncs (#262).
_PORT_LOW, _PORT_HIGH = 10000, 30000
_ports_handed_out: set[int] = set()
_next_port = _PORT_LOW + (os.getpid() * 7919) % (_PORT_HIGH - _PORT_LOW)
_port_lock = threading.Lock()


def _port_is_free(port: int) -> bool:
    """Whether a server could bind *port* now. Binds without listening: a
    listening probe can be connected to by a client dialling the port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if os.name == "posix":
            # As servers do, so a port with a closed connection in TIME_WAIT
            # counts as free. (On Windows this option means something else.)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False


def free_port() -> int:
    """A TCP port for a server the test starts itself: outside the ephemeral
    range, not in use now, and never returned before in this process."""
    global _next_port
    span = _PORT_HIGH - _PORT_LOW
    with _port_lock:
        for _ in range(span):
            port = _next_port
            _next_port = _PORT_LOW + (port - _PORT_LOW + 1) % span
            if port not in _ports_handed_out and _port_is_free(port):
                _ports_handed_out.add(port)
                return port
    raise RuntimeError(f"no free port left in {_PORT_LOW}-{_PORT_HIGH - 1}")


# ── Redis client factory ───────────────────────────────────────────────────────

# ── Protocol under test ────────────────────────────────────────────────────────
#
# The suite runs against both RESP2 and RESP3. The `resp_protocol` fixture (see
# below) drives this for each test, and every client built through this module
# picks it up, including the many tests that call `make_client()` directly in
# the test body rather than taking the `r` fixture. Keeping it ambient is what
# makes the dual-protocol run cover the whole suite instead of only the tests
# that happen to use a fixture.
#
# `KIVI_RESP=2` or `KIVI_RESP=3` restricts a run to one protocol; the default
# exercises both.
_ACTIVE_PROTOCOL = 2


def active_protocol() -> int:
    """Protocol the current test is running under (2 or 3)."""
    return _ACTIVE_PROTOCOL


def set_active_protocol(proto: int) -> None:
    global _ACTIVE_PROTOCOL
    _ACTIVE_PROTOCOL = proto


def resp_params() -> list[int]:
    """Protocols to parametrize over, honouring KIVI_RESP."""
    sel = os.environ.get("KIVI_RESP", "").strip()
    if sel == "2":
        return [2]
    if sel == "3":
        return [3]
    return [2, 3]


# ── Search replies, in one shape ──────────────────────────────────────────────
#
# Under RESP3 the search commands answer with a map — `total_results`,
# `results`, … — where under RESP2 they answer with a flat array. That is what
# Redis does, and what kividb does. The tests in this suite index into the
# array, so that they can be read against the documentation of either
# protocol; a client under RESP3 therefore turns the map back into the array
# as it arrives. The wire shape itself is asserted in the Rust tests.

def _key(mapping, name):
    """`mapping[name]`, whether the client decodes replies or not."""
    if name in mapping:
        return mapping[name]
    return mapping.get(name.encode())


def _pairs(mapping) -> list:
    out = []
    for k, v in mapping.items():
        out.append(k)
        out.append(_pairs(v) if isinstance(v, dict) else v)
    return out


def _search_array(reply, **_options):
    if not isinstance(reply, dict):
        return reply
    out = [_key(reply, "total_results")]
    for result in _key(reply, "results") or []:
        doc_id = _key(result, "id")
        if doc_id is not None:
            out.append(doc_id)
            score = _key(result, "score")
            if score is not None:
                out.append(str(score) if not isinstance(score, bytes) else score)
        fields = _key(result, "extra_attributes")
        if fields is not None:
            out.append(_pairs(fields))
    return out


def _info_array(reply, **_options):
    return _pairs(reply) if isinstance(reply, dict) else reply


def _one_search_shape(client: redis.Redis) -> redis.Redis:
    for command in ("FT.SEARCH", "FT.AGGREGATE", "FT.HYBRID"):
        client.set_response_callback(command, _search_array)
    client.set_response_callback("FT.INFO", _info_array)
    return client


def make_client(**kwargs) -> redis.Redis:
    """Return a connected redis.Redis client pointed at the test server.

    Defaults to whichever protocol the current test is parametrized for (see
    `active_protocol`). Pass `protocol=` explicitly to override, for tests that
    pin one protocol on purpose.

    This used to hardcode `protocol=2`, because kividb implemented RESP2 only
    and redis-py's own default had moved to RESP3 — an ordinary `pip install`
    picking up a newer redis-py would silently negotiate a protocol the server
    could not speak and break the suite at handshake. kividb now implements
    RESP3, so the pin became a parametrization instead of being removed: RESP2
    must keep working exactly as before, and the only way to know that is to
    keep testing it.
    """
    kwargs.setdefault("protocol", active_protocol())
    return _one_search_shape(redis.Redis(
        host=KIVI_HOST,
        port=KIVI_PORT,
        decode_responses=True,
        **kwargs,
    ))


def make_bytes_client(**kwargs) -> redis.Redis:
    """Return a client that returns raw bytes (needed for binary/vector data).

    Follows the protocol under test — see `make_client`.
    """
    kwargs.setdefault("protocol", active_protocol())
    return _one_search_shape(redis.Redis(
        host=KIVI_HOST,
        port=KIVI_PORT,
        decode_responses=False,
        **kwargs,
    ))


# ── pytest fixtures ────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def kivi_server():
    """
    Session-scoped fixture — manages the test server process.

    If KIVI_EXTERNAL=1 is set (CI launches the server separately), this is a
    no-op.  Otherwise it starts the binary resolved by _find_server_binary()
    (defaults to redis-stack-server) and tears it down after the session.

    redis-stack-server flags used:
      --save ""        disable RDB snapshots so tests start with an empty store
      --appendonly no  disable AOF
    These keep the server stateless across test runs.
    """
    if os.environ.get("KIVI_EXTERNAL", "0") == "1":
        if not wait_for_port(KIVI_HOST, KIVI_PORT, timeout=15):
            pytest.fail(f"Server not reachable at {KIVI_HOST}:{KIVI_PORT} after 15s")
        yield
        return

    if _IS_REDIS:
        cmd = [
            KIVI_BINARY,
            "--port", str(KIVI_PORT),
            "--loglevel", "warning",
            "--save", "",
            "--appendonly", "no",
            "--protected-mode", "no",
            "--dir", "/tmp",
        ]
    else:
        # kividb-specific flags
        cmd = [
            KIVI_BINARY,
            "--port", str(KIVI_PORT),
            "--loglevel", "error",
        ]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if not wait_for_port(KIVI_HOST, KIVI_PORT, timeout=30):
        proc.terminate()
        pytest.fail(
            f"Server failed to start within 30s (binary: {KIVI_BINARY})"
        )
    yield
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(params=resp_params(), ids=lambda p: f"resp{p}")
def resp_protocol(request) -> Generator[int, None, None]:
    """
    Protocol the test runs under. Every client fixture depends on this, so the
    whole suite runs once per protocol.

    It also sets the ambient protocol for the duration of the test, so clients
    that tests construct themselves via `make_client()` follow the same
    protocol instead of silently staying on RESP2.
    """
    previous = active_protocol()
    set_active_protocol(request.param)
    yield request.param
    set_active_protocol(previous)


@pytest.fixture
def r(kivi_server, resp_protocol) -> Generator[redis.Redis, None, None]:
    """Function-scoped client with a clean DB (FLUSHALL before each test)."""
    client = make_client()
    client.flushall()
    yield client
    client.close()


@pytest.fixture
def rb(kivi_server, resp_protocol) -> Generator[redis.Redis, None, None]:
    """Function-scoped bytes client with a clean DB."""
    client = make_bytes_client()
    client.flushall()
    yield client
    client.close()


@pytest.fixture
def r_pool(kivi_server, resp_protocol) -> Generator[redis.Redis, None, None]:
    """Function-scoped client backed by a connection pool (for parallel tests)."""
    pool = redis.ConnectionPool(
        host=KIVI_HOST,
        port=KIVI_PORT,
        decode_responses=True,
        max_connections=50,
        protocol=resp_protocol,
    )
    client = _one_search_shape(redis.Redis(connection_pool=pool))
    client.flushall()
    yield client
    pool.disconnect()


# ── Protocol-agnostic result normalisers ───────────────────────────────────────
#
# RESP2 and RESP3 legitimately hand back different Python shapes for the same
# data, and redis-py is the thing that differs — not the server. Two examples
# from its own callback tables:
#
#   ZRANGE ... WITHSCORES   RESP2 -> [('a', 1.0)]   RESP3 -> [['a', 1.0]]
#   ZDIFF  ... WITHSCORES   RESP2 -> ['a', '1']     RESP3 -> [['a', 1.0]]
#
# (`zset_score_pairs` builds tuples, `zset_score_pairs_resp3` builds lists, and
# some commands have a RESP2 pairing callback but no RESP3 one, or vice versa.)
#
# Tests that care about the *data* should normalise through these instead of
# hardcoding one protocol's container types. Tests that specifically assert a
# protocol's wire shape should pin `protocol=` explicitly and say so.


def score_pairs(result) -> list[tuple[str, float]]:
    """
    Normalise any WITHSCORES-style reply to `[(member, score), ...]`.

    Accepts the flat `[m, s, m, s]` form, the nested `[[m, s], ...]` /
    `[(m, s), ...]` forms, and returns scores as floats in all cases.
    """
    if not result:
        return []
    if isinstance(result[0], (list, tuple)):
        return [(m, float(s)) for m, s in result]
    it = iter(result)
    return [(m, float(s)) for m, s in zip(it, it)]


def xread_streams(result) -> list[tuple[str, list]]:
    """
    Normalise an XREAD / XREADGROUP reply to `[(stream_name, entries), ...]`.

    RESP2 gives `[[name, entries], ...]`; RESP3 gives a dict keyed by stream
    name, and redis-py's `parse_xread_resp3` additionally wraps each value in an
    extra list (`{name: [entries]}`). Both are unwrapped here. An empty reply
    (`None` on RESP2, `{}` on RESP3) normalises to `[]`.
    """
    if not result:
        return []
    if isinstance(result, dict):
        out = []
        for name, value in result.items():
            # redis-py wraps RESP3 values one level deeper than RESP2.
            if len(value) == 1 and isinstance(value[0], list):
                value = value[0]
            out.append((name, value))
        return out
    return [(name, entries) for name, entries in result]


def config_pairs(result) -> dict:
    """
    Normalise a raw `CONFIG GET` reply to a dict.

    RESP2 replies with a flat [name, value, ...] array; RESP3 replies with a
    map, which redis-py hands back as a dict. Keys/values are left exactly as
    the client produced them (bytes or str, depending on `decode_responses`).
    """
    if isinstance(result, dict):
        return dict(result)
    it = iter(result)
    return dict(zip(it, it))


def flat_pair(result) -> tuple | None:
    """
    Normalise a `[key, value]` reply (BLPOP/BRPOP) to a tuple.

    redis-py builds a tuple under RESP2 and passes the raw list through under
    RESP3; `None` (nothing popped) passes through unchanged.
    """
    if result is None:
        return None
    key, value = result
    return (key, value)


def flat_triple(result) -> tuple | None:
    """
    Normalise a `[key, member, score]` reply (BZPOPMIN/BZPOPMAX) to a tuple with
    a float score. RESP2 yields a tuple, RESP3 a list; `None` passes through.
    """
    if result is None:
        return None
    key, member, score = result
    return (key, member, float(score))


# ── Vector helpers ─────────────────────────────────────────────────────────────

def vec_to_bytes(floats: list[float]) -> bytes:
    """Encode a list of float32 values as little-endian bytes (FLOAT32 wire format)."""
    return struct.pack(f"<{len(floats)}f", *floats)


def bytes_to_vec(data: bytes) -> list[float]:
    """Decode little-endian float32 bytes back to a Python list."""
    n = len(data) // 4
    return list(struct.unpack(f"<{n}f", data))


def cosine_sim(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    return dot / (na * nb) if na and nb else 0.0


def l2_dist(a: list[float], b: list[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


# ── Assertion helpers ──────────────────────────────────────────────────────────

def assert_resp_error(response: Any, prefix: str = "ERR") -> None:
    """Assert a response is a Redis error string starting with prefix."""
    assert isinstance(response, (redis.ResponseError, Exception)) or (
        isinstance(response, str) and response.startswith(prefix)
    ), f"Expected error starting with {prefix!r}, got {response!r}"


def assert_approx(actual: float, expected: float, tol: float = 1e-4) -> None:
    assert abs(actual - expected) <= tol, f"{actual} not ≈ {expected} (tol={tol})"


def assert_type(client: redis.Redis, key: str, expected_type: str) -> None:
    assert client.type(key) == expected_type, (
        f"Expected type {expected_type!r} for key {key!r}, got {client.type(key)!r}"
    )


# ── Concurrent task helper ─────────────────────────────────────────────────────

def run_parallel(fns: list, n_threads: int = None) -> list:
    """
    Run a list of callables in parallel threads, collect results.
    Each fn should be a zero-argument callable.
    """
    results = [None] * len(fns)
    errors = [None] * len(fns)

    def runner(idx, fn):
        try:
            results[idx] = fn()
        except Exception as exc:
            errors[idx] = exc

    threads = [threading.Thread(target=runner, args=(i, fn)) for i, fn in enumerate(fns)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i, err in enumerate(errors):
        if err is not None:
            raise AssertionError(f"Thread {i} raised: {err}") from err

    return results
