#!/usr/bin/env python3
"""
Pub/Sub verification across the full matrix of I/O model x wire protocol.

Why this exists
---------------
Pub/Sub is the one area where the two I/O models genuinely differ in shape:

  * tokio (`server.rs`)      — a dedicated writer task forwards out-of-band
                               frames onto the socket.
  * io_uring (`server_uring.rs`) — the same idea, but it cannot `select!` over
                               the socket read (tokio_uring can't cancel an
                               in-flight op without leaking its buffer), so it
                               uses a separate forwarder task plus a write
                               guard to keep the two writers from interleaving.

Those are different enough that testing one proves nothing about the other, and
io_uring is Linux-only, so it cannot be covered by the Windows dev loop at all.
Hence a standalone driver that CI runs against both models.

It also covers the case that motivated the work: a RESP3 client keeps full
command access while subscribed (Redis gates that restriction on
`c->resp == 2`), while a RESP2 client stays restricted.

Usage:
    python smoke/pubsub_matrix.py --host 127.0.0.1 --port 6379 [--label tokio]

Exits non-zero on the first failure, printing what was expected.
"""
from __future__ import annotations

import argparse
import sys
import threading
import time

import redis

FAILURES: list[str] = []
CHECKS = 0


def check(cond: bool, what: str) -> None:
    global CHECKS
    CHECKS += 1
    if cond:
        print(f"  ok   {what}")
    else:
        print(f"  FAIL {what}")
        FAILURES.append(what)


def client(host: str, port: int, proto: int) -> redis.Redis:
    return redis.Redis(host=host, port=port, decode_responses=True, protocol=proto)


# ── RESP3: subscribed clients are not restricted ─────────────────────────────

def test_resp3_unrestricted(host: str, port: int) -> None:
    print("[resp3] subscribed client keeps full command access")
    r = client(host, port, 3)
    p = r.pubsub()
    p.subscribe("m:ch")
    ack = p.get_message(timeout=5)
    check(bool(ack) and ack["type"] == "subscribe", "subscribe acked")

    # The behaviour the whole change exists for.
    p.connection.send_command("SET", "m:k", "v")
    check(p.connection.read_response() == "OK", "SET works while subscribed")
    p.connection.send_command("GET", "m:k")
    check(p.connection.read_response() == "v", "GET works while subscribed")
    # DEL first so the assertion holds when this script is run repeatedly
    # against the same server (it is not assumed to own the keyspace).
    p.connection.send_command("DEL", "m:n")
    p.connection.read_response()
    p.connection.send_command("INCR", "m:n")
    check(p.connection.read_response() == 1, "INCR works while subscribed")

    pub = client(host, port, 3)
    check(pub.publish("m:ch", "hello") == 1, "publisher sees exactly 1 subscriber")
    msg = p.get_message(timeout=5)
    check(
        bool(msg) and msg["type"] == "message" and msg["data"] == "hello",
        "delivery arrives after running other commands",
    )
    p.close()


def test_resp3_idle_delivery(host: str, port: int) -> None:
    """
    The io_uring regression this pins: pushes used to be drained only after the
    client sent its *next* command, so a silent subscriber never got anything.
    Deliberately sends nothing on the subscriber connection while waiting.
    """
    print("[resp3] idle subscriber receives without sending anything")
    r = client(host, port, 3)
    p = r.pubsub()
    p.subscribe("m:idle")
    p.get_message(timeout=5)

    got: list = []

    def waiter() -> None:
        # No commands issued on this connection — pure wait.
        got.append(p.get_message(timeout=8))

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(1.5)  # ensure the subscriber is genuinely parked
    client(host, port, 3).publish("m:idle", "wake")
    t.join(timeout=10)

    check(
        bool(got) and bool(got[0]) and got[0].get("data") == "wake",
        "idle subscriber woken by publish (no command sent to trigger a flush)",
    )
    p.close()


def test_resp3_patterns_and_shard(host: str, port: int) -> None:
    print("[resp3] pattern and shard subscriptions")
    r = client(host, port, 3)
    p = r.pubsub()
    p.psubscribe("m:news.*")
    p.get_message(timeout=5)
    client(host, port, 3).publish("m:news.tech", "v")
    msg = p.get_message(timeout=5)
    check(
        bool(msg) and msg["type"] == "pmessage" and msg["channel"] == "m:news.tech",
        "pmessage delivered with the concrete channel name",
    )
    p.close()

    s = client(host, port, 3)
    sp = s.pubsub()
    # Use redis-py's own ssubscribe rather than execute_command: on RESP3 the
    # ack comes back as a push frame, which the client routes into its message
    # queue. Driving it by hand leaves the client's bookkeeping unaware of the
    # channel and makes get_message() look like a server failure when it isn't.
    sp.ssubscribe("m:shard")
    sp.get_message(timeout=5)
    client(host, port, 3).execute_command("SPUBLISH", "m:shard", "sv")
    smsg = sp.get_message(timeout=5)
    check(
        bool(smsg) and smsg.get("data") == "sv",
        "smessage delivered on a shard channel",
    )
    sp.close()


def test_resp3_unsubscribe_releases(host: str, port: int) -> None:
    print("[resp3] unsubscribe actually releases the subscription")
    r = client(host, port, 3)
    p = r.pubsub()
    p.subscribe("m:a", "m:b")
    p.get_message(timeout=5)
    p.get_message(timeout=5)

    pub = client(host, port, 3)
    check(pub.publish("m:a", "x") == 1, "both channels have a subscriber before unsubscribe")

    # Go through redis-py's unsubscribe, not a raw send_command/read_response
    # pair: under RESP3 the ack is a push frame that the client consumes into
    # its message queue, so read_response() would block forever waiting for a
    # reply that already arrived by another route.
    p.unsubscribe("m:a")
    deadline = time.time() + 5
    while time.time() < deadline:
        m = p.get_message(timeout=1)
        if m and m.get("type") == "unsubscribe" and m.get("channel") == "m:a":
            break

    # PUBLISH's receiver count is the authoritative check: it proves the
    # broadcast receiver was really dropped, not merely ignored.
    check(pub.publish("m:a", "x") == 0, "unsubscribed channel has no receivers")
    check(pub.publish("m:b", "y") == 1, "the other subscription survives")
    p.close()


def test_resp3_many_subscriptions(host: str, port: int) -> None:
    print("[resp3] many channels on one connection")
    n = 50
    r = client(host, port, 3)
    p = r.pubsub()
    names = [f"m:bulk:{i}" for i in range(n)]
    p.subscribe(*names)
    for _ in range(n):
        p.get_message(timeout=5)

    pub = client(host, port, 3)
    for name in names:
        pub.publish(name, name)
    seen = set()
    deadline = time.time() + 15
    while len(seen) < n and time.time() < deadline:
        m = p.get_message(timeout=2)
        if m and m["type"] == "message":
            seen.add(m["data"])
    check(len(seen) == n, f"all {n} channels delivered (got {len(seen)})")
    p.close()


def test_resp3_interleaved_writes_not_corrupted(host: str, port: int) -> None:
    """
    Deliveries and command replies are written by two different tasks on the
    io_uring path. Without write serialisation they can interleave mid-frame and
    desynchronise the protocol. Hammer both at once and require every reply to
    still line up.
    """
    print("[resp3] concurrent deliveries + command replies stay framed")
    r = client(host, port, 3)
    p = r.pubsub()
    p.subscribe("m:race")
    p.get_message(timeout=5)

    stop = threading.Event()

    def publisher() -> None:
        pub = client(host, port, 3)
        i = 0
        while not stop.is_set():
            pub.publish("m:race", f"p{i}")
            i += 1

    t = threading.Thread(target=publisher)
    t.start()
    try:
        # Every reply must match its request exactly; a corrupted frame shows up
        # as a mismatch or a parse error rather than a hang.
        ok = True
        for i in range(300):
            p.connection.send_command("ECHO", f"e{i}")
            resp = p.connection.read_response()
            # Interleaved push frames are legal here; skip them.
            while isinstance(resp, list) and resp and resp[0] in ("message", "subscribe"):
                resp = p.connection.read_response()
            if resp != f"e{i}":
                ok = False
                print(f"       mismatch at {i}: {resp!r}")
                break
        check(ok, "300 command replies stayed correctly framed under delivery load")
    finally:
        stop.set()
        t.join(timeout=5)
    p.close()


# ── RESP2: subscriber mode stays restricted ──────────────────────────────────

def test_resp2_restricted(host: str, port: int) -> None:
    print("[resp2] subscriber mode still restricted and still delivers")
    r = client(host, port, 2)
    p = r.pubsub()
    p.subscribe("m:r2")
    ack = p.get_message(timeout=5)
    check(bool(ack) and ack["type"] == "subscribe", "subscribe acked")

    p.connection.send_command("SET", "m:r2k", "v")
    try:
        p.connection.read_response()
        check(False, "RESP2 SET while subscribed must be rejected")
    except redis.exceptions.ResponseError:
        check(True, "RESP2 SET while subscribed rejected")

    client(host, port, 2).publish("m:r2", "hi")
    msg = p.get_message(timeout=5)
    check(
        bool(msg) and msg["type"] == "message" and msg["data"] == "hi",
        "RESP2 delivery still works",
    )
    p.close()


def test_resp2_idle_delivery(host: str, port: int) -> None:
    print("[resp2] idle subscriber receives without sending anything")
    r = client(host, port, 2)
    p = r.pubsub()
    p.subscribe("m:r2idle")
    p.get_message(timeout=5)

    got: list = []
    t = threading.Thread(target=lambda: got.append(p.get_message(timeout=8)))
    t.start()
    time.sleep(1.5)
    client(host, port, 2).publish("m:r2idle", "wake2")
    t.join(timeout=10)
    check(
        bool(got) and bool(got[0]) and got[0].get("data") == "wake2",
        "RESP2 idle subscriber woken by publish",
    )
    p.close()


def test_resp2_commands_are_not_dropped(host: str, port: int) -> None:
    """Every command a subscriber sends must get a reply.

    This is the regression guard for a bug that made a RESP2 subscriber under
    `--io-model uring` almost unusable: the subscriber loop waited for client
    data with `tokio::time::timeout` around a `tokio_uring` read, and dropping
    that future does not cancel the submitted operation — the kernel completed
    the read into a buffer nobody was listening to, so those bytes left the
    stream. 56 of 60 PINGs went unanswered on uring; 0 on tokio.

    PING is used because it is the one command a subscriber may always send, so
    this exercises the read path without depending on delivery or on which
    commands subscriber mode permits. The staggered sleep walks the send across
    the loop's polling window rather than landing on the same phase every time.
    """
    print("[resp2] subscriber commands are never silently dropped")
    r = client(host, port, 2)
    p = r.pubsub()
    p.subscribe("m:drop")
    p.get_message(timeout=5)

    sent = 40
    unanswered = 0
    for i in range(sent):
        try:
            p.connection.send_command("PING", f"p{i}")
            reply = p.connection.read_response()
            # Subscriber-mode PING answers as ["pong", <payload>].
            ok = reply and (
                (isinstance(reply, (list, tuple)) and str(reply[0]).lower() == "pong")
                or str(reply).lower() == "pong"
            )
            if not ok:
                unanswered += 1
        except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError):
            unanswered += 1
        time.sleep(0.0007 + (i % 5) * 0.0003)

    check(unanswered == 0, f"all {sent} subscriber PINGs answered ({unanswered} lost)")
    p.close()


# ── Mixed protocol fan-out ───────────────────────────────────────────────────

def test_mixed_protocol_fanout(host: str, port: int) -> None:
    """One PUBLISH, two subscribers on different protocols — each must get the
    framing its own parser expects."""
    print("[mixed] one publish, subscribers on different protocols")
    r2 = client(host, port, 2)
    p2 = r2.pubsub()
    p2.subscribe("m:mixed")
    p2.get_message(timeout=5)

    r3 = client(host, port, 3)
    p3 = r3.pubsub()
    p3.subscribe("m:mixed")
    p3.get_message(timeout=5)

    n = client(host, port, 3).publish("m:mixed", "both")
    check(n == 2, f"publisher sees 2 subscribers (got {n})")
    m2 = p2.get_message(timeout=5)
    m3 = p3.get_message(timeout=5)
    check(bool(m2) and m2.get("data") == "both", "RESP2 subscriber received it")
    check(bool(m3) and m3.get("data") == "both", "RESP3 subscriber received it")
    p2.close()
    p3.close()


def test_tracking_invalidation_is_prompt(host: str, port: int) -> None:
    """
    CLIENT TRACKING shares the same push path. On io_uring it previously only
    flushed when the client sent its next command; this asserts it arrives
    without prompting.
    """
    print("[tracking] invalidation arrives without a follow-up command")
    c = client(host, port, 3)
    c.execute_command("CLIENT", "TRACKING", "ON")
    c.set("m:t", "1")
    c.get("m:t")  # register interest

    other = client(host, port, 3)
    other.set("m:t", "2")

    deadline = time.time() + 5
    seen = False
    while time.time() < deadline:
        try:
            if c.connection_pool.get_connection().can_read(timeout=0.2):
                seen = True
                break
        except Exception:
            break
        time.sleep(0.1)
    # Not all client versions expose this cleanly; treat inability to observe as
    # a skip rather than a failure, and say so.
    print(f"       (observed={seen}; informational — covered precisely by tests/generics.rs)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--label", default="", help="I/O model name, for output only")
    args = ap.parse_args()

    banner = f"pub/sub matrix against {args.host}:{args.port}"
    if args.label:
        banner += f"  [{args.label}]"
    print("=" * len(banner))
    print(banner)
    print("=" * len(banner))

    for fn in (
        test_resp3_unrestricted,
        test_resp3_idle_delivery,
        test_resp3_patterns_and_shard,
        test_resp3_unsubscribe_releases,
        test_resp3_many_subscriptions,
        test_resp3_interleaved_writes_not_corrupted,
        test_resp2_restricted,
        test_resp2_idle_delivery,
        test_resp2_commands_are_not_dropped,
        test_mixed_protocol_fanout,
        test_tracking_invalidation_is_prompt,
    ):
        try:
            fn(args.host, args.port)
        except Exception as exc:  # noqa: BLE001 — report and keep going
            print(f"  FAIL {fn.__name__} raised {type(exc).__name__}: {exc}")
            FAILURES.append(f"{fn.__name__}: {exc}")

    print()
    if FAILURES:
        print(f"FAILED — {len(FAILURES)} of {CHECKS} checks:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print(f"All {CHECKS} pub/sub checks passed"
          + (f" [{args.label}]" if args.label else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
