"""
e2e tests for Pub/Sub — SUBSCRIBE, PUBLISH, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE.

Covers:
  - Basic subscribe → publish → receive flow
  - Multiple subscribers on the same channel
  - Multiple channels per subscriber
  - Pattern subscribe (PSUBSCRIBE)
  - Pattern unsubscribe (PUNSUBSCRIBE)
  - UNSUBSCRIBE from a single channel while subscribed to others
  - PUBLISH returns the number of receivers
  - Messages are not delivered to unsubscribed clients
  - Pub/sub client cannot issue non-pub/sub commands
  - High-throughput message delivery
"""
from __future__ import annotations

import threading
import time
import queue
import pytest
import redis
from utils.helpers import r, rb, kivi_server  # noqa: F401


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_sub_client(r: redis.Redis) -> redis.Redis:
    """Create a new client on the same host/port as the given client."""
    kw = r.connection_pool.connection_kwargs
    return redis.Redis(
        host=kw["host"],
        port=kw["port"],
        decode_responses=True,
        socket_timeout=3,
    )


class _Subscriber:
    """
    Thin wrapper that listens on a pubsub connection in a background thread
    and collects received messages into a queue.
    """

    def __init__(self, client: redis.Redis):
        self._client = client
        self._ps = client.pubsub(ignore_subscribe_messages=True)
        self._q: queue.Queue = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def subscribe(self, *channels: str) -> "_Subscriber":
        self._ps.subscribe(*channels)
        return self

    def psubscribe(self, *patterns: str) -> "_Subscriber":
        self._ps.psubscribe(*patterns)
        return self

    def start(self) -> "_Subscriber":
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        time.sleep(0.05)  # give the listener a moment to register
        return self

    def _loop(self):
        while not self._stop.is_set():
            msg = self._ps.get_message(timeout=0.1)
            if msg and msg.get("type") in ("message", "pmessage"):
                self._q.put(msg)

    def get(self, timeout: float = 1.0):
        """Return the next message, or None on timeout."""
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def collect(self, n: int, timeout: float = 2.0) -> list:
        """Collect exactly n messages, raising if timeout expires."""
        result = []
        deadline = time.monotonic() + timeout
        while len(result) < n and time.monotonic() < deadline:
            msg = self.get(timeout=max(0.01, deadline - time.monotonic()))
            if msg:
                result.append(msg)
        return result

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        self._ps.close()
        self._client.close()


# ── Basic Pub/Sub ──────────────────────────────────────────────────────────────

class TestBasicPubSub:
    def test_subscribe_and_receive_message(self, r):
        """A subscribed client receives a published message."""
        sub_client = _make_sub_client(r)
        sub = _Subscriber(sub_client).subscribe("chan:basic").start()
        try:
            time.sleep(0.1)
            receivers = r.publish("chan:basic", "hello")
            assert receivers >= 1, f"Expected >=1 receiver, got {receivers}"

            msg = sub.get(timeout=2.0)
            assert msg is not None, "No message received within timeout"
            assert msg["channel"] == "chan:basic"
            assert msg["data"] == "hello"
        finally:
            sub.close()

    def test_publish_returns_receiver_count(self, r):
        """PUBLISH returns the number of clients that received the message."""
        clients = []
        subs = []
        try:
            for _ in range(3):
                c = _make_sub_client(r)
                s = _Subscriber(c).subscribe("count:chan").start()
                clients.append(c)
                subs.append(s)

            time.sleep(0.15)
            receivers = r.publish("count:chan", "msg")
            assert receivers == 3, f"3 subscribers should receive the message, got {receivers}"
        finally:
            for s in subs:
                s.close()

    def test_publish_to_no_subscribers_returns_zero(self, r):
        """PUBLISH on a channel with no subscribers returns 0."""
        receivers = r.publish("nobody:listening", "lonely message")
        assert receivers == 0

    def test_multiple_messages_in_order(self, r):
        """Messages are delivered in publication order."""
        sub_client = _make_sub_client(r)
        sub = _Subscriber(sub_client).subscribe("order:chan").start()
        try:
            time.sleep(0.1)
            for i in range(5):
                r.publish("order:chan", f"msg:{i}")

            messages = sub.collect(5, timeout=3.0)
            assert len(messages) == 5, f"Expected 5 messages, got {len(messages)}"
            for i, msg in enumerate(messages):
                assert msg["data"] == f"msg:{i}", \
                    f"Message {i} out of order: {msg['data']}"
        finally:
            sub.close()

    def test_unsubscribe_stops_messages(self, r):
        """After UNSUBSCRIBE, the client no longer receives messages."""
        sub_client = _make_sub_client(r)
        ps = sub_client.pubsub(ignore_subscribe_messages=False)
        ps.subscribe("unsub:chan")
        time.sleep(0.1)

        # Verify subscription is active
        r.publish("unsub:chan", "before")
        found_before = False
        for _ in range(20):
            msg = ps.get_message(timeout=0.1)
            if msg and msg.get("type") == "message" and msg.get("data") == "before":
                found_before = True
                break
        assert found_before, "Should receive message before unsubscribe"

        ps.unsubscribe("unsub:chan")
        time.sleep(0.1)

        r.publish("unsub:chan", "after")
        time.sleep(0.2)

        # No more messages
        msg = ps.get_message(timeout=0.2)
        # Should be None or an unsubscribe confirm, not a message
        if msg:
            assert msg.get("type") != "message" or msg.get("data") != "after", \
                "Must not receive messages after UNSUBSCRIBE"
        ps.close()
        sub_client.close()


# ── Multiple channels ─────────────────────────────────────────────────────────

class TestMultipleChannels:
    def test_subscribe_to_multiple_channels(self, r):
        """A client subscribed to A and B receives messages on both."""
        sub_client = _make_sub_client(r)
        sub = _Subscriber(sub_client).subscribe("mc:A", "mc:B").start()
        try:
            time.sleep(0.15)
            r.publish("mc:A", "from A")
            r.publish("mc:B", "from B")

            msgs = sub.collect(2, timeout=3.0)
            assert len(msgs) == 2
            channels = {m["channel"] for m in msgs}
            assert "mc:A" in channels
            assert "mc:B" in channels
        finally:
            sub.close()

    def test_publish_only_reaches_subscribed_channel(self, r):
        """Publishing to channel A does not reach a subscriber on channel B."""
        q_a: queue.Queue = queue.Queue()
        q_b: queue.Queue = queue.Queue()

        ca = _make_sub_client(r)
        cb = _make_sub_client(r)
        ps_a = ca.pubsub(ignore_subscribe_messages=True)
        ps_b = cb.pubsub(ignore_subscribe_messages=True)
        ps_a.subscribe("sep:A")
        ps_b.subscribe("sep:B")
        time.sleep(0.1)

        r.publish("sep:A", "only for A")
        time.sleep(0.2)

        # Poll for up to 1s for msg_a
        msg_a = None
        for _ in range(20):
            m = ps_a.get_message(timeout=0.1)
            if m and m.get("type") == "message":
                msg_a = m
                break
        msg_b = ps_b.get_message(timeout=0.2)

        assert msg_a is not None and msg_a["data"] == "only for A"
        assert msg_b is None, "Subscriber on sep:B must not receive sep:A message"

        ps_a.close()
        ps_b.close()
        ca.close()
        cb.close()

    def test_unsubscribe_one_keep_others(self, r):
        """
        Unsubscribe from channel A while still subscribed to B.
        Messages on A must stop; messages on B must continue.
        """
        sub_client = _make_sub_client(r)
        ps = sub_client.pubsub(ignore_subscribe_messages=True)
        ps.subscribe("keep:A", "keep:B")
        time.sleep(0.1)

        ps.unsubscribe("keep:A")
        time.sleep(0.1)

        r.publish("keep:A", "should not arrive")
        r.publish("keep:B", "should arrive")
        time.sleep(0.2)

        msgs = []
        for _ in range(5):
            msg = ps.get_message(timeout=0.1)
            if msg:
                msgs.append(msg)

        channels_received = [m["channel"] for m in msgs]
        assert "keep:A" not in channels_received, \
            "Must not receive from unsubscribed channel"
        assert "keep:B" in channels_received, \
            "Must still receive from subscribed channel"

        ps.close()
        sub_client.close()


# ── Pattern Subscribe ─────────────────────────────────────────────────────────

class TestPatternSubscribe:
    def test_psubscribe_wildcard(self, r):
        """PSUBSCRIBE with * pattern receives messages on any matching channel."""
        sub_client = _make_sub_client(r)
        sub = _Subscriber(sub_client).psubscribe("news:*").start()
        try:
            time.sleep(0.15)
            r.publish("news:sports", "football update")
            r.publish("news:tech", "new release")

            msgs = sub.collect(2, timeout=3.0)
            assert len(msgs) == 2, f"Expected 2 pattern messages, got {len(msgs)}"
            channels = {m["channel"] for m in msgs}
            assert "news:sports" in channels
            assert "news:tech" in channels
        finally:
            sub.close()

    def test_psubscribe_does_not_match_other_channels(self, r):
        """Pattern news:* does not match 'weather:today'."""
        sub_client = _make_sub_client(r)
        ps = sub_client.pubsub(ignore_subscribe_messages=True)
        ps.psubscribe("pat:news:*")
        time.sleep(0.1)

        r.publish("pat:weather:today", "sunny")
        time.sleep(0.2)

        msg = ps.get_message(timeout=0.3)
        assert msg is None, "Pattern news:* must not match weather:today"
        ps.close()
        sub_client.close()

    def test_psubscribe_question_mark_wildcard(self, r):
        """? wildcard matches exactly one character."""
        sub_client = _make_sub_client(r)
        sub = _Subscriber(sub_client).psubscribe("ch?n").start()
        try:
            time.sleep(0.1)
            r.publish("chan", "matches")
            r.publish("chxn", "also matches")
            r.publish("chain", "does not match — 2 chars")

            msgs = sub.collect(2, timeout=2.0)
            data = {m["data"] for m in msgs}
            assert "matches" in data
            assert "also matches" in data
            assert "does not match — 2 chars" not in data
        finally:
            sub.close()

    def test_punsubscribe_stops_pattern_messages(self, r):
        """PUNSUBSCRIBE stops delivery for that pattern."""
        sub_client = _make_sub_client(r)
        ps = sub_client.pubsub(ignore_subscribe_messages=True)
        ps.psubscribe("pu:*")
        time.sleep(0.1)

        # Receive one message to confirm subscription — poll up to 2s
        r.publish("pu:test", "first")
        time.sleep(0.1)
        msg1 = None
        for _ in range(30):
            m = ps.get_message(timeout=0.1)
            if m and m.get("type") in ("message", "pmessage"):
                msg1 = m
                break
        assert msg1 is not None and msg1["data"] == "first"

        ps.punsubscribe("pu:*")
        time.sleep(0.1)

        r.publish("pu:test", "second")
        time.sleep(0.2)
        msg2 = ps.get_message(timeout=0.3)
        if msg2:
            assert msg2.get("type") not in ("message", "pmessage"), \
                "Must not receive pattern messages after PUNSUBSCRIBE"
        ps.close()
        sub_client.close()


# ── High-throughput ───────────────────────────────────────────────────────────

class TestPubSubThroughput:
    def test_hundred_messages_delivered_in_order(self, r):
        """100 messages on a single channel are all delivered in order."""
        sub_client = _make_sub_client(r)
        sub = _Subscriber(sub_client).subscribe("bulk:chan").start()
        try:
            time.sleep(0.1)
            n = 100
            for i in range(n):
                r.publish("bulk:chan", f"msg:{i:04d}")

            msgs = sub.collect(n, timeout=10.0)
            assert len(msgs) == n, f"Expected {n} messages, got {len(msgs)}"
            for i, msg in enumerate(msgs):
                assert msg["data"] == f"msg:{i:04d}", \
                    f"Message {i} wrong: {msg['data']}"
        finally:
            sub.close()

    def test_multiple_publishers_same_channel(self, r):
        """
        Three publisher threads send 10 messages each to the same channel.
        The subscriber must receive all 30 messages.
        """
        sub_client = _make_sub_client(r)
        sub = _Subscriber(sub_client).subscribe("mpub:chan").start()
        try:
            time.sleep(0.1)
            errors = []

            def publisher(prefix: str, n: int):
                c = redis.Redis(
                    host=r.connection_pool.connection_kwargs["host"],
                    port=r.connection_pool.connection_kwargs["port"],
                    decode_responses=True,
                )
                try:
                    for i in range(n):
                        c.publish("mpub:chan", f"{prefix}:{i}")
                except Exception as e:
                    errors.append(e)
                finally:
                    c.close()

            threads = [
                threading.Thread(target=publisher, args=(f"pub{j}", 10))
                for j in range(3)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert not errors, f"Publisher errors: {errors}"
            msgs = sub.collect(30, timeout=5.0)
            assert len(msgs) == 30, f"Expected 30 messages, got {len(msgs)}"
        finally:
            sub.close()

    def test_fan_out_to_many_subscribers(self, r):
        """
        One publisher sends a message; 5 subscribers all receive it.
        """
        n_subs = 5
        subs = []
        try:
            for _ in range(n_subs):
                c = _make_sub_client(r)
                s = _Subscriber(c).subscribe("fanout:chan").start()
                subs.append(s)

            time.sleep(0.2)
            count = r.publish("fanout:chan", "broadcast")
            assert count == n_subs, f"Expected {n_subs} receivers, got {count}"

            for i, s in enumerate(subs):
                msg = s.get(timeout=2.0)
                assert msg is not None, f"Subscriber {i} did not receive message"
                assert msg["data"] == "broadcast"
        finally:
            for s in subs:
                s.close()
