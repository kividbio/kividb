# tests/e2e/modules/test_aggregate_hello_kill.py
#
# E2E tests for:
#   1. FT.AGGREGATE FILTER clause  — post-aggregate row filtering
#   2. FT.AGGREGATE APPLY clause   — computed fields
#   3. HELLO command               — client handshake and protocol negotiation
#   4. CLIENT KILL                 — real socket close
#
# Run with:
#   pytest tests/e2e/modules/test_aggregate_hello_kill.py -v
#
# Or against an already-running server:
#   KIVI_EXTERNAL=1 KIVI_PORT=6380 pytest tests/e2e/modules/test_aggregate_hello_kill.py -v

import time
import socket
import threading
import pytest
import redis
from utils.helpers import rb, r, KIVI_HOST, KIVI_PORT, config_pairs  # noqa: F401 — fixtures


# ── Shared helpers ────────────────────────────────────────────────────────────

def decode(v):
    """Decode bytes to str; pass through if already str."""
    return v.decode() if isinstance(v, bytes) else v


def hello_info(raw):
    """Normalize a raw HELLO reply into a plain {str: value} dict.

    redis-py parses the reply differently depending on the connection's
    negotiated protocol: RESP2 connections get back a flat list of
    alternating key/value pairs, RESP3 connections get back a real dict
    (the server replies with an actual `%` map once RESP3 is negotiated).
    Client defaults have moved to RESP3 in recent redis-py releases, so a
    HELLO test needs to accept either shape rather than assume RESP2.
    """
    if isinstance(raw, dict):
        return {decode(k): decode(v) for k, v in raw.items()}
    flat = [decode(x) for x in raw]
    return dict(zip(flat[::2], flat[1::2]))


def parse_aggregate_rows(raw):
    """
    FT.AGGREGATE response — two formats depending on version/pipeline:
      Flat:   [N, row, row, ...]          each row is [field, value, ...]
      Nested: [N, [row, row, ...]]        all rows inside a single list
    Nested format is used in newer RediSearch when LOAD is present.
    Returns: (count: int, rows: list[dict[str, str]])
    """
    if not raw:
        return 0, []
    # Detect nested format: raw has exactly 2 elements and raw[1] is a list
    # whose first element is itself a list (a row).
    second = raw[1] if len(raw) > 1 else None
    if (second is not None and isinstance(second, (list, tuple)) and
            (len(second) == 0 or isinstance(second[0], (list, tuple)))):
        items = second
    else:
        items = raw[1:]
    rows = []
    for item in items:
        flat = [decode(x) for x in item]
        row = {}
        for i in range(0, len(flat) - 1, 2):
            row[flat[i]] = flat[i + 1]
        rows.append(row)
    return len(rows), rows


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def product_index(rb):
    """
    Create an FT index over hash keys with prefix 'prod:' and load 5 products.
    Automatically torn down after the test.

    Products:
      prod:1  Laptop      electronics  999
      prod:2  Phone       electronics  499
      prod:3  Tablet      electronics  299
      prod:4  Desk        furniture    200
      prod:5  Chair       furniture    150
    """
    rb.execute_command(
        "FT.CREATE", "pidx",
        "ON", "HASH",
        "PREFIX", "1", "prod:",
        "SCHEMA",
        "name",     "TEXT",
        "category", "TAG",
        "price",    "NUMERIC",
    )

    products = [
        ("prod:1", "Laptop",  "electronics", 999),
        ("prod:2", "Phone",   "electronics", 499),
        ("prod:3", "Tablet",  "electronics", 299),
        ("prod:4", "Desk",    "furniture",   200),
        ("prod:5", "Chair",   "furniture",   150),
    ]
    for key, name, cat, price in products:
        rb.hset(key.encode(), mapping={
            b"name":     name.encode(),
            b"category": cat.encode(),
            b"price":    str(price).encode(),
        })

    # Small sleep so the index backfill has time to run
    time.sleep(0.05)
    yield rb

    try:
        rb.execute_command("FT.DROPINDEX", "pidx")
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# FT.AGGREGATE FILTER tests
# ══════════════════════════════════════════════════════════════════════════════

class TestAggregateFilter:
    """Post-aggregate FILTER clause — filters rows after GROUPBY/REDUCE."""

    def test_filter_gt_on_count(self, product_index):
        """
        GROUPBY @category REDUCE COUNT → FILTER "@cnt > 2"
        electronics has 3, furniture has 2 → only electronics passes.
        """
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "cnt",
            "FILTER", "@cnt > 2",
        )
        count, rows = parse_aggregate_rows(raw)
        categories = [r["category"] for r in rows]
        assert "electronics" in categories
        assert "furniture" not in categories

    def test_filter_gte_keeps_boundary(self, product_index):
        """FILTER "@cnt >= 2" keeps furniture (cnt=2) and electronics (cnt=3)."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "cnt",
            "FILTER", "@cnt >= 2",
        )
        count, rows = parse_aggregate_rows(raw)
        categories = {r["category"] for r in rows}
        assert categories == {"electronics", "furniture"}

    def test_filter_eq_string(self, product_index):
        """FILTER '@category == "electronics"' keeps only electronics rows."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "LOAD", "1", "@category",
            "FILTER", '@category == "electronics"',
        )
        _, rows = parse_aggregate_rows(raw)
        assert len(rows) == 3
        for row in rows:
            assert row["category"] == "electronics"

    def test_filter_combined_and(self, product_index):
        """FILTER "@cnt > 1 && @cnt < 4" — both categories pass (2 and 3)."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "cnt",
            "FILTER", "@cnt > 1 && @cnt < 4",
        )
        count, rows = parse_aggregate_rows(raw)
        assert count == 2

    def test_filter_false_returns_empty(self, product_index):
        """FILTER that matches nothing returns 0 rows."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "cnt",
            "FILTER", "@cnt > 100",
        )
        _, rows = parse_aggregate_rows(raw)
        assert len(rows) == 0
        assert rows == []

    def test_filter_on_sum(self, product_index):
        """GROUPBY + REDUCE SUM → FILTER on the sum value."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "GROUPBY", "1", "@category",
            "REDUCE", "SUM", "1", "@price", "AS", "total",
            "FILTER", "@total > 500",
        )
        _, rows = parse_aggregate_rows(raw)
        # electronics total = 1797, furniture total = 350 — both > 500? No.
        # electronics: 999+499+299 = 1797 > 500 ✓
        # furniture: 200+150 = 350 < 500 ✗
        assert len(rows) == 1
        assert rows[0]["category"] == "electronics"


# ══════════════════════════════════════════════════════════════════════════════
# FT.AGGREGATE APPLY tests
# ══════════════════════════════════════════════════════════════════════════════

class TestAggregateApply:
    """APPLY clause — computed fields added to every row."""

    def test_apply_arithmetic(self, product_index):
        """APPLY '@price * 1.1' AS price_with_tax adds a computed field."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "LOAD", "1", "@price",
            "APPLY", "@price * 1.1", "AS", "price_with_tax",
        )
        count, rows = parse_aggregate_rows(raw)
        assert count == 5
        for row in rows:
            assert "price_with_tax" in row
            price = float(row["price"])
            tax = float(row["price_with_tax"])
            assert abs(tax - price * 1.1) < 0.01

    def test_apply_string_function(self, product_index):
        """APPLY 'upper(@category)' AS cat_upper converts to uppercase."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "LOAD", "1", "@category",
            "APPLY", "upper(@category)", "AS", "cat_upper",
        )
        count, rows = parse_aggregate_rows(raw)
        assert count == 5
        for row in rows:
            assert row["cat_upper"] in {"ELECTRONICS", "FURNITURE"}
            assert row["cat_upper"] == row["category"].upper()

    def test_apply_format(self, product_index):
        """APPLY format() — %s specifier concatenates category and price."""
        try:
            raw = product_index.execute_command(
                "FT.AGGREGATE", "pidx", "*",
                "LOAD", "2", "@category", "@price",
                "APPLY", "format(\"%s: %s\", @category, @price)", "AS", "label",
            )
        except redis.ResponseError as e:
            pytest.skip(f"format() not supported: {e}")
        count, rows = parse_aggregate_rows(raw)
        assert count == 5
        for row in rows:
            assert row["label"].startswith(row["category"] + ": ")

    def test_apply_then_filter(self, product_index):
        """Chain: APPLY computed field, then FILTER on the computed field."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "LOAD", "2", "@price", "@name",
            "APPLY", "@price * 1.1", "AS", "price_with_tax",
            "FILTER", "@price_with_tax > 400",
        )
        _, rows = parse_aggregate_rows(raw)
        # price_with_tax > 400 means price > 363.6...
        # Laptop 999*1.1=1098.9 ✓, Phone 499*1.1=548.9 ✓, others < 400
        assert len(rows) == 2
        names = {row["name"] for row in rows}
        assert names == {"Laptop", "Phone"}

    def test_apply_if_conditional(self, product_index):
        """APPLY if() conditional expression."""
        try:
            raw = product_index.execute_command(
                "FT.AGGREGATE", "pidx", "*",
                "LOAD", "1", "@price",
                "APPLY", "if(@price > 300, \"premium\", \"budget\")", "AS", "tier",
            )
        except redis.ResponseError as e:
            pytest.skip(f"if() not supported: {e}")
        count, rows = parse_aggregate_rows(raw)
        assert count == 5
        for row in rows:
            price = float(row["price"])
            expected_tier = "premium" if price > 300 else "budget"
            assert row["tier"] == expected_tier, f"price={price} tier={row['tier']}"

    def test_apply_lower_then_filter_eq(self, product_index):
        """APPLY lower(@category) then FILTER on the lowercased value."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "LOAD", "1", "@category",
            "APPLY", "lower(@category)", "AS", "cat_lower",
            "FILTER", "@cat_lower == \"electronics\"",
        )
        _, rows = parse_aggregate_rows(raw)
        assert len(rows) == 3

    def test_apply_multiple_fields(self, product_index):
        """Two APPLY clauses in sequence — both fields appear in output."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "LOAD", "2", "@price", "@category",
            "APPLY", "@price * 1.1",    "AS", "with_tax",
            "APPLY", "upper(@category)", "AS", "cat_up",
        )
        count, rows = parse_aggregate_rows(raw)
        assert count == 5
        for row in rows:
            assert "with_tax" in row
            assert "cat_up" in row

    def test_apply_exists_function(self, product_index):
        """APPLY 'exists(@name)' AS has_name — all products have a name."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "LOAD", "1", "@name",
            "APPLY", "exists(@name)", "AS", "has_name",
        )
        count, rows = parse_aggregate_rows(raw)
        assert count == 5
        for row in rows:
            assert row["has_name"] == "1"

    def test_apply_groupby_reduce_then_apply(self, product_index):
        """GROUPBY + REDUCE SUM, then APPLY on the reduced value."""
        raw = product_index.execute_command(
            "FT.AGGREGATE", "pidx", "*",
            "GROUPBY", "1", "@category",
            "REDUCE", "SUM", "1", "@price", "AS", "total",
            "APPLY", "@total * 1.1", "AS", "total_with_tax",
        )
        count, rows = parse_aggregate_rows(raw)
        assert count == 2
        for row in rows:
            total = float(row["total"])
            with_tax = float(row["total_with_tax"])
            assert abs(with_tax - total * 1.1) < 0.01


# ══════════════════════════════════════════════════════════════════════════════
# HELLO command tests
# ══════════════════════════════════════════════════════════════════════════════

class TestHello:
    """
    HELLO command — protocol negotiation on connect.

    We send HELLO via execute_command so we can observe the raw response.
    redis-py sends HELLO automatically on connect; here we test the command
    response structure directly.
    """

    def test_hello_no_args_returns_server_info(self, rb):
        """HELLO with no arguments returns server metadata, at whatever protocol
        the connection already negotiated (redis-py defaults to RESP3)."""
        info = hello_info(rb.execute_command("HELLO"))

        assert isinstance(info.get("server"), str) and info.get("server")
        assert info.get("proto") in (2, 3)
        assert info.get("mode") == "standalone"
        assert "version" in info
        assert "id" in info
        assert "role" in info

    def test_hello_2_returns_resp2_info(self, rb):
        """HELLO 2 explicitly requests RESP2 — server responds with metadata."""
        info = hello_info(rb.execute_command("HELLO", "2"))
        assert info.get("proto") == 2

    def test_hello_3_upgrades_to_resp3(self, rb):
        """HELLO 3 requests RESP3, and the reply is a real RESP3 map (`%`).

        Both clients here pin `protocol=` explicitly, because the assertion is
        about the *parser* on each side rather than about the data: a connection
        configured for RESP2 cannot decode a `%` map at all, while one that
        negotiated RESP3 reads it fine. Taking the protocol from the ambient
        parametrization would make the first half pass or fail depending on
        which protocol the run is on.

        This used to also stand in for "kividb is RESP2-only" — that is no
        longer true; RESP3 is implemented across the command surface, and the
        whole suite now runs under both protocols.
        """
        host = rb.connection_pool.connection_kwargs["host"]
        port = rb.connection_pool.connection_kwargs["port"]

        resp2_client = redis.Redis(host=host, port=port, protocol=2)
        try:
            with pytest.raises(redis.exceptions.InvalidResponse):
                resp2_client.execute_command("HELLO", "3")
        finally:
            resp2_client.close()

        resp3_client = redis.Redis(host=host, port=port, protocol=3)
        try:
            info = hello_info(resp3_client.execute_command("HELLO", "3"))
            assert isinstance(info.get("server"), str) and info.get("server")
            assert info.get("proto") == 3
        finally:
            resp3_client.close()

    def test_hello_with_setname(self, rb):
        """HELLO with SETNAME sets the client name."""
        rb.execute_command("HELLO", "2", "SETNAME", "test-client")
        name = rb.execute_command("CLIENT", "GETNAME")
        assert decode(name) == "test-client"

    def test_hello_response_has_unique_id(self, r, rb):
        """Each connection gets a unique client ID in the HELLO response."""
        info1 = hello_info(r.execute_command("HELLO"))
        info2 = hello_info(rb.execute_command("HELLO"))

        assert info1["id"] != info2["id"], "Different connections must have different IDs"

    def test_hello_before_auth_allowed(self, rb):
        """
        HELLO must be permitted before authentication (it is in the allow-list
        along with PING, AUTH, QUIT, RESET).
        This is already tested implicitly by redis-py using HELLO on connect,
        but we verify it explicitly here with a raw socket.
        """
        # Connect via raw socket, send HELLO before AUTH
        host = rb.connection_pool.connection_kwargs["host"]
        port = rb.connection_pool.connection_kwargs["port"]

        s = socket.socket()
        s.connect((host, port))
        s.sendall(b"*1\r\n$5\r\nHELLO\r\n")
        time.sleep(0.05)
        response = s.recv(4096).decode(errors="replace")
        s.close()

        # Should not receive NOAUTH error
        assert "NOAUTH" not in response
        assert "kividb" in response or "server" in response


# ══════════════════════════════════════════════════════════════════════════════
# CLIENT KILL tests
# ══════════════════════════════════════════════════════════════════════════════

class TestClientKill:
    """CLIENT KILL — verifies the socket is actually closed."""

    def test_kill_by_id_returns_1(self, r, rb):
        """CLIENT KILL ID <id> returns 1 on success."""
        # Get the ID of the victim connection (rb)
        victim_id = rb.execute_command("CLIENT", "ID")

        # Kill it from a different connection (r)
        result = r.execute_command("CLIENT", "KILL", "ID", str(victim_id))
        assert int(result) == 1

    def test_kill_nonexistent_id_returns_error(self, r):
        """CLIENT KILL ID <nonexistent> returns 0 or an error — no clients killed."""
        try:
            result = r.execute_command("CLIENT", "KILL", "ID", "999999999")
            # New-style filter form returns count of killed clients (0 = none)
            assert int(result) == 0
        except redis.ResponseError as e:
            # Some servers raise "No such client" instead
            assert "No such client" in str(e) or "ERR" in str(e)

    def test_kill_self_returns_error(self, r):
        """CLIENT KILL with own ID: server either refuses or closes the connection."""
        own_id = r.execute_command("CLIENT", "ID")
        try:
            r.execute_command("CLIENT", "KILL", "ID", str(own_id))
            # Some servers accept self-kill and close the connection
        except (redis.ResponseError, redis.ConnectionError, BrokenPipeError, OSError):
            pass  # Refused or connection closed — both acceptable

    def test_kill_closes_socket(self, rb):
        """
        After CLIENT KILL, the killed connection should receive an error or
        EOF on its next command — the socket must actually be closed.

        We open a raw socket, get its client ID via CLIENT ID, kill it from
        another connection, then verify the socket is dead.
        """
        host = rb.connection_pool.connection_kwargs["host"]
        port = rb.connection_pool.connection_kwargs["port"]

        # Open a raw connection and learn its client ID
        victim = socket.socket()
        victim.connect((host, port))
        victim.settimeout(2.0)

        # Send CLIENT ID and read the response
        victim.sendall(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
        time.sleep(0.05)
        resp = victim.recv(256).decode()
        # Response is ":N\r\n" where N is the client ID
        victim_id = int(resp.strip().lstrip(":"))

        # Kill the victim from the rb (controller) connection
        result = rb.execute_command("CLIENT", "KILL", "ID", str(victim_id))
        assert int(result) == 1

        # Give the server a moment to close the socket
        time.sleep(0.1)

        # Next read on the victim socket should return empty (EOF) or raise
        try:
            victim.sendall(b"*1\r\n$4\r\nPING\r\n")
            time.sleep(0.05)
            data = victim.recv(256)
            # EOF is indicated by empty bytes
            assert data == b"", f"Expected empty (closed), got: {data!r}"
        except (ConnectionResetError, BrokenPipeError, OSError):
            # Any of these means the socket is dead — that's correct
            pass
        finally:
            victim.close()

    def test_kill_by_addr(self, r, rb):
        """CLIENT KILL ADDR <addr> kills by network address."""
        # Get the address of rb
        info_raw = rb.execute_command("CLIENT", "INFO")
        info = decode(info_raw)

        # Extract addr=host:port from CLIENT INFO output
        addr = None
        for part in info.split():
            if part.startswith("addr="):
                addr = part[5:]
                break

        assert addr is not None, f"Could not find addr in CLIENT INFO: {info}"

        result = r.execute_command("CLIENT", "KILL", "ADDR", addr)
        assert int(result) == 1

    def test_killed_client_removed_from_list(self, r, rb):
        """After CLIENT KILL, the client no longer appears in CLIENT LIST."""
        victim_id = rb.execute_command("CLIENT", "ID")

        r.execute_command("CLIENT", "KILL", "ID", str(victim_id))
        time.sleep(0.1)

        client_list = decode(r.execute_command("CLIENT", "LIST"))
        # The victim's ID should not appear in the list
        assert f"id={victim_id} " not in client_list

    def test_kill_concurrent_connections(self, rb):
        """
        Stress: open 5 connections, kill them all from a controller, verify
        each returns 1 and that the list clears.
        """
        host = rb.connection_pool.connection_kwargs["host"]
        port = rb.connection_pool.connection_kwargs["port"]

        # Open 5 raw connections
        victims = []
        victim_ids = []
        for _ in range(5):
            s = socket.socket()
            s.connect((host, port))
            s.settimeout(2.0)
            s.sendall(b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n")
            time.sleep(0.02)
            resp = s.recv(256).decode()
            vid = int(resp.strip().lstrip(":"))
            victims.append(s)
            victim_ids.append(vid)

        # Kill them all
        for vid in victim_ids:
            try:
                result = rb.execute_command("CLIENT", "KILL", "ID", str(vid))
                assert int(result) == 1
            except redis.ResponseError:
                pass  # already disconnected is acceptable

        time.sleep(0.15)

        # None of the victim IDs should appear in CLIENT LIST
        client_list = decode(rb.execute_command("CLIENT", "LIST"))
        for vid in victim_ids:
            assert f"id={vid} " not in client_list, f"client {vid} still listed after kill"

        for s in victims:
            try:
                s.close()
            except Exception:
                pass

# tests/e2e/modules/test_config.py

import pytest
import redis
from utils.helpers import rb  # noqa: F401


class TestConfigSetRequirepass:
    """
    CONFIG SET requirepass — verifies the password change takes effect
    immediately for new authentication attempts on existing connections.
    """

    def test_set_requirepass_then_auth(self, rb):
        """
        After CONFIG SET requirepass, AUTH with the new password succeeds
        and AUTH with the old password fails.
        """
        host = rb.connection_pool.connection_kwargs["host"]
        port = rb.connection_pool.connection_kwargs["port"]

        assert rb.ping()
        rb.execute_command("CONFIG", "SET", "requirepass", "testpass123")
        try:
            result = rb.execute_command("AUTH", "testpass123")
            assert result is True or result == b"OK" or result == "OK"

            try:
                rb.execute_command("AUTH", "wrongpassword")
                assert False, "expected WRONGPASS error"
            except (redis.AuthenticationError, redis.ResponseError):
                pass  # Any auth failure is acceptable — password was set
        finally:
            # Use a dedicated client with the password to guarantee cleanup
            # even if rb's connection was invalidated by the reconnection.
            try:
                cleanup = redis.Redis(host=host, port=port,
                                      password="testpass123",
                                      decode_responses=False)
                cleanup.execute_command("CONFIG", "SET", "requirepass", "")
                cleanup.close()
            except Exception:
                try:
                    rb.execute_command("CONFIG", "SET", "requirepass", "")
                except Exception:
                    pass

    def test_set_requirepass_empty_removes_password(self, rb):
        """
        CONFIG SET requirepass "" removes the password requirement.
        After this, AUTH with any password fails with WRONGPASS but
        commands without AUTH succeed (nopass mode).
        """
        # Set then clear
        rb.execute_command("CONFIG", "SET", "requirepass", "temppass")
        rb.execute_command("AUTH", "temppass")
        rb.execute_command("CONFIG", "SET", "requirepass", "")

        # Should be able to run commands without AUTH now
        assert rb.ping()

    def test_config_get_requirepass_reflects_change(self, rb):
        """
        CONFIG GET requirepass returns empty string when no password is set,
        and some non-empty value when one is set (may be plaintext or redacted).
        """
        # No password
        rb.execute_command("CONFIG", "SET", "requirepass", "")
        # RESP2 replies with a flat array, RESP3 with a map — normalise both.
        pairs = config_pairs(rb.execute_command("CONFIG", "GET", "requirepass"))
        value = pairs.get(b"requirepass", pairs.get("requirepass"))
        assert value == b"" or value == ""

        rb.execute_command("CONFIG", "SET", "requirepass", "secret")
        try:
            pairs = config_pairs(rb.execute_command("CONFIG", "GET", "requirepass"))
            raw = pairs.get(b"requirepass", pairs.get("requirepass"))
            val = raw.decode() if isinstance(raw, bytes) else raw
            assert val != "", "requirepass should be non-empty after being set"
        finally:
            cleanup = redis.Redis(
                host=KIVI_HOST, port=KIVI_PORT,
                password="secret", decode_responses=False,
            )
            try:
                cleanup.execute_command("CONFIG", "SET", "requirepass", "")
            finally:
                cleanup.close()

    def test_acl_setuser_and_config_set_requirepass_consistent(self, rb):
        """
        CONFIG SET requirepass and ACL SETUSER default are two ways to set
        the same thing. After CONFIG SET, ACL GETUSER default should reflect
        the new password hash, not the old one.
        """
        rb.execute_command("CONFIG", "SET", "requirepass", "newpass")
        try:
            # ACL GETUSER default should show a non-empty passwords list
            result = rb.execute_command("ACL", "GETUSER", "default")
            # Result is a flat list: [field, value, ...]
            pairs = {}
            flat = list(result)
            for i in range(0, len(flat) - 1, 2):
                k = flat[i].decode() if isinstance(flat[i], bytes) else flat[i]
                pairs[k] = flat[i + 1]

            passwords = pairs.get("passwords", [])
            assert len(passwords) > 0, "default user should have a password after CONFIG SET"
        finally:
            cleanup = redis.Redis(
                host=KIVI_HOST, port=KIVI_PORT,
                password="newpass", decode_responses=False,
            )
            try:
                cleanup.execute_command("CONFIG", "SET", "requirepass", "")
            finally:
                cleanup.close()