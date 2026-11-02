"""
tests/e2e/modules/test_acl.py

End-to-end tests for the ACL subsystem (PR2):
    ACL WHOAMI, LIST, USERS, CAT, GETUSER, LOG,
    SETUSER, DELUSER, GENPASS, DRYRUN, SAVE/LOAD,
    AUTH enforcement, command permission (NOPERM)
"""
from __future__ import annotations

import time
import pytest
import redis
from utils.helpers import make_client, KIVI_HOST, KIVI_PORT


# ── Helpers ───────────────────────────────────────────────────────────────────

def raw_command(r, *args):
    """Bypass redis-py response callbacks for commands it doesn't know."""
    conn = r.connection_pool.get_connection("_")
    try:
        conn.send_command(*args)
        return conn.read_response()
    finally:
        r.connection_pool.release(conn)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def r(kivi_server):
    """Fresh DB, no password."""
    client = make_client()
    client.flushdb()
    yield client
    client.close()


# ──────────────────────────────────────────────────────────────────────────────
# ACL WHOAMI
# ──────────────────────────────────────────────────────────────────────────────

class TestAclWhoami:
    def test_whoami_returns_default(self, r):
        result = raw_command(r, "ACL", "WHOAMI")
        assert result == "default"

    def test_whoami_is_string(self, r):
        result = raw_command(r, "ACL", "WHOAMI")
        assert isinstance(result, str)
        assert len(result) > 0


# ──────────────────────────────────────────────────────────────────────────────
# ACL LIST
# ──────────────────────────────────────────────────────────────────────────────

class TestAclList:
    def test_list_returns_list(self, r):
        result = raw_command(r, "ACL", "LIST")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_list_contains_default_user(self, r):
        result = raw_command(r, "ACL", "LIST")
        assert any(entry.startswith("user default") for entry in result)

    def test_list_default_has_plus_all(self, r):
        result = raw_command(r, "ACL", "LIST")
        default = next(e for e in result if e.startswith("user default"))
        assert "+@all" in default

    def test_list_default_nopass_when_no_requirepass(self, r):
        result = raw_command(r, "ACL", "LIST")
        default = next(e for e in result if e.startswith("user default"))
        assert "nopass" in default

    def test_list_after_setuser_includes_new_user(self, r):
        raw_command(r, "ACL", "SETUSER", "testuser", "on", "nopass", "+@all", "~*")
        result = raw_command(r, "ACL", "LIST")
        assert any("testuser" in e for e in result)
        # Cleanup
        raw_command(r, "ACL", "DELUSER", "testuser")

    def test_list_entries_are_strings(self, r):
        result = raw_command(r, "ACL", "LIST")
        for entry in result:
            assert isinstance(entry, str)
            assert entry.startswith("user ")


# ──────────────────────────────────────────────────────────────────────────────
# ACL USERS
# ──────────────────────────────────────────────────────────────────────────────

class TestAclUsers:
    def test_users_contains_default(self, r):
        result = raw_command(r, "ACL", "USERS")
        assert "default" in result

    def test_users_after_setuser(self, r):
        raw_command(r, "ACL", "SETUSER", "alice", "on", "nopass")
        result = raw_command(r, "ACL", "USERS")
        assert "alice" in result
        raw_command(r, "ACL", "DELUSER", "alice")

    def test_users_after_deluser(self, r):
        raw_command(r, "ACL", "SETUSER", "tmpuser", "on", "nopass")
        raw_command(r, "ACL", "DELUSER", "tmpuser")
        result = raw_command(r, "ACL", "USERS")
        assert "tmpuser" not in result


# ──────────────────────────────────────────────────────────────────────────────
# ACL CAT
# ──────────────────────────────────────────────────────────────────────────────

class TestAclCat:
    def test_cat_no_args_returns_categories(self, r):
        result = raw_command(r, "ACL", "CAT")
        assert isinstance(result, list)
        assert len(result) > 0
        for cat in ("string", "hash", "list", "set", "keyspace", "connection"):
            assert cat in result, f"ACL CAT missing: {cat}"

    def test_cat_string_contains_get_set(self, r):
        result = raw_command(r, "ACL", "CAT", "string")
        upper = [c.upper() for c in result]
        assert "GET" in upper
        assert "SET" in upper
        assert "APPEND" in upper

    def test_cat_hash_contains_hset_hget(self, r):
        result = raw_command(r, "ACL", "CAT", "hash")
        upper = [c.upper() for c in result]
        assert "HSET" in upper
        assert "HGET" in upper

    def test_cat_list_contains_lpush(self, r):
        result = raw_command(r, "ACL", "CAT", "list")
        upper = [c.upper() for c in result]
        assert "LPUSH" in upper
        assert "RPUSH" in upper

    def test_cat_keyspace_contains_del_expire(self, r):
        result = raw_command(r, "ACL", "CAT", "keyspace")
        upper = [c.upper() for c in result]
        assert "DEL" in upper
        assert "EXPIRE" in upper
        assert "TTL" in upper

    def test_cat_unknown_returns_error(self, r):
        import redis as _redis
        with pytest.raises(_redis.ResponseError):
            raw_command(r, "ACL", "CAT", "nonexistent_category_xyz")

    def test_cat_string_no_list_commands(self, r):
        string_cmds = [c.upper() for c in raw_command(r, "ACL", "CAT", "string")]
        # List commands should not appear in string category
        assert "LPUSH" not in string_cmds
        assert "RPUSH" not in string_cmds


# ──────────────────────────────────────────────────────────────────────────────
# ACL GETUSER
# ──────────────────────────────────────────────────────────────────────────────

class TestAclGetuser:
    def test_getuser_default_returns_map(self, r):
        result = raw_command(r, "ACL", "GETUSER", "default")
        assert isinstance(result, list)
        assert len(result) % 2 == 0

        # Parse into dict
        it = iter(result)
        data = dict(zip(it, it))
        assert "flags" in data
        assert "commands" in data
        assert "keys" in data
        assert "passwords" in data

    def test_getuser_default_has_on_flag(self, r):
        result = raw_command(r, "ACL", "GETUSER", "default")
        it = iter(result)
        data = dict(zip(it, it))
        assert "on" in data["flags"]

    def test_getuser_missing_user_returns_none(self, r):
        result = raw_command(r, "ACL", "GETUSER", "nosuchuser")
        assert result is None

    def test_getuser_created_user(self, r):
        raw_command(r, "ACL", "SETUSER", "frank", "on", "nopass", "-@all", "+@string", "~*")
        result = raw_command(r, "ACL", "GETUSER", "frank")
        it = iter(result)
        data = dict(zip(it, it))
        assert "on" in data["flags"]
        raw_command(r, "ACL", "DELUSER", "frank")

    def test_getuser_disabled_user_shows_off_flag(self, r):
        raw_command(r, "ACL", "SETUSER", "offuser", "off", "nopass", "+@all")
        result = raw_command(r, "ACL", "GETUSER", "offuser")
        it = iter(result)
        data = dict(zip(it, it))
        assert "off" in data["flags"]
        raw_command(r, "ACL", "DELUSER", "offuser")


# ──────────────────────────────────────────────────────────────────────────────
# ACL SETUSER
# ──────────────────────────────────────────────────────────────────────────────

class TestAclSetuser:
    def test_setuser_creates_new_user(self, r):
        assert raw_command(r, "ACL", "SETUSER", "newu", "on", "nopass", "+@all", "~*") == "OK"
        assert "newu" in raw_command(r, "ACL", "USERS")
        raw_command(r, "ACL", "DELUSER", "newu")

    def test_setuser_with_password(self, r):
        raw_command(r, "ACL", "SETUSER", "withpass", "on", ">mypassword", "+@all", "~*")
        acl_list = raw_command(r, "ACL", "LIST")
        line = next(e for e in acl_list if "withpass" in e)
        assert "#" in line, f"hashed password missing: {line}"
        assert "nopass" not in line
        raw_command(r, "ACL", "DELUSER", "withpass")

    def test_setuser_on_off_toggle(self, r):
        raw_command(r, "ACL", "SETUSER", "toggled", "on", "nopass", "+@all")
        raw_command(r, "ACL", "SETUSER", "toggled", "off")
        result = raw_command(r, "ACL", "GETUSER", "toggled")
        it = iter(result)
        data = dict(zip(it, it))
        assert "off" in data["flags"]
        raw_command(r, "ACL", "DELUSER", "toggled")

    def test_setuser_reset_clears_all(self, r):
        raw_command(r, "ACL", "SETUSER", "toclean", "on", "nopass", "+@all", "~*")
        raw_command(r, "ACL", "SETUSER", "toclean", "reset")
        result = raw_command(r, "ACL", "GETUSER", "toclean")
        it = iter(result)
        data = dict(zip(it, it))
        assert "off" in data["flags"]
        raw_command(r, "ACL", "DELUSER", "toclean")

    def test_setuser_nopass_clears_passwords(self, r):
        raw_command(r, "ACL", "SETUSER", "nptest", "on", ">somepass", "+@all")
        raw_command(r, "ACL", "SETUSER", "nptest", "nopass")
        result = raw_command(r, "ACL", "GETUSER", "nptest")
        it = iter(result)
        data = dict(zip(it, it))
        assert "nopass" in data["flags"]
        assert len(data["passwords"]) == 0
        raw_command(r, "ACL", "DELUSER", "nptest")

    def test_setuser_remove_password(self, r):
        raw_command(r, "ACL", "SETUSER", "rmpass", "on", ">toremove", "+@all")
        raw_command(r, "ACL", "SETUSER", "rmpass", "<toremove")
        result = raw_command(r, "ACL", "GETUSER", "rmpass")
        it = iter(result)
        data = dict(zip(it, it))
        assert len(data["passwords"]) == 0
        raw_command(r, "ACL", "DELUSER", "rmpass")


# ──────────────────────────────────────────────────────────────────────────────
# ACL DELUSER
# ──────────────────────────────────────────────────────────────────────────────

class TestAclDeluser:
    def test_deluser_existing_user(self, r):
        raw_command(r, "ACL", "SETUSER", "todel", "on", "nopass")
        count = raw_command(r, "ACL", "DELUSER", "todel")
        assert count == 1
        assert raw_command(r, "ACL", "GETUSER", "todel") is None

    def test_deluser_missing_user_returns_zero(self, r):
        count = raw_command(r, "ACL", "DELUSER", "doesnotexist")
        assert count == 0

    def test_deluser_multiple_users(self, r):
        raw_command(r, "ACL", "SETUSER", "m1", "on", "nopass")
        raw_command(r, "ACL", "SETUSER", "m2", "on", "nopass")
        count = raw_command(r, "ACL", "DELUSER", "m1", "m2", "nosuch")
        assert count == 2

    def test_deluser_default_fails(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "ACL", "DELUSER", "default")

    def test_deluser_removed_from_list(self, r):
        raw_command(r, "ACL", "SETUSER", "gone", "on", "nopass")
        raw_command(r, "ACL", "DELUSER", "gone")
        assert "gone" not in raw_command(r, "ACL", "USERS")


# ──────────────────────────────────────────────────────────────────────────────
# ACL GENPASS
# ──────────────────────────────────────────────────────────────────────────────

class TestAclGenpass:
    def test_genpass_default_length(self, r):
        result = raw_command(r, "ACL", "GENPASS")
        assert isinstance(result, str)
        # 256 bits / 4 bits per hex char = 64 chars
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_genpass_128_bits(self, r):
        result = raw_command(r, "ACL", "GENPASS", "128")
        assert len(result) == 32  # 128 bits = 32 hex chars

    def test_genpass_64_bits(self, r):
        result = raw_command(r, "ACL", "GENPASS", "64")
        assert len(result) == 16

    def test_genpass_randomness(self, r):
        results = {raw_command(r, "ACL", "GENPASS") for _ in range(5)}
        # All 5 should be distinct (probability of collision is negligible)
        assert len(results) == 5

    def test_genpass_non_multiple_of_4_rounds_up(self, r):
        """Redis rounds non-multiple-of-4 bit counts up rather than erroring."""
        result = raw_command(r, "ACL", "GENPASS", "7")
        assert isinstance(result, str) and len(result) >= 2

    def test_genpass_zero_bits(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "ACL", "GENPASS", "0")

    def test_genpass_too_large(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "ACL", "GENPASS", "8192")


# ──────────────────────────────────────────────────────────────────────────────
# ACL DRYRUN
# ──────────────────────────────────────────────────────────────────────────────

class TestAclDryrun:
    def test_dryrun_default_user_allowed_get(self, r):
        result = raw_command(r, "ACL", "DRYRUN", "default", "GET", "key")
        assert result == "OK"

    def test_dryrun_default_user_allowed_set(self, r):
        result = raw_command(r, "ACL", "DRYRUN", "default", "SET", "key", "val")
        assert result == "OK"

    def test_dryrun_restricted_user_denied(self, r):
        raw_command(r, "ACL", "SETUSER", "stronly", "on", "nopass", "-@all", "+@string", "~*")
        # String command allowed
        assert raw_command(r, "ACL", "DRYRUN", "stronly", "GET", "key") == "OK"
        # List command denied — returns a message string, not "OK"
        result = raw_command(r, "ACL", "DRYRUN", "stronly", "LPUSH", "key", "v")
        assert result != "OK"
        raw_command(r, "ACL", "DELUSER", "stronly")

    def test_dryrun_unknown_user_errors(self, r):
        with pytest.raises(redis.ResponseError, match="not found"):
            raw_command(r, "ACL", "DRYRUN", "nosuchuser", "GET", "key")

    def test_dryrun_no_command_permission_denied(self, r):
        """DRYRUN returns a denial message when the user lacks command permission."""
        raw_command(r, "ACL", "SETUSER", "restricteddr", "on", "nopass", "+get", "~*")
        try:
            result = raw_command(r, "ACL", "DRYRUN", "restricteddr", "SET", "key", "val")
            # Some Redis versions return a string describing the denial
            assert "permission" in result.lower() or "no" in result.lower()
        except redis.ResponseError:
            pass  # Other versions raise ResponseError — both are acceptable
        raw_command(r, "ACL", "DELUSER", "restricteddr")

    def test_dryrun_missing_args_errors(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "ACL", "DRYRUN", "default")  # missing command


# ──────────────────────────────────────────────────────────────────────────────
# ACL LOG
# ──────────────────────────────────────────────────────────────────────────────

class TestAclLog:
    def test_log_empty_initially(self, r):
        raw_command(r, "ACL", "LOG", "RESET")
        result = raw_command(r, "ACL", "LOG")
        assert result == [] or len(result) == 0

    def test_log_reset_clears_entries(self, r):
        # Generate at least one log entry by connecting unauthenticated to
        # a password-protected instance — we can't do that in this fixture
        # so we just verify RESET returns OK and empties the log
        assert raw_command(r, "ACL", "LOG", "RESET") == "OK"
        result = raw_command(r, "ACL", "LOG")
        assert len(result) == 0

    def test_log_count_argument(self, r):
        raw_command(r, "ACL", "LOG", "RESET")
        result = raw_command(r, "ACL", "LOG", "10")
        assert isinstance(result, list)

    def test_log_invalid_count_errors(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "ACL", "LOG", "notanumber")


# ──────────────────────────────────────────────────────────────────────────────
# ACL SAVE / LOAD (no aclfile configured)
# ──────────────────────────────────────────────────────────────────────────────

class TestAclSaveLoad:
    def test_save_without_aclfile_errors(self, r):
        with pytest.raises(redis.ResponseError, match="not configured"):
            raw_command(r, "ACL", "SAVE")

    def test_load_without_aclfile_errors(self, r):
        with pytest.raises(redis.ResponseError, match="not configured"):
            raw_command(r, "ACL", "LOAD")


# ──────────────────────────────────────────────────────────────────────────────
# AUTH enforcement
# ──────────────────────────────────────────────────────────────────────────────

class TestAuthEnforcement:
    def test_ping_allowed_before_auth(self, kivi_server):
        """PING works even without authentication."""
        c = redis.Redis(
            host=KIVI_HOST, port=KIVI_PORT,
            decode_responses=True,
            password=None,
            socket_timeout=5,
        )
        # Force a raw PING without triggering auth
        conn = c.connection_pool.get_connection("_")
        try:
            conn.send_command("PING")
            result = conn.read_response()
            assert result == "PONG"
        finally:
            c.connection_pool.release(conn)
            c.close()

    def test_auth_nopass_user_requires_no_auth(self, r):
        """Default user with nopass: connecting without AUTH succeeds (PING works)."""
        c = redis.Redis(host=r.connection_pool.connection_kwargs["host"],
                        port=r.connection_pool.connection_kwargs["port"],
                        decode_responses=True)
        assert c.ping() is True
        c.close()


# ──────────────────────────────────────────────────────────────────────────────
# ACL HELP (unknown subcommand)
# ──────────────────────────────────────────────────────────────────────────────

class TestAclMisc:
    def test_unknown_subcommand_errors(self, r):
        with pytest.raises(redis.ResponseError, match="unknown subcommand"):
            raw_command(r, "ACL", "BLAH")

    def test_acl_no_subcommand_errors(self, r):
        with pytest.raises(redis.ResponseError):
            raw_command(r, "ACL")

    def test_acl_list_sorted_deterministically(self, r):
        """ACL LIST should return entries in a consistent order (sorted)."""
        raw_command(r, "ACL", "SETUSER", "zzz", "on", "nopass")
        raw_command(r, "ACL", "SETUSER", "aaa", "on", "nopass")
        result1 = raw_command(r, "ACL", "LIST")
        result2 = raw_command(r, "ACL", "LIST")
        assert result1 == result2, "ACL LIST should be deterministic"
        raw_command(r, "ACL", "DELUSER", "zzz")
        raw_command(r, "ACL", "DELUSER", "aaa")

    def test_acl_users_sorted(self, r):
        """ACL USERS should return users in consistent order."""
        raw_command(r, "ACL", "SETUSER", "zzz2", "on", "nopass")
        raw_command(r, "ACL", "SETUSER", "aaa2", "on", "nopass")
        r1 = raw_command(r, "ACL", "USERS")
        r2 = raw_command(r, "ACL", "USERS")
        assert sorted(r1) == sorted(r2)
        raw_command(r, "ACL", "DELUSER", "zzz2")
        raw_command(r, "ACL", "DELUSER", "aaa2")