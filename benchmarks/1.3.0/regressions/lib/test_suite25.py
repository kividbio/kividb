#!/usr/bin/env python3
"""Self-test of suite 25's error check; runs anywhere: python lib/test_suite25.py

redis-benchmark keeps going after an error reply, so suite 25 counts the
server's own errors around each round and refuses a round that had any.
"""
import importlib.util
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
spec = importlib.util.spec_from_file_location(
    "suite25", os.path.join(HERE, "..", "suites", "25-centrifuge-scripts-publish.py"))
s25 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s25)


def info(errors=0, failed=0, rejected=0, calls=0):
    return {"total_error_replies": errors,
            "cmdstat_evalsha": {"calls": calls, "failed_calls": failed, "rejected_calls": rejected},
            "cmdstat_ping": {"calls": 1, "failed_calls": 0, "rejected_calls": 0},
            "used_memory": 1}


class FakeClient:
    """INFO answers for one round: `all` before, commandstats twice, `all` after."""

    def __init__(self, before, after, calls=(100, 900)):
        self.alls = [before, after]
        self.stats = [{"cmdstat_evalsha": {"calls": c}} for c in calls]

    def info(self, section=None):
        return self.alls.pop(0) if section == "all" else self.stats.pop(0)


class FakePopen:
    def __init__(self, *a, **kw):
        pass

    def terminate(self):
        pass

    def communicate(self, timeout=None):
        return "", ""


class FakeCtx:
    port = 1

    def __init__(self, client):
        self._c = client

    def client(self):
        return self._c


class ErrorCheck(unittest.TestCase):
    def setUp(self):
        self._popen, self._sleep, self._tool = s25.subprocess.Popen, s25.time.sleep, s25.redis_tool
        s25.subprocess.Popen = FakePopen
        s25.time.sleep = lambda s: None
        s25.redis_tool = lambda name: "/bin/true"

    def tearDown(self):
        s25.subprocess.Popen, s25.time.sleep, s25.redis_tool = self._popen, self._sleep, self._tool

    def test_counts(self):
        self.assertEqual(s25.error_counts(info(errors=3, failed=2, rejected=1)), 6)
        self.assertEqual(s25.errors_during(info(errors=3), info(errors=3)), 0)
        self.assertEqual(s25.errors_during(info(failed=5), info(failed=9)), 4)
        # A server without total_error_replies still counts failed calls.
        self.assertEqual(s25.error_counts({"cmdstat_x": {"failed_calls": 2}}), 2)

    def test_a_clean_round_is_measured(self):
        ctx = FakeCtx(FakeClient(info(), info()))
        self.assertGreater(s25.rate(ctx, "evalsha", ["EVALSHA", "x"]), 0)

    def test_an_erroring_round_is_refused(self):
        # A NOSCRIPT storm: the calls go up, and so do the error replies.
        ctx = FakeCtx(FakeClient(info(errors=0, failed=0), info(errors=5000, failed=5000)))
        with self.assertRaisesRegex(RuntimeError, "error replies"):
            s25.rate(ctx, "evalsha", ["EVALSHA", "nosuchsha"])
        ctx = FakeCtx(FakeClient(info(rejected=0), info(rejected=1)))
        with self.assertRaises(RuntimeError):
            s25.rate(ctx, "evalsha", ["EVALSHA", "x"])

    def test_a_round_with_no_calls_is_refused(self):
        ctx = FakeCtx(FakeClient(info(), info(), calls=(100, 100)))
        with self.assertRaises(RuntimeError):
            s25.rate(ctx, "evalsha", ["EVALSHA", "x"])

    def test_a_server_that_does_not_count_errors_is_refused(self):
        import redis

        class Client:
            def __init__(self, counts):
                self.infos = [info(errors=0), info(errors=counts)]

            def info(self, section=None):
                return self.infos.pop(0)

            def execute_command(self, *a):
                raise redis.ResponseError("NOSCRIPT")
        s25.errors_are_counted(Client(1))
        with self.assertRaisesRegex(RuntimeError, "does not count error replies"):
            s25.errors_are_counted(Client(0))

    def test_the_arguments_are_centrifuges(self):
        args = s25.add_args(100, 100)
        self.assertEqual(args[1:5], ["99", "100", "cf.client.ch", "2592000"])
        self.assertEqual(args[7:9], ["", ""])   # no idempotency TTL, no delta
        self.assertEqual(s25.history_args()[:3], ["1", "-1", "2592000"])


if __name__ == "__main__":
    unittest.main()
