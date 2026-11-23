#!/usr/bin/env python3
"""Portable checks for suite 38's Centrifuge arguments and workload shape."""
import importlib.util
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
spec = importlib.util.spec_from_file_location(
    "suite38", os.path.join(HERE, "..", "suites", "38-centrifuge-history.py"))
s38 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s38)


class Suite38(unittest.TestCase):
    def test_requested_thread_configurations(self):
        self.assertEqual([(config.name, config.threads, config.lua) for config in s38.S.configs], [
            ("threads4", 4, True), ("default", 0, True)])
        self.assertEqual(s38.S.min_cpus, 8)

    def test_stream_reads_are_counted_forward_and_reverse(self):
        self.assertEqual(s38.read_args("stream"), ["1", "0", "4", "0", "2592000", "issue900"])
        self.assertEqual(s38.read_args("stream", reverse=True), ["1", "0", "4", "1", "2592000", "issue900"])
        self.assertIn('"xrange", stream_key, since_offset, "+", "COUNT", limit', s38.HISTORY_STREAM)
        self.assertIn('"xrevrange", stream_key, get_offset, "-", "COUNT", limit', s38.HISTORY_STREAM)

    def test_keys_are_untagged_and_many_has_sixteen_channels(self):
        self.assertNotIn("{", " ".join(s38.keys("stream", "many-00")))
        self.assertEqual(s38.channels("one"), ["one"] * s38.CLIENTS)
        self.assertEqual(len(set(s38.channels("many"))), s38.CLIENTS)

    def test_list_and_stream_sizes_match_centrifuge(self):
        self.assertEqual(s38.add_args("list", "one")[1:5], ["99", "100", "cf38.client.one", "2592000"])
        self.assertEqual(s38.add_args("stream", "one")[1:5], ["100", "100", "cf38.client.one", "2592000"])
        self.assertEqual(s38.add_args("stream", "one")[7:9], ["", ""])

    def test_every_workload_uses_sixteen_evalsha_clients(self):
        shas = {"list_read": "lr", "list_add": "la", "stream_read": "sr", "stream_add": "sa"}
        for case in s38.CASES:
            for mode in ("one", "many"):
                commands = s38.commands_for(case, mode, shas)
                self.assertEqual(len(commands), s38.CLIENTS)
                self.assertTrue(all(command[0] == "EVALSHA" for command in commands))


if __name__ == "__main__":
    unittest.main()
