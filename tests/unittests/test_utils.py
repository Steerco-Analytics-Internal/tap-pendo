import datetime
import json
import os
import tempfile
import unittest

import importlib

# tap_pendo re-exports singer.utils as `utils`, so load the module by path
utils = importlib.import_module("tap_pendo.utils")


class TestUtils(unittest.TestCase):
    def test_strptime_and_strftime_round_trip(self):
        dt = utils.strptime("2021-01-02T03:04:05Z")
        self.assertEqual(dt, datetime.datetime(2021, 1, 2, 3, 4, 5))
        self.assertEqual(utils.strftime(dt), "2021-01-02T03:04:05Z")

    def test_chunk_splits_list(self):
        self.assertEqual(list(utils.chunk([1, 2, 3, 4, 5], 2)), [[1, 2], [3, 4], [5]])

    def test_update_state_keeps_latest_value(self):
        state = {}
        utils.update_state(state, "accounts", None)
        self.assertEqual(state, {})
        utils.update_state(state, "accounts", datetime.datetime(2021, 1, 2))
        utils.update_state(state, "accounts", "2021-01-01T00:00:00Z")
        self.assertEqual(state["accounts"], "2021-01-02T00:00:00Z")
        utils.update_state(state, "accounts", "2021-01-03T00:00:00Z")
        self.assertEqual(state["accounts"], "2021-01-03T00:00:00Z")

    def test_check_config_raises_for_missing_keys(self):
        utils.check_config({"a": 1}, ["a"])
        with self.assertRaises(Exception):
            utils.check_config({}, ["a"])

    def test_load_json_and_schema(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"a": 1}, f)
        try:
            self.assertEqual(utils.load_json(f.name), {"a": 1})
        finally:
            os.unlink(f.name)
        self.assertIn("properties", utils.load_schema("accounts"))

    def test_ratelimit_passes_calls_through(self):
        calls = []
        limited = utils.ratelimit(2, 0)(lambda x: calls.append(x) or x)
        self.assertEqual([limited(i) for i in range(3)], [0, 1, 2])
        self.assertEqual(calls, [0, 1, 2])
