import unittest
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from unittest import mock

from tap_pendo import main, resolve_config


def base_config(**overrides):
    config = {"x_pendo_integration_key": "key", "start_date": "2020-01-01T00:00:00Z",
              "period": "hourRange", "lookback_window": 5}
    config.update(overrides)
    return config


class TestIntegrationKeyAlias(unittest.TestCase):
    def test_api_key_alias_sets_integration_key(self):
        config = resolve_config({"api_key": "alias-key"})
        self.assertEqual(config["x_pendo_integration_key"], "alias-key")

    def test_integration_key_wins_over_api_key(self):
        config = resolve_config({"x_pendo_integration_key": "original", "api_key": "alias-key"})
        self.assertEqual(config["x_pendo_integration_key"], "original")

    def test_empty_integration_key_falls_back_to_api_key(self):
        config = resolve_config({"x_pendo_integration_key": "", "api_key": "alias-key"})
        self.assertEqual(config["x_pendo_integration_key"], "alias-key")

    def test_eu_key_through_alias_keeps_eu_routing(self):
        from tap_pendo.streams import Endpoints, EU_BASE_URL
        config = resolve_config({"api_key": "alias-key.eu"})
        url = Endpoints("/api/v1/aggregation", "POST").get_url(config["x_pendo_integration_key"])
        self.assertTrue(url.startswith(EU_BASE_URL))

    def test_missing_both_keys_raises(self):
        for config in ({}, {"api_key": ""}, {"x_pendo_integration_key": None}):
            with self.assertRaises(Exception) as e:
                resolve_config(dict(config))
            self.assertIn("x_pendo_integration_key", str(e.exception))
            self.assertIn("api_key", str(e.exception))


class TestConfigDefaults(unittest.TestCase):
    def test_start_date_defaults_to_90_days_ago(self):
        config = resolve_config({"api_key": "key"})
        start_date = datetime.strptime(config["start_date"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        expected = datetime.now(timezone.utc) - timedelta(days=90)
        self.assertLess(abs((start_date - expected).total_seconds()), 60)

    def test_lookback_window_defaults_to_2_days(self):
        self.assertEqual(resolve_config({"api_key": "key"})["lookback_window"], 2)

    def test_period_defaults_to_day_range(self):
        self.assertEqual(resolve_config({"api_key": "key"})["period"], "dayRange")

    def test_explicit_values_win(self):
        config = resolve_config(base_config())
        self.assertEqual(config["start_date"], "2020-01-01T00:00:00Z")
        self.assertEqual(config["period"], "hourRange")
        self.assertEqual(config["lookback_window"], 5)

    def test_explicit_zero_lookback_window_wins(self):
        self.assertEqual(resolve_config(base_config(lookback_window=0))["lookback_window"], 0)

    def test_default_lookback_window_reaches_streams(self):
        from tap_pendo.streams import Features
        self.assertEqual(Features(resolve_config({"api_key": "key"})).lookback_window(), 2)


class TestMainAcceptsAlias(unittest.TestCase):
    @mock.patch("tap_pendo.utils.parse_args",
                side_effect=lambda required_config_keys: Namespace(config={"api_key": "alias-key"}, discover=True))
    @mock.patch("tap_pendo.do_discover")
    def test_discover_with_api_key_only(self, mocked_do_discover, mocked_parse_args):
        main()
        mocked_parse_args.assert_called_with([])
        config = mocked_do_discover.call_args[0][0]
        self.assertEqual(config["x_pendo_integration_key"], "alias-key")
        self.assertEqual(config["period"], "dayRange")
        self.assertEqual(config["lookback_window"], 2)
        self.assertIn("start_date", config)
