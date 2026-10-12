import copy
import unittest
from unittest import mock

from singer import metadata
from singer.catalog import CatalogEntry, Schema
from singer.utils import strptime_to_utc

from tap_pendo import streams
from tap_pendo.streams import (API_RECORD_LIMIT, MAX_RECORDS_LIMIT_KEY, FeatureEvents, Features,
                               VisitorHistory, Visitors, get_max_records)
from tap_pendo.sync import sync_full_table, sync_stream

START_DATE = "2021-01-01T00:00:00Z"
BASE_MS = 1609459200000  # 2021-01-01T00:00:00Z


def make_config(limits=None, **extra):
    config = {"x_pendo_integration_key": "key", "start_date": START_DATE, "period": "dayRange",
              "app_ids": "expandAppIds(\"*\")"}
    if limits is not None:
        config[MAX_RECORDS_LIMIT_KEY] = limits
    config.update(extra)
    return config


def catalog_entry(instance):
    mdata = metadata.to_map(instance.load_metadata())
    mdata[()]["selected"] = True
    return CatalogEntry(tap_stream_id=instance.name, stream=instance.name,
                        schema=Schema.from_dict(instance.load_schema()),
                        metadata=metadata.to_list(mdata))


def features(count, start=0):
    return [{"id": f"feature-{i}", "last_updated_at": BASE_MS + i * 1000} for i in range(start, start + count)]


def feature_events(feature_id, count):
    return [{"featureId": feature_id, "visitorId": f"v{i}", "accountId": "a", "remoteIp": "ip",
             "userAgent": "ua", "day": BASE_MS + i * 1000} for i in range(count)]


class TestGetMaxRecords(unittest.TestCase):
    def test_stream_not_in_map_runs_unbounded(self):
        self.assertIsNone(get_max_records(make_config(), "features"))
        self.assertIsNone(get_max_records(make_config({"pages": 5}), "features"))
        self.assertIsNone(get_max_records(None, "features"))

    def test_stream_in_map_is_limited(self):
        self.assertEqual(get_max_records(make_config({"features": 5}), "features"), 5)
        self.assertEqual(get_max_records(make_config({"features": "7"}), "features"), 7)
        self.assertEqual(get_max_records(make_config({"features": 0}), "features"), 0)

    def test_json_string_map_is_parsed(self):
        self.assertEqual(get_max_records(make_config('{"features": 3}'), "features"), 3)

    def test_negative_limit_raises(self):
        with self.assertRaises(ValueError):
            get_max_records(make_config({"features": -1}), "features")


class TestPageSize(unittest.TestCase):
    def test_limited_stream_requests_no_more_than_its_limit(self):
        self.assertEqual(Features(make_config({"features": 25})).record_limit, 25)

    def test_zero_limit_still_requests_a_valid_page(self):
        self.assertEqual(Features(make_config({"features": 0})).record_limit, 1)

    def test_configured_record_limit_below_max_records_wins(self):
        self.assertEqual(Features(make_config({"features": 25}, record_limit=10)).record_limit, 10)

    def test_unlimited_stream_keeps_default_page(self):
        self.assertEqual(Features(make_config()).record_limit, API_RECORD_LIMIT)


class TestBookmarks(unittest.TestCase):
    def test_limited_stream_writes_no_bookmark(self):
        state = {}
        Features(make_config({"features": 1})).update_bookmark(state, "features", START_DATE, "lastUpdatedAt")
        self.assertEqual(state, {})

    def test_limited_stream_does_not_initialize_bookmark(self):
        state = {"bookmarks": {}}
        instance = Features(make_config({"features": 1}))
        self.assertEqual(instance.get_bookmark(state, "features", START_DATE, "lastUpdatedAt"), START_DATE)
        self.assertEqual(state, {"bookmarks": {}})

    def test_unlimited_stream_writes_bookmark(self):
        state = {}
        Features(make_config({"pages": 1})).update_bookmark(state, "features", START_DATE, "lastUpdatedAt")
        self.assertEqual(state["bookmarks"]["features"]["lastUpdatedAt"], START_DATE)


@mock.patch("tap_pendo.streams.update_currently_syncing")
@mock.patch("tap_pendo.sync.singer.write_state")
@mock.patch("tap_pendo.streams.singer.write_state")
@mock.patch("tap_pendo.sync.singer.write_record")
class TestIncrementalSync(unittest.TestCase):
    def setUp(self):
        for stream_cls in streams.STREAMS.values():
            stream_cls.stream = None
            stream_cls.last_processed = None

    def build(self, config):
        instance = Features(config)
        instance.stream = catalog_entry(instance)
        return instance

    @mock.patch("tap_pendo.streams.Stream.request")
    def test_limited_stream_stops_at_limit_and_keeps_state(self, mock_request, mock_write_record, *_):
        mock_request.side_effect = [{"results": features(10)}, {"results": features(1, start=9)}]
        state = {"bookmarks": {"features": {"lastUpdatedAt": START_DATE}}}
        original_state = copy.deepcopy(state)
        instance = self.build(make_config({"features": 3}))

        count = sync_stream(state, START_DATE, instance)

        self.assertEqual(count, 3)
        self.assertEqual(mock_write_record.call_count, 3)
        self.assertEqual(mock_request.call_count, 1)
        self.assertEqual(state, original_state)
        body = mock_request.call_args[1]["json"]
        self.assertEqual(body["request"]["pipeline"][-1]["limit"], 3)

    @mock.patch("tap_pendo.streams.Stream.request")
    def test_zero_limit_emits_nothing_and_skips_requests(self, mock_request, mock_write_record, *_):
        state = {}
        count = sync_stream(state, START_DATE, self.build(make_config({"features": 0})))
        self.assertEqual(count, 0)
        mock_request.assert_not_called()
        mock_write_record.assert_not_called()
        self.assertEqual(state, {})

    @mock.patch("tap_pendo.streams.Stream.request")
    def test_unlimited_stream_syncs_everything_and_bookmarks(self, mock_request, mock_write_record, *_):
        mock_request.side_effect = [{"results": features(10)}, {"results": features(1, start=9)}]
        state = {}
        count = sync_stream(state, START_DATE, self.build(make_config({"pages": 1})))
        self.assertEqual(count, 10)
        self.assertIn("features", state["bookmarks"])

    @mock.patch("tap_pendo.streams.Stream.sync_substream")
    @mock.patch("tap_pendo.streams.Stream.request")
    def test_limited_parent_syncs_children_for_its_records_only(self, mock_request, mock_sync_substream, *_):
        FeatureEvents.stream = catalog_entry(FeatureEvents(make_config()))
        mock_request.side_effect = [{"results": features(10)}]
        instance = self.build(make_config({"features": 2}))

        instance.sync({}, strptime_to_utc(START_DATE))

        parent_records = mock_sync_substream.call_args[0][3]
        self.assertEqual([r["id"] for r in parent_records], ["feature-0", "feature-1"])


@mock.patch("tap_pendo.streams.update_currently_syncing")
@mock.patch("tap_pendo.streams.singer.write_state")
@mock.patch("tap_pendo.streams.singer.write_schema")
@mock.patch("tap_pendo.streams.singer.write_record")
class TestChildStreamLimit(unittest.TestCase):
    def setUp(self):
        for stream_cls in streams.STREAMS.values():
            stream_cls.stream = None

    @mock.patch("tap_pendo.streams.FeatureEvents.sync")
    def test_limited_child_stops_across_parents_and_keeps_state(self, mock_child_sync, mock_write_record, *_):
        config = make_config({"feature_events": 4})
        FeatureEvents.stream = catalog_entry(FeatureEvents(config))
        mock_child_sync.side_effect = lambda state, bookmark, key_id: ((None, feature_events(key_id, 3)), False)
        parents = [{"id": f"feature-{i}", "last_updated_at": BASE_MS + i} for i in range(5)]
        parent = Features(config)
        state = {}

        parent.sync_substream(state, parent, FeatureEvents(config), parents)

        self.assertEqual(mock_write_record.call_count, 4)
        self.assertEqual(mock_child_sync.call_count, 2)
        self.assertEqual(state, {})

    @mock.patch("tap_pendo.streams.FeatureEvents.sync")
    def test_unlimited_child_reads_every_parent(self, mock_child_sync, mock_write_record, *_):
        config = make_config()
        FeatureEvents.stream = catalog_entry(FeatureEvents(config))
        mock_child_sync.side_effect = lambda state, bookmark, key_id: ((None, feature_events(key_id, 3)), False)
        parents = [{"id": f"feature-{i}", "last_updated_at": BASE_MS + i} for i in range(5)]
        parent = Features(config)
        state = {}

        parent.sync_substream(state, parent, FeatureEvents(config), parents)

        self.assertEqual(mock_write_record.call_count, 15)
        self.assertIn("feature_events", state["bookmarks"])


class TestLazyAggregationLimit(unittest.TestCase):
    def tearDown(self):
        VisitorHistory.stream = None

    @mock.patch("tap_pendo.streams.Visitors.is_loop_required", return_value=False)
    @mock.patch("tap_pendo.streams.Stream.sync_substream")
    @mock.patch("tap_pendo.streams.Stream.request")
    def test_limited_visitors_sync_history_for_its_records_only(self, mock_request, mock_sync_substream, *_):
        VisitorHistory.stream = catalog_entry(VisitorHistory(make_config()))
        mock_request.side_effect = lambda *args, **kwargs: iter([{"visitor_id": f"v{i}"} for i in range(5)])
        captured = []
        mock_sync_substream.side_effect = lambda state, parent, sub_stream, records: captured.extend(records)

        Visitors(make_config({"visitors": 1})).sync({})

        self.assertEqual(captured, [{"visitor_id": "v0"}])


class FakeFullTableStream:
    def __init__(self, records, max_records):
        self.stream = CatalogEntry(tap_stream_id="fake", stream="fake",
                                   schema=Schema.from_dict({"type": "object", "properties": {}}),
                                   metadata=[])
        self.records = records
        self.max_records = max_records

    def sync(self, state):
        return self.stream, self.records

    def transform(self, record):
        return record


@mock.patch("tap_pendo.sync.singer.write_record")
class TestFullTableSync(unittest.TestCase):
    def test_limited_full_table_stops_at_limit(self, mock_write_record):
        self.assertEqual(sync_full_table({}, FakeFullTableStream([{}, {}, {}], 2)), 2)
        self.assertEqual(mock_write_record.call_count, 2)

    def test_unlimited_full_table_syncs_everything(self, mock_write_record):
        self.assertEqual(sync_full_table({}, FakeFullTableStream([{}, {}, {}], None)), 3)


# sync.py and streams.py share the singer module, so one patch of each writer covers both
@mock.patch("singer.write_schema")
@mock.patch("singer.write_state")
@mock.patch("singer.write_record")
class TestTapSync(unittest.TestCase):
    def setUp(self):
        for stream_cls in streams.STREAMS.values():
            stream_cls.stream = None
            stream_cls.last_processed = None

    @mock.patch("tap_pendo.streams.FeatureEvents.sync")
    @mock.patch("tap_pendo.streams.Stream.request")
    def test_limited_parent_and_child_keep_state(self, mock_request, mock_child_sync, mock_write_record, *_):
        from singer.catalog import Catalog
        from tap_pendo import sync

        config = make_config({"features": 2, "feature_events": 3})
        catalog = Catalog([catalog_entry(Features(config)), catalog_entry(FeatureEvents(config))])
        mock_request.side_effect = [{"results": features(5)}]
        mock_child_sync.side_effect = lambda state, bookmark, key_id: ((None, feature_events(key_id, 2)), False)
        state = {"bookmarks": {"features": {"lastUpdatedAt": START_DATE}}}
        original_bookmarks = copy.deepcopy(state["bookmarks"])

        sync(config, state, catalog)

        emitted = [c[0][0] for c in mock_write_record.call_args_list]
        self.assertEqual(emitted.count("features"), 2)
        self.assertEqual(emitted.count("feature_events"), 3)
        self.assertEqual(state["bookmarks"], original_bookmarks)
        self.assertNotIn("currently_syncing", state)
