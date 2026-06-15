"""Slice 1 tests: DLQ routing for impossible NYC TLC parquet-shaped rows.

"""

from __future__ import annotations

import copy
import importlib
import os
import sys
import types
import unittest
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from unittest import mock


REQUIRED_FIELDS = (
    "event_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
)


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})


def parquet_shaped_event(**overrides):
    event = {
        "VendorID": 2,
        "tpep_pickup_datetime": "2026-01-01 00:54:04",
        "tpep_dropoff_datetime": "2026-01-01 00:59:37",
        "passenger_count": 1.0,
        "trip_distance": 0.97,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": 239,
        "DOLocationID": 238,
        "payment_type": 1,
        "fare_amount": 7.2,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 3.66,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 15.86,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
        "trip_duration_min": 5.55,
        "event_id": "0-2026-01-01 00:54:04-239-238",
    }
    event.update(overrides)
    return event


class TestSlice1DLQImpossibleRecords(unittest.TestCase):
    def setUp(self):
        os.environ["SOURCE_TOPIC"] = "raw_test_topic"
        os.environ["DEAD_LETTER_TOPIC"] = "dlq_test_topic"
        os.environ["CLEAN_TOPIC"] = "clean_test_topic"
        os.environ["ZONE_METRICS_TOPIC"] = "zone_metrics_test_topic"
        os.environ["ROUTE_METRICS_TOPIC"] = "route_metrics_test_topic"

        fake_kafka = types.ModuleType("kafka")
        fake_kafka.KafkaConsumer = mock.Mock()
        fake_kafka.KafkaProducer = mock.Mock()

        self.module_patcher = mock.patch.dict(sys.modules, {"kafka": fake_kafka})
        self.sleep_patcher = mock.patch("time.sleep", return_value=None)
        self.module_patcher.start()
        self.sleep_patcher.start()
        sys.modules.pop("consumer.consumer", None)

    def tearDown(self):
        sys.modules.pop("consumer.consumer", None)
        self.sleep_patcher.stop()
        self.module_patcher.stop()

    def import_consumer(self):
        try:
            return importlib.import_module("consumer.consumer")
        except Exception as exc:  # pragma: no cover - assertion message is the test output.
            self.fail(f"consumer.consumer must be importable without starting Kafka: {exc!r}")

    def process_one(self, event):
        consumer = self.import_consumer()
        process_raw_event = getattr(consumer, "process_raw_event", None)
        self.assertTrue(
            callable(process_raw_event),
            "Slice 1 should expose process_raw_event(event, producer, stats, now=...)",
        )

        producer = FakeProducer()
        stats = {
            "total_events": 0,
            "clean_events": 0,
            "dlq_events": 0,
            "dlq_reason_counts": Counter(),
            "pickup_zone_counts": defaultdict(int),
            "recent_pickups": deque(maxlen=50),
            "routes_count": defaultdict(int),
            "routes_revenue": defaultdict(float),
            "routes_distance": defaultdict(float),
            "routes_duration": defaultdict(float),
            "routes_tips": defaultdict(float),
        }

        result = process_raw_event(
            copy.deepcopy(event),
            producer=producer,
            stats=stats,
            now=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        return result, producer, stats

    def assert_dlq_only(self, event, expected_reason_type, expected_field=None):
        _result, producer, stats = self.process_one(event)

        self.assertEqual(len(producer.sent), 1)
        sent = producer.sent[0]
        self.assertEqual(sent["topic"], "dlq_test_topic")
        self.assertIsInstance(sent["value"], dict)
        self.assertEqual(sent["value"]["error_type"], "validation_failed")
        self.assertEqual(sent["value"]["raw_event"], event)
        self.assertIn("received_at", sent["value"])

        reasons = sent["value"]["reasons"]
        self.assertIsInstance(reasons, list)
        self.assertIn(expected_reason_type, {reason["type"] for reason in reasons})
        if expected_field is not None:
            matching = [reason for reason in reasons if reason["type"] == expected_reason_type]
            self.assertEqual(matching[0]["field"], expected_field)

        self.assertEqual(stats["total_events"], 1)
        self.assertEqual(stats["clean_events"], 0)
        self.assertEqual(stats["dlq_events"], 1)
        self.assertEqual(stats["dlq_reason_counts"][expected_reason_type], 1)
        self.assertEqual(dict(stats["pickup_zone_counts"]), {})
        self.assertEqual(list(stats["recent_pickups"]), [])
        self.assertEqual(dict(stats["routes_count"]), {})
        self.assertEqual(dict(stats["routes_revenue"]), {})

    def test_missing_required_field_goes_to_structured_dlq_only(self):
        for field in REQUIRED_FIELDS:
            with self.subTest(field=field):
                event = parquet_shaped_event()
                event.pop(field)
                self.assert_dlq_only(event, "missing_required_field", field)

    def test_impossible_numeric_values_go_to_structured_dlq_only(self):
        cases = (
            ("passenger_count", 0, "non_positive_passenger_count"),
            ("passenger_count", -1, "non_positive_passenger_count"),
            ("trip_distance", 0, "non_positive_trip_distance"),
            ("trip_distance", -1.2, "non_positive_trip_distance"),
            ("fare_amount", -0.01, "negative_fare_amount"),
            ("total_amount", -0.01, "negative_total_amount"),
        )
        for field, value, reason_type in cases:
            with self.subTest(field=field, value=value):
                self.assert_dlq_only(parquet_shaped_event(**{field: value}), reason_type, field)

    def test_impossible_datetime_values_go_to_structured_dlq_only(self):
        cases = (
            (
                {"tpep_pickup_datetime": "not-a-date"},
                "invalid_pickup_datetime",
                "tpep_pickup_datetime",
            ),
            (
                {"tpep_dropoff_datetime": "not-a-date"},
                "invalid_dropoff_datetime",
                "tpep_dropoff_datetime",
            ),
            (
                {"tpep_pickup_datetime": "2026-01-01 01:00:00", "tpep_dropoff_datetime": "2026-01-01 01:00:00"},
                "non_positive_trip_duration",
                "tpep_dropoff_datetime",
            ),
            (
                {"tpep_pickup_datetime": "2026-01-01 01:00:00", "tpep_dropoff_datetime": "2026-01-01 00:59:00"},
                "non_positive_trip_duration",
                "tpep_dropoff_datetime",
            ),
            (
                {"tpep_pickup_datetime": "2026-01-03 00:00:00", "tpep_dropoff_datetime": "2026-01-03 00:05:00"},
                "future_pickup_datetime",
                "tpep_pickup_datetime",
            ),
        )
        for overrides, reason_type, field in cases:
            with self.subTest(reason_type=reason_type):
                self.assert_dlq_only(parquet_shaped_event(**overrides), reason_type, field)

    def test_invalid_location_id_types_go_to_structured_dlq_only(self):
        cases = (
            ("PULocationID", None, "invalid_pu_location_id"),
            ("PULocationID", "bad-id", "invalid_pu_location_id"),
            ("DOLocationID", None, "invalid_do_location_id"),
            ("DOLocationID", "bad-id", "invalid_do_location_id"),
        )
        for field, value, reason_type in cases:
            with self.subTest(field=field, value=value):
                self.assert_dlq_only(parquet_shaped_event(**{field: value}), reason_type, field)

    def test_valid_slice1_event_does_not_publish_dlq(self):
        _result, producer, stats = self.process_one(parquet_shaped_event())

        self.assertEqual(
            [sent for sent in producer.sent if sent["topic"] == "dlq_test_topic"],
            [],
        )
        self.assertEqual(stats["total_events"], 1)
        self.assertEqual(stats["dlq_events"], 0)
        self.assertEqual(stats["dlq_reason_counts"], Counter())


if __name__ == "__main__":
    unittest.main()
