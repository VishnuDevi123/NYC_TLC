"""Slice 2 tests: zone enrichment and clean-trip event publishing."""

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


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key=None, value=None):
        self.sent.append({"topic": topic, "key": key, "value": value})


def parquet_shaped_event(**overrides):
    event = {
        "VendorID": 2,
        "tpep_pickup_datetime": "2026-01-01 13:15:00",
        "tpep_dropoff_datetime": "2026-01-01 13:49:12",
        "passenger_count": 1.0,
        "trip_distance": 12.4,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": 132,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 52.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 10.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 73.5,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
        "event_id": "slice2-clean-132-236",
    }
    event.update(overrides)
    return event


class TestSlice2ZoneEnrichedCleanEvents(unittest.TestCase):
    def setUp(self):
        os.environ["ZONE_LOOKUP_PATH"] = "taxi_zone_lookup.csv"

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
            "Slice 2 should expose process_raw_event(event, producer, stats, now=...)",
        )

        producer = FakeProducer()
        stats = {
            "total_events": 0,
            "clean_events": 0,
            "dlq_events": 0,
            "anomaly_events": 0,
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

    def test_valid_raw_event_publishes_one_zone_enriched_clean_event(self):
        result, producer, stats = self.process_one(parquet_shaped_event())

        self.assertEqual(result["status"], "clean")
        self.assertEqual(len(producer.sent), 1)

        sent = producer.sent[0]
        self.assertEqual(sent["topic"], "clean_trip_events")
        self.assertEqual(sent["key"], 132)

        clean_event = sent["value"]
        self.assertEqual(clean_event["event_id"], "slice2-clean-132-236")
        self.assertEqual(clean_event["event_time"], "2026-01-01T13:15:00+00:00")
        self.assertEqual(clean_event["pickup_date"], "2026-01-01")
        self.assertEqual(clean_event["pickup_hour"], 13)
        self.assertEqual(clean_event["dropoff_time"], "2026-01-01T13:49:12+00:00")
        self.assertEqual(clean_event["pu_location_id"], 132)
        self.assertEqual(clean_event["do_location_id"], 236)

        self.assertEqual(clean_event["pickup_borough"], "Queens")
        self.assertEqual(clean_event["pickup_zone"], "JFK Airport")
        self.assertEqual(clean_event["pickup_service_zone"], "Airports")
        self.assertEqual(clean_event["dropoff_borough"], "Manhattan")
        self.assertEqual(clean_event["dropoff_zone"], "Upper East Side North")
        self.assertEqual(clean_event["dropoff_service_zone"], "Yellow Zone")

        self.assertEqual(clean_event["passenger_count"], 1.0)
        self.assertEqual(clean_event["trip_distance"], 12.4)
        self.assertAlmostEqual(clean_event["trip_duration_min"], 34.2)
        self.assertEqual(clean_event["fare_amount"], 52.0)
        self.assertEqual(clean_event["tip_amount"], 10.0)
        self.assertEqual(clean_event["total_amount"], 73.5)
        self.assertAlmostEqual(clean_event["fare_per_mile"], 4.19, places=2)
        self.assertAlmostEqual(clean_event["tip_percent"], 13.61, places=2)
        self.assertEqual(clean_event["route_key"], "132->236")
        self.assertIn("processed_at", clean_event)

        self.assertEqual(stats["total_events"], 1)
        self.assertEqual(stats["clean_events"], 1)
        self.assertEqual(stats["dlq_events"], 0)
        self.assertEqual(stats["dlq_reason_counts"], Counter())

    def test_unknown_pickup_location_id_goes_to_dlq_only(self):
        event = parquet_shaped_event(PULocationID=999999)
        result, producer, stats = self.process_one(event)

        self.assertEqual(result["status"], "dlq")
        self.assertEqual(len(producer.sent), 1)
        sent = producer.sent[0]
        self.assertEqual(sent["topic"], "dead_letter_events")
        self.assertEqual(sent["key"], "slice2-clean-132-236")
        self.assertEqual(sent["value"]["error_type"], "validation_failed")
        self.assertEqual(sent["value"]["raw_event"], event)
        self.assertIn("unknown_pu_location_id", {reason["type"] for reason in sent["value"]["reasons"]})

        self.assertEqual(stats["total_events"], 1)
        self.assertEqual(stats["clean_events"], 0)
        self.assertEqual(stats["dlq_events"], 1)
        self.assertEqual(stats["dlq_reason_counts"]["unknown_pu_location_id"], 1)

    def test_unknown_dropoff_location_id_goes_to_dlq_only(self):
        event = parquet_shaped_event(DOLocationID=999999)
        result, producer, stats = self.process_one(event)

        self.assertEqual(result["status"], "dlq")
        self.assertEqual(len(producer.sent), 1)
        sent = producer.sent[0]
        self.assertEqual(sent["topic"], "dead_letter_events")
        self.assertEqual(sent["key"], "slice2-clean-132-236")
        self.assertEqual(sent["value"]["error_type"], "validation_failed")
        self.assertEqual(sent["value"]["raw_event"], event)
        self.assertIn("unknown_do_location_id", {reason["type"] for reason in sent["value"]["reasons"]})

        self.assertEqual(stats["total_events"], 1)
        self.assertEqual(stats["clean_events"], 0)
        self.assertEqual(stats["dlq_events"], 1)
        self.assertEqual(stats["dlq_reason_counts"]["unknown_do_location_id"], 1)


if __name__ == "__main__":
    unittest.main()
