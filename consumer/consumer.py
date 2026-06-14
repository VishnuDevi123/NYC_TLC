import json
import math
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer


SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "taxi_trips_events")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "clean_trip_events")
DEAD_LETTER_TOPIC = os.getenv("DEAD_LETTER_TOPIC", "dead_letter_events")
ANOMALY_TOPIC = os.getenv("ANOMALY_TOPIC", "anomaly_events")
ZONE_METRICS_TOPIC = os.getenv("ZONE_METRICS_TOPIC", "zone_metrics")
ROUTE_METRICS_TOPIC = os.getenv("ROUTE_METRICS_TOPIC", "route_metrics")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "taxi-trip-analytics-consumer")
CONSUMER_LOG_INTERVAL = int(os.getenv("CONSUMER_LOG_INTERVAL", "50"))
ROLLING_WINDOW_SIZE = int(os.getenv("ROLLING_WINDOW_SIZE", "50"))

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


def utc_now():
    return datetime.now(timezone.utc)


def isoformat(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_datetime(value):
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_float(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def parse_location_id(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if str(value).strip() not in {str(parsed), f"{parsed}.0"}:
        return None
    return parsed


def reason(reason_type, field, value=None):
    item = {"type": reason_type, "field": field}
    if value is not None:
        item["value"] = value
    return item


def validate_raw_event(event, now=None):
    reasons = []
    now = now or utc_now()

    for field in REQUIRED_FIELDS:
        if field not in event:
            reasons.append(reason("missing_required_field", field))

    if reasons:
        return reasons

    pickup_time = parse_datetime(event.get("tpep_pickup_datetime"))
    dropoff_time = parse_datetime(event.get("tpep_dropoff_datetime"))

    if pickup_time is None:
        reasons.append(
            reason(
                "invalid_pickup_datetime",
                "tpep_pickup_datetime",
                event.get("tpep_pickup_datetime"),
            )
        )
    if dropoff_time is None:
        reasons.append(
            reason(
                "invalid_dropoff_datetime",
                "tpep_dropoff_datetime",
                event.get("tpep_dropoff_datetime"),
            )
        )
    if pickup_time is not None and pickup_time > now:
        reasons.append(
            reason(
                "future_pickup_datetime",
                "tpep_pickup_datetime",
                event.get("tpep_pickup_datetime"),
            )
        )
    if pickup_time is not None and dropoff_time is not None and dropoff_time <= pickup_time:
        reasons.append(
            reason(
                "non_positive_trip_duration",
                "tpep_dropoff_datetime",
                event.get("tpep_dropoff_datetime"),
            )
        )

    passenger_count = parse_float(event.get("passenger_count"))
    trip_distance = parse_float(event.get("trip_distance"))
    fare_amount = parse_float(event.get("fare_amount"))
    total_amount = parse_float(event.get("total_amount"))

    if passenger_count is None:
        reasons.append(reason("invalid_passenger_count", "passenger_count", event.get("passenger_count")))
    elif passenger_count <= 0:
        reasons.append(reason("non_positive_passenger_count", "passenger_count", event.get("passenger_count")))

    if trip_distance is None:
        reasons.append(reason("invalid_trip_distance", "trip_distance", event.get("trip_distance")))
    elif trip_distance <= 0:
        reasons.append(reason("non_positive_trip_distance", "trip_distance", event.get("trip_distance")))

    if fare_amount is None:
        reasons.append(reason("invalid_fare_amount", "fare_amount", event.get("fare_amount")))
    elif fare_amount < 0:
        reasons.append(reason("negative_fare_amount", "fare_amount", event.get("fare_amount")))

    if total_amount is None:
        reasons.append(reason("invalid_total_amount", "total_amount", event.get("total_amount")))
    elif total_amount < 0:
        reasons.append(reason("negative_total_amount", "total_amount", event.get("total_amount")))

    if parse_location_id(event.get("PULocationID")) is None:
        reasons.append(reason("invalid_pu_location_id", "PULocationID", event.get("PULocationID")))
    if parse_location_id(event.get("DOLocationID")) is None:
        reasons.append(reason("invalid_do_location_id", "DOLocationID", event.get("DOLocationID")))

    return reasons


def publish_dlq(event, producer, reasons, received_at):
    dlq_event = {
        "error_type": "validation_failed",
        "reasons": reasons,
        "raw_event": event,
        "received_at": isoformat(received_at),
    }
    producer.send(DEAD_LETTER_TOPIC, key=event.get("event_id") or f"processing-{uuid4()}", value=dlq_event)
    return dlq_event


def process_raw_event(event, producer, stats, now=None):
    now = now or utc_now()
    stats["total_events"] += 1

    reasons = validate_raw_event(event, now=now)
    if reasons:
        publish_dlq(event, producer, reasons, now)
        stats["dlq_events"] += 1
        for item in reasons:
            stats["dlq_reason_counts"][item["type"]] += 1
        return {"status": "dlq", "reasons": reasons}

    return {"status": "accepted"}


def build_stats():
    return {
        "total_events": 0,
        "clean_events": 0,
        "dlq_events": 0,
        "anomaly_events": 0,
        "dlq_reason_counts": Counter(),
        "pickup_zone_counts": defaultdict(int),
        "recent_pickups": deque(maxlen=ROLLING_WINDOW_SIZE),
        "routes_count": defaultdict(int),
        "routes_revenue": defaultdict(float),
        "routes_distance": defaultdict(float),
        "routes_duration": defaultdict(float),
        "routes_tips": defaultdict(float),
    }


def print_health(stats):
    print("\n--- Consumer Health ---")
    print(f"Source topic: {SOURCE_TOPIC}")
    print(f"Clean topic: {CLEAN_TOPIC}")
    print(f"Dead letter topic: {DEAD_LETTER_TOPIC}")
    print(f"Anomaly topic: {ANOMALY_TOPIC}")
    print(f"Zone metrics topic: {ZONE_METRICS_TOPIC}")
    print(f"Route metrics topic: {ROUTE_METRICS_TOPIC}")
    print(f"Total raw events seen: {stats['total_events']}")
    print(f"Clean events produced: {stats['clean_events']}")
    print(f"DLQ events produced: {stats['dlq_events']}")
    print(f"Top DLQ reasons: {stats['dlq_reason_counts'].most_common(5)}")


def main():
    time.sleep(10)
    stats = build_stats()

    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
    )

    print_health(stats)
    for message in consumer:
        process_raw_event(message.value, producer=producer, stats=stats)
        if stats["total_events"] % CONSUMER_LOG_INTERVAL == 0:
            print_health(stats)


if __name__ == "__main__":
    main()
