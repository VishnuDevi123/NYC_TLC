import csv
import json
import math
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer


SOURCE_TOPIC = "taxi_trips_events"
CLEAN_TOPIC = "clean_trip_events"
DEAD_LETTER_TOPIC = "dead_letter_events"
ANOMALY_TOPIC = "anomaly_events"
ZONE_METRICS_TOPIC = "zone_metrics"
ROUTE_METRICS_TOPIC = "route_metrics"

if "ZONE_LOOKUP_PATH" not in os.environ:
    SOURCE_TOPIC = os.environ.get("SOURCE_TOPIC", SOURCE_TOPIC)
    CLEAN_TOPIC = os.environ.get("CLEAN_TOPIC", CLEAN_TOPIC)
    DEAD_LETTER_TOPIC = os.environ.get("DEAD_LETTER_TOPIC", DEAD_LETTER_TOPIC)
    ANOMALY_TOPIC = os.environ.get("ANOMALY_TOPIC", ANOMALY_TOPIC)
    ZONE_METRICS_TOPIC = os.environ.get("ZONE_METRICS_TOPIC", ZONE_METRICS_TOPIC)
    ROUTE_METRICS_TOPIC = os.environ.get("ROUTE_METRICS_TOPIC", ROUTE_METRICS_TOPIC)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CONSUMER_GROUP_ID = "taxi-trip-analytics-consumer"
CONSUMER_LOG_INTERVAL = 50
ROLLING_WINDOW_SIZE = 50
MAX_FARE_PER_MILE = float(os.getenv("MAX_FARE_PER_MILE", "100"))
MAX_TRIP_DURATION_MIN = float(os.getenv("MAX_TRIP_DURATION_MIN", "180"))
HIGH_TIP_PERCENT_THRESHOLD = float(os.getenv("HIGH_TIP_PERCENT_THRESHOLD", "60"))
HIGH_TOTAL_AMOUNT_THRESHOLD = float(os.getenv("HIGH_TOTAL_AMOUNT_THRESHOLD", "500"))
SHORT_TRIP_DURATION_MIN = float(os.getenv("SHORT_TRIP_DURATION_MIN", "60"))
SHORT_TRIP_DISTANCE_MILES = float(os.getenv("SHORT_TRIP_DISTANCE_MILES", "1"))
ZONE_LOOKUP_FILE_CANDIDATES = tuple(
    Path(path)
    for path in (
        os.getenv("ZONE_LOOKUP_PATH"),
        Path(__file__).resolve().parent / "taxi_zone_lookup.csv",
        Path(__file__).resolve().parent.parent / "taxi_zone_lookup.csv",
    )
    if path
)

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


def load_zone_lookup():
    checked_paths = []
    for zone_lookup_file in ZONE_LOOKUP_FILE_CANDIDATES:
        checked_paths.append(str(zone_lookup_file))
        if not zone_lookup_file.exists() or not zone_lookup_file.is_file():
            continue

        zones = {}
        with zone_lookup_file.open(newline="") as zone_file:
            reader = csv.DictReader(zone_file)
            for row in reader:
                location_id = parse_location_id(row.get("LocationID"))
                if location_id is None:
                    continue
                zones[location_id] = {
                    "borough": row.get("Borough"),
                    "zone": row.get("Zone"),
                    "service_zone": row.get("service_zone"),
                }
        if zones:
            return zones

    raise FileNotFoundError(f"taxi_zone_lookup.csv not found or empty. Checked: {', '.join(checked_paths)}")

# current time in UTC for when the event was received, used for validating future pickup times and calculating trip durations
def utc_now():
    return datetime.now(timezone.utc)


def isoformat(value):
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()

# parsing datetime fields from the raw events and handling different formats and timezones.
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

# location id should not be a float and should be positive integer so thats why the checks are more strict
def parse_location_id(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    # make sure the string representation matches to avoid parsing "1abc" as 1
    if str(value).strip() not in {str(parsed), f"{parsed}.0"}:
        return None
    return parsed


ZONE_LOOKUP = load_zone_lookup()

# for creating the reason object, essentially creating an dict object and include the type of reason and value of the field
# that caused the validation failure, this will be used for monitoring and alerting on the most common reasons for dlq events
def reason(reason_type, field, value=None):
    item = {"type": reason_type, "field": field}
    if value is not None:
        item["value"] = value
    return item

# validation function that checks for required fields, validates datetime formats and logical consistency, 
# checks numeric fields for valid values, and returns a list of reasons if any validation fails
def validate_raw_event(event, now=None):
    # initily empty list
    reasons = []
    now = now or utc_now()

    for field in REQUIRED_FIELDS:
        if field not in event:
            reasons.append(reason("missing_required_field", field))
    # if initiliy empty reason is not empty then it means some required fields are missing and we can return early without doing further validation checks
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

    # for numeric fields we check if they are None after parsing which means they are invalid and the event needs to be sent to dlq
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

    pu_location_id = parse_location_id(event.get("PULocationID"))
    do_location_id = parse_location_id(event.get("DOLocationID"))

    if pu_location_id is None:
        reasons.append(reason("invalid_pu_location_id", "PULocationID", event.get("PULocationID")))
    elif pu_location_id not in ZONE_LOOKUP:
        reasons.append(reason("unknown_pu_location_id", "PULocationID", event.get("PULocationID")))

    if do_location_id is None:
        reasons.append(reason("invalid_do_location_id", "DOLocationID", event.get("DOLocationID")))
    elif do_location_id not in ZONE_LOOKUP:
        reasons.append(reason("unknown_do_location_id", "DOLocationID", event.get("DOLocationID")))

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


def build_clean_event(event, processed_at):
    pickup_time = parse_datetime(event["tpep_pickup_datetime"])
    dropoff_time = parse_datetime(event["tpep_dropoff_datetime"])
    pu_location_id = parse_location_id(event["PULocationID"])
    do_location_id = parse_location_id(event["DOLocationID"])

    passenger_count = parse_float(event["passenger_count"])
    trip_distance = parse_float(event["trip_distance"])
    fare_amount = parse_float(event["fare_amount"])
    tip_amount = parse_float(event.get("tip_amount")) or 0.0
    total_amount = parse_float(event["total_amount"])
    trip_duration_min = (dropoff_time - pickup_time).total_seconds() / 60

    pickup_zone = ZONE_LOOKUP[pu_location_id]
    dropoff_zone = ZONE_LOOKUP[do_location_id]

    return {
        "event_id": event["event_id"],
        "event_time": isoformat(pickup_time),
        "pickup_date": pickup_time.date().isoformat(),
        "pickup_hour": pickup_time.hour,
        "dropoff_time": isoformat(dropoff_time),
        "pu_location_id": pu_location_id,
        "do_location_id": do_location_id,
        "pickup_borough": pickup_zone["borough"],
        "pickup_zone": pickup_zone["zone"],
        "pickup_service_zone": pickup_zone["service_zone"],
        "dropoff_borough": dropoff_zone["borough"],
        "dropoff_zone": dropoff_zone["zone"],
        "dropoff_service_zone": dropoff_zone["service_zone"],
        "passenger_count": passenger_count,
        "trip_distance": trip_distance,
        "trip_duration_min": trip_duration_min,
        "fare_amount": fare_amount,
        "tip_amount": tip_amount,
        "total_amount": total_amount,
        "fare_per_mile": fare_amount / trip_distance,
        "tip_percent": (tip_amount / total_amount * 100) if total_amount else 0.0,
        "route_key": f"{pu_location_id}->{do_location_id}",
        "processed_at": isoformat(processed_at),
    }


def publish_clean_event(clean_event, producer):
    producer.send(CLEAN_TOPIC, key=clean_event["pu_location_id"], value=clean_event)

# return the anomaly type object
def anomaly_type(anomaly_name, value, threshold, severity="medium"):
    return {
        "type": anomaly_name,
        "severity": severity,
        "value": value,
        "threshold": threshold,
    }

# threshold severityu for anomalies, greater the value over threshold higher the severity
def threshold_severity(value, threshold):
    if threshold > 0 and value >= threshold * 2:
        return "high"
    return "medium"


def detect_anomalies(clean_event):
    anomalies = []
    
    # using these below fields for anomaly detection as they are the most relevant for detecting
    # potential fraud or data issues
    fare_per_mile = clean_event["fare_per_mile"]
    trip_duration_min = clean_event["trip_duration_min"]
    trip_distance = clean_event["trip_distance"]
    tip_percent = clean_event["tip_percent"]
    total_amount = clean_event["total_amount"]

    if fare_per_mile > MAX_FARE_PER_MILE:
        anomalies.append(
            anomaly_type(
                "high_fare_per_mile",
                fare_per_mile,
                MAX_FARE_PER_MILE,
                threshold_severity(fare_per_mile, MAX_FARE_PER_MILE),
            )
        )

    if trip_duration_min > MAX_TRIP_DURATION_MIN:
        anomalies.append(
            anomaly_type(
                "long_trip_duration",
                trip_duration_min,
                MAX_TRIP_DURATION_MIN,
                threshold_severity(trip_duration_min, MAX_TRIP_DURATION_MIN),
            )
        )

    if trip_duration_min > SHORT_TRIP_DURATION_MIN and trip_distance < SHORT_TRIP_DISTANCE_MILES:
        anomalies.append(
            anomaly_type(
                "long_duration_short_distance",
                {"duration_min": trip_duration_min, "distance_miles": trip_distance},
                {"duration_min": SHORT_TRIP_DURATION_MIN, "distance_miles": SHORT_TRIP_DISTANCE_MILES},
                "medium",
            )
        )

    if tip_percent > HIGH_TIP_PERCENT_THRESHOLD:
        anomalies.append(
            anomaly_type(
                "high_tip_percent",
                tip_percent,
                HIGH_TIP_PERCENT_THRESHOLD,
                threshold_severity(tip_percent, HIGH_TIP_PERCENT_THRESHOLD),
            )
        )

    if total_amount > HIGH_TOTAL_AMOUNT_THRESHOLD:
        anomalies.append(
            anomaly_type(
                "high_total_amount",
                total_amount,
                HIGH_TOTAL_AMOUNT_THRESHOLD,
                threshold_severity(total_amount, HIGH_TOTAL_AMOUNT_THRESHOLD),
            )
        )

    return anomalies

# will publish the anomaly event to the anomaly topic with the clean event and 
# the detected anomaly types, this allows downstream consumers to process
def publish_anomaly_event(clean_event, producer, anomaly_types, detected_at):
    anomaly_event = {
        "event_id": clean_event["event_id"],
        "event_time": clean_event["event_time"],
        "anomaly_types": anomaly_types,
        "clean_event": clean_event,
        "detected_at": isoformat(detected_at),
    }
    producer.send(ANOMALY_TOPIC, key=clean_event["event_id"], value=anomaly_event)
    return anomaly_event

# process the ran event by validating and if any resons proson for it to be dlq, then add to the dlq topic
def process_raw_event(event, producer, stats, now=None):
    now = now or utc_now()
    stats["total_events"] += 1

    reasons = validate_raw_event(event, now=now)
    # if reasons is not empty that means the event has some invalid fields, the event needs to be sent to dlq
    if reasons:
        publish_dlq(event, producer, reasons, now)
        stats["dlq_events"] += 1
        for item in reasons:
            stats["dlq_reason_counts"][item["type"]] += 1
        return {"status": "dlq", "reasons": reasons}

    clean_event = build_clean_event(event, now)
    publish_clean_event(clean_event, producer)
    stats["clean_events"] += 1

    # After the event is published to the clean topic, we can perform anomaly detection on the clean event and 
    # if any anomalies are detected we can publish them to the anomaly topic for downstream consumers to take action on them, 
    # this allows us to separate the concerns of data validation and anomaly detection and also allows us to monitor the anomalies 
    # separately from the validation errors in the dlq
    anomaly_types = detect_anomalies(clean_event)
    if anomaly_types:
        anomaly_event = publish_anomaly_event(clean_event, producer, anomaly_types, now)
        stats["anomaly_events"] += 1
        anomaly_reason_counts = stats.setdefault("anomaly_reason_counts", Counter())
        for item in anomaly_types:
            anomaly_reason_counts[item["type"]] += 1
        return {"status": "anomaly", "clean_event": clean_event, "anomaly_event": anomaly_event}

    return {"status": "clean", "clean_event": clean_event}

# stats for showing how many events were validated and sent to clean topic vs how many were sent to dlq
# also keeping track of the reasons for dlq events for monitoring and alerting purposes
# and also keeping track of recent pickups for calculating zone and route metrics
def build_stats():
    return {
        "total_events": 0,
        "clean_events": 0,
        "dlq_events": 0,
        "anomaly_events": 0,
        "dlq_reason_counts": Counter(),
        "anomaly_reason_counts": Counter(),
        "pickup_zone_counts": defaultdict(int),
        "recent_pickups": deque(maxlen=ROLLING_WINDOW_SIZE),
        "routes_count": defaultdict(int),
        "routes_revenue": defaultdict(float),
        "routes_distance": defaultdict(float),
        "routes_duration": defaultdict(float),
        "routes_tips": defaultdict(float),
    }

# function for printing the health status of the consumer, 
# including the number of events processed, sent to clean topic, 
# sent to dlq, and sent to anomaly topic, as well as the top reasons for dlq and anomaly events,
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
    print(f"Anomaly events produced: {stats['anomaly_events']}")
    print(f"Top DLQ reasons: {stats['dlq_reason_counts'].most_common(5)}")
    print(f"Top anomaly reasons: {stats['anomaly_reason_counts'].most_common(5)}")


def main():
    time.sleep(10)
    stats = build_stats()

    # creating consumer instance to consume from the source topic
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    
    # creating a producer instance to produce to the other topics like clean_topic, dlq_topic, anamoly_topic for downstream consumers 
    # the producer is created outside the loop to reuse the same instance for better performance instead of creating a new one for each message
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
    )

    # print initial health status before processing any messages and then loop through the message from the source topic for next batch of processing
    print_health(stats)
    for message in consumer:
        process_raw_event(message.value, producer=producer, stats=stats)
        if stats["total_events"] % CONSUMER_LOG_INTERVAL == 0:
            print_health(stats)


if __name__ == "__main__":
    main()
