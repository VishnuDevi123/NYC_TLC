import csv
import json
import math
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

# The IDEA is
# PRODUCER --> Bootstrap Servers (send data)
# CONSUMER <-- Bootstrap Servers (receive data)

time.sleep(10)  # Wait for Kafka to be ready
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "taxi_trips_events")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "clean_trip_events")
DEAD_LETTER_TOPIC = os.getenv("DEAD_LETTER_TOPIC", "dead_letter_events")
ZONE_METRICS_TOPIC = os.getenv("ZONE_METRICS_TOPIC", "zone_metrics")
ROUTE_METRICS_TOPIC = os.getenv("ROUTE_METRICS_TOPIC", "route_metrics")

LOG_INTERVAL = int(os.getenv("CONSUMER_LOG_INTERVAL", "50"))
ROLLING_WINDOW_SIZE = int(os.getenv("ROLLING_WINDOW_SIZE", "50"))
MIN_PROFITABLE_ROUTE_TRIPS = int(os.getenv("MIN_PROFITABLE_ROUTE_TRIPS", "3"))
MAX_FARE_PER_MILE = float(os.getenv("MAX_FARE_PER_MILE", "100"))
MAX_TRIP_DURATION_MIN = float(os.getenv("MAX_TRIP_DURATION_MIN", "180"))

# Kafka bootstrap servers which will be used to connect to the Kafka cluster for consuming messages.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "taxi-trip-analytics-consumer")
ZONE_LOOKUP_PATH = Path(os.getenv("ZONE_LOOKUP_PATH", "/app/taxi_zone_lookup.csv"))


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

def as_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def calculate_route_profitability():
    profitable_routes = []

    for route_key, trip_count in routes_count.items():
        if trip_count < MIN_PROFITABLE_ROUTE_TRIPS:
            continue

        revenue = routes_revenue[route_key]
        distance = routes_distance[route_key]
        duration = routes_duration[route_key]
        tips = routes_tips[route_key]

        revenue_per_mile = revenue / distance if distance > 0 else 0.0
        revenue_per_minute = revenue / duration if duration > 0 else 0.0
        avg_revenue = revenue / trip_count
        avg_tip = tips / trip_count

        # Simple score that rewards routes that earn well by distance, time, and trip.
        score = (revenue_per_mile * 0.4) + (revenue_per_minute * 0.4) + (avg_revenue * 0.2)

        profitable_routes.append(
            {
                "route": route_key,
                "trips": trip_count,
                "revenue": round(revenue, 2),
                "revenue_per_mile": round(revenue_per_mile, 2),
                "revenue_per_minute": round(revenue_per_minute, 2),
                "avg_revenue": round(avg_revenue, 2),
                "avg_tip": round(avg_tip, 2),
                "score": round(score, 2),
            }
        )

    return sorted(profitable_routes, key=lambda route: route["score"], reverse=True)[:5]


total_events = 0
total_fare = 0.0
pickup_zone_counts = defaultdict(int)
total_distance = 0.0
total_duration = 0.0
anomaly_counts = Counter()
recent_pickups = deque(maxlen=ROLLING_WINDOW_SIZE)

routes_count = defaultdict(int)
routes_revenue = defaultdict(float)
routes_distance = defaultdict(float)
routes_duration = defaultdict(float)
routes_tips = defaultdict(float)

print(f"Listening to topic: {TOPIC}")

# consume messages from the Kafka topic and update the total number of events, total fare amount, and count of pickups per zone
for message in consumer:
    event = message.value
    total_events += 1
    fare_amount = as_float(event.get("fare_amount"))
    pu_location = event.get("PULocationID")
    trip_distance = as_float(event.get("trip_distance"))
    trip_duration = as_float(event.get("trip_duration_min", event.get("trip_duration")))
    do_location = event.get("DOLocationID")

    total_amount = as_float(event.get("total_amount"))
    tip_amount = as_float(event.get("tip_amount"))

    if pu_location is not None and do_location is not None and total_amount is not None:
        route_key = (pu_location, do_location)
        routes_revenue[route_key] += total_amount
        routes_distance[route_key] += trip_distance
        routes_duration[route_key] += trip_duration
        routes_tips[route_key] += tip_amount

    if pu_location is not None and do_location is not None:
        route_key = (pu_location, do_location)
        routes_count[route_key] += 1

    total_duration += trip_duration
    total_distance += trip_distance

    if pu_location is not None:
        pickup_zone_counts[pu_location] += 1
        recent_pickups.append(pu_location)

    total_fare += fare_amount

    if trip_distance <= 0:
        anomaly_counts["non_positive_distance"] += 1
    if fare_amount <= 0 or total_amount <= 0:
        anomaly_counts["non_positive_amount"] += 1
    if trip_distance > 0 and total_amount / trip_distance > 100:
        anomaly_counts["high_fare_per_mile"] += 1
    if trip_duration > 180:
        anomaly_counts["long_duration"] += 1
    if trip_duration > 60 and trip_distance < 1:
        anomaly_counts["long_duration_low_distance"] += 1

    if total_events % LOG_INTERVAL == 0:
        avg_distance = total_distance / total_events if total_events > 0 else 0.0
        avg_duration = total_duration / total_events if total_events > 0 else 0.0
        avg_fare = total_fare / total_events if total_events > 0 else 0.0
        # Get the top 5 pickup zones by count
        top_zones = sorted(pickup_zone_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        top_routes = sorted(routes_count.items(), key=lambda x: x[1], reverse=True)[:5]
        top_revenue_routes = sorted(routes_revenue.items(), key=lambda x: x[1], reverse=True)[:5]
        top_profitable_routes = calculate_route_profitability()
        rolling_hot_zones = Counter(recent_pickups).most_common(5)

        print("\n--- Current Statistics ---")
        print(f"Total events consumed: {total_events}")
        print(f"Average fare so far: {avg_fare:.2f}")
        print(f"Average distance so far: {avg_distance:.2f}")
        print(f"Average duration so far: {avg_duration:.2f} minutes")
        print(f"Top 5 pickup zones: {top_zones}")
        print(f"Top 5 pickup zones in last {ROLLING_WINDOW_SIZE} events: {rolling_hot_zones}")
        print(f"Top 5 routes by trip count: {top_routes}")
        print(f"Top 5 routes by revenue: {top_revenue_routes}")
        print(f"Top 5 profitable routes: {top_profitable_routes}")
        print(f"Anomaly counts: {dict(anomaly_counts)}")