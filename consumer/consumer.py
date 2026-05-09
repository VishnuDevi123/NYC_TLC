import os
import time
import json
from collections import Counter, defaultdict, deque
from kafka import KafkaConsumer

# The IDEA is
# PRODUCER --> Bootstrap Servers (send data)
# CONSUMER <-- Bootstrap Servers (receive data)

time.sleep(10)  # Wait for Kafka to be ready
TOPIC = "taxi_trips_events"
LOG_INTERVAL = int(os.getenv("CONSUMER_LOG_INTERVAL", "50"))
ROLLING_WINDOW_SIZE = int(os.getenv("ROLLING_WINDOW_SIZE", "50"))

# Kafka bootstrap servers which will be used to connect to the Kafka cluster for consuming messages.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

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


total_events = 0
total_fare = 0.0
pickup_zone_counts = defaultdict(int)
total_distance = 0.0
total_duration = 0.0
anomaly_counts = Counter()
recent_pickups = deque(maxlen=ROLLING_WINDOW_SIZE)

routes_count = defaultdict(int)
routes_revenue = defaultdict(float)

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

    if pu_location is not None and do_location is not None and total_amount is not None:
        route_key = (pu_location, do_location)
        routes_revenue[route_key] += total_amount

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
        print(f"Anomaly counts: {dict(anomaly_counts)}")
