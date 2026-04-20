import os
import time
import json
import pandas as pd
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# The IDEA is
# PRODUCER --> Bootstrap Servers (send data)
# CONSUMER <-- Bootstrap Servers (receive data)

time.sleep(10)  # Wait for Kafka to be ready
TOPIC = "taxi_trips_events"

# Kafka bootstrap servers which will be used to connect to the Kafka cluster for consuming messages.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


# def create_consumer(max_retries=2, retry_delay_seconds=2):
#     for attempt in range(1, max_retries + 1):
#         try:
#             # Initialize Kafka consumer with bootstrap servers, auto offset reset to earliest to consume messages from the beginning of the topic,
#             # enable auto commit to automatically commit offsets after consuming messages, and value deserializer to convert the JSON messages back to Python dictionaries.
#             return KafkaConsumer(
#                 TOPIC,
#                 bootstrap_servers=BOOTSTRAP_SERVERS,
#                 auto_offset_reset="earliest",
#                 enable_auto_commit=True,
#                 value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#             )
#         except NoBrokersAvailable:
#             if attempt == max_retries:
#                 raise
#             print(
#                 f"Kafka broker unavailable at {BOOTSTRAP_SERVERS}. "
#                 f"Retrying in {retry_delay_seconds}s ({attempt}/{max_retries})..."
#             )
#             time.sleep(retry_delay_seconds)


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

total_events = 0
total_fare = 0.0
pickup_zone_counts = defaultdict(int)

# consume messages from the Kafka topic and update the total number of events, total fare amount, and count of pickups per zone
for message in consumer:
    event = message.value
    total_events += 1
    fare_amount = event.get("fare_amount", 0.0)
    pu_location = event.get("PULocationID")
    if pu_location is not None:
        pickup_zone_counts[pu_location] += 1

    if fare_amount is not None:
        total_fare += fare_amount

    if total_events % 50 == 0:
        avg_fare = total_fare / total_events if total_events > 0 else 0.0
        # Get the top 5 pickup zones by count
        top_zones = sorted(pickup_zone_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        print("\n--- Current Statistics ---")
        print(f"Total events consumed: {total_events}")
        print(f"Average fare so far: {avg_fare:.2f}")
        print(f"Top 5 pickup zones: {top_zones}")


print(f"Listening to topic: {TOPIC}")

for message in consumer:
    print(message.value)
