import os
import time
import json
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


TOPIC = "taxi_trips_events"

# Kafka bootstrap servers which will be used to connect to the Kafka cluster for consuming messages.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def create_consumer(max_retries=12, retry_delay_seconds=5):
    for attempt in range(1, max_retries + 1):
        try:
            # Initialize Kafka consumer with bootstrap servers, auto offset reset to earliest to consume messages from the beginning of the topic,
            # enable auto commit to automatically commit offsets after consuming messages, and value deserializer to convert the JSON messages back to Python dictionaries.
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
        except NoBrokersAvailable:
            if attempt == max_retries:
                raise
            print(
                f"Kafka broker unavailable at {BOOTSTRAP_SERVERS}. "
                f"Retrying in {retry_delay_seconds}s ({attempt}/{max_retries})..."
            )
            time.sleep(retry_delay_seconds)


consumer = create_consumer()

print(f"Listening to topic: {TOPIC}")

for message in consumer:
    print(message.value)
