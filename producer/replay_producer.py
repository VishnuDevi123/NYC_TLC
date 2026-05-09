import os
import time
import json
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

time.sleep(10)  # Wait for Kafka to be ready
# base file path and topic name
DEFAULT_FILE_PATH = Path(__file__).resolve().parent.parent / "datasets" / "valid_trips.parquet"
FILE_PATH = Path(os.getenv("TRIPS_FILE_PATH", DEFAULT_FILE_PATH))
TOPIC = "taxi_trips_events"
LOG_INTERVAL = int(os.getenv("PRODUCER_LOG_INTERVAL", "10"))
REPLAY_DELAY_SECONDS = float(os.getenv("REPLAY_DELAY_SECONDS", "1.0"))

# Kafka bootstrap servers which will be used to connect to the Kafka cluster for producing messages.
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"),
)

df = pd.read_parquet(FILE_PATH)

# Convert datetime columns to string format for JSON
datetime_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
for col in datetime_cols:
    df[col] = df[col].astype(str)


producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            key_serializer=lambda v: str(v).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        print("Connected to Kafka producer successfully.")
    except NoBrokersAvailable:
        print("Kafka not ready for producer. Retrying in 2 seconds...")
        time.sleep(2)

# def create_producer(max_retries=2, retry_delay_seconds=2):
#     for attempt in range(1, max_retries + 1):
#         try:
#             return KafkaProducer(
#                 bootstrap_servers=BOOTSTRAP_SERVERS,
#                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#             )
#         except NoBrokersAvailable:
#             if attempt == max_retries:
#                 raise
#             print(
#                 f"Kafka broker unavailable at {BOOTSTRAP_SERVERS}. "
#                 f"Retrying in {retry_delay_seconds}s ({attempt}/{max_retries})..."
#             )
#             time.sleep(retry_delay_seconds)


# # initilize kafka producer with bootstrap servers and value serializer to convert the event data to JSON format before sending it to the Kafka topic
# if producer is not None: producer = create_producer()

print(f"Loaded {len(df)} rows from {FILE_PATH}")

# Iterate through the DataFrame and send each row as an event to the Kafka topic.
# Each event includes stable metadata and derived fields for downstream streaming analytics.
for sent_count, (row_index, row) in enumerate(df.iterrows(), start=1):
    event = row.to_dict()
    event["event_time"] = event["tpep_pickup_datetime"]
    event["replay_time"] = pd.Timestamp.utcnow().isoformat()
    event["event_id"] = f"{row_index}-{event['tpep_pickup_datetime']}-{event['PULocationID']}-{event['DOLocationID']}"

    pickup_time = pd.to_datetime(event["tpep_pickup_datetime"], errors="coerce")
    dropoff_time = pd.to_datetime(event["tpep_dropoff_datetime"], errors="coerce")
    if pd.notna(pickup_time):
        event["pickup_date"] = pickup_time.date().isoformat()
        event["pickup_hour"] = int(pickup_time.hour)

    if "trip_duration_min" not in event and pd.notna(pickup_time) and pd.notna(dropoff_time):
        event["trip_duration_min"] = (dropoff_time - pickup_time).total_seconds() / 60

    trip_distance = event.get("trip_distance")
    total_amount = event.get("total_amount")
    if trip_distance and trip_distance > 0 and total_amount is not None:
        event["fare_per_mile"] = total_amount / trip_distance

    producer.send(TOPIC, key=event.get("PULocationID"), value=event)

    if sent_count % LOG_INTERVAL == 0:
        print(f"Sent {sent_count} events")

    time.sleep(REPLAY_DELAY_SECONDS)

producer.flush()
print("Replay complete.")
