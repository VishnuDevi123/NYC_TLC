# NYC Taxi Trip Analytics Pipeline

## Overview

This project builds a real-time data engineering pipeline using **Apache Kafka** and **NYC TLC taxi trip data**.

The main idea is to take historical taxi trip records, replay them as live streaming events, process them in real time, and generate useful analytics such as trip volume, fare trends, demand hotspots, and anomaly alerts.

This project is designed to simulate a real production-style streaming system using a large public dataset.

---

## Project Goal

Build a streaming analytics platform that can:

- read large historical taxi trip datasets
- replay those records into Kafka as live events
- process the events in real time
- compute rolling metrics and demand insights
- store raw and processed outputs for analysis

---

## Core Workflow

### 1. Historical data source
We use **NYC TLC trip record data** as the base dataset.

This data contains trip-level records such as:

- pickup and dropoff timestamps
- pickup and dropoff location IDs
- passenger count
- trip distance
- fare amount
- tip amount
- total amount

We also use the **Taxi Zone Lookup** file to map location IDs to boroughs and zone names.

### 2. Replay as event stream
A producer reads the historical dataset and sends each trip record into Kafka as if it were a live event.

This gives us a realistic streaming environment without needing a real taxi feed.

### 3. Stream processing
Streaming jobs consume data from Kafka and calculate:

- trips per borough
- average fare by location
- surge-like demand zones
- suspicious or anomalous trips

### 4. Storage
The pipeline stores:

- raw trip events for traceability
- cleaned and processed records
- aggregated metrics for dashboards and analysis

---

## Main Components

## Kafka Topics

### `trip_events`
Main event stream containing taxi trip records.

### `fare_events`
Derived stream focused on fare-related analysis.

### `driver_events`
Optional derived stream for driver-style analysis if we simulate driver identifiers.

### `dead_letter_events`
Invalid or malformed records that fail validation.

---

## Processing Goals

The initial implementation focuses on these analytics:

### Real-time trip metrics
- trip counts by borough
- trip counts by zone
- average fare
- average distance
- average duration

### Demand analysis
- identify zones with unusually high recent trip volume
- compare current activity to rolling baselines

### Anomaly detection
- zero or negative trip distance
- negative or unrealistic fares
- very high fare for short trips
- unusually long duration with low distance

---

## Implementation Approach

## Step 1: Data ingestion
- download monthly NYC TLC Parquet files
- inspect schema and relevant fields
- load taxi zone lookup data
- select one taxi dataset first, likely Yellow Taxi

## Step 2: Kafka producer
- build a Python replay producer
- read trip records in order
- publish events into `trip_events`
- attach metadata such as event time and ingest time

## Step 3: Streaming jobs
- consume events from Kafka
- clean and validate records
- compute rolling metrics using event time
- write processed outputs to storage

## Step 4: Data sinks
- store raw records in Parquet for historical traceability
- store processed outputs in analytics-friendly tables
- support both batch review and streaming analytics

---

## Key Engineering Decisions

## Partitioning
Kafka messages will be partitioned using fields that make analytics scalable and practical.

Initial plan:
- Kafka partition key: `PULocationID`
- storage partitioning: `pickup_date`

This supports location-based aggregation and keeps the data organized for downstream processing.

## Schema management
We plan to use **Avro** or **Protobuf** for event schemas.

This helps with:
- consistent message structure
- schema evolution
- safer producer/consumer communication

## Idempotent and reliable processing
We want the pipeline to avoid duplicate effects when possible.

Approach:
- deterministic trip identifiers
- idempotent producer settings
- deduplication or upsert logic in downstream storage

## Late-arriving events
Streaming jobs will use **event time**, not just processing time.

This allows delayed records to still be handled correctly within a chosen time threshold.
