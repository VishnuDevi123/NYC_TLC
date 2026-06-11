# Consumer Streaming Implementation Plan

## Purpose

This document is a handoff for the next development session. It captures the consumer-side streaming design decisions, target Kafka topics, algorithmic implementation plan, and vertical slices to implement.

The project goal is to evolve the current NYC TLC Kafka pipeline from a console-only consumer into a real streaming analytics processor. The consumer should read raw taxi trip events, validate them, enrich them with taxi zone metadata, publish clean records, publish bad records to a dead-letter topic, detect anomalies, and emit downstream metrics topics that can later feed storage, dashboards, alerts, and notebooks.

## Current Context

The producer publishes raw NYC taxi trip events into Kafka.

Current source topic:

```text
taxi_trips_events
```

The current consumer computes basic in-memory stats and prints them:

- total events consumed
- average fare
- average distance
- average duration
- top pickup zones
- rolling hot pickup zones
- top routes by trip count
- top routes by revenue
- top profitable routes
- anomaly counts

The repo also now has a taxi zone lookup CSV:

```text
taxi_zone_lookup.csv
```

Expected lookup columns:

```text
LocationID,Borough,Zone,service_zone
```

This lookup should be used to enrich `PULocationID` and `DOLocationID` into human-readable pickup/dropoff borough, zone, and service zone fields.

## Design Decisions Already Made

Invalid impossible records must be dropped from metrics.

Invalid records should be published only to:

```text
dead_letter_events
```

Unknown `PULocationID` or `DOLocationID` must be treated as invalid for this project and sent to `dead_letter_events`.

Anomalies are different from invalid records. An anomalous trip can still be a valid observed trip.

Anomalies should:

- stay in `clean_trip_events`
- be included in metrics
- also be copied to `anomaly_events`

First metric window style should be event-count based, not event-time based.

Initial rolling window:

```text
last N valid events
```

Default `N` can remain:

```text
50
```

Per-event enriched records should be implemented before aggregate metrics. `clean_trip_events` becomes the trusted source of truth for later storage, dashboards, notebooks, and ML workflows.

## Target Architecture

Raw stream enters the consumer:

```text
taxi_trips_events
```

The consumer validates, enriches, detects anomalies, computes metrics, and publishes to purpose-built output topics:

```text
taxi_trips_events
   |
   v
consumer.py
   |
   +--> clean_trip_events
   +--> dead_letter_events
   +--> anomaly_events
   +--> zone_metrics
   +--> route_metrics
```

Future downstream consumers can then be added:

```text
clean_trip_events      -> storage_writer.py
dead_letter_events     -> dlq_monitor.py
anomaly_events         -> anomaly_alerts.py
zone_metrics           -> dashboard_api.py
route_metrics          -> route_ranker.py
```

## Topic Contracts

### `clean_trip_events`

Purpose: trusted enriched trip stream.

Produced when a raw event is structurally valid and has known pickup/dropoff locations.

Fields should include:

```json
{
  "event_id": "string",
  "event_time": "ISO-8601 pickup timestamp",
  "pickup_date": "YYYY-MM-DD",
  "pickup_hour": 13,
  "dropoff_time": "ISO-8601 dropoff timestamp",
  "pu_location_id": 132,
  "do_location_id": 236,
  "pickup_borough": "Queens",
  "pickup_zone": "JFK Airport",
  "pickup_service_zone": "Airports",
  "dropoff_borough": "Manhattan",
  "dropoff_zone": "Upper East Side North",
  "dropoff_service_zone": "Yellow Zone",
  "passenger_count": 1,
  "trip_distance": 12.4,
  "trip_duration_min": 34.2,
  "fare_amount": 52.0,
  "tip_amount": 10.0,
  "total_amount": 73.5,
  "fare_per_mile": 5.93,
  "tip_percent": 13.61,
  "route_key": "132->236",
  "processed_at": "ISO-8601 timestamp"
}
```

Later uses:

- write to Parquet or object storage
- dashboard source
- notebook source
- feature source for ML
- replay source for downstream jobs

Recommended storage partitioning later:

```text
pickup_date / pickup_hour / pickup_borough
```

### `dead_letter_events`

Purpose: data quality audit stream for unusable records.

Produced when a raw event cannot be trusted or cannot support project analytics.

DLQ reasons should be structured, not free-text only.

Example:

```json
{
  "error_type": "validation_failed",
  "reasons": [
    {
      "type": "non_positive_trip_distance",
      "field": "trip_distance",
      "value": -1.2
    }
  ],
  "raw_event": {},
  "received_at": "ISO-8601 timestamp"
}
```

Invalid conditions:

- missing required fields
- invalid pickup/dropoff datetime
- dropoff time before or equal to pickup time
- pickup time in the future
- `passenger_count <= 0`
- `trip_distance <= 0`
- `fare_amount < 0`
- `total_amount < 0`
- invalid `PULocationID`
- invalid `DOLocationID`
- unknown `PULocationID` not in lookup table
- unknown `DOLocationID` not in lookup table

DLQ records are excluded from:

- `clean_trip_events`
- `anomaly_events`
- `zone_metrics`
- `route_metrics`

Later uses:

- DLQ reason counts
- DLQ rate alerting
- raw failed event samples
- retry workflow after schema or lookup fixes

### `anomaly_events`

Purpose: suspicious but valid records.

Produced when a clean trip has unusual values but is still structurally valid.

Anomalies must also remain in:

```text
clean_trip_events
zone_metrics
route_metrics
```

Example:

```json
{
  "event_id": "string",
  "event_time": "ISO-8601 pickup timestamp",
  "anomaly_types": [
    {
      "type": "high_fare_per_mile",
      "severity": "medium",
      "value": 140.25,
      "threshold": 100.0
    }
  ],
  "clean_event": {},
  "detected_at": "ISO-8601 timestamp"
}
```

Initial anomaly rules:

- high fare per mile, for example `fare_per_mile > 100`
- very long duration, for example `trip_duration_min > 180`
- long duration with low distance, for example `trip_duration_min > 60 and trip_distance < 1`
- unusually high tip percent, for example `tip_percent > 60`
- unusually high total amount, threshold can be configurable

Later uses:

- fraud/quality alerting
- anomaly trend reports
- top anomalous zones/routes
- severe anomaly notification

### `zone_metrics`

Purpose: rolling demand and fare metrics by pickup zone.

Initial window:

```text
last N valid events
```

Recommended default:

```text
N = 50
```

Example:

```json
{
  "window_type": "last_50_valid_events",
  "pu_location_id": 132,
  "pickup_borough": "Queens",
  "pickup_zone": "JFK Airport",
  "pickup_service_zone": "Airports",
  "trip_count": 17,
  "demand_rank": 1,
  "avg_fare": 45.22,
  "avg_distance": 10.4,
  "avg_duration_min": 31.7,
  "computed_at": "ISO-8601 timestamp"
}
```

Initial metrics:

- pickup count by zone
- top pickup zones
- average fare by pickup zone
- average distance by pickup zone
- average duration by pickup zone

Later uses:

- live hot-zone dashboard
- borough-level demand rollup
- geospatial heatmap
- surge-like demand scoring

### `route_metrics`

Purpose: route-level trip volume and profitability.

Route key:

```text
PULocationID->DOLocationID
```

Example:

```json
{
  "route_key": "132->236",
  "pu_location_id": 132,
  "do_location_id": 236,
  "pickup_zone": "JFK Airport",
  "dropoff_zone": "Upper East Side North",
  "trip_count": 42,
  "total_revenue": 3087.0,
  "avg_revenue": 73.5,
  "revenue_per_mile": 5.93,
  "revenue_per_minute": 2.15,
  "avg_tip": 10.0,
  "profitability_score": 7.23,
  "computed_at": "ISO-8601 timestamp"
}
```

Initial metrics:

- trip count
- total revenue
- average revenue
- revenue per mile
- revenue per minute
- average tip
- profitability score

Score can start simple:

```text
(revenue_per_mile * 0.4) + (revenue_per_minute * 0.4) + (avg_revenue * 0.2)
```

Later uses:

- top profitable routes
- top volume routes
- route efficiency ranking
- low-profit route detection
- route recommendation concept

## Algorithmic Implementations

### 1. Required Field Validation

Check the raw event has required fields:

- `event_id`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`
- `PULocationID`
- `DOLocationID`
- `passenger_count`
- `trip_distance`
- `fare_amount`
- `total_amount`

Missing fields go to `dead_letter_events`.

### 2. Impossible Condition Filtering

Reject records that cannot support valid analytics:

- negative or zero trip distance
- zero or negative passenger count
- fare under zero
- total amount under zero
- invalid datetime
- pickup time in future
- dropoff before pickup

Rejected records go to `dead_letter_events` only.

### 3. Zone Lookup Enrichment

Load `taxi_zone_lookup.csv` once at consumer startup.

Use it to map:

- `PULocationID`
- `DOLocationID`

Add:

- pickup borough
- pickup zone
- pickup service zone
- dropoff borough
- dropoff zone
- dropoff service zone

Unknown location IDs go to `dead_letter_events`.

### 4. Derived Trip Features

For every valid event, compute:

- `trip_duration_min`
- `fare_per_mile`
- `tip_percent`
- `pickup_date`
- `pickup_hour`
- `route_key`

Publish enriched result to `clean_trip_events`.

### 5. Anomaly Detection

Run anomaly checks after validation and enrichment.

If anomaly is detected:

- publish event to `clean_trip_events`
- publish anomaly wrapper to `anomaly_events`
- include event in metrics

Do not send structurally valid anomalies to DLQ.

### 6. Rolling Zone Demand Metrics

Maintain event-count window over last `N` valid clean events.

Use a `deque(maxlen=N)`.

Compute top zones and zone-level averages from the current window.

Publish results to `zone_metrics`.

### 7. Route Profitability Metrics

Maintain aggregate dictionaries keyed by route:

- count
- revenue
- distance
- duration
- tips

Compute route profitability at `LOG_INTERVAL` or metric interval.

Publish top routes to `route_metrics`.

### 8. Processing Summary

Print summary to console first:

- total raw events seen
- clean events produced
- DLQ events produced
- anomaly events produced
- top DLQ reasons
- top anomaly reasons

Optional later topic:

```text
consumer_health_metrics
```

## Vertical Slices

Implementation should be vertical, not horizontal. Each slice should deliver a working behavior from raw input to Kafka output.

Do not first build all helpers, then all topics, then all metrics. Instead, ship one complete user-visible pipeline behavior at a time.

### Slice 1: DLQ for Impossible Records

Goal: raw invalid event enters consumer and gets published to `dead_letter_events`.

Scope:

- add `KafkaProducer` to consumer
- configure output topic names via env vars
- implement required field checks
- implement impossible condition checks
- publish structured DLQ event
- exclude invalid event from current counters and metrics

Done when:

- invalid raw event produces one `dead_letter_events` record
- invalid raw event does not change zone or route metrics
- console shows DLQ count and reason count

### Slice 2: Zone-Enriched Clean Events

Goal: valid raw event becomes enriched clean event.

Scope:

- load `taxi_zone_lookup.csv`
- mount lookup file in Docker Compose if needed
- map pickup and dropoff location IDs
- reject unknown IDs to DLQ
- compute derived trip fields
- publish enriched event to `clean_trip_events`

Done when:

- valid raw event produces one `clean_trip_events` record
- clean event has pickup/dropoff borough, zone, service zone
- unknown location ID produces `dead_letter_events`

### Slice 3: Anomaly Stream

Goal: suspicious but valid clean event is copied to anomaly topic while still included in metrics.

Scope:

- implement anomaly rules
- add severity and threshold metadata
- publish anomaly wrapper to `anomaly_events`
- keep event in `clean_trip_events`
- keep event in rolling metrics

Done when:

- high fare-per-mile event produces `clean_trip_events`
- same event also produces `anomaly_events`
- same event affects `zone_metrics` and `route_metrics`
- event does not go to `dead_letter_events`

### Slice 4: Rolling Zone Metrics

Goal: consumer emits rolling demand metrics by pickup zone.

Scope:

- maintain last `N` valid clean events
- compute top pickup zones
- compute average fare, distance, duration by zone
- publish `zone_metrics`

Done when:

- every metric interval produces `zone_metrics`
- only valid events are included
- anomalies are included
- DLQ records are excluded

### Slice 5: Route Profitability Metrics

Goal: consumer emits ranked route metrics.

Scope:

- maintain route aggregates
- compute revenue per mile
- compute revenue per minute
- compute average revenue
- compute average tip
- compute profitability score
- publish top routes to `route_metrics`

Done when:

- every metric interval produces `route_metrics`
- only valid events are included
- anomalies are included
- DLQ records are excluded

### Slice 6: Consumer Health Summary

Goal: make local debugging and demos easy.

Scope:

- print total raw events seen
- print clean events produced
- print DLQ events produced
- print anomaly events produced
- print top DLQ reasons
- print top anomaly reasons
- print output topic names at startup

Done when:

- running Docker Compose gives readable progress logs
- logs explain where events were routed

## Recommended Environment Variables

```text
SOURCE_TOPIC=taxi_trips_events
CLEAN_TOPIC=clean_trip_events
DEAD_LETTER_TOPIC=dead_letter_events
ANOMALY_TOPIC=anomaly_events
ZONE_METRICS_TOPIC=zone_metrics
ROUTE_METRICS_TOPIC=route_metrics
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
CONSUMER_GROUP_ID=taxi-trip-analytics-consumer
ZONE_LOOKUP_PATH=/app/taxi_zone_lookup.csv
CONSUMER_LOG_INTERVAL=50
ROLLING_WINDOW_SIZE=50
MAX_FARE_PER_MILE=100
MAX_TRIP_DURATION_MIN=180
HIGH_TIP_PERCENT_THRESHOLD=60
HIGH_TOTAL_AMOUNT_THRESHOLD=500
```

## Docker Compose Notes

Consumer service should mount the lookup table:

```yaml
volumes:
  - ./consumer:/app
  - ./taxi_zone_lookup.csv:/app/taxi_zone_lookup.csv:ro
environment:
  KAFKA_BOOTSTRAP_SERVERS: kafka:29092
  ZONE_LOOKUP_PATH: /app/taxi_zone_lookup.csv
  PYTHONUNBUFFERED: "1"
```

Kafka auto topic creation is currently enabled, so output topics can be created on first publish. Later, explicit topic creation is better for production-style setup.

## Important Implementation Cautions

Keep DLQ and anomaly logic separate.

DLQ means unusable. Anomaly means usable but suspicious.

Do not let invalid events update:

- rolling zone window
- route aggregates
- average fare/distance/duration

Do let anomaly events update metrics.

Use stable keys when publishing:

- `clean_trip_events`: key by `pu_location_id`
- `dead_letter_events`: key by `event_id` if present, else fallback to raw Kafka offset or processing count
- `anomaly_events`: key by `event_id`
- `zone_metrics`: key by `pu_location_id`
- `route_metrics`: key by `route_key`

Do not hardcode only `TOPIC`; use named source/output topic constants.

Make local parsing robust:

- numeric parsing should distinguish missing/invalid from actual `0`
- datetime parsing should handle ISO strings
- avoid silently converting missing values to `0` during validation

## Future Implementations

After the core consumer topic routing is working:

1. Event-time windows with watermark-like late event handling.
2. Surge detection comparing current zone window to previous baseline.
3. Storage writer consuming `clean_trip_events` into Parquet.
4. DLQ monitor consuming `dead_letter_events`.
5. Anomaly alert consumer for severe anomalies.
6. Dashboard API consuming `zone_metrics` and `route_metrics`.
7. Explicit Kafka topic creation and topic configs.
8. Schema management using Avro or Protobuf.

## Resume Framing

Use this framing:

```text
Built a multi-topic real-time NYC taxi streaming analytics pipeline using Kafka and Python. Replayed historical TLC trip data into Kafka, validated and enriched events with taxi zone metadata, routed invalid records to a dead-letter stream, detected anomalous but valid trips, and emitted clean trip, zone demand, and route profitability topics for downstream storage, dashboards, and alerting.
```

