# Consumer Streaming Implementation Plan

## Goal

Build the consumer-side Kafka pipeline for NYC TLC trip events.

Input topic:

```text
taxi_trips_events
```

Output topics:

```text
clean_trip_events
dead_letter_events
anomaly_events
zone_metrics
route_metrics
```

## Processing Order

Process each raw event in this order:

```text
raw event
   |
   v
validate required fields and impossible values
   |
   +--> invalid -> dead_letter_events only, stop
   |
   v
zone lookup enrichment
   |
   +--> unknown zone -> dead_letter_events only, stop
   |
   v
derive clean trip fields
   |
   +--> clean_trip_events
   |
   v
detect anomaly
   |
   +--> anomaly_events when suspicious but valid
   |
   v
update metric state from this clean valid event
   |
   +--> zone_metrics / route_metrics at metric interval
```

Do not calculate zone or route metrics before validation, DLQ routing, and clean-event construction.

Invalid records must not update:

- rolling zone window
- route aggregates
- fare, distance, or duration averages

Anomalies are valid records. They must:

- stay in `clean_trip_events`
- be copied to `anomaly_events`
- be included in metric state

Metric processors should use `clean_trip_events` as input. Do not read both `clean_trip_events`
and `anomaly_events` for metrics unless deduplicating by `event_id`, because anomaly records
already exist in `clean_trip_events`.

## Topic Contracts

### `clean_trip_events`

Produced when a raw event is structurally valid and has known pickup/dropoff locations.

Required output fields:

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

Kafka key:

```text
pu_location_id
```

### `dead_letter_events`

Produced when a raw event cannot be trusted or cannot support analytics.

Required output shape:

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

Kafka key:

```text
event_id if present, else fallback processing id
```

### `anomaly_events`

Produced when a clean trip is suspicious but still valid.

Required output shape:

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

- `fare_per_mile > MAX_FARE_PER_MILE`
- `trip_duration_min > MAX_TRIP_DURATION_MIN`
- `trip_duration_min > 60 and trip_distance < 1`
- `tip_percent > HIGH_TIP_PERCENT_THRESHOLD`
- `total_amount > HIGH_TOTAL_AMOUNT_THRESHOLD`

Kafka key:

```text
event_id
```

### `zone_metrics`

Source: valid clean trips from `clean_trip_events`, or the equivalent clean-event object if
metrics are computed inside the same process.

Window:

```text
last N valid clean events
```

Required output shape:

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

Kafka key:

```text
pu_location_id
```

### `route_metrics`

Source: valid clean trips from `clean_trip_events`, or the equivalent clean-event object if
metrics are computed inside the same process.

Route key:

```text
PULocationID->DOLocationID
```

Required output shape:

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

Profitability score:

```text
(revenue_per_mile * 0.4) + (revenue_per_minute * 0.4) + (avg_revenue * 0.2)
```

Kafka key:

```text
route_key
```

## Implementation Steps

### 1. Required Field Validation

Required raw fields:

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

Reject records with:

- `trip_distance <= 0`
- `passenger_count <= 0`
- `fare_amount < 0`
- `total_amount < 0`
- invalid pickup/dropoff datetime
- pickup time in future
- dropoff time before or equal to pickup time
- invalid pickup/dropoff location ID type

Rejected records go to `dead_letter_events` only.

### 3. Zone Lookup Enrichment

Load `taxi_zone_lookup.csv` once at startup.

Expected columns:

```text
LocationID,Borough,Zone,service_zone
```

Use it to map:

- `PULocationID`
- `DOLocationID`

Unknown IDs go to `dead_letter_events`.

### 4. Derived Trip Features

For every valid enriched event, compute:

- `trip_duration_min`
- `fare_per_mile`
- `tip_percent`
- `pickup_date`
- `pickup_hour`
- `route_key`

Publish result to `clean_trip_events`.

### 5. Anomaly Detection

Run anomaly checks after validation, enrichment, and derived fields.

If anomaly is detected:

- publish clean event to `clean_trip_events`
- publish anomaly wrapper to `anomaly_events`
- update metric state from the clean event

Do not send structurally valid anomalies to DLQ.

### 6. Rolling Zone Metrics

Maintain a `deque(maxlen=ROLLING_WINDOW_SIZE)` of clean events.

Compute:

- pickup count by zone
- demand rank
- average fare by pickup zone
- average distance by pickup zone
- average duration by pickup zone

Publish to `zone_metrics` at metric interval.

### 7. Route Profitability Metrics

Maintain route aggregates keyed by `route_key`:

- trip count
- total revenue
- total distance
- total duration
- total tips

Compute:

- average revenue
- revenue per mile
- revenue per minute
- average tip
- profitability score

Publish to `route_metrics` at metric interval.

### 8. Consumer Health Summary

Print:

- source topic
- output topics
- total raw events seen
- clean events produced
- DLQ events produced
- anomaly events produced
- top DLQ reasons
- top anomaly reasons

## Vertical Slices

### Slice 1: DLQ for Impossible Records

Scope:

- add `KafkaProducer`
- configure output topic names via env vars
- implement required field checks
- implement impossible condition checks
- publish structured DLQ event
- exclude invalid event from counters and metrics

Done when:

- invalid raw event produces one `dead_letter_events` record
- invalid raw event does not change zone or route metrics
- console shows DLQ count and reason count

### Slice 2: Zone-Enriched Clean Events

Scope:
- keep topic defaults initialized in `consumer.py`
- use local `taxi_zone_lookup.csv` for enrichment
- mount lookup file in Docker Compose if needed
- map pickup and dropoff location IDs
- reject unknown IDs to DLQ
- compute derived trip fields
- publish enriched event to `clean_trip_events`
- leave anomaly routing and metric publishing for later slices

Done when:

- valid raw event produces one `clean_trip_events` record
- clean event has pickup/dropoff borough, zone, service zone
- unknown location ID produces `dead_letter_events`

### Slice 3: Anomaly Stream

Scope:

- implement anomaly rules
- add severity and threshold metadata
- publish anomaly wrapper to `anomaly_events`
- keep event in `clean_trip_events`
- keep event eligible for metric state through `clean_trip_events`

Done when:

- high fare-per-mile event produces `clean_trip_events`
- same event also produces `anomaly_events`
- event remains eligible for metric state through `clean_trip_events`
- event does not go to `dead_letter_events`

### Slice 4: Rolling Zone Metrics

Scope:

- maintain last `N` valid clean events
- use `clean_trip_events` as metric input if implemented downstream
- compute zone counts and averages
- publish `zone_metrics`

Done when:

- every metric interval produces `zone_metrics`
- only valid events are included
- anomalies are included
- DLQ records are excluded

### Slice 5: Route Profitability Metrics

Scope:

- maintain route aggregates
- use `clean_trip_events` as metric input if implemented downstream
- compute route profitability fields
- publish top routes to `route_metrics`

Done when:

- every metric interval produces `route_metrics`
- only valid events are included
- anomalies are included
- DLQ records are excluded

### Slice 6: Consumer Health Summary

Scope:

- print raw, clean, DLQ, and anomaly counts
- print top DLQ reasons
- print top anomaly reasons
- print output topic names at startup

Done when:

- Docker Compose logs show routing and topic progress clearly

## Environment Variables

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

Kafka auto topic creation is currently enabled, so output topics can be created on first publish.
