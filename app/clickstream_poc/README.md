# Kafka Clickstream POC

## Overview

This proof of concept demonstrates a basic real-time clickstream
pipeline. A user interacts with a React storefront, the frontend sends
click events to a FastAPI backend, FastAPI publishes Avro-serialized
events to Kafka, and a Kafka consumer updates in-memory metrics exposed
to a React dashboard.

The POC demonstrates: - Capturing product click events from a website. -
Publishing events to Kafka using Avro serialization. - Managing event
schemas with Confluent Schema Registry. - Consuming Kafka events in a
background thread started by FastAPI. - Showing event totals, event-type
counts, popular products, and recent events. - Streaming dashboard
updates through Server-Sent Events (SSE).

> **Important:** The metrics store is in memory. The consumer and API
> routes must run in the same Python process for the dashboard to
> reflect consumer updates. Restarting the API clears the metrics.

## Architecture

``` text
React storefront
      |
      v
sendClickstreamEvent()
      |
      v
FastAPI event endpoint
      |
      v
Avro serializer + Schema Registry
      |
      v
Kafka topic: clickstream_event_topic
      |
      v
Kafka consumer background thread
      |
      v
metrics_store.py
      |
      v
FastAPI dashboard APIs
      |
      v
React dashboard (SSE)
```

## Technology stack

-   Frontend: React.js
-   Backend: FastAPI
-   Event streaming: Apache Kafka
-   Serialization and schema governance: Avro and Confluent Schema
    Registry
-   Kafka client: `confluent-kafka`
-   Dashboard updates: Server-Sent Events (SSE)
-   Metrics storage: Python in-memory data structures

## Event structure

A clickstream event contains fields similar to the following. The exact
fields are determined by the registered Avro schema.

``` json
{
  "event_id": "evt-example-id",
  "event_type": "product_clicked",
  "user_id": "user-example-id",
  "session_id": "session-example-id",
  "page_url": "/products/12",
  "product_id": "12",
  "product_name": "Wireless Headphones",
  "timestamp": "2026-10-22T11:37:44.712Z"
}
```

  Field            Purpose
  ---------------- --------------------------------------------------
  `event_id`       Unique identifier for the event
  `event_type`     Action being tracked, such as `product_clicked`
  `user_id`        User/browser identity used by the frontend
  `session_id`     Browsing session identifier
  `page_url`       Page where the event occurred
  `product_id`     Product associated with the event, if applicable
  `product_name`   Product display name, if applicable
  `timestamp`      Time the event was generated

The frontend payload, registered Avro schema, and consumer must agree on
field names. If `product_name` is included in the frontend payload,
verify it remains present in the serialized event and consumer output.

## Event flow

1.  A user clicks a product in the React storefront.
2.  The frontend calls `sendClickstreamEvent()` with the event type and
    product details.
3.  The frontend sends the payload to FastAPI.
4.  FastAPI serializes the event with Avro and publishes it to Kafka.
5.  The consumer deserializes the Kafka event.
6.  The consumer calls `record_event(event)` in `metrics_store.py`.
7.  The dashboard summary endpoint returns current metrics.
8.  The dashboard stream endpoint sends updates through SSE.

## API endpoints

The router is mounted with prefix `/clickstream_event/09`.

  ---------------------------------------------------------------------------------------------------
  Method                  Endpoint                                            Purpose
  ----------------------- --------------------------------------------------- -----------------------
  `POST`                  `/clickstream_event/09/events`                      Accept an event and
                                                                              publish it to Kafka

  `GET`                   `/clickstream_event/09/dashboard/summary`           Return current
                                                                              dashboard metrics

  `GET`                   `/clickstream_event/09/dashboard/stream`            Stream dashboard
                                                                              updates through SSE

  `POST`                  `/clickstream_event/09/schemas/register`            Register the
                                                                              clickstream Avro schema

  `POST`                  `/clickstream_event/09/topics/clickstream-events`   Create the configured
                                                                              topic, if supported by
                                                                              the implementation
  ---------------------------------------------------------------------------------------------------

Inspect the routes using FastAPI Swagger UI at
`http://localhost:8000/docs`.

## Kafka producer

The producer publishes to:

``` text
clickstream_event_topic
```

The configuration includes `enable.idempotence=True`, `acks="all"`,
`auto.register.schemas=False`, and a subject-name strategy that uses
`clickstream-events-value`.

Because `auto.register.schemas=False`, the schema must already be
registered under the subject expected by the serializer. Verify the
subject name is exactly:

``` text
clickstream-events-value
```

A schema registered under a different subject does not automatically
satisfy a lookup under this subject.

### Passing the product name

The frontend caller should pass the product name, for example:

``` javascript
sendClickstreamEvent("product_clicked", {
  productId: product.id,
  title: product.name,
  pageUrl: `/products/${product.id}`
});
```

The event helper maps `title` to `product_name`:

``` javascript
const payload = {
  event_id: createId("evt"),
  event_type: eventType,
  user_id: getUserId(),
  session_id: getSessionId(),
  page_url: pageUrl || window.location.pathname,
  product_id: productId == null ? null : String(productId),
  product_name: title == null ? null : title,
  timestamp: new Date().toISOString()
};
```

If `product_name` is `null` in the consumer event, inspect the caller
arguments and request payload, then verify the schema used by the
producer.

## Schema Registry and Avro schema evolution

Schema Registry stores schema versions and can enforce compatibility
rules. The POC uses this subject:

``` text
clickstream-events-value
```

Useful checks:

``` bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/clickstream-events-value/versions
curl http://localhost:8081/subjects/clickstream-events-value/versions/latest
```

These commands assume Schema Registry is available at
`http://localhost:8081`.

An optional `product_name` field can be defined as:

``` json
{
  "name": "product_name",
  "type": ["null", "string"],
  "default": null
}
```

When changing a schema:

1.  Update the schema file used by the producer and consumer as
    appropriate.
2.  Register the updated schema under the correct subject.
3.  Check compatibility settings and confirm the new version was
    created.
4.  Restart or reload application processes if they load the schema only
    at startup.
5.  Produce a new event and verify the consumer receives the new field.

`BACKWARD` compatibility checks whether a new schema can read data
written with an earlier schema under the applicable rules. Compatibility
does not automatically populate a field that the producer did not send.

## Kafka consumer

The consumer reads from `clickstream_event_topic` with a consumer group
such as `clickstream-consumer-group`. Its configuration includes:

``` python
{
    "group.id": "clickstream-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}
```

With automatic commits disabled, the application controls when offsets
are committed. The consumer should commit only after successful
processing according to the POC's intended semantics.

The consumer deserializes each Avro event, passes it to
`record_event(event)`, and then commits the offset. During development,
log fields such as `event_type`, `product_id`, and `product_name`.

## Starting the consumer in a FastAPI background thread

FastAPI starts the consumer during application startup through the
lifespan handler. The consumer runs in a daemon thread in the **same
process** as the API routes.

Example pattern for `app/main.py`:

``` python
from contextlib import asynccontextmanager
import threading
from fastapi import FastAPI

from app.clickstream_poc.consumer import main as start_consumer


@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(
        target=start_consumer,
        daemon=True,
        name="clickstream-consumer",
    )
    thread.start()
    print("Clickstream consumer background thread started.")

    yield

    print("FastAPI shutting down — consumer thread will stop.")


app = FastAPI(lifespan=lifespan)
```

**Integrate this pattern into the existing `main.py`; do not replace the
whole application file with this abbreviated example.** Preserve the
existing routers, middleware, exception handlers, and other
configuration.

### Why the same process matters

The consumer calls `record_event()` and dashboard endpoints call
`get_dashboard_snapshot()`. Both use the in-memory structures in
`metrics_store.py`. A separate consumer process has a separate copy of
those structures, so the API dashboard will not see the consumer's
updates.

When using this background-thread setup, do not also start a second
consumer manually unless that is intentional and its process and storage
behavior are understood.

### Limitations

-   A daemon thread does not guarantee graceful consumer shutdown.
-   If the consumer thread crashes, this pattern does not automatically
    restart it.
-   Multiple Uvicorn workers have separate process memory and can start
    separate consumers.
-   API restarts and development reloads clear in-memory metrics.
-   For production, consider a dedicated consumer service,
    persistent/shared storage, explicit shutdown handling, health
    monitoring, and a controlled restart strategy.

## In-memory metrics store

The metrics store maintains total events, event-type counts, product
click counts, product names, and a bounded list of recent events. Use a
lock when reading or updating shared in-process structures from multiple
threads.

### Product-name mapping

The product-name dictionary should be keyed by product ID, not by
product name:

``` python
if product_id:
    _product_counts[str(product_id)] += 1
    if product_name:
        _product_names[str(product_id)] = product_name
```

The dashboard can then look up the name by product ID and use a fallback
only when no name is available.

## Dashboard

The dashboard summary endpoint is:

``` text
GET /clickstream_event/09/dashboard/summary
```

Example response:

``` json
{
  "total_events": 1,
  "event_counts": {
    "product_clicked": 1
  },
  "popular_products": [
    {
      "product_id": "12",
      "product_name": "Wireless Headphones",
      "count": 1
    }
  ],
  "recent_events": []
}
```

The actual response depends on events processed since the API process
started. The popular-products UI should render `product.product_name`
from the API response.

## Running and testing the POC

Use the service names and commands from your Docker Compose
configuration. A typical validation sequence is:

1.  Start Kafka and Schema Registry.
2.  Confirm both services are reachable.
3.  Ensure `clickstream_event_topic` exists, or create it using the POC
    endpoint or Kafka tooling.
4.  Confirm `clickstream-events-value` has the expected schema version.
5.  Start FastAPI with the lifespan configuration enabled.
6.  Check the API logs for the consumer startup message.
7.  Start the React storefront and dashboard.
8.  Click a product.
9.  Confirm the event endpoint succeeds.
10. Confirm the event appears in Kafka and the consumer logs.
11. Call the dashboard summary endpoint and verify totals, product IDs,
    and product names.
12. Confirm the dashboard updates through SSE.

### Typical local URLs

  Service              URL
  -------------------- ------------------------------
  FastAPI Swagger UI   `http://localhost:8000/docs`
  Schema Registry      `http://localhost:8081`
  Kafka UI             `http://localhost:8080`

These URLs assume the services are exposed on the corresponding local
ports.

## Troubleshooting

### `40403 Schema not found`

-   Verify the subject name expected by the serializer.
-   Check that the schema is registered under
    `clickstream-events-value`.
-   Confirm the application uses the correct Schema Registry URL.
-   Confirm the producer schema matches a registered version.
-   If `auto.register.schemas=False`, register the schema before
    producing.

### `product_name` is `null` in consumer events

-   Confirm the React caller passes `title: product.name`.
-   Inspect the request payload in browser developer tools.
-   Inspect the payload received by FastAPI before serialization.
-   Confirm the Avro schema contains `product_name`.
-   Verify the producer is using the expected schema subject and
    version.
-   Log the deserialized event before calling `record_event()`.

### Dashboard shows `Product 11` instead of the product name

-   Inspect `recent_events` in the dashboard summary.
-   If `recent_events.product_name` is `null`, debug the event path
    before the metrics store.
-   If the recent event has a name but popular products do not, check
    that `_product_names` is keyed by `str(product_id)`.
-   Ensure the consumer and dashboard routes run in the same process.

### Dashboard always shows zero

-   Check whether the consumer background thread started.
-   Check consumer logs for connection or deserialization errors.
-   Verify the topic and consumer group configuration.
-   Ensure `record_event()` is called for consumed events.
-   Ensure the consumer and dashboard use the same imported
    `metrics_store` module and process.
-   Avoid multiple Uvicorn workers when relying on in-memory metrics.

### Kafka has events but the dashboard does not update

-   Confirm the consumer is subscribed to `clickstream_event_topic`.
-   Check whether the consumer processed the records.
-   Verify offsets are committed after successful processing.
-   Check the SSE endpoint and browser network console.
-   Remember that an in-memory store resets when the API process
    restarts.


This POC demonstrates a clickstream pipeline from React to FastAPI to
Kafka, with Avro schemas managed by Schema Registry and a consumer
updating dashboard metrics. The FastAPI lifespan handler starts the
consumer in a background thread so the consumer and dashboard share the
same in-memory store. This is convenient for a local demonstration, but
production deployments should use shared persistent storage and explicit
consumer lifecycle management.