# Kafka POC-8: Kafka Connect Reliability
## Elasticsearch Recovery, Pause/Resume, Idempotency, and Dead Letter Queue (DLQ)

**Document type:** Practical POC implementation and test guide  
**Environment:** Existing local Kafka + Kafka Connect + MySQL + Debezium + Elasticsearch setup  
**Status:** Test plan — record observed results during execution

---

# Chapter 1 — Overview

## 1.1 Objective

This POC demonstrates how a Kafka Connect pipeline behaves under four reliability scenarios:

1. **Pause and resume:** Temporarily pause the Elasticsearch sink connector, create new product events, resume it, and verify that pending events are processed.
2. **Elasticsearch outage and recovery:** Stop Elasticsearch while events are produced, restore Elasticsearch, and observe whether the sink recovers automatically or needs intervention.
3. **Idempotency:** Replay the same valid Kafka record with the same key and verify that Elasticsearch updates the same document rather than creating another document.
4. **Dead Letter Queue (DLQ):** Send a record that fails supported Kafka Connect record processing and verify that it is routed to the configured DLQ with error context.

These are separate test cases in one POC. Pause/resume is an administrative action, outage recovery is an infrastructure-failure scenario, idempotency concerns duplicate effects, and DLQ handles supported record-processing errors.

## 1.2 Existing architecture

```text
FastAPI product endpoint
        |
        v
MySQL: poc_db.products_poc_8
        |
        v
Debezium MySQL source connector
        |
        v
Kafka topic:
mysql-product.poc_db.products_poc_8
        |
        v
Kafka Connect Elasticsearch sink
        |
        v
Elasticsearch index:
mysql-product.poc_db.products_poc_8
```

The DLQ is a separate Kafka topic:

```text
elasticsearch-sink-dlq
```

## 1.3 Existing environment and names

| Component | Value |
|---|---|
| Kafka UI | `http://localhost:8080` |
| Kafka Connect REST API | `http://localhost:8083` |
| Elasticsearch | `http://localhost:9200` |
| Kafka Connect container | `poc-kafka-connect` |
| Elasticsearch container | `poc-elasticsearch` |
| Debezium source connector | `mysql-product-source` |
| Elasticsearch sink connector | `elasticsearch-product-sink` |
| MySQL database/table | `poc_db.products_poc_8` |
| Source Kafka topic | `mysql-product.poc_db.products_poc_8` |
| Elasticsearch index | `mysql-product.poc_db.products_poc_8` |
| DLQ topic | `elasticsearch-sink-dlq` |
| Debezium schema history topic | `schema-changes.poc_db` (confirm actual connector config) |

> Verify names against the running environment before executing commands. The schema history topic name depends on the source connector configuration.

---

# Chapter 2 — Topics and Their Responsibilities

## 2.1 Topics to reuse

| Topic | Responsibility | Action |
|---|---|---|
| `mysql-product.poc_db.products_poc_8` | Product change events emitted by Debezium | Reuse |
| `elasticsearch-sink-dlq` | Failed records routed by the sink's Connect error handler | Reuse or create if missing |
| `schema-changes.poc_db` | Debezium schema history, if configured with this name | Keep existing; do not use as a product event topic |

You do **not** need a new source topic for pause/resume, Elasticsearch recovery, or idempotency. Use unique test values and keys to distinguish test runs.

## 2.2 Check topics

Use Kafka UI at `http://localhost:8080` to inspect topics and messages. If the Kafka CLI is installed inside a suitable container, list topics using the command appropriate to that container, for example:

```bash
kafka-topics.sh --bootstrap-server kafka:9092 --list
```

The bootstrap address depends on where the command runs. Inside the Docker network, `kafka:9092` may be appropriate; from the host, use the advertised host listener configured in your setup.

---

# Chapter 3 — Preconditions and Baseline Checks

## 3.1 Before starting

- [ ] Kafka is running.
- [ ] Kafka Connect is running.
- [ ] MySQL is running and contains `poc_db.products_poc_8`.
- [ ] Elasticsearch is running.
- [ ] The source connector is running.
- [ ] The sink connector is running.
- [ ] The source topic exists.
- [ ] The DLQ topic exists, or the worker has permission/configuration to create it.
- [ ] This is a local POC, not a production system.

Do not delete topics, reset offsets, or drop the table as part of these tests.

## 3.2 Check connector status

```bash
curl -s http://localhost:8083/connectors/mysql-product-source/status
```

```bash
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
```

A connector's status and its task status are different. A connector can report `RUNNING` while a task is `FAILED`, so inspect the `tasks` array in the response.

## 3.3 Check Elasticsearch

```bash
curl -s http://localhost:9200
```

If Elasticsearch does not respond, resolve its startup/health issue before beginning baseline tests.

## 3.4 Inspect logs

```bash
docker logs --tail 100 poc-kafka-connect
```

Record the baseline connector/task status and any errors before each test.

---

# Chapter 4 — Sink Connector Configuration

## 4.1 Relevant existing configuration

The following is the relevant configuration described for this POC. It is a **partial configuration example**, not a complete replacement for the live connector configuration. Keep all other existing properties, including converters, transforms, connection settings, and connector-specific options.

```json
{
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "topics": "mysql-product.poc_db.products_poc_8",
  "connection.url": "http://poc-elasticsearch:9200",
  "key.ignore": "false",
  "schema.ignore": "true",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.drop.tombstones": "false",
  "behavior.on.null.values": "DELETE",
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.deadletterqueue.topic.name": "elasticsearch-sink-dlq",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

**Connection URL note:** Use the hostname reachable from the Kafka Connect container. Your previous working setup used `http://poc-elasticsearch:9200`; `http://localhost:9200` inside the Connect container would refer to that container itself, not Elasticsearch. Confirm the actual container/service name in your Compose file.

## 4.2 Why these properties matter

| Property | Purpose |
|---|---|
| `topics` | Kafka topic consumed by the Elasticsearch sink |
| `connection.url` | Elasticsearch URL reachable from Kafka Connect |
| `key.ignore=false` | Uses the Kafka key for the Elasticsearch document ID |
| `schema.ignore=true` | Disables schema-based mapping behavior in the connector |
| `transforms=unwrap` | Enables the Debezium unwrap transformation |
| `transforms.unwrap.type` | Configures `ExtractNewRecordState` |
| `errors.tolerance=all` | Allows supported record-processing errors to be tolerated |
| `errors.log.enable=true` | Enables error logging |
| `errors.log.include.messages=true` | Includes record information in error logs; use carefully outside a POC |
| `errors.deadletterqueue.topic.name` | Sets the DLQ topic |
| `errors.deadletterqueue.topic.replication.factor=1` | Single-replica setting for the local single-broker POC |
| `errors.deadletterqueue.context.headers.enable=true` | Adds error context headers to DLQ records |

## 4.3 Important DLQ limitation

`errors.tolerance=all` and DLQ settings do not mean every connector failure goes to the DLQ. Kafka Connect can route errors handled by its error-handling framework, such as certain transformation/conversion errors. Connection outages and many Elasticsearch request/task failures may instead be retried or fail the task. Test and document the observed error type.

## 4.4 Inspect the live configuration before editing

```bash
curl -s \
  http://localhost:8083/connectors/elasticsearch-product-sink/config
```

If updating configuration, preserve all required existing properties. Do not replace the complete configuration with the partial example above.

---

# Chapter 5 — Test Case 1: Pause and Resume the Sink Connector

## 5.1 Objective

Verify that while the sink connector is paused, new source events remain available in Kafka, and after resume the sink processes pending events when healthy.

## 5.2 Steps

### Step 1 — Confirm baseline status

```bash
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
```

### Step 2 — Pause the connector

```bash
curl -i -X PUT \
  http://localhost:8083/connectors/elasticsearch-product-sink/pause
```

Pause is asynchronous. Poll the status endpoint until the connector/task state reflects the pause; do not assume the HTTP response alone proves the pause has fully taken effect.

### Step 3 — Create test products

Use your existing FastAPI endpoint to create two distinct products, for example:

- `Pause Resume Test 1`
- `Pause Resume Test 2`

Use the actual request schema required by your FastAPI model. Set a unique category such as `PauseResumeTest` if that field is supported.

### Step 4 — Verify source-side events

- Confirm the product rows exist in MySQL.
- Confirm the Debezium source connector is healthy.
- Inspect `mysql-product.poc_db.products_poc_8` in Kafka UI for the events.

### Step 5 — Resume the connector

```bash
curl -i -X PUT \
  http://localhost:8083/connectors/elasticsearch-product-sink/resume
```

### Step 6 — Verify status and Elasticsearch

```bash
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
```

```bash
curl -s \
  "http://localhost:9200/mysql-product.poc_db.products_poc_8/_search?pretty"
```

If you used a unique category, query it:

```bash
curl -s -X POST \
  "http://localhost:9200/mysql-product.poc_db.products_poc_8/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "category": "PauseResumeTest"
      }
    }
  }'
```

### Pass criteria

- [ ] New products were saved in MySQL.
- [ ] Source events appeared in Kafka.
- [ ] The sink was paused during the test.
- [ ] After resume, the sink task became healthy.
- [ ] Both test products appeared in Elasticsearch.

### Record observations

- Pause time:
- Connector/task state while paused:
- Number of events created:
- Resume time:
- Products indexed:
- Pass/Fail:

---

# Chapter 6 — Test Case 2: Elasticsearch Outage and Recovery

## 6.1 Objective

Determine how the sink behaves when Elasticsearch becomes unavailable and what is required for recovery.

## 6.2 Steps

### Step 1 — Capture baseline

```bash
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
curl -s http://localhost:9200
```

### Step 2 — Stop Elasticsearch

```bash
docker stop poc-elasticsearch
```

### Step 3 — Create products through FastAPI

Create two products with unique names and a category such as `FailureTesting`, if supported:

- `ES Down Test 1`
- `ES Down Test 2`

Verify they are saved in MySQL.

### Step 4 — Inspect source topic, sink status, and logs

```bash
curl -s http://localhost:8083/connectors/mysql-product-source/status
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
docker logs --tail 100 poc-kafka-connect
```

Inspect the source topic in Kafka UI. Record whether the sink task continues running, retries, or becomes `FAILED`.

### Step 5 — Restore Elasticsearch

```bash
docker start poc-elasticsearch
```

Wait for Elasticsearch to respond:

```bash
curl -s http://localhost:9200
```

### Step 6 — Inspect sink status

```bash
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
```

If the task is `FAILED`, and the error is resolved, restart the failed task using its actual task ID from the status response. Example for task `0`:

```bash
curl -i -X POST \
  http://localhost:8083/connectors/elasticsearch-product-sink/tasks/0/restart
```

This is a manual restart. It does not enable automatic task restart.

### Step 7 — Verify documents

```bash
curl -s -X POST \
  "http://localhost:9200/mysql-product.poc_db.products_poc_8/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "category": "FailureTesting"
      }
    }
  }'
```

### Pass criteria

- [ ] Products persisted in MySQL while Elasticsearch was down.
- [ ] Source events were published to Kafka.
- [ ] Outage behavior was captured from status/logs.
- [ ] Elasticsearch recovered.
- [ ] The sink resumed automatically or was manually restarted as documented.
- [ ] Both products eventually appeared in Elasticsearch.

### Record observations

- Outage start/end:
- Error in logs:
- Sink task state during outage:
- Automatic recovery?:
- Manual restart required?:
- Products indexed:
- Pass/Fail:

---

# Chapter 7 — Test Case 3: Idempotency by Replaying the Same Kafka Record

## 7.1 Objective

Verify that processing the same valid record more than once does not create multiple Elasticsearch documents when the same stable Kafka key is used as the document ID.

## 7.2 Configuration

Confirm this sink setting:

```json
{
  "key.ignore": "false"
}
```

Keep all other existing connector properties. Check the actual key converter and key format too; they affect how the connector derives the Elasticsearch `_id`.

## 7.3 Steps

1. Choose a unique, stable Kafka key, for example `idempotency-test-001`.
2. Produce a valid record twice with the **same key and same value** to the source topic.
3. Ensure the record is valid for the configured Debezium unwrap SMT. A plain product JSON message may fail because the unwrap transform expects a Debezium envelope.
4. Inspect the topic to verify both records were produced.
5. Search Elasticsearch for the test document and inspect `_id` and `_source`.
6. Count matching documents using a unique test field, such as a unique product name or ID.

Example search (replace the product name with the actual test value):

```bash
curl -s -X POST \
  "http://localhost:9200/mysql-product.poc_db.products_poc_8/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "name": "Idempotency Test Product"
      }
    }
  }'
```

### Pass criteria

- [ ] Two record deliveries used the same key.
- [ ] Both writes targeted the same Elasticsearch `_id`.
- [ ] Only one matching document exists for that stable ID.
- [ ] Final document content is correct.

**Interpretation:** This demonstrates idempotent effects at the Elasticsearch document level for the chosen key. It does not mean Kafka or the connector processed the event only once. With `key.ignore=true`, generated IDs may cause duplicate documents.

### Record observations

- Kafka key:
- Records produced:
- Elasticsearch `_id`:
- Matching document count:
- Final content correct?:
- Pass/Fail:

---

# Chapter 8 — Test Case 4: Dead Letter Queue (DLQ)

## 8.1 Objective

Verify that a record which fails a supported Kafka Connect record-processing stage is routed to the DLQ and includes useful error context.

## 8.2 Verify configuration

Inspect the live sink config:

```bash
curl -s \
  http://localhost:8083/connectors/elasticsearch-product-sink/config
```

Verify the relevant settings:

```json
{
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.deadletterqueue.topic.name": "elasticsearch-sink-dlq",
  "errors.deadletterqueue.topic.replication.factor": "1",
  "errors.deadletterqueue.context.headers.enable": "true"
}
```

These are the relevant properties only; do not overwrite the full connector config with this fragment.

## 8.3 Steps

1. Confirm the topic `elasticsearch-sink-dlq` exists in Kafka UI.
2. Send a controlled test record that is invalid for the configured `ExtractNewRecordState` transformation.
3. In the earlier POC test, a plain product-shaped JSON message without a Debezium `after` envelope produced `java.lang.IllegalArgumentException: Unknown field: after`. This is an example of a transformation error that was routed to the DLQ in this environment.
4. Inspect sink status and logs:

   ```bash
   curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
   docker logs --tail 100 poc-kafka-connect
   ```

5. Open Kafka UI (`http://localhost:8080`), select `elasticsearch-sink-dlq`, and inspect the record and headers.
6. Record the error, original topic/partition/offset if available in headers, and the sink task status.

### Pass criteria

- [ ] Invalid record causes the expected transformation error.
- [ ] Record appears in `elasticsearch-sink-dlq`.
- [ ] Error context headers are present.
- [ ] Connector status and logs match the observed behavior.

**Important:** An Elasticsearch outage is not automatically a DLQ event. The DLQ only handles errors supported by Kafka Connect's error-handling framework. Connectivity or task-level errors may instead be retried or cause the task to fail.

### Record observations

- Test record key:
- Error message:
- Record found in DLQ?:
- Error headers present?:
- Sink task status:
- Pass/Fail:

---

# Chapter 9 — Final Verification and Troubleshooting

## 9.1 Commands

### Source connector status

```bash
curl -s http://localhost:8083/connectors/mysql-product-source/status
```

### Sink connector status

```bash
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/status
```

### Live sink configuration

```bash
curl -s http://localhost:8083/connectors/elasticsearch-product-sink/config
```

### Elasticsearch health

```bash
curl -s http://localhost:9200
```

### Search index

```bash
curl -s \
  "http://localhost:9200/mysql-product.poc_db.products_poc_8/_search?pretty"
```

### Kafka Connect logs

```bash
docker logs --tail 100 poc-kafka-connect
```

## 9.2 Troubleshooting decision order

If a product is missing from Elasticsearch, investigate in this order:

1. **MySQL:** Does the row exist in `poc_db.products_poc_8`?
2. **Source connector:** Is `mysql-product-source` healthy?
3. **Kafka:** Is the change event in `mysql-product.poc_db.products_poc_8`?
4. **Sink status:** Is `elasticsearch-product-sink` and its task healthy, paused, or failed?
5. **Logs:** What is the exact exception?
6. **Elasticsearch:** Is the service healthy and is the expected index present?
7. **DLQ:** If a record-processing error occurred, is the record and error context in `elasticsearch-sink-dlq`?

Common distinctions:

- **PAUSED:** Intentional administrative pause; use the resume endpoint when ready.
- **FAILED task:** Read the error, fix the cause, then restart the failed task if needed.
- **Elasticsearch unavailable:** Connectivity/retry/task failure scenario; not automatically a DLQ case.
- **DLQ record:** A record was routed for a supported record-processing error; inspect the headers and exception.
- **Duplicate document:** Check Kafka key, `key.ignore`, key converter, and actual Elasticsearch `_id`.

---

# Chapter 10 — Acceptance Checklist and Conclusion

## 10.1 Combined checklist

- [ ] Baseline connector statuses captured.
- [ ] Pause/resume test completed.
- [ ] Events created during pause were later indexed.
- [ ] Elasticsearch outage test completed.
- [ ] Recovery behavior and any manual task restart documented.
- [ ] Idempotency replay test completed using the same key and value.
- [ ] Elasticsearch document ID and matching document count verified.
- [ ] DLQ test completed using a controlled invalid record.
- [ ] DLQ record and error headers inspected.
- [ ] Logs and final connector status captured for all tests.

## 10.2 Conclusion template

> This POC evaluated pause/resume, Elasticsearch outage recovery, idempotent Elasticsearch writes, and Dead Letter Queue handling. During pause/resume, the observed result was **[result]**. During the Elasticsearch outage, the sink **[recovered automatically / required a manual restart / other]**. Replaying the same Kafka key resulted in **[document ID and document count]**. The invalid-record test resulted in **[DLQ result and error]**. Based on the acceptance checklist, the POC **[passed / partially passed / failed]**.

## 10.3 Production considerations

This is a local demonstration configuration. Before production use, review DLQ topic replication and retention, access control, monitoring and alerting, sensitive data in error logs, retry/backoff configuration, connector/task restart strategy, Elasticsearch availability, and operational recovery procedures.