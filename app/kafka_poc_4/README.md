# Kafka POC-04 – Poison Pill Handling & Dead Letter Queue (DLQ) Pattern

**Apache Kafka 4.0.0 | Python | confluent-kafka | Pydantic | FastAPI**

---

## 1. POC Overview

This POC demonstrates how a production-grade Apache Kafka consumer handles **Poison Pill messages** (corrupted, unparseable, or business-invalid events) without crashing, getting stuck in an infinite retry loop, or causing **Head-of-Line Blocking** for valid messages.

When a consumer encounters a poison pill, it:
1. Catches the deserialization or schema validation exception.
2. Captures rich error metadata (error type, traceback details, original topic, partition, offset, timestamp).
3. Forwards the poisoned payload + metadata to a **Dead Letter Queue (DLQ)** topic (`poc_4_orders_dlq`).
4. Commits the offset of the poison pill message on the main topic (`poc_4_orders`) so partition processing continues uninterrupted.

---

## 2. What is a Poison Pill & Head-of-Line Blocking?

### The Problem
A **Poison Pill** is a record published to a Kafka topic that cannot be processed successfully by a consumer.

Common examples:
- **Corrupted Payloads**: Malformed JSON strings (e.g. `{"order_id": 102, "amount": invalid_json`).
- **Schema Violations**: Missing required fields or invalid data types (e.g. negative amount `amount: -999.0`).
- **Uncaught Business Logic Exceptions**: Unexpected values causing unexpected crashes.

```text
Partition 0:  [Offset 1: Valid] ──> [Offset 2: POISON PILL] ──> [Offset 3: Valid]
                                              │
                                    Consumer Crashes / Fails
                                              │
                                   Polls Offset 2 Again!
                                 (Head-of-Line Blocking ⛔)
```

Without DLQ error handling:
- The consumer reads Offset 2, fails, throws an unhandled exception, and crashes.
- Upon restart, the consumer group polls Offset 2 again (because Offset 2 was never committed).
- Offset 3 and all future valid messages are **blocked forever** (Head-of-Line Blocking).

---

## 3. Architecture & Solution Flow

```text
                   Produce Mixed Stream
               (produce_mixed_events.py)
                           │
       ┌───────────────────┴───────────────────┐
       ▼                                       ▼
  Valid Events                           Poison Pills
 (Order 101, 103, 105)             (Malformed JSON, Negative Amount)
       │                                       │
       └───────────────────┬───────────────────┘
                           ▼
               Kafka Topic: poc_4_orders
                           │
                           ▼
             Main Consumer (consumer.py)
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
      Valid Message                Poison Pill!
             │                           │
    1. Process Order            1. Log Error Details
    2. Commit Offset            2. Send Payload + Metadata to DLQ
                                3. Commit Main Offset
                                         │
                                         ▼
                            Kafka Topic: poc_4_orders_dlq
                                         │
                                         ▼
                             DLQ Audit Consumer
                              (dlq_consumer.py)
```

---

## 4. Key Components

| File | Purpose |
|---|---|
| [`order.py`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/kafka_poc_4/order.py) | Pydantic model (`OrderRequest`) enforcing strict schema validation (`gt=0` constraints). |
| [`producer.py`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/kafka_poc_4/producer.py) | Helper functions to produce valid orders, corrupted JSON poison pills, schema violation poison pills, and forward failed payloads to DLQ. |
| [`produce_mixed_events.py`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/kafka_poc_4/produce_mixed_events.py) | Script publishing a real-time interleaved stream of valid orders and poison pills. |
| [`consumer.py`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/kafka_poc_4/consumer.py) | Main Kafka consumer handling deserialization, validation, DLQ routing, and offset commits. |
| [`dlq_consumer.py`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/kafka_poc_4/dlq_consumer.py) | Dedicated DLQ audit consumer for auditing poisoned messages. |
| [`router.py`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/kafka_poc_4/router.py) | FastAPI endpoints under `/poc/04` for optional HTTP triggers. |

---

## 5. Consumer Error Handling & DLQ Logic

```python
# Step 1: Catch Corrupted JSON
try:
    payload = json.loads(raw_bytes.decode("utf-8"))
except (json.JSONDecodeError, UnicodeDecodeError) as e:
    send_to_dlq(raw_bytes, error_metadata)
    consumer.commit(message=message)  # Commit offset so partition progress continues
    continue

# Step 2: Catch Schema Violations
try:
    validated_order = OrderRequest(**payload)
except ValidationError as ve:
    send_to_dlq(raw_bytes, error_metadata)
    consumer.commit(message=message)  # Commit offset so partition progress continues
    continue

# Step 3: Process Valid Message & Commit
process_order(validated_order)
consumer.commit(message=message)
```

---

## 6. How to Run & Verify

### Step 1: Start Kafka Infrastructure
Ensure Kafka is running locally via Docker Compose:
```bash
docker compose up -d
```

### Step 2: Run DLQ Audit Consumer (Terminal 1)
```bash
PYTHONPATH=. python app/kafka_poc_4/dlq_consumer.py
```

### Step 3: Run Main Order Consumer (Terminal 2)
```bash
PYTHONPATH=. python app/kafka_poc_4/consumer.py
```

### Step 4: Execute Mixed Stream Producer (Terminal 3)
```bash
PYTHONPATH=. python app/kafka_poc_4/produce_mixed_events.py
```

---

## 7. Observed Behavior

1. **Main Consumer (`consumer.py`)**:
   - Processes valid orders (`order_id=101, 103, 105, 107`).
   - Detects poison pills (`order_id=102` corrupted JSON, `order_id=104` negative amount, `order_id=106` missing user_id).
   - Routes poison pills to `poc_4_orders_dlq` and commits the main offset without crashing.

2. **DLQ Consumer (`dlq_consumer.py`)**:
   - Prints full audit report of all 3 poison pill messages including original raw payload, error details, and original topic/partition/offset metadata.

3. **Kafka UI (`http://localhost:8080`)**:
   - Main consumer lag returns to `0`.
   - `poc_4_orders_dlq` topic holds the isolated bad messages.

---

## 8. Key Learnings

- **Head-of-Line Blocking**: Unhandled errors on uncommitted offsets stop consumer groups from making progress.
- **Dead Letter Queue (DLQ)**: Routing bad records to a separate topic preserves data while keeping the main stream flowing.
- **Explicit Offset Commits after DLQ Routing**: The offset of a poison pill *must* be committed on the main topic after it is safely stored in the DLQ.
- **Rich Error Metadata**: Always attach original topic, partition, offset, timestamp, and error stack trace when publishing to a DLQ for developer auditing.
