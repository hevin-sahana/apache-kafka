# Kafka POC-01 – Producer, Consumer, Offset & Manual Commit

**Apache Kafka 4.0.0 | Docker Compose | Python | FastAPI | confluent-kafka**

---

## 1. POC Overview

This POC demonstrates a realistic order-event flow using Apache Kafka.

FastAPI is used as the application API through which an order is submitted. A Python Kafka producer publishes the order to the `orders` topic. A Python Kafka consumer receives and processes the message, then manually commits the offset only after successful processing.

---

## 2. Objectives

- Understand the producer → Kafka topic → consumer flow.
- Send messages through a FastAPI endpoint.
- Understand topics, partitions, offsets and consumer groups.
- Understand producer delivery acknowledgement.
- Implement manual consumer offset commits.
- Understand the difference between reading a message and committing its offset.
- Observe consumer lag using Kafka UI.
- Test what happens when processing fails before the offset is committed.
- Use standardized application logging.

---

## 3. Architecture

```text
FastAPI / Swagger
        ↓
Kafka Producer
        ↓
Kafka topic: orders
        ↓
Partition + Offset
        ↓
Kafka Consumer Group: order-consumer-group
        ↓
Process Order
        ↓
Manual Offset Commit
```

---

## 4. Technology Stack

| Technology | Purpose |
|---|---|
| Apache Kafka 4.0.0 | Event streaming/message broker |
| Docker Compose | Run Kafka and Kafka UI locally |
| Python | Producer and consumer implementation |
| FastAPI | HTTP API used to submit orders |
| confluent-kafka | Python Kafka client |
| Kafka UI | Inspect topics, partitions, messages and consumer groups |

---

## 5. Kafka Docker Setup

The POC uses a single Kafka broker running in KRaft mode and three partitions for the `orders` topic.

Important connection addresses:

- Python/FastAPI running on the host: `localhost:9092`
- Kafka UI running in Docker: `kafka:29092`
- Kafka UI browser address: `http://localhost:8080`

---

## 6. Topic and Partition Model

The `orders` topic is configured with three partitions.

Each partition maintains its own independent offset sequence.

```text
Partition 0: offset 0, 1, 2, 3...
Partition 1: offset 0, 1, 2, 3...
Partition 2: offset 0, 1, 2, 3...
```

If Partition 1 currently contains offsets `0, 1, 2, 3`, the next record written to Partition 1 receives offset `4`.

The next produced message is not necessarily sent to Partition 1; partitioning logic determines the target partition.

---

## 7. FastAPI Producer Flow

FastAPI provides the application entry point. The client sends an order to an HTTP endpoint. The endpoint converts the request into a Kafka message and publishes it to the `orders` topic.

### Producer configuration

```python
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})
```

### Publishing

```python
producer.produce(
    topic="orders",
    value=json.dumps(order),
    callback=delivery_report
)

producer.flush()
```

---

## 8. Producer Acknowledgement

Producer acknowledgement answers:

> Did Kafka successfully accept/store the record?

The delivery callback can log the topic, partition and offset assigned by Kafka.

Example:

```text
Message delivered | topic=orders | partition=1 | offset=4
```

This acknowledgement does **not** mean that a consumer has processed the order.

It confirms successful producer delivery according to the producer's delivery result.

---

## 9. Consumer Configuration

The consumer uses a named consumer group and disables automatic commits so that the application controls when processing progress is committed.

```python
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe(["orders"])
```

### Important setting

```python
"enable.auto.commit": False
```

This means:

> The application will decide when the consumer group's offset should be committed.

---

## 10. Consumer Processing Flow

The consumer:

1. Polls Kafka.
2. Checks for errors.
3. Deserializes the JSON message.
4. Processes the order.
5. Commits the offset after successful processing.

```python
message = consumer.poll(1.0)

order = json.loads(
    message.value().decode("utf-8")
)

logger.info(
    f"Order received | "
    f"topic={message.topic()} | "
    f"partition={message.partition()} | "
    f"offset={message.offset()} | "
    f"group=order-consumer-group"
)

logger.info(
    f"Order processed | "
    f"order_id={order['order_id']}"
)

consumer.commit(message=message)

logger.info(
    f"Offset committed | "
    f"topic={message.topic()} | "
    f"partition={message.partition()} | "
    f"offset={message.offset()}"
)
```

---

## 11. Consumer Acknowledgement / Offset Commit

Kafka consumers do not acknowledge messages exactly like a traditional queue.

The consumer records its progress by **committing offsets** for its consumer group.

The correct sequence is:

```text
Receive message
      ↓
Process message
      ↓
Processing succeeds
      ↓
consumer.commit(message=message)
```

The key rule is:

> Do not commit before successful processing.

If the application commits first and then crashes during processing, the consumer group may consider the record completed even though the business operation was not completed.

---

## 12. Read vs Commit

Reading a message and committing its offset are different operations.

| Operation | Meaning | Evidence |
|---|---|---|
| Read / poll | Consumer fetched the record | `Order received` log |
| Process | Application performed the business operation | `Order processed` log |
| Commit | Consumer group recorded its progress | Consumer UI / lag |

Therefore:

```text
Read ≠ Commit
```

A message can be read by a consumer but not yet have its offset committed.

---

## 13. Understanding Offsets and Kafka UI

Kafka offsets are zero-based.

For example, if Partition 0 contains:

```text
Offset 0 → Message 1
Offset 1 → Message 2
Offset 2 → Message 3
```

then:

```text
Message Count = 3
Next Offset   = 3
```

Therefore, if the consumer log says:

```text
partition=0 | offset=2
```

while Kafka UI shows:

```text
Next Offset = 3
```

there is no mismatch.

Offset `2` is the third existing record, while `3` is the next position.

---

## 14. Verify Consumer Commit in Kafka UI

Open:

```text
Kafka UI → Consumers → order-consumer-group
```

The consumer group view shows:

- Consumer lag
- Current offset
- End offset
- Assigned partitions

### Example

```text
Partition | Consumer Lag | Current Offset | End Offset
----------|--------------|----------------|-----------
2         | 0            | 5              | 5
1         | 0            | 9              | 9
0         | 1            | 2              | 3
```

For Partition 0:

```text
End Offset     = 3
Current Offset = 2
Lag            = 1
```

Therefore:

```text
Lag = 3 - 2 = 1
```

The consumer group is one position behind the end of the log.

After successful processing and commit, the consumer should catch up:

```text
Consumer Lag = 0
Current Offset = 3
End Offset = 3
```

### Important

A committed Kafka message is **not deleted** from the topic.

The topic can still show the record in:

```text
Topics → orders → Messages
```

Consumer-group progress and topic message retention are separate concepts.

---

## 15. Failure Handling POC

To demonstrate why manual commits matter, simulate a processing failure before the commit.

Example:

```python
try:
    process_order(order)

    consumer.commit(message=message)

except Exception as e:
    logger.error(
        f"Order processing failed | error={e}"
    )
```

For testing, `process_order()` can intentionally raise an exception.

If processing fails:

```text
Receive message
      ↓
Process message
      ↓
❌ Exception
      ↓
No commit
```

The record can remain visible in Kafka because Kafka retains records independently of consumer commits.

After restarting the consumer, an uncommitted record can be consumed again according to the consumer group's committed position and configuration.

---

## 16. Successful Processing vs Failure

### Successful processing

```text
Receive offset 2
       ↓
Process successfully
       ↓
Commit
       ↓
Consumer group records progress
       ↓
Lag becomes 0 when caught up
```

### Failure before commit

```text
Receive offset 2
       ↓
Process
       ↓
❌ Failure
       ↓
No commit
       ↓
Consumer group does not advance past that point
       ↓
Record can be consumed again
```

---

## 17. Standard Logging

The centralized logging configuration should add the timestamp automatically.

You do not need to manually add a timestamp to every log message.

Example:

```text
2026-09-23 12:12:26,105 | INFO | __main__ | Order received | topic=orders | partition=1 | offset=3 | group=order-consumer-group
```

Fields:

```text
Timestamp
Log level
Logger name
Event
Topic
Partition
Offset
Consumer group
```

Example commit log:

```text
2026-09-23 12:12:26,107 | INFO | __main__ | Offset committed | topic=orders | partition=1 | offset=3
```

---

## 18. Useful Commands

### Start Kafka and Kafka UI

```bash
docker compose up -d
```

### Check containers

```bash
docker ps
```

### Start FastAPI

From the project root:

```bash
uvicorn app.main:app --reload
```

### Open Swagger

```text
http://localhost:8000/docs
```

### Open Kafka UI

```text
http://localhost:8080
```

---

## 19. Troubleshooting Notes

### `ModuleNotFoundError: No module named 'app'`

Run the command from the project root or set the appropriate `PYTHONPATH`.

### Python cannot connect to Kafka

When Python runs on the host, use:

```text
localhost:9092
```

When another Docker container connects to Kafka, use:

```text
kafka:29092
```

### Consumer exits immediately

Check that:

```python
if __name__ == "__main__":
    configure_logging("poc-01")
    consume_orders()
```

is present and that the consumer is actually polling.

### No messages received

Verify:

- Producer topic is `orders`.
- Consumer topic is `orders`.
- Producer bootstrap server is `localhost:9092`.
- Kafka is running.
- Consumer group is correct.

### Consumer lag is not zero

Open:

```text
Kafka UI → Consumers → order-consumer-group
```

Compare:

```text
Current Offset
End Offset
Consumer Lag
```

---

## 20. Key Learnings

- Kafka stores records in topics.
- Topics are divided into partitions.
- Each partition has its own offset sequence.
- Producers send records to Kafka.
- Producer delivery callbacks can confirm delivery.
- Consumers read records from Kafka.
- Consumers belong to consumer groups.
- Consumer groups track processing progress using committed offsets.
- Reading a record does not automatically mean the record was successfully processed.
- Manual commits allow the application to commit only after successful processing.
- Kafka does not delete a record when a consumer commits its offset.
- Consumer lag helps identify whether a consumer group has caught up.
- Kafka UI can be used to observe topics, partitions, messages and consumer-group progress.

---

## 21. Complete POC-01 Flow

```text
FastAPI request
      ↓
Producer publishes order
      ↓
Kafka producer acknowledgement
      ↓
orders topic
      ↓
Partition + Offset
      ↓
Consumer polls message
      ↓
Order received
      ↓
Order processed
      ↓
Manual offset commit
      ↓
Consumer group progress
      ↓
Kafka UI shows lag/progress
```

---

## 22. POC-01 Deliverables

- Docker Compose file for Kafka and Kafka UI.
- FastAPI order endpoint.
- Kafka producer with delivery callback.
- Kafka consumer with manual offset commit.
- Centralized logging configuration.
- Failure test demonstrating processing failure before commit.
- Kafka UI verification of topic partitions and consumer-group lag.
- POC documentation.

---

## 23. Main Concepts Demonstrated

```text
Producer
   ↓
Topic
   ↓
Partition
   ↓
Offset
   ↓
Consumer Group
   ↓
Consumer
   ↓
Processing
   ↓
Offset Commit
   ↓
Consumer Lag
```

This completes the core concepts for Kafka POC-01.