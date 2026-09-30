# POC-3: Kafka Idempotency

## 1. Objective

The objective of POC-3 is to understand and practically demonstrate **producer-side and consumer-side idempotency** in Apache Kafka.

This POC focuses only on idempotency and the supporting producer/consumer behavior required to test it.

---

# 2. Scope

| # | Concept | Covered |
|---|---|---|
| 1 | Producer Idempotency | ✅ |
| 2 | Consumer Idempotency | ✅ |
| 3 | Manual Offset Commit | ✅ |
| 4 | Consumer Failure & Redelivery | ✅ |
| 5 | Producer Retry after Kafka Failure | ✅ |
| 6 | `producer.poll(0)` | ✅ |
| 7 | `producer.flush()` | ✅ |


---

# 3. High-Level Architecture

```text
                 Producer
                    |
                    | enable.idempotence=True
                    |
                    v
              Kafka Topic
                    |
                    v
                 Consumer
                    |
             Check order_id
                    |
          +---------+---------+
          |                   |
       Already              New
       processed            order
          |                   |
          v                   v
        Skip              Process order
          |                   |
          |                   v
          |             Save order_id
          |                   |
          +---------+---------+
                    |
                    v
              Commit Offset
```

---

# 4. Producer-Side Idempotency

## 4.1 Producer Configuration

The producer is configured as:

```python
producer = Producer({
    "bootstrap.servers": bootstrap_servers,
    "enable.idempotence": True,
    "acks": "all",
})
```

### Important settings

| Setting | Purpose |
|---|---|
| `enable.idempotence=True` | Enables idempotent producer behavior |
| `acks=all` | Producer waits for the required acknowledgement from replicas |

---

# 5. Producer Implementation

The POC uses multiple messages to test producer behavior during Kafka failure.

```python
def produce_orders():
    for i in range(1, 1001):
        order = {
            "order_id": i,
            "customer_id": 100 + i,
            "event_type": "ORDER_PLACED",
            "sequence": 1,
            "amount": 5000
        }

        logger.info(
            f"Publishing order={order}"
        )

        producer.produce(
            topic=TOPIC,
            key=str(i),
            value=json.dumps(order),
            callback=delivery_report
        )

        producer.poll(0)

    producer.flush()
```

The producer generates:

```text
order_id=1
order_id=2
order_id=3
...
order_id=1000
```

---

# 6. Delivery Callback

The producer uses a callback to observe delivery results.

```python
def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered | "
        f"topic={message.topic()} | "
        f"partition={message.partition()} | "
        f"offset={message.offset()}"
    )
```

Successful delivery example:

```text
Delivered |
topic=poc_4_orders |
partition=2 |
offset=101
```

---

# 7. Producer `poll(0)`

The producer is asynchronous.

```python
producer.produce(...)
```

does not wait for the final delivery result.

The following:

```python
producer.poll(0)
```

allows the producer to process available events and delivery callbacks without blocking.

### Flow

```text
producer.produce()
        |
        v
Producer queue
        |
        v
Kafka
        |
        v
Delivery event
        |
        v
producer.poll(0)
        |
        v
delivery_report()
```

---

# 8. Producer `flush()`

At the end of production:

```python
producer.flush()
```

waits for outstanding producer messages to complete.

You can also inspect the return value:

```python
remaining = producer.flush(timeout=10)

logger.info(
    f"Flush completed | "
    f"messages_remaining={remaining}"
)
```

### Result

```text
remaining = 0
```

means no messages remain outstanding after the timeout.

For example:

```text
remaining = 5
```

means five messages were still outstanding after the specified timeout.

### Important distinction

```text
enable.idempotence=True
        |
        +--> Producer idempotency


flush()
        |
        +--> Wait for outstanding messages
```

`flush()` does **not** provide idempotency.

---

# 9. Producer Idempotency Test

## Test scenario

The purpose of this test is to observe producer behavior when Kafka becomes unavailable while messages are being produced.

### Step 1 — Start Kafka

```bash
docker start kafka
```

### Step 2 — Start the producer

Run the producer that sends many messages:

```text
1 → 1000
```

### Step 3 — Stop Kafka while the producer is running

```bash
docker stop kafka
```

The producer can show an error such as:

```text
Connect to ipv4#127.0.0.1:9092 failed:
Connection refused
```

### Step 4 — Start Kafka again

```bash
docker start kafka
```

### Step 5 — Observe producer recovery

The producer can reconnect and continue handling outstanding records while the producer process remains alive.

---

# 10. What We Learned from the Producer Test

The test demonstrated that a Kafka failure can cause the producer to lose its connection:

```text
Producer
   |
   v
Kafka
   |
   X
Kafka unavailable
   |
   v
Producer connection failure
   |
   v
Producer retry/reconnect
   |
   v
Kafka available
   |
   v
Outstanding records continue delivery
```

### Important clarification

Producer idempotency is **not the same thing as consumer-side duplicate prevention**.

Producer idempotency is concerned with duplicate records that can arise from producer retries.

It does not guarantee that a consumer will never process a message more than once.

Consumer-side idempotency is used for that problem.

---

# 11. Consumer-Side Idempotency

Consumer-side idempotency is implemented using a database table:

```text
processed_orders
----------------
id
order_id
processed_at
```

The important field is:

```python
order_id
```

which is unique.

---

# 12. Processed Order Model

The implemented model is:

```python
from datetime import datetime

from sqlalchemy import Column, Integer, DateTime

from app.kafka_poc_3.db.base import Base


class ProcessedOrder(Base):
    __tablename__ = "processed_orders"

    id = Column(
        Integer,
        primary_key=True,
        index=True
    )

    order_id = Column(
        Integer,
        unique=True,
        nullable=False,
        index=True
    )

    processed_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )
```

The unique `order_id` is used to determine whether the order has already been processed.

---

# 13. Consumer Configuration

The consumer is configured for manual offset commits:

```python
consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
```

### Important settings

| Setting | Meaning |
|---|---|
| `group.id` | Identifies the consumer group |
| `auto.offset.reset=earliest` | Starts from earliest available offset when no committed offset exists |
| `enable.auto.commit=False` | Application controls when offsets are committed |

---

# 14. Consumer Idempotency Logic

The main duplicate-checking logic is:

```python
already_processed = (
    ProcessedOrderRepository.exists(
        db=db,
        order_id=order_id
    )
)

if already_processed:
    logger.info(
        f"Duplicate order skipped | "
        f"consumer={consumer_name} | "
        f"order_id={order_id}"
    )

    consumer.commit(message=message)

    continue
```

If the order is new:

```python
ProcessedOrderRepository.save(
    db=db,
    order_id=order_id
)

consumer.commit(message=message)
```

---

# 15. Complete Consumer Flow

The implemented logic follows:

```text
Kafka message
     |
     v
Read order_id
     |
     v
Check processed_orders
     |
     +-------------------------+
     |                         |
     v                         v
Already processed          Not processed
     |                         |
     v                         v
Skip business             Process order
processing                     |
     |                         v
     |                    Save order_id
     |                         |
     +------------+------------+
                  |
                  v
             Commit offset
```

---

# 16. Why Manual Commit Is Important

The consumer uses:

```python
"enable.auto.commit": False
```

and explicitly commits:

```python
consumer.commit(message=message)
```

The intended sequence is:

```text
Receive message
      |
      v
Process message
      |
      v
Save processing result
      |
      v
Commit Kafka offset
```

This allows the application to avoid committing the offset before the business processing has completed.

---

# 17. Consumer Failure Test

This is the main consumer-idempotency test.

### Scenario

Suppose Kafka contains:

```text
order_id=101
order_id=102
order_id=103
...
```

The consumer receives:

```text
order_id=101
```

and processes it.

```text
Order 101
   |
   v
Business processing
   |
   v
processed_orders
   |
   v
order_id=101 stored
```

Now imagine the consumer fails before its Kafka offset is committed.

Kafka can deliver the message again.

```text
Kafka
   |
   v
order_id=101
   |
   v
Consumer restarted
   |
   v
Check processed_orders
   |
   v
order_id=101 already exists
   |
   v
Duplicate skipped
```

Expected log:

```text
Processing order | order_id=101
Order marked as processed | order_id=101
```

After restart:

```text
Duplicate order skipped | order_id=101
```

---

# 18. What This Test Proves

The important point is:

```text
Kafka can redeliver a message
            |
            v
Consumer receives it again
            |
            v
Database detects previous processing
            |
            v
Business operation is NOT repeated
```

This is consumer-side idempotency.

---

# 19. Producer vs Consumer Idempotency

| Aspect | Producer Idempotency | Consumer Idempotency |
|---|---|---|
| Location | Producer/Kafka boundary | Consumer/business layer |
| Main problem | Duplicate caused by producer retry | Duplicate business processing |
| Example | Producer retries a record | Consumer crashes before offset commit |
| Implementation | `enable.idempotence=True` | `processed_orders` |
| Main protection | Kafka record duplication during producer retry | Duplicate business operation |
| Offset handling | Not responsible for consumer offsets | Uses manual offset commit |

### Key learning

```text
Producer Idempotency
        |
        v
Handles producer retry duplication


Consumer Idempotency
        |
        v
Handles duplicate business processing
```

They solve different problems.

---

# 20. Test Cases

| Test | Expected Result |
|---|---|
| Normal producer send | Messages delivered |
| Producer with idempotence enabled | Producer uses idempotent delivery behavior |
| Kafka stopped during production | Producer loses connection |
| Kafka restarted | Producer can reconnect/retry outstanding records |
| `poll(0)` | Available producer events/callbacks are processed |
| `flush()` | Producer waits for outstanding messages |
| Normal consumer processing | Order processed |
| Consumer fails before commit | Message can be redelivered |
| Redelivered order already in DB | Duplicate is skipped |
| New order | Processed and stored |
| Successful processing | Kafka offset committed |

---

# 21. Final POC Flow

```text
                 PRODUCER
                    |
                    | enable.idempotence=True
                    | acks=all
                    v
              Kafka Topic
                    |
                    v
                CONSUMER
                    |
                    v
             Check order_id
                    |
             +------+------+
             |             |
        Already exists    New
             |             |
             v             v
           Skip         Process
             |             |
             |             v
             |        Save order_id
             |             |
             +------+------+
                    |
                    v
              Commit Offset
```

---

# 22. Final Learning

POC-4 demonstrates that reliable Kafka processing requires different mechanisms for different failure scenarios.

### Producer side

```python
"enable.idempotence": True
```

is used for producer idempotency.

### Consumer side

```python
ProcessedOrderRepository.exists(...)
```

is used to detect previously processed business records.

### Offset control

```python
"enable.auto.commit": False
```

and:

```python
consumer.commit(message=message)
```

allow the application to commit only after processing.

### Producer utilities

```python
producer.poll(0)
```

processes available producer events/callbacks.

```python
producer.flush()
```

waits for outstanding producer messages.

---

# 23. POC-3 Conclusion

POC-4 focuses exclusively on **Kafka idempotency**:

```text
Producer Idempotency
        +
Consumer Idempotency
        +
Manual Offset Commit
        +
Producer Retry Testing
        +
poll()
        +
flush()
```

