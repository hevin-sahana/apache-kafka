
# Kafka POC-02 -- Key, Partition & Consumer Groups

**Apache Kafka 4.0.0 \| Docker Compose \| Python \| FastAPI \|
confluent-kafka**

------------------------------------------------------------------------

## 1. POC Overview

This POC demonstrates how Kafka handles **message keys, partitions,
explicit partition selection, consumer groups, and partition-to-consumer
assignment**.

The producer sends order events to the `poc_2_orders` topic. The POC
tests how a record is placed into a partition and then how consumers
receive those records based on their consumer-group membership.

The main observation is:

``` text
Producer
   ↓
Key / Explicit Partition
   ↓
Kafka Topic
   ↓
Partitions
   ↓
Consumer Group Assignment
   ↓
Consumers
```

------------------------------------------------------------------------

## 2. Objectives

-   Understand the relationship between a **Kafka key and partition**.
-   Understand what happens when **key** is provided.
-   Understand what happens when **no key** is provided.
-   Understand how an **explicit partition** is selected.
-   Understand how partitions are distributed among consumers in the
    **same consumer group**.
-   Understand how consumers in **different consumer groups** consume
    the same topic independently.
-   Understand who decides which consumer receives which partition.
-   Observe partition and consumer information through application logs.
-   Understand the difference between **producer-side partition
    selection** and **consumer-side partition assignment**.

------------------------------------------------------------------------

## 3. Architecture

The POC uses the following flow:

``` text
Producer
   ↓
Key / Partition
   ↓
Kafka Topic: poc_2_orders
   ↓
┌─────────┬─────────┬─────────┐
│ P0      │ P1      │ P2      │
└─────────┴─────────┴─────────┘
        ↓
Consumer Group Assignment
        ↓
┌───────────────────────────────┐
│ Group 1                       │
│ consumer-1 + consumer-2      │
└───────────────────────────────┘

        AND

┌───────────────────────────────┐
│ Group 2                       │
│ consumer-3                   │
└───────────────────────────────┘
```

------------------------------------------------------------------------

## 4. Technology Stack

| **Technology**         | **Purpose**                             |
| ---------------------- | --------------------------------------- |
| **Apache Kafka 4.0.0** | Event streaming / message broker        |
| **Docker Compose**     | Run Kafka and Kafka UI locally          |
| **Python**             | Producer and consumer implementation    |
| **FastAPI**            | Application API used to submit orders   |
| **confluent-kafka**    | Python Kafka client                     |
| **Kafka UI**           | Inspect topics, partitions, messages and consumer groups |


------------------------------------------------------------------------

## 5. Kafka Docker Setup

The POC uses Kafka with **three partitions** for the order topic.

Important connection addresses:

-   Python/FastAPI running on the host: `localhost:9092`
-   Kafka UI running in Docker: `kafka:29092`
-   Kafka UI browser address: `http://localhost:8080`

------------------------------------------------------------------------

## 6. Topic and Partition Model

The `poc_2_orders` topic has three partitions:

``` text
Partition 0
Partition 1
Partition 2
```

Each partition maintains its own independent offset sequence.

Example:

``` text
Partition 0: offset 0, 1, 2, 3...
Partition 1: offset 0, 1, 2, 3...
Partition 2: offset 0, 1, 2, 3...
```

A record is stored in exactly one partition.

The producer/partitioning logic determines which partition receives the
record.

------------------------------------------------------------------------

## 7. Producer Key and Partition Behaviour

The producer supports both a `key` and an optional explicit `partition`.

Conceptual implementation:

``` python
def produce_order(order, key=None, partition=None):

    kwargs = {
        "topic": TOPIC,
        "value": json.dumps(order.model_dump()),
        "callback": delivery_report,
    }

    if key is not None:
        kwargs["key"] = key

    if partition is not None:
        kwargs["partition"] = partition

    producer.produce(**kwargs)
    producer.poll(0)
```

There are four important cases.

 | **Key**      | **Partition** | **Behaviour**                                                                     |
| ------------ | ------------- | --------------------------------------------------------------------------------- |
| Not provided | Not provided  | Kafka's partitioning logic determines the partition                               |
| Provided     | Not provided  | The key is used by the partitioner to determine the partition                     |
| Not provided | Provided      | The record is sent to the explicitly specified partition                          |

------------------------------------------------------------------------

## 8. Key-Based Partitioning

When a key is provided a,the producer's partitioner uses the key to determine the target
partition.

In the POC, the same key was observed going to the same partition.

Example:

``` text
Order A → key=customer-1 → Partition 0
Order B → key=customer-1 → Partition 0
Order C → key=customer-2 → Partition 1
```

The important observation is:

``` text
Same Key
   ↓
Same Partition
```

This is useful when related records need to stay ordered within a
partition.

------------------------------------------------------------------------

## 9. No Key and No Explicit Partition

If the producer does not provide either a key or an explicit partition:

``` python
producer.produce(
    topic=TOPIC,
    value=...
)
```

Kafka's producer partitioning logic determines the partition.

Do not describe this simply as "random partition selection."

The correct understanding is:

``` text
No Key
  +
No Explicit Partition
        ↓
Producer Partitioning Logic
        ↓
Partition selected automatically
```

The exact placement depends on the producer's partitioning behaviour.

------------------------------------------------------------------------

## 10. Explicit Partition

The producer can explicitly specify the target partition.

Example:

``` python
produce_order(
    order,
    partition=0
)
```

The record is sent to partition `0`.

Similarly:

``` python
produce_order(order, partition=1)
produce_order(order, partition=2)
```

send records to partitions `1` and `2`.

The flow is:

``` text
Producer
   ↓
partition=2
   ↓
Partition 2
```

This is different from key-based partition selection.

------------------------------------------------------------------------

## 11. Key vs Explicit Partition

The important distinction is:

``` text
Key provided
     ↓
Partitioner determines partition
```

versus:

``` text
Partition provided
     ↓
Producer sends to specified partition
```

Therefore, `key` and `partition` are not the same mechanism.

The producer code allows both values to be supplied, but an explicit
partition controls where the record is placed.

------------------------------------------------------------------------

## 12. Consumer Configuration

The consumer uses:

``` python
consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe([TOPIC])
```

The important part for this POC is:

``` python
"group.id": group_id
```

Consumers using the same group ID belong to the same consumer group.

------------------------------------------------------------------------

## 13. Consumer Group Setup

The POC uses two consumer groups.

### Group 1

``` text
consumer_order_group_1
```

Contains:

``` text
consumer-1
consumer-2
```

### Group 2

``` text
consumer_order_group_2
```

Contains:

``` text
consumer-3
```

Therefore:

``` text
                 poc_2_orders
                      │
          ┌───────────┴───────────┐
          │                       │
       Group 1                 Group 2
          │                       │
     ┌────┴────┐                  │
     │         │                  │
    C1        C2                 C3
```

------------------------------------------------------------------------

## 14. Partition Distribution Within a Consumer Group

Consumers in the **same consumer group share partitions**.

For example, with three partitions and two consumers:

``` text
Topic
 ├── P0
 ├── P1
 └── P2
      ↓
Group 1
 ├── consumer-1
 └── consumer-2
```

A possible assignment could be:

| **Partition** | **Consumer** |
| ------------- | ------------ |
| P0            | consumer-1   |
| P1            | consumer-2   |
| P2            | consumer-1   |


Therefore:

``` text
consumer-1 → P0, P2
consumer-2 → P1
```

The exact assignment is decided by Kafka's consumer-group partition
assignment mechanism and can change after a rebalance.

------------------------------------------------------------------------

## 15. Who Decides Which Consumer Gets Which Partition?

When using:

``` python
consumer.subscribe([TOPIC])
```

the application normally does **not** manually choose the
partition-to-consumer assignment.

Kafka's consumer-group coordination and partition-assignment mechanism
assign partitions among active consumers in the same group.

The application says:

``` text
"I want to consume this topic as part of this group."
```

Kafka then determines the partition assignment.

Therefore:

``` text
Consumer-1 ─┐
            ├──→ Consumer Group
Consumer-2 ─┘
                 ↓
          Partition Assignment
                 ↓
             P0 / P1 / P2
```

------------------------------------------------------------------------

## 16. Different Consumer Groups

Consumers in different groups consume independently.

For example:

``` text
Group 1
 ├── consumer-1
 └── consumer-2

Group 2
 └── consumer-3
```

Group 1 and Group 2 maintain independent consumption progress.

Therefore, the same record can be consumed by:

``` text
Group 1 → record
Group 2 → same record
```

This is why `consumer-3` can receive the same order events that are also
consumed by consumer-1 or consumer-2.

------------------------------------------------------------------------

## 17. POC Log Observation

The POC logs demonstrate that consumer-1 and consumer-2 are members of:

``` text
order-consumer-group_1
```

and consumer-3 is a member of:

``` text
order-consumer-group_2
```

Example observation:

``` text
consumer-1 → partition=2 → group=order-consumer-group_1
consumer-2 → partition=0 → group=order-consumer-group_1
consumer-3 → partition=0 → group=order-consumer-group_2

```

The logs also show the same order being consumed independently by
different groups.

For example, order `46` was observed by consumer-3 and consumer-1 from
partition `0`, but under different consumer groups.

------------------------------------------------------------------------

## 18. Producer vs Consumer Responsibility

This POC demonstrates two different decisions.

### Producer side

The producer determines:

``` text
Which partition should receive this record?
```

based on:

-   key-based partitioning, or
-   explicit partition selection.

### Consumer side

Kafka's consumer-group assignment determines:

``` text
Which consumer should process this partition?
```

within a consumer group.

Therefore:

``` text
Producer
   ↓
Key / Partition Selection
   ↓
Kafka Partition
   ↓
Consumer Group Assignment
   ↓
Consumer
```

------------------------------------------------------------------------

## 19. Important Difference: Partition Selection vs Consumer Assignment

These are two completely different concepts.

| **Stage**           | **Question**                            | **Mechanism**                                 |
| ------------------- | --------------------------------------- | --------------------------------------------- |
| **Producer**        | Which partition receives the record?    | Key / partitioning logic / explicit partition |
| **Consumer group**  | Which consumer reads the partition?     | Kafka partition assignment                    |
| **Different group** | Can another group read the same record? | Yes, independently                            |


------------------------------------------------------------------------

## 20. Common Scenarios

### Scenario 1 -- Same Key

``` text
key=customer-1
      ↓
Partition 0

key=customer-1
      ↓
Partition 0
```

Same key was observed on the same partition in this POC.

### Scenario 2 -- No Key

``` text
No Key
  ↓
Producer partitioning logic
  ↓
Partition selected automatically
```

### Scenario 3 -- Explicit Partition

``` text
partition=2
     ↓
Partition 2
```

### Scenario 4 -- Same Consumer Group

``` text
P0 ──→ consumer-1
P1 ──→ consumer-2
P2 ──→ consumer-1
```

The consumers share the partitions.

### Scenario 5 -- Different Consumer Group

``` text
Group 1 → consumes topic
Group 2 → independently consumes topic
```

The same records can therefore be consumed by both groups.

---
### Ideal Condition — 3 Partitions, 4 Consumers in Group 1

Your setup is:

```text
consumer-1 ─┐
consumer-2 ─┤
consumer-4 ─┼── consumer_order_group_1
consumer-5 ─┘

consumer-3 ──── consumer_order_group_2
```

Assuming the topic has **3 partitions**:

```text
P0
P1
P2
```

### Expected behavior

For `consumer_order_group_1`:

| Consumer   | Group                    | Expected partition assignment |
| ---------- | ------------------------ | ----------------------------- |
| consumer-1 | `consumer_order_group_1` | P0 / P1 / P2                  |
| consumer-2 | `consumer_order_group_1` | P0 / P1 / P2                  |
| consumer-4 | `consumer_order_group_1` | P0 / P1 / P2                  |
| consumer-5 | `consumer_order_group_1` | **No partition — idle**       |
| consumer-3 | `consumer_order_group_2` | P0, P1, P2 independently      |

The exact assignment of P0/P1/P2 among the first three consumers can vary after Kafka's group assignment/rebalance.

### Ideal condition

Because:

```text
Partitions = 3
Consumers in Group 1 = 4
```

the maximum number of consumers that can actively consume partitions in that group at one time is **3**.

Therefore:

```text
consumer-1 ──→ P0
consumer-2 ──→ P1
consumer-4 ──→ P2
consumer-5 ──→ IDLE
```

This is an **ideal example**, not a guaranteed exact assignment. Kafka may choose a different three consumers.

### Group 2

`consumer-3` has a different group:

```text
consumer-3
     │
     ↓
consumer_order_group_2
     │
 ┌───┼───┐
 P0  P1  P2
```

So `consumer-3` can consume the same topic records independently of Group 1.

### Key rule to add to your POC-2 docs

> **Within a consumer group, one partition can be assigned to only one active consumer at a time. Therefore, if the number of consumers is greater than the number of partitions, some consumers will remain idle. If the number of partitions is greater than or equal to the number of consumers, each consumer can potentially receive a partition.**

For your exact setup:

```text
3 partitions + 4 consumers
        ↓
3 consumers can be active
1 consumer can be idle
```

This is a very good **ideal-condition test** to keep in POC-2 because it clearly demonstrates the relationship between **partition count and consumer count**.


------------------------------------------------------------------------

## 21. Key Learnings

-   Kafka topics are divided into partitions.
-   A record is stored in one partition.
-   A message key can be used by the producer partitioner to determine
    the target partition.
-   The same key was observed going to the same partition in this POC
    when no explicit partition was supplied.
-   Without a key and without an explicit partition, the producer's
    partitioning logic determines placement.
-   An explicit partition tells the producer exactly which partition to
    use.
-   Key-based partitioning and explicit partition selection are
    different mechanisms.
-   Consumers with the same `group.id` belong to the same consumer group
    and share partitions.
-   Consumers in different consumer groups consume the topic
    independently.
-   Kafka's consumer-group partition assignment mechanism decides which
    consumer gets which partition when using `subscribe()`.
-   A partition can be consumed by one consumer in a given consumer
    group at a time, while another consumer group can independently
    consume that same partition.
-   Producer-side partition selection and consumer-side partition
    assignment are separate stages.
-   idle condition in kafka.
------------------------------------------------------------------------

## 22. POC-02 Final Flow

``` text
                    Producer
                       │
                       ▼
                 Key / Partition 
                       │
                       ▼
                   Kafka Topic
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
         P0           P1           P2
          │            │            │
          └────────────┼────────────┘
                       │
              Consumer Group
                 Assignment
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
          Group 1              Group 2
          C1 + C2                 C3
             │                   │
             ▼                   ▼
       Shared partitions    Independent copy
```

------------------------------------------------------------------------

## 23. POC-02 Summary

The key concept demonstrated by this POC is:

``` text
Producer decides:
    Record → Partition

Kafka consumer group decides:
    Partition → Consumer
```

And:

``` text
Same Group
    ↓
Consumers share partitions

Different Groups
    ↓
Consumers consume independently
```

This POC therefore demonstrates the relationship between **keys,
partitions, consumer groups, and consumer assignment**.

------------------------------------------------------------------------

