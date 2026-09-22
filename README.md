# apache-kafka
--
## What is kafka?
Apache Kafka is an open-source distributed event streaming platform developed by LinkedIn and later donated to the Apache Software Foundation. It is used to handle large-scale real-time data streams efficiently and reliably.

Kafka follows the publish-subscribe model, where producers send messages to topics and consumers read them. It provides high scalability, fault tolerance, and fast data processing, making it ideal for real-time data streaming and event-driven applications.

## Need of Apache kafka.
Modern applications generate huge amounts of real-time data from various sources and Traditional systems often struggle to process such large-scale data efficiently. Kafka solves these problems by providing:

- **Real-Time Processing** → Processes events as soon as they arrive.
- **Fault Tolerance** → Keeps copies of data so failures don't easily cause data loss.
- **Scalability** → Can handle increasing data by adding more Kafka brokers/partitions.
- **Event-Driven Architecture** → Applications can react to events instead of constantly asking for updates.
- **High Throughput** → Can handle a very large number of messages efficiently.
- **Offset Management** → Consumers remember where they stopped and can continue from that position.

---
# Kafka Example – How Kafka Solves a Real-World Problem

## 1. Real-World Example

Consider an e-commerce application.

When a customer places an order, several things need to happen:

* Payment needs to be processed.
* Inventory needs to be updated.
* The customer needs to receive a notification.
* The shipping process needs to start.
* The order needs to be recorded for analytics.

## 2. Without Kafka

The Order Service directly communicates with all other services.

```text
Customer
   |
   v
Order Service
   |
   +----> Payment Service
   |
   +----> Inventory Service
   |
   +----> Notification Service
   |
   +----> Shipping Service
```

For example, after creating an order, the Order Service calls:

```text
Payment Service
       ↓
Inventory Service
       ↓
Notification Service
       ↓
Shipping Service
```

### Problems

This approach creates several problems.

### Tight Coupling

The Order Service needs to know about multiple services.

If a new service needs the order information, another connection may need to be added.

### Service Failure

Suppose the Notification Service is temporarily unavailable.

```text
Order Service
   |
   +----> Payment       ✓
   |
   +----> Inventory     ✓
   |
   +----> Notification  ✗
```

The Order Service now needs to handle the failure, retry, timeout, or error.

### High Traffic

Suppose an e-commerce sale generates a very large number of orders.

The Order Service has to communicate with multiple services for every order.

This increases the load on the system.

### Slow Processing

The Order Service may need to wait for other services to respond before completing the overall operation.

---

# 3. With Kafka

Kafka introduces an event between the services.

Instead of directly calling every service, the Order Service publishes an event to Kafka.

```text
Customer
   |
   v
Order Service
   |
   | Order Created Event
   v
 Kafka
   |
   +----> Payment Service
   |
   +----> Inventory Service
   |
   +----> Notification Service
   |
   +----> Shipping Service
```

The Order Service simply publishes:

```text
Order Created
```

Kafka stores the event, and the interested services consume it.

---

# 4. What Happens If a Service Is Down?

Suppose the Notification Service is temporarily unavailable.

```text
Order Service
      |
      v
    Kafka
      |
      +----> Payment Service       ✓
      |
      +----> Inventory Service    ✓
      |
      +----> Notification Service ✗
```

The Order Service does not need to directly wait for the Notification Service.

The event remains available in Kafka according to the topic's retention configuration.

When the Notification Service becomes available again, it can consume the event.

---

# 5. Multiple Services Can Use the Same Event

Suppose the Order Service publishes:

```text
OrderCreated
```

Different services can use that event for different purposes.

```text
                    Order Service
                         |
                         v
                       Kafka
                         |
                  OrderCreated Event
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
       Payment       Inventory      Notification
       Service         Service          Service
```

The Payment Service uses the event for payment processing.

The Inventory Service uses it to update inventory.

The Notification Service uses it to notify the customer.

The Order Service does not need to know the internal processing details of each service.

---

# 6. How Kafka Overcomes the Problems

| Problem                                      | Kafka Approach                                         |
| -------------------------------------------- | ------------------------------------------------------ |
| Services are tightly connected               | Kafka provides an intermediary between services        |
| Direct service-to-service communication      | Services communicate through events                    |
| One service may be temporarily unavailable   | Events can remain available for consumers              |
| High traffic                                 | Kafka is designed for high-throughput event streaming  |
| Producer must communicate with every service | Producer publishes the event once                      |
| Multiple services need the same event        | Multiple consumers can process the event independently |
| Synchronous processing                       | Services can process events asynchronously             |

---

# 7. Simple Comparison

### Without Kafka

```text
Order Service
   |
   +----> Payment
   |
   +----> Inventory
   |
   +----> Notification
   |
   +----> Shipping
```

The Order Service is directly connected to many services.

### With Kafka

```text
Order Service
      |
      v
    Kafka
      |
      +----> Payment
      |
      +----> Inventory
      |
      +----> Notification
      |
      +----> Shipping
```

Kafka acts as the **middle layer for event communication**.

---

# 8. Simple Definition

> **Kafka is a distributed event-streaming platform that allows applications to publish events and other applications to consume those events independently.**

In this example:

```text
Order Service = Producer
Kafka         = Event Streaming Platform
Order Event   = Message/Event
Other Services = Consumers
```

---

# 9. Main Idea to Remember

The main problem Kafka solves is:

> **Instead of making every service directly communicate with every other service, services can publish and consume events through Kafka.**

```text
Producer
   |
   v
 Kafka
   |
   +----> Consumer
   +----> Consumer
   +----> Consumer
```

**For now, focus only on this flow:**

**Producer → Kafka → Consumer**

After you understand this clearly, learn **Topic**, then **Partition**, then **Consumer Group**, and finally **Broker/Cluster** in more depth.


---
## Core Components

**Producer:** A Producer is an application or service that sends messages/events to Kafka topic.

**Kafka Broker:**
A Kafka broker is a server that stores & managing data.

**Kafka Topic:**
A topic in Kafka is a category or feed where messages are stored.

**Offset:**
An offset is a unique identifier for a message in a partition.

**Consumer:**
A Consumer is an application or service that reads messages/events from Kafka topic.

**Consumer Groups:**
A Consumer Group is a group of consumers that read messages/events from the same topic.

**Zookeeper:**
Apache ZooKeeper is a system used to help manage and coordinate distributed systems.
ZooKeeper helped the Kafka brokers coordinate with each other.

---

This diagram shows a **basic Kafka event-driven architecture** using an e-commerce order example.

```text
                    Order Service
                        |
                        | Order Created
                        v
                +----------------+
                |  kafka Broker  |
                |                |
                |  kafka Topic   |
                +----------------+
                  /    |    \
                 /     |     \
                v      v      v
           Payment  Inventory  Notification
           Service   Service     Service
```

### 1. Order Service — Producer

The **Order Service** is responsible for creating orders.

When a customer places an order, it creates an event such as:

```text
Order Created
```

The Order Service acts as the **producer** because it publishes the event to Kafka.

---

### 2. Kafka — Message/Event Broker

The **Kafka** system receives the `Order Created` event from the Order Service.

Kafka acts as the middle layer between the producer and consumers.

Instead of the Order Service directly calling every service:

```text
Order Service → Payment
Order Service → Inventory
Order Service → Notification
```

it publishes the event to Kafka:

```text
Order Service → Kafka
```

---

### 3. Topic — Where the Event Is Published

The event is published to a Kafka **topic**.

For example:

```text
orders
```

So the flow is:

```text
Order Service
      |
      | Order Created
      v
orders topic
```

The topic provides a logical stream/category for related events.

For example, you might have:

```text
orders
payments
notifications
users
```

---

### 4. Payment Service — Consumer

The **Payment Service** consumes the `Order Created` event.

It can use the event to start payment processing.

```text
Order Created
      ↓
Payment Service
      ↓
Process Payment
```

The Payment Service is a **consumer**.

---

### 5. Inventory Service — Consumer

The **Inventory Service** also consumes the order event.

It can use the event to update inventory.

```text
Order Created
      ↓
Inventory Service
      ↓
Update Stock
```

It is also a **consumer**.

---

### 6. Notification Service — Consumer

The **Notification Service** consumes the same type of event.

It can use it to send an email, SMS, or application notification.

```text
Order Created
      ↓
Notification Service
      ↓
Send Notification
```

It is also a **consumer**.

---

## Overall Flow

```text
1. Customer places order
             ↓
2. Order Service creates order
             ↓
3. Order Service publishes "Order Created"
             ↓
4. Kafka receives the event
             ↓
5. Event is available through the orders topic
             ↓
6. Interested services consume the event
             ↓
   ┌─────────┬───────────┬──────────────┐
   ↓         ↓           ↓
Payment   Inventory   Notification      ?
```

### The main concept

> **The Order Service does not need to directly communicate with every downstream service. It publishes an event to Kafka, and interested services consume that event independently.**

For your current learning stage, remember this simple mapping:

```text
Order Service  → Producer
Broker         → server that receives, stores, and serves the event
Kafka Topic    → Event/message stream where the event is published
Payment        → Consumer
Inventory      → Consumer
Notification   → Consumer
```

This is the basic **Producer → Kafka Topic → Consumer** model.
