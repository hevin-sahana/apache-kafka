# Kafka POC-05 – Debezium MySQL CDC, Kafka Connect & Elasticsearch Sink with Fallback Search

**Apache Kafka 4.0.0 | Kafka Connect | Debezium MySQL CDC | Elasticsearch 7.17 | MySQL 8.0 | Python | FastAPI | SQLAlchemy**

---

## 1. POC Overview

This POC demonstrates an end-to-end event-driven Change Data Capture (CDC) and search indexing pipeline using Apache Kafka, Debezium, Kafka Connect, Elasticsearch, and FastAPI:

1. **MySQL Database (`poc_db`)**: Acts as the primary transactional data store containing the `products` table.
2. **FastAPI Application**: Provides endpoints to create products, manage Kafka Connectors, and perform search requests.
3. **Debezium MySQL Source Connector**: Reads MySQL binary logs in real time, capturing row-level inserts/updates, and streams them as CDC records to the Kafka topic `mysql.poc_db.products`.
4. **Confluent Elasticsearch Sink Connector**: Consumes CDC events from the Kafka topic and indexes documents into Elasticsearch index `mysql.poc_db.products`.
5. **Product Search Web UI & Fallback Search API**: Serves a standalone single-page search web interface (`/poc/06/search-page`) that queries **Elasticsearch** first and automatically falls back to **MySQL Database** if Elasticsearch returns 0 results or is unreachable.

---

## 2. Objectives

- Understand Change Data Capture (CDC) using MySQL binary logging and Debezium.
- Stream database table mutations into Kafka topics without writing custom application producer logic.
- Configure and deploy Kafka Connect Source and Sink connectors using REST APIs.
- Stream events from Kafka to Elasticsearch for full-text search indexing.
- Parse and normalize complex Debezium CDC payload structures (e.g., `_source.after`) and Base64-encoded MySQL decimal fields (e.g. `"Opc="` -> `$149.99`).
- Implement a resilient search service featuring **Elasticsearch as primary search** and **MySQL as automatic fallback**.
- Serve a modular web UI separated into templates (`templates/search.html`) with cross-origin CORS support.

---

## 3. Architecture

```text
  +-----------------------+
  |  FastAPI / Web Client |
  +-----------+-----------+
              |
POST /poc/06/create-product
              |
              v
 +-------------------------+
 |   MySQL DB (poc_db)     |
 |    Table: products      |
 +------------+------------+
              |
     MySQL Binary Log (binlog)
              |
              v
+-------------------------------------+
| Debezium MySQL Source Connector     |
| (Kafka Connect - Port 8083)         |
+------------------+------------------+
                   |
      Topic: mysql.poc_db.products
                   |
                   v
         +-------------------+
         |   Kafka Broker    |
         +---------+---------+
                   |
                   v
+-------------------------------------------+
| Confluent Elasticsearch Sink Connector    |
| (Kafka Connect - Port 8083)               |
+---------------------+---------------------+
                      |
                      v
       +------------------------------+
       | Elasticsearch (Port 9200)    |
       | Index: mysql.poc_db.products |
       +--------------+---------------+
                      ^
                      | 1. Search Query (Primary)
                      |
          +-----------+-----------+
          |  Search API & Web UI  |
          | /poc/06/search-page   |
          +-----------+-----------+
                      |
                      | 2. DB Query (Fallback if ES fails/empty)
                      v
         +-------------------------+
         |   MySQL DB (poc_db)     |
         +-------------------------+
```

---

## 4. Technology Stack

| Technology | Version / Port | Purpose |
|---|---|---|
| **Apache Kafka** | `4.0.0` (Port 9092) | Message broker / Event streaming platform |
| **Kafka Connect** | Port `8083` | Framework for connecting Kafka with external datastores |
| **Debezium MySQL Connector** | `2.7.3.Final` | Captures row-level MySQL binlog changes |
| **Elasticsearch Sink Connector** | Confluent | Streams Kafka topic records to Elasticsearch |
| **Elasticsearch** | `7.17.18` (Port 9200) | Full-text search engine |
| **MySQL Database** | `8.0` (Port 13306) | Relational database (`poc_db`) |
| **FastAPI** | Python | Application backend API |
| **SQLAlchemy** | `2.0` | Python ORM for MySQL DB operations |
| **HTML5 / CSS3 / ES6 JS** | Frontend Web | Standalone web UI for searching products |

---

## 5. Directory Structure

```text
app/kafka_poc_6/
├── db/
│   ├── base.py                   # SQLAlchemy DeclarativeBase
│   └── database.py               # Engine & SessionLocal setup
├── connectors/
│   ├── connector_schema.py       # Pydantic schema for connector payloads
│   ├── connector_service.py      # HTTP client for Kafka Connect REST API
│   ├── source/
│   │   └── mysql-source.json     # Debezium MySQL Source Connector config
│   └── sink/
│       └── es-sink.json          # Elasticsearch Sink Connector config
├── templates/
│   └── search.html               # Web UI for product search (HTML/CSS/JS)
├── product_model.py              # SQLAlchemy Product ORM model
├── product_repository.py         # DB access methods (create & fallback search)
├── schemas.py                    # Pydantic request & response models
├── service.py                    # Business logic wrapper
├── router.py                     # FastAPI endpoints (Products, Connectors, Search, Web UI)
└── README.md                     # Comprehensive POC-05 Documentation
```

---

## 6. What is Change Data Capture (CDC)?

Traditional applications publish event messages manually:

```text
API Request -> Insert to DB -> Call Kafka Producer -> Publish to Topic
```

If the application crashes between inserting into the database and sending the Kafka message, the database and message broker become out of sync.

With **CDC (Change Data Capture)**:
1. The application simply writes to the MySQL Database.
2. The database writes the row modification to its transaction log (MySQL binary log).
3. **Debezium** tail-reads the binary log asynchronously and publishes CDC event messages to Kafka.

```text
API Request -> Insert to DB -> MySQL Binlog -> Debezium -> Kafka Topic
```

This guarantees that every database change is captured without altering application code.

---

## 7. Debezium MySQL Source Connector Configuration

File: `connectors/source/mysql-source.json`

```json
{
  "name": "mysql-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "root123",
    "database.server.id": "184054",
    "topic.prefix": "mysql",
    "database.include.list": "poc_db",
    "table.include.list": "poc_db.products",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
    "schema.history.internal.kafka.topic": "schema-changes.poc_db"
  }
}
```

### Key Parameters:
- `topic.prefix`: `mysql` -> Prepended to database and table names.
- `database.include.list`: `poc_db` -> Specifies the target database.
- `table.include.list`: `poc_db.products` -> Specifies the table to monitor.
- **Generated Kafka Topic**: `mysql.poc_db.products`

---

## 8. Debezium CDC Message Structure

When a row is inserted or updated in MySQL `poc_db.products`, Debezium publishes a JSON message to `mysql.poc_db.products`:

```json
{
  "_source": {
    "before": null,
    "after": {
      "id": 1,
      "name": "Wireless Headphones",
      "price": "Opc=",
      "quantity": 10,
      "category": "Electronics"
    },
    "source": {
      "version": "2.7.3.Final",
      "connector": "mysql",
      "name": "mysql",
      "db": "poc_db",
      "table": "products"
    },
    "op": "c",
    "ts_ms": 1791191024000
  }
}
```

### Explanation of Fields:
- `before`: Previous row state (null for inserts).
- `after`: New row state containing `id`, `name`, `price`, `quantity`, `category`.
- `op`: Operation type (`c` = create/insert, `u` = update, `d` = delete, `r` = read/snapshot).
- `price`: Debezium encodes MySQL `DECIMAL(10,2)` values as **Base64 encoded big-endian bytes** (e.g. `"Opc="` represents decimal `$149.99`).

---

## 9. Elasticsearch Sink Connector Configuration

File: `connectors/sink/es-sink.json`

```json
{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "connection.url": "http://elasticsearch:9200",
    "topics": "mysql.poc_db.products",
    "key.ignore": "false",
    "schema.ignore": "true",
    "behavior.on.malformed.documents": "warn",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"
  }
}
```

### Key Parameters:
- `connection.url`: `http://elasticsearch:9200` -> Elasticsearch cluster URL.
- `topics`: `mysql.poc_db.products` -> Source Kafka topic.
- **Generated Elasticsearch Index**: `mysql.poc_db.products`

---

## 10. API Endpoints Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/poc/06/create-product` | Inserts a new product into MySQL database |
| `POST` | `/poc/06/connectors/mysql` | Deploys Debezium MySQL source connector |
| `POST` | `/poc/06/connectors/elasticsearch` | Deploys Elasticsearch sink connector |
| `GET` | `/poc/06/connectors` | Lists active Kafka Connect connectors |
| `GET` | `/poc/06/connectors/{name}/status` | Checks status of a specific connector |
| `GET` | `/poc/06/products/all` | Retrieves all indexed records from Elasticsearch |
| `GET` | `/poc/06/products/search-with-fallback` | Primary Elasticsearch search with automatic MySQL DB fallback |
| `GET` | `/poc/06/search-page` | Serves the HTML product search web interface |

---

## 11. Dual-Layer Resilient Search Architecture

The search service implements a primary-secondary fallback strategy:

```python
@router.get("/products/search-with-fallback")
def search_products_with_fallback(query: str = "", db: Session = Depends(get_db)):
    q = (query or "").strip()
    
    # Step 1: Try Elasticsearch Primary Search
    try:
        es_q = {"match_all": {}} if not q or q == "*" else {
            "query_string": {
                "query": f"*{q}*",
                "fields": ["*"],
                "analyze_wildcard": True,
                "allow_leading_wildcard": True
            }
        }
        
        response = requests.post(f"{ELASTICSEARCH_URL}/{PRODUCT_INDEX}/_search", json={"query": es_q}, timeout=5)
        if response.status_code == 200:
            data = response.json()
            total = data.get("hits", {}).get("total", {}).get("value", 0)
            if total > 0:
                products = parse_es_hits(data["hits"]["hits"])
                return {"source": "elasticsearch", "total": total, "products": products}
    except Exception as e:
        logger.warning(f"Elasticsearch query failed: {e}. Falling back to database.")

    # Step 2: Fallback to MySQL Database Search
    logger.info(f"Fallback to MySQL DB for query: '{q}'")
    db_items = search_products_db(db, q)
    products = [format_db_product(item) for item in db_items]
    
    return {"source": "database", "total": len(products), "products": products}
```

### Search Flow Behavior:
1. **Primary**: Query Elasticsearch index `mysql.poc_db.products` with wildcard `*query*`.
2. **If ES matches (`total > 0`)**: Return results with `"source": "elasticsearch"`.
3. **If ES returns 0 results OR fails/is down**: Fall back to MySQL database using `search_products_db()` and return results with `"source": "database"`.

---



## 12. Modular Web UI & CORS Integration

The user interface is maintained in a separate template file [`templates/search.html`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/kafka_poc_6/templates/search.html):

- **Origin Auto-Detection**: JavaScript automatically detects if the page was opened from IDE previews (e.g. PyCharm port `63342`) and directs API calls to `http://localhost:8000`.
- **CORS Middleware**: `CORSMiddleware` in [`app/main.py`](file:///home/dev1070/SSL-0061/data-engineering-training/apache-kafka/app/main.py) allows cross-origin requests from frontend previews.
- **Dynamic Source Badges**: Displays **⚡ Found via Elasticsearch** (Green) or **🗄️ Fallback from MySQL Database** (Amber).

---

## 13. Step-by-Step Setup & Execution

### 1. Start Infrastructure Containers

```bash
docker compose up -d
```

Check running services:
```bash
docker ps
```

### 2. Run FastAPI Application

```bash
uvicorn app.main:app --reload --port 8000
```

### 3. Register Connectors

Deploy Debezium MySQL Source Connector:
```bash
curl -X POST "http://localhost:8000/poc/06/connectors/mysql"
```

Deploy Elasticsearch Sink Connector:
```bash
curl -X POST "http://localhost:8000/poc/06/connectors/elasticsearch"
```

Check connector status:
```bash
curl "http://localhost:8000/poc/06/connectors"
```

### 4. Create Sample Products

```bash
curl -X POST "http://localhost:8000/poc/06/create-product" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "Wireless Headphones",
       "price": 149.99,
       "quantity": 10,
       "category": "Electronics"
     }'
```

### 5. Open Search Web Interface

Open your browser to:
```text
http://localhost:8000/poc/06/search-page
```

- Type a search term (e.g. `"Wireless"` or `"Electronics"`).
- Observe results returned with **⚡ Found via Elasticsearch**.
- If Elasticsearch is stopped, search automatically completes with **🗄️ Fallback from MySQL Database**.

---

## 14. Key Takeaways

1. **Zero-Code CDC**: Debezium captures database mutations from MySQL binlogs without adding producer logic in FastAPI endpoints.
2. **Elasticsearch Sink Integration**: Kafka Connect streams Kafka messages straight into Elasticsearch indices.
3. **Resilient Dual-Layer Search**: Primary full-text search with automatic SQL database fallback guarantees maximum query availability.
4. **Clean Code Structure**: Decoupled architecture separating database models, repository queries, connectors, API routes, and HTML templates.
