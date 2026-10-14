
# Kafka Multi-Broker Replication POC Configuration

# External listener ports mapped in docker-compose.yml
BOOTSTRAP_SERVERS = (
    "localhost:9095,"
    "localhost:9094,"
    "localhost:9096"
)

# Topic configuration
TOPIC_NAME = "replication-demo"
PARTITIONS = 3
REPLICATION_FACTOR = 3
MIN_IN_SYNC_REPLICAS = 2

# Consumer configuration
CONSUMER_GROUP = "replication-demo-group"
AUTO_OFFSET_RESET = "earliest"
ENABLE_AUTO_COMMIT = False

# Producer configuration
ACKS = "all"
ENABLE_IDEMPOTENCE = True
