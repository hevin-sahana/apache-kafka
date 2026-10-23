from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
AVRO_SCHEMA_FILE = BASE_DIR / "avro" / "clickstream_event.avsc"
CLICKSTREAM_SCHEMA_SUBJECT = "clickstream-events-value"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
