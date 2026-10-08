from pathlib import Path

SCHEMA_REGISTRY_URL = "http://localhost:8081"
PRODUCT_SCHEMA_SUBJECT = "products-value"
PRODUCT_TOPIC = "products"

BASE_DIR = Path(__file__).resolve().parent
AVRO_SCHEMA_FILE = BASE_DIR / "avro_schema" / "product.avsc"
