import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
PRODUCT_INDEX = os.getenv("PRODUCT_INDEX", "mysql.poc_db.products")

SOURCE_CONFIG_FILE = BASE_DIR / "connectors" / "source" / "mysql-product-source.json"
SINK_CONFIG_FILE = BASE_DIR / "connectors" / "sink" / "elasticsearch-product-sink.json"
