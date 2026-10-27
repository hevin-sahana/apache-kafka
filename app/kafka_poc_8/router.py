import json
import logging

import requests
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import or_
from sqlalchemy.orm import Session
from starlette import status

from app.kafka_poc_8 import service
from app.kafka_poc_8.connectors import connector_service, elasticsearch_service
from app.kafka_poc_8.connectors.connector_service import get_connectors, \
    get_connector_status, KAFKA_CONNECT_URL
from app.kafka_poc_8.db.database import get_db
from app.kafka_poc_8.product_model import Product
from app.kafka_poc_8.schemas import ProductCreate, ProductResponse
from app.kafka_poc_8.service import create_product, search_products_db
from app.kafka_poc_8.utils import SOURCE_CONFIG_FILE, SINK_CONFIG_FILE, \
    PRODUCT_INDEX, ELASTICSEARCH_URL, BASE_DIR
from app.utility.logging_config import configure_logging

router = APIRouter(tags=["kafka_poc_8"])
configure_logging("kafka_poc_8")
logger = logging.getLogger(__name__)


@router.post("/create-product", status_code=status.HTTP_201_CREATED,
             response_model=ProductResponse)
def create_product_endpoint(request: ProductCreate,
                            db: Session = Depends(get_db)):
    logger.info(
        f"Product API request received : product: {request}")
    return create_product(db, request)


@router.put("/products/{product_id}")
def update_product_api(
        product_id: int,
        request: ProductCreate,
        db: Session = Depends(get_db)
):
    product = service.update_product(
        db,
        product_id,
        request
    )

    return {
        "message": "Product updated successfully",
        "product": product
    }


@router.delete("/products/{product_id}")
def delete_product_api(
        product_id: int,
        db: Session = Depends(get_db)
):
    return service.delete_product(
        db,
        product_id
    )


# -------------------------------------------------
#   mysql  Connectors
# -------------------------------------------------
@router.post("/connectors/mysql")
def create_mysql_connector():
    try:
        with open(SOURCE_CONFIG_FILE, "r") as file:
            connector_config = json.load(file)

        return connector_service.register_connector(connector_config)

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="MySQL connector config file not found"
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON in MySQL connector config"
        )


@router.put("/connectors/mysql")
def update_mysql_connector():
    try:
        with SOURCE_CONFIG_FILE.open("r") as file:
            connector_data = json.load(file)

        name = connector_data["name"]
        config = connector_data["config"]

        return connector_service.update_connector(
            name,
            config
        )

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="mysql-product-source.json not found"
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON in mysql-product-source.json"
        )

    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="JSON must contain a 'config' object"
        )


# -------------------------------------------------
#   Elastic-search   Connectors
# -------------------------------------------------
@router.post("/connectors/elasticsearch")
def create_elasticsearch_connector():
    try:
        with open(SINK_CONFIG_FILE, "r") as file:
            connector_config = json.load(file)

        return connector_service.register_connector(connector_config)

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=" Elastic-search connector config file not found"
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON in Elastic-search connector config"
        )


@router.put("/connectors/elasticsearch")
def update_elasticsearch_connector():
    try:
        with SINK_CONFIG_FILE.open("r") as file:
            connector_data = json.load(file)

        name = connector_data["name"]
        config = connector_data["config"]

        return connector_service.update_connector(
            name,
            config
        )

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="elasticsearch-product-sink.json not found"
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON in elasticsearch-product-sink.json"
        )

    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="JSON must contain a 'config' object"
        )


@router.get("/elasticsearch/indices")
def list_elasticsearch_indices():
    return elasticsearch_service.list_elasticsearch_indices()


@router.get("/elasticsearch/{index_name}/documents")
def get_elasticsearch_documents(index_name: str):
    return elasticsearch_service.get_elasticsearch_documents(index_name)


# -------------------------------------------------
#   Get  Connectors
# -------------------------------------------------


@router.get("/connectors")
def list_connectors():
    try:
        return {
            "connectors": get_connectors()
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@router.get("/connectors/{name}/status")
def connector_status(name: str):
    try:
        return get_connector_status(name)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@router.get("/products/all")
def get_all_products():
    return elasticsearch_service.get_all_products()


@router.get("/products/search")
def search_products(
        query: str = "",
        db: Session = Depends(get_db)
):
    q = (query or "").strip()

    if not q:
        search_query = {
            "query": {
                "match_all": {}
            }
        }
    else:
        search_query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "wildcard": {
                                "name": {
                                    "value": f"*{q.lower()}*",
                                    "case_insensitive": True
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "category": {
                                    "value": f"*{q.lower()}*",
                                    "case_insensitive": True
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "description": {
                                    "value": f"*{q.lower()}*",
                                    "case_insensitive": True
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }

    try:
        response = requests.post(
            f"{ELASTICSEARCH_URL}/{PRODUCT_INDEX}/_search",
            json=search_query,
            timeout=10
        )

        response.raise_for_status()

        data = response.json()

        # Total count
        hits_total = data.get("hits", {}).get("total", 0)

        if isinstance(hits_total, dict):
            total = hits_total.get("value", 0)
        else:
            total = hits_total

        products = []

        for hit in data.get("hits", {}).get("hits", []):
            source = hit.get("_source") or {}

            # Case 1:
            # Raw Debezium document
            #
            # {
            #   "before": null,
            #   "after": {
            #       "id": 1,
            #       "name": "Wireless Headphones"
            #   }
            # }
            if isinstance(source.get("after"), dict):
                product = source["after"]

            # Case 2:
            # Debezium payload format
            #
            # {
            #   "payload": {
            #       "after": {
            #           ...
            #       }
            #   }
            # }
            elif (
                    isinstance(source.get("payload"), dict)
                    and isinstance(source["payload"].get("after"), dict)
            ):
                product = source["payload"]["after"]

            # Case 3:
            # Current Elasticsearch format
            #
            # {
            #   "id": 1,
            #   "name": "Wireless Headphones",
            #   "price": 149.99
            # }
            else:
                product = source

            products.append({
                "id": product.get("id"),
                "name": product.get("name", "N/A"),
                "description": product.get("description", "N/A"),
                "price": product.get("price", 0),
                "quantity": product.get("quantity", 0),
                "category": product.get("category", "N/A")
            })

        return {
            "source": "elasticsearch",
            "total": total,
            "products": products
        }

    except requests.RequestException as e:
        logger.error(
            f"Elasticsearch search failed: {e}"
        )

        try:
            search_pattern = f"%{q}%"

            products_db = (
                db.query(Product)
                .filter(
                    or_(
                        Product.name.ilike(search_pattern),
                        Product.category.ilike(search_pattern),
                        Product.description.ilike(search_pattern)
                    )
                )
                .all()
            )

            return {
                "source": "mysql",
                "total": len(products_db),
                "products": [
                    {
                        "id": product.id,
                        "name": product.name,
                        "description": product.description,
                        "price": product.price,
                        "quantity": product.quantity,
                        "category": product.category
                    }
                    for product in products_db
                ]
            }

        except Exception as mysql_error:
            logger.error(
                f"MySQL fallback search failed: {mysql_error}"
            )

            raise HTTPException(
                status_code=500,
                detail="Both Elasticsearch and MySQL search failed"
            )


@router.get("/search-page", response_class=FileResponse)
def get_search_page():
    search_html_path = BASE_DIR / "templates" / "search.html"
    return FileResponse(search_html_path)


# -------------------------------------------------
#   restart Connectors
# -------------------------------------------------
@router.post("/kafka-connect/restart/{task_id}")
def restart_elasticsearch_task(task_id: int):
    # Get all connectors
    connectors = connector_service.get_connectors()

    # Find Elasticsearch sink connector
    connector_name = None

    for name in connectors:
        config_response = requests.get(
            f"{KAFKA_CONNECT_URL}/connectors/{name}/config",
            timeout=10
        )

        config_response.raise_for_status()

        config = config_response.json()

        if (
                config.get("connector.class")
                == "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector"
        ):
            connector_name = name
            break

    if not connector_name:
        raise HTTPException(
            status_code=404,
            detail="Elasticsearch sink connector not found"
        )

    # Restart task
    restart_url = (
        f"{KAFKA_CONNECT_URL}/connectors/"
        f"{connector_name}/tasks/{task_id}/restart"
    )

    restart_response = requests.post(
        restart_url,
        timeout=10
    )

    if restart_response.status_code not in (200, 204):
        raise HTTPException(
            status_code=500,
            detail=f"Failed to restart task: {restart_response.text}"
        )

    return {
        "message": "Kafka Connect task restarted",
        "connector": connector_name,
        "task_id": task_id
    }


@router.post("/kafka-connect/restart-sink-connectors")
def restart_sink_connector():
    try:
        with SINK_CONFIG_FILE.open("r") as file:
            connector_data = json.load(file)

        name = connector_data["name"]

        return connector_service.restart_connector(
            name
        )

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="elasticsearch-product-sink.json not found"
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON in elasticsearch-product-sink.json"
        )

    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="JSON must contain a 'config' object"
        )


@router.post("/kafka-connect/restart-source-connectors")
def restart_source_connector():
    try:
        with SOURCE_CONFIG_FILE.open("r") as file:
            connector_data = json.load(file)

        name = connector_data["name"]

        return connector_service.restart_connector(
            name
        )

    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="mysql-product-source.json not found"
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON in mysql-product-source.json"
        )

    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="JSON must contain a 'config' object"
        )
