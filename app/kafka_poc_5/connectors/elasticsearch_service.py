import requests

from app.kafka_poc_5.utils import ELASTICSEARCH_URL, PRODUCT_INDEX


def get_elasticsearch_documents(index_name: str):
    response = requests.get(
        f"{ELASTICSEARCH_URL}/{index_name}/_search",
        params={
            "size": 100
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def list_elasticsearch_indices():
    response = requests.get(
        f"{ELASTICSEARCH_URL}/_cat/indices?format=json",
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def get_all_products():
    response = requests.post(
        f"{ELASTICSEARCH_URL}/{PRODUCT_INDEX}/_search",
        json={
            "query": {
                "match_all": {}
            }
        },
        timeout=10,
    )

    response.raise_for_status()

    data = response.json()
    hits_total = data["hits"]["total"]
    total = hits_total["value"] if isinstance(hits_total, dict) else hits_total

    return {
        "total": total,
        "products": [
            hit["_source"]
            for hit in data["hits"]["hits"]
        ],
    }
