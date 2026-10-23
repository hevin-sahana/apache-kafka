import os
from collections import Counter, deque
from threading import Lock

_lock = Lock()

_total_events = 0
_event_counts = Counter()
_product_counts = Counter()
_recent_events = deque(maxlen=100)
_product_names = {}


def record_event(event: dict) -> None:
    """Update dashboard metrics for one consumed Kafka event."""
    global _total_events

    with _lock:
        _total_events += 1

        event_type = event.get("event_type", "unknown")
        product_id = event.get("product_id")
        product_name = event.get("product_name")

        _event_counts[event_type] += 1

        if product_id:
            _product_counts[str(product_id)] += 1

            if product_name:
                _product_names[product_id] = product_name

        _recent_events.appendleft(dict(event))

        print(
            f"CONSUMER PID={os.getpid()}, "
            f"total_events={_total_events}"
        )


def get_dashboard_snapshot() -> dict:
    """Return a thread-safe snapshot of current dashboard metrics."""
    with _lock:
        print(
            f"CONSUMER PID={os.getpid()}, "
            f"total_events={_total_events}"
        )

        return {
            "total_events": _total_events,
            "event_counts": dict(_event_counts),
            "popular_products": [
            {
                "product_id": product_id,
                "product_name": _product_names.get(
                    product_id,
                    f"Product {product_id}"
                ),
                "count": count,
            }
            for product_id, count in _product_counts.most_common(5)
        ],
            "recent_events": list(_recent_events),
        }
