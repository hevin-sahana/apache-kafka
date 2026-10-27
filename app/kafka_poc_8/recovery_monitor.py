
import time
import socket
from collections import defaultdict, deque

import requests

CONNECT_URL = "http://localhost:8083"

CHECK_INTERVAL_SECONDS = 15
RESTART_COOLDOWN_SECONDS = 60
MAX_RESTARTS_PER_15_MINUTES = 5

# Change these values if your host ports or container names differ.
CONNECTORS = {
    "elasticsearch-product-sink": {
        "health_check": ("localhost", 9200),
    },
    "mysql-product-source": {
        "health_check": ("localhost", 13306),
    },
}

session = requests.Session()
restart_history = defaultdict(deque)


def service_is_reachable(host, port):
    """Check whether the destination service accepts TCP connections."""
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def get_connector_status(connector_name):
    url = f"{CONNECT_URL}/connectors/{connector_name}/status"
    response = session.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


def restart_is_allowed(restart_key):
    now = time.monotonic()
    history = restart_history[restart_key]

    while history and now - history[0] > 900:
        history.popleft()

    if len(history) >= MAX_RESTARTS_PER_15_MINUTES:
        return False

    if history and now - history[-1] < RESTART_COOLDOWN_SECONDS:
        return False

    return True


def request_restart(connector_name, endpoint, restart_key):
    if not restart_is_allowed(restart_key):
        print(f"[{connector_name}] Restart limited; waiting.")
        return

    url = f"{CONNECT_URL}/connectors/{connector_name}/{endpoint}"

    try:
        response = session.post(url, timeout=10)

        if response.status_code in (200, 202, 204):
            restart_history[restart_key].append(time.monotonic())
            print(
                f"[{connector_name}] Restart requested: "
                f"{endpoint} (HTTP {response.status_code})"
            )
        else:
            print(
                f"[{connector_name}] Restart request failed: "
                f"HTTP {response.status_code}: {response.text}"
            )

    except requests.RequestException as exc:
        print(f"[{connector_name}] Restart request error: {exc}")


def monitor_connector(connector_name, config):
    host, port = config["health_check"]

    # Do not attempt recovery while the destination is unavailable.
    if not service_is_reachable(host, port):
        print(
            f"[{connector_name}] Dependency "
            f"{host}:{port} is unavailable; postponing recovery."
        )
        return

    try:
        status = get_connector_status(connector_name)
    except requests.RequestException as exc:
        print(f"[{connector_name}] Cannot read status: {exc}")
        return

    connector_state = status.get("connector", {}).get(
        "state", "UNKNOWN"
    )
    tasks = status.get("tasks", [])

    # Never override an intentional pause or stop.
    if connector_state in ("PAUSED", "STOPPED"):
        print(f"[{connector_name}] State={connector_state}; skipping.")
        return

    # Handle a connector-level failure with the same restart limits.
    if connector_state == "FAILED":
        request_restart(
            connector_name,
            "restart",
            f"{connector_name}:connector",
        )
        return

    failed_tasks = [
        task for task in tasks
        if task.get("state") == "FAILED"
    ]

    if not failed_tasks:
        states = [task.get("state", "UNKNOWN") for task in tasks]
        print(
            f"[{connector_name}] Connector={connector_state}, "
            f"tasks={states}"
        )
        return

    for task in failed_tasks:
        task_id = task.get("id")
        if task_id is None:
            continue

        request_restart(
            connector_name,
            f"tasks/{task_id}/restart",
            f"{connector_name}:task:{task_id}",
        )


def monitor_once():
    for connector_name, config in CONNECTORS.items():
        monitor_connector(connector_name, config)


if __name__ == "__main__":
    print("Monitoring Kafka Connect source and sink connectors.")

    while True:
        monitor_once()
        time.sleep(CHECK_INTERVAL_SECONDS)
