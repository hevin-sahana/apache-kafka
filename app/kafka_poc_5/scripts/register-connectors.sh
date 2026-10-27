#!/bin/bash
set -e

CONNECT_URL="http://kafka-connect:8083"

until curl -fsS "$CONNECT_URL/connectors" >/dev/null; do
  sleep 3
done

curl -fsS -X PUT \
  "$CONNECT_URL/connectors/elasticsearch-sink/config" \
  -H "Content-Type: application/json" \
  --data @/connectors/elasticsearch-sink.json