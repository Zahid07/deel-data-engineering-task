#!/bin/sh

CONNECT_URL="http://kafka-connect:8083"
CONNECTORS_DIR="/connectors"
MAX_RETRIES=30
RETRY_COUNT=0

echo "Waiting for Kafka Connect to be ready..."
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connectors" 2>/dev/null)
  if [ "$HTTP_CODE" = "200" ]; then
    echo "Kafka Connect is ready (HTTP $HTTP_CODE)"
    break
  fi
  echo "Kafka Connect not ready yet (HTTP $HTTP_CODE), retrying in 5s... ($RETRY_COUNT/$MAX_RETRIES)"
  RETRY_COUNT=$((RETRY_COUNT + 1))
  sleep 5
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "Failed to connect to Kafka Connect after $MAX_RETRIES attempts"
  exit 1
fi

echo "Kafka Connect is ready. Registering connectors..."

for CONNECTOR_FILE in "${CONNECTORS_DIR}"/*.json; do
  CONNECTOR_NAME=$(basename "${CONNECTOR_FILE}" .json)
  echo "Creating connector from ${CONNECTOR_NAME}..."

  RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @"${CONNECTOR_FILE}")

  HTTP_CODE=$(echo "${RESPONSE}" | tail -n1)
  BODY=$(echo "${RESPONSE}" | sed '$d')

  if [ "${HTTP_CODE}" = "201" ]; then
    echo "Connector ${CONNECTOR_NAME} created successfully."
  elif [ "${HTTP_CODE}" = "409" ]; then
    echo "Connector ${CONNECTOR_NAME} already exists, skipping."
  else
    echo "Failed to create connector ${CONNECTOR_NAME} (HTTP ${HTTP_CODE}): ${BODY}"
  fi
done

echo ""
echo "All connectors processed. Current status:"
curl -s "${CONNECT_URL}/connectors?expand=status"
