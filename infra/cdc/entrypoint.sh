set -e

CONNECT_URL="http://connect:8083"
CONNECTOR_NAME="postgres-cdc-connector"

echo "Waiting for Kafka Connect to be ready..."

until curl -s -o /dev/null -w "%{http_code}" ${CONNECT_URL}/connectors \
    | grep -q "200"
do
    echo "  Connect not ready, retrying..."
    sleep 3
done

echo "Checking connector..."

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  ${CONNECT_URL}/connectors/${CONNECTOR_NAME})

if [ "$HTTP_STATUS" = "200" ]; then
    echo "Connector already exists."
else
    echo "Creating connector..."
    
    JSON_BODY=$(eval "echo \"$(cat /postgres-connector.json | sed 's/"/\\"/g')\"")

    curl -X POST ${CONNECT_URL}/connectors \
      -H "Content-Type: application/json" \
      -d "$JSON_BODY"
fi

echo "Done."