#!/usr/bin/env bash
# download-connectors.sh
# Path: <your-flink-cluster>/scripts/download-connectors.sh
#
# Downloads the correct Flink connector JARs for ANY Flink version.
# Usage:
#   ./download-connectors.sh                   # auto-detect from running container
#   ./download-connectors.sh 1.19.1            # specify manually
#   FLINK_VERSION=1.18.1 ./download-connectors.sh

set -euo pipefail

FLINK_VERSION="${1:-${FLINK_VERSION:-}}"
CONNECTORS_DIR="${CONNECTORS_DIR:-./connectors}"
BASE_URL="https://repo1.maven.org/maven2/org/apache/flink"

# Kafka connector version map
kafka_connector_for() {
    case "$1" in
        1.20*) echo "3.3.0-1.20" ;;
        1.19*) echo "3.3.0-1.19" ;;
        1.18*) echo "3.3.0-1.18" ;;
        1.17*) echo "3.2.0-1.17" ;;
        1.16*) echo "3.1.0-1.16" ;;
        *)     echo "3.3.0-$(echo "$1" | cut -d. -f1-2)" ;;
    esac
}

# Auto-detect if not specified
if [[ -z "$FLINK_VERSION" ]]; then
    echo "[INFO] Auto-detecting Flink version from running container..."
    JM=$(docker ps --filter "name=jobmanager" --format "{{.Names}}" | head -1)
    if [[ -n "$JM" ]]; then
        FLINK_VERSION=$(docker exec "$JM" bash -c "flink --version 2>/dev/null | grep -oP '\d+\.\d+\.\d+' | head -1" 2>/dev/null || true)
    fi
fi

if [[ -z "$FLINK_VERSION" ]]; then
    echo "[ERROR] Could not detect Flink version. Specify it:"
    echo "  ./download-connectors.sh 1.19.1"
    exit 1
fi

KAFKA_CONNECTOR=$(kafka_connector_for "$FLINK_VERSION")
mkdir -p "$CONNECTORS_DIR"

echo ""
echo "=== Str:::Lab Studio Connector Downloader ==="
echo "  Flink version  : $FLINK_VERSION"
echo "  Kafka connector: $KAFKA_CONNECTOR"
echo "  Output dir     : $CONNECTORS_DIR"
echo ""

# Remove stale kafka JARs
echo "=== Removing stale kafka connector JARs ==="
TARGET="flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar"
for f in "$CONNECTORS_DIR"/flink-sql-connector-kafka-*.jar; do
    [[ -f "$f" ]] || continue
    if [[ "$(basename "$f")" != "$TARGET" ]]; then
        echo "[REMOVE] $(basename "$f")"
        rm -f "$f"
    fi
done

# Download JARs
download_jar() {
    local url="$1" file="$2" desc="$3"
    local dest="$CONNECTORS_DIR/$file"
    if [[ -f "$dest" ]]; then
        echo "[SKIP]     $file already exists"
    else
        echo "[DOWNLOAD] $desc..."
        if curl -fsSL -o "$dest" "$url"; then
            size=$(du -k "$dest" | cut -f1)
            echo "[OK]       $file  (${size} KB)"
        else
            echo "[ERROR]    Failed to download $file"
            echo "           Check: $url"
            rm -f "$dest"
        fi
    fi
}

download_jar \
    "$BASE_URL/flink-sql-connector-kafka/$KAFKA_CONNECTOR/flink-sql-connector-kafka-$KAFKA_CONNECTOR.jar" \
    "flink-sql-connector-kafka-$KAFKA_CONNECTOR.jar" \
    "Kafka SQL connector"

download_jar \
    "$BASE_URL/flink-sql-avro-confluent-registry/$FLINK_VERSION/flink-sql-avro-confluent-registry-$FLINK_VERSION.jar" \
    "flink-sql-avro-confluent-registry-$FLINK_VERSION.jar" \
    "Avro Confluent Schema Registry format"

download_jar \
    "$BASE_URL/flink-sql-avro/$FLINK_VERSION/flink-sql-avro-$FLINK_VERSION.jar" \
    "flink-sql-avro-$FLINK_VERSION.jar" \
    "Avro core format"

echo ""
echo "=== Connector JARs in $CONNECTORS_DIR ==="
ls -lh "$CONNECTORS_DIR"/*.jar 2>/dev/null || echo "  (none)"

echo ""
echo "=== Next steps ==="
echo "  docker compose down && docker compose build && docker compose up -d"
echo "  docker exec <taskmanager> bash -c 'ls /opt/flink/lib/ | grep kafka'"
echo "  Expected: flink-sql-connector-kafka-$KAFKA_CONNECTOR.jar"
