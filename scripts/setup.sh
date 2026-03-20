#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Str:::Lab Studio — Linux / macOS setup script
# Usage:
#   # Connect to an existing Flink cluster:
#   ./scripts/setup.sh --gateway my-gateway.example.com:8083 --jm my-jm.example.com:8081
#
#   # Quick start with defaults (localhost):
#   ./scripts/setup.sh
# ─────────────────────────────────────────────────────────────────────────────
set -e

IMAGE="codedstreams/strlabstudio:latest"
CONTAINER="strlabstudio"
STUDIO_PORT=3030
GATEWAY_HOST="localhost"
GATEWAY_PORT=8083
JM_HOST="localhost"
JM_PORT=8081

# Parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    --gateway) IFS=':' read -r GATEWAY_HOST GATEWAY_PORT <<< "$2"; shift 2 ;;
    --jm)      IFS=':' read -r JM_HOST JM_PORT <<< "$2"; shift 2 ;;
    --port)    STUDIO_PORT="$2"; shift 2 ;;
    --build)   BUILD=1; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Str:::Lab Studio — Setup                    ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

command -v docker &>/dev/null || { echo "❌  Docker not found. Install from https://docs.docker.com/get-docker/"; exit 1; }

# Remove any existing container
docker rm -f "${CONTAINER}" 2>/dev/null || true

if [[ -n "${BUILD}" ]]; then
  echo "🔨  Building from source ..."
  docker build -t "${IMAGE}" "$(dirname "$0")/.."
else
  echo "📦  Pulling ${IMAGE} ..."
  docker pull "${IMAGE}"
fi

echo "🚀  Starting on port ${STUDIO_PORT} ..."
docker run -d \
  --name "${CONTAINER}" \
  --restart unless-stopped \
  -p "${STUDIO_PORT}:80" \
  -e FLINK_GATEWAY_HOST="${GATEWAY_HOST}" \
  -e FLINK_GATEWAY_PORT="${GATEWAY_PORT}" \
  -e JOBMANAGER_HOST="${JM_HOST}" \
  -e JOBMANAGER_PORT="${JM_PORT}" \
  "${IMAGE}"

echo ""
echo "✅  Str:::Lab Studio is running!"
echo "   Open:       http://localhost:${STUDIO_PORT}"
echo "   Gateway:    ${GATEWAY_HOST}:${GATEWAY_PORT}"
echo "   JobManager: ${JM_HOST}:${JM_PORT}"
echo ""
echo "   Stop:  docker stop ${CONTAINER}"
echo "   Logs:  docker logs -f ${CONTAINER}"
echo ""

command -v xdg-open &>/dev/null && xdg-open "http://localhost:${STUDIO_PORT}" &
command -v open &>/dev/null && open "http://localhost:${STUDIO_PORT}" &
true
