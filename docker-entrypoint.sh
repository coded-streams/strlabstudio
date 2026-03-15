#!/bin/sh
# docker-entrypoint.sh — substitutes env vars into nginx config at runtime
set -e

echo "Str:::lab Studio starting..."
echo "  Gateway:    ${FLINK_GATEWAY_HOST}:${FLINK_GATEWAY_PORT}"
echo "  JobManager: ${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

# envsubst replaces ${FLINK_GATEWAY_HOST} etc. in the .template file
envsubst '${FLINK_GATEWAY_HOST} ${FLINK_GATEWAY_PORT} ${JOBMANAGER_HOST} ${JOBMANAGER_PORT}' \
    < /etc/nginx/templates/default.conf.template \
    > /etc/nginx/conf.d/default.conf

echo "nginx config written. Starting nginx..."
exec "$@"