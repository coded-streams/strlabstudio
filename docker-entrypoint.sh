#!/bin/sh
# docker-entrypoint.sh — Str:::lab Studio v1.2.5
# Runs at container startup. Substitutes env vars into nginx template, starts nginx.
# Compatible with: Docker Compose · Kubernetes · docker run
set -e

echo "Str:::lab Studio starting..."
echo "  Gateway:    ${FLINK_GATEWAY_HOST}:${FLINK_GATEWAY_PORT}"
echo "  JobManager: ${JOBMANAGER_HOST}:${JOBMANAGER_PORT}"

# ── Warn if defaults are unchanged ───────────────────────────────────────────
if [ -z "$FLINK_GATEWAY_HOST" ] || [ "$FLINK_GATEWAY_HOST" = "localhost" ]; then
  echo ""
  echo "  WARNING: FLINK_GATEWAY_HOST is not set (defaulting to localhost)."
  echo "  The Studio nginx proxy will point at itself, not your Flink cluster."
  echo "  Pass the correct value: -e FLINK_GATEWAY_HOST=flink-sql-gateway"
  echo ""
fi

# ── DNS resolver ──────────────────────────────────────────────────────────────
# nginx needs an explicit resolver to look up container names at request time.
# We read the nameserver from /etc/resolv.conf — Docker and Kubernetes both
# write the correct value there automatically:
#   Docker Compose / docker run  → 127.0.0.11
#   Kubernetes pod               → cluster DNS IP (e.g. 10.96.0.10)
#   Local / host network         → host DNS server
RESOLVER=$(awk '/^nameserver/ { print $2; exit }' /etc/resolv.conf)
RESOLVER="${RESOLVER:-127.0.0.11}"
export RESOLVER
echo "  DNS resolver: ${RESOLVER}"

# ── UDF JAR storage ───────────────────────────────────────────────────────────
# nginx WebDAV PUT saves uploaded JARs here.
# Must be owned by the nginx worker user, otherwise PUT returns HTTP 500.
mkdir -p /var/www/udf-jars
chown -R nginx:nginx /var/www/udf-jars
chmod 755 /var/www/udf-jars
echo "  JAR storage: /var/www/udf-jars (owned by nginx)"

# ── Write nginx config ────────────────────────────────────────────────────────
# envsubst substitutes ONLY the five listed variables.
# All nginx runtime variables ($host, $uri, $remote_addr, $flink_gw, $flink_jm,
# $request_method, $proxy_add_x_forwarded_for, etc.) are excluded so they pass
# through unchanged into the final nginx config.
envsubst '${RESOLVER} ${FLINK_GATEWAY_HOST} ${FLINK_GATEWAY_PORT} ${JOBMANAGER_HOST} ${JOBMANAGER_PORT}' \
    < /etc/nginx/templates/default.conf.template \
    > /etc/nginx/conf.d/default.conf

echo "  nginx config written."
echo "  Upstream: flink-api → ${FLINK_GATEWAY_HOST}:${FLINK_GATEWAY_PORT}"
echo ""
echo "nginx starting..."
exec "$@"