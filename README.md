# Str:::lab Studio

> A zero-dependency, modular browser SQL Studio for Flink SQL pipelines — built for engineers who want a real query interface without leaving their laptop.

![License](https://img.shields.io/badge/license-Apache%202.0-green)
![Flink](https://img.shields.io/badge/Flink%20SQL%20Gateway-1.19%2B-orange)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![Version](https://img.shields.io/badge/version-v1.2.4-teal)

> **Apache Flink** is a trademark of the Apache Software Foundation.
> Str:::lab Studio is an independent open-source project that uses the Flink SQL Gateway REST API.
> It is not affiliated with or endorsed by the Apache Software Foundation.

---

## What is it?

Str:::lab Studio is a self-hosted web IDE that connects to the **Flink SQL Gateway** and lets you:

- Write and run Flink SQL in a multi-tab editor with **live streaming results from Kafka**
- Visualise running job DAGs with real-time metrics, fault highlighting, and animated edges
- Manage multiple sessions — each with its own isolated workspace (tabs, logs, history, jobs)
- Monitor cluster health: backpressure, checkpoints, slot utilisation, JVM heap, records/s
- Connect to Kafka with or without Schema Registry, sink to S3/MinIO, run ML inference pipelines
- Manage **User-Defined Functions** — SQL UDFs, JAR upload to cluster, Maven/Gradle build config
- Organise work as named **Projects** — save, load, run, and export your pipelines
- Use **Admin Session** for full cluster visibility, cross-session oversight, and professional reports
- Generate PDF reports — standard session reports, or admin-grade Technical / Business reports

---

## What do I need to run this?

The only hard requirement is a running **Flink SQL Gateway**. The Studio talks to the Gateway REST API exclusively — it does not connect directly to Kafka, ZooKeeper, or any other infrastructure.

| What you have | What to do |
|---|---|
| Nothing yet | `git clone` + `docker compose up -d` — starts everything |
| Flink cluster, no SQL Gateway | Add `flink-sql-gateway` to your compose (snippet in [Option 3](#option-3--i-have-a-flink-cluster-but-no-sql-gateway)) |
| Flink cluster + SQL Gateway | Add Studio only — [Option 2](#option-2--i-already-have-a-flink-cluster-and-sql-gateway) |
| Cloud Flink (Confluent, Ververica, AWS) | [Option 5](#option-5--cloud--remote-flink) — Direct Gateway or Remote mode with token |
| Kubernetes | [Option 6](#option-6--kubernetes) — Helm operator |

> **No CORS proxy required.** The Studio image includes nginx which proxies all browser requests to the gateway and adds CORS headers itself. A separate CORS proxy (`flink-gateway-cors-proxy`) is provided in the repo but is **optional** — you only need it if you want browsers to call the gateway directly on port 8084 without going through the Studio.

---

## Quickstart

### Option 1 — Start everything from scratch

```bash
git clone https://github.com/coded-streams/strlabstudio
cd strlabstudio
docker compose up -d
open http://localhost:3030
```

Starts: JobManager, TaskManagers, SQL Gateway, Studio, and an optional CORS proxy.
Select **Via Studio** on the connect screen — done.

---

### Option 2 — I already have a Flink cluster and SQL Gateway

Add Studio to your existing `docker-compose.yml`. Point it at your gateway and jobmanager.
**No CORS proxy needed.** Via Studio mode routes through the Studio nginx which handles CORS.

```yaml
services:

  flink-studio:
    image: codedstreams/strlabstudio:latest
    container_name: flink-studio
    restart: unless-stopped
    ports:
      - "3030:80"
    environment:
      FLINK_GATEWAY_HOST: flink-sql-gateway      # your gateway container name
      FLINK_GATEWAY_PORT: "8083"
      JOBMANAGER_HOST:    your-jobmanager         # your jobmanager container name
      JOBMANAGER_PORT:    "8081"
    volumes:
      - udf-jars:/var/www/udf-jars               # required for UDF JAR upload
    networks:
      - your-network                             # same network as your cluster

volumes:
  udf-jars:

networks:
  your-network:
    external: true                               # joins your existing network
```

For UDF JAR upload also add `- udf-jars:/var/www/udf-jars` to your `flink-sql-gateway` volumes.
See the [UDF JAR Upload](#udf-jar-upload) section.

```bash
docker compose up -d flink-studio
open http://localhost:3030
```

---

### Option 3 — I have a Flink cluster but no SQL Gateway

The SQL Gateway is the API layer between the Studio and your Flink cluster. Add it alongside the Studio:

```yaml
services:

  flink-sql-gateway:
    image: flink:1.19.1-scala_2.12-java11
    container_name: flink-sql-gateway
    depends_on:
      your-jobmanager:
        condition: service_healthy
    command: >
      /bin/bash -c "
        cp /opt/flink/plugins/connectors/*.jar /opt/flink/lib/ 2>/dev/null || true &&
        exec /opt/flink/bin/sql-gateway.sh start-foreground
      "
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: your-jobmanager
        sql-gateway.endpoint.rest.address: 0.0.0.0
        sql-gateway.endpoint.rest.port: 8083
    volumes:
      - ./connectors:/opt/flink/plugins/connectors
      - udf-jars:/var/www/udf-jars
    networks:
      - your-network
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://localhost:8083/v1/info || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 20
      start_period: 45s

  flink-studio:
    image: codedstreams/strlabstudio:latest
    container_name: flink-studio
    depends_on:
      flink-sql-gateway:
        condition: service_healthy
    ports:
      - "3030:80"
    environment:
      FLINK_GATEWAY_HOST: flink-sql-gateway
      FLINK_GATEWAY_PORT: "8083"
      JOBMANAGER_HOST:    your-jobmanager
      JOBMANAGER_PORT:    "8081"
    volumes:
      - udf-jars:/var/www/udf-jars
    networks:
      - your-network

volumes:
  udf-jars:
```

Put your Kafka/Flink connector JARs in `./connectors/`. They are copied to `/opt/flink/lib/` at Gateway startup.

---

### Option 4 — Standalone `docker run`

```bash
docker run -d -p 3030:80 \
  --name flink-studio \
  --network <your-flink-network> \
  -e FLINK_GATEWAY_HOST=flink-sql-gateway \
  -e FLINK_GATEWAY_PORT=8083 \
  -e JOBMANAGER_HOST=your-jobmanager \
  -e JOBMANAGER_PORT=8081 \
  codedstreams/strlabstudio:latest
```

`--network` is required for **Via Studio** mode. Without it, nginx cannot resolve the gateway hostname by container name and returns HTTP 500. Use **Direct Gateway** mode if you cannot join the network.

> UDF JAR upload requires a shared Docker named volume between Studio and the gateway. Use docker compose if you need that feature.

---

### Option 5 — Cloud / Remote Flink

Run Studio anywhere and point it at your remote gateway:

```bash
docker run -d -p 3030:80 codedstreams/strlabstudio:latest
open http://localhost:3030
```

On the connect screen select **Remote / Cloud**, enter your gateway URL (e.g. `https://your-gateway.example.com`), and provide your Bearer token or Basic auth credentials.

Works with: Confluent Cloud, Ververica Platform, Amazon Managed Service for Apache Flink, or any self-hosted gateway with a public endpoint.

---

### Option 6 — Kubernetes

```bash
helm repo add strlabstudio https://coded-streams.github.io/strlabstudio-operator/charts
helm install strlabstudio-operator strlabstudio/strlabstudio-operator \
  --namespace flinksql-system --create-namespace
```

Apply a `StrlabStudio` custom resource and the operator manages the Deployment, Service, and PVC:

```yaml
apiVersion: codedstreams.io/v1alpha1
kind: StrlabStudio
metadata:
  name: my-studio
  namespace: flink
spec:
  image: codedstreams/strlabstudio:latest
  gateway:
    host: flink-sql-gateway     # Kubernetes Service name
    port: 8083
  jobmanager:
    host: flink-jobmanager
    port: 8081
  service:
    type: ClusterIP
    port: 80
```

**The operator is unaffected by any changes in v1.2.4.** The same 4 env vars are used. CORS is irrelevant in-cluster — all traffic is same-network. The `udf-jars` volume is provisioned as a `ReadWriteMany` PVC shared between the Studio pod and the Gateway pod. No CORS proxy is needed or deployed.

---

## How the Studio connects — architecture

```
Browser (http://localhost:3030)
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│  flink-studio  (codedstreams/strlabstudio:latest)               │
│                                                                 │
│  nginx serves the IDE at port 80                                │
│  docker-entrypoint.sh writes nginx.conf at container startup    │
│                                                                 │
│  /flink-api/*       → proxy → FLINK_GATEWAY_HOST:PORT           │
│                               + CORS headers added by nginx     │
│  /jobmanager-api/*  → proxy → JOBMANAGER_HOST:PORT              │
│  /udf-jars/         → WebDAV PUT → /var/www/udf-jars/           │
└─────────────────────────────────────────────────────────────────┘
         │
         ├─── flink-sql-gateway:8083      SQL sessions, statements, results
         │           └─── your-jobmanager:8081   job submission
         │                     ├── taskmanager-1
         │                     └── taskmanager-2
         │
         └─── [optional] flink-gateway-cors-proxy:8084
                    Only for Direct Gateway mode — not required for Via Studio
```

---

## Connection Modes

| Mode | When to use | How it works |
|------|-------------|--------------|
| **Via Studio** | Docker Compose or Kubernetes — Studio on same network as cluster | Browser → Studio nginx → SQL Gateway. Same-origin. No CORS issues. |
| **Direct Gateway** | Studio not on same network, or browser → gateway directly | Browser → `flink-gateway-cors-proxy:8084`. Needs CORS proxy. |
| **Remote / Cloud** | Confluent Cloud, Ververica, AWS, any remote cluster | Browser → your cloud gateway URL. Supports Bearer token + Basic auth. |
| **🛡 Admin Session** | Platform operators — full cluster visibility | Any of the above + admin passcode. |

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLINK_GATEWAY_HOST` | `localhost` | Container name or hostname of the Flink SQL Gateway |
| `FLINK_GATEWAY_PORT` | `8083` | SQL Gateway REST port |
| `JOBMANAGER_HOST` | `localhost` | Container name or hostname of the Flink JobManager |
| `JOBMANAGER_PORT` | `8081` | JobManager REST API port |

Substituted into `nginx.conf` at container startup by `docker-entrypoint.sh`. The Dockerfile defaults to `localhost` — always pass correct values for Docker and Kubernetes deployments.

---

## What is the CORS proxy and do I need it?

`nginx/flink-cors.conf` and the `flink-gateway-cors-proxy` service are provided in this repo as an **optional** component.

**You do NOT need it if** you use Via Studio connection mode (the default). The Studio nginx proxies all requests to the gateway and adds CORS headers itself. This covers the vast majority of use cases.

**You need it only if** you want a browser tab to call the SQL Gateway directly on port 8084 without going through the Studio. This is the "Direct Gateway" connection mode. In that case, add the service to your compose and mount `./nginx/flink-cors.conf`.

**On Kubernetes** — CORS is not relevant. In-cluster traffic is same-origin. The proxy is never deployed by the operator.

---

## UDF JAR Upload

Upload JARs directly from the IDE — no SSH, no `docker cp`.

**How it works:**
1. Open **⨍ UDFs → ⬆ Upload JAR** — drag your shaded JAR and click Upload
2. Studio nginx saves the file to `/var/www/udf-jars/` via WebDAV PUT
3. Studio runs `ADD JAR '/var/www/udf-jars/yourjar.jar'` in the active Gateway session
4. Go to **＋ Register UDF** → enter your class path → Execute Registration

**Requirement:** the `udf-jars` named volume must be mounted on **both** `flink-studio` and `flink-sql-gateway` at the same path:

```yaml
flink-studio:
  volumes:
    - udf-jars:/var/www/udf-jars

flink-sql-gateway:
  volumes:
    - udf-jars:/var/www/udf-jars

volumes:
  udf-jars:
```

> `ADD JAR` runs inside the Gateway JVM and reads from the gateway container's own filesystem. The shared volume makes `/var/www/udf-jars/` identical on both containers. Flink 1.19 without Hadoop cannot use `ADD JAR 'http://...'` — the local path approach is the correct solution.

---

## Admin Session

Connect using the **🛡 Admin** button. Default passcode: `admin1234` — change it immediately via the 🛡 badge in the topbar after first login.

| Feature | Regular Session | Admin Session |
|---------|----------------|---------------|
| Jobs visible | Own session only | All cluster jobs |
| Cancel job | Own jobs only | Any job |
| Session Inspector | — | Full cross-session breakdown |
| Audit trail | — | Timestamped log of admin actions |
| Report type | Standard session report | Technical or Business/Management PDF |

---

## Connector JARs

Connector JARs must match your Flink version exactly. Wrong version → `NoClassDefFoundError` on every INSERT job.

| Flink version | Kafka connector JAR |
|---|---|
| 1.20.x | `flink-sql-connector-kafka-3.3.0-1.20.jar` |
| 1.19.x ← recommended | `flink-sql-connector-kafka-3.3.0-1.19.jar` |
| 1.18.x | `flink-sql-connector-kafka-3.3.0-1.18.jar` |
| 1.17.x | `flink-sql-connector-kafka-3.2.0-1.17.jar` |

Place JARs in `./connectors/`. The compose file copies them to `/opt/flink/lib/` at Gateway startup.

---

## Recommended session setup

Available as a snippet: **CONFIG → ⚡ Recommended Streaming Config**

```sql
SET 'execution.runtime-mode'              = 'streaming';
SET 'parallelism.default'                 = '2';
SET 'execution.checkpointing.interval'    = '10000';
SET 'execution.checkpointing.mode'        = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'                = '3600000';
SET 'table.exec.source.idle-timeout'      = '10000';
SET 'table.exec.mini-batch.enabled'       = 'true';
SET 'table.exec.mini-batch.allow-latency' = '500 ms';
SET 'table.exec.mini-batch.size'          = '5000';
SET 'table.optimizer.agg-phase-strategy'  = 'TWO_PHASE';
```

---

## Changelog

### v1.2.4 (current)
- **Studio image is now self-contained** — replaces `nginx:alpine` + bind-mounted HTML. The `codedstreams/strlabstudio:latest` image includes nginx, proxy config, WebDAV JAR storage, and entrypoint. No bind mounts needed — just 4 env vars.
- **No CORS proxy required** — Studio nginx adds CORS headers itself when proxying to the gateway. Engineers with just a raw SQL Gateway + Flink cluster need nothing else.
- **CORS proxy is now optional** — `flink-gateway-cors-proxy` is still provided in the repo for Direct Gateway mode but is clearly marked optional and removed from the critical startup path.
- **UDF JAR upload via shared Docker volume** — `ADD JAR` uses a local path via a named volume. No Hadoop required.
- **Kubernetes operator unaffected** — same env var pattern, operator handles DNS and PVC. No changes to operator CRDs or Helm chart needed.
- **nginx startup crash fixed** — variable upstreams + DNS resolver from `/etc/resolv.conf`. Works in Docker (127.0.0.11), Kubernetes (cluster DNS), and local environments.

### v1.2.0
- UDF Manager overhaul — 3-step guided wizard, ClassNotFoundException diagnosis

### v1.0.22
- Brand rename to Str:::lab Studio; Apache Flink trademark attribution added

### v1.0.21
- Project Manager, JAR upload fix, Maven/Gradle config generator

### v1.0.19 – v1.0.20
- Admin Session, UDF Manager, Colour Describe, duplicate submission guard

### v1.0.1 – v1.0.18
- Initial build: SQL editor, sessions, results, Job Graph, performance, themes, workspace

---

## Contributing

1. Fork the repo and create a feature branch.
2. Each JS module has a single clear responsibility — keep it that way.
3. No build step, no bundler, no dependencies — plain HTML/CSS/JS only.
4. Test against Flink SQL Gateway 1.17+.
5. Open a PR with a clear description of what changed and why.

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
Created by **Nestor A. A** · [coded-streams](https://github.com/coded-streams)