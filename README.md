# FlinkSQL Studio

> A zero-dependency, modular browser IDE for Apache Flink SQL — built for engineers who want a real query interface without leaving their laptop.

![License](https://img.shields.io/badge/license-MIT-green)
![Flink](https://img.shields.io/badge/Apache%20Flink-1.19%2B-orange)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![Admin](https://img.shields.io/badge/admin-session-yellow)

---

## What is it?

FlinkSQL Studio is a self-hosted web IDE that connects to the **Flink SQL Gateway** and lets you:

- Write and run Flink SQL in a multi-tab editor with **live streaming results from Kafka**
- Visualise running job DAGs with real-time metrics, fault highlighting, and animated edges
- Manage multiple sessions — each session has its own isolated workspace (tabs, logs, history, jobs)
- Monitor cluster health: backpressure, checkpoints, slot utilisation, JVM heap, records/s
- Connect to Kafka with or without Schema Registry, sink to S3/MinIO, and run ML inference pipelines
- Use **Admin Session** for full cluster visibility, cross-session oversight, and professional reports
- Generate PDF reports — standard session reports, or admin-grade Technical / Business reports

---

## Project Structure

```
flinksql-studio/
│
├── index.html              # Entry point — HTML structure + modal markup only (no inline logic)
│
├── css/
│   ├── theme.css           # CSS variables: dark, light, monokai, dracula, nord, contrast
│   ├── layout.css          # Connection screen, topbar, sidebar, editor, results, statusbar
│   └── panels.css          # Performance panel, Job Graph panel, Node Detail modal
│
├── js/
│   ├── state.js            # Global state, constants (MAX_ROWS), SNIPPETS library, api()
│   ├── connection.js       # Connect screen: modes, auth, doConnect(), heartbeat, tips modal,
│   │                       #   admin session, existing session loader, session renewal
│   ├── tabs.js             # Tab add / rename / switch / close
│   ├── editor.js           # SQL editor: line numbers, format, Ctrl+/ comment, keyboard shortcuts
│   ├── execution.js        # runSQL(), pollOperation(), duplicate submission guard, job tagging
│   ├── results.js          # Result table render, pagination, CSV export, stream slot selector
│   ├── log.js              # Log panel, operations list, query history, error parsing
│   ├── catalog.js          # Catalog browser: refresh, tree render, table detail
│   ├── session.js          # Session CRUD, job scoping, cancel confirmation, audit trail,
│   │                       #   admin session detail inspector, duplicate guard helpers
│   ├── perf.js             # KPIs, sparklines, gauges, checkpoints, cluster resources, reports
│   ├── theme.js            # Theme select, applyTheme(), persist
│   ├── workspace.js        # Workspace export/import JSON, restoreWorkspace()
│   ├── modals.js           # Error modal, job list, cancelSelectedJob(), admin sessions overview
│   ├── jobgraph.js         # DAG render, zoom/pan, node detail, live metrics, vertex timeline
│   ├── timeline.js         # Vertex status timeline chart (Gantt-style)
│   ├── misc.js             # togglePerfLive(), filterPerfQueries(), init
│   └── admin.js            # Admin badge, Sessions button, report UI, Flink docs links,
│                           #   query count hook, launchApp/disconnectAll patch
│
├── docs/
│   ├── index.html                  # Documentation site
│   ├── demo-datagen-kafka.sql      # Simple demo pipeline
│   └── full-pipeline.sql           # Full multi-node pipeline
│
├── nginx/studio.conf               # nginx reverse proxy config
├── scripts/
│   ├── setup.sh / setup.ps1        # Quickstart scripts
│   └── download-connectors.sh/.ps1 # Auto-download matching connector JARs
├── k8s/
│   ├── manifests/deployment.yaml
│   └── helm/flinksql-studio/
├── Dockerfile
├── docker-compose.yml
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

---

## Quickstart

```bash
git clone https://github.com/coded-streams/flinksql-studio
cd flinksql-studio
docker compose up -d
open http://localhost:3030
```

On first connect, a **Tips & Concepts** modal walks you through the IDE and key Flink concepts — with SQL code examples for every tip.

---

## Connection Modes

| Mode | When to use | Auth |
|------|-------------|------|
| **Via Studio** | Local Docker — routes through nginx at `localhost:3030` | None |
| **Direct Gateway** | Remote clusters, Confluent Cloud, AWS Managed Flink | Bearer token or Basic |
| **🛡 Admin Session** | Platform operators — full cluster visibility, professional reports | Passcode |

> **Test Connection first** — the IDE auto-fetches existing sessions from the gateway and populates a dropdown so you can reconnect without copy-pasting UUIDs.

---

## Admin Session

Connect using the **🛡 Admin** button on the connect screen and enter the admin passcode (`admin1234` by default — change it immediately after first login via the 🛡 badge in the topbar).

### What admin can do

| Feature | Regular Session | Admin Session |
|---------|----------------|---------------|
| Jobs visible in Job Graph | Own session jobs only | All cluster jobs |
| Cancel job | Own jobs only | Any job on cluster |
| Session Inspector | — | Full cross-session breakdown |
| Audit trail | — | Timestamped log of admin actions |
| Report type | Standard session report | Technical or Business/Management |

### Sessions Panel (Admin view)

When connected as admin, the **Sessions panel** (sidebar) and the **🛡 Sessions** topbar button both show all registered sessions with live counts:

- **Queries run** — total queries executed in that session
- **Jobs** — total submitted / currently running
- **⚠ Admin activity** — red badge appears on sessions where an admin has taken action

Click **🔍 Inspect** on any session to open the **Session Inspector** — a tabbed modal showing:

- **Overview** — stats, session info, open tab names and line counts
- **Queries** — full query history with timestamps and status
- **Jobs** — live job list with inline Cancel buttons
- **Audit Log** — every admin action (cancel, delete, inspect) with admin name and timestamp

### Audit Trail

Every admin action records:

```
🛡 John Smith — Cancel Job  at 14:32:07
🛡 John Smith — Session Deleted  at 14:35:12
```

This notice appears on the session card in the sidebar so any user reconnecting to that session can see what changed and when.

### Admin Reports

From **Performance → Report**, admin sees two report type options:

| Report Type | Audience | Content |
|-------------|----------|---------|
| **Technical Report** | Engineering teams | Vertex-level metrics, JVM heap, checkpoint durations, per-session job breakdown, exact SQL statements |
| **Business / Management Report** | Non-technical stakeholders | Self-explaining resource utilisation %, job health summaries, throughput in plain language, no SQL or technical jargon |

Both reports carry the **admin's name, session ID, and generation timestamp** in the header.

---

## Session Model

- **Regular sessions** see only the jobs they submitted — jobs are tagged with the session name via `pipeline.name` SET before every INSERT.
- Sessions are kept alive by a 30-second heartbeat. They do **not** expire from the IDE due to inactivity — only an explicit disconnect or container restart ends them.
- **Duplicate submission guard** — submitting the same INSERT INTO while it's already RUNNING is blocked with a clear warning message.
- **Cancel confirmation modal** — cancelling any job shows "Are you sure?" before sending the signal to JobManager.

---

## Connector JARs

Connector JARs **must match your Flink version exactly**. Wrong version → `NoClassDefFoundError: guava30/.../Closer` on every INSERT job.

| Flink version | Kafka connector JAR |
|---|---|
| 1.20.x | `flink-sql-connector-kafka-3.3.0-1.20.jar` |
| 1.19.x ← recommended | `flink-sql-connector-kafka-3.3.0-1.19.jar` |
| 1.18.x | `flink-sql-connector-kafka-3.3.0-1.18.jar` |
| 1.17.x | `flink-sql-connector-kafka-3.2.0-1.17.jar` |
| 1.16.x | `flink-sql-connector-kafka-3.1.0-1.16.jar` |

```bash
# Auto-detect version and download:
./scripts/download-connectors.sh
# Or Windows:
.\scripts\download-connectors.ps1
```

---

## Connecting to Kafka

```sql
CREATE TABLE kafka_events (
  event_id   STRING,
  user_id    STRING,
  amount     DOUBLE,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'user-events',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'flink-consumer-01',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'json'
);
```

---

## Connecting to Confluent Schema Registry (Avro)

Requires: `flink-sql-avro-confluent-registry-<ver>.jar` and `flink-sql-avro-<ver>.jar` in `/opt/flink/lib/`.

```sql
CREATE TABLE payments_avro (
  payment_id STRING,
  amount     DOUBLE,
  currency   STRING,
  ts         TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'payments',
  'properties.bootstrap.servers' = 'kafka-01:29092',
  'properties.group.id'          = 'flink-payments-01',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = 'http://schemaregistry0-01:8081',
  'scan.startup.mode'            = 'earliest-offset'
);
```

---

## Checkpointing and Sinking to S3 / MinIO

```sql
-- 1. Enable checkpointing to S3/MinIO
SET 'state.backend'         = 'filesystem';
SET 'state.checkpoints.dir' = 's3://flink-checkpoints/cp';
SET 'execution.checkpointing.interval' = '10000';

-- 2. Sink results to data lake (Parquet, partitioned)
CREATE TABLE trade_archive (
  trade_date STRING,
  symbol     STRING,
  volume     DOUBLE,
  total_usd  DOUBLE
) PARTITIONED BY (trade_date)
WITH (
  'connector'           = 'filesystem',
  'path'                = 's3://my-data-lake/trades/',
  'format'              = 'parquet',
  'sink.rolling-policy.rollover-interval' = '10 min',
  'sink.partition-commit.delay'           = '1 min',
  'sink.partition-commit.policy.kind'     = 'success-file'
);
```

For MinIO, add these to your Flink config:
```yaml
fs.s3a.endpoint: http://minio:9000
fs.s3a.access-key: minioadmin
fs.s3a.secret-key: minioadmin
fs.s3a.path.style.access: true
```

---

## Real-time ML Inference

### Pattern 1 — UDF calling a hosted model REST API

Register a Java/Python UDF that calls your model endpoint (SageMaker, Vertex AI, custom FastAPI):

```sql
-- Register UDF (JAR must be in /opt/flink/lib/)
CREATE FUNCTION predict_risk AS 'com.mycompany.flink.RiskScoringUDF';

-- Use inline in pipeline — scores every record in real time
SELECT
  trade_id,
  symbol,
  amount,
  predict_risk(symbol, amount, volume, volatility) AS risk_score
FROM enriched_trades_source
WHERE predict_risk(symbol, amount, volume, volatility) > 0.75;
```

### Pattern 2 — Feature pipeline → Kafka → Python AI service

```sql
-- Flink: compute and publish features
INSERT INTO ml_features_kafka
SELECT
  user_id,
  COUNT(*)       AS txn_count_5m,
  SUM(amount)    AS total_spend_5m,
  MAX(amount)    AS max_txn_5m,
  window_end     AS feature_time
FROM TABLE(HOP(TABLE transactions, DESCRIPTOR(event_time),
    INTERVAL '1' MINUTE, INTERVAL '5' MINUTE))
GROUP BY user_id, window_start, window_end;

-- Python AI service consumes 'ml-features', runs model.predict(),
-- publishes predictions back to 'ml-predictions' topic

-- Flink: join predictions back for downstream actions
SELECT s.user_id, s.amount, p.fraud_score, p.recommendation
FROM transactions s
JOIN predictions_source p
  ON s.user_id = p.user_id
  AND p.ts BETWEEN s.event_time - INTERVAL '10' SECOND
                AND s.event_time + INTERVAL '10' SECOND;
```

---

## Recommended session setup

Run this at the start of **every** Flink SQL session (available as a snippet under CONFIG → ⚡ Recommended Streaming Config):

```sql
SET 'execution.runtime-mode'                     = 'streaming';
SET 'parallelism.default'                        = '2';
SET 'execution.checkpointing.interval'           = '10000';
SET 'execution.checkpointing.mode'               = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'                       = '3600000';
SET 'table.exec.source.idle-timeout'             = '10000';
SET 'table.exec.mini-batch.enabled'              = 'true';
SET 'table.exec.mini-batch.allow-latency'        = '500 ms';
SET 'table.exec.mini-batch.size'                 = '5000';
SET 'table.optimizer.agg-phase-strategy'         = 'TWO_PHASE';
```

---

## Architecture

```
Browser (localhost:3030)
    │
    ▼
[nginx: flink-studio container]
    │  /flink-api/       → flink-sql-gateway:8083
    │  /jobmanager-api/  → flink-jobmanager:8081
    ▼
[Flink SQL Gateway] ←→ [JobManager / TaskManagers]
                              │
                    [Kafka / Schema Registry / MinIO / Elasticsearch]
```

---

## Features

### Editor
- Multi-tab SQL editor — double-click to rename tabs
- `Ctrl+Enter` run, `Ctrl+S` save, `Ctrl+/` comment, selection-only run
- Format SQL, Explain Plan, line numbers, cursor position

### Sessions
- Each session has a fully isolated workspace — tabs, logs, history, results, jobs
- Sessions never expire from the IDE side — heartbeat keeps them alive
- Duplicate submission guard prevents re-running the same INSERT INTO twice
- Existing sessions fetched from gateway on Test Connection — select from dropdown to reconnect

### Results & Streaming
- Live rows from Kafka stream into Results tab as they arrive
- INSERT INTO detects Job ID and auto-switches to Job Graph
- Paginated result table with CSV export and stream slot history

### Job Graph
- Visual DAG with real-time vertex metrics (records/s, backpressure)
- Fault node highlighting — pulsing red + error message in node
- Animated flowing edges (RUNNING) and red dashed edges (FAILED)
- Drag-to-pan, mouse-wheel zoom (20%–300%), double-click node for detail
- Cancel Job button — shows confirmation modal before sending cancel signal

### Performance
- Live KPIs: records in/out, backpressure %, slot utilisation, checkpoint health
- Throughput sparklines, per-query timing bars, cluster resource cards
- Checkpoint history sparkline and detail grid

### Admin Session
- Full cluster job visibility with session attribution
- Session Inspector — queries, jobs, open tabs, audit log per session
- Audit trail records every admin action with name and timestamp
- Technical and Business/Management PDF reports with distinct themes

### Tips & Concepts Modal
- 25+ tips across 7 categories: Getting Started, IDE Tips, Flink Architecture, Flink Concepts, Connectors, AI & ML Workloads, Performance Tips, Admin
- Every tip includes a SQL code example demonstrating the concept
- Paginated with progress dots, category tag navigation, "Don't show on startup" option

### About Modal
- Links to Apache Flink SQL official documentation: SQL Overview, Connectors, Window Aggregation, Checkpointing

---

## Saving your work

FlinkSQL Studio auto-saves tabs to `localStorage`. For permanent backup across machines:

1. Click **Workspace** in the topbar
2. Enter a filename (e.g. `crypto-pipeline`)
3. Click **↓ Export Workspace** — saves all tabs, SQL, history, logs, theme

To resume: **↑ Import Workspace** → select your `.json` file.

> Flink sessions are ephemeral — the export saves your *scripts*, not the running session. After importing, re-run your `USE CATALOG` and `CREATE TABLE` statements.

---

## Connecting to your cluster

| What | Value |
|---|---|
| Internal Kafka bootstrap | `kafka-01:29092` |
| Schema Registry | `http://schemaregistry0-01:8081` |
| Kafka UI | `http://localhost:28040` |
| Flink UI | `http://localhost:8012` |

---

## Changelog

### v1.0.19 (current)

- **Admin Session** — 🛡 Admin mode on connect screen with passcode auth
- **Session Inspector** — tabbed modal: Overview, Queries, Jobs, Audit Log per session
- **Audit trail** — every admin action recorded with name, timestamp, detail; visible on session cards
- **Live session stats** — sidebar shows query count and running/total jobs per session
- **Admin sessions overview** — summary bar with cluster-wide counts (sessions, jobs, running, failed, total queries)
- **Admin reports** — Technical Report and Business/Management Report with distinct themes and titling
- **Duplicate submission guard** — blocks re-submitting a running INSERT INTO
- **Cancel job confirmation modal** — explicit "Are you sure?" before sending cancel signal
- **Job scoping** — regular sessions see only their own jobs; admin sees all with session attribution
- **Job tagging** — `pipeline.name` SET injected before INSERT with session label prefix
- **Load existing sessions** — connect screen auto-fetches sessions dropdown after Test Connection
- **Tips & Concepts modal** — 25+ tips with SQL code demos: Kafka, S3, Schema Registry, ML inference, AI workloads
- **Apache Flink SQL docs links** in About modal
- **Query count tracking** — session cards show live query counts

### v1.0.18
- Streaming results fixed — Kafka SELECT rows reliably stream into Results tab
- Mouse wheel zoom on Job Graph (20%–300%)
- Catalog context fix — sidebar refresh no longer hijacks active USE CATALOG context
- Session isolation — new sessions always start clean

### v1.0.17
- Job Graph DAG with drag-to-pan, animated edges, fault highlighting
- Tab rename with 20-char limit, per-session workspace isolation

### v1.0.16
- Job Graph tab, multi-session panel, + Catalog / + Database buttons

---

## Contributing

1. Fork the repo and create a feature branch.
2. Each JS module has a single clear responsibility — keep it that way.
3. No build step, no bundler, no dependencies — plain HTML/CSS/JS only.
4. Test against Flink 1.18+ SQL Gateway.
5. Open a PR with a clear description of what changed and why.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## License

MIT — see [LICENSE](LICENSE).
Created by **Nestor Martourez. A** · [codedstreams](https://github.com/coded-streams)