# FlinkSQL Studio

> A zero-dependency, single-file browser IDE for Apache Flink SQL — built for developers who want a real query interface without leaving their laptop.

![License](https://img.shields.io/badge/license-MIT-green)
![Flink](https://img.shields.io/badge/Apache%20Flink-1.19%2B-orange)
![Docker](https://img.shields.io/badge/docker-ready-blue)

## What is it?

FlinkSQL Studio is a self-hosted web IDE that connects to the **Flink SQL Gateway** and lets you:

- Write and run Flink SQL in a multi-tab editor with **live streaming results from Kafka**
- See actual rows flowing from Kafka topics directly into the Results tab in real time
- Visualize running job DAGs with metrics, fault highlighting, and animated edges
- Cancel running Flink jobs from the Job Graph toolbar
- Manage multiple sessions — each session has its own isolated workspace (tabs, logs, history)
- Save and restore work with named workspace exports/imports
- Create catalogs and databases without losing your session context
- Track active `catalog.database` in the statusbar — always know where you are
- Browse catalogs, databases, tables in the sidebar

It is a **single HTML file** served by nginx. No Node.js, no npm, no build step.

---

## Quickstart

```bash
git clone https://github.com/coded-streams/flinksql-studio
cd flinksql-studio
docker compose up -d
```

Open **http://localhost:3030** — connect using **Proxy mode** (default).

---

## Connector JARs (important)

Your Flink connector JARs **must match your Flink version exactly**. For Flink 1.19.1:

| JAR | Version |
|---|---|
| `flink-sql-connector-kafka` | `3.3.0-1.19` |
| `flink-sql-avro-confluent-registry` | `1.19.1` |
| `flink-sql-avro` | `1.19.1` |

Use the included `download-connectors.ps1` (Windows) or run:

```bash
curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar
```

Wrong version → `NoClassDefFoundError: guava30/.../Closer` on every INSERT job.

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
                    [Kafka / MinIO / Elasticsearch]
```

---

## Features

### Sessions
- Create multiple Flink SQL Gateway sessions
- Each session gets a **fully isolated workspace** — tabs, logs, history, results are separate
- New session always starts at **Query 1** with an empty editor
- Switch sessions — workspace is saved and restored automatically

### Catalog & Database management
- Create catalogs/databases without losing your active context
- Catalog sidebar refresh no longer silently switches your session away from your catalog
- **Active `catalog.database` shown in the statusbar** — updates live as you run `USE CATALOG` / `USE`
- Sidebar **＋ Catalog** and **＋ Database** buttons insert DDL templates directly into the editor

### Editor & Tabs
- Multi-tab SQL editor — double-click any tab to rename it
- Ctrl+Enter to run, Ctrl+S to save, selection-only run
- Tab counter resets to 1 on every new session

### Results & Streaming
- **Live rows from Kafka** — SELECT from a Kafka source streams rows into the Results tab as they arrive
- Results tab auto-activates and badge counter updates as rows flow in
- INSERT INTO auto-detects the job ID response and switches to Job Graph
- Paginated result table with CSV export

### Job Graph
- Visual DAG of Flink execution plan
- Real-time vertex metrics (records/s)
- Fault node highlighting — pulsing red border + error message
- Animated flowing edges (RUNNING) and red dashed edges (FAILED)
- Drag-to-pan, double-click node for details
- **Cancel Job button** — visible only for running/restartable jobs

### Snippets
- **⚡ Recommended Streaming Config** — one-click block: streaming mode, parallelism=2, checkpointing, state TTL, MiniBatch
- 30+ templates: DDL, Kafka, datagen, windowing, joins, MinIO, performance tuning

### Workspace
- **Named export** — provide your own filename before exporting
- Import a previous export to resume exactly where you left off
- Auto-save to localStorage on every change

---

## Recommended session setup

Run this at the start of **every** Flink SQL session (available as a snippet under CONFIG):

```sql
USE CATALOG default_catalog;
USE `default`;

SET 'execution.runtime-mode'           = 'streaming';
SET 'parallelism.default'              = '2';
SET 'execution.checkpointing.interval' = '10000';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'             = '3600000';
SET 'table.exec.source.idle-timeout'   = '10000';
```

---

## Example pipelines

| File | Description |
|---|---|
| `docs/demo-datagen-kafka.sql` | Simple datagen → Kafka → Kafka with live preview |
| `docs/full-pipeline.sql` | Full multi-node pipeline: datagen → enrich → aggregate → print |

### Full pipeline topology

```
[datagen] → trade-events (Kafka)
                 │
            [enrich + route]
           /              \
enriched-trades (Kafka)   pipeline-risk-alerts (Kafka)
           │
    [10s tumble window agg]
           │
    trade-summary (Kafka) + print (stdout)
```

Produces a full multi-node Job Graph in the IDE.

---

## Connecting to your cluster

| What | Value |
|---|---|
| Internal Kafka bootstrap | `kafka-01:29092` |
| Schema Registry | `http://schemaregistry0-01:8081` |
| Kafka UI | `http://localhost:28040` |
| Flink UI | `http://localhost:8012` |

---

## Project structure

```
flinksql-studio/
├── studio/
│   └── index.html                  # The entire IDE (single self-contained file)
├── nginx/
│   └── studio.conf                 # nginx reverse proxy config
├── scripts/
│   ├── setup.sh                    # Linux/macOS quickstart
│   ├── setup.ps1                   # Windows PowerShell quickstart
│   └── setup.bat                   # Windows batch quickstart
├── k8s/
│   ├── manifests/deployment.yaml   # Plain Kubernetes deployment
│   └── helm/flinksql-studio/       # Helm chart
├── docs/
│   ├── index.html                  # Documentation site
│   ├── architecture.svg            # Architecture diagram
│   ├── demo-datagen-kafka.sql      # Simple demo pipeline
│   └── full-pipeline.sql           # Full multi-node pipeline
├── .github/workflows/publish.yml   # Docker Hub CI/CD
├── Dockerfile
├── docker-compose.yml
├── docker-entrypoint.sh
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

---

## Changelog

### v8 (current)
- **Catalog context fix** — sidebar refresh no longer hijacks your active `USE CATALOG` context
- **Active catalog.database in statusbar** — tracks `USE CATALOG` / `USE` statements live
- **Streaming results fixed** — Kafka SELECT rows now reliably appear in Results tab
- **Results tab auto-activates** when first rows arrive from a streaming query
- Session isolation — new sessions always start clean at Query 1
- Cancel Job button in Job Graph toolbar
- Named workspace export with custom filename
- Catalog sidebar ＋ Catalog and ＋ Database quick-insert buttons
- INSERT INTO auto-redirects to Job Graph
- Graph node HTML rendering fixed
- Tab rename fixed — double-click reliability
- ⚡ Recommended Streaming Config snippet added

### v7
- Job Graph DAG with drag-to-pan, animated edges, fault highlighting
- Tab rename with 20-char limit

### v6
- Job Graph tab, per-session workspace isolation

---

## License

MIT — see [LICENSE](LICENSE)

---

## Saving your work & resuming later

FlinkSQL Studio auto-saves your tabs and SQL to browser `localStorage` after every keystroke. But to truly save your work across machines, browsers, or container restarts you should **export your workspace**.

### Export

1. Click **Workspace** in the top bar
2. Type a name in the **Export filename** field (e.g. `crypto-pipeline`)
3. Click **↓ Export Workspace**
4. A file `flinksql-crypto-pipeline.json` downloads to your machine

This file contains: all editor tabs, SQL content, query history, session logs, performance timings, and theme preference.

### Import (resume later)

1. Click **Workspace** → **↑ Import Workspace**
2. Select your `.json` file
3. All your tabs, SQL, and history are restored instantly

> **Note:** Flink sessions are ephemeral — they live in the SQL Gateway container and are lost on container restart. The export saves your *scripts and history*, not the running session state. After importing, re-run your `USE CATALOG` and `CREATE TABLE` statements to reconnect to your data.

### Export results, logs and performance

Click the **↓ Export** button in the results panel to download a combined `.csv` file containing:

- **Query results** — all rows from the last query
- **Session log** — every SQL statement, error, and info message
- **Performance timings** — duration and row count for each query

---

## Demo project

See `docs/demo-datagen-kafka.sql` for a quick-start demo that:

1. Creates a `datagen` source generating synthetic trade events
2. Writes them to a Kafka topic (`demo-trades-raw`)
3. Reads back from Kafka and shows live rows in the Results tab
4. Enriches and writes to a second topic (`demo-trades-enriched`)

### Run it

```sql
-- Paste each step in a separate tab and run in order.
-- Steps 1-4b: CREATE TABLE statements (run together)
-- Step 5:     INSERT datagen → Kafka  (own tab — shows Job Graph)
-- Step 6:     SELECT from Kafka       (own tab — shows live rows)
-- Step 7:     INSERT enrich → Kafka   (own tab — shows Job Graph)
-- Step 8:     SELECT enriched         (own tab — shows live rows)
```

See `docs/full-pipeline.sql` for the full multi-node pipeline with aggregations, risk routing, and stdout output.

---

## Connector version reference

| Flink version | Kafka connector JAR |
|---|---|
| 1.20.x | `flink-sql-connector-kafka-3.3.0-1.20.jar` |
| 1.19.x | `flink-sql-connector-kafka-3.3.0-1.19.jar` |
| 1.18.x | `flink-sql-connector-kafka-3.3.0-1.18.jar` |
| 1.17.x | `flink-sql-connector-kafka-3.2.0-1.17.jar` |
| 1.16.x | `flink-sql-connector-kafka-3.1.0-1.16.jar` |

The `download-connectors.ps1` / `download-connectors.sh` scripts auto-detect your Flink version and download the correct JAR. Always match the JAR suffix to your Flink minor version.
