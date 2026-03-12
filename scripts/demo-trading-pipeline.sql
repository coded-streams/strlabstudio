-- ════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — TRADING ANALYTICS DEMO PIPELINE
-- Multi-branch streaming pipeline with catalog setup
--
-- HOW TO USE IN FLINKSQL STUDIO:
--   This demo is split across 6 tabs. Create each tab in the IDE,
--   paste the SQL for that tab, and run them IN ORDER (Tab 1 → Tab 6).
--   Each tab is labelled at the top. You can name your tabs by
--   double-clicking the tab title.
--
-- IMPORTANT: Run Tab 1 first (catalog & session config).
--   Tabs 2-4 create sources and sinks — run each individually.
--   Tabs 5-6 are the streaming INSERT jobs — run them last.
-- ════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════
--  TAB 1 — "Setup"
--  Catalog, database, session configuration
--  Run this first. Run each statement individually with Ctrl+Enter.
-- ════════════════════════════════════════════════════════════════════════

-- 1a. Use the default catalog (always exists in Flink)
USE CATALOG default_catalog;

-- 1b. Create a dedicated database for this demo
CREATE DATABASE IF NOT EXISTS trading_demo;

-- 1c. Switch into it
USE trading_demo;

-- 1d. Session configuration — apply before creating any tables
SET 'execution.runtime-mode'      = 'streaming';
SET 'parallelism.default'         = '2';
SET 'pipeline.operator-chaining'  = 'false';
SET 'table.exec.mini-batch.enabled'       = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2 s';
SET 'table.exec.mini-batch.size'          = '1000';

-- Verify you are in the right place
SHOW DATABASES;
SHOW CURRENT DATABASE;


-- ════════════════════════════════════════════════════════════════════════
--  TAB 2 — "Sources"
--  Create the two datagen source tables
--  Run after Tab 1. Each CREATE TABLE runs instantly (DDL only).
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE trading_demo;

-- SOURCE A: Trade events — simulates market trade feed
CREATE TEMPORARY TABLE trades (
  trade_id    BIGINT,
  user_id     BIGINT,
  symbol_id   INT,           -- 1=BTC 2=ETH 3=SOL 4=AAPL 5=TSLA
  side        INT,           -- 0=BUY 1=SELL
  quantity    DOUBLE,
  price       DOUBLE,
  region_id   INT,           -- 1=US 2=EU 3=ASIA
  ts          TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector'                  = 'datagen',
  'rows-per-second'            = '50',
  'fields.trade_id.kind'       = 'sequence',
  'fields.trade_id.start'      = '1',
  'fields.trade_id.end'        = '9999999',
  'fields.user_id.kind'        = 'random',
  'fields.user_id.min'         = '1',
  'fields.user_id.max'         = '500',
  'fields.symbol_id.kind'      = 'random',
  'fields.symbol_id.min'       = '1',
  'fields.symbol_id.max'       = '5',
  'fields.side.kind'           = 'random',
  'fields.side.min'            = '0',
  'fields.side.max'            = '1',
  'fields.quantity.kind'       = 'random',
  'fields.quantity.min'        = '0.01',
  'fields.quantity.max'        = '100.0',
  'fields.price.kind'          = 'random',
  'fields.price.min'           = '10.0',
  'fields.price.max'           = '60000.0',
  'fields.region_id.kind'      = 'random',
  'fields.region_id.min'       = '1',
  'fields.region_id.max'       = '3'
);

-- SOURCE B: User risk scores — simulates a slow-changing risk feed
CREATE TEMPORARY TABLE user_risk (
  user_id     BIGINT,
  risk_score  INT,           -- 0-100
  tier        INT,           -- 1=retail 2=pro 3=institutional
  ts          TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
  'connector'                  = 'datagen',
  'rows-per-second'            = '5',
  'fields.user_id.kind'        = 'random',
  'fields.user_id.min'         = '1',
  'fields.user_id.max'         = '500',
  'fields.risk_score.kind'     = 'random',
  'fields.risk_score.min'      = '0',
  'fields.risk_score.max'      = '100',
  'fields.tier.kind'           = 'random',
  'fields.tier.min'            = '1',
  'fields.tier.max'            = '3'
);

-- Quick test — verify sources emit rows (runs a bounded batch preview)
SELECT trade_id, user_id, symbol_id, quantity, price, ts
FROM trades LIMIT 10;


-- ════════════════════════════════════════════════════════════════════════
--  TAB 3 — "Sinks"
--  Create all output tables (blackhole connector — no external deps)
--  Run after Tab 2.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE trading_demo;

-- SINK 1: Raw enriched trades (Branch A output)
CREATE TEMPORARY TABLE sink_raw_trades (
  trade_id  BIGINT,
  user_id   BIGINT,
  symbol    STRING,
  side      STRING,
  quantity  DOUBLE,
  price     DOUBLE,
  notional  DOUBLE,
  region    STRING,
  ts        TIMESTAMP(3),
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH ('connector' = 'blackhole');

-- SINK 2: 1-minute tumbling window OHLCV per symbol (Branch B output)
CREATE TEMPORARY TABLE sink_ohlcv (
  symbol       STRING,
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  open_price   DOUBLE,
  high_price   DOUBLE,
  low_price    DOUBLE,
  close_price  DOUBLE,
  volume       DOUBLE,
  trade_count  BIGINT,
  PRIMARY KEY (symbol, window_end) NOT ENFORCED
) WITH ('connector' = 'blackhole');

-- SINK 3: User trading summary — 2-min sliding window (Branch C output)
CREATE TEMPORARY TABLE sink_user_summary (
  user_id      BIGINT,
  window_end   TIMESTAMP(3),
  trade_count  BIGINT,
  buy_notional  DOUBLE,
  sell_notional DOUBLE,
  net_notional  DOUBLE,
  PRIMARY KEY (user_id, window_end) NOT ENFORCED
) WITH ('connector' = 'blackhole');

-- SINK 4: Fraud alerts — high-velocity users (Branch D output)
CREATE TEMPORARY TABLE sink_alerts (
  alert_id    BIGINT,
  user_id     BIGINT,
  alert_type  STRING,
  trade_id    BIGINT,
  notional    DOUBLE,
  spend_1min  DOUBLE,
  risk_score  INT,
  ts          TIMESTAMP(3),
  PRIMARY KEY (alert_id) NOT ENFORCED
) WITH ('connector' = 'blackhole');

-- SINK 5: Region leaderboard — 30s hop window (Branch E output)
CREATE TEMPORARY TABLE sink_region_board (
  region       STRING,
  symbol       STRING,
  window_end   TIMESTAMP(3),
  total_volume DOUBLE,
  trade_count  BIGINT,
  avg_price    DOUBLE,
  PRIMARY KEY (region, symbol, window_end) NOT ENFORCED
) WITH ('connector' = 'blackhole');


-- ════════════════════════════════════════════════════════════════════════
--  TAB 4 — "Verify"
--  Spot-check sources and verify schema before launching jobs
--  Optional — run any of these SELECT statements to confirm data flows
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE trading_demo;

-- Check trades source
SELECT
  trade_id,
  user_id,
  CASE symbol_id
    WHEN 1 THEN 'BTC'  WHEN 2 THEN 'ETH'
    WHEN 3 THEN 'SOL'  WHEN 4 THEN 'AAPL'
    ELSE 'TSLA'
  END AS symbol,
  CASE side WHEN 0 THEN 'BUY' ELSE 'SELL' END AS side,
  ROUND(quantity * price, 2) AS notional,
  ts
FROM trades
LIMIT 20;

-- Check user_risk source
SELECT user_id, risk_score, tier, ts
FROM user_risk
LIMIT 10;

-- Show created tables
SHOW TABLES;


-- ════════════════════════════════════════════════════════════════════════
--  TAB 5 — "Pipeline A+B+C"
--  Branch A: Raw trade enrichment
--  Branch B: OHLCV tumbling window (1 min per symbol)
--  Branch C: User trading summary (2-min hop window)
--
--  Run this tab to launch 3 branches as a single Flink job.
--  After running, go to Job Graph tab to see the multi-branch DAG.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE trading_demo;

-- ── BRANCH A: Enrich and write all trades ──────────────────────────────
-- DAG nodes: Source → Calc(CASE) → Sink
INSERT INTO sink_raw_trades
SELECT
  trade_id,
  user_id,
  CASE symbol_id
    WHEN 1 THEN 'BTC'  WHEN 2 THEN 'ETH'
    WHEN 3 THEN 'SOL'  WHEN 4 THEN 'AAPL'
    ELSE 'TSLA'
  END AS symbol,
  CASE side WHEN 0 THEN 'BUY' ELSE 'SELL' END AS side,
  quantity,
  price,
  ROUND(quantity * price, 2) AS notional,
  CASE region_id WHEN 1 THEN 'US' WHEN 2 THEN 'EU' ELSE 'ASIA' END AS region,
  ts
FROM trades;

-- ── BRANCH B: OHLCV per symbol — 1-minute tumbling window ─────────────
-- DAG nodes: Source → Calc → LocalAgg → GlobalAgg → Sink
-- Shows: LocalWindowAggregate + GlobalWindowAggregate pattern in the graph
INSERT INTO sink_ohlcv
SELECT
  CASE symbol_id
    WHEN 1 THEN 'BTC' WHEN 2 THEN 'ETH'
    WHEN 3 THEN 'SOL' WHEN 4 THEN 'AAPL' ELSE 'TSLA'
  END AS symbol,
  TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,
  TUMBLE_END(ts,   INTERVAL '1' MINUTE) AS window_end,
  FIRST_VALUE(price)     AS open_price,
  MAX(price)             AS high_price,
  MIN(price)             AS low_price,
  LAST_VALUE(price)      AS close_price,
  SUM(quantity)          AS volume,
  COUNT(*)               AS trade_count
FROM trades
GROUP BY symbol_id, TUMBLE(ts, INTERVAL '1' MINUTE);

-- ── BRANCH C: User trading summary — 2-minute HOP window ──────────────
-- DAG nodes: Source → Calc → LocalHopAgg → GlobalHopAgg → Sink
INSERT INTO sink_user_summary
SELECT
  user_id,
  HOP_END(ts, INTERVAL '30' SECOND, INTERVAL '2' MINUTE) AS window_end,
  COUNT(*)                                                 AS trade_count,
  ROUND(SUM(CASE WHEN side = 0 THEN quantity * price ELSE 0 END), 2) AS buy_notional,
  ROUND(SUM(CASE WHEN side = 1 THEN quantity * price ELSE 0 END), 2) AS sell_notional,
  ROUND(SUM(CASE side WHEN 0 THEN quantity*price ELSE -(quantity*price) END), 2) AS net_notional
FROM trades
GROUP BY user_id, HOP(ts, INTERVAL '30' SECOND, INTERVAL '2' MINUTE);


-- ════════════════════════════════════════════════════════════════════════
--  TAB 6 — "Pipeline D+E"
--  Branch D: Fraud velocity alert (OVER window + join with risk scores)
--  Branch E: Region/symbol leaderboard (HOP window)
--
--  Run AFTER Tab 5. Creates a second Flink job visible in Job Graph.
--  Select the different jobs from the dropdown to compare DAGs.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE trading_demo;

-- ── BRANCH D: Fraud alert — OVER window spend velocity + risk join ─────
-- DAG nodes: Source(trades) + Source(user_risk)
--            → Interval Join → OVER Window Agg → Filter → Sink
-- The interval join between trades and user_risk creates the multi-source
-- branching visible in the job graph.
INSERT INTO sink_alerts
SELECT
  trade_id                          AS alert_id,
  t.user_id,
  CASE
    WHEN spend_1min > 500000 AND r.risk_score > 80 THEN 'CRITICAL_VELOCITY'
    WHEN spend_1min > 500000                        THEN 'HIGH_VELOCITY'
    WHEN r.risk_score > 80                          THEN 'HIGH_RISK_USER'
    ELSE 'VELOCITY_ALERT'
  END                               AS alert_type,
  t.trade_id,
  ROUND(t.quantity * t.price, 2)    AS notional,
  ROUND(spend_1min, 2)              AS spend_1min,
  r.risk_score,
  t.ts
FROM (
  -- OVER window: rolling 1-minute spend per user
  SELECT
    trade_id,
    user_id,
    quantity,
    price,
    ts,
    SUM(quantity * price) OVER (
      PARTITION BY user_id
      ORDER BY ts
      RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
    ) AS spend_1min
  FROM trades
) t
JOIN user_risk r
  ON t.user_id = r.user_id
  AND r.ts BETWEEN t.ts - INTERVAL '5' MINUTE AND t.ts + INTERVAL '1' MINUTE
WHERE spend_1min > 100000 OR r.risk_score > 75;

-- ── BRANCH E: Region + Symbol leaderboard — 30s HOP window ────────────
-- DAG nodes: Source → Calc → LocalHopAgg → GlobalHopAgg → Sink
INSERT INTO sink_region_board
SELECT
  CASE region_id WHEN 1 THEN 'US' WHEN 2 THEN 'EU' ELSE 'ASIA' END AS region,
  CASE symbol_id
    WHEN 1 THEN 'BTC' WHEN 2 THEN 'ETH'
    WHEN 3 THEN 'SOL' WHEN 4 THEN 'AAPL' ELSE 'TSLA'
  END AS symbol,
  HOP_END(ts, INTERVAL '10' SECOND, INTERVAL '30' SECOND) AS window_end,
  ROUND(SUM(quantity), 4)        AS total_volume,
  COUNT(*)                       AS trade_count,
  ROUND(AVG(price), 2)           AS avg_price
FROM trades
GROUP BY
  region_id,
  symbol_id,
  HOP(ts, INTERVAL '10' SECOND, INTERVAL '30' SECOND);
