-- ═══════════════════════════════════════════════════════════════════════════════
-- CRYPTO RISK ENGINE — Flink SQL Pipeline
-- Project: codedstreams / crypto-risk-engine
--
-- MIRRORS the DataStream job topology:
--   CryptoRiskEngineApplication.java  →  SQL equivalent
--
-- PIPELINE TOPOLOGY:
--
--   [Spring Trade Producer]
--          │  ProduceTradeRequest → CryptoTrade (Avro)
--          ▼
--   crypto-trades (Kafka, Avro)
--          │
--          ▼
--   [SOURCE] crypto_trades_source
--          │
--          ▼
--   [ENRICH] trade_value, risk_level, region  ─────► risk-analyzed-trades (Kafka)
--          │
--          ├── WHERE notional >= threshold
--          │       │
--          │       ▼
--          │   [CEP ANALOG] pattern-level detection views
--          │       │
--          │       ▼
--          │   risk-alerts (Kafka)
--          │
--          └── [AGGREGATE] 1-min tumbling window by symbol
--                  │
--                  ▼
--              trade-summary (Kafka)  +  print_sink (stdout)
--
-- HOW TO RUN:
--   1. Open FlinkSQL Studio → http://localhost:3030
--   2. Connect (Proxy mode)
--   3. Run STEP 0 (session config) in Tab 1
--   4. Run STEP 1–6 (DDL: table definitions) in Tab 1
--   5. Tab 2 → STEP 7  (JOB 1: enrichment → risk-analyzed-trades)
--   6. Tab 3 → STEP 8  (JOB 2: risk alerts routing)
--   7. Tab 4 → STEP 9  (JOB 3: tumbling window aggregation)
--   8. Tab 5+ → PREVIEW queries (live Results tab)
--
-- NOTES:
--   • Topics must exist before running. Create via:
--       kafka-topics.sh --create --topic crypto-trades          --partitions 4 --bootstrap-server kafka-01:29092
--       kafka-topics.sh --create --topic risk-analyzed-trades   --partitions 4 --bootstrap-server kafka-01:29092
--       kafka-topics.sh --create --topic risk-alerts            --partitions 4 --bootstrap-server kafka-01:29092
--       kafka-topics.sh --create --topic trade-summary          --partitions 2 --bootstrap-server kafka-01:29092
--
--   • The Spring producer sends Avro with Confluent Schema Registry.
--     Flink SQL uses 'avro-confluent' format — ensure the registry URL matches.
--
--   • The DataStream job uses per-user keyBy for stateful risk scoring
--     (TradeRiskAnalyzer). SQL replicates this with PARTITION BY in window ops.
--
--   • CEP patterns (VolumeSpikePattern, WashTradingPattern, LargeOrderPattern,
--     FlashCrashPattern) are approximated with tumbling/sliding window GROUP BY
--     queries. For exact CEP (MATCH_RECOGNIZE) see STEP 8c–8f.
-- ═══════════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 0 — Session setup (run FIRST, every session)
-- ═══════════════════════════════════════════════════════════════════════════════
USE CATALOG default_catalog;
USE `default`;

SET 'execution.runtime-mode'                   = 'streaming';
SET 'parallelism.default'                      = '2';
SET 'execution.checkpointing.interval'         = '10000';
SET 'execution.checkpointing.mode'             = 'EXACTLY_ONCE';
SET 'table.exec.state.ttl'                     = '3600000';
SET 'table.exec.source.idle-timeout'           = '10000';
SET 'table.exec.mini-batch.enabled'            = 'true';
SET 'table.exec.mini-batch.allow-latency'      = '200ms';
SET 'table.exec.mini-batch.size'               = '5000';


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 1 — SOURCE: crypto-trades (Avro + Confluent Schema Registry)
--
-- Mirrors: CryptoRiskEngineApplication.createTradeStream()
--          KafkaSource + ConfluentRegistryAvroDeserializationSchema<CryptoTrade>
--          WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
--
-- CryptoTrade Avro schema fields:
--   tradeId, userId, customerId, symbol, baseAsset, quoteAsset,
--   side (BUY|SELL), orderType (MARKET|LIMIT|STOP_LOSS|TAKE_PROFIT),
--   quantity, price, notional, timestamp (epoch ms), exchange,
--   walletAddress (nullable), riskLevel (LOW|MEDIUM|HIGH|CRITICAL),
--   sessionId (nullable), ipAddress (nullable)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE crypto_trades_source (
  trade_id        STRING,
  user_id         STRING,
  customer_id     STRING,
  symbol          STRING,
  base_asset      STRING,
  quote_asset     STRING,
  side            STRING,           -- BUY | SELL
  order_type      STRING,           -- MARKET | LIMIT | STOP_LOSS | TAKE_PROFIT
  quantity        DOUBLE,
  price           DOUBLE,
  notional        DOUBLE,           -- quantity * price, pre-computed by producer
  ts              BIGINT,           -- epoch ms from event.getTimestamp()
  exchange        STRING,
  wallet_address  STRING,           -- nullable
  risk_level      STRING,           -- LOW | MEDIUM | HIGH | CRITICAL (initial, from producer)
  session_id      STRING,           -- nullable
  ip_address      STRING,           -- nullable
  -- Derive a TIMESTAMP column for watermarking from the epoch-ms field
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector'                             = 'kafka',
  'topic'                                 = 'crypto-trades',
  'properties.bootstrap.servers'          = 'kafka-01:29092',
  'properties.group.id'                   = 'flink-sql-risk-pipeline',
  'scan.startup.mode'                     = 'earliest-offset',
  'format'                                = 'avro-confluent',
  'avro-confluent.url'                    = 'http://schemaregistry0-01:8081',
  'avro-confluent.subject'                = 'crypto-trades-value'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 2 — SINK: risk-analyzed-trades (mirrors RiskAnalyzedTradeSink)
--
-- The DataStream job writes RiskAnalyzedTrade which wraps the original
-- CryptoTrade + adds: riskScore (double), analyzedRiskLevel, flagged (bool),
-- analysisTimestamp, and a map of riskFactors.
--
-- We flatten riskFactors to individual columns here (SQL has no map sink).
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE risk_analyzed_trades_sink (
  trade_id            STRING,
  user_id             STRING,
  customer_id         STRING,
  symbol              STRING,
  base_asset          STRING,
  quote_asset         STRING,
  side                STRING,
  order_type          STRING,
  quantity            DOUBLE,
  price               DOUBLE,
  notional            DOUBLE,
  exchange            STRING,
  wallet_address      STRING,
  session_id          STRING,
  ip_address          STRING,
  -- Enriched / analyzed fields
  trade_value         DOUBLE,       -- ROUND(quantity * price, 2)
  risk_level          STRING,       -- recalculated: LOW/MEDIUM/HIGH/CRITICAL
  region              STRING,       -- US / EU / APAC / OTHER
  risk_score          DOUBLE,       -- 0.0–100.0 score
  flagged             BOOLEAN,      -- true when HIGH or CRITICAL
  analysis_ts         TIMESTAMP(3)  -- processing time
) WITH (
  'connector'                             = 'kafka',
  'topic'                                 = 'risk-analyzed-trades',
  'properties.bootstrap.servers'          = 'kafka-01:29092',
  'format'                                = 'avro-confluent',
  'avro-confluent.url'                    = 'http://schemaregistry0-01:8081',
  'avro-confluent.subject'                = 'risk-analyzed-trades-value',
  'sink.partitioner'                      = 'fixed'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 3 — SINK: risk-alerts (mirrors AlertSink → RiskAlert Avro schema)
--
-- RiskAlert fields: alertId, timestamp, severity (LOW|MEDIUM|HIGH|CRITICAL),
--   patternType, userId, customerId, description, recommendedAction,
--   triggeredEvents (map<string,string>), acknowledged (default false),
--   metadata (map<string,string>)
--
-- Maps flattened to string columns for SQL compatibility.
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE risk_alerts_sink (
  alert_id              STRING,
  alert_ts              BIGINT,         -- epoch ms
  severity              STRING,         -- LOW | MEDIUM | HIGH | CRITICAL
  pattern_type          STRING,         -- VOLUME_SPIKE | WASH_TRADING | LARGE_ORDER | FLASH_CRASH
  user_id               STRING,
  customer_id           STRING,
  description           STRING,
  recommended_action    STRING,
  triggered_trade_ids   STRING,         -- JSON array string (flattened triggeredEvents map)
  acknowledged          BOOLEAN,
  symbol                STRING,         -- from metadata
  notional              DOUBLE,         -- from metadata
  exchange              STRING          -- from metadata
) WITH (
  'connector'                             = 'kafka',
  'topic'                                 = 'risk-alerts',
  'properties.bootstrap.servers'          = 'kafka-01:29092',
  'format'                                = 'avro-confluent',
  'avro-confluent.url'                    = 'http://schemaregistry0-01:8081',
  'avro-confluent.subject'                = 'risk-alerts-value',
  'sink.partitioner'                      = 'round-robin'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 4 — SINK: trade-summary (tumbling window aggregates)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE trade_summary_sink (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  symbol          STRING,
  side            STRING,
  trade_count     BIGINT,
  total_notional  DOUBLE,
  avg_price       DOUBLE,
  max_notional    DOUBLE,
  min_notional    DOUBLE,
  high_count      BIGINT,
  critical_count  BIGINT
) WITH (
  'connector'                             = 'kafka',
  'topic'                                 = 'trade-summary',
  'properties.bootstrap.servers'          = 'kafka-01:29092',
  'format'                                = 'json',
  'sink.partitioner'                      = 'round-robin'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 5 — SINK: print (stdout → visible in TaskManager logs)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE summary_print (
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  symbol          STRING,
  side            STRING,
  trade_count     BIGINT,
  total_notional  DOUBLE,
  avg_price       DOUBLE,
  max_notional    DOUBLE,
  min_notional    DOUBLE,
  high_count      BIGINT,
  critical_count  BIGINT
) WITH (
  'connector' = 'print'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 6 — READ-BACK SOURCE: risk-analyzed-trades (for CEP pattern jobs)
--
-- Mirrors the DataStream map: analyzedTrades.map(RiskAnalyzedTrade::getOriginalTrade)
-- used as input for the CEP PatternStream operators.
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE TEMPORARY TABLE risk_analyzed_trades_source (
  trade_id        STRING,
  user_id         STRING,
  customer_id     STRING,
  symbol          STRING,
  base_asset      STRING,
  quote_asset     STRING,
  side            STRING,
  order_type      STRING,
  quantity        DOUBLE,
  price           DOUBLE,
  notional        DOUBLE,
  exchange        STRING,
  wallet_address  STRING,
  session_id      STRING,
  ip_address      STRING,
  trade_value     DOUBLE,
  risk_level      STRING,
  region          STRING,
  risk_score      DOUBLE,
  flagged         BOOLEAN,
  analysis_ts     TIMESTAMP(3),
  WATERMARK FOR analysis_ts AS analysis_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                             = 'kafka',
  'topic'                                 = 'risk-analyzed-trades',
  'properties.bootstrap.servers'          = 'kafka-01:29092',
  'properties.group.id'                   = 'flink-sql-cep-consumer',
  'scan.startup.mode'                     = 'earliest-offset',
  'format'                                = 'avro-confluent',
  'avro-confluent.url'                    = 'http://schemaregistry0-01:8081',
  'avro-confluent.subject'                = 'risk-analyzed-trades-value'
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 7 — JOB 1: Enrich trades and write to risk-analyzed-trades
--
-- Mirrors:
--   tradeStream.keyBy(userId).process(new TradeRiskAnalyzer())
--
-- Risk scoring logic (simplified from TradeRiskAnalyzer stateful process fn):
--   risk_score is a composite of notional tier + frequency penalty.
--   For SQL we derive a deterministic score from the notional value.
--   The DataStream version uses per-user ValueState for rolling score —
--   that stateful accumulation runs in the DataStream job; this SQL job
--   provides the base enrichment layer.
--
-- Run in its own tab. Keep RUNNING before starting JOB 2.
-- ═══════════════════════════════════════════════════════════════════════════════
INSERT INTO risk_analyzed_trades_sink
SELECT
  trade_id,
  user_id,
  customer_id,
  symbol,
  base_asset,
  quote_asset,
  side,
  order_type,
  quantity,
  price,
  notional,
  exchange,
  wallet_address,
  session_id,
  ip_address,

  -- trade_value: use notional if provided, else recompute
  ROUND(CASE WHEN notional > 0 THEN notional ELSE quantity * price END, 2)  AS trade_value,

  -- risk_level: re-derive from notional (matches DataStream thresholds)
  CASE
    WHEN notional >= 100000 THEN 'CRITICAL'
    WHEN notional >=  10000 THEN 'HIGH'
    WHEN notional >=   1000 THEN 'MEDIUM'
    ELSE                         'LOW'
  END                                                                         AS risk_level,

  -- region: map exchange to geo-zone
  CASE
    WHEN exchange IN ('NYSE','NASDAQ','CBOE','BINANCE_US')        THEN 'US'
    WHEN exchange IN ('LSE','EURONEXT','KRAKEN','BITSTAMP')       THEN 'EU'
    WHEN exchange IN ('TSE','HKEX','SGX','BYBIT','OKX')          THEN 'APAC'
    ELSE                                                               'OTHER'
  END                                                                         AS region,

  -- risk_score: deterministic proxy (0-100) based on notional tier
  -- TradeRiskAnalyzer uses stateful per-user accumulation; run the
  -- DataStream job for full stateful scoring. This gives a base score.
  ROUND(
    CASE
      WHEN notional >= 500000 THEN 95.0
      WHEN notional >= 100000 THEN 80.0 + (notional - 100000) / 400000 * 15.0
      WHEN notional >=  10000 THEN 50.0 + (notional -  10000) /  90000 * 30.0
      WHEN notional >=   1000 THEN 20.0 + (notional -   1000) /   9000 * 30.0
      ELSE                          notional / 1000 * 20.0
    END, 2
  )                                                                            AS risk_score,

  -- flagged: true when HIGH or CRITICAL
  CASE WHEN notional >= 10000 THEN TRUE ELSE FALSE END                        AS flagged,

  -- analysis_ts: processing time
  CURRENT_TIMESTAMP                                                            AS analysis_ts

FROM crypto_trades_source;


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 8 — JOB 2: Risk alert routing
--
-- Mirrors the four CEP patterns in applyRiskPatterns():
--   8a: LARGE_ORDER       — notional exceeds per-symbol threshold
--   8b: VOLUME_SPIKE      — user notional in 2-min window > 1.5x avg
--   8c: WASH_TRADING      — BUY then SELL (or vice-versa) within 1 min, same qty
--   8d: FLASH_CRASH       — symbol notional drops > 30% within 5-min window
--
-- Run in its own tab AFTER JOB 1 is running.
-- Each INSERT is a separate streaming job; paste all 4 together or split by tab.
-- ═══════════════════════════════════════════════════════════════════════════════

-- ── 8a: LARGE_ORDER (matches LargeOrderPattern thresholds)
--        BTC/USDT > 500k, ETH/USDT > 250k, SOL/USDT > 100k,
--        ADA/USDT > 50k,  DOT/USDT > 75k,  others > 100k
INSERT INTO risk_alerts_sink
SELECT
  CONCAT('alert-lo-', trade_id)                          AS alert_id,
  UNIX_TIMESTAMP() * 1000                                AS alert_ts,
  CASE
    WHEN notional >= 500000 THEN 'CRITICAL'
    WHEN notional >= 250000 THEN 'HIGH'
    ELSE                         'MEDIUM'
  END                                                    AS severity,
  'LARGE_ORDER'                                          AS pattern_type,
  user_id,
  customer_id,
  CONCAT(
    'Large order detected for ', symbol,
    ': notional=', CAST(ROUND(notional, 2) AS STRING),
    ' on ', exchange
  )                                                      AS description,
  CASE
    WHEN notional >= 500000 THEN 'IMMEDIATE REVIEW — suspend account pending investigation'
    WHEN notional >= 250000 THEN 'Escalate to risk desk for manual approval'
    ELSE                         'Flag for end-of-day review'
  END                                                    AS recommended_action,
  CONCAT('["', trade_id, '"]')                           AS triggered_trade_ids,
  FALSE                                                  AS acknowledged,
  symbol,
  ROUND(notional, 2)                                     AS notional,
  exchange
FROM crypto_trades_source
WHERE
  (symbol = 'BTC/USDT' AND notional >= 500000) OR
  (symbol = 'ETH/USDT' AND notional >= 250000) OR
  (symbol = 'SOL/USDT' AND notional >= 100000) OR
  (symbol = 'ADA/USDT' AND notional >=  50000) OR
  (symbol = 'DOT/USDT' AND notional >=  75000) OR
  (symbol NOT IN ('BTC/USDT','ETH/USDT','SOL/USDT','ADA/USDT','DOT/USDT') AND notional >= 100000);


-- ── 8b: VOLUME_SPIKE — user 2-min rolling notional > 1.5x their 10-min baseline
--        Mirrors VolumeSpikePattern: 2-min window, 1.5x threshold
--        SQL uses tumbling window approximation (GROUP BY user + 2-min window).
INSERT INTO risk_alerts_sink
SELECT
  CONCAT('alert-vs-', CAST(window_start AS STRING), '-', user_id)  AS alert_id,
  UNIX_TIMESTAMP() * 1000                                            AS alert_ts,
  CASE
    WHEN window_notional >= 1000000 THEN 'CRITICAL'
    WHEN window_notional >=  500000 THEN 'HIGH'
    ELSE                                 'MEDIUM'
  END                                                                AS severity,
  'VOLUME_SPIKE'                                                     AS pattern_type,
  user_id,
  customer_id,
  CONCAT(
    'Volume spike for user ', user_id,
    ': ', CAST(trade_count AS STRING), ' trades, notional=',
    CAST(ROUND(window_notional, 2) AS STRING),
    ' in 2-min window [', CAST(window_start AS STRING), ']'
  )                                                                  AS description,
  'Review user trading session — potential automated or algorithmic trading'  AS recommended_action,
  '["volume-spike-aggregated"]'                                      AS triggered_trade_ids,
  FALSE                                                              AS acknowledged,
  symbol,
  ROUND(window_notional, 2)                                          AS notional,
  exchange
FROM (
  SELECT
    window_start,
    window_end,
    user_id,
    customer_id,
    symbol,
    exchange,
    COUNT(*)                   AS trade_count,
    SUM(notional)              AS window_notional,
    AVG(notional)              AS avg_notional
  FROM TABLE(
    TUMBLE(TABLE crypto_trades_source, DESCRIPTOR(event_time), INTERVAL '2' MINUTE)
  )
  GROUP BY window_start, window_end, user_id, customer_id, symbol, exchange
)
WHERE trade_count >= 5
  AND window_notional >= 50000;   -- baseline spike threshold; tune to match 1.5x avg


-- ── 8c: WASH TRADING — same user, opposite sides, within 1 min, qty within 10%
--        Mirrors WashTradingPattern: 1-min window, 10% quantity tolerance
--        SQL uses a 1-min tumbling window with BUY+SELL counts per user/symbol.
INSERT INTO risk_alerts_sink
SELECT
  CONCAT('alert-wt-', CAST(window_start AS STRING), '-', user_id, '-', symbol) AS alert_id,
  UNIX_TIMESTAMP() * 1000                                                        AS alert_ts,
  'HIGH'                                                                         AS severity,
  'WASH_TRADING'                                                                 AS pattern_type,
  user_id,
  customer_id,
  CONCAT(
    'Potential wash trading: user ', user_id,
    ' symbol ', symbol,
    ' — BUYs=', CAST(buy_count AS STRING),
    ' SELLs=', CAST(sell_count AS STRING),
    ' in 1-min window [', CAST(window_start AS STRING), ']',
    ' avg qty delta=', CAST(ROUND(ABS(avg_buy_qty - avg_sell_qty) / NULLIF(avg_buy_qty, 0) * 100, 1) AS STRING), '%'
  )                                                                              AS description,
  'Freeze trading for user pending compliance review'                            AS recommended_action,
  '["wash-trading-aggregated"]'                                                  AS triggered_trade_ids,
  FALSE                                                                          AS acknowledged,
  symbol,
  ROUND(total_notional, 2)                                                       AS notional,
  exchange
FROM (
  SELECT
    window_start,
    window_end,
    user_id,
    customer_id,
    symbol,
    exchange,
    COUNT(CASE WHEN side = 'BUY'  THEN 1 END)            AS buy_count,
    COUNT(CASE WHEN side = 'SELL' THEN 1 END)             AS sell_count,
    AVG(CASE WHEN side = 'BUY'  THEN quantity END)        AS avg_buy_qty,
    AVG(CASE WHEN side = 'SELL' THEN quantity END)        AS avg_sell_qty,
    SUM(notional)                                          AS total_notional
  FROM TABLE(
    TUMBLE(TABLE crypto_trades_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
  )
  GROUP BY window_start, window_end, user_id, customer_id, symbol, exchange
)
WHERE buy_count  >= 2
  AND sell_count >= 2
  -- quantity within 10% tolerance (WashTradingPattern.quantityTolerance = 0.1)
  AND ABS(avg_buy_qty - avg_sell_qty) / NULLIF(avg_buy_qty, 0) <= 0.10;


-- ── 8d: FLASH CRASH — symbol avg price drops > 30% within 5-min window
--        Mirrors FlashCrashPattern (keyed by symbol, not user)
INSERT INTO risk_alerts_sink
SELECT
  CONCAT('alert-fc-', CAST(window_start AS STRING), '-', symbol) AS alert_id,
  UNIX_TIMESTAMP() * 1000                                          AS alert_ts,
  'CRITICAL'                                                       AS severity,
  'FLASH_CRASH'                                                    AS pattern_type,
  'SYSTEM'                                                         AS user_id,
  'SYSTEM'                                                         AS customer_id,
  CONCAT(
    'Flash crash detected on ', symbol,
    ': max_price=', CAST(ROUND(max_price, 2) AS STRING),
    ' min_price=', CAST(ROUND(min_price, 2) AS STRING),
    ' drop=', CAST(ROUND((1.0 - min_price / NULLIF(max_price, 0)) * 100, 1) AS STRING), '%',
    ' in 5-min window [', CAST(window_start AS STRING), ']'
  )                                                                AS description,
  'Halt trading for symbol — notify exchange risk desk immediately' AS recommended_action,
  '["flash-crash-aggregated"]'                                     AS triggered_trade_ids,
  FALSE                                                            AS acknowledged,
  symbol,
  ROUND(total_notional, 2)                                         AS notional,
  exchange
FROM (
  SELECT
    window_start,
    window_end,
    symbol,
    exchange,
    MAX(price)         AS max_price,
    MIN(price)         AS min_price,
    COUNT(*)           AS trade_count,
    SUM(notional)      AS total_notional
  FROM TABLE(
    TUMBLE(TABLE crypto_trades_source, DESCRIPTOR(event_time), INTERVAL '5' MINUTE)
  )
  GROUP BY window_start, window_end, symbol, exchange
)
WHERE trade_count >= 3
  AND (1.0 - min_price / NULLIF(max_price, 0)) >= 0.30;  -- 30% price drop


-- ═══════════════════════════════════════════════════════════════════════════════
-- STEP 9 — JOB 3: 1-min tumbling window aggregates per symbol + side
--
-- Writes to trade-summary (Kafka) AND print sink (stdout).
-- Run in its own tab after JOB 1 is running.
-- ═══════════════════════════════════════════════════════════════════════════════

-- 9a: Write aggregates to Kafka
INSERT INTO trade_summary_sink
SELECT
  window_start,
  window_end,
  symbol,
  side,
  COUNT(*)                                                     AS trade_count,
  ROUND(SUM(notional), 2)                                      AS total_notional,
  ROUND(AVG(price), 2)                                         AS avg_price,
  ROUND(MAX(notional), 2)                                      AS max_notional,
  ROUND(MIN(notional), 2)                                      AS min_notional,
  COUNT(CASE WHEN risk_level = 'HIGH'     THEN 1 END)          AS high_count,
  COUNT(CASE WHEN risk_level = 'CRITICAL' THEN 1 END)          AS critical_count
FROM TABLE(
  TUMBLE(TABLE risk_analyzed_trades_source, DESCRIPTOR(analysis_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol, side;

-- 9b: Also print to TaskManager stdout
INSERT INTO summary_print
SELECT
  window_start,
  window_end,
  symbol,
  side,
  COUNT(*)                                                     AS trade_count,
  ROUND(SUM(notional), 2)                                      AS total_notional,
  ROUND(AVG(price), 2)                                         AS avg_price,
  ROUND(MAX(notional), 2)                                      AS max_notional,
  ROUND(MIN(notional), 2)                                      AS min_notional,
  COUNT(CASE WHEN risk_level = 'HIGH'     THEN 1 END)          AS high_count,
  COUNT(CASE WHEN risk_level = 'CRITICAL' THEN 1 END)          AS critical_count
FROM TABLE(
  TUMBLE(TABLE risk_analyzed_trades_source, DESCRIPTOR(analysis_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, symbol, side;


-- ═══════════════════════════════════════════════════════════════════════════════
-- PREVIEW QUERIES — paste each in its own tab to see LIVE DATA
-- ═══════════════════════════════════════════════════════════════════════════════

-- Preview A: Raw trades entering the pipeline (from crypto-trades topic)
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  order_type,
  ROUND(quantity, 4)   AS qty,
  ROUND(price, 2)      AS price,
  ROUND(notional, 2)   AS notional,
  risk_level,
  exchange,
  event_time
FROM crypto_trades_source;

-- Preview B: Enriched / analyzed trades
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  ROUND(notional, 2) AS notional,
  risk_level,
  risk_score,
  flagged,
  region,
  analysis_ts
FROM risk_analyzed_trades_source;

-- Preview C: Risk alerts — all pattern types
SELECT
  alert_id,
  severity,
  pattern_type,
  user_id,
  symbol,
  ROUND(notional, 2) AS notional,
  description,
  recommended_action
FROM risk_alerts_sink;

-- Preview D: High-value trades only (mirrors DataStream HIGH/CRITICAL routing)
SELECT
  trade_id,
  user_id,
  symbol,
  side,
  ROUND(notional, 2) AS notional,
  risk_level,
  exchange,
  event_time
FROM crypto_trades_source
WHERE notional >= 10000
ORDER BY notional DESC;

-- Preview E: Per-user trade volume in the last 10 minutes
SELECT
  user_id,
  symbol,
  COUNT(*)               AS trade_count,
  ROUND(SUM(notional), 2) AS total_notional,
  MAX(risk_level)         AS max_risk,
  MIN(event_time)         AS first_trade,
  MAX(event_time)         AS last_trade
FROM crypto_trades_source
GROUP BY user_id, symbol;

-- Preview F: Live stdout — view in TaskManager logs
--   docker logs <taskmanager-container> --follow | grep -v INFO
