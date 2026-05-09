-- ════════════════════════════════════════════════════════════════════════
-- STR:::LAB STUDIO — BINANCE FIX API STREAMING ANALYTICS DEMO
-- Producer: binance-fix-producer (Spring Boot + QuickFIX/J)
-- Schema  : Binance FIX 4.4 tags flattened to JSON
-- Topics  : fix.orders.raw | fix.exec.reports | fix.market.data
--            fix.drop.copy | fix.alerts
--
-- SETUP: Start the producer first
--   cd binance-fix-producer && mvn spring-boot:run
--
-- HOW TO USE IN STR:::LAB STUDIO:
--   Create 5 tabs. Name them as shown per section.
--   Run Tab 1 statements one by one (Ctrl+Enter).
--   Run Tab 2 (CREATE TABLE statements) in sequence.
--   Run Tab 3 verification SELECTs to confirm data is flowing.
--   Run Tab 4 INSERT pipelines (Pipeline A — Order Flow Analytics).
--   Run Tab 5 INSERT pipelines (Pipeline B — Market Data + Risk).
-- ════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════
-- TAB 1 — "Setup"
-- Name this tab: Setup
-- Run each statement individually with Ctrl+Enter
-- Tip: Use Snippets → Config → Recommended Streaming Config as baseline
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

SET 'execution.runtime-mode'                = 'streaming';
SET 'parallelism.default'                   = '2';
SET 'pipeline.operator-chaining'            = 'false';
SET 'state.backend'                         = 'filesystem';
SET 'state.checkpoints.dir'                 = 'file:///tmp/flink-checkpoints';
SET 'execution.checkpointing.interval'      = '15000';
SET 'execution.checkpointing.mode'          = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'       = '60000';
SET 'execution.checkpointing.min-pause'     = '5000';
SET 'table.exec.state.ttl'                  = '3600000';
SET 'table.exec.source.idle-timeout'        = '10000';
SET 'table.exec.mini-batch.enabled'         = 'true';
SET 'table.exec.mini-batch.allow-latency'   = '2000 ms';
SET 'table.exec.mini-batch.size'            = '500';


-- ════════════════════════════════════════════════════════════════════════
-- TAB 2 — "Tables"
-- Name this tab: Tables
-- All tables are TEMPORARY (session-scoped).
-- Run Tab 1 first every session. Run each CREATE TABLE individually.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── SOURCE 1: Raw FIX order stream (NewOrderSingle <D> + all ExecutionReport <8>) ──
-- Produced by: FixSimulationScheduler.emitOrderLifecycle()
-- Key field   : symbol (Kafka partition key)
-- FIX Tags mapped:
--   msg_type         → tag 35  (D=NewOrderSingle, 8=ExecutionReport)
--   cl_ord_id        → tag 11
--   order_id         → tag 37
--   symbol           → tag 55
--   side             → tag 54  (BUY/SELL)
--   ord_type         → tag 40  (LIMIT/MARKET/STOP_LOSS_LIMIT)
--   order_qty        → tag 38
--   price            → tag 44
--   exec_type        → tag 150 (NEW/PARTIAL_FILL/FILL/REJECTED/F=Trade)
--   ord_status_label → tag 39  decoded (NEW/PARTIALLY_FILLED/FILLED/REJECTED/CANCELLED)
--   cum_qty          → tag 14
--   leaves_qty       → tag 151
--   avg_px           → tag 6
--   last_px          → tag 31
--   last_qty         → tag 32
--   trade_value_usd  → computed: order_qty * price
--   filled_value_usd → computed: cum_qty * avg_px
--   fill_pct         → computed: cum_qty / order_qty * 100
--   error_code       → tag 25016 (Binance custom — e.g. -1013 price filter)
--   sending_time_epoch → tag 52 in epoch ms (used as event time)
CREATE TEMPORARY TABLE fix_orders_raw (
    begin_string        STRING,
    msg_type            STRING,
    msg_seq_num         BIGINT,
    sender_comp_id      STRING,
    target_comp_id      STRING,
    sending_time        STRING,
    sending_time_epoch  BIGINT,
    cl_ord_id           STRING,
    orig_cl_ord_id      STRING,
    order_id            STRING,
    symbol              STRING,
    side                STRING,
    side_raw            INT,
    ord_type            STRING,
    ord_type_raw        INT,
    order_qty           DECIMAL(20, 6),
    price               DECIMAL(20, 2),
    stop_px             DECIMAL(20, 2),
    time_in_force       STRING,
    exec_id             STRING,
    exec_type           STRING,
    ord_status          STRING,
    ord_status_label    STRING,
    cum_qty             DECIMAL(20, 6),
    leaves_qty          DECIMAL(20, 6),
    avg_px              DECIMAL(20, 2),
    last_px             DECIMAL(20, 2),
    last_qty            DECIMAL(20, 6),
    transact_time       STRING,
    transact_time_epoch BIGINT,
    trade_value_usd     DECIMAL(20, 2),
    filled_value_usd    DECIMAL(20, 2),
    fill_pct            DECIMAL(10, 4),
    text                STRING,
    ord_rej_reason      INT,
    error_code          INT,
    session_type        STRING,
    is_drop_copy        BOOLEAN,
    producer_ts         BIGINT,
    -- Event time from sending_time_epoch (milliseconds)
    event_time AS TO_TIMESTAMP_LTZ(sending_time_epoch, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.orders.raw',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'properties.group.id'           = 'studio-fix-orders-consumer',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true',
    'scan.startup.mode'             = 'earliest-offset'
);


-- ── SOURCE 2: ExecutionReport stream only ────────────────────────────────────────
-- Produced by: FixKafkaProducer.publishExecutionReport()
-- Contains only MsgType=8 records. Subset of fix_orders_raw for fill analytics.
CREATE TEMPORARY TABLE fix_exec_reports (
    msg_type            STRING,
    msg_seq_num         BIGINT,
    sending_time_epoch  BIGINT,
    cl_ord_id           STRING,
    order_id            STRING,
    symbol              STRING,
    side                STRING,
    ord_type            STRING,
    order_qty           DECIMAL(20, 6),
    price               DECIMAL(20, 2),
    exec_id             STRING,
    exec_type           STRING,
    ord_status_label    STRING,
    cum_qty             DECIMAL(20, 6),
    leaves_qty          DECIMAL(20, 6),
    avg_px              DECIMAL(20, 2),
    last_px             DECIMAL(20, 2),
    last_qty            DECIMAL(20, 6),
    transact_time_epoch BIGINT,
    trade_value_usd     DECIMAL(20, 2),
    filled_value_usd    DECIMAL(20, 2),
    fill_pct            DECIMAL(10, 4),
    text                STRING,
    error_code          INT,
    producer_ts         BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(sending_time_epoch, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.exec.reports',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'properties.group.id'           = 'studio-fix-exec-consumer',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true',
    'scan.startup.mode'             = 'earliest-offset'
);


-- ── SOURCE 3: Market Data stream (W=Snapshot, X=IncrementalRefresh) ─────────────
-- Produced by: FixSimulationScheduler.emitMarketData()
-- FIX Tags:
--   msg_type     → W=MarketDataSnapshot, X=MarketDataIncrementalRefresh
--   md_entry_type→ tag 269 (BID/ASK/SNAPSHOT)
--   bid_px/ask_px→ best bid/offer from snapshot
--   spread       → ask - bid
--   mid_px       → (ask + bid) / 2
CREATE TEMPORARY TABLE fix_market_data (
    msg_type            STRING,
    msg_seq_num         BIGINT,
    sending_time_epoch  BIGINT,
    symbol              STRING,
    md_req_id           STRING,
    md_entry_type       STRING,
    md_entry_px         DECIMAL(20, 2),
    md_entry_size       DECIMAL(20, 6),
    md_entry_time       STRING,
    bid_px              DECIMAL(20, 2),
    ask_px              DECIMAL(20, 2),
    bid_size            DECIMAL(20, 6),
    ask_size            DECIMAL(20, 6),
    spread              DECIMAL(20, 4),
    mid_px              DECIMAL(20, 2),
    session_type        STRING,
    producer_ts         BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(sending_time_epoch, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.market.data',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'properties.group.id'           = 'studio-fix-md-consumer',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true',
    'scan.startup.mode'             = 'earliest-offset'
);


-- ── SOURCE 4: Drop Copy stream (all executions, ~1s delayed) ────────────────────
-- Produced by: FixSimulationScheduler.scheduleDropCopy()
-- Mirrors fix.exec.reports with is_drop_copy=true and drop_copy_delay_ms populated.
-- Used to validate latency drift between order-entry and drop-copy sessions.
CREATE TEMPORARY TABLE fix_drop_copy (
    msg_type            STRING,
    sending_time_epoch  BIGINT,
    cl_ord_id           STRING,
    order_id            STRING,
    symbol              STRING,
    side                STRING,
    exec_type           STRING,
    ord_status_label    STRING,
    cum_qty             DECIMAL(20, 6),
    avg_px              DECIMAL(20, 2),
    trade_value_usd     DECIMAL(20, 2),
    filled_value_usd    DECIMAL(20, 2),
    is_drop_copy        BOOLEAN,
    drop_copy_delay_ms  BIGINT,
    transact_time_epoch BIGINT,
    producer_ts         BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(sending_time_epoch, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.drop.copy',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'properties.group.id'           = 'studio-fix-dc-consumer',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true',
    'scan.startup.mode'             = 'earliest-offset'
);


-- ── SOURCE 5: Alerts stream (high-value orders + rejections) ────────────────────
CREATE TEMPORARY TABLE fix_alerts (
    msg_type            STRING,
    sending_time_epoch  BIGINT,
    cl_ord_id           STRING,
    symbol              STRING,
    side                STRING,
    ord_type            STRING,
    exec_type           STRING,
    ord_status_label    STRING,
    trade_value_usd     DECIMAL(20, 2),
    filled_value_usd    DECIMAL(20, 2),
    text                STRING,
    error_code          INT,
    producer_ts         BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(sending_time_epoch, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.alerts',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'properties.group.id'           = 'studio-fix-alerts-consumer',
    'format'                        = 'json',
    'json.ignore-parse-errors'      = 'true',
    'scan.startup.mode'             = 'earliest-offset'
);


-- ── SINK A: 1-min order flow summary → Kafka ────────────────────────────────────
-- Kafka topic: fix.out.order.summary
CREATE TEMPORARY TABLE sink_order_summary (
    window_start        TIMESTAMP(3),
    window_end          TIMESTAMP(3),
    symbol              STRING,
    side                STRING,
    ord_type            STRING,
    total_orders        BIGINT,
    new_count           BIGINT,
    filled_count        BIGINT,
    partial_count       BIGINT,
    rejected_count      BIGINT,
    total_order_value   DECIMAL(20, 2),
    total_filled_value  DECIMAL(20, 2),
    avg_fill_pct        DECIMAL(10, 4),
    avg_price           DECIMAL(20, 2),
    min_price           DECIMAL(20, 2),
    max_price           DECIMAL(20, 2)
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.out.order.summary',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'format'                        = 'json',
    'sink.partitioner'              = 'round-robin'
);


-- ── SINK B: Real-time fill metrics (every fill event) → Kafka ───────────────────
-- Kafka topic: fix.out.fills
CREATE TEMPORARY TABLE sink_fills (
    cl_ord_id           STRING,
    order_id            STRING,
    symbol              STRING,
    side                STRING,
    exec_type           STRING,
    ord_status_label    STRING,
    order_qty           DECIMAL(20, 6),
    last_qty            DECIMAL(20, 6),
    cum_qty             DECIMAL(20, 6),
    last_px             DECIMAL(20, 2),
    avg_px              DECIMAL(20, 2),
    trade_value_usd     DECIMAL(20, 2),
    filled_value_usd    DECIMAL(20, 2),
    fill_pct            DECIMAL(10, 4),
    slippage_bps        DECIMAL(10, 4),
    event_time          TIMESTAMP(3)
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.out.fills',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'format'                        = 'json',
    'sink.partitioner'              = 'round-robin'
);


-- ── SINK C: BBO spread analytics → Kafka ────────────────────────────────────────
-- Kafka topic: fix.out.bbo
CREATE TEMPORARY TABLE sink_bbo (
    window_start        TIMESTAMP(3),
    window_end          TIMESTAMP(3),
    symbol              STRING,
    avg_bid             DECIMAL(20, 4),
    avg_ask             DECIMAL(20, 4),
    avg_spread          DECIMAL(20, 6),
    min_spread          DECIMAL(20, 6),
    max_spread          DECIMAL(20, 6),
    avg_mid_px          DECIMAL(20, 4),
    avg_bid_size        DECIMAL(20, 6),
    avg_ask_size        DECIMAL(20, 6),
    snapshot_count      BIGINT
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.out.bbo',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'format'                        = 'json',
    'sink.partitioner'              = 'round-robin'
);


-- ── SINK D: Drop-copy latency drift → Kafka ─────────────────────────────────────
-- Kafka topic: fix.out.dc.latency
CREATE TEMPORARY TABLE sink_dc_latency (
    window_start            TIMESTAMP(3),
    window_end              TIMESTAMP(3),
    symbol                  STRING,
    exec_type               STRING,
    message_count           BIGINT,
    avg_delay_ms            DOUBLE,
    max_delay_ms            BIGINT,
    min_delay_ms            BIGINT,
    p99_estimated_ms        BIGINT
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.out.dc.latency',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'format'                        = 'json',
    'sink.partitioner'              = 'round-robin'
);


-- ── SINK E: Reject analysis → Kafka ─────────────────────────────────────────────
-- Kafka topic: fix.out.rejects
CREATE TEMPORARY TABLE sink_rejects (
    window_start        TIMESTAMP(3),
    window_end          TIMESTAMP(3),
    symbol              STRING,
    ord_type            STRING,
    error_code          INT,
    reject_reason       STRING,
    reject_count        BIGINT,
    total_blocked_value DECIMAL(20, 2)
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'fix.out.rejects',
    'properties.bootstrap.servers'  = 'kafka-01:29092',
    'format'                        = 'json',
    'sink.partitioner'              = 'round-robin'
);


-- ════════════════════════════════════════════════════════════════════════
-- TAB 3 — "Verify"
-- Name this tab: Verify
-- Run these SELECTs to confirm data is flowing from the producer.
-- These are bounded previews (LIMIT stops them after N rows).
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- Confirm raw FIX messages arriving (mix of D and 8 msg_types)
SELECT
    msg_type,
    symbol,
    side,
    ord_type,
    exec_type,
    ord_status_label,
    order_qty,
    price,
    trade_value_usd,
    cl_ord_id,
    event_time
FROM fix_orders_raw
LIMIT 20;

-- Confirm execution reports only (should all be msg_type=8)
SELECT
    symbol,
    side,
    exec_type,
    ord_status_label,
    cum_qty,
    avg_px,
    filled_value_usd,
    fill_pct,
    cl_ord_id
FROM fix_exec_reports
WHERE exec_type IN ('F', '1', '8')   -- Trade, PartialFill, Rejected
LIMIT 15;

-- Confirm market data snapshots
SELECT
    msg_type,
    symbol,
    bid_px,
    ask_px,
    spread,
    mid_px,
    bid_size,
    ask_size,
    event_time
FROM fix_market_data
WHERE msg_type = 'W'
LIMIT 10;

-- Confirm drop copy arriving with delay flag
SELECT
    symbol,
    exec_type,
    ord_status_label,
    is_drop_copy,
    drop_copy_delay_ms,
    trade_value_usd,
    event_time
FROM fix_drop_copy
LIMIT 10;

-- Confirm alert routing (high-value or rejected)
SELECT
    msg_type,
    symbol,
    side,
    ord_status_label,
    trade_value_usd,
    text,
    error_code
FROM fix_alerts
LIMIT 10;

-- Show all tables registered in this session
SHOW TABLES;


-- ════════════════════════════════════════════════════════════════════════
-- TAB 4 — "Pipeline A"
-- Name this tab: Pipeline A — Order Flow + Fill Analytics
-- Run AFTER Tab 1 and Tab 2. Each INSERT is a separate Flink job.
-- After running, go to Job Graph to inspect the DAG for each job.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── Pipeline A1: 1-Minute Tumble Window — Order Flow Summary ─────────────────────
-- DAG: KafkaSource(fix.orders.raw) → Filter(msg_type=D only)
--       → TumblingWindowAgg(1min GROUP BY symbol, side, ord_type)
--       → KafkaSink(fix.out.order.summary)
--
-- Why filter msg_type=D: we count submitted orders, not execution events.
-- One NewOrderSingle → many ExecutionReports; counting D avoids duplication.
INSERT INTO sink_order_summary
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE)               AS window_start,
    TUMBLE_END(event_time,   INTERVAL '1' MINUTE)               AS window_end,
    symbol,
    side,
    ord_type,
    COUNT(*)                                                     AS total_orders,
    COUNT(*) FILTER (WHERE ord_status_label = 'NEW')            AS new_count,
    COUNT(*) FILTER (WHERE ord_status_label = 'FILLED')         AS filled_count,
    COUNT(*) FILTER (WHERE ord_status_label = 'PARTIALLY_FILLED') AS partial_count,
    COUNT(*) FILTER (WHERE ord_status_label = 'REJECTED')       AS rejected_count,
    ROUND(SUM(trade_value_usd), 2)                              AS total_order_value,
    ROUND(SUM(COALESCE(filled_value_usd, 0)), 2)                AS total_filled_value,
    ROUND(AVG(COALESCE(fill_pct, 0)), 4)                        AS avg_fill_pct,
    ROUND(AVG(COALESCE(price, 0)), 2)                           AS avg_price,
    MIN(COALESCE(price, 0))                                     AS min_price,
    MAX(COALESCE(price, 0))                                     AS max_price
FROM fix_orders_raw
WHERE msg_type = 'D'    -- NewOrderSingle only; exclude ExecutionReport duplicates
GROUP BY
    symbol,
    side,
    ord_type,
    TUMBLE(event_time, INTERVAL '1' MINUTE);


-- ── Pipeline A2: Real-Time Fill Stream with Slippage ─────────────────────────────
-- DAG: KafkaSource(fix.exec.reports) → Filter(exec_type IN 1/F/TRADE)
--       → Calc(slippage_bps = (last_px - price) / price * 10000)
--       → KafkaSink(fix.out.fills)
--
-- Slippage = how far the actual fill price deviated from the limit price.
-- Positive slippage on a BUY means filled above limit (adverse).
-- Tag 31 last_px vs tag 44 price (limit).
INSERT INTO sink_fills
SELECT
    cl_ord_id,
    order_id,
    symbol,
    side,
    exec_type,
    ord_status_label,
    order_qty,
    last_qty,
    cum_qty,
    last_px,
    avg_px,
    trade_value_usd,
    COALESCE(filled_value_usd, cum_qty * avg_px)                AS filled_value_usd,
    COALESCE(fill_pct, 0)                                       AS fill_pct,
    -- Slippage in basis points: ((last_px - limit_price) / limit_price) * 10000
    CASE
        WHEN price IS NOT NULL AND price > 0 AND last_px IS NOT NULL
        THEN ROUND(((last_px - price) / price) * 10000, 4)
        ELSE 0.0
    END                                                         AS slippage_bps,
    event_time
FROM fix_exec_reports
WHERE exec_type IN ('1', 'F', '2');  -- PartialFill=1, Trade=F, Fill=2


-- ── Pipeline A3: 30-Second Reject Analysis ───────────────────────────────────────
-- DAG: KafkaSource(fix.exec.reports) → Filter(exec_type=8 REJECTED)
--       → TumblingWindowAgg(30sec GROUP BY symbol, ord_type, error_code)
--       → KafkaSink(fix.out.rejects)
--
-- Groups Binance error codes (tag 25016) to understand which filter rules
-- are being hit most often: -1013=price filter, -2010=insufficient balance, etc.
INSERT INTO sink_rejects
SELECT
    TUMBLE_START(event_time, INTERVAL '30' SECOND)              AS window_start,
    TUMBLE_END(event_time,   INTERVAL '30' SECOND)              AS window_end,
    symbol,
    ord_type,
    error_code,
    FIRST_VALUE(text)                                           AS reject_reason,
    COUNT(*)                                                    AS reject_count,
    ROUND(SUM(trade_value_usd), 2)                             AS total_blocked_value
FROM fix_exec_reports
WHERE exec_type = '8'    -- OrdStatus REJECTED (tag 150 = 8)
GROUP BY
    symbol,
    ord_type,
    error_code,
    TUMBLE(event_time, INTERVAL '30' SECOND);


-- ════════════════════════════════════════════════════════════════════════
-- TAB 5 — "Pipeline B"
-- Name this tab: Pipeline B — Market Data + Drop Copy Latency
-- Run AFTER Pipeline A is running and market data is flowing.
-- ════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- ── Pipeline B1: 30-Second BBO Spread Analytics ──────────────────────────────────
-- DAG: KafkaSource(fix.market.data) → Filter(msg_type=W SNAPSHOT)
--       → TumblingWindowAgg(30sec GROUP BY symbol)
--       → KafkaSink(fix.out.bbo)
--
-- Tracks bid-ask spread tightness over time. Wider spreads = lower liquidity.
-- Uses MarketDataSnapshot <W> messages (full BBO, not incremental deltas).
-- FIX tag 270 (MDEntryPx) projected as bid_px/ask_px in the schema.
INSERT INTO sink_bbo
SELECT
    TUMBLE_START(event_time, INTERVAL '30' SECOND)              AS window_start,
    TUMBLE_END(event_time,   INTERVAL '30' SECOND)              AS window_end,
    symbol,
    ROUND(AVG(bid_px), 4)                                       AS avg_bid,
    ROUND(AVG(ask_px), 4)                                       AS avg_ask,
    ROUND(AVG(spread), 6)                                       AS avg_spread,
    ROUND(MIN(spread), 6)                                       AS min_spread,
    ROUND(MAX(spread), 6)                                       AS max_spread,
    ROUND(AVG(mid_px), 4)                                       AS avg_mid_px,
    ROUND(AVG(bid_size), 6)                                     AS avg_bid_size,
    ROUND(AVG(ask_size), 6)                                     AS avg_ask_size,
    COUNT(*)                                                    AS snapshot_count
FROM fix_market_data
WHERE msg_type = 'W'    -- MarketDataSnapshot only; exclude IncrementalRefresh <X>
  AND bid_px IS NOT NULL
  AND ask_px IS NOT NULL
GROUP BY
    symbol,
    TUMBLE(event_time, INTERVAL '30' SECOND);


-- ── Pipeline B2: Drop Copy Latency Monitor ───────────────────────────────────────
-- DAG: KafkaSource(fix.drop.copy) → Filter(is_drop_copy=true)
--       → TumblingWindowAgg(1min GROUP BY symbol, exec_type)
--       → KafkaSink(fix.out.dc.latency)
--
-- Binance Drop Copy sessions receive all account executions with ~1s delay.
-- drop_copy_delay_ms = wall-clock delay between order-entry session send
-- and drop-copy delivery. Tracks p99 drift.
INSERT INTO sink_dc_latency
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE)               AS window_start,
    TUMBLE_END(event_time,   INTERVAL '1' MINUTE)               AS window_end,
    symbol,
    exec_type,
    COUNT(*)                                                    AS message_count,
    AVG(CAST(drop_copy_delay_ms AS DOUBLE))                     AS avg_delay_ms,
    MAX(drop_copy_delay_ms)                                     AS max_delay_ms,
    MIN(drop_copy_delay_ms)                                     AS min_delay_ms,
    -- Rough p99 estimate: avg + 2.3 * std_dev (normal distribution approximation)
    CAST(
        AVG(CAST(drop_copy_delay_ms AS DOUBLE))
        + 2.3 * STDDEV_POP(CAST(drop_copy_delay_ms AS DOUBLE))
    AS BIGINT)                                                  AS p99_estimated_ms
FROM fix_drop_copy
WHERE is_drop_copy = TRUE
GROUP BY
    symbol,
    exec_type,
    TUMBLE(event_time, INTERVAL '1' MINUTE);


-- ── Pipeline B3: Live Alert Dashboard (unbounded SELECT — use in Results tab) ────
-- DO NOT INSERT — run this as a SELECT to see live alerts streaming.
-- Use Colour Describe in the Results toolbar to highlight:
--   REJECTED → red border
--   trade_value_usd > 50000 → orange background
SELECT
    symbol,
    side,
    ord_type,
    ord_status_label,
    ROUND(trade_value_usd, 2)               AS value_usd,
    COALESCE(text, 'HIGH VALUE ORDER')      AS alert_reason,
    error_code,
    event_time
FROM fix_alerts
ORDER BY event_time DESC;


-- ── Pipeline B4: Interval Join — Correlate Orders with BBO at Order Time ──────────
-- DAG: KafkaSource(fix.orders.raw) INNER JOIN KafkaSource(fix.market.data)
--       ON symbol + 10-second interval window
--       → Calc(spread_at_order, mid_deviation)
--       → Print (results visible in Results tab)
--
-- Answers: "What was the spread when each order was placed?"
-- and "How far was the limit price from the mid-price?"
-- Uses Flink Interval Join (event-time based, no window aggregation).
SELECT
    o.cl_ord_id,
    o.symbol,
    o.side,
    o.ord_type,
    o.price                                                     AS limit_price,
    m.bid_px,
    m.ask_px,
    m.spread                                                    AS spread_at_order,
    m.mid_px,
    -- How far the limit price is from mid (positive = aggressive, negative = passive)
    CASE
        WHEN o.price IS NOT NULL AND m.mid_px IS NOT NULL
        THEN ROUND(((o.price - m.mid_px) / m.mid_px) * 10000, 2)
        ELSE NULL
    END                                                         AS limit_to_mid_bps,
    o.trade_value_usd,
    o.event_time                                               AS order_time,
    m.event_time                                               AS bbo_time
FROM fix_orders_raw AS o
JOIN fix_market_data AS m
    ON o.symbol = m.symbol
    AND m.msg_type = 'W'
    AND m.event_time BETWEEN o.event_time - INTERVAL '5' SECOND
                         AND o.event_time + INTERVAL '5' SECOND
WHERE o.msg_type = 'D'   -- NewOrderSingle only
LIMIT 50;
