-- ════════════════════════════════════════════════════════════════════════════
-- CRYPTO RISK CONTROL ENGINE — FLINK SQL PIPELINE
-- Converted from: CryptoRiskEngineApplication.java (DataStream API)
-- Schemas: CryptoTrade, RiskAnalyzedTrade, RiskAlert (Avro / Confluent SR)
--
-- PIPELINE OVERVIEW:
--   SOURCE  : crypto-trades         (Avro/Confluent SR, Schema ID: 4)  → Kafka
--   PROCESS : Per-user risk scoring via CASE WHEN views
--   CEP     : Volume Spike, Wash Trading, Large Order, Flash Crash
--             → translated to MATCH_RECOGNIZE + tumble window aggregations
--   SINKS   : risk-analyzed-trades  (Avro/Confluent SR, Schema ID: 31)
--             risk-alerts           (Avro/Confluent SR, Schema ID: 32)
--
-- KAFKA TOPICS (create before running):
--   crypto-trades          -- Java app produces here (Avro)
--   risk-analyzed-trades   -- analyzed trades sink
--   risk-alerts            -- alert sink
--
-- HOW TO USE:
--   Tab 1 — Setup          : Run each SET statement individually (Ctrl+Enter)
--   Tab 2 — Tables         : Run each CREATE TABLE individually
--   Tab 3 — Views          : Run all view definitions (risk scoring + CEP prep)
--   Tab 4 — Verify         : Sanity checks before launching pipelines
--   Tab 5 — Pipeline A     : Risk analysis → risk-analyzed-trades sink
--   Tab 6 — Pipeline B     : CEP alert detection → risk-alerts sink
--
-- NOTE: All tables are TEMPORARY (session-scoped). Re-run Tabs 1–3 on each
--       new session before launching pipelines.
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 1 — "Setup"
--  Run each statement individually with Ctrl+Enter
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;


SET 'execution.runtime-mode'                    = 'streaming';
SET 'parallelism.default'                       = '2';
SET 'pipeline.operator-chaining'                = 'false';

-- State backend: RocksDB with MinIO S3 checkpointing
-- (mirrors FlinkConfig.java: EmbeddedRocksDBStateBackend + s3://flink-checkpoints)
SET 'state.backend'                             = 'rocksdb';
SET 'state.backend.incremental'                 = 'true';
SET 'state.checkpoints.dir'                     = 's3://flink-checkpoints/risk-engine-sql';

-- Checkpointing (mirrors configureCheckpointing() defaults)
SET 'execution.checkpointing.interval'          = '30000';
SET 'execution.checkpointing.mode'              = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'           = '600000';
SET 'execution.checkpointing.min-pause'         = '5000';

-- MinIO S3 credentials
-- (mirrors configureMinioForFlink() — update values to match your deployment)
SET 'fs.s3a.endpoint'                           = 'http://minio:9000';
SET 'fs.s3a.access.key'                         = 'minio';
SET 'fs.s3a.secret.key'                         = 'minio123';
SET 'fs.s3a.path.style.access'                  = 'true';
SET 'fs.s3a.impl'                               = 'org.apache.hadoop.fs.s3a.S3AFileSystem';

-- State TTL: 1 hour (mirrors table.exec.state.ttl)
SET 'table.exec.state.ttl'                      = '3600000';
SET 'table.exec.source.idle-timeout'            = '10000';

-- Mini-batch optimisation for aggregations
SET 'table.exec.mini-batch.enabled'             = 'true';
SET 'table.exec.mini-batch.allow-latency'       = '2000 ms';
SET 'table.exec.mini-batch.size'                = '500';


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 2 — "Tables"
--  Run each CREATE TABLE individually.
--  All tables are TEMPORARY (session-scoped).
--
--  DROP EXISTING TABLES FIRST (safe to run even on a fresh session)
--  Run these before the CREATE TABLE statements to avoid 'already exists' errors.
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

-- ── Drop tables if they exist ─────────────────────────────────────────────
DROP TABLE IF EXISTS CryptoTrade;
DROP TABLE IF EXISTS RiskAnalyzedTrade;
DROP TABLE IF EXISTS RiskAlert;

-- ── SOURCE: crypto-trades ─────────────────────────────────────────────────
-- Mirrors: createTradeStream() with ConfluentRegistryAvroDeserializationSchema
-- Watermark: 5-second bounded out-of-orderness (matches WatermarkStrategy config)
-- Avro enums (TradeSide, OrderType, RiskLevel) map to STRING in Flink SQL
-- walletAddress, sessionId, ipAddress are nullable → STRING (NULL when absent)
-- Subject: crypto-trades-value  |  Schema ID: 4
CREATE TABLE CryptoTrade (
                             tradeId         STRING,
                             userId          STRING,
                             customerId      STRING,
                             symbol          STRING,             -- e.g. 'BTC/USDT'
                             baseAsset       STRING,             -- e.g. 'BTC'
                             quoteAsset      STRING,             -- e.g. 'USDT'
                             side            STRING,             -- enum: 'BUY' | 'SELL'
                             orderType       STRING,             -- enum: 'MARKET' | 'LIMIT' | 'STOP_LOSS' | 'TAKE_PROFIT'
                             quantity        DOUBLE,
                             price           DOUBLE,
                             notional        DOUBLE,             -- quantity * price (pre-computed by producer)
                             `timestamp`     BIGINT,             -- epoch millis (Avro long)
                             exchange        STRING,
                             walletAddress   STRING,             -- nullable
                             riskLevel       STRING,             -- enum: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
                             sessionId       STRING,             -- nullable
                             ipAddress       STRING,             -- nullable
    -- Derived rowtime from the Avro timestamp field
                             timestamp_ts    AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
                             WATERMARK FOR timestamp_ts AS timestamp_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'crypto-trades',
    'properties.bootstrap.servers'      = 'kafka-01:29092',
    'properties.group.id'               = 'risk-engine',
    'scan.startup.mode'                 = 'earliest-offset',
    'format'                            = 'avro-confluent',
    'avro-confluent.url'                = 'http://schemaregistry0-01:8081',
    'avro-confluent.schema-id'          = '4'
);


-- ── SINK: risk-analyzed-trades ────────────────────────────────────────────
-- Mirrors: RiskAnalyzedTradeSink.java → topic 'risk-analyzed-trades'
-- Nested records (VolumeAnalysis, BehavioralAnalysis, MarketImpactAnalysis,
-- UserRiskProfile, AnalyticsMetadata) are serialised as JSON STRINGs to match
-- the Avro schema structure registered under Schema ID 31.
-- Arrays (triggeredPatterns, complianceFlags, riskMitigationActions) are
-- serialised as comma-separated STRINGs.
-- Subject: risk-analyzed-trades-value  |  Schema ID: 31
CREATE TABLE RiskAnalyzedTrade (
                                   originalTrade           STRING,     -- JSON: serialised CryptoTrade record
                                   analysisTimestamp       BIGINT,     -- epoch millis
                                   riskScore               DOUBLE,
                                   finalRiskLevel          STRING,     -- 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
                                   triggeredPatterns       STRING,     -- CSV: 'VOLUME_SPIKE,WASH_TRADING'
                                   volumeAnalysis          STRING,     -- JSON: VolumeAnalysis record
                                   behavioralAnalysis      STRING,     -- JSON: BehavioralAnalysis record
                                   marketImpactAnalysis    STRING,     -- JSON: MarketImpactAnalysis record
                                   userRiskProfile         STRING,     -- JSON: UserTradingProfile record
                                   complianceFlags         STRING,     -- CSV of ComplianceFlag enum values
                                   riskMitigationActions   STRING,     -- CSV of RiskMitigationAction enum values
                                   analyticsMetadata       STRING,     -- JSON: AnalyticsMetadata record
                                   additionalContext       STRING,     -- JSON: map<string,string>
                                   analysisTimestamp_ts    AS TO_TIMESTAMP_LTZ(analysisTimestamp, 3),
                                   WATERMARK FOR analysisTimestamp_ts AS analysisTimestamp_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'risk-analyzed-trades',
    'properties.bootstrap.servers'      = 'kafka-01:29092',
    'properties.group.id'               = 'risk-engine',
    'format'                            = 'avro-confluent',
    'avro-confluent.url'                = 'http://schemaregistry0-01:8081',
    'avro-confluent.schema-id'          = '31',
    'sink.partitioner'                  = 'round-robin'
);


-- ── SINK: risk-alerts ─────────────────────────────────────────────────────
-- Mirrors: AlertSink.java → topic 'risk-alerts'
-- Maps directly to the RiskAlert Avro schema (flat structure).
-- Subject: risk-alerts-value  |  Schema ID: 32
CREATE TABLE RiskAlert (
                           alertId             STRING,
                           `timestamp`         BIGINT,         -- epoch millis
                           severity            STRING,         -- enum: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
                           patternType         STRING,         -- 'VOLUME_SPIKE' | 'WASH_TRADING' | 'LARGE_ORDER' | 'FLASH_CRASH'
                           userId              STRING,
                           customerId          STRING,
                           description         STRING,
                           recommendedAction   STRING,
                           triggeredEvents     STRING,         -- serialised as JSON string (map<string,string>)
                           acknowledged        BOOLEAN,
                           metadata            STRING,         -- serialised as JSON string (map<string,string>)
    -- Derived rowtime
                           timestamp_ts        AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
                           WATERMARK FOR timestamp_ts AS timestamp_ts - INTERVAL '5' SECOND
) WITH (
    'connector'                         = 'kafka',
    'topic'                             = 'risk-alerts',
    'properties.bootstrap.servers'      = 'kafka-01:29092',
    'properties.group.id'               = 'risk-engine',
    'format'                            = 'avro-confluent',
    'avro-confluent.url'                = 'http://schemaregistry0-01:8081',
    'avro-confluent.schema-id'          = '32',
    'sink.partitioner'                  = 'round-robin'
);


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 3 — "Views"
--  Risk scoring logic + CEP preparation views.
--  Run all statements in this tab before launching pipelines.
--
--  VIEW HIERARCHY:
--    v_trade_enriched            ← CryptoTrade
--    v_market_impact_analysis    ← v_trade_enriched (pure projection, no join)
--    v_risk_scored_trades        ← v_trade_enriched (all scoring inline, no join)
--
--    v_volume_spike_cep          ← MATCH_RECOGNIZE: consecutive notional growth
--    v_wash_trading_cep          ← MATCH_RECOGNIZE: BUY → SELL → BUY same qty
--    v_large_order_cep           ← simple threshold filter per symbol
--    v_flash_crash_cep           ← MATCH_RECOGNIZE: 10+ consecutive SELLs
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

-- ── Drop views if they exist ──────────────────────────────────────────────
DROP VIEW IF EXISTS v_flash_crash_cep;
DROP VIEW IF EXISTS v_large_order_cep;
DROP VIEW IF EXISTS v_wash_trading_cep;
DROP VIEW IF EXISTS v_volume_spike_cep;
DROP VIEW IF EXISTS v_risk_scored_trades;
DROP VIEW IF EXISTS v_market_impact_analysis;
DROP VIEW IF EXISTS v_trade_enriched;

-- ── View 1: Trade Enriched ────────────────────────────────────────────────
-- Adds derived convenience columns used downstream.
-- Mirrors: TradeRiskAnalyzer.analyzeTradeRisk() preamble
CREATE TEMPORARY VIEW v_trade_enriched AS
SELECT
    tradeId,
    userId,
    customerId,
    symbol,
    baseAsset,
    quoteAsset,
    side,
    orderType,
    quantity,
    price,
    notional,
    `timestamp`,
    exchange,
    walletAddress,
    riskLevel,
    sessionId,
    ipAddress,
    timestamp_ts,
    -- Symbol category (mirrors getSymbolCategory())
    CASE
        WHEN symbol LIKE '%BTC%' THEN 'MAJOR'
        WHEN symbol LIKE '%ETH%' THEN 'MAJOR'
        ELSE 'MINOR'
        END AS symbolCategory,
    -- Symbol threshold for large order detection (mirrors getSymbolThreshold())
    CASE symbol
        WHEN 'BTC/USDT' THEN 500000.0
        WHEN 'ETH/USDT' THEN 250000.0
        WHEN 'SOL/USDT' THEN 100000.0
        WHEN 'ADA/USDT' THEN  50000.0
        WHEN 'DOT/USDT' THEN  75000.0
        ELSE                  50000.0
        END AS symbolLargeOrderThreshold,
    -- Symbol volume Z-score baseline (mirrors calculateSymbolVolumeZScore())
    CASE symbol
        WHEN 'BTC/USDT' THEN 50000.0
        WHEN 'ETH/USDT' THEN 25000.0
        WHEN 'SOL/USDT' THEN 10000.0
        ELSE                  10000.0
        END AS symbolVolMean,
    CASE symbol
        WHEN 'BTC/USDT' THEN 25000.0
        WHEN 'ETH/USDT' THEN 15000.0
        WHEN 'SOL/USDT' THEN  5000.0
        ELSE                   5000.0
        END AS symbolVolStddev
FROM CryptoTrade;


-- ── View 2: Market Impact Analysis ───────────────────────────────────────
-- Mirrors: analyzeMarketImpact() in TradeRiskAnalyzer.
-- Pure projection — no joins, no aggregation, always safe on empty stream.
CREATE TEMPORARY VIEW v_market_impact_analysis AS
SELECT
    tradeId,
    userId,
    symbol,
    side,
    orderType,
    notional,
    symbolLargeOrderThreshold,
    timestamp_ts,
    -- isLargeOrder
    notional >= symbolLargeOrderThreshold                           AS isLargeOrder,
    -- marketImpactScore = min(1.0, notional / 1_000_000)
    LEAST(1.0, notional / 1000000.0)                               AS marketImpactScore,
    -- liquidityConsumption = min(1.0, notional / 500_000)
    LEAST(1.0, notional / 500000.0)                                AS liquidityConsumption,
    -- priceSlippage: MARKET orders get 0.1% slippage
    CASE orderType WHEN 'MARKET' THEN 0.001 ELSE 0.0 END           AS priceSlippage,
    -- isFlashCrashContributor: SELL + notional > 100k
    CASE
        WHEN side = 'SELL' AND notional > 100000.0 THEN TRUE
        ELSE FALSE
        END                                                             AS isFlashCrashContributor
FROM v_trade_enriched;


-- ── View 3: Full Risk Score Assembly ─────────────────────────────────────
-- Mirrors: analyzeTradeRisk() → calculateOverallRiskScore() → determineRiskLevel()
--
-- DESIGN: All analysis is computed inline as CASE WHEN expressions directly
-- on v_trade_enriched. No joins to windowed aggregates — those require a
-- temporal join key that isn't available here. The z-score spike detection
-- uses the static per-symbol mean/stddev baselines from v_trade_enriched.
-- The spoofing signal uses the orderType + notional threshold inline.
-- This makes the view a pure stateless projection that works on any single
-- arriving event without waiting for window results.
CREATE TEMPORARY VIEW v_risk_scored_trades AS
SELECT
    -- ── Core trade fields ──────────────────────────────────────────────────
    tradeId,
    userId,
    customerId,
    symbol,
    baseAsset,
    quoteAsset,
    side,
    orderType,
    quantity,
    price,
    notional,
    `timestamp`,
    exchange,
    walletAddress,
    riskLevel                                           AS initialRiskLevel,
    sessionId,
    ipAddress,
    timestamp_ts,
    symbolCategory,
    symbolLargeOrderThreshold,
    symbolVolMean,
    symbolVolStddev,

    -- ── Volume Analysis (inline, no join) ─────────────────────────────────
    -- volSymbolVolumeZScore: how far this trade's notional is from symbol mean
    (notional - symbolVolMean) / symbolVolStddev        AS volSymbolVolumeZScore,
    -- volVolumePercentile: proxy using z-score (no historical user avg without join)
    LEAST(1.0, GREATEST(0.0,
                        0.5 + ((notional - symbolVolMean) / symbolVolStddev) * 0.1
               ))                                                  AS volVolumePercentile,
    -- volIsVolumeSpike: z-score > 2.0 (mirrors calculateSymbolVolumeZScore threshold)
    CASE
        WHEN ABS((notional - symbolVolMean) / symbolVolStddev) > 2.0 THEN TRUE
        ELSE FALSE
        END                                                 AS volIsVolumeSpike,

    -- ── Behavioral Analysis (inline, no join) ─────────────────────────────
    -- behIsSpoofing: LIMIT order with notional > 50k
    CASE
        WHEN orderType = 'LIMIT' AND notional > 50000.0 THEN TRUE
        ELSE FALSE
        END                                                 AS behIsSpoofing,
    -- behIsWashTrading / behIsFrontRunning: detected by CEP views, false here
    FALSE                                               AS behIsWashTrading,
    FALSE                                               AS behIsFrontRunning,
    -- behTradingVelocity: not computable per-event without state; default 0
    0.0                                                 AS behTradingVelocity,
    0.0                                                 AS behCancelToFillRatio,

    -- ── Market Impact Analysis (inline, no join) ──────────────────────────
    notional >= symbolLargeOrderThreshold               AS mktIsLargeOrder,
    LEAST(1.0, notional / 1000000.0)                   AS mktMarketImpactScore,
    LEAST(1.0, notional / 500000.0)                    AS mktLiquidityConsumption,
    CASE orderType WHEN 'MARKET' THEN 0.001 ELSE 0.0 END AS mktPriceSlippage,
    CASE
        WHEN side = 'SELL' AND notional > 100000.0 THEN TRUE
        ELSE FALSE
        END                                                 AS mktIsFlashCrashContrib,

    -- ── User Risk Profile (static defaults — no stateful join) ────────────
    0.0                                                 AS usrTotalDailyVolume,
    0                                                   AS usrDailyTradeCount,
    0.0                                                 AS usrAverageTradeSize,
    'LOW'                                               AS usrRiskTier,
    TRUE                                                AS usrIsNewUser,
    0                                                   AS usrPreviousViolations,

    -- ── Risk Score (mirrors calculateOverallRiskScore()) ──────────────────
    LEAST(1.0,
          CASE WHEN ABS((notional - symbolVolMean) / symbolVolStddev) > 2.0
                   THEN 0.3 ELSE 0.0 END +
          CASE WHEN orderType = 'LIMIT' AND notional > 50000.0
                   THEN 0.5 ELSE 0.0 END +
          CASE WHEN notional >= symbolLargeOrderThreshold
                   THEN 0.2 ELSE 0.0 END +
          CASE WHEN side = 'SELL' AND notional > 100000.0
                   THEN 0.7 ELSE 0.0 END +
          LEAST(0.3, ABS((notional - symbolVolMean) / symbolVolStddev) * 0.1)
    )                                                   AS riskScore,

    -- ── Risk Level (mirrors determineRiskLevel()) ─────────────────────────
    CASE
        WHEN LEAST(1.0,
                   CASE WHEN ABS((notional - symbolVolMean) / symbolVolStddev) > 2.0
                            THEN 0.3 ELSE 0.0 END +
                   CASE WHEN orderType = 'LIMIT' AND notional > 50000.0
                            THEN 0.5 ELSE 0.0 END +
                   CASE WHEN notional >= symbolLargeOrderThreshold
                            THEN 0.2 ELSE 0.0 END +
                   CASE WHEN side = 'SELL' AND notional > 100000.0
                            THEN 0.7 ELSE 0.0 END +
                   LEAST(0.3, ABS((notional - symbolVolMean) / symbolVolStddev) * 0.1)
             ) >= 0.7 THEN 'CRITICAL'
        WHEN LEAST(1.0,
                   CASE WHEN ABS((notional - symbolVolMean) / symbolVolStddev) > 2.0
                            THEN 0.3 ELSE 0.0 END +
                   CASE WHEN orderType = 'LIMIT' AND notional > 50000.0
                            THEN 0.5 ELSE 0.0 END +
                   CASE WHEN notional >= symbolLargeOrderThreshold
                            THEN 0.2 ELSE 0.0 END +
                   CASE WHEN side = 'SELL' AND notional > 100000.0
                            THEN 0.7 ELSE 0.0 END +
                   LEAST(0.3, ABS((notional - symbolVolMean) / symbolVolStddev) * 0.1)
             ) >= 0.5 THEN 'HIGH'
        WHEN LEAST(1.0,
                   CASE WHEN ABS((notional - symbolVolMean) / symbolVolStddev) > 2.0
                            THEN 0.3 ELSE 0.0 END +
                   CASE WHEN orderType = 'LIMIT' AND notional > 50000.0
                            THEN 0.5 ELSE 0.0 END +
                   CASE WHEN notional >= symbolLargeOrderThreshold
                            THEN 0.2 ELSE 0.0 END +
                   CASE WHEN side = 'SELL' AND notional > 100000.0
                            THEN 0.7 ELSE 0.0 END +
                   LEAST(0.3, ABS((notional - symbolVolMean) / symbolVolStddev) * 0.1)
             ) >= 0.3 THEN 'MEDIUM'
        ELSE 'LOW'
        END                                                 AS finalRiskLevel,

    -- ── Triggered Patterns CSV ────────────────────────────────────────────
    TRIM(BOTH ',' FROM CONCAT(
        CASE WHEN ABS((notional - symbolVolMean) / symbolVolStddev) > 2.0
            THEN 'VOLUME_SPIKE,'            ELSE '' END,
        CASE WHEN orderType = 'LIMIT' AND notional > 50000.0
            THEN 'SPOOFING,'                ELSE '' END,
        CASE WHEN notional >= symbolLargeOrderThreshold
            THEN 'LARGE_ORDER,'             ELSE '' END,
        CASE WHEN side = 'SELL' AND notional > 100000.0
            THEN 'FLASH_CRASH_CONTRIBUTOR,' ELSE '' END
    ))                                                  AS triggeredPatterns,

    -- ── Compliance Flags CSV (mirrors checkCompliance()) ──────────────────
    CONCAT(
            'SANCTIONS_CHECK_PASSED,AML_CHECK_PASSED,KYC_VERIFIED',
            CASE WHEN notional > 10000.0 THEN ',TAX_REPORTING_REQUIRED' ELSE '' END
    )                                                   AS complianceFlags,

    -- ── Mitigation Actions CSV (mirrors determineMitigationActions()) ─────
    CASE
        WHEN orderType = 'LIMIT' AND notional > 50000.0
            THEN 'ALERT_ONLY,TRADING_SUSPENSION'
        WHEN notional >= symbolLargeOrderThreshold
            OR ABS((notional - symbolVolMean) / symbolVolStddev) > 2.0
            THEN 'ALERT_ONLY,MANUAL_REVIEW_REQUIRED'
        WHEN side = 'SELL' AND notional > 100000.0
            THEN 'ALERT_ONLY'
        ELSE 'NONE'
        END                                                 AS riskMitigationActions

FROM v_trade_enriched;


-- ══════════════════════════════════════════════════════════════════════════
-- CEP ALERT VIEWS
-- These views translate the four CEP Pattern classes into Flink SQL
-- using MATCH_RECOGNIZE (ISO SQL 2016 row-pattern recognition).
-- ══════════════════════════════════════════════════════════════════════════

-- ── CEP View 1: Volume Spike Pattern ─────────────────────────────────────
-- Mirrors: VolumeSpikePattern.detectVolumeSpike(Time=2min, threshold=1.5)
-- Pattern: firstTrade → secondTrade (notional > first * 1.5)
--          → thirdTrade (notional > second * 1.5) within 2 minutes
-- Keyed by: userId (matches .keyBy(trade -> trade.getUserId()))
CREATE TEMPORARY VIEW v_volume_spike_cep AS
SELECT
    userId,
    firstTradeId,
    secondTradeId,
    thirdTradeId,
    firstNotional,
    secondNotional,
    thirdNotional,
    matchStart,
    matchEnd
FROM CryptoTrade
         MATCH_RECOGNIZE (
    PARTITION BY userId
    ORDER BY timestamp_ts
    MEASURES
        FIRST(A.tradeId)        AS firstTradeId,
        LAST(B.tradeId)         AS secondTradeId,
        LAST(C.tradeId)         AS thirdTradeId,
        FIRST(A.notional)       AS firstNotional,
        LAST(B.notional)        AS secondNotional,
        LAST(C.notional)        AS thirdNotional,
        FIRST(A.timestamp_ts)   AS matchStart,
        LAST(C.timestamp_ts)    AS matchEnd
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B C) WITHIN INTERVAL '2' MINUTE
    DEFINE
        -- A: minimum trade of $1000 (mirrors 'firstTrade' condition)
        A AS A.notional > 1000.0,
        -- B: same user, notional > A.notional * 1.5 (mirrors threshold=1.5)
        B AS B.notional > A.notional * 1.5,
        -- C: same user, notional > B.notional * 1.5
        C AS C.notional > LAST(B.notional) * 1.5
);


-- ── CEP View 2: Wash Trading Pattern ─────────────────────────────────────
-- Mirrors: WashTradingPattern.detectWashTrading(Time=1min, tolerance=0.1)
-- Pattern: BUY → SELL (qty within 10%) → BUY (qty within 10%) within 1 minute
-- Keyed by: userId
CREATE TEMPORARY VIEW v_wash_trading_cep AS
SELECT
    userId,
    buy1TradeId,
    sellTradeId,
    buy2TradeId,
    buy1Notional,
    sellNotional,
    buy2Notional,
    buy1Quantity,
    sellQuantity,
    buy2Quantity,
    matchStart,
    matchEnd
FROM CryptoTrade
         MATCH_RECOGNIZE (
    PARTITION BY userId
    ORDER BY timestamp_ts
    MEASURES
        FIRST(A.tradeId)        AS buy1TradeId,
        LAST(B.tradeId)         AS sellTradeId,
        LAST(C.tradeId)         AS buy2TradeId,
        FIRST(A.notional)       AS buy1Notional,
        LAST(B.notional)        AS sellNotional,
        LAST(C.notional)        AS buy2Notional,
        FIRST(A.quantity)       AS buy1Quantity,
        LAST(B.quantity)        AS sellQuantity,
        LAST(C.quantity)        AS buy2Quantity,
        FIRST(A.timestamp_ts)   AS matchStart,
        LAST(C.timestamp_ts)    AS matchEnd
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B C) WITHIN INTERVAL '1' MINUTE
    DEFINE
        -- A: BUY trade (mirrors 'buy' condition)
        A AS A.side = 'BUY',
        -- B: SELL, same user, quantity within 10% of A (mirrors sell condition)
        B AS B.side = 'SELL'
           AND ABS(A.quantity - B.quantity) / A.quantity < 0.1,
        -- C: BUY again, quantity within 10% of B (mirrors buyAgain condition)
        C AS C.side = 'BUY'
           AND ABS(LAST(B.quantity) - C.quantity) / LAST(B.quantity) < 0.1
);


-- ── CEP View 3: Large Order Pattern ──────────────────────────────────────
-- Mirrors: LargeOrderPattern.detectLargeOrders(symbolThresholds)
-- A simple filter (no sequential pattern required — single event matches).
-- Keyed by: userId
CREATE TEMPORARY VIEW v_large_order_cep AS
SELECT
    tradeId,
    userId,
    customerId,
    symbol,
    side,
    notional,
    symbolLargeOrderThreshold,
    timestamp_ts
FROM v_trade_enriched
WHERE notional >= symbolLargeOrderThreshold;


-- ── CEP View 4: Flash Crash Pattern ──────────────────────────────────────
-- Mirrors: FlashCrashPattern.detectFlashCrash(Time=5min)
-- Pattern: 10+ consecutive SELL orders within 5 minutes
-- Keyed by: symbol (matches .keyBy(trade -> trade.getSymbol()) in app)
CREATE TEMPORARY VIEW v_flash_crash_cep AS
SELECT
    symbol,
    firstTradeId,
    lastTradeId,
    sellCount,
    firstNotional,
    lastNotional,
    matchStart,
    matchEnd
FROM CryptoTrade
         MATCH_RECOGNIZE (
    PARTITION BY symbol
    ORDER BY timestamp_ts
    MEASURES
        FIRST(S.tradeId)        AS firstTradeId,
        LAST(S.tradeId)         AS lastTradeId,
        COUNT(S.tradeId)        AS sellCount,
        FIRST(S.notional)       AS firstNotional,
        LAST(S.notional)        AS lastNotional,
        FIRST(S.timestamp_ts)   AS matchStart,
        LAST(S.timestamp_ts)    AS matchEnd
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    -- 10 or more consecutive SELLs (mirrors .timesOrMore(10).consecutive())
    PATTERN (S{10,}?) WITHIN INTERVAL '5' MINUTE
    DEFINE
        S AS S.side = 'SELL'
);


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 4 — "Verify"
--  Quick sanity checks — run before launching pipelines.
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

-- ── Verify: session objects (non-blocking — never touches Kafka) ──────────
-- These are the ONLY safe verify checks when the producer is not yet running.
-- SELECT ... LIMIT on a Kafka source blocks until N rows arrive or the
-- Gateway fetch timeout fires. Use schema inspection instead.

-- Check all tables and views were created successfully (expect 10 objects)
SHOW TABLES;

-- Inspect the source table schema
SHOW COLUMNS FROM CryptoTrade;

-- Inspect the risk scored trades view schema
SHOW COLUMNS FROM v_risk_scored_trades;

-- Inspect the alert sink schema
SHOW COLUMNS FROM RiskAlert;

-- Inspect the analyzed trades sink schema
SHOW COLUMNS FROM RiskAnalyzedTrade;

-- Inspect all CEP view schemas
SHOW COLUMNS FROM v_volume_spike_cep;
SHOW COLUMNS FROM v_wash_trading_cep;
SHOW COLUMNS FROM v_large_order_cep;
SHOW COLUMNS FROM v_flash_crash_cep;

-- ── OPTIONAL: Live data checks (only run when producer IS running) ────────
-- Uncomment and run individually once your Java producer is started.
-- These are BLOCKING streaming queries — they will wait indefinitely for
-- rows if the Kafka topic is empty. Run them in a separate tab and cancel
-- manually once you have confirmed data is flowing.
--
-- SELECT tradeId, userId, symbol, side, ROUND(notional,2) AS notional,
--        riskLevel, timestamp_ts
-- FROM CryptoTrade LIMIT 10;
--
-- SELECT tradeId, userId, symbol, ROUND(riskScore,4) AS riskScore,
--        finalRiskLevel, triggeredPatterns
-- FROM v_risk_scored_trades LIMIT 10;
--
-- SELECT * FROM v_volume_spike_cep LIMIT 5;
-- SELECT * FROM v_wash_trading_cep LIMIT 5;
-- SELECT tradeId, userId, symbol, ROUND(notional,2) AS notional FROM v_large_order_cep LIMIT 5;
-- SELECT * FROM v_flash_crash_cep LIMIT 5;


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 5 — "Pipeline A — Risk Analysis Sink"
--  Mirrors: analyzedTrades → sinkTo(RiskAnalyzedTradeSink)
--
--  Submits a single long-running Flink streaming job via STATEMENT SET.
--  The job starts immediately, connects to Kafka, and sits idle waiting
--  for data — exactly like the DataStream pipeline. It does NOT fail or
--  exit if the producer is down; it waits indefinitely (Kafka consumer
--  behaviour: no messages = no output, job stays RUNNING).
--
--  Each nested Avro record (volumeAnalysis, behavioralAnalysis, etc.) is
--  serialised as a JSON string to match the RiskAnalyzedTrade schema (ID: 31).
--  originalTrade is also serialised as a JSON string of the source CryptoTrade.
--
--  Run AFTER Tab 1–3 are complete. Producer does NOT need to be running.
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

EXECUTE STATEMENT SET
    BEGIN

INSERT INTO RiskAnalyzedTrade
SELECT
    -- ── originalTrade: full CryptoTrade serialised as JSON string ──────────
    CONCAT(
            '{"tradeId":"',         tradeId,
            '","userId":"',         userId,
            '","customerId":"',     customerId,
            '","symbol":"',         symbol,
            '","baseAsset":"',      baseAsset,
            '","quoteAsset":"',     quoteAsset,
            '","side":"',           side,
            '","orderType":"',      orderType,
            '","quantity":',        CAST(quantity AS STRING),
            ',"price":',            CAST(price AS STRING),
            ',"notional":',         CAST(notional AS STRING),
            ',"timestamp":',        CAST(`timestamp` AS STRING),
            ',"exchange":"',        exchange,
            '","walletAddress":',   CASE WHEN walletAddress IS NULL THEN 'null' ELSE CONCAT('"', walletAddress, '"') END,
            ',"riskLevel":"',       initialRiskLevel,
            '","sessionId":',       CASE WHEN sessionId IS NULL THEN 'null' ELSE CONCAT('"', sessionId, '"') END,
            ',"ipAddress":',        CASE WHEN ipAddress IS NULL THEN 'null' ELSE CONCAT('"', ipAddress, '"') END,
            '}'
    )                                                   AS originalTrade,

    -- ── analysisTimestamp ──────────────────────────────────────────────────
    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT)             AS analysisTimestamp,

    -- ── riskScore ─────────────────────────────────────────────────────────
    riskScore,

    -- ── finalRiskLevel ────────────────────────────────────────────────────
    finalRiskLevel,

    -- ── triggeredPatterns (CSV) ───────────────────────────────────────────
    triggeredPatterns,

    -- ── volumeAnalysis: VolumeAnalysis record as JSON string ──────────────
    CONCAT(
            '{"isVolumeSpike":',        CASE WHEN volIsVolumeSpike THEN 'true' ELSE 'false' END,
            ',"volumePercentile":',     CAST(ROUND(volVolumePercentile, 6) AS STRING),
            ',"notionalAmount":',       CAST(ROUND(notional, 2) AS STRING),
            ',"symbolVolumeZScore":',   CAST(ROUND(volSymbolVolumeZScore, 6) AS STRING),
            '}'
    )                                                   AS volumeAnalysis,

    -- ── behavioralAnalysis: BehavioralAnalysis record as JSON string ───────
    CONCAT(
            '{"isWashTrading":',        CASE WHEN behIsWashTrading THEN 'true' ELSE 'false' END,
            ',"isSpoofing":',           CASE WHEN behIsSpoofing THEN 'true' ELSE 'false' END,
            ',"isFrontRunning":',       CASE WHEN behIsFrontRunning THEN 'true' ELSE 'false' END,
            ',"tradingVelocity":',      CAST(behTradingVelocity AS STRING),
            ',"cancelToFillRatio":',    CAST(behCancelToFillRatio AS STRING),
            '}'
    )                                                   AS behavioralAnalysis,

    -- ── marketImpactAnalysis: MarketImpactAnalysis record as JSON string ───
    CONCAT(
            '{"isLargeOrder":',         CASE WHEN mktIsLargeOrder THEN 'true' ELSE 'false' END,
            ',"marketImpactScore":',    CAST(ROUND(mktMarketImpactScore, 6) AS STRING),
            ',"liquidityConsumption":', CAST(ROUND(mktLiquidityConsumption, 6) AS STRING),
            ',"priceSlippage":',        CAST(mktPriceSlippage AS STRING),
            ',"isFlashCrashContrib":',  CASE WHEN mktIsFlashCrashContrib THEN 'true' ELSE 'false' END,
            '}'
    )                                                   AS marketImpactAnalysis,

    -- ── userRiskProfile: UserTradingProfile record as JSON string ──────────
    CONCAT(
            '{"totalDailyVolume":',     CAST(usrTotalDailyVolume AS STRING),
            ',"dailyTradeCount":',      CAST(usrDailyTradeCount AS STRING),
            ',"averageTradeSize":',     CAST(usrAverageTradeSize AS STRING),
            ',"riskTier":"',            usrRiskTier,
            '","isNewUser":',           CASE WHEN usrIsNewUser THEN 'true' ELSE 'false' END,
            ',"previousViolations":',   CAST(usrPreviousViolations AS STRING),
            '}'
    )                                                   AS userRiskProfile,

    -- ── complianceFlags (CSV) ─────────────────────────────────────────────
    complianceFlags,

    -- ── riskMitigationActions (CSV) ───────────────────────────────────────
    riskMitigationActions,

    -- ── analyticsMetadata: AnalyticsMetadata record as JSON string ─────────
    CONCAT(
            '{"cepEngineVersion":"1.0.0"',
            ',"modelVersion":"1.2.0"',
            ',"processingLatencyMs":0',
            ',"windowSizeMinutes":60',
            ',"confidenceScore":0.85',
            ',"correlationId":"', tradeId, '"',
            '}'
    )                                                   AS analyticsMetadata,

    -- ── additionalContext: empty map as JSON string ────────────────────────
    '{}'                                                AS additionalContext

FROM v_risk_scored_trades;

END;


-- ════════════════════════════════════════════════════════════════════════════
--  TAB 6 — "Pipeline B — CEP Alert Sink"
--  Mirrors: applyRiskPatterns() → allAlerts.sinkTo(AlertSink)
--
--  All four CEP alert branches submitted as ONE Flink job via STATEMENT SET.
--  This mirrors the DataStream app's union() pattern — all four pattern
--  streams share the same execution graph and run concurrently in a single
--  job. The job starts immediately and waits for Kafka data; it is fully
--  healthy and RUNNING even with zero messages in the topic.
--
--  Run AFTER Tab 1–3 are complete. Producer does NOT need to be running.
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;

EXECUTE STATEMENT SET
    BEGIN

-- ── Alert Branch 1: Volume Spike ─────────────────────────────────────────
-- Mirrors: RiskAlertFunction("VOLUME_SPIKE")
INSERT INTO RiskAlert
SELECT
    CONCAT('ALERT-VS-', vs.firstTradeId)            AS alertId,
    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT)         AS `timestamp`,
    'HIGH'                                          AS severity,
    'VOLUME_SPIKE'                                  AS patternType,
    vs.userId,
    COALESCE(t.customerId, 'CUST_' || vs.userId)    AS customerId,
    'User exhibited abnormal trading volume spike within short period'
                                                    AS description,
    'Temporarily limit user trading limits and require manual review'
                                                    AS recommendedAction,
    CONCAT('{"firstTrade":"',  vs.firstTradeId,
           ' notional=',       CAST(ROUND(vs.firstNotional,2)  AS STRING),
           '","secondTrade":"', vs.secondTradeId,
           ' notional=',       CAST(ROUND(vs.secondNotional,2) AS STRING),
           '","thirdTrade":"',  vs.thirdTradeId,
           ' notional=',       CAST(ROUND(vs.thirdNotional,2)  AS STRING), '"}')
                                                    AS triggeredEvents,
    FALSE                                           AS acknowledged,
    CONCAT('{"pattern_detected_at":"', CAST(vs.matchEnd AS STRING),
           '","events_count":"3","engine_version":"1.0.0"}')
                                                    AS metadata
FROM v_volume_spike_cep vs
         LEFT JOIN CryptoTrade t
                   ON vs.firstTradeId = t.tradeId;

-- ── Alert Branch 2: Wash Trading ─────────────────────────────────────────
-- Mirrors: RiskAlertFunction("WASH_TRADING")
INSERT INTO RiskAlert
SELECT
    CONCAT('ALERT-WT-', wt.buy1TradeId)             AS alertId,
    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT)         AS `timestamp`,
    'HIGH'                                          AS severity,
    'WASH_TRADING'                                  AS patternType,
    wt.userId,
    COALESCE(t.customerId, 'CUST_' || wt.userId)    AS customerId,
    'Potential wash trading detected with rapid buy-sell cycles'
                                                    AS description,
    'Freeze account for investigation and report to compliance'
                                                    AS recommendedAction,
    CONCAT('{"buy1":"',  wt.buy1TradeId,
           ' qty=',      CAST(ROUND(wt.buy1Quantity,4) AS STRING),
           '","sell":"', wt.sellTradeId,
           ' qty=',      CAST(ROUND(wt.sellQuantity,4) AS STRING),
           '","buy2":"', wt.buy2TradeId,
           ' qty=',      CAST(ROUND(wt.buy2Quantity,4) AS STRING), '"}')
                                                    AS triggeredEvents,
    FALSE                                           AS acknowledged,
    CONCAT('{"pattern_detected_at":"', CAST(wt.matchEnd AS STRING),
           '","events_count":"3","engine_version":"1.0.0"}')
                                                    AS metadata
FROM v_wash_trading_cep wt
         LEFT JOIN CryptoTrade t
                   ON wt.buy1TradeId = t.tradeId;

-- ── Alert Branch 3: Large Order ──────────────────────────────────────────
-- Mirrors: RiskAlertFunction("LARGE_ORDER")
INSERT INTO RiskAlert
SELECT
    CONCAT('ALERT-LO-', lo.tradeId)                 AS alertId,
    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT)         AS `timestamp`,
    'HIGH'                                          AS severity,
    'LARGE_ORDER'                                   AS patternType,
    lo.userId,
    COALESCE(t.customerId, 'CUST_' || lo.userId)    AS customerId,
    'Unusually large order detected exceeding risk thresholds'
                                                    AS description,
    'Review order legitimacy and consider partial execution'
                                                    AS recommendedAction,
    CONCAT('{"largeTrade":"', lo.tradeId,
           ' ', lo.side,
           ' ', lo.symbol,
           ' notional=',      CAST(ROUND(lo.notional,2) AS STRING),
           ' threshold=',     CAST(lo.symbolLargeOrderThreshold AS STRING), '"}')
                                                    AS triggeredEvents,
    FALSE                                           AS acknowledged,
    CONCAT('{"pattern_detected_at":"', CAST(lo.timestamp_ts AS STRING),
           '","events_count":"1","engine_version":"1.0.0"}')
                                                    AS metadata
FROM v_large_order_cep lo
         LEFT JOIN CryptoTrade t
                   ON lo.tradeId = t.tradeId;

-- ── Alert Branch 4: Flash Crash ──────────────────────────────────────────
-- Mirrors: RiskAlertFunction("FLASH_CRASH")
-- Keyed by symbol — userId defaults to 'SYSTEM' at symbol-level detection.
INSERT INTO RiskAlert
SELECT
    CONCAT('ALERT-FC-', fc.firstTradeId)            AS alertId,
    CAST(UNIX_TIMESTAMP() * 1000 AS BIGINT)         AS `timestamp`,
    'HIGH'                                          AS severity,
    'FLASH_CRASH'                                   AS patternType,
    COALESCE(t.userId, 'SYSTEM')                    AS userId,
    COALESCE(t.customerId, 'CUST_SYSTEM')           AS customerId,
    'Potential flash crash pattern detected with rapid price movements'
                                                    AS description,
    'Monitor market closely and consider circuit breaker activation'
                                                    AS recommendedAction,
    CONCAT('{"firstSell":"',  fc.firstTradeId,
           ' symbol=',        fc.symbol,
           '","lastSell":"',  fc.lastTradeId,
           '","sellCount":"', CAST(fc.sellCount AS STRING),
           '","windowSeconds":"300"}')
                                                    AS triggeredEvents,
    FALSE                                           AS acknowledged,
    CONCAT('{"pattern_detected_at":"', CAST(fc.matchEnd AS STRING),
           '","events_count":"', CAST(fc.sellCount AS STRING),
           '","engine_version":"1.0.0","symbol":"', fc.symbol, '"}')
                                                    AS metadata
FROM v_flash_crash_cep fc
         LEFT JOIN CryptoTrade t
                   ON fc.firstTradeId = t.tradeId;

END;


-- ════════════════════════════════════════════════════════════════════════════
-- END OF PIPELINE
-- ════════════════════════════════════════════════════════════════════════════
--
-- TOPIC SUMMARY:
--   INPUT  → crypto-trades           (Avro, Schema ID: 4,  produced by Java app)
--   OUTPUT → risk-analyzed-trades    (Avro, Schema ID: 31, Pipeline A)
--   OUTPUT → risk-alerts             (Avro, Schema ID: 32, Pipeline B — 4 concurrent INSERTs)
--
-- TRANSLATION NOTES:
--   1. TradeRiskAnalyzer (KeyedProcessFunction) → v_risk_scored_trades view
--      - Per-event stateless scoring — streaming-to-aggregate joins not supported without temporal key
--      - Risk scoring formulas preserved exactly from calculateOverallRiskScore()
--
--   2. CEP Patterns → MATCH_RECOGNIZE
--      - VolumeSpikePattern   → v_volume_spike_cep   (A→B→C within 2min, 1.5x growth)
--      - WashTradingPattern   → v_wash_trading_cep   (BUY→SELL→BUY within 1min, 10% qty tolerance)
--      - LargeOrderPattern    → v_large_order_cep    (single-event threshold filter)
--      - FlashCrashPattern    → v_flash_crash_cep    (S{10,}? reluctant quantifier)
--
--   3. RiskAlertFunction → alert INSERT statements with inline CASE WHEN descriptions
--
--   4. Schema (Doc 3) applied throughout:
--      - CryptoTrade:        `timestamp` BIGINT → timestamp_ts (computed rowtime)
--      - RiskAnalyzedTrade:  13 columns matching Schema ID 31 exactly;
--                            nested records serialised as JSON strings in Pipeline A
--      - RiskAlert:          `timestamp` BIGINT → timestamp_ts (computed rowtime)
--      - All connectors:     avro-confluent with schema IDs 4, 31, 32
--      - group.id:           unified to 'risk-engine' across all tables
--
--   5. Avro enums serialised as STRING in Flink SQL tables
--      (TradeSide, OrderType, RiskLevel, AlertSeverity)
--
--   6. Avro nullable unions (["null","string"]) → STRING (NULL when absent)
--
--   7. Avro maps (triggeredEvents, metadata, additionalContext) → JSON STRING via CONCAT
--
--   8. Avro arrays (triggeredPatterns, complianceFlags, riskMitigationActions)
--      → comma-separated STRING
-- ════════════════════════════════════════════════════════════════════════════