-- ════════════════════════════════════════════════════════════════════════════
-- FLINKSQL STUDIO — FORMULA 1 RACE TELEMETRY PIPELINE DEMO
-- Real-time sensor & timing data from 20 F1 cars across a race weekend
--
-- SCENARIO:
--   20 cars (10 teams × 2 drivers) stream raw sensor data at ~100ms intervals.
--   Each car has 300+ sensors. We model the most critical ones:
--   engine vitals, tyre temperatures/pressures, GPS lap timing, DRS, ERS,
--   fuel load, g-forces, brake temps, gearbox events and pit-lane triggers.
--
--   Downstream pipelines detect incidents, score race positions in real time,
--   flag mechanical risk, compute fastest sectors, feed a live leaderboard
--   to the Results tab, and persist enriched data to MinIO S3 / local files.
--
-- ARCHITECTURE (10 Kafka topics, 8 Flink jobs + 1 live monitor):
--
--  [datagen: f1_car_telemetry]  ← 20 cars × 10 events/s = 200 rec/s
--       │
--       ├──► Job A  — Enrich + normalise  → f1.telemetry.enriched
--       │
--       ├──► Job B  — Lap timing + sector splits (TUMBLE 90s)
--       │              → f1.lap.times
--       │
--       ├──► Job C  — Tyre thermal model (HOP 10s/60s)
--       │              → f1.tyre.thermals
--       │
--       ├──► Job D  — Engine & power-unit risk scoring
--       │              → f1.engine.risk
--       │
--       ├──► Job E  — Race position tracker (per-lap leaderboard)
--       │              → f1.race.positions  (live Results tab view)
--       │
--       ├──► Job F  — Incident detector: lock-ups, spins, collisions
--       │              → f1.incidents
--       │
--       ├──► Job G  — Pit-stop strategy signals (SESSION window)
--       │              → f1.pit.signals
--       │
--       ├──► Job H  — Enriched archive → MinIO S3 (Parquet via Filesystem)
--       │
--       └──► Live SELECT — stream leaderboard into Results tab
--
-- KAFKA TOPICS TO CREATE:
-- ─────────────────────────────────────────────────────────────────────────
-- docker exec -it kafka-01 bash -c "
-- for topic in f1.telemetry.enriched f1.lap.times f1.tyre.thermals \
--              f1.engine.risk f1.race.positions f1.incidents \
--              f1.pit.signals; do
--   kafka-topics.sh --bootstrap-server localhost:9092 --create \
--     --topic \$topic --partitions 4 --replication-factor 1
-- done"
-- ─────────────────────────────────────────────────────────────────────────
-- MINIO BUCKET SETUP (optional):
--   mc alias set local http://localhost:9000 minioadmin minioadmin
--   mc mb local/f1-telemetry-archive
-- ─────────────────────────────────────────────────────────────────────────
-- HOW TO USE:
--   Tab 1 → Session config (SET statements + checkpointing)
--   Tab 2 → CREATE all tables
--   Tab 3 → Verify datagen source with a LIMIT SELECT
--   Tab 4 → Job A: Ingest & Enrich
--   Tab 5 → Job B+C: Lap timing & Tyre thermals
--   Tab 6 → Job D+E: Engine risk & Race positions
--   Tab 7 → Job F+G: Incidents & Pit-stop signals
--   Tab 8 → Job H: Archive to MinIO S3 or local filesystem
--   Tab 9 → Live leaderboard SELECT → Results tab
--             Filter by: "LEADER", "PIT", "LOCK-UP", "CRITICAL", driver name
--             Toggle "↓ Newest first" for latest events at top
-- ════════════════════════════════════════════════════════════════════════════


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 1 — "Session Config"
-- Run ALL lines together — sets up checkpointing, state backend, and tuning
-- ════════════════════════════════════════════════════════════════════════════

USE CATALOG default_catalog;
USE `default`;

-- Runtime
SET 'execution.runtime-mode'                                     = 'streaming';
SET 'parallelism.default'                                        = '2';
SET 'pipeline.operator-chaining'                                 = 'false';
SET 'pipeline.name'                                              = 'F1 Race Telemetry';

-- Checkpointing (every 10 s, exactly-once, retained on cancel)
SET 'execution.checkpointing.interval'                           = '10000';
SET 'execution.checkpointing.mode'                               = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout'                            = '60000';
SET 'execution.checkpointing.min-pause'                          = '3000';
SET 'execution.checkpointing.max-concurrent-checkpoints'         = '1';
SET 'execution.checkpointing.externalized-checkpoint-retention'  = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.unaligned'                          = 'false';

-- State backend (filesystem — swap to 'rocksdb' for large state)
SET 'state.backend'                                              = 'filesystem';
SET 'state.checkpoints.dir'                                      = 'file:///tmp/flink-checkpoints/f1';
SET 'state.savepoints.dir'                                       = 'file:///tmp/flink-savepoints/f1';

-- TTL — keep per-car state for 2 hours (a full race)
SET 'table.exec.state.ttl'                                       = '7200000';
SET 'table.exec.source.idle-timeout'                             = '15000';

-- Mini-batch for aggregations (reduces state I/O)
SET 'table.exec.mini-batch.enabled'                              = 'true';
SET 'table.exec.mini-batch.allow-latency'                        = '200 ms';
SET 'table.exec.mini-batch.size'                                 = '200';


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 2 — "Tables"
-- Run each CREATE TABLE with Ctrl+Enter (or select all and run)
-- ════════════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────────────────
-- SOURCE: raw car telemetry (datagen — 200 events/s, 20 cars × 10 Hz)
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE f1_car_telemetry (
    -- Identity
    car_number_raw     INT,           -- 0–19 (mapped to real car numbers below)
    driver_raw         INT,           -- 0–19  (mapped to real driver names)
    team_raw           INT,           -- 0–9   (mapped to real team names)
    circuit_raw        INT,           -- 0–5   (6 circuits in our race weekend)

    -- Positional / lap
    lap_number         INT,           -- 1–70
    sector_raw         INT,           -- 0=S1, 1=S2, 2=S3
    distance_on_lap_m  DOUBLE,        -- metres from start/finish line 0–5600
    speed_kph          DOUBLE,        -- 0–380 kph
    throttle_pct       DOUBLE,        -- 0–100 %
    brake_pct          DOUBLE,        -- 0–100 %
    steering_angle_deg DOUBLE,        -- -540 to +540 degrees
    gear_raw           INT,           -- 0–8 (0=neutral, 8=8th gear)

    -- Engine & power unit
    rpm                INT,           -- 6000–15000
    engine_temp_c      DOUBLE,        -- 80–140°C
    oil_pressure_bar   DOUBLE,        -- 2.0–8.0 bar
    fuel_load_kg       DOUBLE,        -- 0–110 kg (decreases over race)
    ers_deploy_kj      DOUBLE,        -- 0–4000 kJ per lap allocation used
    ers_harvest_kj     DOUBLE,        -- 0–2000 kJ harvested this interval
    drs_raw            INT,           -- 0=closed, 1=open

    -- Tyre
    tyre_compound_raw  INT,           -- 0=Soft, 1=Medium, 2=Hard, 3=Inter, 4=Wet
    tyre_age_laps      INT,           -- 0–40 laps on current set
    tyre_temp_fl_c     DOUBLE,        -- Front-left tyre temperature 60–130°C
    tyre_temp_fr_c     DOUBLE,
    tyre_temp_rl_c     DOUBLE,
    tyre_temp_rr_c     DOUBLE,
    tyre_press_fl_bar  DOUBLE,        -- 20–26 psi equivalent in bar
    tyre_press_fr_bar  DOUBLE,
    tyre_press_rl_bar  DOUBLE,
    tyre_press_rr_bar  DOUBLE,
    tyre_wear_pct      DOUBLE,        -- 0–100% wear on current set

    -- Brakes
    brake_temp_fl_c    DOUBLE,        -- 200–1100°C
    brake_temp_fr_c    DOUBLE,
    brake_temp_rl_c    DOUBLE,
    brake_temp_rr_c    DOUBLE,
    brake_bias_pct     DOUBLE,        -- 50–70 % (front bias)

    -- Chassis / aero
    g_lat              DOUBLE,        -- lateral g-force -6 to +6
    g_lon              DOUBLE,        -- longitudinal g-force -6 to +4
    downforce_n        DOUBLE,        -- 5000–20000 N

    -- Status flags
    pit_in_raw         INT,           -- 0=on track, 1=entering pit, 2=in pit box
    safety_car_raw     INT,           -- 0=racing, 1=VSC, 2=full SC

    event_ts           TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '2' SECOND
) WITH (
    'connector'                              = 'datagen',
    'rows-per-second'                        = '200',

    -- Identity
    'fields.car_number_raw.kind'             = 'random',
    'fields.car_number_raw.min'              = '0',
    'fields.car_number_raw.max'              = '19',
    'fields.driver_raw.kind'                 = 'random',
    'fields.driver_raw.min'                  = '0',
    'fields.driver_raw.max'                  = '19',
    'fields.team_raw.kind'                   = 'random',
    'fields.team_raw.min'                    = '0',
    'fields.team_raw.max'                    = '9',
    'fields.circuit_raw.kind'                = 'random',
    'fields.circuit_raw.min'                 = '0',
    'fields.circuit_raw.max'                 = '5',

    -- Lap / position
    'fields.lap_number.kind'                 = 'random',
    'fields.lap_number.min'                  = '1',
    'fields.lap_number.max'                  = '70',
    'fields.sector_raw.kind'                 = 'random',
    'fields.sector_raw.min'                  = '0',
    'fields.sector_raw.max'                  = '2',
    'fields.distance_on_lap_m.kind'          = 'random',
    'fields.distance_on_lap_m.min'           = '0.0',
    'fields.distance_on_lap_m.max'           = '5600.0',
    'fields.speed_kph.kind'                  = 'random',
    'fields.speed_kph.min'                   = '0.0',
    'fields.speed_kph.max'                   = '380.0',
    'fields.throttle_pct.kind'               = 'random',
    'fields.throttle_pct.min'                = '0.0',
    'fields.throttle_pct.max'                = '100.0',
    'fields.brake_pct.kind'                  = 'random',
    'fields.brake_pct.min'                   = '0.0',
    'fields.brake_pct.max'                   = '100.0',
    'fields.steering_angle_deg.kind'         = 'random',
    'fields.steering_angle_deg.min'          = '-540.0',
    'fields.steering_angle_deg.max'          = '540.0',
    'fields.gear_raw.kind'                   = 'random',
    'fields.gear_raw.min'                    = '1',
    'fields.gear_raw.max'                    = '8',

    -- Engine
    'fields.rpm.kind'                        = 'random',
    'fields.rpm.min'                         = '6000',
    'fields.rpm.max'                         = '15000',
    'fields.engine_temp_c.kind'              = 'random',
    'fields.engine_temp_c.min'               = '80.0',
    'fields.engine_temp_c.max'               = '140.0',
    'fields.oil_pressure_bar.kind'           = 'random',
    'fields.oil_pressure_bar.min'            = '2.0',
    'fields.oil_pressure_bar.max'            = '8.0',
    'fields.fuel_load_kg.kind'               = 'random',
    'fields.fuel_load_kg.min'                = '0.0',
    'fields.fuel_load_kg.max'                = '110.0',
    'fields.ers_deploy_kj.kind'              = 'random',
    'fields.ers_deploy_kj.min'               = '0.0',
    'fields.ers_deploy_kj.max'               = '4000.0',
    'fields.ers_harvest_kj.kind'             = 'random',
    'fields.ers_harvest_kj.min'              = '0.0',
    'fields.ers_harvest_kj.max'              = '2000.0',
    'fields.drs_raw.kind'                    = 'random',
    'fields.drs_raw.min'                     = '0',
    'fields.drs_raw.max'                     = '1',

    -- Tyre
    'fields.tyre_compound_raw.kind'          = 'random',
    'fields.tyre_compound_raw.min'           = '0',
    'fields.tyre_compound_raw.max'           = '2',
    'fields.tyre_age_laps.kind'              = 'random',
    'fields.tyre_age_laps.min'               = '0',
    'fields.tyre_age_laps.max'               = '40',
    'fields.tyre_temp_fl_c.kind'             = 'random',
    'fields.tyre_temp_fl_c.min'              = '60.0',
    'fields.tyre_temp_fl_c.max'              = '130.0',
    'fields.tyre_temp_fr_c.kind'             = 'random',
    'fields.tyre_temp_fr_c.min'              = '60.0',
    'fields.tyre_temp_fr_c.max'              = '130.0',
    'fields.tyre_temp_rl_c.kind'             = 'random',
    'fields.tyre_temp_rl_c.min'              = '60.0',
    'fields.tyre_temp_rl_c.max'              = '130.0',
    'fields.tyre_temp_rr_c.kind'             = 'random',
    'fields.tyre_temp_rr_c.min'              = '60.0',
    'fields.tyre_temp_rr_c.max'              = '130.0',
    'fields.tyre_press_fl_bar.kind'          = 'random',
    'fields.tyre_press_fl_bar.min'           = '1.4',
    'fields.tyre_press_fl_bar.max'           = '1.8',
    'fields.tyre_press_fr_bar.kind'          = 'random',
    'fields.tyre_press_fr_bar.min'           = '1.4',
    'fields.tyre_press_fr_bar.max'           = '1.8',
    'fields.tyre_press_rl_bar.kind'          = 'random',
    'fields.tyre_press_rl_bar.min'           = '1.2',
    'fields.tyre_press_rl_bar.max'           = '1.6',
    'fields.tyre_press_rr_bar.kind'          = 'random',
    'fields.tyre_press_rr_bar.min'           = '1.2',
    'fields.tyre_press_rr_bar.max'           = '1.6',
    'fields.tyre_wear_pct.kind'              = 'random',
    'fields.tyre_wear_pct.min'               = '0.0',
    'fields.tyre_wear_pct.max'               = '100.0',

    -- Brakes
    'fields.brake_temp_fl_c.kind'            = 'random',
    'fields.brake_temp_fl_c.min'             = '200.0',
    'fields.brake_temp_fl_c.max'             = '1100.0',
    'fields.brake_temp_fr_c.kind'            = 'random',
    'fields.brake_temp_fr_c.min'             = '200.0',
    'fields.brake_temp_fr_c.max'             = '1100.0',
    'fields.brake_temp_rl_c.kind'            = 'random',
    'fields.brake_temp_rl_c.min'             = '200.0',
    'fields.brake_temp_rl_c.max'             = '1100.0',
    'fields.brake_temp_rr_c.kind'            = 'random',
    'fields.brake_temp_rr_c.min'             = '200.0',
    'fields.brake_temp_rr_c.max'             = '1100.0',
    'fields.brake_bias_pct.kind'             = 'random',
    'fields.brake_bias_pct.min'              = '50.0',
    'fields.brake_bias_pct.max'              = '70.0',

    -- Chassis
    'fields.g_lat.kind'                      = 'random',
    'fields.g_lat.min'                       = '-6.0',
    'fields.g_lat.max'                       = '6.0',
    'fields.g_lon.kind'                      = 'random',
    'fields.g_lon.min'                       = '-6.0',
    'fields.g_lon.max'                       = '4.0',
    'fields.downforce_n.kind'                = 'random',
    'fields.downforce_n.min'                 = '5000.0',
    'fields.downforce_n.max'                 = '20000.0',

    -- Status
    'fields.pit_in_raw.kind'                 = 'random',
    'fields.pit_in_raw.min'                  = '0',
    'fields.pit_in_raw.max'                  = '2',
    'fields.safety_car_raw.kind'             = 'random',
    'fields.safety_car_raw.min'              = '0',
    'fields.safety_car_raw.max'              = '2'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: enriched telemetry → f1.telemetry.enriched
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE enriched_telemetry_sink (
    driver_name        VARCHAR,
    car_number         INT,
    team_name          VARCHAR,
    circuit_name       VARCHAR,
    lap_number         INT,
    sector             VARCHAR,
    distance_on_lap_m  DOUBLE,
    speed_kph          DOUBLE,
    throttle_pct       DOUBLE,
    brake_pct          DOUBLE,
    gear               INT,
    rpm                INT,
    engine_temp_c      DOUBLE,
    oil_pressure_bar   DOUBLE,
    fuel_load_kg       DOUBLE,
    ers_deploy_kj      DOUBLE,
    ers_harvest_kj     DOUBLE,
    drs_active         VARCHAR,
    tyre_compound      VARCHAR,
    tyre_age_laps      INT,
    tyre_temp_fl_c     DOUBLE,
    tyre_temp_fr_c     DOUBLE,
    tyre_temp_rl_c     DOUBLE,
    tyre_temp_rr_c     DOUBLE,
    tyre_temp_avg_c    DOUBLE,
    tyre_press_fl_bar  DOUBLE,
    tyre_press_fr_bar  DOUBLE,
    tyre_wear_pct      DOUBLE,
    brake_temp_fl_c    DOUBLE,
    brake_temp_fr_c    DOUBLE,
    brake_temp_max_c   DOUBLE,
    g_lat              DOUBLE,
    g_lon              DOUBLE,
    pit_status         VARCHAR,
    safety_car_status  VARCHAR,
    tyre_condition     VARCHAR,
    engine_risk_flag   VARCHAR,
    event_ts           TIMESTAMP(3)
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.telemetry.enriched',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'format'                         = 'json',
    'sink.partitioner'               = 'round-robin'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SOURCE: read back enriched telemetry for downstream jobs
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE enriched_telemetry_source (
    driver_name        VARCHAR,
    car_number         INT,
    team_name          VARCHAR,
    circuit_name       VARCHAR,
    lap_number         INT,
    sector             VARCHAR,
    distance_on_lap_m  DOUBLE,
    speed_kph          DOUBLE,
    throttle_pct       DOUBLE,
    brake_pct          DOUBLE,
    gear               INT,
    rpm                INT,
    engine_temp_c      DOUBLE,
    oil_pressure_bar   DOUBLE,
    fuel_load_kg       DOUBLE,
    ers_deploy_kj      DOUBLE,
    ers_harvest_kj     DOUBLE,
    drs_active         VARCHAR,
    tyre_compound      VARCHAR,
    tyre_age_laps      INT,
    tyre_temp_fl_c     DOUBLE,
    tyre_temp_fr_c     DOUBLE,
    tyre_temp_rl_c     DOUBLE,
    tyre_temp_rr_c     DOUBLE,
    tyre_temp_avg_c    DOUBLE,
    tyre_press_fl_bar  DOUBLE,
    tyre_press_fr_bar  DOUBLE,
    tyre_wear_pct      DOUBLE,
    brake_temp_fl_c    DOUBLE,
    brake_temp_fr_c    DOUBLE,
    brake_temp_max_c   DOUBLE,
    g_lat              DOUBLE,
    g_lon              DOUBLE,
    pit_status         VARCHAR,
    safety_car_status  VARCHAR,
    tyre_condition     VARCHAR,
    engine_risk_flag   VARCHAR,
    event_ts           TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '3' SECOND
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.telemetry.enriched',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'properties.group.id'            = 'flinksql-f1-enriched-reader',
    'scan.startup.mode'              = 'latest-offset',
    'format'                         = 'json'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: lap timing + sector splits → f1.lap.times
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE lap_times_sink (
    window_start          TIMESTAMP(3),
    window_end            TIMESTAMP(3),
    driver_name           VARCHAR,
    car_number            INT,
    team_name             VARCHAR,
    circuit_name          VARCHAR,
    lap_number            INT,
    avg_speed_kph         DOUBLE,
    max_speed_kph         DOUBLE,
    avg_throttle_pct      DOUBLE,
    avg_brake_pct         DOUBLE,
    avg_ers_deploy_kj     DOUBLE,
    total_drs_activations BIGINT,
    avg_fuel_kg           DOUBLE,
    sectors_covered       BIGINT,
    lap_classification    VARCHAR
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.lap.times',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'format'                         = 'json'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: tyre thermal model → f1.tyre.thermals
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE tyre_thermals_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    driver_name        VARCHAR,
    car_number         INT,
    team_name          VARCHAR,
    tyre_compound      VARCHAR,
    tyre_age_laps      DOUBLE,
    avg_tyre_temp_c    DOUBLE,
    max_tyre_temp_c    DOUBLE,
    min_tyre_temp_c    DOUBLE,
    temp_delta_c       DOUBLE,
    avg_tyre_wear_pct  DOUBLE,
    avg_tyre_press_bar DOUBLE,
    thermal_state      VARCHAR,
    overheating_count  BIGINT
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.tyre.thermals',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'format'                         = 'json'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: engine & power-unit risk → f1.engine.risk
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE engine_risk_sink (
    window_start       TIMESTAMP(3),
    window_end         TIMESTAMP(3),
    driver_name        VARCHAR,
    car_number         INT,
    team_name          VARCHAR,
    avg_rpm            DOUBLE,
    max_rpm            DOUBLE,
    avg_engine_temp_c  DOUBLE,
    max_engine_temp_c  DOUBLE,
    avg_oil_pressure   DOUBLE,
    min_oil_pressure   DOUBLE,
    avg_ers_deploy_kj  DOUBLE,
    total_ers_kj       DOUBLE,
    overrev_events     BIGINT,
    oil_low_events     BIGINT,
    overheat_events    BIGINT,
    risk_level         VARCHAR,
    risk_score         INT
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.engine.risk',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'format'                         = 'json'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: live race positions → f1.race.positions
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE race_positions_sink (
    window_start         TIMESTAMP(3),
    window_end           TIMESTAMP(3),
    driver_name          VARCHAR,
    car_number           INT,
    team_name            VARCHAR,
    circuit_name         VARCHAR,
    current_lap          INT,
    avg_speed_kph        DOUBLE,
    avg_fuel_kg          DOUBLE,
    avg_tyre_wear_pct    DOUBLE,
    tyre_compound        VARCHAR,
    pit_status           VARCHAR,
    safety_car_status    VARCHAR,
    engine_risk_flag     VARCHAR,
    tyre_condition       VARCHAR,
    events_count         BIGINT,
    position_class       VARCHAR
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.race.positions',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'format'                         = 'json'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: incidents (lock-ups, spins, contact) → f1.incidents
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE incidents_sink (
    incident_id        VARCHAR,
    driver_name        VARCHAR,
    car_number         INT,
    team_name          VARCHAR,
    circuit_name       VARCHAR,
    incident_type      VARCHAR,
    severity           VARCHAR,
    speed_at_event_kph DOUBLE,
    g_lat_peak         DOUBLE,
    g_lon_peak         DOUBLE,
    brake_temp_max_c   DOUBLE,
    tyre_condition     VARCHAR,
    lap_number         INT,
    sector             VARCHAR,
    event_ts           TIMESTAMP(3)
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.incidents',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'format'                         = 'json'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: pit strategy signals → f1.pit.signals
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE pit_signals_sink (
    driver_name        VARCHAR,
    car_number         INT,
    team_name          VARCHAR,
    session_start      TIMESTAMP(3),
    session_end        TIMESTAMP(3),
    laps_in_session    INT,
    avg_tyre_wear_pct  DOUBLE,
    avg_tyre_temp_c    DOUBLE,
    avg_fuel_kg        DOUBLE,
    pit_events         BIGINT,
    tyre_compound      VARCHAR,
    stop_recommendation VARCHAR
) WITH (
    'connector'                      = 'kafka',
    'topic'                          = 'f1.pit.signals',
    'properties.bootstrap.servers'   = 'kafka-01:29092',
    'format'                         = 'json'
);


-- ────────────────────────────────────────────────────────────────────────────
-- SINK: archive to MinIO S3 (Parquet via Flink Filesystem connector)
-- Change path to 'file:///tmp/f1-archive' for local filesystem fallback
-- ────────────────────────────────────────────────────────────────────────────
CREATE TEMPORARY TABLE f1_archive_sink (
    driver_name        VARCHAR,
    car_number         INT,
    team_name          VARCHAR,
    circuit_name       VARCHAR,
    lap_number         INT,
    sector             VARCHAR,
    speed_kph          DOUBLE,
    rpm                INT,
    engine_temp_c      DOUBLE,
    tyre_compound      VARCHAR,
    tyre_temp_avg_c    DOUBLE,
    tyre_wear_pct      DOUBLE,
    tyre_condition     VARCHAR,
    engine_risk_flag   VARCHAR,
    pit_status         VARCHAR,
    event_ts           TIMESTAMP(3)
) PARTITIONED BY (circuit_name, tyre_compound) WITH (
    'connector'        = 'filesystem',
    -- MinIO S3 via S3A:
    -- 'path'          = 's3a://f1-telemetry-archive/enriched/',
    -- Local filesystem (always works for demo):
    'path'             = 'file:///tmp/f1-archive/enriched/',
    'format'           = 'json',
    -- Roll a new file every 60 s or 64 MB — keeps MinIO objects manageable
    'sink.rolling-policy.rollover-interval'          = '60000',
    'sink.rolling-policy.file-size'                  = '67108864',
    'sink.rolling-policy.check-interval'             = '10000',
    'sink.partition-commit.delay'                    = '30 s',
    'sink.partition-commit.policy.kind'              = 'success-file'
);


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 3 — "Verify"
-- Quick sanity check — decodes one batch from datagen, no sinks
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    CASE driver_raw
        WHEN 0  THEN 'Max Verstappen'   WHEN 1  THEN 'Sergio Perez'
        WHEN 2  THEN 'Lewis Hamilton'   WHEN 3  THEN 'George Russell'
        WHEN 4  THEN 'Charles Leclerc'  WHEN 5  THEN 'Carlos Sainz'
        WHEN 6  THEN 'Lando Norris'     WHEN 7  THEN 'Oscar Piastri'
        WHEN 8  THEN 'Fernando Alonso'  WHEN 9  THEN 'Lance Stroll'
        WHEN 10 THEN 'Esteban Ocon'     WHEN 11 THEN 'Pierre Gasly'
        WHEN 12 THEN 'Valtteri Bottas'  WHEN 13 THEN 'Zhou Guanyu'
        WHEN 14 THEN 'Nico Hulkenberg'  WHEN 15 THEN 'Kevin Magnussen'
        WHEN 16 THEN 'Yuki Tsunoda'     WHEN 17 THEN 'Daniel Ricciardo'
        WHEN 18 THEN 'Logan Sargeant'   ELSE    'Alexander Albon'
        END                                        AS driver_name,
    CASE team_raw
        WHEN 0 THEN 'Red Bull Racing'   WHEN 1 THEN 'Mercedes'
        WHEN 2 THEN 'Ferrari'           WHEN 3 THEN 'McLaren'
        WHEN 4 THEN 'Aston Martin'      WHEN 5 THEN 'Alpine'
        WHEN 6 THEN 'Alfa Romeo'        WHEN 7 THEN 'Haas'
        WHEN 8 THEN 'AlphaTauri'        ELSE   'Williams'
        END                                        AS team,
    CASE circuit_raw
        WHEN 0 THEN 'Monza'         WHEN 1 THEN 'Silverstone'
        WHEN 2 THEN 'Monaco'        WHEN 3 THEN 'Spa-Francorchamps'
        WHEN 4 THEN 'Suzuka'        ELSE   'Interlagos'
        END                                        AS circuit,
    lap_number,
    CASE sector_raw WHEN 0 THEN 'S1' WHEN 1 THEN 'S2' ELSE 'S3' END AS sector,
    ROUND(speed_kph, 1)                        AS speed_kph,
    rpm,
    ROUND(engine_temp_c, 1)                    AS engine_temp_c,
    CASE tyre_compound_raw
        WHEN 0 THEN 'SOFT' WHEN 1 THEN 'MEDIUM' WHEN 2 THEN 'HARD'
        WHEN 3 THEN 'INTER' ELSE 'WET'
        END                                        AS tyre_compound,
    ROUND((tyre_temp_fl_c + tyre_temp_fr_c + tyre_temp_rl_c + tyre_temp_rr_c) / 4.0, 1)
        AS tyre_temp_avg_c,
    ROUND(tyre_wear_pct, 1)                    AS tyre_wear_pct,
    CASE
        WHEN engine_temp_c > 130 THEN 'CRITICAL'
        WHEN engine_temp_c > 120 THEN 'WARNING'
        ELSE 'NORMAL'
        END                                        AS engine_status
FROM f1_car_telemetry
         LIMIT 20;

SHOW TABLES;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 4 — "Job A: Ingest & Enrich"
-- Decodes all raw integers, derives tyre/engine status flags,
-- writes to f1.telemetry.enriched Kafka topic
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO enriched_telemetry_sink
SELECT
    -- Driver & car identity
    CASE driver_raw
        WHEN 0  THEN 'Max Verstappen'   WHEN 1  THEN 'Sergio Perez'
        WHEN 2  THEN 'Lewis Hamilton'   WHEN 3  THEN 'George Russell'
        WHEN 4  THEN 'Charles Leclerc'  WHEN 5  THEN 'Carlos Sainz'
        WHEN 6  THEN 'Lando Norris'     WHEN 7  THEN 'Oscar Piastri'
        WHEN 8  THEN 'Fernando Alonso'  WHEN 9  THEN 'Lance Stroll'
        WHEN 10 THEN 'Esteban Ocon'     WHEN 11 THEN 'Pierre Gasly'
        WHEN 12 THEN 'Valtteri Bottas'  WHEN 13 THEN 'Zhou Guanyu'
        WHEN 14 THEN 'Nico Hulkenberg'  WHEN 15 THEN 'Kevin Magnussen'
        WHEN 16 THEN 'Yuki Tsunoda'     WHEN 17 THEN 'Daniel Ricciardo'
        WHEN 18 THEN 'Logan Sargeant'   ELSE    'Alexander Albon'
        END                                                AS driver_name,
    -- map driver_raw → official car number (1-indexed real car numbers)
    CASE car_number_raw
        WHEN 0 THEN 1  WHEN 1 THEN 11 WHEN 2 THEN 44 WHEN 3 THEN 63
        WHEN 4 THEN 16 WHEN 5 THEN 55 WHEN 6 THEN 4  WHEN 7 THEN 81
        WHEN 8 THEN 14 WHEN 9 THEN 18 WHEN 10 THEN 31 WHEN 11 THEN 10
        WHEN 12 THEN 77 WHEN 13 THEN 24 WHEN 14 THEN 27 WHEN 15 THEN 20
        WHEN 16 THEN 22 WHEN 17 THEN 3  WHEN 18 THEN 2  ELSE 23
        END                                                AS car_number,
    CASE team_raw
        WHEN 0 THEN 'Red Bull Racing'   WHEN 1 THEN 'Mercedes'
        WHEN 2 THEN 'Ferrari'           WHEN 3 THEN 'McLaren'
        WHEN 4 THEN 'Aston Martin'      WHEN 5 THEN 'Alpine'
        WHEN 6 THEN 'Alfa Romeo'        WHEN 7 THEN 'Haas'
        WHEN 8 THEN 'AlphaTauri'        ELSE   'Williams'
        END                                                AS team_name,
    CASE circuit_raw
        WHEN 0 THEN 'Monza'         WHEN 1 THEN 'Silverstone'
        WHEN 2 THEN 'Monaco'        WHEN 3 THEN 'Spa-Francorchamps'
        WHEN 4 THEN 'Suzuka'        ELSE   'Interlagos'
        END                                                AS circuit_name,
    lap_number,
    CASE sector_raw WHEN 0 THEN 'S1' WHEN 1 THEN 'S2' ELSE 'S3' END
                                                           AS sector,
    ROUND(distance_on_lap_m, 1)                        AS distance_on_lap_m,
    ROUND(speed_kph, 2)                                AS speed_kph,
    ROUND(throttle_pct, 1)                             AS throttle_pct,
    ROUND(brake_pct, 1)                                AS brake_pct,
    gear_raw                                           AS gear,
    rpm,
    ROUND(engine_temp_c, 1)                            AS engine_temp_c,
    ROUND(oil_pressure_bar, 2)                         AS oil_pressure_bar,
    ROUND(fuel_load_kg, 2)                             AS fuel_load_kg,
    ROUND(ers_deploy_kj, 1)                            AS ers_deploy_kj,
    ROUND(ers_harvest_kj, 1)                           AS ers_harvest_kj,
    CASE drs_raw WHEN 1 THEN 'OPEN' ELSE 'CLOSED' END AS drs_active,
    CASE tyre_compound_raw
        WHEN 0 THEN 'SOFT' WHEN 1 THEN 'MEDIUM' WHEN 2 THEN 'HARD'
        WHEN 3 THEN 'INTER' ELSE 'WET'
        END                                                AS tyre_compound,
    tyre_age_laps,
    ROUND(tyre_temp_fl_c, 1)                           AS tyre_temp_fl_c,
    ROUND(tyre_temp_fr_c, 1)                           AS tyre_temp_fr_c,
    ROUND(tyre_temp_rl_c, 1)                           AS tyre_temp_rl_c,
    ROUND(tyre_temp_rr_c, 1)                           AS tyre_temp_rr_c,
    ROUND((tyre_temp_fl_c+tyre_temp_fr_c+tyre_temp_rl_c+tyre_temp_rr_c)/4.0, 1)
                                                           AS tyre_temp_avg_c,
    ROUND(tyre_press_fl_bar, 3)                        AS tyre_press_fl_bar,
    ROUND(tyre_press_fr_bar, 3)                        AS tyre_press_fr_bar,
    ROUND(tyre_wear_pct, 1)                            AS tyre_wear_pct,
    ROUND(brake_temp_fl_c, 1)                          AS brake_temp_fl_c,
    ROUND(brake_temp_fr_c, 1)                          AS brake_temp_fr_c,
    ROUND(GREATEST(brake_temp_fl_c, brake_temp_fr_c, brake_temp_rl_c, brake_temp_rr_c), 1)
                                                           AS brake_temp_max_c,
    ROUND(g_lat, 3)                                    AS g_lat,
    ROUND(g_lon, 3)                                    AS g_lon,
    CASE pit_in_raw
        WHEN 0 THEN 'ON_TRACK'
        WHEN 1 THEN 'PIT_ENTRY'
        ELSE        'IN_PIT_BOX'
        END                                                AS pit_status,
    CASE safety_car_raw
        WHEN 0 THEN 'RACING'
        WHEN 1 THEN 'VSC'
        ELSE        'SAFETY_CAR'
        END                                                AS safety_car_status,
    -- Tyre condition composite
    CASE
        WHEN (tyre_temp_fl_c+tyre_temp_fr_c+tyre_temp_rl_c+tyre_temp_rr_c)/4.0 > 115
            OR tyre_wear_pct > 80  THEN 'CRITICAL'
        WHEN (tyre_temp_fl_c+tyre_temp_fr_c+tyre_temp_rl_c+tyre_temp_rr_c)/4.0 > 105
            OR tyre_wear_pct > 60  THEN 'DEGRADED'
        WHEN (tyre_temp_fl_c+tyre_temp_fr_c+tyre_temp_rl_c+tyre_temp_rr_c)/4.0 < 75
            THEN 'COLD'
        ELSE                            'OPTIMAL'
        END                                                AS tyre_condition,
    -- Engine risk flag
    CASE
        WHEN engine_temp_c > 135 OR rpm > 14500 OR oil_pressure_bar < 2.5
            THEN 'CRITICAL'
        WHEN engine_temp_c > 125 OR rpm > 14000 OR oil_pressure_bar < 3.0
            THEN 'WARNING'
        ELSE 'NORMAL'
        END                                                AS engine_risk_flag,
    event_ts
FROM f1_car_telemetry;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 5 — "Job B+C: Lap Timing & Tyre Thermals"
-- Wait ~20s after Job A starts before running these
-- ════════════════════════════════════════════════════════════════════════════

-- ── Job B: TUMBLE(90s) — Lap timing & sector performance ─────────────────
INSERT INTO lap_times_sink
SELECT
    TUMBLE_START(event_ts, INTERVAL '90' SECOND)              AS window_start,
    TUMBLE_END(event_ts, INTERVAL '90' SECOND)                AS window_end,
    driver_name,
    car_number,
    team_name,
    circuit_name,
    -- Take the most recent lap number seen in this window
    MAX(lap_number)                                           AS lap_number,
    ROUND(AVG(speed_kph), 2)                                  AS avg_speed_kph,
    ROUND(MAX(speed_kph), 2)                                  AS max_speed_kph,
    ROUND(AVG(throttle_pct), 2)                               AS avg_throttle_pct,
    ROUND(AVG(brake_pct), 2)                                  AS avg_brake_pct,
    ROUND(AVG(ers_deploy_kj), 1)                              AS avg_ers_deploy_kj,
    COUNT(CASE WHEN drs_active = 'OPEN' THEN 1 END)           AS total_drs_activations,
    ROUND(AVG(fuel_load_kg), 2)                               AS avg_fuel_kg,
    COUNT(DISTINCT sector)                                    AS sectors_covered,
    CASE
        WHEN AVG(speed_kph) > 250 AND MAX(speed_kph) > 320   THEN 'FLYING_LAP'
        WHEN COUNT(CASE WHEN pit_status <> 'ON_TRACK' THEN 1 END) > 0
            THEN 'PIT_LAP'
        WHEN MIN(speed_kph) < 60                              THEN 'SLOW_LAP'
        ELSE                                                       'NORMAL_LAP'
        END                                                       AS lap_classification
FROM enriched_telemetry_source
GROUP BY
    TUMBLE(event_ts, INTERVAL '90' SECOND),
    driver_name,
    car_number,
    team_name,
    circuit_name;


-- ── Job C: HOP(10s/60s) — Rolling tyre thermal model ─────────────────────
INSERT INTO tyre_thermals_sink
SELECT
    HOP_START(event_ts, INTERVAL '10' SECOND, INTERVAL '60' SECOND) AS window_start,
    HOP_END(event_ts, INTERVAL '10' SECOND, INTERVAL '60' SECOND)   AS window_end,
    driver_name,
    car_number,
    team_name,
    tyre_compound,
    ROUND(AVG(CAST(tyre_age_laps AS DOUBLE)), 1)                     AS tyre_age_laps,
    ROUND(AVG(tyre_temp_avg_c), 2)                                   AS avg_tyre_temp_c,
    ROUND(MAX(tyre_temp_avg_c), 2)                                   AS max_tyre_temp_c,
    ROUND(MIN(tyre_temp_avg_c), 2)                                   AS min_tyre_temp_c,
    ROUND(MAX(tyre_temp_avg_c) - MIN(tyre_temp_avg_c), 2)            AS temp_delta_c,
    ROUND(AVG(tyre_wear_pct), 2)                                     AS avg_tyre_wear_pct,
    ROUND(AVG(tyre_press_fl_bar), 3)                                 AS avg_tyre_press_bar,
    CASE
        WHEN AVG(tyre_temp_avg_c) > 115 OR AVG(tyre_wear_pct) > 80  THEN 'GRAINING'
        WHEN AVG(tyre_temp_avg_c) > 105                              THEN 'OVERHEATING'
        WHEN AVG(tyre_temp_avg_c) < 75                               THEN 'COLD'
        WHEN AVG(tyre_wear_pct) > 60                                 THEN 'HIGH_WEAR'
        ELSE                                                               'NOMINAL'
        END                                                              AS thermal_state,
    COUNT(CASE WHEN tyre_temp_avg_c > 115 THEN 1 END)                AS overheating_count
FROM enriched_telemetry_source
GROUP BY
    HOP(event_ts, INTERVAL '10' SECOND, INTERVAL '60' SECOND),
    driver_name,
    car_number,
    team_name,
    tyre_compound;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 6 — "Job D+E: Engine Risk & Race Positions"
-- Wait ~20s after Job A starts before running these
-- ════════════════════════════════════════════════════════════════════════════

-- ── Job D: TUMBLE(30s) — Engine & power-unit risk ─────────────────────────
INSERT INTO engine_risk_sink
SELECT
    TUMBLE_START(event_ts, INTERVAL '30' SECOND)              AS window_start,
    TUMBLE_END(event_ts, INTERVAL '30' SECOND)                AS window_end,
    driver_name,
    car_number,
    team_name,
    ROUND(AVG(CAST(rpm AS DOUBLE)), 0)                         AS avg_rpm,
    ROUND(MAX(CAST(rpm AS DOUBLE)), 0)                         AS max_rpm,
    ROUND(AVG(engine_temp_c), 2)                               AS avg_engine_temp_c,
    ROUND(MAX(engine_temp_c), 2)                               AS max_engine_temp_c,
    ROUND(AVG(oil_pressure_bar), 3)                            AS avg_oil_pressure,
    ROUND(MIN(oil_pressure_bar), 3)                            AS min_oil_pressure,
    ROUND(AVG(ers_deploy_kj), 1)                               AS avg_ers_deploy_kj,
    ROUND(SUM(ers_deploy_kj), 1)                               AS total_ers_kj,
    -- Dangerous events within this window
    COUNT(CASE WHEN rpm > 14500 THEN 1 END)                    AS overrev_events,
    COUNT(CASE WHEN oil_pressure_bar < 2.5 THEN 1 END)         AS oil_low_events,
    COUNT(CASE WHEN engine_temp_c > 135 THEN 1 END)            AS overheat_events,
    CASE
        WHEN MAX(engine_temp_c) > 135 OR MIN(oil_pressure_bar) < 2.5
            OR COUNT(CASE WHEN rpm > 14500 THEN 1 END) > 5     THEN 'CRITICAL'
        WHEN MAX(engine_temp_c) > 125 OR MIN(oil_pressure_bar) < 3.0
            OR COUNT(CASE WHEN rpm > 14000 THEN 1 END) > 10    THEN 'HIGH'
        WHEN MAX(engine_temp_c) > 115
            OR COUNT(CASE WHEN rpm > 13500 THEN 1 END) > 20    THEN 'MEDIUM'
        ELSE                                                        'LOW'
        END                                                        AS risk_level,
    CAST(
            CASE
                WHEN MAX(engine_temp_c) > 135 OR MIN(oil_pressure_bar) < 2.5 THEN 100
                WHEN MAX(engine_temp_c) > 125 OR MIN(oil_pressure_bar) < 3.0 THEN 75
                WHEN MAX(engine_temp_c) > 115                                 THEN 50
                ELSE 25
                END AS INT
    )                                                          AS risk_score
FROM enriched_telemetry_source
GROUP BY
    TUMBLE(event_ts, INTERVAL '30' SECOND),
    driver_name,
    car_number,
    team_name;


-- ── Job E: TUMBLE(15s) — Live race position tracker ───────────────────────
-- Aggregates per-car over 15-second windows: speed, fuel, tyre state.
-- In Results tab filter by "LEADER", "PIT", or a driver name.
INSERT INTO race_positions_sink
SELECT
    TUMBLE_START(event_ts, INTERVAL '15' SECOND)             AS window_start,
    TUMBLE_END(event_ts, INTERVAL '15' SECOND)               AS window_end,
    driver_name,
    car_number,
    team_name,
    circuit_name,
    MAX(lap_number)                                          AS current_lap,
    ROUND(AVG(speed_kph), 2)                                 AS avg_speed_kph,
    ROUND(AVG(fuel_load_kg), 2)                              AS avg_fuel_kg,
    ROUND(AVG(tyre_wear_pct), 2)                             AS avg_tyre_wear_pct,
    -- MAX() on string picks the alphabetically greatest value seen in the window;
    -- for compound: SOFT > MEDIUM > INTER > HARD (good enough for a window snapshot)
    -- FIRST_VALUE is not supported in window agg (no merge() implementation in Flink)
    MAX(tyre_compound)                                       AS tyre_compound,
    MAX(pit_status)                                          AS pit_status,
    MAX(safety_car_status)                                   AS safety_car_status,
    MAX(engine_risk_flag)                                    AS engine_risk_flag,
    MAX(tyre_condition)                                      AS tyre_condition,
    COUNT(*)                                                 AS events_count,
    CASE
        WHEN AVG(speed_kph) > 280                            THEN 'PUSHING'
        WHEN AVG(speed_kph) > 200                            THEN 'RACING'
        WHEN MAX(CASE WHEN pit_status <> 'ON_TRACK' THEN 1 ELSE 0 END) = 1
            THEN 'PIT'
        WHEN AVG(speed_kph) < 100                            THEN 'SLOW_ZONE'
        ELSE                                                      'CRUISING'
        END                                                      AS position_class
FROM enriched_telemetry_source
GROUP BY
    TUMBLE(event_ts, INTERVAL '15' SECOND),
    driver_name,
    car_number,
    team_name,
    circuit_name;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 7 — "Job F+G: Incidents & Pit Strategy"
-- Wait ~20s after Job A starts before running these
-- ════════════════════════════════════════════════════════════════════════════

-- ── Job F: Per-event incident detection ───────────────────────────────────
-- Detects lock-ups (high brake + speed + lateral g), spins (extreme g_lat),
-- possible contact (sudden deceleration g_lon spike)
INSERT INTO incidents_sink
SELECT
    CONCAT('INC-', driver_name, '-', CAST(lap_number AS VARCHAR), '-',
           CAST(CAST(event_ts AS BIGINT) AS VARCHAR))         AS incident_id,
    driver_name,
    car_number,
    team_name,
    circuit_name,
    CASE
        WHEN ABS(g_lat) > 5.0 AND speed_kph > 100            THEN 'SPIN'
        WHEN g_lon < -5.0 AND speed_kph > 150                THEN 'HEAVY_BRAKING_EVENT'
        WHEN brake_pct > 80 AND ABS(g_lat) > 3.0
            AND speed_kph > 80                               THEN 'LOCK-UP'
        WHEN g_lon < -4.0 AND brake_pct < 20                 THEN 'POSSIBLE_CONTACT'
        WHEN brake_temp_max_c > 1050                          THEN 'BRAKE_OVERTEMP'
        ELSE                                                       'ANOMALY'
        END                                                       AS incident_type,
    CASE
        WHEN ABS(g_lat) > 5.5 OR g_lon < -5.5                THEN 'CRITICAL'
        WHEN ABS(g_lat) > 4.5 OR g_lon < -4.5                THEN 'HIGH'
        WHEN ABS(g_lat) > 3.5 OR g_lon < -3.5                THEN 'MEDIUM'
        ELSE                                                       'LOW'
        END                                                       AS severity,
    ROUND(speed_kph, 1)                                       AS speed_at_event_kph,
    ROUND(g_lat, 3)                                           AS g_lat_peak,
    ROUND(g_lon, 3)                                           AS g_lon_peak,
    ROUND(brake_temp_max_c, 1)                                AS brake_temp_max_c,
    tyre_condition,
    lap_number,
    sector,
    event_ts
FROM enriched_telemetry_source
WHERE
    ABS(g_lat) > 3.5          -- significant lateral g
   OR g_lon < -4.0           -- heavy longitudinal deceleration
   OR brake_temp_max_c > 1000;  -- brake overheat


-- ── Job G: SESSION window — pit-stop strategy signals ────────────────────
-- A session is a continuous block of laps without a pit stop.
-- Session closes when a car goes into the pit box (pit_status = IN_PIT_BOX)
-- and stays silent for 30 seconds.
INSERT INTO pit_signals_sink
SELECT
    driver_name,
    car_number,
    team_name,
    SESSION_START(event_ts, INTERVAL '30' SECOND)            AS session_start,
    SESSION_END(event_ts, INTERVAL '30' SECOND)              AS session_end,
    CAST(MAX(lap_number) - MIN(lap_number) AS INT)           AS laps_in_session,
    ROUND(AVG(tyre_wear_pct), 2)                             AS avg_tyre_wear_pct,
    ROUND(AVG(tyre_temp_avg_c), 2)                           AS avg_tyre_temp_c,
    ROUND(AVG(fuel_load_kg), 2)                              AS avg_fuel_kg,
    COUNT(CASE WHEN pit_status = 'IN_PIT_BOX' THEN 1 END)   AS pit_events,
    -- MAX() on string is safe inside SESSION windows; FIRST_VALUE has no merge()
    MAX(tyre_compound)                                       AS tyre_compound,
    CASE
        WHEN AVG(tyre_wear_pct) > 75                         THEN 'STOP_NOW'
        WHEN AVG(tyre_wear_pct) > 55                         THEN 'PREPARE_STOP'
        WHEN AVG(tyre_temp_avg_c) > 110                      THEN 'THERMAL_DEGRADATION'
        WHEN AVG(fuel_load_kg) < 15                          THEN 'FUEL_CRITICAL'
        ELSE                                                      'CONTINUE'
        END                                                      AS stop_recommendation
FROM enriched_telemetry_source
GROUP BY
    SESSION(event_ts, INTERVAL '30' SECOND),
    driver_name,
    car_number,
    team_name;


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 8 — "Job H: Archive to MinIO S3 / Local Filesystem"
-- Persists enriched telemetry snapshots partitioned by circuit + tyre compound
-- Swap the 'path' in the f1_archive_sink DDL above for MinIO S3 path
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO f1_archive_sink
SELECT
    driver_name,
    car_number,
    team_name,
    circuit_name,
    lap_number,
    sector,
    ROUND(speed_kph, 2)         AS speed_kph,
    rpm,
    ROUND(engine_temp_c, 1)     AS engine_temp_c,
    tyre_compound,
    ROUND(tyre_temp_avg_c, 1)   AS tyre_temp_avg_c,
    ROUND(tyre_wear_pct, 1)     AS tyre_wear_pct,
    tyre_condition,
    engine_risk_flag,
    pit_status,
    event_ts
FROM enriched_telemetry_source
-- Only persist on-track events to keep archive lean
WHERE pit_status = 'ON_TRACK';


-- ════════════════════════════════════════════════════════════════════════════
-- TAB 9 — "Live Race Monitor"
-- Run after Job A has been going for ~15s
-- Streams into the Results tab — your live F1 timing screen
--
-- Useful filters in "Search rows":
--   Driver names:  "Verstappen", "Hamilton", "Leclerc", "Norris"
--   Risk:          "CRITICAL", "WARNING", "HIGH"
--   Tyre state:    "GRAINING", "OVERHEATING", "DEGRADED"
--   Pit:           "PIT_ENTRY", "IN_PIT_BOX"
--   Safety car:    "SAFETY_CAR", "VSC"
--   Circuits:      "Monza", "Monaco", "Suzuka"
--
-- Toggle "↓ Newest first" for latest events at the top
-- ════════════════════════════════════════════════════════════════════════════

SELECT
    driver_name,
    car_number,
    team_name,
    circuit_name,
    lap_number,
    sector,
    ROUND(speed_kph, 1)                                       AS speed_kph,
    rpm,
    gear,
    ROUND(engine_temp_c, 1)                                   AS engine_temp_c,
    engine_risk_flag,
    tyre_compound,
    tyre_age_laps,
    ROUND(tyre_temp_avg_c, 1)                                 AS tyre_temp_avg_c,
    ROUND(tyre_wear_pct, 1)                                   AS tyre_wear_pct,
    tyre_condition,
    ROUND(fuel_load_kg, 1)                                    AS fuel_load_kg,
    ROUND(ers_deploy_kj, 0)                                   AS ers_deploy_kj,
    drs_active,
    pit_status,
    safety_car_status,
    ROUND(g_lat, 2)                                           AS g_lat,
    ROUND(g_lon, 2)                                           AS g_lon,
    ROUND(brake_temp_max_c, 0)                                AS brake_temp_max_c,
    event_ts
-- NOTE: ORDER BY is intentionally omitted.
-- Flink streaming only supports ORDER BY on the ascending rowtime watermark
-- attribute — DESC or non-time-field sorts are not supported at runtime.
-- Use the Studio UI "↓ Newest first" toggle to reverse display order.
FROM enriched_telemetry_source;