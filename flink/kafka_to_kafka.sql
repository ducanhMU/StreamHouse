-- =========================================================
-- Flink SQL job: consume Debezium CDC from Kafka topics
-- and write into Pinot
-- =========================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '30s';
SET 'parallelism.default' = '1';

USE CATALOG default_catalog;
USE default_database;

-- -------------------------------------------------
-- create source tables
-- -------------------------------------------------

CREATE TABLE IF NOT EXISTS regions_cdc (
    id STRING,
    region_name STRING,
    terrain_type STRING,
    infrastructure_level DECIMAL(19,4),
    area_sqkm DECIMAL(19,4),
    lat_center DECIMAL(19,8),
    lon_center DECIMAL(19,8),
    classification STRING,
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.regions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS targets_cdc (
    id STRING,
    target_id STRING,
    target_type STRING,
    domain STRING,
    estimated_intent STRING,
    threat_level STRING,
    iff_status STRING,
    classification STRING,
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.targets',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS users_cdc (
    id STRING,
    username STRING,
    `role` STRING,
    classification STRING,
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.users',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS units_cdc (
    id STRING,
    `name` STRING,
    domain STRING,
    unit_type STRING,
    `status` STRING,
    classification STRING,
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.units',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS weapons_cdc (
    id STRING,
    unit_id STRING,
    `name` STRING,
    `type` STRING,
    effective_domain STRING,
    range_km DECIMAL(19,4),
    hit_probability DECIMAL(19,4),
    speed_kmh DECIMAL(19,4),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.weapons',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS sensors_cdc (
    id STRING,
    unit_id STRING,
    sensor_type STRING,
    range_km DECIMAL(19,4),
    `status` STRING,
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.sensors',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS detections_cdc (
    id STRING,
    sensor_id STRING,
    target_id STRING,
    target_domain STRING,
    region_id STRING,
    lat DECIMAL(19,8),
    lon DECIMAL(19,8),
    altitude_depth DECIMAL(19,4),
    speed_kmh DECIMAL(19,4),
    heading_deg DECIMAL(19,4),
    confidence DECIMAL(19,4),
    iff_status STRING,
    threat_level STRING,
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.detections',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS weather_events_cdc (
    id STRING,
    region_id STRING,
    `type` STRING,
    intensity DECIMAL(19,4),
    wind_speed_kmh DECIMAL(19,4),
    precipitation_mm DECIMAL(19,4),
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.weather_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS unit_status_updates_cdc (
    id STRING,
    unit_id STRING,
    region_id STRING,
    lat DECIMAL(19,8),
    lon DECIMAL(19,8),
    health_percent DECIMAL(19,4),
    max_range_km DECIMAL(19,4),
    `status` STRING,
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.unit_status_updates',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS supply_status_cdc (
    id STRING,
    unit_id STRING,
    region_id STRING,
    supply_level DECIMAL(19,4),
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.supply_status',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS cyber_ew_events_cdc (
    id STRING,
    source_id STRING,
    target_sensor_id STRING,
    effect STRING,
    impact_domain STRING,
    `level` STRING,
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.cyber_ew_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS engagement_events_cdc (
    id STRING,
    attacker_id STRING,
    target_id STRING,
    weapon_id STRING,
    hit BOOLEAN,
    `result` STRING,
    roe_compliant BOOLEAN,
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.engagement_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS roe_updates_cdc (
    id STRING,
    rules STRING,
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.roe_updates',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS alerts_cdc (
    id STRING,
    detection_id STRING,
    `status` STRING,
    msg STRING,
    threat_level STRING,
    created_at TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.alerts',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS commands_cdc (
    id STRING,
    alert_id STRING,
    unit_id STRING,
    `user_id` STRING,
    `action` STRING,
    event_time TIMESTAMP_LTZ(6),
    ingest_timestamp TIMESTAMP_LTZ(6),
    src STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'jadc2.raw.commands',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'properties.max.poll.records' = '100',
    'debezium-json.timestamp-format.standard' = 'ISO-8601'
);

-- =========================================================
-- Pinot Sink Tables for Gold Layer
-- =========================================================

-- =========================================================
-- Effective Unit Strength
-- =========================================================
CREATE TABLE IF NOT EXISTS gold_effective_unit_strength (
    unit_id STRING,
    region_id STRING,
    weapon_id STRING,
    terrain_factor DECIMAL(19,4),
    effective_range_km DECIMAL(19,4),
    adjusted_hit_probability DECIMAL(19,4),
    attacking_strength DECIMAL(19,4),
    event_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (unit_id, region_id, weapon_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'jadc2.gold.effective_unit_strength',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- =========================================================
-- Threat Assessment
-- =========================================================
CREATE TABLE IF NOT EXISTS gold_threat_assessment (
    target_id STRING,
    region_id STRING,
    iff_status STRING,
    adjusted_confidence DECIMAL(19,4),
    predicted_threat STRING,
    distance_km DECIMAL(19,4),
    response_time_sec DECIMAL(19,4),
    event_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (target_id, region_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'jadc2.gold.threat_assessment',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- =========================================================
-- Alerts with Commands
-- =========================================================
CREATE TABLE IF NOT EXISTS gold_alerts_with_commands (
    alert_id STRING,
    detection_id STRING,
    region_id STRING,
    threat_level STRING,
    command_id STRING,
    `action` STRING,
    `user_id` STRING,
    event_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'jadc2.gold.alerts_with_commands',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- =========================================================
-- Logistics Readiness
-- =========================================================
CREATE TABLE IF NOT EXISTS gold_logistics_readiness (
    unit_id STRING,
    region_id STRING,
    supply_level DECIMAL(19,4),
    projected_supply DECIMAL(19,4),
    resupply_feasibility INT,
    event_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (unit_id, region_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'jadc2.gold.logistics_readiness',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- =========================================================
-- Engagement Analysis
-- =========================================================
CREATE TABLE IF NOT EXISTS gold_engagement_analysis (
    engagement_id STRING,
    attacker_id STRING,
    target_id STRING,
    weapon_id STRING,
    region_id STRING,
    `result` STRING,
    adjusted_hit_probability DECIMAL(19,4),
    impact_factor DECIMAL(19,4),
    event_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (engagement_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'jadc2.gold.engagement_analysis',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);


-- =========================================================
-- 1. Effective Unit Strength (upsert)
-- =========================================================
INSERT INTO gold_effective_unit_strength
SELECT
    u.id AS unit_id,
    us.region_id,
    w.id AS weapon_id,
    CASE r.terrain_type
        WHEN 'mountain' THEN 0.8
        WHEN 'swamp' THEN 0.7
        ELSE 1.0
    END AS terrain_factor,
    w.range_km
        * CASE r.terrain_type WHEN 'mountain' THEN 0.8 WHEN 'swamp' THEN 0.7 ELSE 1.0 END
        * (1 - 0.1 * COALESCE(we.wind_speed_kmh, 0) / 100) AS effective_range_km,
    w.hit_probability
        * (1 - 0.15 * COALESCE(we.precipitation_mm, 0)
        - 0.2 * COALESCE(we.intensity, 0) * (CASE WHEN we.type = 'fog' THEN 1 ELSE 0 END)) AS adjusted_hit_probability,
    (w.hit_probability
        * (1 - 0.15 * COALESCE(we.precipitation_mm, 0)
        - 0.2 * COALESCE(we.intensity, 0) * (CASE WHEN we.type = 'fog' THEN 1 ELSE 0 END)))
        * COALESCE(us.health_percent, 0) / 100 AS attacking_strength,
    us.event_time
FROM units_cdc u
JOIN unit_status_updates_cdc us ON u.id = us.unit_id
JOIN weapons_cdc w ON w.unit_id = u.id
JOIN regions_cdc r ON r.id = us.region_id
LEFT JOIN weather_events_cdc we
    ON we.region_id = r.id AND we.event_time <= us.event_time
;

-- =========================================================
-- 2. Threat Assessment (upsert) — dùng window + haversine UDF
-- =========================================================

-- intermediate tables: get latest records (in event_time column) of unit_status_updates by ROW_NUMBER
 WITH latest_unit_status AS (
     SELECT
         unit_id,
         region_id,
         lat,
         lon,
         health_percent,
         event_time,
         ROW_NUMBER() OVER (PARTITION BY unit_id ORDER BY event_time DESC) AS rn
     FROM unit_status_updates_cdc
 )
 , filtered_latest_us AS (
     SELECT unit_id, region_id, lat, lon, health_percent, event_time
     FROM latest_unit_status
     WHERE rn = 1
 )

 INSERT INTO gold_threat_assessment
 SELECT
     t.id AS target_id,
     d.region_id,
     t.iff_status,
     d.confidence
         * (1 - 0.3 * COALESCE(we.intensity, 0)
             * (CASE WHEN we.type IN ('fog','storm') THEN 1 ELSE 0 END)
             - 0.5 * (CASE WHEN ce.effect = 'jammed' THEN 1 ELSE 0 END)) AS adjusted_confidence,
     CASE
         WHEN (d.speed_kmh > 500 OR t.iff_status = 'foe')
              AND (d.confidence
                   * (1 - 0.3 * COALESCE(we.intensity, 0)
                       * (CASE WHEN we.type IN ('fog','storm') THEN 1 ELSE 0 END)
                       - 0.5 * (CASE WHEN ce.effect = 'jammed' THEN 1 ELSE 0 END))) > 0.8
         THEN 'high'
         ELSE 'medium'
     END AS predicted_threat,
     COALESCE(haversine(fl.lat, fl.lon, d.lat, d.lon), 1e9) AS distance_km,
     CASE
         WHEN d.speed_kmh > 0
         THEN COALESCE(haversine(fl.lat, fl.lon, d.lat, d.lon), 1e9) / d.speed_kmh * 3600
         ELSE 1e9
     END AS response_time_sec,
     d.event_time
 FROM targets_cdc t
 JOIN detections_cdc d ON d.target_id = t.id
 LEFT JOIN sensors_cdc s ON s.id = d.sensor_id
 LEFT JOIN units_cdc u ON u.id = s.unit_id
 LEFT JOIN filtered_latest_us fl ON fl.unit_id = u.id
 LEFT JOIN weather_events_cdc we
     ON we.region_id = d.region_id AND we.event_time <= d.event_time
 LEFT JOIN cyber_ew_events_cdc ce
     ON ce.target_sensor_id = d.sensor_id AND ce.event_time <= d.event_time
 ;

-- =========================================================
-- 3. Alerts with Commands (upsert)
-- =========================================================
INSERT INTO gold_alerts_with_commands
SELECT
    a.id AS alert_id,
    a.detection_id,
    d.region_id,
    a.threat_level,
    c.id AS command_id,
    CASE WHEN u.role = 'commander' THEN c.action ELSE NULL END AS `action`,
    u.id AS `user_id`,
    GREATEST(a.created_at, c.event_time) AS event_time
FROM alerts_cdc a
JOIN detections_cdc d ON d.id = a.detection_id
LEFT JOIN commands_cdc c ON c.alert_id = a.id
LEFT JOIN users_cdc u ON u.id = c.user_id
WHERE a.threat_level >= 'high'
;

-- =========================================================
-- 4. Logistics Readiness (upsert)
-- =========================================================
INSERT INTO gold_logistics_readiness
SELECT
    s.unit_id,
    us.region_id,
    s.supply_level,
    s.supply_level * (
        1 - 0.5 * COALESCE(we.intensity, 0)
            * (CASE WHEN we.type IN ('storm','rain') THEN 1 ELSE 0 END)
            * (CASE WHEN r.terrain_type = 'mountain' THEN 0.7 ELSE 1.0 END)
    ) AS projected_supply,
    CASE
        WHEN COALESCE(we.wind_speed_kmh, 0) > 50 OR COALESCE(we.precipitation_mm, 0) > 10
        THEN 0
        ELSE 1
    END AS resupply_feasibility,
    GREATEST(s.event_time, we.event_time) AS event_time
FROM supply_status_cdc s
JOIN unit_status_updates_cdc us ON us.unit_id = s.unit_id
JOIN regions_cdc r ON r.id = us.region_id
LEFT JOIN weather_events_cdc we
    ON we.region_id = r.id AND we.event_time <= s.event_time
;

-- =========================================================
-- 5. Engagement Analysis (upsert, refined design)
-- =========================================================
INSERT INTO gold_engagement_analysis
SELECT
    e.id AS engagement_id,
    e.attacker_id,
    e.target_id,
    e.weapon_id,
    us.region_id,  
    e.`result`,
    w.hit_probability
        * (1 - 0.2 * COALESCE(we.intensity, 0)
            * (CASE WHEN we.type = 'fog' THEN 1 ELSE 0 END)
            - 0.3 * (CASE WHEN ce.effect = 'jammed' THEN 1 ELSE 0 END)
          ) AS adjusted_hit_probability,
    (1 - 0.2 * (CASE WHEN we.type = 'fog' THEN 1 ELSE 0 END)
        - 0.3 * (CASE WHEN ce.effect = 'jammed' THEN 1 ELSE 0 END))
        * (CASE r.terrain_type WHEN 'mountain' THEN 0.8 ELSE 1.0 END) AS impact_factor,
    e.event_time
FROM engagement_events_cdc e
JOIN weapons_cdc w 
    ON w.id = e.weapon_id
JOIN unit_status_updates_cdc us
    ON us.unit_id = e.attacker_id 
   AND us.event_time <= e.event_time
JOIN regions_cdc r 
    ON r.id = us.region_id
LEFT JOIN weather_events_cdc we
    ON we.region_id = us.region_id 
   AND we.event_time <= e.event_time
LEFT JOIN cyber_ew_events_cdc ce
    ON ce.target_sensor_id = e.attacker_id 
   AND ce.event_time <= e.event_time
;
