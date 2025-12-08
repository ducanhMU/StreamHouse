-- Enable UUID extension for unique IDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create ENUM types for military operations
CREATE TYPE unit_status AS ENUM ('ready', 'engaged', 'damaged', 'destroyed', 'retreat');
CREATE TYPE domain_type AS ENUM ('land', 'sea', 'air', 'space', 'cyber');
CREATE TYPE alert_status AS ENUM ('active', 'resolved', 'acknowledged', 'dismissed');
CREATE TYPE ew_effect AS ENUM ('jammed', 'spoofed', 'disabled', 'ddos');
CREATE TYPE weather_type AS ENUM ('rain', 'fog', 'storm', 'solar_flare');
CREATE TYPE engagement_result AS ENUM ('destroyed', 'damaged', 'escaped', 'missed');
CREATE TYPE threat_level AS ENUM ('low', 'medium', 'high', 'critical');
CREATE TYPE iff_status AS ENUM ('friend', 'foe', 'neutral', 'unknown');
CREATE TYPE user_role AS ENUM ('commander', 'analyst', 'operator');

-- create schema
CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION admin;

-- Create Bronze layer tables
CREATE TABLE IF NOT EXISTS raw.regions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region_name VARCHAR(255),
    terrain_type VARCHAR(50), -- e.g., 'mountain', 'desert', 'urban'
    infrastructure_level DECIMAL(19,4), -- 0-1, e.g., 0.8 for developed roads
    area_sqkm DECIMAL(19,4),
    lat_center DECIMAL(19,8),
    lon_center DECIMAL(19,8),
    classification VARCHAR(50), -- e.g., 'secret'
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.targets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    target_id VARCHAR(255) UNIQUE, -- e.g., 'HOSTILE_001', UNIQUE for ??
    target_type VARCHAR(255), -- e.g., 'missile', 'aircraft'
    domain domain_type,
    estimated_intent VARCHAR(255), -- e.g., 'attack', 'recon'
    threat_level threat_level,
    iff_status iff_status,
    classification VARCHAR(50),
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(255) UNIQUE,
    role user_role,
    classification VARCHAR(50), -- Access level, e.g., 'secret'
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.units (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255),
    domain domain_type,
    unit_type VARCHAR(255), -- e.g., 'F-16', 'tank'
    status unit_status,
    classification VARCHAR(50),
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.weapons (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    unit_id UUID,
    name VARCHAR(255),
    type VARCHAR(255), -- e.g., 'radar-guided missile'
    effective_domain domain_type,
    range_km DECIMAL(19,4),
    hit_probability DECIMAL(19,4),
    speed_kmh DECIMAL(19,4),
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.sensors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    unit_id UUID,
    sensor_type VARCHAR(255), -- e.g., 'AN/APG-81'
    range_km DECIMAL(19,4),
    status VARCHAR(50), -- e.g., 'active'
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.detections (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sensor_id UUID,
    target_id UUID, -- FK to targets_raw
    target_domain domain_type,
    region_id UUID, -- FK to regions_raw
    lat DECIMAL(19,8),
    lon DECIMAL(19,8),
    altitude_depth DECIMAL(19,4),
    speed_kmh DECIMAL(19,4),
    heading_deg DECIMAL(19,4),
    confidence DECIMAL(19,4),
    iff_status iff_status,
    threat_level threat_level,
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.weather_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    region_id UUID, -- FK to regions_raw
    type weather_type,
    intensity DECIMAL(19,4), -- 0-1
    wind_speed_kmh DECIMAL(19,4),
    precipitation_mm DECIMAL(19,4),
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.unit_status_updates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    unit_id UUID,
    region_id UUID, -- FK to regions_raw
    lat DECIMAL(19,8),
    lon DECIMAL(19,8),
    health_percent DECIMAL(19,4), -- Consolidated fuel/ammo/health
    max_range_km DECIMAL(19,4), -- Dynamic range
    status unit_status,
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.supply_status (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    unit_id UUID,
    region_id UUID, -- FK to regions_raw
    supply_level DECIMAL(19,4), -- 0-100
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.cyber_ew_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID, track_id UUID,
    target_sensor_id UUID,
    effect ew_effect,
    impact_domain domain_type,
    level threat_level,
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.engagement_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    attacker_id UUID,
    target_id UUID, -- FK to targets_raw
    weapon_id UUID,
    hit BOOLEAN,
    result engagement_result,
    roe_compliant BOOLEAN,
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.roe_updates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rules JSONB, -- e.g., ["no_collateral"]
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    detection_id UUID,
    status alert_status,
    msg TEXT DEFAULT '',
    threat_level threat_level,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS raw.commands (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    alert_id UUID,
    unit_id UUID,
    user_id UUID, -- FK to users_raw
    action VARCHAR(255), -- e.g., 'Engage target'
    event_time TIMESTAMP WITH TIME ZONE,
    ingest_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    src VARCHAR(255)
);

-- Foreign key constraints
ALTER TABLE raw.weapons
ADD CONSTRAINT fk_weapons_unit_id
FOREIGN KEY (unit_id) REFERENCES raw.units(id);

ALTER TABLE raw.sensors
ADD CONSTRAINT fk_sensors_unit_id
FOREIGN KEY (unit_id) REFERENCES raw.units(id);

ALTER TABLE raw.detections
ADD CONSTRAINT fk_detections_sensor_id
FOREIGN KEY (sensor_id) REFERENCES raw.sensors(id),
ADD CONSTRAINT fk_detections_target_id
FOREIGN KEY (target_id) REFERENCES raw.targets(id),
ADD CONSTRAINT fk_detections_region_id
FOREIGN KEY (region_id) REFERENCES raw.regions(id);

ALTER TABLE raw.weather_events
ADD CONSTRAINT fk_weather_region_id
FOREIGN KEY (region_id) REFERENCES raw.regions(id);

ALTER TABLE raw.unit_status_updates
ADD CONSTRAINT fk_unit_status_unit_id
FOREIGN KEY (unit_id) REFERENCES raw.units(id),
ADD CONSTRAINT fk_unit_status_region_id
FOREIGN KEY (region_id) REFERENCES raw.regions(id);

ALTER TABLE raw.supply_status
ADD CONSTRAINT fk_supply_status_unit_id
FOREIGN KEY (unit_id) REFERENCES raw.units(id),
ADD CONSTRAINT fk_supply_status_region_id
FOREIGN KEY (region_id) REFERENCES raw.regions(id);

ALTER TABLE raw.cyber_ew_events
ADD CONSTRAINT fk_cyber_ew_source_id
FOREIGN KEY (source_id) REFERENCES raw.units(id),
ADD CONSTRAINT fk_cyber_ew_target_sensor_id
FOREIGN KEY (target_sensor_id) REFERENCES raw.sensors(id);

ALTER TABLE raw.engagement_events
ADD CONSTRAINT fk_engagement_attacker_id
FOREIGN KEY (attacker_id) REFERENCES raw.units(id),
ADD CONSTRAINT fk_engagement_weapon_id
FOREIGN KEY (weapon_id) REFERENCES raw.weapons(id),
ADD CONSTRAINT fk_engagement_target_id
FOREIGN KEY (target_id) REFERENCES raw.targets(id);

ALTER TABLE raw.alerts
ADD CONSTRAINT fk_alerts_detection_id
FOREIGN KEY (detection_id) REFERENCES raw.detections(id);

ALTER TABLE raw.commands
ADD CONSTRAINT fk_commands_alert_id
FOREIGN KEY (alert_id) REFERENCES raw.alerts(id),
ADD CONSTRAINT fk_commands_unit_id
FOREIGN KEY (unit_id) REFERENCES raw.units(id),
ADD CONSTRAINT fk_commands_user_id
FOREIGN KEY (user_id) REFERENCES raw.users(id);

-- Indexes for auditability and joins
CREATE INDEX idx_regions_ingest_timestamp ON raw.regions(ingest_timestamp DESC);
CREATE INDEX idx_units_ingest_timestamp ON raw.units(ingest_timestamp DESC);
CREATE INDEX idx_weapons_ingest_timestamp ON raw.weapons(ingest_timestamp DESC);
CREATE INDEX idx_sensors_unit_id_ingest_timestamp ON raw.sensors(unit_id, ingest_timestamp DESC);

CREATE INDEX idx_regions_source ON raw.regions(src);
CREATE INDEX idx_targets_source ON raw.targets(src);
CREATE INDEX idx_users_source ON raw.users(src);
CREATE INDEX idx_units_source ON raw.units(src);
CREATE INDEX idx_weapons_source ON raw.weapons(src);
CREATE INDEX idx_sensors_source ON raw.sensors(src);
CREATE INDEX idx_detections_source ON raw.detections(src);
CREATE INDEX idx_weather_events_source ON raw.weather_events(src);
CREATE INDEX idx_unit_status_updates_source ON raw.unit_status_updates(src);
CREATE INDEX idx_supply_status_source ON raw.supply_status(src);
CREATE INDEX idx_cyber_ew_events_source ON raw.cyber_ew_events(src);
CREATE INDEX idx_engagement_events_source ON raw.engagement_events(src);
CREATE INDEX idx_roe_updates_source ON raw.roe_updates(src);
CREATE INDEX idx_alerts_source ON raw.alerts(src);
CREATE INDEX idx_commands_source ON raw.commands(src);

CREATE INDEX idx_detections_sensor_id_timestamp ON raw.detections(sensor_id, event_time DESC);
CREATE INDEX idx_weather_events_region_id_timestamp ON raw.weather_events(region_id, event_time DESC);
CREATE INDEX idx_unit_status_updates_unit_id_timestamp ON raw.unit_status_updates(unit_id, event_time DESC);
CREATE INDEX idx_supply_status_unit_id_timestamp ON raw.supply_status(unit_id, event_time DESC);
CREATE INDEX idx_cyber_ew_events_target_sensor_id_timestamp ON raw.cyber_ew_events(target_sensor_id, event_time DESC);
CREATE INDEX idx_engagement_events_attacker_id_timestamp ON raw.engagement_events(attacker_id, event_time DESC);
CREATE INDEX idx_alerts_detection_id_created_at ON raw.alerts(detection_id, created_at DESC);
CREATE INDEX idx_commands_alert_id_timestamp ON raw.commands(alert_id, event_time DESC);
CREATE INDEX idx_roe_updates_timestamp ON raw.roe_updates(event_time DESC);


CREATE INDEX idx_detections_target_id ON raw.detections(target_id);
CREATE INDEX idx_detections_region_id ON raw.detections(region_id);
CREATE INDEX idx_targets_threat_level ON raw.targets(threat_level);
CREATE INDEX idx_users_role ON raw.users(role);
CREATE INDEX idx_unit_status_updates_region_id ON raw.unit_status_updates(region_id);
CREATE INDEX idx_supply_status_region_id ON raw.supply_status(region_id);
CREATE INDEX idx_engagement_events_target_id ON raw.engagement_events(target_id);
CREATE INDEX idx_commands_user_id ON raw.commands(user_id);

-- WAL configuration for auditability
ALTER TABLE raw.regions REPLICA IDENTITY FULL;
ALTER TABLE raw.targets REPLICA IDENTITY FULL;
ALTER TABLE raw.users REPLICA IDENTITY FULL;
ALTER TABLE raw.units REPLICA IDENTITY FULL;
ALTER TABLE raw.weapons REPLICA IDENTITY FULL;
ALTER TABLE raw.sensors REPLICA IDENTITY FULL;
ALTER TABLE raw.detections REPLICA IDENTITY FULL;
ALTER TABLE raw.weather_events REPLICA IDENTITY FULL;
ALTER TABLE raw.unit_status_updates REPLICA IDENTITY FULL;
ALTER TABLE raw.supply_status REPLICA IDENTITY FULL;
ALTER TABLE raw.cyber_ew_events REPLICA IDENTITY FULL;
ALTER TABLE raw.engagement_events REPLICA IDENTITY FULL;
ALTER TABLE raw.roe_updates REPLICA IDENTITY FULL;
ALTER TABLE raw.alerts REPLICA IDENTITY FULL;
ALTER TABLE raw.commands REPLICA IDENTITY FULL;

-- Retention policies for high-volume tables (30 days)
-- SELECT add_retention_policy('raw.detections', INTERVAL '30 days');
-- SELECT add_retention_policy('raw.weather_events', INTERVAL '30 days');
-- SELECT add_retention_policy('raw.unit_status_updates', INTERVAL '30 days');
-- SELECT add_retention_policy('raw.alerts', INTERVAL '30 days');
-- SELECT add_retention_policy('raw.commands', INTERVAL '30 days');

-- Create Debezium replication user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin') THEN
        CREATE ROLE admin WITH LOGIN PASSWORD 'password';
    END IF;

    -- Grant replication and logical decoding rights
    ALTER ROLE admin WITH REPLICATION;

    -- Ensure database access
    GRANT ALL PRIVILEGES ON DATABASE jadc2_db TO admin;
    GRANT ALL PRIVILEGES ON SCHEMA raw TO admin;
END
$$;

-- Make sure new tables in schema raw are automatically owned by admin
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
GRANT ALL PRIVILEGES ON TABLES TO admin;