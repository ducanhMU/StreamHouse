-- ============================================================================
-- postgresql/init_dest.sql
-- Initialize Gold Layer (OLAP) Database for Destination PostgreSQL
-- 
-- Purpose: Create schema, tables, and constraints for Spark Upsert compatibility
-- Layer: Gold Layer (Medallion Architecture)
-- Target: postgres-dest:5432/jadc2_db
-- ============================================================================

-- Connect to the specific database (assumes it exists or is default)
\c jadc2_db;

-- ============================================================================
-- 1. SCHEMA SETUP
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;
SET search_path TO gold, public;

-- ============================================================================
-- 2. ENUM TYPES
-- ============================================================================

DO $$ 
BEGIN
    -- unit_status
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'unit_status') THEN
        CREATE TYPE unit_status AS ENUM ('ready', 'engaged', 'damaged', 'destroyed', 'retreat');
    END IF;

    -- domain_type
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'domain_type') THEN
        CREATE TYPE domain_type AS ENUM ('land', 'sea', 'air', 'space', 'cyber');
    END IF;

    -- alert_status
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'alert_status') THEN
        CREATE TYPE alert_status AS ENUM ('active', 'resolved', 'acknowledged', 'dismissed');
    END IF;

    -- ew_effect
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ew_effect') THEN
        CREATE TYPE ew_effect AS ENUM ('jammed', 'spoofed', 'disabled', 'ddos');
    END IF;

    -- weather_type
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'weather_type') THEN
        CREATE TYPE weather_type AS ENUM ('rain', 'fog', 'storm', 'solar_flare');
    END IF;

    -- engagement_result
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'engagement_result') THEN
        CREATE TYPE engagement_result AS ENUM ('destroyed', 'damaged', 'escaped', 'missed');
    END IF;

    -- threat_level
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'threat_level') THEN
        CREATE TYPE threat_level AS ENUM ('low', 'medium', 'high', 'critical');
    END IF;

    -- iff_status
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'iff_status') THEN
        CREATE TYPE iff_status AS ENUM ('friend', 'foe', 'neutral', 'unknown');
    END IF;

    -- user_role
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
        CREATE TYPE user_role AS ENUM ('commander', 'analyst', 'operator');
    END IF;
END $$;

-- ============================================================================
-- 3. GOLD LAYER TABLES
-- Critical Update: Added UNIQUE constraints to support Spark ON CONFLICT Upserts
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table 1: EFFECTIVE_UNIT_STRENGTH
-- Business Key: unit_id + weapon_id
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.effective_unit_strength CASCADE;

CREATE TABLE gold.effective_unit_strength (
    id                      SERIAL PRIMARY KEY,
    unit_id                 UUID NOT NULL,
    region_id               UUID NOT NULL,
    weapon_id               UUID NOT NULL, -- Required for composite key
    terrain_factor          DECIMAL(19,4),
    effective_range_km      DECIMAL(19,4),
    adjusted_hit_probability DECIMAL(19,4),
    attacking_strength      DECIMAL(19,4),
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- CRITICAL FOR SPARK UPSERT:
    CONSTRAINT uq_eus_unit_weapon UNIQUE (unit_id, weapon_id)
);

CREATE INDEX idx_eus_unit_id ON gold.effective_unit_strength(unit_id);
CREATE INDEX idx_eus_region_id ON gold.effective_unit_strength(region_id);
CREATE INDEX idx_eus_event_time ON gold.effective_unit_strength(event_time DESC);

-- ----------------------------------------------------------------------------
-- Table 2: THREAT_ASSESSMENT
-- Business Key: target_id
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.threat_assessment CASCADE;

CREATE TABLE gold.threat_assessment (
    id                      SERIAL PRIMARY KEY,
    target_id               VARCHAR(255) NOT NULL,
    region_id               UUID NOT NULL,
    iff_status              iff_status,
    adjusted_confidence     DECIMAL(19,4),
    predicted_threat        VARCHAR(50),
    distance_km             DECIMAL(19,4),
    response_time_sec       DECIMAL(19,4),
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- CRITICAL FOR SPARK UPSERT:
    CONSTRAINT uq_ta_target UNIQUE (target_id)
);

CREATE INDEX idx_ta_target_id ON gold.threat_assessment(target_id);
CREATE INDEX idx_ta_region_id ON gold.threat_assessment(region_id);
CREATE INDEX idx_ta_event_time ON gold.threat_assessment(event_time DESC);
CREATE INDEX idx_ta_predicted_threat ON gold.threat_assessment(predicted_threat);

-- ----------------------------------------------------------------------------
-- Table 3: ALERTS_WITH_COMMANDS
-- Business Key: alert_id
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.alerts_with_commands CASCADE;

CREATE TABLE gold.alerts_with_commands (
    id                      SERIAL PRIMARY KEY,
    alert_id                UUID NOT NULL,
    detection_id            UUID NOT NULL,
    region_id               UUID NOT NULL,
    threat_level            threat_level,
    command_id              UUID,
    action                  VARCHAR(255),
    user_id                 UUID,
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- CRITICAL FOR SPARK UPSERT:
    CONSTRAINT uq_awc_alert UNIQUE (alert_id)
);

CREATE INDEX idx_awc_alert_id ON gold.alerts_with_commands(alert_id);
CREATE INDEX idx_awc_region_id ON gold.alerts_with_commands(region_id);
CREATE INDEX idx_awc_event_time ON gold.alerts_with_commands(event_time DESC);

-- ----------------------------------------------------------------------------
-- Table 4: LOGISTICS_READINESS
-- Business Key: unit_id
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.logistics_readiness CASCADE;

CREATE TABLE gold.logistics_readiness (
    id                      SERIAL PRIMARY KEY,
    unit_id                 UUID NOT NULL,
    region_id               UUID NOT NULL,
    supply_level            DECIMAL(19,4),
    projected_supply        DECIMAL(19,4),
    resupply_feasibility    INTEGER, -- 0 or 1
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- CRITICAL FOR SPARK UPSERT:
    CONSTRAINT uq_lr_unit UNIQUE (unit_id)
);

CREATE INDEX idx_lr_unit_id ON gold.logistics_readiness(unit_id);
CREATE INDEX idx_lr_region_id ON gold.logistics_readiness(region_id);
CREATE INDEX idx_lr_event_time ON gold.logistics_readiness(event_time DESC);

-- ----------------------------------------------------------------------------
-- Table 5: ENGAGEMENT_ANALYSIS
-- Business Key: engagement_id
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS gold.engagement_analysis CASCADE;

CREATE TABLE gold.engagement_analysis (
    id                      SERIAL PRIMARY KEY,
    engagement_id           UUID NOT NULL,
    attacker_id             UUID NOT NULL,
    target_id               UUID NOT NULL,
    weapon_id               UUID,
    region_id               UUID,
    result                  engagement_result,
    adjusted_hit_probability DECIMAL(19,4),
    impact_factor           DECIMAL(19,4),
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- CRITICAL FOR SPARK UPSERT:
    CONSTRAINT uq_ea_engagement UNIQUE (engagement_id)
);

CREATE INDEX idx_ea_engagement_id ON gold.engagement_analysis(engagement_id);
CREATE INDEX idx_ea_result ON gold.engagement_analysis(result);
CREATE INDEX idx_ea_event_time ON gold.engagement_analysis(event_time DESC);

-- ============================================================================
-- 4. ANALYTIC VIEWS (MCP Helpers)
-- ============================================================================

-- View: Recent High Threats
CREATE OR REPLACE VIEW gold.v_recent_high_threats AS
SELECT region_id, target_id, iff_status, predicted_threat, distance_km, response_time_sec, event_time
FROM gold.threat_assessment
WHERE predicted_threat IN ('high', 'critical')
AND event_time > CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY event_time DESC;

-- View: Units Needing Resupply
CREATE OR REPLACE VIEW gold.v_units_needing_resupply AS
SELECT unit_id, region_id, supply_level, projected_supply, resupply_feasibility, event_time,
    CASE 
        WHEN supply_level < 20 THEN 'CRITICAL'
        WHEN supply_level < 40 THEN 'LOW'
        ELSE 'MODERATE'
    END AS supply_status
FROM gold.logistics_readiness
WHERE supply_level < 60
ORDER BY supply_level ASC;

-- View: Engagement Success Rate
CREATE OR REPLACE VIEW gold.v_engagement_success_by_region AS
SELECT region_id,
    COUNT(*) AS total_engagements,
    ROUND(100.0 * COUNT(*) FILTER (WHERE result = 'destroyed') / NULLIF(COUNT(*), 0), 2) AS success_rate_pct
FROM gold.engagement_analysis
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY region_id;

-- View: Combat Strength
CREATE OR REPLACE VIEW gold.v_combat_strength_by_region AS
SELECT region_id,
    COUNT(DISTINCT unit_id) AS active_units,
    AVG(attacking_strength) AS avg_strength
FROM gold.effective_unit_strength
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY region_id;

-- View: Unresolved Alerts
CREATE OR REPLACE VIEW gold.v_unresolved_critical_alerts AS
SELECT alert_id, region_id, threat_level, event_time,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - event_time)) / 60 AS minutes_pending
FROM gold.alerts_with_commands
WHERE threat_level = 'critical' AND command_id IS NULL;

-- ============================================================================
-- 5. AUTOMATION TRIGGERS
-- ============================================================================

CREATE OR REPLACE FUNCTION gold.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_eus_updated_at BEFORE UPDATE ON gold.effective_unit_strength
FOR EACH ROW EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_ta_updated_at BEFORE UPDATE ON gold.threat_assessment
FOR EACH ROW EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_awc_updated_at BEFORE UPDATE ON gold.alerts_with_commands
FOR EACH ROW EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_lr_updated_at BEFORE UPDATE ON gold.logistics_readiness
FOR EACH ROW EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_ea_updated_at BEFORE UPDATE ON gold.engagement_analysis
FOR EACH ROW EXECUTE FUNCTION gold.update_updated_at_column();

-- ============================================================================
-- END INITIALIZATION
-- ============================================================================
\echo 'JADC2 Gold Layer Initialized with Spark Upsert Support'