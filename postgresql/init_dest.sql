-- ============================================================================
-- postgresql/init_dest.sql
-- Initialize Gold Layer (OLAP) Database for Destination PostgreSQL
-- 
-- Purpose: Create schema and tables for analytics and MCP chatbot queries
-- Layer: Gold Layer (Medallion Architecture)
-- Target: postgres-dest:5432/jadc2_db
-- ============================================================================

\c jadc2_db;

-- ============================================================================
-- CREATE GOLD SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

SET search_path TO gold, public;

-- ============================================================================
-- ENUM TYPES (Must match Bronze/Silver layers)
-- ============================================================================

-- Create enum types if they don't exist in public schema
DO $$ 
BEGIN
    -- unit_status enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'unit_status') THEN
        CREATE TYPE unit_status AS ENUM (
            'ready',        -- Operational
            'engaged',      -- In combat
            'damaged',      -- Degraded capability
            'destroyed',    -- Neutralized
            'retreat'       -- Withdrawing
        );
    END IF;

    -- domain_type enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'domain_type') THEN
        CREATE TYPE domain_type AS ENUM (
            'land',         -- Tanks, infantry
            'sea',          -- Submarines, carriers
            'air',          -- Fighters, drones
            'space',        -- Satellites
            'cyber'         -- EW, network ops
        );
    END IF;

    -- alert_status enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'alert_status') THEN
        CREATE TYPE alert_status AS ENUM (
            'active',       -- Unresolved
            'resolved',     -- Neutralized
            'acknowledged', -- Reviewed
            'dismissed'     -- False positive
        );
    END IF;

    -- ew_effect enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ew_effect') THEN
        CREATE TYPE ew_effect AS ENUM (
            'jammed',       -- Signal interference
            'spoofed',      -- False data
            'disabled',     -- Sensor outage
            'ddos'          -- Network denial
        );
    END IF;

    -- weather_type enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'weather_type') THEN
        CREATE TYPE weather_type AS ENUM (
            'rain',         -- Reduces mobility
            'fog',          -- Impairs radar
            'storm',        -- Disrupts sea/air
            'solar_flare'   -- Degrades space/cyber
        );
    END IF;

    -- engagement_result enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'engagement_result') THEN
        CREATE TYPE engagement_result AS ENUM (
            'destroyed',    -- Target eliminated
            'damaged',      -- Degraded
            'escaped',      -- Evaded
            'missed'        -- Weapon failed
        );
    END IF;

    -- threat_level enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'threat_level') THEN
        CREATE TYPE threat_level AS ENUM (
            'low',          -- Distant
            'medium',       -- Approaching
            'high',         -- Imminent
            'critical'      -- Direct attack
        );
    END IF;

    -- iff_status enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'iff_status') THEN
        CREATE TYPE iff_status AS ENUM (
            'friend',       -- Allied
            'foe',          -- Hostile
            'neutral',      -- Civilian
            'unknown'       -- Unidentified
        );
    END IF;

    -- user_role enum
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
        CREATE TYPE user_role AS ENUM (
            'commander',    -- Authorizes commands
            'analyst',      -- Monitors
            'operator'      -- Executes tasks
        );
    END IF;
END $$;

-- ============================================================================
-- GOLD LAYER TABLES (OLAP - Analytics Ready)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. EFFECTIVE_UNIT_STRENGTH
--    Terrain/weather-adjusted combat effectiveness
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS gold.effective_unit_strength CASCADE;

CREATE TABLE gold.effective_unit_strength (
    id                      SERIAL PRIMARY KEY,
    unit_id                 UUID NOT NULL,
    region_id               UUID NOT NULL,
    weapon_id               UUID,
    terrain_factor          DECIMAL(19,4),
    effective_range_km      DECIMAL(19,4),
    adjusted_hit_probability DECIMAL(19,4),
    attacking_strength      DECIMAL(19,4),
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for query optimization
CREATE INDEX idx_eus_unit_id ON gold.effective_unit_strength(unit_id);
CREATE INDEX idx_eus_region_id ON gold.effective_unit_strength(region_id);
CREATE INDEX idx_eus_event_time ON gold.effective_unit_strength(event_time DESC);
CREATE INDEX idx_eus_attacking_strength ON gold.effective_unit_strength(attacking_strength DESC);
CREATE INDEX idx_eus_region_time ON gold.effective_unit_strength(region_id, event_time DESC);

COMMENT ON TABLE gold.effective_unit_strength IS 'Combat effectiveness adjusted for terrain and weather conditions';
COMMENT ON COLUMN gold.effective_unit_strength.terrain_factor IS 'Terrain impact multiplier (e.g., 0.8 for mountains)';
COMMENT ON COLUMN gold.effective_unit_strength.effective_range_km IS 'Adjusted weapon range considering terrain and wind';
COMMENT ON COLUMN gold.effective_unit_strength.adjusted_hit_probability IS 'Hit probability after weather adjustments';
COMMENT ON COLUMN gold.effective_unit_strength.attacking_strength IS 'Overall combat effectiveness (0-1)';

-- ----------------------------------------------------------------------------
-- 2. THREAT_ASSESSMENT
--    Threat prioritization with weather and EW adjustments
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
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_ta_target_id ON gold.threat_assessment(target_id);
CREATE INDEX idx_ta_region_id ON gold.threat_assessment(region_id);
CREATE INDEX idx_ta_event_time ON gold.threat_assessment(event_time DESC);
CREATE INDEX idx_ta_predicted_threat ON gold.threat_assessment(predicted_threat);
CREATE INDEX idx_ta_iff_status ON gold.threat_assessment(iff_status);
CREATE INDEX idx_ta_region_threat_time ON gold.threat_assessment(region_id, predicted_threat, event_time DESC);
CREATE INDEX idx_ta_response_time ON gold.threat_assessment(response_time_sec);

COMMENT ON TABLE gold.threat_assessment IS 'Prioritized threats with environmental and electronic warfare adjustments';
COMMENT ON COLUMN gold.threat_assessment.adjusted_confidence IS 'Detection confidence adjusted for weather/EW (0-1)';
COMMENT ON COLUMN gold.threat_assessment.predicted_threat IS 'AI-predicted threat level (low/medium/high/critical)';
COMMENT ON COLUMN gold.threat_assessment.distance_km IS 'Distance from nearest friendly sensor';
COMMENT ON COLUMN gold.threat_assessment.response_time_sec IS 'Estimated time to intercept';

-- ----------------------------------------------------------------------------
-- 3. ALERTS_WITH_COMMANDS
--    Alerts joined with authorized C2 actions
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
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_awc_alert_id ON gold.alerts_with_commands(alert_id);
CREATE INDEX idx_awc_region_id ON gold.alerts_with_commands(region_id);
CREATE INDEX idx_awc_threat_level ON gold.alerts_with_commands(threat_level);
CREATE INDEX idx_awc_event_time ON gold.alerts_with_commands(event_time DESC);
CREATE INDEX idx_awc_user_id ON gold.alerts_with_commands(user_id);
CREATE INDEX idx_awc_region_threat ON gold.alerts_with_commands(region_id, threat_level);

COMMENT ON TABLE gold.alerts_with_commands IS 'High/critical alerts with associated command decisions';
COMMENT ON COLUMN gold.alerts_with_commands.action IS 'Command action (only from commanders)';
COMMENT ON COLUMN gold.alerts_with_commands.user_id IS 'Commander who authorized the action';

-- ----------------------------------------------------------------------------
-- 4. LOGISTICS_READINESS
--    Supply feasibility under weather/terrain constraints
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS gold.logistics_readiness CASCADE;

CREATE TABLE gold.logistics_readiness (
    id                      SERIAL PRIMARY KEY,
    unit_id                 UUID NOT NULL,
    region_id               UUID NOT NULL,
    supply_level            DECIMAL(19,4),
    projected_supply        DECIMAL(19,4),
    resupply_feasibility    INTEGER,  -- 0 (blocked) or 1 (possible)
    event_time              TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_lr_unit_id ON gold.logistics_readiness(unit_id);
CREATE INDEX idx_lr_region_id ON gold.logistics_readiness(region_id);
CREATE INDEX idx_lr_event_time ON gold.logistics_readiness(event_time DESC);
CREATE INDEX idx_lr_resupply_feasibility ON gold.logistics_readiness(resupply_feasibility);
CREATE INDEX idx_lr_region_feasibility ON gold.logistics_readiness(region_id, resupply_feasibility);
CREATE INDEX idx_lr_supply_level ON gold.logistics_readiness(supply_level);

COMMENT ON TABLE gold.logistics_readiness IS 'Supply status with weather and terrain impact analysis';
COMMENT ON COLUMN gold.logistics_readiness.supply_level IS 'Current supply level (0-100)';
COMMENT ON COLUMN gold.logistics_readiness.projected_supply IS 'Projected supply after weather/terrain impacts';
COMMENT ON COLUMN gold.logistics_readiness.resupply_feasibility IS 'Can resupply proceed? (0=No, 1=Yes)';

-- ----------------------------------------------------------------------------
-- 5. ENGAGEMENT_ANALYSIS
--    After-action review with terrain/weather/EW context
-- ----------------------------------------------------------------------------

DROP TABLE IF EXISTS gold.engagement_analysis CASCADE;

CREATE TABLE gold.engagement_analysis (
    id                          SERIAL PRIMARY KEY,
    engagement_id               UUID NOT NULL,
    attacker_id                 UUID NOT NULL,
    target_id                   UUID NOT NULL,
    weapon_id                   UUID,
    region_id                   UUID,
    result                      engagement_result,
    adjusted_hit_probability    DECIMAL(19,4),
    impact_factor               DECIMAL(19,4),
    event_time                  TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_ea_engagement_id ON gold.engagement_analysis(engagement_id);
CREATE INDEX idx_ea_attacker_id ON gold.engagement_analysis(attacker_id);
CREATE INDEX idx_ea_target_id ON gold.engagement_analysis(target_id);
CREATE INDEX idx_ea_region_id ON gold.engagement_analysis(region_id);
CREATE INDEX idx_ea_result ON gold.engagement_analysis(result);
CREATE INDEX idx_ea_event_time ON gold.engagement_analysis(event_time DESC);
CREATE INDEX idx_ea_region_result ON gold.engagement_analysis(region_id, result);

COMMENT ON TABLE gold.engagement_analysis IS 'Engagement outcomes with environmental and electronic warfare context';
COMMENT ON COLUMN gold.engagement_analysis.adjusted_hit_probability IS 'Hit probability after weather/fog/jamming adjustments';
COMMENT ON COLUMN gold.engagement_analysis.impact_factor IS 'Overall effectiveness factor (weather + terrain + EW)';

-- ============================================================================
-- HELPER VIEWS FOR MCP CHATBOT QUERIES
-- ============================================================================

-- View: Recent High Threats by Region
CREATE OR REPLACE VIEW gold.v_recent_high_threats AS
SELECT 
    region_id,
    target_id,
    iff_status,
    predicted_threat,
    distance_km,
    response_time_sec,
    event_time
FROM gold.threat_assessment
WHERE predicted_threat IN ('high', 'critical')
    AND event_time > CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY event_time DESC, response_time_sec ASC;

COMMENT ON VIEW gold.v_recent_high_threats IS 'High/critical threats from the last hour, sorted by urgency';

-- View: Units Needing Resupply
CREATE OR REPLACE VIEW gold.v_units_needing_resupply AS
SELECT 
    unit_id,
    region_id,
    supply_level,
    projected_supply,
    resupply_feasibility,
    event_time,
    CASE 
        WHEN supply_level < 20 THEN 'CRITICAL'
        WHEN supply_level < 40 THEN 'LOW'
        WHEN supply_level < 60 THEN 'MODERATE'
        ELSE 'ADEQUATE'
    END AS supply_status
FROM gold.logistics_readiness
WHERE supply_level < 60
ORDER BY supply_level ASC, event_time DESC;

COMMENT ON VIEW gold.v_units_needing_resupply IS 'Units with supply levels below 60%, prioritized by urgency';

-- View: Engagement Success Rate by Region
CREATE OR REPLACE VIEW gold.v_engagement_success_by_region AS
SELECT 
    region_id,
    COUNT(*) AS total_engagements,
    COUNT(*) FILTER (WHERE result = 'destroyed') AS successful,
    COUNT(*) FILTER (WHERE result = 'missed') AS missed,
    COUNT(*) FILTER (WHERE result = 'escaped') AS escaped,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE result = 'destroyed') / NULLIF(COUNT(*), 0), 
        2
    ) AS success_rate_pct,
    AVG(adjusted_hit_probability) AS avg_hit_probability,
    AVG(impact_factor) AS avg_impact_factor
FROM gold.engagement_analysis
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY region_id
ORDER BY success_rate_pct DESC;

COMMENT ON VIEW gold.v_engagement_success_by_region IS 'Engagement effectiveness by region over last 24 hours';

-- View: Combat Strength by Region
CREATE OR REPLACE VIEW gold.v_combat_strength_by_region AS
SELECT 
    region_id,
    COUNT(DISTINCT unit_id) AS active_units,
    AVG(attacking_strength) AS avg_strength,
    MIN(attacking_strength) AS min_strength,
    MAX(attacking_strength) AS max_strength,
    AVG(effective_range_km) AS avg_range_km,
    MAX(event_time) AS last_updated
FROM gold.effective_unit_strength
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY region_id
ORDER BY avg_strength DESC;

COMMENT ON VIEW gold.v_combat_strength_by_region IS 'Aggregated combat effectiveness by region';

-- View: Unresolved Critical Alerts
CREATE OR REPLACE VIEW gold.v_unresolved_critical_alerts AS
SELECT 
    alert_id,
    detection_id,
    region_id,
    threat_level,
    command_id,
    action,
    user_id,
    event_time,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - event_time)) / 60 AS minutes_pending
FROM gold.alerts_with_commands
WHERE threat_level = 'critical'
    AND command_id IS NULL
ORDER BY event_time ASC;

COMMENT ON VIEW gold.v_unresolved_critical_alerts IS 'Critical alerts awaiting command decision';

-- ============================================================================
-- MAINTENANCE FUNCTIONS
-- ============================================================================

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION gold.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add triggers for all tables
CREATE TRIGGER trg_eus_updated_at
    BEFORE UPDATE ON gold.effective_unit_strength
    FOR EACH ROW
    EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_ta_updated_at
    BEFORE UPDATE ON gold.threat_assessment
    FOR EACH ROW
    EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_awc_updated_at
    BEFORE UPDATE ON gold.alerts_with_commands
    FOR EACH ROW
    EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_lr_updated_at
    BEFORE UPDATE ON gold.logistics_readiness
    FOR EACH ROW
    EXECUTE FUNCTION gold.update_updated_at_column();

CREATE TRIGGER trg_ea_updated_at
    BEFORE UPDATE ON gold.engagement_analysis
    FOR EACH ROW
    EXECUTE FUNCTION gold.update_updated_at_column();

-- ============================================================================
-- PARTITIONING (Optional - for high-volume production)
-- ============================================================================

-- Example: Partition threat_assessment by event_time (monthly)
-- Uncomment and adjust for production use:

/*
CREATE TABLE gold.threat_assessment_template (LIKE gold.threat_assessment INCLUDING ALL);

-- Create partitions for each month
CREATE TABLE gold.threat_assessment_2025_01 PARTITION OF gold.threat_assessment
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE gold.threat_assessment_2025_02 PARTITION OF gold.threat_assessment
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

-- Add more partitions as needed...
*/

-- ============================================================================
-- GRANTS AND PERMISSIONS
-- ============================================================================

-- Grant read access to analyst role
-- CREATE ROLE analyst;
-- GRANT USAGE ON SCHEMA gold TO analyst;
-- GRANT SELECT ON ALL TABLES IN SCHEMA gold TO analyst;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA gold TO analyst;

-- Grant read/write access to operator role
-- CREATE ROLE operator;
-- GRANT USAGE ON SCHEMA gold TO operator;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA gold TO operator;
-- GRANT USAGE ON ALL SEQUENCES IN SCHEMA gold TO operator;

-- ============================================================================
-- INITIALIZATION COMPLETE
-- ============================================================================

\echo '============================================================================'
\echo 'Gold Layer (OLAP) Schema Initialized Successfully!'
\echo '============================================================================'
\echo 'Schema: gold'
\echo 'Tables: 5 (effective_unit_strength, threat_assessment, alerts_with_commands,'
\echo '           logistics_readiness, engagement_analysis)'
\echo 'Views: 5 helper views for MCP chatbot queries'
\echo 'Indexes: Optimized for real-time analytics'
\echo '============================================================================'

-- Display table information
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'gold'
ORDER BY tablename;