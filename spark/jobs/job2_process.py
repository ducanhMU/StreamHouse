"""
Spark Job 2: Silver to Gold Layer Processing
Reads refined data from Delta Lake (Silver Layer), applies business logic, 
and writes to PostgreSQL (Gold Layer).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
import logging

# ==========================================
# CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("JADC2_SilverToGold")

# Input: Silver Layer (Delta Lake)
HDFS_SILVER_PATH = "hdfs://namenode:9000/data/delta/silver"

# Output: Gold Layer (PostgreSQL)
POSTGRES_URL = "jdbc:postgresql://postgres-dest:5432/jadc2_db"
POSTGRES_PROPS = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("JADC2_Job2_Silver_To_Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()

def read_silver(spark, table_name):
    """Reads a Silver table from Delta Lake."""
    path = f"{HDFS_SILVER_PATH}/{table_name}"
    logger.info(f"Reading Silver Table: {table_name} from {path}")
    return spark.read.format("delta").load(path)

def write_gold(df, table_name):
    """Writes a Gold DataFrame to PostgreSQL."""
    logger.info(f"Writing {df.count()} rows to Gold Table: gold.{table_name}")
    
    # Write mode is Append (or Overwrite depending on requirement). 
    # For reporting tables, usually Overwrite specific partitions or Append new events.
    # Here we use Overwrite for simplicity of the snapshot view.
    df.write.jdbc(
        url=POSTGRES_URL,
        table=f"gold.{table_name}",
        mode="overwrite", 
        properties=POSTGRES_PROPS
    )

def get_latest_snapshot(df, group_cols, time_col="event_time"):
    """Helper to get the most recent record for each ID (deduplication)."""
    window = Window.partitionBy(group_cols).orderBy(F.col(time_col).desc())
    return df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

def calculate_distance_km(lat1, lon1, lat2, lon2):
    """Haversine formula to calculate distance in km."""
    # Convert Decimal to Double for Trig functions
    R = 6371.0
    lat1_rad = F.radians(lat1.cast(DoubleType()))
    lon1_rad = F.radians(lon1.cast(DoubleType()))
    lat2_rad = F.radians(lat2.cast(DoubleType()))
    lon2_rad = F.radians(lon2.cast(DoubleType()))
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = (F.sin(dlat / 2)**2) + F.cos(lat1_rad) * F.cos(lat2_rad) * (F.sin(dlon / 2)**2)
    c = 2 * F.asin(F.sqrt(a))
    return R * c

# ==========================================
# PROCESSING LOGIC (GOLD TABLES)
# ==========================================

def process_effective_unit_strength(spark):
    """
    Gold Table 1: effective_unit_strength
    Logic: Unit combat power adjusted by Terrain & Weather.
    """
    logger.info("Processing: effective_unit_strength")
    
    # 1. Load Data
    units = read_silver(spark, "units")
    weapons = read_silver(spark, "weapons")
    regions = read_silver(spark, "regions")
    weather = read_silver(spark, "weather_events")
    unit_status = read_silver(spark, "unit_status_updates")

    # 2. Get Latest State (Snapshot)
    latest_status = get_latest_snapshot(unit_status, "unit_id")
    latest_weather = get_latest_snapshot(weather, "region_id")

    # 3. Join
    df = latest_status.alias("s") \
        .join(units.alias("u"), F.col("s.unit_id") == F.col("u.id")) \
        .join(regions.alias("r"), F.col("s.region_id") == F.col("r.id")) \
        .join(weapons.alias("w"), F.col("s.unit_id") == F.col("w.unit_id"), "left") \
        .join(latest_weather.alias("we"), F.col("s.region_id") == F.col("we.region_id"), "left")

    # 4. Calculations
    # Handle Nulls for weather using coalesce
    wind = F.coalesce(F.col("we.wind_speed_kmh"), F.lit(0)).cast(DoubleType())
    precip = F.coalesce(F.col("we.precipitation_mm"), F.lit(0)).cast(DoubleType())
    intensity = F.coalesce(F.col("we.intensity"), F.lit(0)).cast(DoubleType())
    
    df = df.withColumn("terrain_factor", 
        F.when(F.col("r.terrain_type") == "mountain", 0.8)
         .when(F.col("r.terrain_type") == "swamp", 0.7)
         .otherwise(1.0)
    ).withColumn("effective_range_km", 
        F.col("w.range_km").cast(DoubleType()) * F.col("terrain_factor") * (1 - 0.1 * wind / 100)
    ).withColumn("adjusted_hit_probability",
        F.col("w.hit_probability").cast(DoubleType()) * (1 - 0.15 * precip) * (1 - 0.2 * intensity * F.when(F.col("we.type") == "fog", 1).otherwise(0))
    ).withColumn("attacking_strength",
        F.col("adjusted_hit_probability") * F.col("s.health_percent") / 100
    )

    # 5. Select & Write
    gold_df = df.select(
        F.col("s.unit_id"),
        F.col("s.region_id"),
        F.col("w.id").alias("weapon_id"),
        F.col("terrain_factor"),
        F.col("effective_range_km"),
        F.col("adjusted_hit_probability"),
        F.col("attacking_strength"),
        F.col("s.event_time")
    )
    
    write_gold(gold_df, "effective_unit_strength")


def process_threat_assessment(spark):
    """
    Gold Table 2: threat_assessment
    Logic: Prioritize threats based on distance, capability, and environment.
    """
    logger.info("Processing: threat_assessment")

    targets = read_silver(spark, "targets")
    detections = read_silver(spark, "detections")
    sensors = read_silver(spark, "sensors")
    unit_status = read_silver(spark, "unit_status_updates")
    weather = read_silver(spark, "weather_events")
    ew_events = read_silver(spark, "cyber_ew_events")

    # Get Snapshots
    latest_detections = get_latest_snapshot(detections, "target_id") # Assuming we want latest detection per target
    latest_unit_pos = get_latest_snapshot(unit_status, "unit_id")
    latest_weather = get_latest_snapshot(weather, "region_id")
    latest_ew = get_latest_snapshot(ew_events, "target_sensor_id")

    # Join Chain
    # Detection -> Target
    # Detection -> Sensor -> Unit -> Unit Position (to calculate distance)
    # Detection -> Region -> Weather
    # Sensor -> EW
    
    df = latest_detections.alias("d") \
        .join(targets.alias("t"), F.col("d.target_id") == F.col("t.id")) \
        .join(sensors.alias("s"), F.col("d.sensor_id") == F.col("s.id")) \
        .join(latest_unit_pos.alias("u_pos"), F.col("s.unit_id") == F.col("u_pos.unit_id"), "left") \
        .join(latest_weather.alias("we"), F.col("d.region_id") == F.col("we.region_id"), "left") \
        .join(latest_ew.alias("ew"), F.col("d.sensor_id") == F.col("ew.target_sensor_id"), "left")

    # Variables
    intensity = F.coalesce(F.col("we.intensity"), F.lit(0)).cast(DoubleType())
    is_jammed = F.when(F.col("ew.effect") == "jammed", 1).otherwise(0)
    is_bad_weather = F.when(F.col("we.type").isin("fog", "storm"), 1).otherwise(0)
    
    # Calculations
    df = df.withColumn("distance_km", 
        F.when(F.col("u_pos.lat").isNotNull(), 
               calculate_distance_km(F.col("u_pos.lat"), F.col("u_pos.lon"), F.col("d.lat"), F.col("d.lon")))
         .otherwise(F.lit(1000000.0)) # Max value if unknown
    ).withColumn("adjusted_confidence",
        F.col("d.confidence").cast(DoubleType()) * (1 - 0.3 * intensity * is_bad_weather) * (1 - 0.5 * is_jammed)
    ).withColumn("predicted_threat",
        F.when(
            ((F.col("d.speed_kmh") > 500) | (F.col("t.iff_status") == "foe")) & 
            (F.col("adjusted_confidence") > 0.8), 
            "high"
        ).otherwise("medium")
    ).withColumn("response_time_sec",
        F.when(F.col("d.speed_kmh") > 0, 
               (F.col("distance_km") / F.col("d.speed_kmh")) * 3600)
         .otherwise(None)
    )

    gold_df = df.select(
        F.col("t.id").alias("target_id"),
        F.col("d.region_id"),
        F.col("t.iff_status"),
        F.col("adjusted_confidence"),
        F.col("predicted_threat"),
        F.col("distance_km"),
        F.col("response_time_sec"),
        F.col("d.event_time")
    )

    write_gold(gold_df, "threat_assessment")


def process_alerts_with_commands(spark):
    """
    Gold Table 3: alerts_with_commands
    Logic: Correlate Alerts with Command actions.
    """
    logger.info("Processing: alerts_with_commands")

    alerts = read_silver(spark, "alerts")
    commands = read_silver(spark, "commands")
    detections = read_silver(spark, "detections")
    users = read_silver(spark, "users")

    # Filter High Threats
    high_alerts = alerts.filter(F.col("threat_level").isin("high", "critical"))

    df = high_alerts.alias("a") \
        .join(detections.alias("d"), F.col("a.detection_id") == F.col("d.id")) \
        .join(commands.alias("c"), F.col("a.id") == F.col("c.alert_id"), "left") \
        .join(users.alias("u"), F.col("c.user_id") == F.col("u.id"), "left")

    df = df.withColumn("action", 
        F.when(F.col("u.role") == "commander", F.col("c.action")).otherwise(None)
    ).withColumn("final_event_time", 
        F.greatest(F.col("a.event_time"), F.col("c.event_time"))
    )

    gold_df = df.select(
        F.col("a.id").alias("alert_id"),
        F.col("a.detection_id"),
        F.col("d.region_id"),
        F.col("a.threat_level"),
        F.col("c.id").alias("command_id"),
        F.col("action"),
        F.col("u.id").alias("user_id"),
        F.col("final_event_time").alias("event_time")
    ).distinct() # Commands join might duplicate alerts if multiple commands exist

    write_gold(gold_df, "alerts_with_commands")


def process_logistics_readiness(spark):
    """
    Gold Table 4: logistics_readiness
    Logic: Supply levels constrained by weather access.
    """
    logger.info("Processing: logistics_readiness")

    supply = read_silver(spark, "supply_status")
    regions = read_silver(spark, "regions")
    weather = read_silver(spark, "weather_events")
    unit_status = read_silver(spark, "unit_status_updates")

    # Latest states
    latest_supply = get_latest_snapshot(supply, "unit_id")
    latest_unit_loc = get_latest_snapshot(unit_status, "unit_id") # To find unit's current region
    latest_weather = get_latest_snapshot(weather, "region_id")

    df = latest_supply.alias("s") \
        .join(latest_unit_loc.alias("u"), F.col("s.unit_id") == F.col("u.unit_id")) \
        .join(regions.alias("r"), F.col("u.region_id") == F.col("r.id")) \
        .join(latest_weather.alias("we"), F.col("u.region_id") == F.col("we.region_id"), "left")

    # Variables
    intensity = F.coalesce(F.col("we.intensity"), F.lit(0)).cast(DoubleType())
    wind = F.coalesce(F.col("we.wind_speed_kmh"), F.lit(0)).cast(DoubleType())
    precip = F.coalesce(F.col("we.precipitation_mm"), F.lit(0)).cast(DoubleType())
    
    df = df.withColumn("terrain_penalty", 
        F.when(F.col("r.terrain_type") == "mountain", 0.7).otherwise(1.0)
    ).withColumn("projected_supply",
        F.col("s.supply_level").cast(DoubleType()) * (1 - 0.5 * intensity * F.when(F.col("we.type").isin("storm", "rain"), 1).otherwise(0) * F.col("terrain_penalty"))
    ).withColumn("resupply_feasibility",
        F.when((wind > 50) | (precip > 10), 0).otherwise(1)
    ).withColumn("final_event_time", 
         F.greatest(F.col("s.event_time"), F.col("we.event_time"))
    )

    gold_df = df.select(
        F.col("s.unit_id"),
        F.col("u.region_id"),
        F.col("s.supply_level"),
        F.col("projected_supply"),
        F.col("resupply_feasibility"),
        F.col("final_event_time").alias("event_time")
    )

    write_gold(gold_df, "logistics_readiness")


def process_engagement_analysis(spark):
    """
    Gold Table 5: engagement_analysis
    Logic: Complex correlation of battle results with environmental context AT THAT TIME.
    """
    logger.info("Processing: engagement_analysis")

    engagements = read_silver(spark, "engagement_events")
    unit_status = read_silver(spark, "unit_status_updates")
    regions = read_silver(spark, "regions")
    weather = read_silver(spark, "weather_events")
    ew_events = read_silver(spark, "cyber_ew_events")
    weapons = read_silver(spark, "weapons")

    # Step 1: Find Attacker's Region AT THE TIME of engagement
    # Using a Window strategy to find the last status update before the engagement
    # (Simplified for Batch: Join on Unit ID, Filter Status Time <= Eng Time, Take Max Status Time)
    
    eng_context = engagements.alias("e") \
        .join(unit_status.alias("us"), 
              (F.col("e.attacker_id") == F.col("us.unit_id")) & 
              (F.col("us.event_time") <= F.col("e.event_time")))

    # Pick the status closest to engagement time
    window_spec = Window.partitionBy("e.id").orderBy(F.col("us.event_time").desc())
    eng_with_region = eng_context.withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .select(
            F.col("e.*"), 
            F.col("us.region_id").alias("attacker_region_id")
        )

    # Step 2: Join with static/slow changing dimensions (Regions, Weapons)
    df = eng_with_region.alias("e") \
        .join(regions.alias("r"), F.col("e.attacker_region_id") == F.col("r.id"), "left") \
        .join(weapons.alias("w"), F.col("e.weapon_id") == F.col("w.id"), "left")

    # Step 3: Find Weather AT THE TIME of engagement in that region
    # Similar window logic
    df_weather = df.join(weather.alias("we"), 
        (F.col("e.attacker_region_id") == F.col("we.region_id")) & 
        (F.col("we.event_time") <= F.col("e.event_time")), "left")
    
    w_window = Window.partitionBy("e.id").orderBy(F.col("we.event_time").desc())
    df_step3 = df_weather.withColumn("rn", F.row_number().over(w_window)).filter(F.col("rn") == 1).drop("rn")

    # Step 4: Find EW Events (Assuming jamming impacts the whole region or correlated via sensor logic - simplifying to Time proximity here)
    # Note: In real scenarios, we need to link Attacker Sensor -> EW Target Sensor. 
    # Here we assume checking if ANY jamming was active nearby.
    
    # Logic: Variables
    intensity = F.coalesce(F.col("we.intensity"), F.lit(0)).cast(DoubleType())
    is_fog = F.when(F.col("we.type") == "fog", 1).otherwise(0)
    # Placeholder for EW join (requires complex sensor mapping, skipping strict join for simplicity, assuming no jamming if null)
    is_jammed = F.lit(0) 

    df_final = df_step3.withColumn("adjusted_hit_probability",
        F.col("w.hit_probability").cast(DoubleType()) * (1 - 0.2 * intensity * is_fog) * (1 - 0.3 * is_jammed)
    ).withColumn("impact_factor",
        (1 - 0.2 * is_fog) * (1 - 0.3 * is_jammed) * F.when(F.col("r.terrain_type") == "mountain", 0.8).otherwise(1.0)
    )

    gold_df = df_final.select(
        F.col("e.id").alias("engagement_id"),
        F.col("e.attacker_id"),
        F.col("e.target_id"),
        F.col("e.weapon_id"),
        F.col("e.attacker_region_id").alias("region_id"),
        F.col("e.result"),
        F.col("adjusted_hit_probability"),
        F.col("impact_factor"),
        F.col("e.event_time")
    )

    write_gold(gold_df, "engagement_analysis")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        process_effective_unit_strength(spark)
        process_threat_assessment(spark)
        process_alerts_with_commands(spark)
        process_logistics_readiness(spark)
        process_engagement_analysis(spark)
        logger.info("All Gold Layer processing completed.")
    except Exception as e:
        logger.error(f"Gold processing failed: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()