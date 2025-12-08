"""
Spark Job 2: Silver to Gold Layer Processing
============================================
Reads cleaned data from Delta Lake (Silver Layer) and writes aggregated,
denormalized views to Postgres (Gold Layer) for OLAP queries.

Architecture: Silver Layer (HDFS/Delta) -> Gold Layer (Postgres OLAP)
Processing: Batch/Micro-batch with windowing and aggregations
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
HDFS_BASE = "hdfs://namenode:9000/data/delta"
POSTGRES_URL = "jdbc:postgresql://postgres-dest:5432/jadc2_db"
POSTGRES_PROPS = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    """Initialize Spark session with Delta Lake and Postgres support"""
    return SparkSession.builder \
        .appName("JADC2-SilverToGold-Process") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def read_delta_table(spark, table_name):
    """Read Delta table from Silver layer"""
    path = f"{HDFS_BASE}/{table_name}"
    logger.info(f"Reading Delta table: {path}")
    return spark.read.format("delta").load(path)

def write_to_postgres(df, table_name, mode="overwrite"):
    """Write DataFrame to Postgres with upsert capability"""
    logger.info(f"Writing to Postgres table: gold.{table_name} (mode: {mode})")
    df.write \
        .jdbc(
            url=POSTGRES_URL,
            table=f"gold.{table_name}",
            mode=mode,
            properties=POSTGRES_PROPS
        )
    logger.info(f"Successfully wrote {df.count()} rows to gold.{table_name}")

def calculate_haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points using Haversine formula (in km)"""
    R = 6371.0  # Earth radius in km
    
    lat1_rad = F.radians(lat1)
    lat2_rad = F.radians(lat2)
    delta_lat = F.radians(lat2 - lat1)
    delta_lon = F.radians(lon2 - lon1)
    
    a = (F.sin(delta_lat / 2) ** 2 + 
         F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(delta_lon / 2) ** 2)
    c = 2 * F.asin(F.sqrt(a))
    
    return R * c

def process_effective_unit_strength(spark):
    """
    Gold Table: effective_unit_strength
    Calculate terrain/weather-adjusted combat effectiveness
    """
    logger.info("Processing effective_unit_strength...")
    
    # Read Silver tables
    units = read_delta_table(spark, "units")
    weapons = read_delta_table(spark, "weapons")
    regions = read_delta_table(spark, "regions")
    weather = read_delta_table(spark, "weather_events")
    unit_status = read_delta_table(spark, "unit_status_updates")
    
    # Get latest weather per region using window function
    weather_window = Window.partitionBy("region_id").orderBy(F.col("event_time").desc())
    latest_weather = weather.withColumn("rn", F.row_number().over(weather_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    # Get latest unit status
    status_window = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_status = unit_status.withColumn("rn", F.row_number().over(status_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    # Join all tables
    result = latest_status \
        .join(weapons, latest_status.unit_id == weapons.unit_id, "left") \
        .join(regions, latest_status.region_id == regions.id, "left") \
        .join(latest_weather, latest_status.region_id == latest_weather.region_id, "left")
    
    # Calculate terrain factor
    result = result.withColumn("terrain_factor",
        F.when(F.col("terrain_type") == "mountain", 0.8)
         .when(F.col("terrain_type") == "swamp", 0.7)
         .when(F.col("terrain_type") == "desert", 0.9)
         .otherwise(1.0)
    )
    
    # Calculate effective range (terrain + wind impact)
    result = result.withColumn("effective_range_km",
        F.col("range_km") * F.col("terrain_factor") * 
        (1 - 0.1 * F.coalesce(F.col("wind_speed_kmh"), F.lit(0)) / 100)
    )
    
    # Calculate adjusted hit probability (weather impacts)
    result = result.withColumn("adjusted_hit_probability",
        F.col("hit_probability") * 
        (1 - 0.15 * F.coalesce(F.col("precipitation_mm"), F.lit(0)) / 100) *
        (1 - 0.2 * F.coalesce(F.col("intensity"), F.lit(0)) * 
         F.when(F.col("type") == "fog", 1).otherwise(0))
    )
    
    # Calculate attacking strength
    result = result.withColumn("attacking_strength",
        F.col("adjusted_hit_probability") * F.col("health_percent") / 100
    )
    
    # Select final columns
    gold_df = result.select(
        latest_status.unit_id,
        latest_status.region_id,
        weapons.id.alias("weapon_id"),
        F.col("terrain_factor"),
        F.col("effective_range_km"),
        F.col("adjusted_hit_probability"),
        F.col("attacking_strength"),
        latest_status.event_time
    )
    
    write_to_postgres(gold_df, "effective_unit_strength")
    return gold_df

def process_threat_assessment(spark):
    """
    Gold Table: threat_assessment
    Threat prioritization with weather and EW adjustments
    """
    logger.info("Processing threat_assessment...")
    
    # Read Silver tables
    targets = read_delta_table(spark, "targets")
    detections = read_delta_table(spark, "detections")
    sensors = read_delta_table(spark, "sensors")
    units = read_delta_table(spark, "units")
    unit_status = read_delta_table(spark, "unit_status_updates")
    weather = read_delta_table(spark, "weather_events")
    ew_events = read_delta_table(spark, "cyber_ew_events")
    
    # Get latest detection per target
    detection_window = Window.partitionBy("target_id").orderBy(F.col("event_time").desc())
    latest_detections = detections.withColumn("rn", F.row_number().over(detection_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    # Join with targets
    result = latest_detections.join(targets, latest_detections.target_id == targets.id)
    
    # Join with sensors and units to get unit position
    result = result \
        .join(sensors, latest_detections.sensor_id == sensors.id, "left") \
        .join(units, sensors.unit_id == units.id, "left")
    
    # Get latest unit position
    status_window = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_unit_pos = unit_status.withColumn("rn", F.row_number().over(status_window)) \
        .filter(F.col("rn") == 1) \
        .select("unit_id", "lat", "lon", "event_time") \
        .withColumnRenamed("lat", "unit_lat") \
        .withColumnRenamed("lon", "unit_lon") \
        .withColumnRenamed("event_time", "unit_event_time") \
        .drop("rn")
    
    result = result.join(latest_unit_pos, sensors.unit_id == latest_unit_pos.unit_id, "left")
    
    # Calculate distance
    result = result.withColumn("distance_km",
        F.when(F.col("unit_lat").isNotNull() & F.col("unit_lon").isNotNull(),
            calculate_haversine_distance(
                F.col("unit_lat"), F.col("unit_lon"),
                latest_detections.lat, latest_detections.lon
            )
        ).otherwise(F.lit(1e9))  # Large value if position unknown
    )
    
    # Get latest weather for region
    weather_window = Window.partitionBy("region_id").orderBy(F.col("event_time").desc())
    latest_weather = weather.withColumn("rn", F.row_number().over(weather_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    result = result.join(latest_weather, 
        latest_detections.region_id == latest_weather.region_id, "left")
    
    # Get latest EW event for sensor
    ew_window = Window.partitionBy("target_sensor_id").orderBy(F.col("event_time").desc())
    latest_ew = ew_events.withColumn("rn", F.row_number().over(ew_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    result = result.join(latest_ew, 
        latest_detections.sensor_id == latest_ew.target_sensor_id, "left")
    
    # Calculate adjusted confidence
    result = result.withColumn("adjusted_confidence",
        latest_detections.confidence * 
        (1 - 0.3 * F.coalesce(latest_weather.intensity, F.lit(0)) * 
         F.when(F.col("type").isin("fog", "storm"), 1).otherwise(0)) *
        (1 - 0.5 * F.when(F.col("effect") == "jammed", 1).otherwise(0))
    )
    
    # Predict threat level
    result = result.withColumn("predicted_threat",
        F.when(
            ((latest_detections.speed_kmh > 500) | (targets.iff_status == "foe")) & 
            (F.col("adjusted_confidence") > 0.8),
            F.lit("high")
        ).otherwise(F.lit("medium"))
    )
    
    # Calculate response time
    result = result.withColumn("response_time_sec",
        F.when(F.col("distance_km") < 1e9,
            (F.col("distance_km") / F.when(latest_detections.speed_kmh > 0, 
                latest_detections.speed_kmh).otherwise(F.lit(1))) * 3600
        ).otherwise(F.lit(1e9))
    )
    
    # Select final columns
    gold_df = result.select(
        targets.target_id,
        latest_detections.region_id,
        targets.iff_status,
        F.col("adjusted_confidence"),
        F.col("predicted_threat"),
        F.col("distance_km"),
        F.col("response_time_sec"),
        latest_detections.event_time
    )
    
    write_to_postgres(gold_df, "threat_assessment")
    return gold_df

def process_alerts_with_commands(spark):
    """
    Gold Table: alerts_with_commands
    Alerts joined with authorized C2 actions
    """
    logger.info("Processing alerts_with_commands...")
    
    # Read Silver tables
    alerts = read_delta_table(spark, "alerts")
    commands = read_delta_table(spark, "commands")
    detections = read_delta_table(spark, "detections")
    users = read_delta_table(spark, "users")
    
    # Filter high/critical alerts
    high_alerts = alerts.filter(F.col("threat_level").isin("high", "critical"))
    
    # Join with detections to get region
    result = high_alerts.join(detections, high_alerts.detection_id == detections.id)
    
    # Join with commands
    result = result.join(commands, high_alerts.id == commands.alert_id, "left")
    
    # Join with users to filter commanders only
    result = result.join(users, commands.user_id == users.id, "left")
    
    # Add action only if user is commander
    result = result.withColumn("action",
        F.when(users.role == "commander", commands.action).otherwise(F.lit(None))
    )
    
    # Calculate latest event time
    result = result.withColumn("event_time",
        F.greatest(
            high_alerts.created_at,
            F.coalesce(commands.event_time, high_alerts.created_at)
        )
    )
    
    # Select final columns
    gold_df = result.select(
        high_alerts.id.alias("alert_id"),
        high_alerts.detection_id,
        detections.region_id,
        high_alerts.threat_level,
        commands.id.alias("command_id"),
        F.col("action"),
        users.id.alias("user_id"),
        F.col("event_time")
    ).distinct()
    
    write_to_postgres(gold_df, "alerts_with_commands")
    return gold_df

def process_logistics_readiness(spark):
    """
    Gold Table: logistics_readiness
    Supply feasibility under weather/terrain constraints
    """
    logger.info("Processing logistics_readiness...")
    
    # Read Silver tables
    supply_status = read_delta_table(spark, "supply_status")
    regions = read_delta_table(spark, "regions")
    weather = read_delta_table(spark, "weather_events")
    unit_status = read_delta_table(spark, "unit_status_updates")
    
    # Get latest supply status per unit
    supply_window = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_supply = supply_status.withColumn("rn", F.row_number().over(supply_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    # Get latest unit status to determine current region
    status_window = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_unit = unit_status.withColumn("rn", F.row_number().over(status_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    # Join supply with current unit location
    result = latest_supply.join(latest_unit, latest_supply.unit_id == latest_unit.unit_id)
    
    # Join with regions
    result = result.join(regions, latest_unit.region_id == regions.id)
    
    # Get latest weather for region
    weather_window = Window.partitionBy("region_id").orderBy(F.col("event_time").desc())
    latest_weather = weather.withColumn("rn", F.row_number().over(weather_window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn")
    
    result = result.join(latest_weather, latest_unit.region_id == latest_weather.region_id, "left")
    
    # Calculate terrain factor
    result = result.withColumn("terrain_factor",
        F.when(F.col("terrain_type") == "mountain", 0.7).otherwise(1.0)
    )
    
    # Calculate projected supply with weather/terrain impacts
    result = result.withColumn("projected_supply",
        latest_supply.supply_level * 
        (1 - 0.5 * F.coalesce(latest_weather.intensity, F.lit(0)) * 
         F.when(F.col("type").isin("storm", "rain"), 1).otherwise(0) * 
         F.col("terrain_factor"))
    )
    
    # Calculate resupply feasibility
    result = result.withColumn("resupply_feasibility",
        F.when(
            (F.coalesce(latest_weather.wind_speed_kmh, F.lit(0)) > 50) | 
            (F.coalesce(latest_weather.precipitation_mm, F.lit(0)) > 10),
            0
        ).otherwise(1)
    )
    
    # Calculate event time
    result = result.withColumn("event_time",
        F.greatest(
            latest_supply.event_time,
            F.coalesce(latest_weather.event_time, latest_supply.event_time)
        )
    )
    
    # Select final columns
    gold_df = result.select(
        latest_supply.unit_id,
        latest_unit.region_id,
        latest_supply.supply_level,
        F.col("projected_supply"),
        F.col("resupply_feasibility"),
        F.col("event_time")
    )
    
    write_to_postgres(gold_df, "logistics_readiness")
    return gold_df

def process_engagement_analysis(spark):
    """
    Gold Table: engagement_analysis
    After-action review with terrain/weather/EW context
    """
    logger.info("Processing engagement_analysis...")
    
    # Read Silver tables
    engagements = read_delta_table(spark, "engagement_events")
    unit_status = read_delta_table(spark, "unit_status_updates")
    regions = read_delta_table(spark, "regions")
    weather = read_delta_table(spark, "weather_events")
    ew_events = read_delta_table(spark, "cyber_ew_events")
    weapons = read_delta_table(spark, "weapons")
    
    # For each engagement, find attacker's region at time of engagement
    # Using window to get closest unit status update before engagement
    result = engagements.alias("eng")
    
    # Get unit status closest to engagement time
    unit_with_time = unit_status.alias("us").join(
        engagements.alias("eng2"),
        (unit_status.unit_id == engagements.attacker_id) &
        (unit_status.event_time <= engagements.event_time),
        "inner"
    )
    
    status_window = Window.partitionBy("eng2.id").orderBy(F.col("us.event_time").desc())
    closest_status = unit_with_time.withColumn("rn", F.row_number().over(status_window)) \
        .filter(F.col("rn") == 1) \
        .select(
            F.col("eng2.id").alias("engagement_id"),
            F.col("us.region_id")
        )
    
    result = result.join(closest_status, result.id == closest_status.engagement_id, "left")
    
    # Join with regions
    result = result.join(regions, closest_status.region_id == regions.id, "left")
    
    # Join with weapons
    result = result.join(weapons, result.weapon_id == weapons.id, "left")
    
    # Get weather at engagement time and region
    weather_joined = result.join(
        weather.alias("w"),
        (closest_status.region_id == weather.region_id) &
        (weather.event_time <= result.event_time),
        "left"
    )
    
    weather_window = Window.partitionBy("eng.id").orderBy(F.col("w.event_time").desc())
    closest_weather = weather_joined.withColumn("rn", F.row_number().over(weather_window)) \
        .filter((F.col("rn") == 1) | F.col("w.id").isNull()) \
        .drop("rn")
    
    # Get EW events affecting the engagement
    ew_joined = closest_weather.join(
        ew_events.alias("ew"),
        (ew_events.event_time <= result.event_time) &
        (F.abs(F.unix_timestamp(ew_events.event_time) - F.unix_timestamp(result.event_time)) < 300),  # Within 5 minutes
        "left"
    )
    
    # Calculate adjusted hit probability
    result_final = ew_joined.withColumn("adjusted_hit_probability",
        weapons.hit_probability * 
        (1 - 0.2 * F.coalesce(F.col("w.intensity"), F.lit(0)) * 
         F.when(F.col("w.type") == "fog", 1).otherwise(0)) *
        (1 - 0.3 * F.when(F.col("ew.effect") == "jammed", 1).otherwise(0))
    )
    
    # Calculate impact factor
    result_final = result_final.withColumn("impact_factor",
        (1 - 0.2 * F.when(F.col("w.type") == "fog", 1).otherwise(0)) *
        (1 - 0.3 * F.when(F.col("ew.effect") == "jammed", 1).otherwise(0)) *
        F.when(regions.terrain_type == "mountain", 0.8).otherwise(1.0)
    )
    
    # Select final columns
    gold_df = result_final.select(
        F.col("eng.id").alias("engagement_id"),
        F.col("eng.attacker_id"),
        F.col("eng.target_id"),
        F.col("eng.weapon_id"),
        closest_status.region_id,
        F.col("eng.result"),
        F.col("adjusted_hit_probability"),
        F.col("impact_factor"),
        F.col("eng.event_time")
    ).distinct()
    
    write_to_postgres(gold_df, "engagement_analysis")
    return gold_df

def main():
    """Main processing pipeline"""
    logger.info("Starting Silver to Gold ETL job...")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Process all Gold tables
        logger.info("=" * 60)
        process_effective_unit_strength(spark)
        
        logger.info("=" * 60)
        process_threat_assessment(spark)
        
        logger.info("=" * 60)
        process_alerts_with_commands(spark)
        
        logger.info("=" * 60)
        process_logistics_readiness(spark)
        
        logger.info("=" * 60)
        process_engagement_analysis(spark)
        
        logger.info("=" * 60)
        logger.info("All Gold tables processed successfully!")
        
    except Exception as e:
        logger.error(f"Error in processing: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()