"""
Spark Job 2: Silver to Gold Layer Processing
Reads cleaned data from Delta Lake (Bronze Layer), casts types, performs aggregations,
and writes to Postgres (Gold Layer).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
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
    """Initialize Spark session with Delta Lake and Hive support"""
    return SparkSession.builder \
        .appName("JADC2-SilverToGold-Process") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()

def read_bronze_table(spark, table_name):  
    """
    Read from Bronze layer and Cast columns automatically.
    Job 1 saves data as 'tablename_raw' in 'bronze' folder with all String types.
    """
    # Path matches Job 1 output
    path = f"{HDFS_BASE}/bronze/{table_name}_raw"
    logger.info(f"Reading Delta table: {path}")
    
    try:
        df = spark.read.format("delta").load(path)
    except Exception as e:
        logger.error(f"Table {table_name} not found at {path}. Job 1 might not have processed it yet.")
        raise e
    
    # AUTO-CASTING: Convert columns from String to appropriate types for calculation
    for col_name in df.columns:
        # Numeric casting (Double)   
        if any(x in col_name for x in ['lat', 'lon', 'speed', 'range', 'probability', 'health', 'supply', 'intensity', 'precipitation']):
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
        # Integer casting
        elif any(x in col_name for x in ['id', 'level', 'count']):
            if col_name == 'id' or col_name.endswith('_id'):
                df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
        # Timestamp casting
        elif 'time' in col_name or 'created_at' in col_name or 'updated_at' in col_name:
             df = df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
             
    return df

def write_to_postgres(df, table_name, mode="overwrite"):
    """Write DataFrame to Postgres"""
    logger.info(f"Writing to Postgres table: gold.{table_name} (mode: {mode})")
    try:
        df.write \
            .jdbc(
                url=POSTGRES_URL,
                table=f"gold.{table_name}",
                mode=mode,
                properties=POSTGRES_PROPS
            )
        logger.info(f"Successfully wrote rows to gold.{table_name}")
    except Exception as e:
        logger.error(f"Failed to write to Postgres: {e}")

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
    """Gold Table: effective_unit_strength"""
    logger.info("Processing effective_unit_strength...")
    
    units = read_bronze_table(spark, "units")
    weapons = read_bronze_table(spark, "weapons")
    regions = read_bronze_table(spark, "regions")
    weather = read_bronze_table(spark, "weather_events")
    unit_status = read_bronze_table(spark, "unit_status_updates")
    
    # Latest Weather
    weather_window = Window.partitionBy("region_id").orderBy(F.col("event_time").desc())
    latest_weather = weather.withColumn("rn", F.row_number().over(weather_window)).filter(F.col("rn") == 1).drop("rn")
    
    # Latest Unit Status
    status_window = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_status = unit_status.withColumn("rn", F.row_number().over(status_window)).filter(F.col("rn") == 1).drop("rn")
    
    # Join
    result = latest_status \
        .join(weapons, latest_status.unit_id == weapons.unit_id, "left") \
        .join(regions, latest_status.region_id == regions.id, "left") \
        .join(latest_weather, latest_status.region_id == latest_weather.region_id, "left")
    
    # Calculations
    result = result.withColumn("terrain_factor",
        F.when(F.col("terrain_type") == "mountain", 0.8)
         .when(F.col("terrain_type") == "swamp", 0.7)
         .when(F.col("terrain_type") == "desert", 0.9)
         .otherwise(1.0)
    ).withColumn("effective_range_km",
        F.col("range_km") * F.col("terrain_factor") * (1 - 0.1 * F.coalesce(F.col("wind_speed_kmh"), F.lit(0)) / 100)
    ).withColumn("adjusted_hit_probability",
        F.col("hit_probability") * (1 - 0.15 * F.coalesce(F.col("precipitation_mm"), F.lit(0)) / 100) *
        (1 - 0.2 * F.coalesce(F.col("intensity"), F.lit(0)) * F.when(F.col("type") == "fog", 1).otherwise(0))
    ).withColumn("attacking_strength",
        F.col("adjusted_hit_probability") * F.col("health_percent") / 100
    )
    
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

def process_threat_assessment(spark):
    """Gold Table: threat_assessment"""
    logger.info("Processing threat_assessment...")
    
    targets = read_bronze_table(spark, "targets")
    detections = read_bronze_table(spark, "detections")
    sensors = read_bronze_table(spark, "sensors")
    units = read_bronze_table(spark, "units")
    unit_status = read_bronze_table(spark, "unit_status_updates")
    weather = read_bronze_table(spark, "weather_events")
    ew_events = read_bronze_table(spark, "cyber_ew_events")
    
    # Latest Detections
    det_window = Window.partitionBy("target_id").orderBy(F.col("event_time").desc())
    latest_detections = detections.withColumn("rn", F.row_number().over(det_window)).filter(F.col("rn") == 1).drop("rn")
    
    result = latest_detections.join(targets, latest_detections.target_id == targets.id) \
        .join(sensors, latest_detections.sensor_id == sensors.id, "left") \
        .join(units, sensors.unit_id == units.id, "left")
    
    # Latest Unit Pos
    status_window = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_unit_pos = unit_status.withColumn("rn", F.row_number().over(status_window)).filter(F.col("rn") == 1) \
        .select(F.col("unit_id"), F.col("lat").alias("unit_lat"), F.col("lon").alias("unit_lon")).drop("rn")
    
    result = result.join(latest_unit_pos, sensors.unit_id == latest_unit_pos.unit_id, "left")
    
    # Distance
    result = result.withColumn("distance_km",
        F.when(F.col("unit_lat").isNotNull(),
            calculate_haversine_distance(F.col("unit_lat"), F.col("unit_lon"), latest_detections.lat, latest_detections.lon)
        ).otherwise(F.lit(999999.0))
    )
    
    # Weather & EW joins
    weather_window = Window.partitionBy("region_id").orderBy(F.col("event_time").desc())
    latest_weather = weather.withColumn("rn", F.row_number().over(weather_window)).filter(F.col("rn") == 1).drop("rn")
    
    ew_window = Window.partitionBy("target_sensor_id").orderBy(F.col("event_time").desc())
    latest_ew = ew_events.withColumn("rn", F.row_number().over(ew_window)).filter(F.col("rn") == 1).drop("rn")
    
    result = result.join(latest_weather, latest_detections.region_id == latest_weather.region_id, "left") \
                   .join(latest_ew, latest_detections.sensor_id == latest_ew.target_sensor_id, "left")
    
    # Threat logic
    result = result.withColumn("adjusted_confidence",
        latest_detections.confidence * (1 - 0.3 * F.coalesce(latest_weather.intensity, F.lit(0)) * F.when(F.col("type").isin("fog", "storm"), 1).otherwise(0)) *
        (1 - 0.5 * F.when(F.col("effect") == "jammed", 1).otherwise(0))
    ).withColumn("predicted_threat",
        F.when(((latest_detections.speed_kmh > 500) | (targets.iff_status == "foe")) & (F.col("adjusted_confidence") > 0.8), "high").otherwise("medium")
    ).withColumn("response_time_sec",
        F.when(F.col("distance_km") < 999999,
            (F.col("distance_km") / F.when(latest_detections.speed_kmh > 0, latest_detections.speed_kmh).otherwise(1)) * 3600
        ).otherwise(999999.0)
    )
    
    gold_df = result.select(
        targets.id.alias("target_id"),
        latest_detections.region_id,
        targets.iff_status,
        F.col("adjusted_confidence"),
        F.col("predicted_threat"),
        F.col("distance_km"),
        F.col("response_time_sec"),
        latest_detections.event_time
    )
    
    write_to_postgres(gold_df, "threat_assessment")

def process_alerts_with_commands(spark):
    """Gold Table: alerts_with_commands"""
    logger.info("Processing alerts_with_commands...")
    
    alerts = read_bronze_table(spark, "alerts")
    commands = read_bronze_table(spark, "commands")
    detections = read_bronze_table(spark, "detections")
    users = read_bronze_table(spark, "users")
    
    high_alerts = alerts.filter(F.col("threat_level").isin("high", "critical"))
    
    result = high_alerts.join(detections, high_alerts.detection_id == detections.id) \
        .join(commands, high_alerts.id == commands.alert_id, "left") \
        .join(users, commands.user_id == users.id, "left")
    
    result = result.withColumn("action", F.when(users.role == "commander", commands.action).otherwise(F.lit(None))) \
        .withColumn("event_time", F.greatest(high_alerts.created_at, F.coalesce(commands.event_time, high_alerts.created_at)))
    
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

def process_logistics_readiness(spark):
    """Gold Table: logistics_readiness"""
    logger.info("Processing logistics_readiness...")
    
    supply_status = read_bronze_table(spark, "supply_status")
    regions = read_bronze_table(spark, "regions")
    weather = read_bronze_table(spark, "weather_events")
    unit_status = read_bronze_table(spark, "unit_status_updates")
    
    supply_win = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_supply = supply_status.withColumn("rn", F.row_number().over(supply_win)).filter(F.col("rn") == 1).drop("rn")
    
    status_win = Window.partitionBy("unit_id").orderBy(F.col("event_time").desc())
    latest_unit = unit_status.withColumn("rn", F.row_number().over(status_win)).filter(F.col("rn") == 1).drop("rn")
    
    result = latest_supply.join(latest_unit, "unit_id").join(regions, latest_unit.region_id == regions.id)
    
    weather_win = Window.partitionBy("region_id").orderBy(F.col("event_time").desc())
    latest_weather = weather.withColumn("rn", F.row_number().over(weather_win)).filter(F.col("rn") == 1).drop("rn")
    
    result = result.join(latest_weather, latest_unit.region_id == latest_weather.region_id, "left")
    result = result.withColumn("terrain_factor", F.when(F.col("terrain_type") == "mountain", 0.7).otherwise(1.0)) \
        .withColumn("projected_supply",
            latest_supply.supply_level * (1 - 0.5 * F.coalesce(latest_weather.intensity, F.lit(0)) * F.when(F.col("type").isin("storm", "rain"), 1).otherwise(0) * F.col("terrain_factor"))
        ).withColumn("resupply_feasibility",
            F.when((F.coalesce(latest_weather.wind_speed_kmh, F.lit(0)) > 50) | (F.coalesce(latest_weather.precipitation_mm, F.lit(0)) > 10), 0).otherwise(1)
        ).withColumn("event_time", F.greatest(latest_supply.event_time, F.coalesce(latest_weather.event_time, latest_supply.event_time)))
    
    gold_df = result.select(
        latest_supply.unit_id,
        latest_unit.region_id,
        latest_supply.supply_level,
        F.col("projected_supply"),
        F.col("resupply_feasibility"),
        F.col("event_time")
    )
    write_to_postgres(gold_df, "logistics_readiness")

def process_engagement_analysis(spark):
    """Gold Table: engagement_analysis"""
    logger.info("Processing engagement_analysis...")
    
    engagements = read_bronze_table(spark, "engagement_events")
    unit_status = read_bronze_table(spark, "unit_status_updates")
    regions = read_bronze_table(spark, "regions")
    weather = read_bronze_table(spark, "weather_events")
    ew_events = read_bronze_table(spark, "cyber_ew_events")
    weapons = read_bronze_table(spark, "weapons")
    
    # Find region at time of engagement
    unit_with_time = unit_status.alias("us").join(engagements.alias("eng2"), (unit_status.unit_id == engagements.attacker_id) & (unit_status.event_time <= engagements.event_time))
    status_win = Window.partitionBy("eng2.id").orderBy(F.col("us.event_time").desc())
    closest_status = unit_with_time.withColumn("rn", F.row_number().over(status_win)).filter(F.col("rn") == 1).select(F.col("eng2.id").alias("engagement_id"), F.col("us.region_id"))
    
    result = engagements.alias("eng").join(closest_status, F.col("eng.id") == closest_status.engagement_id, "left") \
        .join(regions, closest_status.region_id == regions.id, "left") \
        .join(weapons, F.col("eng.weapon_id") == weapons.id, "left")
        
    weather_joined = result.join(weather.alias("w"), (closest_status.region_id == weather.region_id) & (weather.event_time <= result.event_time), "left")
    weather_win = Window.partitionBy("eng.id").orderBy(F.col("w.event_time").desc())
    closest_weather = weather_joined.withColumn("rn", F.row_number().over(weather_win)).filter((F.col("rn") == 1) | F.col("w.id").isNull()).drop("rn")
    
    ew_joined = closest_weather.join(ew_events.alias("ew"), (ew_events.event_time <= result.event_time) & (F.abs(F.unix_timestamp(ew_events.event_time) - F.unix_timestamp(result.event_time)) < 300), "left")
    
    final_df = ew_joined.withColumn("adjusted_hit_probability",
        weapons.hit_probability * (1 - 0.2 * F.coalesce(F.col("w.intensity"), F.lit(0)) * F.when(F.col("w.type") == "fog", 1).otherwise(0)) *
        (1 - 0.3 * F.when(F.col("ew.effect") == "jammed", 1).otherwise(0))
    ).withColumn("impact_factor",
        (1 - 0.2 * F.when(F.col("w.type") == "fog", 1).otherwise(0)) *
        (1 - 0.3 * F.when(F.col("ew.effect") == "jammed", 1).otherwise(0)) *
        F.when(regions.terrain_type == "mountain", 0.8).otherwise(1.0)
    ).select(
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
    
    write_to_postgres(final_df, "engagement_analysis")

def main():
    logger.info("Starting Silver to Gold ETL job...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        process_effective_unit_strength(spark)
        process_threat_assessment(spark)
        process_alerts_with_commands(spark)
        process_logistics_readiness(spark)
        process_engagement_analysis(spark)
        logger.info("All Gold tables processed successfully!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()