"""
Spark Job 2: Silver to Gold Layer Processing (Continuous Micro-batch)
Reads refined data from Delta Lake (Silver Layer), applies business logic, 
and writes to PostgreSQL (Gold Layer) every 2 minutes.

FIXES APPLIED:
- Explicit UUID casting for ID columns.
- Context-aware casting for 'target_id' (UUID in engagement, Text in threat).
- Enum casting for Postgres custom types.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, TimestampType, StringType
import logging
import time
import psycopg2 # Standard driver for Postgres SQL execution

# ==========================================
# CONFIGURATION
# ==========================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("JADC2_SilverToGold")

# Input: Silver Layer (Delta Lake)
HDFS_SILVER_PATH = "hdfs://namenode:9000/data/delta/silver"

# Output: Gold Layer (PostgreSQL)
POSTGRES_URL = "jdbc:postgresql://postgres-dest:5432/jadc2_db?stringtype=unspecified"

POSTGRES_PROPS = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Loop Config
LOOP_INTERVAL_SECONDS = 120  # 2 Minutes

# Columns that are ALWAYS UUIDs in your schema
GLOBAL_UUID_COLS = {
    "unit_id", "region_id", "weapon_id", "alert_id", 
    "detection_id", "command_id", "user_id", 
    "attacker_id", "engagement_id", "sensor_id"
}

# Columns that map to Postgres Enums
ENUM_CASTS = {
    "iff_status": "gold.iff_status",
    "threat_level": "gold.threat_level",
    "result": "gold.engagement_result",
    "unit_status": "gold.unit_status"
}

def create_spark_session():
    # Optimized configs for long-running jobs with growing data
    return SparkSession.builder \
        .appName("JADC2_Job2_Silver_To_Gold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "100s") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.cleaner.periodicGC.interval", "10min") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

def read_silver(spark, table_name):
    """Reads a Silver table from Delta Lake."""
    path = f"{HDFS_SILVER_PATH}/{table_name}"
    # Read full table to ensure we have latest state for deduplication
    return spark.read.format("delta").load(path)

def write_gold(df, table_name, primary_keys):
    """Writes a Gold DataFrame to PostgreSQL using optimized UPSERT logic with Type Casting."""
    start_time = time.time()
    try:
        # Repartition to reduce memory pressure and parallelize write
        count = df.count()
        if count == 0:
            logger.info(f"[{table_name}] No data to write. Skipping.")
            return

        logger.info(f"Upserting {count} rows to Gold Table: gold.{table_name}...")
        
        # Optimize partitioning based on data size
        if count < 1000:
            num_partitions = 1
        elif count < 10000:
            num_partitions = 4
        else:
            num_partitions = 8
        
        df = df.repartition(num_partitions)
        df.cache()
        
        # Create temporary table name
        temp_table = f"gold.temp_{table_name}"
        
        # Step 1: Write to temporary table (Spark writes everything as Text/Double)
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", temp_table) \
            .option("user", POSTGRES_PROPS["user"]) \
            .option("password", POSTGRES_PROPS["password"]) \
            .option("driver", POSTGRES_PROPS["driver"]) \
            .option("batchsize", "5000") \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()
        
        # Step 2: Construct SQL with EXPLICIT CASTING
        columns = df.columns
        select_parts = []
        
        for c in columns:
            # 1. Global UUIDs (e.g. unit_id, region_id)
            if c in GLOBAL_UUID_COLS:
                select_parts.append(f'"{c}"::uuid')
            
            # 2. Context-Aware UUIDs (The fix for your error)
            # target_id is UUID in engagement_analysis, but Text in threat_assessment
            elif c == "target_id" and table_name == "engagement_analysis":
                 select_parts.append(f'"{c}"::uuid')
            
            # 3. Enum Casting
            elif c in ENUM_CASTS:
                enum_type = ENUM_CASTS[c]
                select_parts.append(f'"{c}"::{enum_type}')
            
            # 4. Default
            else:
                select_parts.append(f'"{c}"')

        select_clause = ", ".join(select_parts)
        insert_cols = ", ".join([f'"{c}"' for c in columns])
        
        # Build conflict columns for ON CONFLICT clause
        conflict_cols = ", ".join([f'"{k}"' for k in primary_keys])
        
        # Build UPDATE SET clause (all columns except primary keys)
        update_cols = [col for col in columns if col not in primary_keys]
        update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
        
        upsert_sql = f"""
        INSERT INTO gold.{table_name} ({insert_cols})
        SELECT {select_clause} FROM {temp_table}
        ON CONFLICT ({conflict_cols}) 
        DO UPDATE SET {update_set};
        """
        
        drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
        
        # Step 3: Execute via psycopg2 (Robust Driver)
        # Parse JDBC URL to get connection parameters
        jdbc_url_clean = POSTGRES_URL.replace("jdbc:postgresql://", "").split("?")[0]
        host_port_db = jdbc_url_clean.split("/")
        host_port = host_port_db[0].split(":")
        host = host_port[0]
        port = int(host_port[1])
        database = host_port_db[1]

        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=POSTGRES_PROPS["user"],
                password=POSTGRES_PROPS["password"]
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Execute UPSERT
            cursor.execute(upsert_sql)
            
            # Drop temp table
            cursor.execute(drop_sql)
            
            cursor.close()
            conn.close()
            
        except Exception as db_error:
            logger.error(f"Database execution error: {db_error}")
            raise db_error
        
        duration = time.time() - start_time
        logger.info(f"SUCCESS [{table_name}] Upserted: {count} rows | Duration: {duration:.2f}s")
        
    except Exception as e:
        logger.error(f"FAILED [{table_name}] Error: {e}")
        # Continue loop even if one table fails
    finally:
        df.unpersist()

def get_latest_snapshot(df, group_cols, time_col="event_time"):
    """Helper to get the most recent record for each ID (deduplication)."""
    # Use created_at if event_time doesn't exist (fallback)
    if time_col not in df.columns and "created_at" in df.columns:
        time_col = "created_at"
        
    window = Window.partitionBy(group_cols).orderBy(F.col(time_col).desc())
    return df.withColumn("rn", F.row_number().over(window)).filter(F.col("rn") == 1).drop("rn")

def calculate_distance_km(lat1, lon1, lat2, lon2):
    """Haversine formula to calculate distance in km."""
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
    logger.info(">>> Processing: effective_unit_strength")
    try:
        units = read_silver(spark, "units")
        weapons = read_silver(spark, "weapons")
        regions = read_silver(spark, "regions")
        weather = read_silver(spark, "weather_events")
        unit_status = read_silver(spark, "unit_status_updates")

        latest_status = get_latest_snapshot(unit_status, "unit_id")
        latest_weather = get_latest_snapshot(weather, "region_id")

        df = latest_status.alias("s") \
            .join(units.alias("u"), F.col("s.unit_id") == F.col("u.id")) \
            .join(regions.alias("r"), F.col("s.region_id") == F.col("r.id")) \
            .join(weapons.alias("w"), F.col("s.unit_id") == F.col("w.unit_id"), "left") \
            .join(latest_weather.alias("we"), F.col("s.region_id") == F.col("we.region_id"), "left")

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

        gold_df = df.select(
            F.col("s.unit_id"),
            F.col("s.region_id"),
            F.col("w.id").alias("weapon_id"),
            F.col("terrain_factor"),
            F.col("effective_range_km"),
            F.col("adjusted_hit_probability"),
            F.col("attacking_strength"),
            F.col("s.event_time")
        ).filter(
            F.col("unit_id").isNotNull() & 
            F.col("region_id").isNotNull() & 
            F.col("weapon_id").isNotNull()
        )
        
        write_gold(gold_df, "effective_unit_strength", ["unit_id", "weapon_id"])
    except Exception as e:
        logger.error(f"Error in effective_unit_strength: {e}")

def process_threat_assessment(spark):
    logger.info(">>> Processing: threat_assessment")
    try:
        targets = read_silver(spark, "targets")
        detections = read_silver(spark, "detections")
        sensors = read_silver(spark, "sensors")
        unit_status = read_silver(spark, "unit_status_updates")
        weather = read_silver(spark, "weather_events")
        ew_events = read_silver(spark, "cyber_ew_events")

        latest_detections = get_latest_snapshot(detections, "target_id")
        latest_unit_pos = get_latest_snapshot(unit_status, "unit_id")
        latest_weather = get_latest_snapshot(weather, "region_id")
        latest_ew = get_latest_snapshot(ew_events, "target_sensor_id")

        df = latest_detections.alias("d") \
            .join(targets.alias("t"), F.col("d.target_id") == F.col("t.id")) \
            .join(sensors.alias("s"), F.col("d.sensor_id") == F.col("s.id")) \
            .join(latest_unit_pos.alias("u_pos"), F.col("s.unit_id") == F.col("u_pos.unit_id"), "left") \
            .join(latest_weather.alias("we"), F.col("d.region_id") == F.col("we.region_id"), "left") \
            .join(latest_ew.alias("ew"), F.col("d.sensor_id") == F.col("ew.target_sensor_id"), "left")

        intensity = F.coalesce(F.col("we.intensity"), F.lit(0)).cast(DoubleType())
        is_jammed = F.when(F.col("ew.effect") == "jammed", 1).otherwise(0)
        is_bad_weather = F.when(F.col("we.type").isin("fog", "storm"), 1).otherwise(0)
        
        df = df.withColumn("distance_km", 
            F.when(F.col("u_pos.lat").isNotNull(), 
                calculate_distance_km(F.col("u_pos.lat"), F.col("u_pos.lon"), F.col("d.lat"), F.col("d.lon")))
            .otherwise(F.lit(1000000000.0)) 
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
            .otherwise(F.lit(1000000000.0))
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
        ).filter(
            F.col("target_id").isNotNull() & 
            F.col("region_id").isNotNull()
        )

        write_gold(gold_df, "threat_assessment", ["target_id"])
    except Exception as e:
        logger.error(f"Error in threat_assessment: {e}")

def process_alerts_with_commands(spark):
    logger.info(">>> Processing: alerts_with_commands")
    try:
        alerts = read_silver(spark, "alerts")
        commands = read_silver(spark, "commands")
        detections = read_silver(spark, "detections")
        users = read_silver(spark, "users")

        high_alerts = alerts.filter(F.col("threat_level").isin("high", "critical"))

        df = high_alerts.alias("a") \
            .join(detections.alias("d"), F.col("a.detection_id") == F.col("d.id")) \
            .join(commands.alias("c"), F.col("a.id") == F.col("c.alert_id"), "left") \
            .join(users.alias("u"), F.col("c.user_id") == F.col("u.id"), "left")

        df = df.withColumn("action", 
            F.when(F.col("u.role") == "commander", F.col("c.action")).otherwise(None)
        ).withColumn("final_event_time", 
            F.greatest(F.col("a.created_at"), F.col("c.event_time"))
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
        ).filter(
            F.col("alert_id").isNotNull() & 
            F.col("detection_id").isNotNull() & 
            F.col("region_id").isNotNull() & 
            F.col("command_id").isNotNull() & 
            F.col("user_id").isNotNull()
        ).distinct()

        write_gold(gold_df, "alerts_with_commands", ["alert_id"])
    except Exception as e:
        logger.error(f"Error in alerts_with_commands: {e}")

def process_logistics_readiness(spark):
    logger.info(">>> Processing: logistics_readiness")
    try:
        supply = read_silver(spark, "supply_status")
        regions = read_silver(spark, "regions")
        weather = read_silver(spark, "weather_events")
        unit_status = read_silver(spark, "unit_status_updates")

        latest_supply = get_latest_snapshot(supply, "unit_id")
        latest_unit_loc = get_latest_snapshot(unit_status, "unit_id")
        latest_weather = get_latest_snapshot(weather, "region_id")

        df = latest_supply.alias("s") \
            .join(latest_unit_loc.alias("u"), F.col("s.unit_id") == F.col("u.unit_id")) \
            .join(regions.alias("r"), F.col("u.region_id") == F.col("r.id")) \
            .join(latest_weather.alias("we"), F.col("u.region_id") == F.col("we.region_id"), "left")

        intensity = F.coalesce(F.col("we.intensity"), F.lit(0)).cast(DoubleType())
        wind = F.coalesce(F.col("we.wind_speed_kmh"), F.lit(0)).cast(DoubleType())
        
        df = df.withColumn("terrain_penalty", 
                F.when(F.col("r.terrain_type") == "mountain", 0.7).otherwise(1.0)
            ).withColumn("projected_supply",
                F.col("s.supply_level").cast(DoubleType()) * (1 - 0.5 * intensity * F.when(F.col("we.type").isin("storm", "rain"), 1).otherwise(0) * F.col("terrain_penalty"))
            ).withColumn("resupply_feasibility",
                F.when((wind > 50) | (intensity > 10), 0).otherwise(1)
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
        ).filter(
            F.col("unit_id").isNotNull() & 
            F.col("region_id").isNotNull()
        )

        write_gold(gold_df, "logistics_readiness", ["unit_id"])
    except Exception as e:
        logger.error(f"Error in logistics_readiness: {e}")

def process_engagement_analysis(spark):
    logger.info(">>> Processing: engagement_analysis")
    try:
        engagements = read_silver(spark, "engagement_events")
        unit_status = read_silver(spark, "unit_status_updates")
        regions = read_silver(spark, "regions")
        weather = read_silver(spark, "weather_events")
        weapons = read_silver(spark, "weapons")
        ew_events = read_silver(spark, "cyber_ew_events")

        # 1. Resolve Attacker's Region at the time of engagement
        eng_context = engagements.alias("e") \
            .join(unit_status.alias("us"), 
                (F.col("e.attacker_id") == F.col("us.unit_id")) & 
                (F.col("us.event_time") <= F.col("e.event_time")))

        window_spec = Window.partitionBy("e.id").orderBy(F.col("us.event_time").desc())
        eng_with_region = eng_context.withColumn("rn", F.row_number().over(window_spec)) \
            .filter(F.col("rn") == 1) \
            .select(
                F.col("e.*"), 
                F.col("us.region_id").alias("attacker_region_id")
            )

        # 2. Join Context (Region, Weapon, Weather, EW)
        df = eng_with_region.alias("e") \
            .join(regions.alias("r"), F.col("e.attacker_region_id") == F.col("r.id"), "left") \
            .join(weapons.alias("w"), F.col("e.weapon_id") == F.col("w.id"), "left")

        # Join Weather (Most recent before engagement)
        df_weather = df.join(weather.alias("we"), 
            (F.col("e.attacker_region_id") == F.col("we.region_id")) & 
            (F.col("we.event_time") <= F.col("e.event_time")), "left")
        
        w_window = Window.partitionBy("e.id").orderBy(F.col("we.event_time").desc())
        df_step3 = df_weather.withColumn("rn", F.row_number().over(w_window)).filter(F.col("rn") == 1).drop("rn")

        # Join EW (Most recent before engagement for Attacker)
        df_ew = df_step3.join(ew_events.alias("ew"),
            (F.col("e.attacker_id") == F.col("ew.target_sensor_id")) & 
            (F.col("ew.event_time") <= F.col("e.event_time")), "left")
        
        ew_window = Window.partitionBy("e.id").orderBy(F.col("ew.event_time").desc())
        df_final_step = df_ew.withColumn("rn", F.row_number().over(ew_window)).filter(F.col("rn") == 1).drop("rn")

        # 3. Calculate Factors
        intensity = F.coalesce(F.col("we.intensity"), F.lit(0)).cast(DoubleType())
        is_fog = F.when(F.col("we.type") == "fog", 1).otherwise(0)
        is_jammed = F.when(F.col("ew.effect") == "jammed", 1).otherwise(0)

        df_final = df_final_step.withColumn("adjusted_hit_probability",
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
        ).filter(
            F.col("engagement_id").isNotNull() & 
            F.col("attacker_id").isNotNull() & 
            F.col("target_id").isNotNull() & 
            F.col("weapon_id").isNotNull() & 
            F.col("region_id").isNotNull()
        )

        write_gold(gold_df, "engagement_analysis", ["engagement_id"])
    except Exception as e:
        logger.error(f"Error in engagement_analysis: {e}")

def main():
    # 1. Initialize Spark Once
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session Initialized. Starting Continuous Processing Loop...")

    try:
        # 2. Continuous Loop
        while True:
            start_cycle = time.time()
            cycle_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            logger.info(f"--- STARTING BATCH CYCLE AT {cycle_time_str} ---")

            # Run all processing functions
            process_effective_unit_strength(spark)
            process_threat_assessment(spark)
            process_alerts_with_commands(spark)
            process_logistics_readiness(spark)
            process_engagement_analysis(spark)

            end_cycle = time.time()
            duration = end_cycle - start_cycle
            logger.info(f"--- CYCLE COMPLETED in {duration:.2f}s. Sleeping for {LOOP_INTERVAL_SECONDS}s... ---")
            
            # Force garbage collection after each cycle to free memory
            spark.catalog.clearCache()
            import gc
            gc.collect()
            
            # 3. Sleep
            time.sleep(LOOP_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Stopping job due to User Interrupt (Ctrl+C).")
    except Exception as e:
        logger.error(f"Critical Job Failure: {e}")
        raise e
    finally:
        spark.stop()
        logger.info("Spark Session Stopped.")

if __name__ == "__main__":
    main()