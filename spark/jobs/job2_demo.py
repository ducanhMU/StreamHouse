"""
Spark Job 2 DEMO: Silver to Gold (Lineage-Enabled Simulation)
-----------------------------------------------------------------------
LOGIC:
1. INPUT: Reads 50 rows from ACTUAL Silver tables (HDFS) to trigger Lineage.
2. COMPUTE: Takes the primary table, fills missing 'join' columns with 
            random data to guarantee valid Gold output.
3. OUTPUT: TRUNCATES and INSERTs rows into PostgreSQL (Overwrite Mode).
   FIX: Added deduplication to prevent Unique Constraint violations.
-----------------------------------------------------------------------
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType
import logging
import time
import psycopg2 

# ==========================================
# CONFIGURATION
# ==========================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("JADC2_Job2_Demo")

HDFS_SILVER_PATH = "hdfs://namenode:9000/data/delta/silver"
POSTGRES_URL = "jdbc:postgresql://postgres-dest:5432/jadc2_db?stringtype=unspecified"

POSTGRES_PROPS = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# DEMO CONFIG
LOOP_INTERVAL_SECONDS = 60
DEMO_ROW_LIMIT = 50 

# ==========================================
# TYPE SAFETY CONSTANTS
# ==========================================
GLOBAL_UUID_COLS = {
    "unit_id", "region_id", "weapon_id", "alert_id", 
    "detection_id", "command_id", "user_id", 
    "attacker_id", "engagement_id", "sensor_id"
}

ENUM_CASTS = {
    "iff_status": "gold.iff_status",
    "threat_level": "gold.threat_level",
    "result": "gold.engagement_result",
    "unit_status": "gold.unit_status",
    "domain": "gold.domain_type"
}

# Enum Options for Randomization
ENUM_OPTS = {
    "iff": ["friend", "foe", "neutral", "unknown"],
    "threat": ["low", "medium", "high", "critical"],
    "result": ["destroyed", "damaged", "escaped", "missed"]
}

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def create_spark_session():
    return SparkSession.builder \
        .appName("JADC2_Job2_Lineage_Demo") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .enableHiveSupport() \
        .getOrCreate()

def read_silver_limit(spark, table_name):
    """
    Reads Silver table from HDFS (Triggering Input Lineage) and limits to 50 rows.
    """
    path = f"{HDFS_SILVER_PATH}/{table_name}"
    try:
        df = spark.read.format("delta").load(path)
        # Order by time to get latest data
        if "event_time" in df.columns:
            df = df.orderBy(F.col("event_time").desc())
        elif "created_at" in df.columns:
            df = df.orderBy(F.col("created_at").desc())
            
        # Limit to 50 rows
        return df.limit(DEMO_ROW_LIMIT)
    except Exception:
        logger.warning(f"Silver table '{table_name}' not found. Returning empty DF.")
        return spark.createDataFrame([], schema="id string")

def write_gold_overwrite(df, table_name):
    """
    Writes to Postgres using TRUNCATE + INSERT strategy.
    Handles UUID and Enum casting automatically.
    """
    start_time = time.time()
    try:
        count = df.count()
        if count == 0:
            logger.info(f"[{table_name}] No data to write.")
            return

        logger.info(f"Overwriting Gold Table: gold.{table_name} with {count} rows...")

        # 1. Write to Temp Table
        temp_table = f"gold.temp_{table_name}"
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", temp_table) \
            .option("user", POSTGRES_PROPS["user"]) \
            .option("password", POSTGRES_PROPS["password"]) \
            .option("driver", POSTGRES_PROPS["driver"]) \
            .option("batchsize", "1000") \
            .mode("overwrite") \
            .save()
        
        # 2. Build SQL with Type Casting
        columns = df.columns
        select_parts = []
        for c in columns:
            if c in GLOBAL_UUID_COLS:
                select_parts.append(f'"{c}"::uuid')
            elif c == "target_id" and table_name == "engagement_analysis":
                 select_parts.append(f'"{c}"::uuid')
            elif c in ENUM_CASTS:
                enum_type = ENUM_CASTS[c]
                select_parts.append(f'"{c}"::{enum_type}')
            else:
                select_parts.append(f'"{c}"')

        select_clause = ", ".join(select_parts)
        insert_cols = ", ".join([f'"{c}"' for c in columns])
        
        # 3. Transaction: Truncate -> Insert
        overwrite_sql = f"""
        BEGIN;
        TRUNCATE TABLE gold.{table_name};
        INSERT INTO gold.{table_name} ({insert_cols})
        SELECT {select_clause} FROM {temp_table};
        COMMIT;
        """
        
        drop_sql = f"DROP TABLE IF EXISTS {temp_table}"
        
        # 4. Execute
        jdbc_url_clean = POSTGRES_URL.replace("jdbc:postgresql://", "").split("?")[0]
        host_port_db = jdbc_url_clean.split("/")
        host_port = host_port_db[0].split(":")
        
        conn = psycopg2.connect(
            host=host_port[0],
            port=int(host_port[1]),
            database=host_port_db[1],
            user=POSTGRES_PROPS["user"],
            password=POSTGRES_PROPS["password"]
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(overwrite_sql)
        cursor.execute(drop_sql)
        cursor.close()
        conn.close()
        
        logger.info(f"SUCCESS [{table_name}] Overwrite complete in {time.time() - start_time:.2f}s")
        
    except Exception as e:
        logger.error(f"FAILED [{table_name}]: {e}")

# Generators for Synthetic Data
def rand_uuid(): return F.expr("uuid()")
def rand_val(min_v, max_v): return (F.rand() * (max_v - min_v) + min_v).cast(DoubleType())

def rand_enum(values): 
    # Use IntegerType for index
    arr = F.array([F.lit(x) for x in values])
    index_expr = (F.floor(F.rand() * len(values)) + 1).cast(IntegerType())
    return F.element_at(arr, index_expr)

# ==========================================
# BUSINESS LOGIC (MOCK COMPUTE)
# ==========================================

def process_effective_unit_strength(spark):
    logger.info(">>> Processing: effective_unit_strength")
    
    # 1. READ ALL SOURCES (For Lineage)
    units = read_silver_limit(spark, "units")
    weapons = read_silver_limit(spark, "weapons")
    regions = read_silver_limit(spark, "regions")
    weather = read_silver_limit(spark, "weather_events")
    unit_status = read_silver_limit(spark, "unit_status_updates") 

    if unit_status.isEmpty(): return

    # 2. MOCK COMPUTE
    df = unit_status.select(
        F.col("id").alias("unit_id"),  # Keep Real ID
        F.col("region_id"), 
        rand_uuid().alias("weapon_id"),
        rand_val(0.5, 1.0).alias("terrain_factor"),
        rand_val(10.0, 100.0).alias("effective_range_km"),
        rand_val(0.0, 1.0).alias("adjusted_hit_probability"),
        rand_val(0.0, 100.0).alias("attacking_strength"),
        F.current_timestamp().alias("event_time")
    )

    # DEDUPLICATE: One strength report per unit
    df = df.dropDuplicates(["unit_id"])
    
    write_gold_overwrite(df, "effective_unit_strength")

def process_threat_assessment(spark):
    logger.info(">>> Processing: threat_assessment")
    
    # 1. READ SOURCES
    targets = read_silver_limit(spark, "targets")
    detections = read_silver_limit(spark, "detections") 
    sensors = read_silver_limit(spark, "sensors")
    units = read_silver_limit(spark, "units")
    unit_status = read_silver_limit(spark, "unit_status_updates")
    weather = read_silver_limit(spark, "weather_events")
    ew = read_silver_limit(spark, "cyber_ew_events")

    if detections.isEmpty(): return

    # 2. MOCK COMPUTE
    df = detections.select(
        F.col("target_id"), # Real ID
        F.col("region_id"), 
        rand_enum(ENUM_OPTS["iff"]).alias("iff_status"),
        rand_val(0.5, 1.0).alias("adjusted_confidence"),
        rand_enum(ENUM_OPTS["threat"]).alias("predicted_threat"),
        rand_val(0.0, 5000.0).alias("distance_km"),
        rand_val(0.0, 600.0).alias("response_time_sec"),
        F.current_timestamp().alias("event_time")
    )

    # DEDUPLICATE: One threat assessment per target
    df = df.dropDuplicates(["target_id"])
    
    write_gold_overwrite(df, "threat_assessment")

def process_alerts_with_commands(spark):
    logger.info(">>> Processing: alerts_with_commands")
    
    # 1. READ SOURCES
    alerts = read_silver_limit(spark, "alerts") 
    commands = read_silver_limit(spark, "commands")
    detections = read_silver_limit(spark, "detections")
    users = read_silver_limit(spark, "users")

    if alerts.isEmpty(): return

    # 2. MOCK COMPUTE
    df = alerts.select(
        F.col("id").alias("alert_id"), # Real ID
        F.coalesce(F.col("detection_id"), rand_uuid()).alias("detection_id"),
        rand_uuid().alias("region_id"),
        F.coalesce(F.col("threat_level"), F.lit("high")).alias("threat_level"),
        rand_uuid().alias("command_id"),
        F.lit("ENGAGE_TARGET").alias("action"),
        rand_uuid().alias("user_id"),
        F.current_timestamp().alias("event_time")
    )
    
    # Alerts are usually unique events, but dedupe just in case
    df = df.dropDuplicates(["alert_id"])

    write_gold_overwrite(df, "alerts_with_commands")

def process_logistics_readiness(spark):
    logger.info(">>> Processing: logistics_readiness")
    
    # 1. READ SOURCES
    supply = read_silver_limit(spark, "supply_status") 
    regions = read_silver_limit(spark, "regions")
    weather = read_silver_limit(spark, "weather_events")
    unit_status = read_silver_limit(spark, "unit_status_updates")

    if supply.isEmpty(): return

    # 2. MOCK COMPUTE
    df = supply.select(
        F.col("unit_id"), # Real ID
        rand_uuid().alias("region_id"),
        F.coalesce(F.col("supply_level"), F.lit(50.0)).alias("supply_level"),
        (F.col("supply_level") * 0.8).alias("projected_supply"),
        F.lit(1).alias("resupply_feasibility"),
        F.current_timestamp().alias("event_time")
    )
    
    # DEDUPLICATE: One supply status per unit
    df = df.dropDuplicates(["unit_id"])
    
    write_gold_overwrite(df, "logistics_readiness")

def process_engagement_analysis(spark):
    logger.info(">>> Processing: engagement_analysis")
    
    # 1. READ SOURCES
    engagements = read_silver_limit(spark, "engagement_events") 
    unit_status = read_silver_limit(spark, "unit_status_updates")
    regions = read_silver_limit(spark, "regions")
    weather = read_silver_limit(spark, "weather_events")
    ew = read_silver_limit(spark, "cyber_ew_events")
    weapons = read_silver_limit(spark, "weapons")

    if engagements.isEmpty(): return

    # 2. MOCK COMPUTE
    df = engagements.select(
        F.col("id").alias("engagement_id"), # Real ID
        F.col("attacker_id"),
        F.col("target_id"),
        F.col("weapon_id"),
        rand_uuid().alias("region_id"),
        F.coalesce(F.col("result"), F.lit("missed")).alias("result"),
        rand_val(0.0, 1.0).alias("adjusted_hit_probability"),
        rand_val(0.8, 1.2).alias("impact_factor"),
        F.current_timestamp().alias("event_time")
    )
    
    # DEDUPLICATE: One record per engagement ID
    df = df.dropDuplicates(["engagement_id"])

    write_gold_overwrite(df, "engagement_analysis")

# ==========================================
# MAIN LOOP
# ==========================================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session Initialized (DEMO MODE: Read 50 rows -> Mock Compute -> Postgres).")

    try:
        while True:
            start_cycle = time.time()
            logger.info(f"--- STARTING DEMO CYCLE ---")

            process_effective_unit_strength(spark)
            process_threat_assessment(spark)
            process_alerts_with_commands(spark)
            process_logistics_readiness(spark)
            process_engagement_analysis(spark)

            duration = time.time() - start_cycle
            sleep_time = max(0, LOOP_INTERVAL_SECONDS - duration)
            logger.info(f"--- CYCLE DONE in {duration:.2f}s. Sleeping {sleep_time:.0f}s ---")
            
            spark.catalog.clearCache()
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Stopping job.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()