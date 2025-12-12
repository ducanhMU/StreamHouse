"""
Spark Job 1: Bronze to Silver Layer (Throttled Micro-batch)
------------------------------------------------------------
Mode: Structured Streaming
Frequency: Every 1 minute
Batch Size: Max 50 records per trigger (Very Low Load)
Logic: UPSERT (Merge) to keep Silver tables synchronized
------------------------------------------------------------
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, get_json_object, expr
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from delta import DeltaTable
import logging
import time

# ==========================================
# CONFIGURATION
# ==========================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("JADC2_Job1_Throttled")

KAFKA_BOOTSTRAP = "kafka:9092"
HDFS_BASE_PATH = "hdfs://namenode:9000/data/delta/silver"
TOPIC_PREFIX = "jadc2.raw"

# ==========================================
# SCHEMA DEFINITIONS
# ==========================================
def get_schema(table_name):
    DEC_19_4 = DecimalType(19, 4)
    DEC_19_8 = DecimalType(19, 8)
    
    schemas = {
        "regions": StructType([
            StructField("id", StringType(), True),
            StructField("region_name", StringType(), True),
            StructField("terrain_type", StringType(), True),
            StructField("infrastructure_level", DEC_19_4, True),
            StructField("area_sqkm", DEC_19_4, True),
            StructField("lat_center", DEC_19_8, True),
            StructField("lon_center", DEC_19_8, True),
            StructField("classification", StringType(), True)
        ]),
        "targets": StructType([
            StructField("id", StringType(), True),
            StructField("target_id", StringType(), True),
            StructField("target_type", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("estimated_intent", StringType(), True),
            StructField("threat_level", StringType(), True),
            StructField("iff_status", StringType(), True),
            StructField("classification", StringType(), True)
        ]),
        "users": StructType([
            StructField("id", StringType(), True),
            StructField("username", StringType(), True),
            StructField("role", StringType(), True),
            StructField("classification", StringType(), True)
        ]),
        "units": StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("unit_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("classification", StringType(), True)
        ]),
        "weapons": StructType([
            StructField("id", StringType(), True),
            StructField("unit_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("type", StringType(), True),
            StructField("effective_domain", StringType(), True),
            StructField("range_km", DEC_19_4, True),
            StructField("hit_probability", DEC_19_4, True),
            StructField("speed_kmh", DEC_19_4, True)
        ]),
        "sensors": StructType([
            StructField("id", StringType(), True),
            StructField("unit_id", StringType(), True),
            StructField("sensor_type", StringType(), True),
            StructField("range_km", DEC_19_4, True),
            StructField("status", StringType(), True)
        ]),
        "detections": StructType([
            StructField("id", StringType(), True),
            StructField("sensor_id", StringType(), True),
            StructField("target_id", StringType(), True),
            StructField("target_domain", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("lat", DEC_19_8, True),
            StructField("lon", DEC_19_8, True),
            StructField("altitude_depth", DEC_19_4, True),
            StructField("speed_kmh", DEC_19_4, True),
            StructField("heading_deg", DEC_19_4, True),
            StructField("confidence", DEC_19_4, True),
            StructField("iff_status", StringType(), True),
            StructField("threat_level", StringType(), True),
            StructField("event_time", TimestampType(), True)
        ]),
        "weather_events": StructType([
            StructField("id", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("intensity", DEC_19_4, True),
            StructField("wind_speed_kmh", DEC_19_4, True),
            StructField("precipitation_mm", DEC_19_4, True),
            StructField("event_time", TimestampType(), True)
        ]),
        "unit_status_updates": StructType([
            StructField("id", StringType(), True),
            StructField("unit_id", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("lat", DEC_19_8, True),
            StructField("lon", DEC_19_8, True),
            StructField("health_percent", DEC_19_4, True),
            StructField("max_range_km", DEC_19_4, True),
            StructField("status", StringType(), True),
            StructField("event_time", TimestampType(), True)
        ]),
        "supply_status": StructType([
            StructField("id", StringType(), True),
            StructField("unit_id", StringType(), True),
            StructField("region_id", StringType(), True),
            StructField("supply_level", DEC_19_4, True),
            StructField("event_time", TimestampType(), True)
        ]),
        "cyber_ew_events": StructType([
            StructField("id", StringType(), True),
            StructField("source_id", StringType(), True),
            StructField("target_sensor_id", StringType(), True),
            StructField("effect", StringType(), True),
            StructField("impact_domain", StringType(), True),
            StructField("level", StringType(), True),
            StructField("event_time", TimestampType(), True)
        ]),
        "engagement_events": StructType([
            StructField("id", StringType(), True),
            StructField("attacker_id", StringType(), True),
            StructField("target_id", StringType(), True),
            StructField("weapon_id", StringType(), True),
            StructField("hit", BooleanType(), True),
            StructField("result", StringType(), True),
            StructField("roe_compliant", BooleanType(), True),
            StructField("event_time", TimestampType(), True)
        ]),
        "roe_updates": StructType([
            StructField("id", StringType(), True),
            StructField("rules", StringType(), True),
            StructField("event_time", TimestampType(), True)
        ]),
        "alerts": StructType([
            StructField("id", StringType(), True),
            StructField("detection_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("msg", StringType(), True),
            StructField("threat_level", StringType(), True),
            StructField("created_at", TimestampType(), True)
        ]),
        "commands": StructType([
            StructField("id", StringType(), True),
            StructField("alert_id", StringType(), True),
            StructField("unit_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("event_time", TimestampType(), True)
        ])
    }
    return schemas.get(table_name)

# ==========================================
# STREAMING PROCESSING LOGIC
# ==========================================
def start_stream_for_table(spark, table_name, schema):
    topic_name = f"{TOPIC_PREFIX}.{table_name}"
    delta_path = f"{HDFS_BASE_PATH}/{table_name}"
    checkpoint_dir = f"{HDFS_BASE_PATH}/_checkpoints/{table_name}"
    
    logger.info(f"Initializing stream for: {table_name}")

    # 1. READ STREAM (With Constraints)
    # maxOffsetsPerTrigger=50 ensures we only read 50 messages at a time
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic_name)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50) # <--- LIMIT 50 MESSAGES
        .option("failOnDataLoss", "false")
        .load())

    # 2. TRANSFORM
    parsed_stream = (raw_stream
        .select(
            get_json_object(expr("CAST(value AS STRING)"), "$.after").alias("after_json"),
            get_json_object(expr("CAST(value AS STRING)"), "$.op").alias("op")
        )
        .filter(col("op").isin(["c", "u", "r"])) 
        .withColumn("data", from_json(col("after_json"), schema)) 
        .select("data.*") 
        .withColumn("update_timestamp", current_timestamp())
    )

    # 3. UPSERT FUNCTION (Batch Logic)
    def upsert_to_delta(batch_df, batch_id):
        start_time = time.time()
        
        # Optimization: Persist to prevent re-read from Kafka during merge
        batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            count = batch_df.count()
            if count == 0:
                logger.info(f"[{table_name}] Batch {batch_id}: No new data. Skipping.")
                return

            logger.info(f"--- STARTING BATCH CYCLE for {table_name} (Batch {batch_id}) ---")
            logger.info(f"[{table_name}] Processing {count} records...")
            
            # Use the active session from the DataFrame
            _spark = batch_df.sparkSession
            
            # Deduplicate the batch (last win)
            batch_df = batch_df.dropDuplicates(["id"])

            if DeltaTable.isDeltaTable(_spark, delta_path):
                target_table = DeltaTable.forPath(_spark, delta_path)
                (target_table.alias("target")
                    .merge(batch_df.alias("source"), "target.id = source.id")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute())
                action = "MERGED"
            else:
                batch_df.write \
                    .format("delta") \
                    .mode("append") \
                    .option("path", delta_path) \
                    .saveAsTable(f"default.{table_name}")
                action = "CREATED/APPENDED"
            
            duration = time.time() - start_time
            logger.info(f"SUCCESS [{table_name}] {action} {count} records in {duration:.2f}s")
            logger.info(f"--- BATCH DONE for {table_name} ---")

        except Exception as e:
            logger.error(f"FAILED [{table_name}] Batch {batch_id} Error: {e}")
        finally:
            batch_df.unpersist()

    # 4. START STREAM (With Trigger)
    # trigger(processingTime="1 minute") ensures we wait 1 min between runs
    return (parsed_stream.writeStream
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="1 minute") # <--- RUN EVERY 1 MINUTE
        .start())

# ==========================================
# MAIN
# ==========================================
def main():
    spark = (SparkSession.builder
        .appName("JADC2_Job1_Throttled_Stream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("hive.metastore.uris", "thrift://hms:9083")
        .enableHiveSupport()
        .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Job 1 Started: Streaming from Kafka -> Silver Delta Lake (Throttled)")

    tables = [
        "regions", "targets", "users", "units", "weapons", 
        "sensors", "detections", "weather_events", "unit_status_updates", 
        "supply_status", "cyber_ew_events", "engagement_events", 
        "roe_updates", "alerts", "commands"
    ]

    active_streams = []

    for table in tables:
        schema = get_schema(table)
        if schema:
            try:
                stream = start_stream_for_table(spark, table, schema)
                active_streams.append(stream)
            except Exception as e:
                logger.error(f"Failed to start stream for {table}: {e}")
        else:
            logger.error(f"No schema defined for {table}")

    logger.info(f"Running {len(active_streams)} streams. (1 min trigger, 50 msg limit per batch)")
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Job stopped by user.")

if __name__ == "__main__":
    main()