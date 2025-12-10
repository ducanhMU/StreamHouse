"""
Spark Streaming Job 1: Ingest from Kafka to Delta Lake (Bronze Layer)
Reads CDC events from Debezium Kafka topics (jadc2.raw.*) and writes to HDFS in Delta format.
Emits lineage metadata to OpenMetadata via Spark Agent.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from delta import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURATION
# ==========================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_PREFIX = "jadc2.raw"  # Matches pattern: jadc2.raw.tablename
HDFS_BASE_PATH = "hdfs://namenode:9000/data/delta"

# List of tables from source (matches Kafka topics)
TABLE_LIST = [
    "alerts",
    "commands",
    "cyber_ew_events",
    "detections",
    "engagement_events",
    "regions",
    "roe_updates",
    "sensors",
    "supply_status",
    "targets",
    "unit_status_updates",
    "units",
    "users",
    "weapons",
    "weather_events"
]

# Generate configs dynamically
TABLE_CONFIGS = {}
for table in TABLE_LIST:
    TABLE_CONFIGS[table] = {
        "topic": f"{KAFKA_TOPIC_PREFIX}.{table}",
        "path": f"{HDFS_BASE_PATH}/bronze/{table}_raw",
        "table_name": f"default.{table}_raw",
        "key_cols": ["id"]
    }

def create_spark_session():
    """Initialize Spark with Delta Lake, Hive, and OpenMetadata support"""
    return (SparkSession.builder
        .appName("JADC2_Kafka_To_Delta_Ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "300s")
        .enableHiveSupport()
        .getOrCreate())

def get_debezium_schema():
    """Standard Debezium Envelope Schema"""
    return StructType([
        StructField("before", StringType(), True),
        StructField("after", StringType(), True),
        StructField("source", StringType(), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True),
        StructField("transaction", StringType(), True)
    ])

def process_cdc_stream(spark, source_table, config):
    """Reads from Kafka -> Parses Debezium JSON -> Upserts to Delta Lake"""
    logger.info(f"--- Starting Stream: {config['topic']} -> {config['path']} ---")
    
    # 1. Read from Kafka
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config["topic"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load())
    
    # 2. Extract Value as String
    json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_value")

    # 3. Parse Debezium Envelope
    debezium_schema = get_debezium_schema()
    parsed_stream = (json_stream
        .select(from_json(col("json_value"), debezium_schema).alias("data"))
        .select("data.*"))
    
    # 4. Filter & Transform
    # Keep Create (c), Update (u), Snapshot (r). Parse 'after' as Map<String,String>
    transformed_stream = (parsed_stream
        .filter(col("op").isin(["c", "u", "r"]))
        .selectExpr("from_json(after, 'map<string,string>') as after_map")
        .select("after_map.*") # Flattens map into columns
        .withColumn("ingest_timestamp", current_timestamp()))
    
    # 5. Micro-batch Upsert Logic
    def upsert_to_delta(batch_df, batch_id):
        if batch_df.count() == 0: 
            return
        
        delta_path = config["path"]
        table_name = config["table_name"]
        key_cols = config["key_cols"]
        
        # Check if table exists
        if DeltaTable.isDeltaTable(spark, delta_path):
            delta_table = DeltaTable.forPath(spark, delta_path)
            # Construct merge condition
            merge_cond = " AND ".join([f"target.{c} = source.{c}" for c in key_cols])
            
            (delta_table.alias("target")
                .merge(batch_df.alias("source"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        else:
            # First run: Create table and register in Hive Metastore
            logger.info(f"Initializing Delta Table: {table_name}")
            batch_df.write \
                .format("delta") \
                .mode("append") \
                .option("path", delta_path) \
                .saveAsTable(table_name)
            
    # 6. Start Query
    checkpoint_dir = f"{HDFS_BASE_PATH}/checkpoints/{source_table}"
    
    return (transformed_stream.writeStream
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="15 seconds")
        .start())

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    active_streams = []
    
    # Launch a stream for each table
    for table_name, config in TABLE_CONFIGS.items():
        try:
            query = process_cdc_stream(spark, table_name, config)
            active_streams.append(query)
        except Exception as e:
            logger.error(f"Failed to start stream for {table_name}: {e}")
    
    logger.info(f"Started {len(active_streams)} streams. Awaiting termination...")
    
    for q in active_streams:
        q.awaitTermination()

if __name__ == "__main__":
    main()