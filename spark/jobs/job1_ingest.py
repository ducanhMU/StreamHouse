"""
Spark Streaming Job 1: Ingest from Kafka to Delta Lake (Bronze -> Silver Layer)
Reads CDC events from Debezium Kafka topics and writes to HDFS in Delta format.
Emits lineage metadata to OpenMetadata via Spark Agent.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, lit, current_timestamp, 
    to_timestamp, expr, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, TimestampType, IntegerType
)
from delta import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_PREFIX = "server.public"

# HDFS Delta Lake paths
HDFS_BASE_PATH = "hdfs://namenode:9000/data/delta"

# Table configurations mapping Debezium topics to Delta tables
TABLE_CONFIGS = {
    "regions_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_regions_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/regions_raw",
        "key_cols": ["id"]
    },
    "targets_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_targets_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/targets_raw",
        "key_cols": ["id"]
    },
    "users_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_users_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/users_raw",
        "key_cols": ["id"]
    },
    "units_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_units_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/units_raw",
        "key_cols": ["id"]
    },
    "weapons_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_weapons_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/weapons_raw",
        "key_cols": ["id"]
    },
    "sensors_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_sensors_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/sensors_raw",
        "key_cols": ["id"]
    },
    "detections_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_detections_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/detections_raw",
        "key_cols": ["id"]
    },
    "weather_events_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_weather_events_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/weather_events_raw",
        "key_cols": ["id"]
    },
    "unit_status_updates_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_unit_status_updates_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/unit_status_updates_raw",
        "key_cols": ["id"]
    },
    "supply_status_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_supply_status_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/supply_status_raw",
        "key_cols": ["id"]
    },
    "cyber_ew_events_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_cyber_ew_events_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/cyber_ew_events_raw",
        "key_cols": ["id"]
    },
    "engagement_events_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_engagement_events_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/engagement_events_raw",
        "key_cols": ["id"]
    },
    "roe_updates_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_roe_updates_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/roe_updates_raw",
        "key_cols": ["id"]
    },
    "alerts_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_alerts_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/alerts_raw",
        "key_cols": ["id"]
    },
    "commands_raw": {
        "topic": f"{KAFKA_TOPIC_PREFIX}.bronze_commands_raw",
        "path": f"{HDFS_BASE_PATH}/bronze/commands_raw",
        "key_cols": ["id"]
    }
}


def create_spark_session():
    """Initialize Spark session with Delta Lake and OpenMetadata support."""
    return (SparkSession.builder
        .appName("JADC2_Kafka_To_Delta_Ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", f"{HDFS_BASE_PATH}/checkpoints")
        .config("spark.extraListeners", "org.openmetadata.spark.agent.OpenMetadataSparkListener")
        .config("spark.openmetadata.transport.type", "http")
        .config("spark.openmetadata.transport.hostPort", "http://om-server:8585")
        .config("spark.openmetadata.transport.pipelineServiceName", "spark_pipeline")
        .config("spark.openmetadata.transport.pipelineServiceType", "Spark")
        .config("spark.openmetadata.transport.pipelineName", "kafka_to_delta_ingestion")
        .config("spark.openmetadata.transport.timeout", "30")
        .getOrCreate())


def get_debezium_schema():
    """Define schema for Debezium CDC JSON payload."""
    return StructType([
        StructField("before", StringType(), True),
        StructField("after", StringType(), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", StringType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
        ]), True),
        StructField("op", StringType(), True),  # c=create, u=update, d=delete
        StructField("ts_ms", StringType(), True),
    ])


def process_cdc_stream(spark, table_name, config):
    """
    Read Kafka CDC stream and write to Delta Lake with UPSERT logic.
    
    Args:
        spark: SparkSession
        table_name: Name of the table (e.g., 'regions_raw')
        config: Table configuration dict with topic, path, key_cols
    """
    logger.info(f"Starting ingestion for table: {table_name}")
    
    topic = config["topic"]
    delta_path = config["path"]
    key_cols = config["key_cols"]
    
    # Read from Kafka
    raw_stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load())
    
    # Parse Debezium JSON
    debezium_schema = get_debezium_schema()
    parsed_stream = (raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), debezium_schema).alias("data"))
        .select("data.*"))
    
    # Extract 'after' payload (for INSERT/UPDATE) or handle DELETE
    # For simplicity, we'll focus on INSERT/UPDATE (op='c' or 'u')
    # Parse the 'after' JSON string into columns
    transformed_stream = (parsed_stream
        .filter(col("op").isin(["c", "u"]))  # Only create/update
        .selectExpr("from_json(after, 'map<string,string>') as after_map")
        .select("after_map.*")
        .withColumn("ingest_timestamp", current_timestamp()))
    
    # Write to Delta with merge (upsert)
    def upsert_to_delta(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        
        # Check if Delta table exists
        try:
            delta_table = DeltaTable.forPath(spark, delta_path)
            
            # Build merge condition
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in key_cols])
            
            # Perform merge (upsert)
            (delta_table.alias("target")
                .merge(batch_df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            
            logger.info(f"Batch {batch_id} merged into {table_name}")
        
        except Exception as e:
            # If table doesn't exist, create it
            logger.info(f"Creating new Delta table for {table_name}")
            batch_df.write.format("delta").mode("append").save(delta_path)
    
    # Start streaming query
    query = (transformed_stream.writeStream
        .foreachBatch(upsert_to_delta)
        .option("checkpointLocation", f"{HDFS_BASE_PATH}/checkpoints/{table_name}")
        .trigger(processingTime="30 seconds")
        .start())
    
    return query


def main():
    """Main execution function."""
    logger.info("Initializing Spark session for Kafka -> Delta ingestion...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Start streaming queries for all tables
    queries = []
    for table_name, config in TABLE_CONFIGS.items():
        try:
            query = process_cdc_stream(spark, table_name, config)
            queries.append(query)
            logger.info(f"Started streaming query for {table_name}")
        except Exception as e:
            logger.error(f"Failed to start query for {table_name}: {str(e)}")
    
    # Wait for all queries to finish (runs indefinitely)
    logger.info(f"All {len(queries)} streaming queries started. Waiting for termination...")
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()