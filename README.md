# JADC2 Monitoring System

## Overview
The JADC2-inspired Multi-Domain Operations Monitoring System enables real-time command and control (C2), intelligence, surveillance, reconnaissance (ISR), and logistics across land, sea, air, space, and cyber domains. It uses a `medallion architecture` (Bronze, Silver, Gold) to process data from PostgreSQL to MinIO (Iceberg), ClickHouse, and end-user applications (MCP chatbot, Superset).

## Data Pipeline

- **PostgreSQL**: Data source with Bronze schema, populated by `data_generator`.
- **Debezium**: Streams changes (CDC) from PostgreSQL to Kafka.
- **Kafka**: Queues real-time data.
- **Flink**: Processes Kafka data into MinIO (Iceberg) Bronze tables.
- **MinIO (Iceberg)**: Stores `Bronze` (raw), `Silver` (cleaned), and `Gold` (analytics-ready) layers, transformed via DBT/Dremio/Airflow.
- **ClickHouse**: Imports Gold layer data for OLAP queries.
- **Chatbot/Superset**: Supports MCP chatbot queries and data visualization.

## Project Structure

- **postgresql**: Defines Bronze schema and CDC setup for PostgreSQL.
- **data_generator**: Generates synthetic data for Bronze tables.
- **debezium**: Configures CDC to Kafka.
- **flink**: Processes data into MinIO.
- **hms-standalone-s3**: Hive Metastore for Iceberg tables.
- **dbt**: Transforms data in MinIO (Bronze to Silver/Gold) using Dremio.
- **airflow**: Schedules DBT transformation jobs.
- **clickhouse**: OLAP datastore for Gold layer data.
- **chatbot_clickhouse**: Connects ClickHouse to MCP chatbot.
- **superset**: Visualizes data.
- **Medallion_DB_design.md**: Describes medallion architecture.
- **docker-compose.yml**: Orchestrates all services.

## Usage
Run the entire system using Docker Compose:
``` bash 
docker-compose up --build
```

## Validate data:

- **PostgreSQL**: `SELECT COUNT(*) FROM bronze.detections_raw;`
- **ClickHouse**: `SELECT * FROM gold_threats WHERE threat_level = 'critical';`

## Notes

- Use Docker secrets for sensitive credentials (e.g., `DB_PASSWORD`).
- See each folder's `README.md` for service-specific details.
- Refer to `./medallion_DB_design.md` for Bronze, Silver, Gold layer design.

```mermaid
graph LR
    S_DB[(Postgres Source)] -->|Debezium Kafka Connect| K(Kafka Topic)
    K -->|Spark Stream Agent| H(HDFS/Delta Lake)
    H -->|Spark Batch Agent| D_DB[(Postgres Dest)]

    style S_DB fill:#f9f,stroke:#333
    style K fill:#ff9,stroke:#333
    style H fill:#9ff,stroke:#333
    style D_DB fill:#9f9,stroke:#333
```