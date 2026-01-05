# End-to-End Data Pipeline

## 1. High-level Architecture

```
Data Generator (Python)
        ↓
PostgreSQL (Source)
        ↓  CDC (Debezium)
Kafka Topics
        ↓
Spark Ingest Job
        ↓
HDFS (Delta Lake) + Hive Metastore
        ↓
Spark Process Job
        ↓
PostgreSQL (Destination)

+ OpenMetadata for lineage, metadata & observability
```

This document serves **three purposes**:

1. Explain **what each service does** in the pipeline
2. Standardize **Docker service name / container name / volume name**
3. Provide **step-by-step health checks** to verify the pipeline works end‑to‑end

---

## 2. Service Inventory (Docker Compose)

| #  | Layer         | Service                      | Container                    | Volume(s)                 | Purpose                                            |
| -- | ------------- | ---------------------------- | ---------------------------- | ------------------------- | -------------------------------------------------- |
| 1  | Source        | `data-generator`             | `dp_data_generator`          | –                         | Generate mock data and insert into Postgres Source |
| 2  | Source        | `postgres-source`            | `dp_postgres_source`         | `postgres_source_data`    | OLTP source database                               |
| 3  | Destination   | `postgres-dest`              | `dp_postgres_dest`           | `postgres_dest_data`      | Final serving database                             |
| 4  | CDC           | `debezium`                   | `dp_debezium`                | –                         | Capture Postgres changes                           |
| 5  | Messaging     | `kafka`                      | `dp_kafka`                   | `kafka_data`              | Event streaming backbone                           |
| 6  | UI            | `kafka-ui`                   | `dp_kafka_ui`                | –                         | Inspect Kafka topics & messages                    |
| 7  | Compute       | `spark-master`               | `dp_spark_master`            | –                         | Spark cluster master                               |
| 8  | Compute       | `spark-worker`               | `dp_spark_worker`            | –                         | Spark worker node                                  |
| 9  | Compute       | `spark-job-ingest`           | `dp_spark_ingest`            | –                         | Kafka → Delta Lake ingestion                       |
| 10 | Compute       | `spark-job-process`          | `dp_spark_process`           | –                         | Delta Lake → Postgres Dest                         |
| 11 | Metadata      | `hms-db`                     | `dp_hms_db`                  | `hms_data`                | Hive Metastore database                            |
| 12 | Metadata      | `hms`                        | `hms`                        | –                         | Hive Metastore service                             |
| 13 | Storage       | `hdfs-namenode`              | `dp_hdfs_namenode`           | `namenode_data`           | HDFS namespace                                     |
| 14 | Storage       | `hdfs-datanode`              | `dp_hdfs_datanode`           | `datanode_data`           | HDFS data blocks                                   |
| 15 | Resource      | `hdfs-resourcemanager`       | `dp_hdfs_resourcemanager`    | –                         | YARN RM                                            |
| 16 | Resource      | `hdfs-nodemanager`           | `dp_hdfs_nodemanager`        | –                         | YARN NM                                            |
| 17 | Observability | `openmetadata-mysql`         | `openmetadata_mysql`         | `openmetadata-mysql-data` | OpenMetadata DB                                    |
| 18 | Observability | `openmetadata-elasticsearch` | `openmetadata_elasticsearch` | `es-data`                 | Metadata search                                    |
| 19 | Observability | `execute-migrate-all`        | `execute_migrate_all`        | –                         | OpenMetadata schema migration                      |
| 20 | Observability | `openmetadata-server`        | `openmetadata_server`        | –                         | OpenMetadata API/UI                                |
| 21 | Observability | `openmetadata-ingestion`     | `openmetadata_ingestion`     | Airflow volumes           | Metadata ingestion & lineage                       |

---

## 3. Health Check Playbook

Follow **top → bottom**, matching real data flow.

---

## 3.1 Source Data Layer

### 1️⃣ Data Generator

**Goal:** ensure data is continuously produced

```bash
# Check logs to ensure data is being generated and inserted into Postgres
docker logs -f dp_data_generator
```

✅ Expect logs like:

```
Inserted data into table regions
Inserted data into table customers
```

---

### 2️⃣ Postgres Source

**Goal:** confirm tables & row counts

```bash
# Enter container and check if tables exist and have records
docker exec -it dp_postgres_source psql -U admin -d jadc2_db -c "\dt"

docker exec -it dp_postgres_source psql -U admin -d jadc2_db \
  -c "SELECT count(*) FROM regions;"
# (Replace 'regions' with whatever table your generator creates)
```

✅ Tables exist and row count increases over time

---

## 3.2 Ingestion Layer (CDC)

### 3️⃣ Kafka

**Goal:** Debezium topics created

```bash
# Check if Debezium has created topics for the captured tables
docker exec -it dp_kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

✅ Expected:

```bash
# You should see topics like:
jadc2_db.public.regions
jadc2_db.public.customers
```

---

### 4️⃣ Debezium

**Goal:** Connector running without errors

```bash
# Check the status of the connectors to ensure they are RUNNING
docker exec -it dp_debezium curl -s http://localhost:8083/connectors

docker exec -it dp_debezium curl -s \
  http://localhost:8083/connectors/postgres-connector/status
```

✅ Expect:

```bash
# Expected output: "state": "RUNNING" for both connector and tasks
"state": "RUNNING"
```

---

### 5️⃣ Kafka UI

**Goal:** Inspect messages visually

```bash
# Check logs to ensure it connected to the Kafka cluster
docker logs dp_kafka_ui
```

✅ UI reachable, topics visible, messages flowing

---

## 3.3 Storage & Metadata Layer

### 6️⃣ HDFS Namenode

**Goal:** HDFS healthy & Delta data exists

```bash
# Check if HDFS is healthy and check for Delta Lake data written by Spark
docker exec -it dp_hdfs_namenode hdfs dfsadmin -report

docker exec -it dp_hdfs_namenode hdfs dfs -ls -R /data/delta
```

✅ Example path:

```bash
# Should show files in /data/delta/silver/regions/...
/data/delta/silver/regions/
```

---

### 7️⃣ Hive Metastore DB

**Goal:** Hive schema initialized

```bash
# Check if Hive has initialized its schema (DBS, TBLS tables should exist)
docker exec -it dp_hms_db psql -U hive -d metastore -c "\dt"

docker exec -it dp_hms_db psql -U hive -d metastore \
  -c "SELECT * FROM \"TBLS\";"
```

✅ Tables like `DBS`, `TBLS`, `SDS` exist

---

### 8️⃣ Hive Metastore Service

```bash
# Check logs to ensure it connected to Postgres and HDFS
docker logs hms
```

✅ Connected to Postgres & HDFS

---

## 3.4 Processing Layer (Spark)

### 9️⃣ Spark Master

```bash
# Check logs to see registered workers and submitted applications
docker logs dp_spark_master
```

✅ Worker registered & applications submitted

---

### 🔟 Spark Worker

```bash
# Check logs to see if it is executing tasks
docker logs dp_spark_worker
```

✅ Tasks executing

---

### 1️⃣1️⃣ Spark Ingest Job (Kafka → Delta)

```bash
# This container runs once (or loops). Check logs for "Success" or Stack Traces.
docker logs -f dp_spark_ingest
```

✅ Look for:

```
INFO DAGScheduler: Job X finished
```

---

### 1️⃣2️⃣ Spark Process Job (Delta → Postgres)

```bash
# Check logs. This job waits for HDFS data then writes to Postgres Dest.
docker logs -f dp_spark_process
```

✅ No stack traces, rows written to destination

---

## 3.5 Destination Layer

### 1️⃣3️⃣ Postgres Destination

```bash
# Check if the aggregated/processed tables have been created and populated
docker exec -it dp_postgres_dest psql -U admin -d jadc2_db -c "\\dt"

docker exec -it dp_postgres_dest psql -U admin -d jadc2_db \
  -c "SELECT * FROM processed_data LIMIT 10;"
# (Replace 'processed_data' with your actual destination table name)
```

✅ Aggregated / curated data available

---

## 3.6 Observability (OpenMetadata)

### 1️⃣4️⃣ OpenMetadata MySQL

```bash
# Check connection and schema existence
docker exec -it openmetadata_mysql mysql -uroot -ppassword -e "SHOW DATABASES;"
```

---

### 1️⃣5️⃣ OpenMetadata Elasticsearch

```bash
# Check cluster health
docker exec -it openmetadata_elasticsearch \
  curl -s http://localhost:9200/_cluster/health?pretty
```

✅ `status: green | yellow`

---

### 1️⃣6️⃣ OpenMetadata Migration

```bash
# Check if migration finished successfully (Exit Code 0)
docker logs execute_migrate_all
```

✅ Exit code `0`

---

### 1️⃣7️⃣ OpenMetadata Server

```bash
# Check if the API is up and healthy
docker exec -it openmetadata_server \
  curl -s http://localhost:8585/healthcheck
```

✅ Response:

```json
{"status":"UP"}
```

---

### 1️⃣8️⃣ OpenMetadata Ingestion (Airflow)

```bash
# Check if Airflow scheduler/webserver is running
docker exec -it openmetadata_ingestion airflow dags list

docker logs openmetadata_ingestion
```

✅ DAGs visible, lineage ingestion successful

---

## 4. Mental Model (Interview‑Ready)

* **Postgres** → system of record
* **Debezium** → change capture
* **Kafka** → event log
* **Spark Ingest** → bronze/silver Delta
* **Hive Metastore** → schema governance
* **Spark Process** → business logic
* **Postgres Dest** → serving layer
* **OpenMetadata** → lineage & trust

## 5. Docker Volumes
```bash
# 1. Tạo thư mục cha chứa toàn bộ volume
sudo mkdir -p /drive1/docker_volumes

# 2. Tạo các thư mục con tương ứng với danh sách volume
sudo mkdir -p /drive1/docker_volumes/postgres_source_data
sudo mkdir -p /drive1/docker_volumes/postgres_dest_data
sudo mkdir -p /drive1/docker_volumes/kafka_data
sudo mkdir -p /drive1/docker_volumes/namenode_data
sudo mkdir -p /drive1/docker_volumes/datanode_data
sudo mkdir -p /drive1/docker_volumes/hms_data
sudo mkdir -p /drive1/docker_volumes/ingestion-volume-dag-airflow
sudo mkdir -p /drive1/docker_volumes/ingestion-volume-dags
sudo mkdir -p /drive1/docker_volumes/ingestion-volume-tmp
sudo mkdir -p /drive1/docker_volumes/es-data
sudo mkdir -p /drive1/docker_volumes/openmetadata-mysql-data

# 3. Cấp quyền ghi tối đa (777) cho toàn bộ thư mục này 
# (Bắt buộc, nếu không Postgres/ES sẽ crash vì không có quyền ghi)
sudo chmod -R 777 /drive1/docker_volumes
```