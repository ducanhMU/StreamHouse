# ============================================================================
# Makefile - JADC2 Multi-Domain Operations Monitoring System
# ============================================================================
# Automation for building, deploying, and managing the complete data pipeline
# Architecture: CDC -> Kafka -> Spark -> Delta Lake -> Postgres -> OpenMetadata
# ============================================================================

.PHONY: help build up down restart logs clean purge status health \
        check-deps init-env validate-config test-connections \
        spark-submit-ingest spark-submit-process spark-logs \
        kafka-topics kafka-create-topics kafka-consumer \
        hdfs-info hdfs-browse hdfs-format hdfs-safemode-off \
        postgres-source-cli postgres-dest-cli postgres-hms-cli \
        om-status om-token om-ingest-all om-ui \
        debezium-register debezium-status debezium-delete \
        data-generator-start data-generator-stop data-generator-logs \
        backup-postgres backup-hdfs restore-postgres restore-hdfs \
        monitor perf-test security-scan docs

# ============================================================================
# CONFIGURATION
# ============================================================================

# Project name
PROJECT_NAME := jadc2-pipeline

# Docker Compose file
COMPOSE_FILE := docker-compose.yml

# Container names (adjust if needed)
CONTAINER_SPARK_MASTER := dp_spark_master
CONTAINER_SPARK_INGEST := dp_spark_ingest
CONTAINER_SPARK_PROCESS := dp_spark_process
CONTAINER_NAMENODE := dp_namenode
CONTAINER_DATANODE := dp_datanode
CONTAINER_KAFKA := dp_kafka
CONTAINER_DEBEZIUM := dp_debezium
CONTAINER_POSTGRES_SOURCE := dp_postgres_source
CONTAINER_POSTGRES_DEST := dp_postgres_dest
CONTAINER_HMS := dp_hms
CONTAINER_HMS_DB := dp_hms_db
CONTAINER_OM_SERVER := dp_om_server
CONTAINER_DATA_GEN := dp_data_generator

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

# ============================================================================
# DEFAULT TARGET
# ============================================================================

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "$(BLUE)============================================================================$(NC)"
	@echo "$(GREEN)JADC2 Multi-Domain Operations Monitoring System$(NC)"
	@echo "$(BLUE)============================================================================$(NC)"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-25s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(BLUE)============================================================================$(NC)"
	@echo "$(YELLOW)Quick Start:$(NC)"
	@echo "  1. make check-deps        # Verify prerequisites"
	@echo "  2. make init-env          # Create .env file"
	@echo "  3. make build             # Build all images"
	@echo "  4. make up                # Start all services"
	@echo "  5. make health            # Check service health"
	@echo "  6. make om-ingest-all     # Ingest metadata to OpenMetadata"
	@echo "$(BLUE)============================================================================$(NC)"

# ============================================================================
# PREREQUISITES & INITIALIZATION
# ============================================================================

check-deps: ## Check if required tools are installed
	@echo "$(BLUE)Checking prerequisites...$(NC)"
	@command -v docker >/dev/null 2>&1 || { echo "$(RED)Error: docker is not installed$(NC)"; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || { echo "$(RED)Error: docker-compose is not installed$(NC)"; exit 1; }
	@docker --version
	@docker-compose --version
	@echo "$(GREEN)✓ All prerequisites met$(NC)"

init-env: ## Create .env file from template
	@echo "$(BLUE)Initializing environment configuration...$(NC)"
	@if [ -f .env ]; then \
		echo "$(YELLOW)Warning: .env already exists. Backing up to .env.bak$(NC)"; \
		cp .env .env.bak; \
	fi
	@cat > .env <<-'EOF'
	# ============================================================================
	# JADC2 Pipeline Environment Configuration
	# ============================================================================
	
	# Project
	COMPOSE_PROJECT_NAME=jadc2-pipeline
	
	# PostgreSQL Source (Bronze Layer)
	POSTGRES_SOURCE_HOST=postgres-source
	POSTGRES_SOURCE_PORT=5432
	POSTGRES_SOURCE_DB=jadc2_db
	POSTGRES_SOURCE_USER=admin
	POSTGRES_SOURCE_PASSWORD=password
	
	# PostgreSQL Destination (Gold Layer)
	POSTGRES_DEST_HOST=postgres-dest
	POSTGRES_DEST_PORT=5432
	POSTGRES_DEST_DB=jadc2_db
	POSTGRES_DEST_USER=admin
	POSTGRES_DEST_PASSWORD=password
	
	# PostgreSQL Hive Metastore
	HMS_DB_HOST=hms-db
	HMS_DB_PORT=5432
	HMS_DB_NAME=metastore
	HMS_DB_USER=hive
	HMS_DB_PASSWORD=hive
	
	# HDFS
	HDFS_NAMENODE_HOST=namenode
	HDFS_NAMENODE_PORT=9000
	HDFS_REPLICATION=1
	
	# Kafka
	KAFKA_HOST=kafka
	KAFKA_PORT=9092
	KAFKA_BROKER_ID=1
	
	# Debezium
	DEBEZIUM_HOST=debezium
	DEBEZIUM_PORT=8083
	
	# Hive Metastore
	HMS_HOST=hive-metastore
	HMS_PORT=9083
	
	# OpenMetadata
	OM_SERVER_HOST=om-server
	OM_SERVER_PORT=8585
	OM_ADMIN_USER=admin
	OM_ADMIN_PASSWORD=admin
	OM_JWT_TOKEN=
	
	# Spark
	SPARK_MASTER_HOST=spark-master
	SPARK_MASTER_PORT=7077
	SPARK_MASTER_WEBUI_PORT=8080
	
	# Data Generator
	DATA_GEN_INTERVAL=5
	
	# Versions
	SPARK_VERSION=3.4.1
	KAFKA_VERSION=3.4.0
	DELTA_VERSION=2.4.0
	POSTGRES_VERSION=15-alpine
	HADOOP_VERSION=3.3.4
	EOF
	@echo "$(GREEN)✓ .env file created$(NC)"
	@echo "$(YELLOW)Please review and adjust values in .env if needed$(NC)"

validate-config: ## Validate docker-compose configuration
	@echo "$(BLUE)Validating docker-compose configuration...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) config > /dev/null
	@echo "$(GREEN)✓ Configuration is valid$(NC)"

# ============================================================================
# BUILD & DEPLOYMENT
# ============================================================================

build: ## Build all Docker images
	@echo "$(BLUE)Building all Docker images...$(NC)"
	docker-compose -f $(COMPOSE_FILE) build --no-cache
	@echo "$(GREEN)✓ Build complete$(NC)"

build-spark: ## Build only Spark images
	@echo "$(BLUE)Building Spark images...$(NC)"
	docker-compose -f $(COMPOSE_FILE) build --no-cache spark-master spark-worker spark-job-ingest spark-job-process
	@echo "$(GREEN)✓ Spark build complete$(NC)"

build-hms: ## Build only Hive Metastore image
	@echo "$(BLUE)Building Hive Metastore image...$(NC)"
	docker-compose -f $(COMPOSE_FILE) build --no-cache hive-metastore
	@echo "$(GREEN)✓ HMS build complete$(NC)"

up: ## Start all services
	@echo "$(BLUE)Starting all services...$(NC)"
	docker-compose -f $(COMPOSE_FILE) up -d
	@echo "$(GREEN)✓ All services started$(NC)"
	@echo "$(YELLOW)Waiting for services to be healthy (this may take 5-10 minutes)...$(NC)"
	@sleep 10
	@make health

up-infra: ## Start only infrastructure (no Spark jobs)
	@echo "$(BLUE)Starting infrastructure services...$(NC)"
	docker-compose -f $(COMPOSE_FILE) up -d \
		postgres-source postgres-dest hms-db \
		namenode datanode \
		kafka debezium \
		hive-metastore \
		om-mysql om-elasticsearch om-server
	@echo "$(GREEN)✓ Infrastructure started$(NC)"

down: ## Stop all services
	@echo "$(BLUE)Stopping all services...$(NC)"
	docker-compose -f $(COMPOSE_FILE) down
	@echo "$(GREEN)✓ All services stopped$(NC)"

restart: ## Restart all services
	@echo "$(BLUE)Restarting all services...$(NC)"
	@make down
	@make up
	@echo "$(GREEN)✓ Restart complete$(NC)"

restart-spark: ## Restart only Spark jobs
	@echo "$(BLUE)Restarting Spark jobs...$(NC)"
	docker-compose -f $(COMPOSE_FILE) restart spark-job-ingest spark-job-process
	@echo "$(GREEN)✓ Spark jobs restarted$(NC)"

# ============================================================================
# MONITORING & LOGS
# ============================================================================

status: ## Show status of all containers
	@echo "$(BLUE)Container Status:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) ps

health: ## Check health of all services
	@echo "$(BLUE)Checking service health...$(NC)"
	@echo ""
	@echo "$(YELLOW)PostgreSQL Source:$(NC)"
	@docker exec $(CONTAINER_POSTGRES_SOURCE) pg_isready -U admin -d jadc2_db || echo "$(RED)✗ Not healthy$(NC)"
	@echo ""
	@echo "$(YELLOW)PostgreSQL Dest:$(NC)"
	@docker exec $(CONTAINER_POSTGRES_DEST) pg_isready -U admin -d jadc2_db || echo "$(RED)✗ Not healthy$(NC)"
	@echo ""
	@echo "$(YELLOW)HDFS NameNode:$(NC)"
	@curl -sf http://localhost:9870/jmx || echo "$(RED)✗ Not healthy$(NC)"
	@echo ""
	@echo "$(YELLOW)Kafka:$(NC)"
	@docker exec $(CONTAINER_KAFKA) kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "$(GREEN)✓ Healthy$(NC)" || echo "$(RED)✗ Not healthy$(NC)"
	@echo ""
	@echo "$(YELLOW)OpenMetadata:$(NC)"
	@curl -sf http://localhost:8585/api/v1/health-check > /dev/null && echo "$(GREEN)✓ Healthy$(NC)" || echo "$(RED)✗ Not healthy$(NC)"
	@echo ""

logs: ## Show logs from all services
	docker-compose -f $(COMPOSE_FILE) logs -f --tail=100

logs-spark-ingest: ## Show Spark ingest job logs
	docker logs -f $(CONTAINER_SPARK_INGEST)

logs-spark-process: ## Show Spark process job logs
	docker logs -f $(CONTAINER_SPARK_PROCESS)

logs-kafka: ## Show Kafka logs
	docker logs -f $(CONTAINER_KAFKA)

logs-debezium: ## Show Debezium logs
	docker logs -f $(CONTAINER_DEBEZIUM)

logs-om: ## Show OpenMetadata logs
	docker logs -f $(CONTAINER_OM_SERVER)

logs-data-gen: ## Show data generator logs
	docker logs -f $(CONTAINER_DATA_GEN)

# ============================================================================
# SPARK OPERATIONS
# ============================================================================

spark-submit-ingest: ## Submit Spark ingest job manually
	@echo "$(BLUE)Submitting Spark ingest job...$(NC)"
	docker exec $(CONTAINER_SPARK_MASTER) \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/spark/jobs/job1_ingest.py
	@echo "$(GREEN)✓ Job submitted$(NC)"

spark-submit-process: ## Submit Spark process job manually
	@echo "$(BLUE)Submitting Spark process job...$(NC)"
	docker exec $(CONTAINER_SPARK_MASTER) \
		/opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--deploy-mode client \
		/opt/spark/jobs/job2_process.py
	@echo "$(GREEN)✓ Job submitted$(NC)"

spark-ui: ## Open Spark Master UI in browser
	@echo "$(BLUE)Opening Spark Master UI...$(NC)"
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8080 || \
		command -v open >/dev/null 2>&1 && open http://localhost:8080 || \
		echo "$(YELLOW)Please open http://localhost:8080 in your browser$(NC)"

# ============================================================================
# KAFKA OPERATIONS
# ============================================================================

kafka-topics: ## List all Kafka topics
	@echo "$(BLUE)Kafka Topics:$(NC)"
	@docker exec $(CONTAINER_KAFKA) kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--list

kafka-create-topics: ## Create necessary Kafka topics
	@echo "$(BLUE)Creating Kafka topics...$(NC)"
	@docker exec $(CONTAINER_KAFKA) kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic server.public.regions_raw \
		--partitions 3 --replication-factor 1
	@echo "$(GREEN)✓ Topics created$(NC)"

kafka-consumer: ## Start Kafka console consumer for a topic
	@read -p "Enter topic name: " topic; \
	docker exec -it $(CONTAINER_KAFKA) kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $$topic \
		--from-beginning

kafka-ui: ## Open Kafka UI in browser
	@echo "$(BLUE)Opening Kafka UI...$(NC)"
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8085 || \
		command -v open >/dev/null 2>&1 && open http://localhost:8085 || \
		echo "$(YELLOW)Please open http://localhost:8085 in your browser$(NC)"

# ============================================================================
# HDFS OPERATIONS
# ============================================================================

hdfs-info: ## Show HDFS information
	@echo "$(BLUE)HDFS Information:$(NC)"
	@docker exec $(CONTAINER_NAMENODE) hdfs dfsadmin -report

hdfs-browse: ## Browse HDFS filesystem
	@echo "$(BLUE)HDFS Directory Structure:$(NC)"
	@docker exec $(CONTAINER_NAMENODE) hdfs dfs -ls -R /

hdfs-safemode-off: ## Force HDFS out of safe mode
	@echo "$(BLUE)Taking HDFS out of safe mode...$(NC)"
	@docker exec $(CONTAINER_NAMENODE) hdfs dfsadmin -safemode leave
	@echo "$(GREEN)✓ Safe mode disabled$(NC)"

hdfs-format: ## Format HDFS (DANGER: Deletes all data!)
	@echo "$(RED)WARNING: This will delete ALL data in HDFS!$(NC)"
	@read -p "Are you sure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		docker exec $(CONTAINER_NAMENODE) hdfs namenode -format -force; \
		echo "$(GREEN)✓ HDFS formatted$(NC)"; \
	else \
		echo "$(YELLOW)Aborted$(NC)"; \
	fi

hdfs-ui: ## Open HDFS NameNode UI in browser
	@echo "$(BLUE)Opening HDFS UI...$(NC)"
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:9870 || \
		command -v open >/dev/null 2>&1 && open http://localhost:9870 || \
		echo "$(YELLOW)Please open http://localhost:9870 in your browser$(NC)"

# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

postgres-source-cli: ## Connect to PostgreSQL source CLI
	@echo "$(BLUE)Connecting to PostgreSQL Source...$(NC)"
	docker exec -it $(CONTAINER_POSTGRES_SOURCE) \
		psql -U admin -d jadc2_db

postgres-dest-cli: ## Connect to PostgreSQL destination CLI
	@echo "$(BLUE)Connecting to PostgreSQL Destination...$(NC)"
	docker exec -it $(CONTAINER_POSTGRES_DEST) \
		psql -U admin -d jadc2_db

postgres-hms-cli: ## Connect to PostgreSQL HMS CLI
	@echo "$(BLUE)Connecting to Hive Metastore DB...$(NC)"
	docker exec -it $(CONTAINER_HMS_DB) \
		psql -U hive -d metastore

# ============================================================================
# OPENMETADATA OPERATIONS
# ============================================================================

om-status: ## Check OpenMetadata status
	@echo "$(BLUE)OpenMetadata Status:$(NC)"
	@curl -s http://localhost:8585/api/v1/health-check | jq '.' || echo "$(RED)✗ Not responding$(NC)"

om-token: ## Fetch OpenMetadata JWT token
	@echo "$(BLUE)Fetching OpenMetadata token...$(NC)"
	docker exec $(CONTAINER_SPARK_INGEST) /opt/scripts/get_om_token.sh
	@echo "$(GREEN)✓ Token updated$(NC)"

om-ingest-all: ## Run all OpenMetadata ingestion workflows
	@echo "$(BLUE)Running OpenMetadata ingestion...$(NC)"
	@echo "$(YELLOW)Please run ingestion manually through the UI:$(NC)"
	@echo "  1. Open http://localhost:8585"
	@echo "  2. Login with admin/admin"
	@echo "  3. Go to Services and run ingestion for each service"
	@make om-ui

om-ui: ## Open OpenMetadata UI in browser
	@echo "$(BLUE)Opening OpenMetadata UI...$(NC)"
	@command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8585 || \
		command -v open >/dev/null 2>&1 && open http://localhost:8585 || \
		echo "$(YELLOW)Please open http://localhost:8585 in your browser$(NC)"
	@echo "$(YELLOW)Login: admin / admin$(NC)"

# ============================================================================
# DEBEZIUM OPERATIONS
# ============================================================================

debezium-register: ## Register Debezium PostgreSQL connector
	@echo "$(BLUE)Registering Debezium connector...$(NC)"
	@curl -i -X POST -H "Content-Type: application/json" \
		--data @debezium/register-postgres.json \
		http://localhost:8083/connectors
	@echo ""
	@echo "$(GREEN)✓ Connector registered$(NC)"

debezium-status: ## Check Debezium connector status
	@echo "$(BLUE)Debezium Connector Status:$(NC)"
	@curl -s http://localhost:8083/connectors | jq '.'
	@echo ""
	@curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq '.'

debezium-delete: ## Delete Debezium connector
	@echo "$(BLUE)Deleting Debezium connector...$(NC)"
	@curl -i -X DELETE http://localhost:8083/connectors/postgres-source-connector
	@echo ""
	@echo "$(GREEN)✓ Connector deleted$(NC)"

# ============================================================================
# DATA GENERATOR OPERATIONS
# ============================================================================

data-generator-start: ## Start data generator
	@echo "$(BLUE)Starting data generator...$(NC)"
	docker-compose -f $(COMPOSE_FILE) up -d data-generator
	@echo "$(GREEN)✓ Data generator started$(NC)"

data-generator-stop: ## Stop data generator
	@echo "$(BLUE)Stopping data generator...$(NC)"
	docker-compose -f $(COMPOSE_FILE) stop data-generator
	@echo "$(GREEN)✓ Data generator stopped$(NC)"

data-generator-logs: ## Show data generator logs
	docker logs -f $(CONTAINER_DATA_GEN)

# ============================================================================
# CLEANUP
# ============================================================================

clean: ## Remove stopped containers and dangling images
	@echo "$(BLUE)Cleaning up...$(NC)"
	docker-compose -f $(COMPOSE_FILE) down
	docker system prune -f
	@echo "$(GREEN)✓ Cleanup complete$(NC)"

purge: ## Remove all containers, volumes, and images (DANGER!)
	@echo "$(RED)WARNING: This will delete ALL data and images!$(NC)"
	@read -p "Are you sure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		docker-compose -f $(COMPOSE_FILE) down -v --rmi all; \
		echo "$(GREEN)✓ Full purge complete$(NC)"; \
	else \
		echo "$(YELLOW)Aborted$(NC)"; \
	fi

# ============================================================================
# BACKUP & RESTORE
# ============================================================================

backup-postgres: ## Backup PostgreSQL databases
	@echo "$(BLUE)Backing up PostgreSQL databases...$(NC)"
	@mkdir -p backups
	docker exec $(CONTAINER_POSTGRES_SOURCE) pg_dump -U admin jadc2_db > backups/postgres_source_$(shell date +%Y%m%d_%H%M%S).sql
	docker exec $(CONTAINER_POSTGRES_DEST) pg_dump -U admin jadc2_db > backups/postgres_dest_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "$(GREEN)✓ Backup complete$(NC)"

backup-hdfs: ## Backup HDFS data
	@echo "$(BLUE)Backing up HDFS data...$(NC)"
	@mkdir -p backups
	docker exec $(CONTAINER_NAMENODE) hdfs dfs -get /data /tmp/hdfs_backup
	docker cp $(CONTAINER_NAMENODE):/tmp/hdfs_backup backups/hdfs_$(shell date +%Y%m%d_%H%M%S)
	@echo "$(GREEN)✓ HDFS backup complete$(NC)"

# ============================================================================
# TESTING
# ============================================================================

test-connections: ## Test all service connections
	@echo "$(BLUE)Testing service connections...$(NC)"
	@echo ""
	@echo "$(YELLOW)Testing PostgreSQL Source...$(NC)"
	@docker exec $(CONTAINER_POSTGRES_SOURCE) psql -U admin -d jadc2_db -c "SELECT 1;" > /dev/null && echo "$(GREEN)✓ Connected$(NC)" || echo "$(RED)✗ Failed$(NC)"
	@echo ""
	@echo "$(YELLOW)Testing PostgreSQL Dest...$(NC)"
	@docker exec $(CONTAINER_POSTGRES_DEST) psql -U admin -d jadc2_db -c "SELECT 1;" > /dev/null && echo "$(GREEN)✓ Connected$(NC)" || echo "$(RED)✗ Failed$(NC)"
	@echo ""
	@echo "$(YELLOW)Testing Kafka...$(NC)"
	@docker exec $(CONTAINER_KAFKA) kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "$(GREEN)✓ Connected$(NC)" || echo "$(RED)✗ Failed$(NC)"
	@echo ""
	@echo "$(YELLOW)Testing HDFS...$(NC)"
	@docker exec $(CONTAINER_NAMENODE) hdfs dfs -ls / > /dev/null 2>&1 && echo "$(GREEN)✓ Connected$(NC)" || echo "$(RED)✗ Failed$(NC)"

# ============================================================================
# DOCUMENTATION
# ============================================================================

docs: ## Open documentation
	@echo "$(BLUE)Documentation:$(NC)"
	@echo "  - Architecture: See README.md"
	@echo "  - API Docs: http://localhost:8585/swagger.html"
	@echo "  - Spark UI: http://localhost:8080"
	@echo "  - HDFS UI: http://localhost:9870"
	@echo "  - Kafka UI: http://localhost:8085"
	@echo "  - OpenMetadata: http://localhost:8585"