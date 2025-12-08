# hive-metastore/Dockerfile
# Standalone Hive Metastore Service (HMS)
# Provides centralized metadata management for Spark, Presto, and other engines

# Use Apache Hive official image as base
FROM apache/hive:3.1.3

# Metadata
LABEL maintainer="JADC2 DevOps Team"
LABEL description="Standalone Hive Metastore Service with PostgreSQL backend"
LABEL version="3.1.3"

# Switch to root for installations
USER root

# ==========================================
# SYSTEM DEPENDENCIES
# ==========================================

# Install system utilities
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    netcat \
    postgresql-client \
    vim \
    procps \
    && rm -rf /var/lib/apt/lists/*

# ==========================================
# ENVIRONMENT VARIABLES
# ==========================================

ENV HIVE_HOME=/opt/hive
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

# PostgreSQL connection settings (can be overridden via docker-compose)
ENV POSTGRES_HOST=hms-db
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=metastore
ENV POSTGRES_USER=hive
ENV POSTGRES_PASSWORD=hive

# HDFS NameNode settings
ENV HDFS_NAMENODE_HOST=namenode
ENV HDFS_NAMENODE_PORT=9000

# Metastore settings
ENV METASTORE_PORT=9083
ENV METASTORE_TYPE=postgres

# ==========================================
# DOWNLOAD ADDITIONAL JARS
# ==========================================

# Create directory for additional JARs
RUN mkdir -p $HIVE_HOME/auxlib

# Download PostgreSQL JDBC driver
RUN wget -P $HIVE_HOME/lib \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Download Hadoop AWS libraries (for S3 support if needed)
RUN wget -P $HIVE_HOME/auxlib \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && wget -P $HIVE_HOME/auxlib \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Download Guava library (fix compatibility issues)
RUN wget -P $HIVE_HOME/lib \
    https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar

# ==========================================
# CONFIGURATION FILES
# ==========================================

# Create configuration directory
RUN mkdir -p $HIVE_CONF_DIR

# Copy custom hive-site.xml
COPY conf/hive-site.xml $HIVE_CONF_DIR/

# Create core-site.xml for HDFS connectivity
RUN cat > $HIVE_CONF_DIR/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <n>fs.defaultFS</n>
        <value>hdfs://namenode:9000</value>
    </property>
    <property>
        <n>fs.hdfs.impl</n>
        <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
    </property>
</configuration>
EOF

# Create hdfs-site.xml for HDFS client configuration
RUN cat > $HIVE_CONF_DIR/hdfs-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <n>dfs.replication</n>
        <value>1</value>
    </property>
    <property>
        <n>dfs.client.use.datanode.hostname</n>
        <value>true</value>
    </property>
</configuration>
EOF

# ==========================================
# ENTRYPOINT SCRIPT
# ==========================================

# Create entrypoint script
RUN cat > /opt/entrypoint.sh <<'ENTRYPOINT_EOF'
#!/bin/bash
set -e

echo "=========================================="
echo "Starting Hive Metastore Service"
echo "=========================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# ==========================================
# WAIT FOR DEPENDENCIES
# ==========================================

log_info "Waiting for PostgreSQL database..."
max_retries=30
retry=0
while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
    retry=$((retry + 1))
    if [ $retry -ge $max_retries ]; then
        log_error "PostgreSQL is not available after $max_retries attempts"
        exit 1
    fi
    log_warn "Attempt ${retry}/${max_retries}: PostgreSQL not ready. Retrying in 2s..."
    sleep 2
done
log_success "PostgreSQL is available!"

log_info "Waiting for HDFS NameNode..."
retry=0
while ! nc -z $HDFS_NAMENODE_HOST $HDFS_NAMENODE_PORT; do
    retry=$((retry + 1))
    if [ $retry -ge $max_retries ]; then
        log_error "HDFS NameNode is not available after $max_retries attempts"
        exit 1
    fi
    log_warn "Attempt ${retry}/${max_retries}: HDFS not ready. Retrying in 2s..."
    sleep 2
done
log_success "HDFS NameNode is available!"

# Additional wait for HDFS to exit safe mode
log_info "Waiting for HDFS to exit safe mode..."
sleep 10

# ==========================================
# INITIALIZE SCHEMA
# ==========================================

log_info "Checking metastore schema..."

# Check if schema is already initialized
if ! PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 1 FROM \"VERSION\" LIMIT 1;" > /dev/null 2>&1; then
    log_info "Schema not found. Initializing metastore schema..."
    
    # Initialize schema
    $HIVE_HOME/bin/schematool -dbType postgres -initSchema --verbose
    
    if [ $? -eq 0 ]; then
        log_success "Metastore schema initialized successfully!"
    else
        log_error "Failed to initialize metastore schema"
        exit 1
    fi
else
    log_success "Metastore schema already exists"
    
    # Optionally upgrade schema
    log_info "Checking if schema upgrade is needed..."
    $HIVE_HOME/bin/schematool -dbType postgres -info
fi

# ==========================================
# CREATE WAREHOUSE DIRECTORIES IN HDFS
# ==========================================

log_info "Setting up HDFS warehouse directories..."

# Create warehouse directories
hdfs dfs -mkdir -p /user/hive/warehouse 2>/dev/null || true
hdfs dfs -mkdir -p /user/hive/warehouse/external 2>/dev/null || true
hdfs dfs -mkdir -p /tmp/hive 2>/dev/null || true

# Set permissions
hdfs dfs -chmod -R 777 /user/hive/warehouse 2>/dev/null || true
hdfs dfs -chmod -R 777 /tmp/hive 2>/dev/null || true

log_success "HDFS directories created"

# ==========================================
# START METASTORE SERVICE
# ==========================================

log_info "=========================================="
log_info "Configuration Summary:"
log_info "  Database: ${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
log_info "  HDFS NameNode: ${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"
log_info "  Metastore Port: ${METASTORE_PORT}"
log_info "  Warehouse Dir: hdfs://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}/user/hive/warehouse"
log_info "=========================================="

log_success "Starting Hive Metastore Service on port ${METASTORE_PORT}..."

# Start metastore service in foreground
exec $HIVE_HOME/bin/hive --service metastore
ENTRYPOINT_EOF

# Make entrypoint executable
RUN chmod +x /opt/entrypoint.sh

# ==========================================
# HEALTH CHECK SCRIPT
# ==========================================

# Create health check script
RUN cat > /opt/healthcheck.sh <<'HEALTHCHECK_EOF'
#!/bin/bash
# Health check: verify Metastore is responding

# Check if metastore port is listening
if ! nc -z localhost 9083; then
    echo "Metastore port 9083 is not listening"
    exit 1
fi

# Try to connect via beeline (if available)
# This is optional and can be commented out if beeline is not available
# timeout 5 beeline -u "jdbc:hive2://localhost:9083" -e "SHOW DATABASES;" > /dev/null 2>&1
# if [ $? -ne 0 ]; then
#     echo "Cannot connect to Metastore via beeline"
#     exit 1
# fi

echo "Metastore is healthy"
exit 0
HEALTHCHECK_EOF

RUN chmod +x /opt/healthcheck.sh

# ==========================================
# DIRECTORY PERMISSIONS
# ==========================================

# Create necessary directories
RUN mkdir -p /tmp/hive \
    && mkdir -p /user/hive/warehouse \
    && chmod -R 777 /tmp/hive

# ==========================================
# EXPOSE PORTS
# ==========================================

# 9083: Metastore Thrift service port
EXPOSE 9083

# ==========================================
# WORKING DIRECTORY
# ==========================================

WORKDIR $HIVE_HOME

# ==========================================
# USER & ENTRYPOINT
# ==========================================

# Keep as root for now (needed for some operations)
# In production, consider running as non-root user
USER root

# Set entrypoint
ENTRYPOINT ["/opt/entrypoint.sh"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD /opt/healthcheck.sh || exit 1