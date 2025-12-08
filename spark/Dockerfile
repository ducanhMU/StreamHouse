# spark/Dockerfile
# Custom Spark image with Delta Lake, Postgres, and OpenMetadata Agent

FROM apache/spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    netcat \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Create directories for JARs and configurations
RUN mkdir -p $SPARK_HOME/jars_extra \
    && mkdir -p $SPARK_HOME/conf_custom

# Download Delta Lake libraries (compatible with Spark 3.4.x)
RUN wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
    && wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# Download PostgreSQL JDBC Driver
RUN wget -P $SPARK_HOME/jars_extra \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Download Hadoop AWS libraries for S3 support (optional but useful)
RUN wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Download Kafka libraries for streaming
RUN wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar \
    && wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
    && wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && wget -P $SPARK_HOME/jars_extra \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar

# Download OpenMetadata Spark Agent for lineage tracking
RUN wget -P $SPARK_HOME/jars_extra \
    https://github.com/open-metadata/openmetadata-spark-agent/releases/download/1.3.1/openmetadata-spark-agent-1.3.1.jar

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    delta-spark==2.4.0 \
    psycopg2-binary \
    requests \
    py4j

# Copy configuration files
COPY conf/spark-defaults.conf $SPARK_HOME/conf_custom/
COPY conf/hive-site.xml $SPARK_HOME/conf_custom/

# Copy helper scripts
COPY scripts/get_om_token.sh /opt/scripts/
RUN chmod +x /opt/scripts/get_om_token.sh

# Create symbolic links for configurations
RUN ln -sf $SPARK_HOME/conf_custom/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf \
    && ln -sf $SPARK_HOME/conf_custom/hive-site.xml $SPARK_HOME/conf/hive-site.xml

# Expose Spark ports
# 4040: Spark UI
# 7077: Spark Master port
# 8080: Spark Master Web UI
# 8081: Spark Worker Web UI
EXPOSE 4040 7077 8080 8081

# Set working directory
WORKDIR /opt/spark/work-dir

# Default user
USER spark

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:4040 || exit 1

# Default command (can be overridden in docker-compose)
CMD ["/bin/bash"]