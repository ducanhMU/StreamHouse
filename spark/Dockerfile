# spark/Dockerfile
FROM apache/spark:3.4.1-scala2.12-java11-python3-ubuntu

USER root

# 1. Install system utilities
RUN apt-get update && apt-get install -y \
    curl wget netcat postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# 2. Download Core Integration Libraries (Delta, Postgres, Kafka)
# Delta Lake 2.4.0
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# PostgreSQL Driver
RUN wget -P $SPARK_HOME/jars https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Kafka Integration
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar

# 3. [NEW] Pre-download Hive & Derby Libraries
# This prevents the runtime download loop and "FileNotFound" crashes.
# Based on Spark 3.4.1 compatibility (Hive 2.3.9, Derby 10.14)
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hive/hive-exec/2.3.9/hive-exec-2.3.9.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/2.3.9/hive-metastore-2.3.9.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hive/hive-common/2.3.9/hive-common-2.3.9.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/hive/hive-serde/2.3.9/hive-serde-2.3.9.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/derby/derby/10.14.2.0/derby-10.14.2.0.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/com/google/guava/guava/14.0.1/guava-14.0.1.jar

# 4. OpenMetadata Agent
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/open-metadata/openmetadata-spark-agent/1.0-beta/openmetadata-spark-agent-1.0-beta.jar

# 5. Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    delta-spark==2.4.0 \
    psycopg2-binary \
    requests \
    py4j

# 6. Copy Configurations
COPY conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml

# 7. Copy Helper Scripts
COPY scripts/get_om_token.sh /opt/scripts/
RUN chmod +x /opt/scripts/get_om_token.sh

# 8. [CRITICAL] Fix Permissions & Create Home Directory
# Creates the /home/spark directory structure and gives 'spark' user ownership.
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark $SPARK_HOME/jars /opt/scripts

# 9. Expose Ports
EXPOSE 4040 7077 8080 8081

# 10. Set Working Directory & User
WORKDIR /opt/spark/work-dir
# Ensure Java knows where the user home is
ENV HOME=/home/spark 
USER spark

CMD ["tail", "-f", "/dev/null"]