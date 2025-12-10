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
# BỎ DÒNG NÀY: ENV SPARK_CLASSPATH... (Không cần thiết nữa)

# 2. Download Libraries
# CHIẾN THUẬT: Tải thẳng vào $SPARK_HOME/jars để Spark tự nhận diện

# Delta Lake 2.4.0
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# PostgreSQL Driver
RUN wget -P $SPARK_HOME/jars https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Kafka Integration (Đã đủ bộ)
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.4.1/spark-token-provider-kafka-0-10_2.12-3.4.1.jar

# OpenMetadata Agent
RUN wget -P $SPARK_HOME/jars https://repo1.maven.org/maven2/org/open-metadata/openmetadata-spark-agent/1.0-beta/openmetadata-spark-agent-1.0-beta.jar

# 3. Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    delta-spark==2.4.0 \
    psycopg2-binary \
    requests \
    py4j

# 4. Copy Configurations
COPY conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml

# 5. Copy Helper Scripts
COPY scripts/get_om_token.sh /opt/scripts/
RUN chmod +x /opt/scripts/get_om_token.sh

# 6. Fix Permissions
# Quan trọng: Cấp quyền cho user spark sở hữu file jar mới tải
RUN chown -R spark:spark $SPARK_HOME/jars /opt/scripts

# 7. Expose Ports
EXPOSE 4040 7077 8080 8081

# 8. Set Working Directory & User
WORKDIR /opt/spark/work-dir
USER spark

CMD ["tail", "-f", "/dev/null"]