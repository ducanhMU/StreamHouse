# hive-metastore/Dockerfile
FROM apache/hive:3.1.3

LABEL maintainer="JADC2 DevOps Team"

USER root

# 1. Cài đặt các công cụ cần thiết
RUN apt-get update && apt-get install -y \
    curl wget netcat postgresql-client vim procps \
    && rm -rf /var/lib/apt/lists/*

# 2. Thiết lập biến môi trường chuẩn
ENV HIVE_HOME=/opt/hive
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH

# 3. Tải JDBC Driver Postgres và Guava (Fix lỗi version conflict kinh điển)
RUN rm -f $HIVE_HOME/lib/guava-*.jar && \
    wget -P $HIVE_HOME/lib https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar && \
    wget -P $HIVE_HOME/lib https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# 4. Tạo script entrypoint.sh (Logic khởi động)
# Script này: Chờ DB -> Chờ HDFS -> Init Schema (nếu chưa có) -> Start Metastore
RUN cat > /opt/entrypoint.sh <<'EOF'
#!/bin/bash
set -e

echo "--- Waiting for Postgres at $POSTGRES_HOST:$POSTGRES_PORT ---"
while ! nc -z $POSTGRES_HOST $POSTGRES_PORT; do
  sleep 2
  echo "Waiting for Postgres..."
done

echo "--- Waiting for NameNode at $HDFS_NAMENODE_HOST:$HDFS_NAMENODE_PORT ---"
while ! nc -z $HDFS_NAMENODE_HOST $HDFS_NAMENODE_PORT; do
  sleep 2
  echo "Waiting for NameNode..."
done
# Chờ thêm chút để HDFS thoát Safe Mode hoàn toàn
sleep 10

echo "--- Checking/Initializing Schema ---"
# Kiểm tra xem schema đã có chưa bằng cách query bảng VERSION
export PGPASSWORD=$POSTGRES_PASSWORD
if psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 1 FROM \"VERSION\" LIMIT 1;" >/dev/null 2>&1; then
    echo "Schema already exists. Skipping init."
else
    echo "Initializing Schema for Postgres..."
    $HIVE_HOME/bin/schematool -dbType postgres -initSchema -verbose
fi

echo "--- Creating Warehouse Directories on HDFS ---"
# Lệnh này có thể fail nếu thư mục đã tồn tại, nên ta dùng || true để không crash container
hdfs dfs -mkdir -p /user/hive/warehouse || true
hdfs dfs -chmod -R 777 /user/hive/warehouse || true
hdfs dfs -mkdir -p /tmp/hive || true
hdfs dfs -chmod -R 777 /tmp/hive || true

echo "--- Starting Hive Metastore Service ---"
exec $HIVE_HOME/bin/hive --service metastore
EOF

# 5. Tạo script healthcheck.sh
RUN cat > /opt/healthcheck.sh <<'EOF'
#!/bin/bash
nc -z localhost 9083 || exit 1
EOF

# 6. Cấp quyền thực thi
RUN chmod +x /opt/entrypoint.sh /opt/healthcheck.sh

# 7. Expose port Thrift
EXPOSE 9083

WORKDIR $HIVE_HOME
ENTRYPOINT ["/opt/entrypoint.sh"]
