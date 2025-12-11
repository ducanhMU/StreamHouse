# file: scripts/check_hdfs_path.py
import sys
from pyspark.sql import SparkSession

def check_path(path):
    spark = SparkSession.builder \
        .appName("Check HDFS Path") \
        .master("local[*]") \
        .getOrCreate()
    
    # Tắt log nhiễu
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Lấy Hadoop FileSystem từ Spark Context
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))
        
        if exists:
            print(f"FOUND: Path {path} exists.")
            sys.exit(0) # Trả về 0 báo thành công cho Bash
        else:
            print(f"NOT FOUND: Path {path} does not exist.")
            sys.exit(1) # Trả về 1 báo lỗi cho Bash
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_hdfs_path.py <hdfs_path>")
        sys.exit(1)
    
    check_path(sys.argv[1])
