import os
import logging
from pyspark.sql import SparkSession

# Must be set BEFORE SparkSession is created
os.environ['HADOOP_HOME'] = 'C:/hadoop'

def bronze_transform():
    spark = SparkSession.builder \
        .appName("olist_bronze") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    raw_data_path = "/mnt/d/Practise Projects/E-commerce/raw_data"
    bronze_data_path = "/mnt/d/Practise Projects/E-commerce/data_lake/bronze"

    # read each CSV file from raw_data, write to bronze as parquet
    for file in os.listdir(raw_data_path):
        if file.endswith(".csv"):
            print(f"Processing file: {file}")
            df = spark.read.csv(f"{raw_data_path}/{file}", header=True, inferSchema=True)
            print(f"  Rows: {df.count()}")
            df.write.mode("overwrite").parquet(f"{bronze_data_path}/{file.replace('.csv', '')}")
            print(f"  Written to bronze successfully")

    print("\n=== Bronze ingestion complete ===")
    spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bronze_transform()

    