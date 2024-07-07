from pandas import DataFrame
import io
import pandas as pd
import requests
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from mage.utils.spark_session_factory import get_spark_session

# Function to stop any existing Spark session
def stop_existing_spark_session():
    try:
        existing_spark = SparkSession.builder.getOrCreate()
        if existing_spark:
            existing_spark.stop()
    except Exception as e:
        print(f"No existing Spark session to stop: {e}")

stop_existing_spark_session()

delta_spark_session = get_spark_session(
    "delta",
    app_name="MageSparkSession",
    s3_endpoint="http://minio:9000",
    s3_access_key="zefko",
    s3_secret_key="sparkTutorial"
)
client = Minio(
    "minio:9000",
    access_key="zefko",
    secret_key="sparkTutorial",
    secure=False
)

minio_bucket = "delta-demo-bucket"
found = client.bucket_exists(minio_bucket)
if not found:
    client.make_bucket(minio_bucket)


@custom
def delta_table_write(*args, **kwargs):
    data_folder = "mage/data"  # Adjust this path according to your directory structure
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(data_folder, filename)
            # Read the CSV file into a Spark DataFrame
            df = delta_spark_session.spark.read.csv(file_path, header=True, inferSchema=True)
            
            # Generate table name and path
            table_name = f"delta_demo_{os.path.splitext(os.path.basename(file_path))[0]}"
            table_path = f"s3a://{minio_bucket}/delta/{table_name}"
            
            if table_name.split('_')[-1] == 'listings':
                print('process listings')
                split_cols = F.split(df['name'], '·')
                is_review_present = F.trim(split_cols.getItem(1)).startswith('★')
                # Extract, clean and assign new columns
                df = df.withColumn('description', F.trim(split_cols.getItem(0))) \
                    .withColumn('reviews', F.when(is_review_present, F.trim(F.regexp_replace(split_cols.getItem(1), '★', ''))).otherwise(None)) \
                    .withColumn('bedrooms', F.when(is_review_present, F.trim(split_cols.getItem(2))).otherwise(F.trim(split_cols.getItem(1)))) \
                    .withColumn('beds', F.when(is_review_present, F.trim(split_cols.getItem(3))).otherwise(F.trim(split_cols.getItem(2)))) \
                    .withColumn('baths', F.when(is_review_present, F.trim(split_cols.getItem(4))).otherwise(F.trim(split_cols.getItem(3))))
                df = df.drop('name', 'neighbourhood_group', 'license')
            
            # Write to Delta table
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(table_path)
            

    return "Delta tables created successfully"
          
  


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'