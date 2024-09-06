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

MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
NESSIE_URI = 'http://nessie:19120/api/v1'


iceberg_spark_session = get_spark_session(
    "nessie",
    app_name="MageSparkSession",
    warehouse_path="s3a://iceberg-demo-bucket-nessie/warehouse",
    s3_endpoint="http://minio:9000",
    s3_access_key=MINIO_ACCESS_KEY,
    s3_secret_key=MINIO_SECRET_KEY,
    nessie_uri = NESSIE_URI,
    aws_region = 'us-east-1'
)
client = Minio(
    "minio:9000",
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

minio_bucket = "iceberg-demo-bucket-nessie"
found = client.bucket_exists(minio_bucket)
if not found:
    client.make_bucket(minio_bucket)

@custom
def iceberg_table_write(*args, **kwargs):
    data_folder = "mage/data"  # Adjust this path according to your directory structure
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(data_folder, filename)
            
            # Read the CSV file into a Spark DataFrame
            df = iceberg_spark_session.spark.read.csv(file_path, header=True, inferSchema=True)
            # Define the table name
            table_name = f"iceberg_demo_{os.path.splitext(os.path.basename(file_path))[0]}"
            
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
            
            # Create or replace the Iceberg table
            if iceberg_spark_session.spark._jsparkSession.catalog().tableExists(table_name):
                df.writeTo(f"nessie.{table_name}").overwrite()
            else:
                df.writeTo(f"nessie.{table_name}").create()

    return "Iceberg tables created successfully"
          
  


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'



