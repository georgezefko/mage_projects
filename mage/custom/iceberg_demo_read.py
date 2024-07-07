from pandas import DataFrame
import io
import pandas as pd
import requests
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from mage.utils.spark_session_factory import get_spark_session

# Initialize Spark session and MinIO client
iceberg_spark_session = get_spark_session(
    "iceberg",
    app_name="MageSparkSession",
    warehouse_path="s3a://iceberg-demo-bucket/warehouse",
    s3_endpoint="http://minio:9000",
    s3_access_key="zefko",
    s3_secret_key="sparkTutorial"
)


@custom
def iceberg_table_read(*args, **kwargs):
    """
    Read data from a MinIO bucket using either Iceberg .
    """
    # Construct the full path to the table in the MinIO bucket
    table_name = "local.iceberg_demo.listings" 
    
    # Read the table into a Spark DataFrame
    df = iceberg_spark_session.spark.table(table_name)
    
    # TODO: Further cleaning and processing can be added here
    
    return df
    

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'