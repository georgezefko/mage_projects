if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from pandas import DataFrame
import io
import pandas as pd
import requests
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from mage.utils.spark_session_factory import get_spark_session
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *

MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
NESSIE_URI = 'http://nessie:19120/api/v1'
# Initialize Spark session and MinIO client
iceberg_spark_session = get_spark_session(
    "nessie",
    app_name="MageSparkSession",
    warehouse_path="s3a://iot-bronze",
    s3_endpoint="http://minio:9000",
    s3_access_key=MINIO_ACCESS_KEY,
    s3_secret_key=MINIO_SECRET_KEY,
    nessie_uri = NESSIE_URI,
    aws_region = 'us-east-1'
)




@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    spark = iceberg_spark_session.spark
    kwargs['context']['spark'] = spark

    events_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("severity", StringType(), True),

    # Failure-specific columns:
    StructField("error_code", StringType(), True),
    StructField("component", StringType(), True),
    StructField("root_cause", StringType(), True),

    # Maintenance-specific columns:
    StructField("technician", StringType(), True),
    StructField("duration_min", IntegerType(), True),
    StructField("parts_replaced", StringType(), True),

    # Inspection-specific columns:
    StructField("status", StringType(), True),
    StructField("next_inspection_days", IntegerType(), True),

    ])


    try:
        # Read data with explicit S3 configuration
        events_df = (spark.read
                 .schema(events_schema)
                 .parquet("s3a://iot-bronze/events/date=*/device_id=*"))

        telemetry_df = (spark.read
                 .option("basePath", "s3a://iot-bronze/telemetry")
                 .parquet("s3a://iot-bronze/telemetry/date=*/device_id=*"))

        
        return events_df, telemetry_df
        
    except Exception as e:
        print(f"Error reading data: {str(e)}")
        raise  # Re-raise the exception for debugging


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
