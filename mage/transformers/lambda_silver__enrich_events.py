if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from mage.utils.spark_session_factory import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    # Specify your transformation logic here
    path_json = '/home/src/mage_data/mage/pipelines/lambda_silver/.variables/lambda_silver__get_data_minio/output_1/resource_usage.json'
    if os.path.exists(path_json):
        os.remove(path_json)
    bronze_events = data[0] #based on the output from previous block
    bronze_events.printSchema()
    filter_ = bronze_events.filter(col("event_type") == "failure")
    filter_.show()

    silver_events = bronze_events.withColumn(
        "event_metadata",
        struct(
            col("error_code"),
            col("component"),
            col("root_cause"),
            col("technician"),
            col("parts_replaced")
        )
    ).withColumn(
        "duration_minutes",
        when(
            col("event_type") == "maintenance",
            col("duration_min")
        ).otherwise(None)
    ).withColumn(
        "is_critical",
        col("severity") == "high"
    ).withColumn(
        "maintenance_type",
        when(
            col("event_type") == "maintenance",
            coalesce(col("type"), lit("routine"))  # Default to routine if null
        )
    ).drop("duration_min", "type")  # Cleanup old fields

    # Handle inspection events separately
    inspections = silver_events.filter(col("event_type") == "inspection") \
        .select(
            "device_id",
            "event_timestamp",
            col("status").alias("inspection_result"),
            col("next_inspection_days")
        )

    return silver_events, inspections


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
