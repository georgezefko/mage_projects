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
    path_json = '/home/src/mage_data/mage/pipelines/lambda_silver/.variables/lambda_silver__get_data_minio/output_1/resource_usage.json'
    if os.path.exists(path_json):
        os.remove(path_json)
    bronze_telemetry = data[1] #based on the output from previous block
    print('bronze_telemtry', bronze_telemetry.show())
    # Transformations
    silver_telemetry = bronze_telemetry.filter(
        (col("energy_usage").between(0.1, 20.0)) &  # Wider range to catch anomalies
        (col("temperature").between(-10, 100)) &    # Account for extreme environments
        (col("vibration") >= 0)                     # No negative vibrations
    ).withColumn(
        "status",
        when(
            (col("vibration") > 5) | 
            (col("temperature") > 28) |
            (col("signal_strength") < 75),  # New field from generator
            lit("warning")
        ).otherwise(lit("normal"))
    ).withColumn(
        "is_anomaly",
        col("status") != "normal"
    ).withColumn(
        "rolling_5min_energy",
        avg(col("energy_usage")).over(
            Window.partitionBy("device_id")
                .orderBy(col("timestamp").cast("long"))
                .rangeBetween(-300, 0)  # 5-minute window
        )
    ).withColumn(
        "data_quality",
        when(col("signal_strength") < 70, "low")
        .otherwise("high")
    )
    return silver_telemetry


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
