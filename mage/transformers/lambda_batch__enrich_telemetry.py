if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import polars as pl
from polars import col 
import os
from datetime import datetime, timedelta 


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
    
   
    bronze_telemetry = data[1] #based on the output from previous block
    print(bronze_telemetry.dtypes)
    #bronze_telemetry = bronze_telemetry.with_columns(
    #    pl.col("timestamp").str.to_datetime()  # Convert string to datetime
    #)
    # Define device-specific windows
    silver_telemetry = bronze_telemetry.with_columns([
    pl.col("timestamp").str.to_datetime().alias("timestamp"),

    # derive date & hour directly from the converted expression
    pl.col("timestamp").str.to_datetime().dt.date().alias("date"),
    pl.col("timestamp").str.to_datetime().dt.hour().alias("hour"),

    (pl.col("vibration") > 3.0).alias("is_vibration_anomaly"),
    (pl.col("temperature") > pl.col("temperature").mean() + 2*pl.col("temperature").std()).alias("is_temp_anomaly"),
    (pl.col("energy_usage") > pl.col("energy_usage").mean() + 2*pl.col("energy_usage").std()).alias("is_energy_spike"),

    (
        0.4 * (1 - pl.col("temperature")/30)
        + 0.3 * (1 - pl.col("vibration")/5)
        + 0.3 * (pl.col("signal_strength")/100)
    ).round(2).alias("composite_health_score"),
    ])
    return silver_telemetry


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
