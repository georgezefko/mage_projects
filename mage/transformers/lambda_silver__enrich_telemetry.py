if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import polars as pl
from polars import col 
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
    
   
    bronze_telemetry = data[1] #based on the output from previous block
    print(bronze_telemetry.dtypes)
    bronze_telemetry = bronze_telemetry.with_columns(
        pl.col("timestamp").str.to_datetime()  # Convert string to datetime
    )

    # Transformations
    silver_telemetry = (
        bronze_telemetry.filter(
            (col("energy_usage").is_between(0.1, 20.0)) &  # Wider range to catch anomalies
            (col("temperature").is_between(-10, 100)) &    # Account for extreme environments
            (col("vibration") >= 0)                       # No negative vibrations
        )
        .with_columns(
            pl.when(
                (col("vibration") > 5) | 
                (col("temperature") > 28) |
                (col("signal_strength") < 75)  # New field from generator
            )
            .then(pl.lit("warning"))
            .otherwise(pl.lit("normal"))
            .alias("status")
        )
        .with_columns(
            (col("status") != "normal").alias("is_anomaly")
        )
        # Group by device_id first, then calculate rolling mean within each group
        .group_by("device_id", maintain_order=True)
        .agg(
            pl.all().exclude("rolling_5min_energy"),
            pl.col("energy_usage")
            .rolling_mean(
                window_size="300s",
                by="timestamp",
                closed="both"
            )
            .alias("rolling_5min_energy")
        )
        .explode(pl.all().exclude("device_id"))
        .with_columns(
            pl.when(col("signal_strength") < 70)
            .then(pl.lit("low"))
            .otherwise(pl.lit("high"))
            .alias("data_quality")
        )
    )
    return silver_telemetry


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
