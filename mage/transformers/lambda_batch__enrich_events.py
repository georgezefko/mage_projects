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
    # Specify your transformation logic here
    # Specify your transformation logic here
  
    bronze_events = data[0] #based on the output from previous block

    # Main transformations
    
    silver_events = bronze_events.with_columns([
    pl.col("event_timestamp").dt.date().alias("event_date"),
    (pl.col("event_type") == "failure").alias("is_failure"),
    (pl.col("event_type") == "maintenance").alias("is_maintenance"),
    pl.col("event_timestamp")
        .sort()
        .over("device_id")
        .diff()
        .dt.days()  # Use .days() instead of .total_days()
        .fill_null(0)
        .cast(pl.Int64)
        .alias("days_since_last_event")
    ])

    return silver_events


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
