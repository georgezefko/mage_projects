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
 
    filter_ = bronze_events.filter(col("event_type") == "failure")

    # Main transformations
    silver_events = (
        bronze_events
        .with_columns(
            # Create struct for event metadata
            pl.struct(
                ["error_code", "component", "root_cause", "technician", "parts_replaced"]
            ).alias("event_metadata"),
            
            # Conditional duration_minutes
            pl.when(col("event_type") == "maintenance")
            .then(col("duration_min"))
            .otherwise(None)
            .alias("duration_minutes"),
            
            # Boolean is_critical column
            (col("severity") == "high").alias("is_critical"),
            
            # Conditional maintenance_type
            pl.when(col("event_type") == "maintenance")
            .then(pl.lit("routine"))
            .otherwise(None)
            .alias("maintenance_type")
        )
        .drop("duration_min")  # Remove old column
    )

    # Handle inspection events
    inspections = (
        silver_events
        .filter(col("event_type") == "inspection")
        .select(
            "device_id",
            "event_timestamp",
            pl.col("status").alias("inspection_result"),
            "next_inspection_days"
        )
    )

    return silver_events, inspections


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
