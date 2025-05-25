if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import polars as pl

@transformer
def transform(failure_predictions,device_health, *args, **kwargs):
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
    # First create the priority column
    data = device_health.join(failure_predictions, on="device_id", how="inner")
    data = data.with_columns(
        pl.when(pl.col("failure_risk_score") > 0.7).then("P0")
        .when(pl.col("days_since_maintenance") > 90).then("P1")
        .when(pl.col("vibration_anomalies") > 5).then("P2")
        .otherwise("P3")
        .alias("priority")
    )

    # Then create the suggested_intervention column
    data = data.with_columns(
        pl.when(pl.col("priority") == "P0").then(pl.struct(
            window=pl.lit("within_4_hours"),
            estimated_cost=pl.lit(5000)
        ))
        .when(pl.col("priority") == "P1").then(pl.struct(
            window=pl.lit("within_3_days"),
            estimated_cost=pl.lit(2000)
        ))
        .otherwise(pl.struct(
            window=pl.lit("next_routine_cycle"),
            estimated_cost=pl.lit(500)
        ))
        .alias("suggested_intervention")  # Note: Fixed typo here (removed extra 'n')
    )

    # Debug: Print columns to verify
    print("Available columns:", data.columns)

    # Select the columns you want
    data = data.select([
        "device_id", 
        "date", 
        "priority",
        "suggested_intervention",  # Select the struct column first
        "failure_risk_score"
    ])

    # If you want to expand the struct, do it after selecting
    data = data.with_columns(
        pl.col("suggested_intervention").struct.rename_fields(["window", "estimated_cost"])
    ).unnest("suggested_intervention")

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
