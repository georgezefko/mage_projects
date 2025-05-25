if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import polars as pl
from datetime import datetime, timedelta
@transformer
def transform(device_health, silver_telemetry, *args, **kwargs):
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
    current_date = datetime.now().date()
    print(silver_telemetry.columns)
    lookback_7d = silver_telemetry.filter(
    pl.col("date") >= (current_date - timedelta(days=60))
    ).group_by("device_id").agg([
        pl.col("is_vibration_anomaly").sum().alias("anomalies_7d"),
        pl.col("temperature").mean().round(1).alias("avg_temp_7d"),
        pl.col("energy_usage").max().alias("peak_energy_7d"),
        pl.col("composite_health_score").min().alias("worst_health_score_7d")
    ])

    # 24-hour lookback
    lookback_24h = silver_telemetry.filter(
        pl.col("date") == current_date
    ).group_by("device_id").agg([
        pl.col("is_vibration_anomaly").sum().alias("anomalies_24h"),
        pl.col("temperature").max().alias("peak_temp_24h")
    ])

    # First create the failure_risk_score
    data = (
        lookback_7d.join(lookback_24h, on="device_id", how="left")
        .with_columns(
            (
                (0.3 * pl.col("anomalies_24h") / 10) + 
                (0.4 * pl.col("peak_temp_24h") / 30) + 
                (0.2 * (1 - pl.col("worst_health_score_7d"))) + 
                (0.1 * pl.col("peak_energy_7d") / 5)
            ).round(2).alias("failure_risk_score"),
            pl.lit(current_date).alias("prediction_date")
        )
    )

    # Then add the recommended_action based on the failure_risk_score
    data = data.with_columns(
        pl.when(pl.col("failure_risk_score") > 0.8).then("immediate_shutdown")
        .when(pl.col("failure_risk_score") > 0.6).then("emergency_maintenance")
        .when(pl.col("failure_risk_score") > 0.4).then("schedule_maintenance")
        .otherwise("monitor_only")
        .alias("recommended_action")
    )
    print(data.columns)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
