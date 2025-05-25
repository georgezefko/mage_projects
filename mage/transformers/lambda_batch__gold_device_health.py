if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import polars as pl

@transformer
def transform(silver_telemetry, silver_events, *args, **kwargs):
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
    telemetry_daily = silver_telemetry.group_by(["device_id", "date"]).agg([
        pl.col("energy_usage").mean().round(2).alias("avg_energy"),
        pl.col("temperature").max().alias("max_temp"),
        pl.col("composite_health_score").min().alias("min_health_score"),
        pl.col("is_vibration_anomaly").sum().alias("vibration_anomalies"),
        pl.col("is_temp_anomaly").sum().alias("temp_anomalies"),
        pl.col("signal_strength").mean().alias("avg_signal_strength")
    ])
    
    # Failure Stats
    failure_stats = silver_events.filter(pl.col("is_failure")).group_by(["device_id", "event_date"]).agg([
        pl.count().alias("failures_count"),
        pl.col("component").unique().alias("failed_components")
    ])
    
    # Maintenance Impact
    maintenance_impact = silver_events.filter(pl.col("is_maintenance")).group_by("device_id").agg([
        pl.col("event_date").max().alias("last_maintenance_date"),
        pl.col("duration_min").mean().alias("avg_maintenance_duration")
    ])
    
    # Join all
    result = (
        telemetry_daily.join(
            failure_stats,
            left_on=["device_id", "date"],
            right_on=["device_id", "event_date"],
            how="left"
        )
        .join(maintenance_impact, on="device_id", how="left")
        .with_columns(
            (pl.col("date") - pl.col("last_maintenance_date")).dt.days().alias("days_since_maintenance")
        )
    )
    
    # Fill nulls separately for each column
    return result.with_columns([
        pl.col("failures_count").fill_null(0),
        pl.col("failed_components").fill_null([]),
        pl.col("days_since_maintenance").fill_null(999)
    ])


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
