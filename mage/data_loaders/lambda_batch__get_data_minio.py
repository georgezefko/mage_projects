if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import polars as pl
import os
from minio import Minio
from io import BytesIO
from pyarrow.fs import S3FileSystem
import pyarrow.dataset as ds
# Environment variables
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_ENDPOINT = 'http://minio:9000'
EVENTS_SCHEMA: dict[str, pl.DataType] = {
    "device_id": pl.Utf8,
    "event_timestamp": pl.Utf8,
    "event_type": pl.Utf8,
    "severity": pl.Utf8,
    "error_code": pl.Utf8,
    "component": pl.Utf8,
    "root_cause": pl.Utf8,
    "technician": pl.Utf8,
    "duration_min": pl.Int64,
    "parts_replaced": pl.Utf8,
    "status": pl.Utf8,
    "next_inspection_days": pl.Int64
}


def get_minio_fs():
    """Create MinIO filesystem connection"""
    return S3FileSystem(
        endpoint_override=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        allow_bucket_creation=False,
        allow_bucket_deletion=False,
        connect_timeout=15  # Adjust as needed
    )

def read_with_pyarrow_dataset(bucket: str, prefix: str) -> pl.DataFrame:
    """Read Parquet data from MinIO using PyArrow dataset
    
    Args:
        bucket: MinIO bucket name (e.g., 'iot-bronze')
        prefix: Path prefix (e.g., 'telemetry' or 'telemetry/date=2024-01-01')
    
    Returns:
        polars.DataFrame with the combined data
    """
    try:
        fs = get_minio_fs()
        
        dataset = ds.dataset(
            f"{bucket}/{prefix}",
            format="parquet",
            filesystem=fs,
            partitioning=["date", "device_id"],  # Hive-style partitioning
            exclude_invalid_files=True  # Skip corrupt files
        )
        
        # Convert to Polars DataFrame
        return pl.from_arrow(dataset.to_table())
        
    except Exception as e:
        raise RuntimeError(f"Failed to load data from MinIO: {str(e)}")

# 2. Simple schema enforcement function
def prepare_events(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure DataFrame has all expected columns with proper types"""
    # Add missing columns with null values
    for col_name, dtype in EVENTS_SCHEMA.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col_name))
    
    # Convert timestamp and return
    return df.with_columns(
        pl.col("event_timestamp").str.to_datetime()
    ).select(EVENTS_SCHEMA.keys())

@data_loader
def load_data(*args, **kwargs):
    try:
        events_df    = prepare_events(read_with_pyarrow_dataset("iot-bronze", "events"))
        telemetry_df = read_with_pyarrow_dataset("iot-bronze", "telemetry")

        if events_df.is_empty():
            raise ValueError("Loaded empty Events DataFrame")
        if telemetry_df.is_empty():
            raise ValueError("Loaded empty Telemetry DataFrame")
        
       
            
        return events_df, telemetry_df
        
    except Exception as e:
        raise