if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from mage.utils.spark_session_factory import get_spark_session


delta_spark_session = get_spark_session("delta", 
                                  app_name="MageSparkSession", 
                                  s3_endpoint="http://minio:9000", 
                                  s3_access_key="your_access_key", 
                                  s3_secret_key="your_secret_key")


@custom
def delta_table_read( *args, **kwargs):
    """
    Read data from a MinIO bucket using Delta format.
    """
    minio_bucket = "delta-demo-bucket"
    table_name = "delta_demo_listings"
    # Construct the full path to the Delta table in the MinIO bucket
    table_path = f"s3a://{minio_bucket}/delta/{table_name}"
    
    print(f"Attempting to read Delta table from: {table_path}")
    
    # Read the Delta table into a Spark DataFrame
    
    df = delta_spark_session.spark.read.format("delta").load(table_path)
    
    # TODO: Further cleaning and processing can be added here
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
