if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import polars as pl

from pyiceberg.catalog import load_catalog
import os


MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_ENDPOINT = "minio:9000" 
BUCKET_NAME = "iceberg-demo-nessie" 
NAMESPACE = "silver"
TABLE_NAME = "orders_fact"

def initialize_rest_catalog(branch_name):
    """Initialize the PyIceberg REST catalog to communicate with Nessie."""
    try:
        catalog = load_catalog(
            name="nessie",
            type="rest",
            uri=f"http://nessie:19120/iceberg/{branch_name}",  # REST endpoint for Nessie
            **{
                "s3.endpoint": f"http://{MINIO_ENDPOINT}",
                "s3.access-key-id": MINIO_ACCESS_KEY,
                "s3.secret-access-key": MINIO_SECRET_KEY,
                "nessie.default-branch.name": 'main',  # Default Nessie branch
            }
        )
        print(f"Catalog initialized successfully for branch: {branch_name}")
        return catalog
    except Exception as e:
        print(f"Error initializing catalog for branch '{branch_name}': {str(e)}")
        raise



@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
   
    main_catalog = initialize_rest_catalog('main')
    tables = main_catalog.list_tables(NAMESPACE)

    print(tables)
    table_name = f"{NAMESPACE}.{TABLE_NAME}"
    table = main_catalog.load_table(table_name)
 
    arrow_table = table.scan().to_arrow()
   
    polars_df = pl.from_arrow(arrow_table)

    return polars_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

