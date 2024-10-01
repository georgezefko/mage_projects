if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


from minio import Minio
import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl
from datetime import datetime
from pyiceberg.catalog import load_catalog
import os
from minio import Minio
import time
from mage.utils.nessie_branch_manager import NessieBranchManager
from mage.utils.iceberg_table_manager import IcebergTableManager




tables = ["payments", "orders", "customers","reviews"]


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    table_manager = IcebergTableManager()
    NAMESPACE = kwargs['namespace']
    main_catalog = table_manager.initialize_rest_catalog('bronze')
    #tables = main_catalog.list_tables(NAMESPACE)

    print(tables)
    bronze_tbls = []
    for tbl in tables:
        table_name = f"{NAMESPACE}.{tbl}-bronze"
        table = main_catalog.load_table(table_name)
    
        arrow_table = table.scan().to_arrow()
    
        polars_df = pl.from_arrow(arrow_table)
        bronze_tbls.append(polars_df)
    print(len(bronze_tbls))
    return bronze_tbls


@test
def test_output(output,tables, *args) -> None:
    """
    Template code for testing the output of the block.
    """
   
    assert output is not None, 'The output is undefined'
    assert len(output) == len(tables), 'Table is missing'
