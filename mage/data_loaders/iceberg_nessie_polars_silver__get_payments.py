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

def get_tables(tbl,namespace, catalog):

    
    #tables = main_catalog.list_tables(NAMESPACE)
    table_name = f"{namespace}.{tbl}-bronze"
    table = catalog.load_table(table_name)

    arrow_table = table.scan().to_arrow()

    polars_df = pl.from_arrow(arrow_table)
    return polars_df


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    NAMESPACE = kwargs['namespace']
    table_manager = IcebergTableManager()
    
    main_catalog = table_manager.initialize_rest_catalog('bronze')
    
    payments = get_tables('payments', NAMESPACE,main_catalog)
    orders = get_tables('orders', NAMESPACE,main_catalog)
    customers = get_tables('customers', NAMESPACE,main_catalog)
    reviews = get_tables('reviews', NAMESPACE,main_catalog)
    products = get_tables('products', NAMESPACE,main_catalog)
    sellers = get_tables('sellers', NAMESPACE,main_catalog)
    items = get_tables('order_items', NAMESPACE,main_catalog)
    geolocation = get_tables('geolocation', NAMESPACE,main_catalog)


    return payments, orders, customers, reviews, products, sellers, items, geolocation


@test
def test_output(output,tables, *args) -> None:
    """
    Template code for testing the output of the block.
    """
   
    assert output is not None, 'The output is undefined'
    #assert len(output) == len(tables), 'Table is missing'
