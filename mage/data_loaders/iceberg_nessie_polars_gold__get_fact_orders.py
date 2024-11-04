if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import polars as pl

from pyiceberg.catalog import load_catalog
import os
from mage.utils.nessie_branch_manager import NessieBranchManager
from mage.utils.iceberg_table_manager import IcebergTableManager


def get_tables(tbl,namespace, catalog):

    
    #tables = main_catalog.list_tables(NAMESPACE)
    table_name = f"{namespace}.{tbl}_silver"
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
    
    main_catalog = table_manager.initialize_rest_catalog('silver')
    
    orders_fact = get_tables('orders_fct', NAMESPACE,main_catalog)
    order_items_fct = get_tables('order_items_fct', NAMESPACE,main_catalog)
    products_dim = get_tables('products_dim', NAMESPACE,main_catalog)
    sellers_dim = get_tables('sellers_dim', NAMESPACE,main_catalog)
    cutomers_dim = get_tables('customers_dim', NAMESPACE,main_catalog)
    


    return orders_fact, order_items_fct, products_dim, sellers_dim, cutomers_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

