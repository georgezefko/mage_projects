if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
import pyarrow as pa
import polars as pl
from datetime import datetime
from pyiceberg.catalog import load_catalog
import os
from minio import Minio
import time
from mage.utils.nessie_branch_manager import NessieBranchManager
from mage.utils.iceberg_table_manager import IcebergTableManager


def data_quality_check(table):

    arrow_table = table.scan().to_arrow()

    for column_name in arrow_table.schema.names:
        column = arrow_table[column_name]
        
        # Check the null count for each column
        null_count = column.null_count
        
        if null_count > 0:
            print(f"Column '{column_name}' contains {null_count} null values.")
            return False
        else:
            print(f"No null values.")
            return True



@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    BUCKET_NAME = kwargs["bucket_name"]
    NAMESPACE = kwargs["namespace"]
    TABLE_NAME = kwargs['table_name']

    branch_manager = NessieBranchManager()
    table_manager = IcebergTableManager()
    
    schema = data.schema

    branch_name = branch_manager.generate_custom_branch_name(TABLE_NAME, NAMESPACE)
   
    arrow_schema = table_manager.polars_to_pyarrow_schema(schema)
    arrow_table = table_manager.polars_to_arrow_with_schema(table, arrow_schema)

    # Initialize the REST catalog for Nessie and Iceberg
    main_catalog = table_manager.initialize_rest_catalog('main')

    #create namespace 
    namespace = table_manager.create_namespace_if_not_exists(main_catalog, NAMESPACE)

    # Create the Iceberg table if it doesn't exist
    table_manager.create_iceberg_table(main_catalog, namespace, table_name, arrow_schema, f"s3a://{BUCKET_NAME}/{NAMESPACE}")
   
    new_branch_name = branch_manager.create_branch(branch_name)

    # Merge from main branch to ensure the table exists on the new branch
    branch_manager.merge_branch(from_branch="main", to_branch=new_branch_name)

    # Reinitialize catalog for the specific branch
    catalog = table_manager.initialize_rest_catalog(new_branch_name)

    # Load the table from the branch
    table_identifier = f"{NAMESPACE}.{table_name}"
    _table = catalog.load_table(f"{table_identifier}")

    # Append the Arrow table data to the Iceberg table
    _table.append(arrow_table)


    _pass = data_quality_check(_table)
   
    if _pass:
        branch_manager.merge_branch(from_branch=new_branch_name)
        branch_manager.delete_branch(new_branch_name)

    else:
        raise ValueError(f"Failed to pass tests for table {table_name}")


