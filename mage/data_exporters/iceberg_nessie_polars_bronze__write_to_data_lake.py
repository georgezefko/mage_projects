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

    NAMESPACE = kwargs['namespace']
    BUCKET_NAME = kwargs['bucket_name']
    DATA_LAYER = kwargs['data_layer']


    # Initialize branch manager
    branch_manager = NessieBranchManager()
    table_manager = IcebergTableManager()

    #create data layer branch
    bronze_br = branch_manager.create_branch(DATA_LAYER)
    # generate branch name
    branch_name = branch_manager.generate_custom_branch_name(DATA_LAYER, NAMESPACE)
    

    for data in data:
        table = data[0]
        schema = data[1]

        table_name = f"{table['table_name'][0]}-{DATA_LAYER}"
    
        arrow_schema = table_manager.polars_to_pyarrow_schema(schema)
        arrow_table = table_manager.polars_to_arrow_with_schema(table, arrow_schema)

        
        # Initialize the REST catalog for Nessie and Iceberg
        main_catalog = table_manager.initialize_rest_catalog(bronze_br)

        #create namespace 
        namespace = table_manager.create_namespace_if_not_exists(main_catalog, NAMESPACE)

        # Create the Iceberg table if it doesn't exist
        table_manager.create_iceberg_table(main_catalog, namespace, table_name, arrow_schema, f"s3a://{BUCKET_NAME}/{NAMESPACE}")

        # create branch from bronze NOT main
        branch_name = branch_manager.generate_custom_branch_name(table_name, NAMESPACE)
        new_cr_branch =  branch_manager.create_branch(branch_name, bronze_br)
        branch_manager.merge_branch(from_branch=bronze_br, to_branch=new_cr_branch)

        # Reinitialize catalog for the specific branch
        catalog = table_manager.initialize_rest_catalog(new_cr_branch)

        # Load the table from the branch
        table_identifier = f"{NAMESPACE}.{table_name}"
        _table = catalog.load_table(f"{table_identifier}")

        # Append the Arrow table data to the Iceberg table
        _table.append(arrow_table)


        _pass = data_quality_check(_table)
    
        if _pass:
            branch_manager.merge_branch(from_branch=new_cr_branch, to_branch = bronze_br)
            branch_manager.delete_branch(new_cr_branch)
        else:
            raise ValueError(f"Failed to pass tests for table {table_name}")

    


