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


def write_data(data, NAMESPACE, table_manager, tbl_name, BUCKET_NAME):
    
    table_name = tbl_name
    schema = data.schema
   
    arrow_schema = table_manager.polars_to_pyarrow_schema(schema)
    arrow_table = table_manager.polars_to_arrow_with_schema(data, arrow_schema)

    # Initialize the REST catalog
    catalog = table_manager.initialize_catalog()  # No branch parameter needed

    # Create namespace 
    namespace = table_manager.create_namespace_if_not_exists(catalog, NAMESPACE)

    # Create the Iceberg table if it doesn't exist
    table_manager.create_iceberg_table(
        catalog, 
        namespace, 
        table_name, 
        arrow_schema, 
        f"s3a://{BUCKET_NAME}/{NAMESPACE}"
    )
   
    # Load the table
    table_identifier = f"{NAMESPACE}.{table_name}"
    _table = catalog.load_table(table_identifier)

    # Append the Arrow table data to the Iceberg table
    _table.append(arrow_table)

    # Run data quality check
    _pass = data_quality_check(_table)
   
    if not _pass:
        raise ValueError(f"Failed to pass quality tests for table {table_name}")


@data_exporter
def export_data(device_health, failure_prediction, maint_schedule, *args, **kwargs):
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
    BUCKET_NAME = "iot-silver"
    NAMESPACE = "iot-gold"
 
    table_manager = IcebergTableManager(catalog_type='nessie')


    _ = write_data(device_health, NAMESPACE,  table_manager, 'gold_device_health', BUCKET_NAME)
    
    _ = write_data(failure_prediction, NAMESPACE,  table_manager, 'gold_failure_prediction', BUCKET_NAME)

    _ = write_data(maint_schedule, NAMESPACE,  table_manager, 'gold_maintenance_schedule', BUCKET_NAME)

    
    