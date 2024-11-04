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


MINIO_ENDPOINT = "minio:9000"  # MinIO service endpoint without http://
NESSIE_ENDPOINT = os.environ.get('NESSIE_ENDPOINT', "http://nessie:19120/iceberg/v1/")
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')  # Use base URL without /api/v1
BUCKET_NAME = "iceberg-demo-nessie" 
NAMESPACE = "gold"
# Function to convert Polars schema to PyArrow schema
def polars_to_pyarrow_schema(polars_schema):
    schema_map = {
        pl.Utf8: pa.string(),
        pl.Int32: pa.int32(),
        pl.Float64: pa.float64(),
        pl.Boolean: pa.bool_(),
        pl.Datetime: pa.timestamp('ns'),
        pl.UInt32 : pa.int32()
        # You can add more mappings as needed
    }

    fields = []
    for col, dtype in polars_schema.items():
        if dtype in schema_map:
            fields.append(pa.field(col, schema_map[dtype]))
        else:
            fields.append(pa.field(col, pa.string()))  # Default to string if type not found

    return pa.schema(fields)

# Corrected function to convert Polars DataFrame to PyArrow Table with schema
def polars_to_arrow_with_schema(df, arrow_schema):
    """Convert Polars DataFrame to PyArrow Table using a given schema."""
    
    # Create an empty dictionary to store Arrow columns
    arrow_columns = {}
    
    # Convert each column in Polars DataFrame to an Arrow Array
    for col in df.columns:
        polars_series = df[col]
        
        # Extract the data type from the schema for the current column
        arrow_dtype = arrow_schema.field_by_name(col).type
        
        # Convert Polars column to PyArrow array using the extracted data type
        arrow_columns[col] = pa.array(polars_series.to_list(), type=arrow_dtype)
    
    # Build Arrow Table using the Arrow columns
    arrow_table = pa.Table.from_pydict(arrow_columns, schema=arrow_schema)
    
    return arrow_table


def create_namespace_if_not_exists(catalog, namespace):
    try:
        # Check if the namespace exists
        existing_namespaces = [ns[0] for ns in catalog.list_namespaces()]
        if namespace not in existing_namespaces:
            print(f"Namespace '{namespace}' does not exist. Creating it.")
            catalog.create_namespace(namespace)
            print(f"Namespace '{namespace}' created successfully.")
        else:
            print(f"Namespace '{namespace}' already exists.")
        return namespace
    except Exception as e:
        print(f"Failed to create or list namespace: {e}")
        raise

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


def create_iceberg_table(catalog, namespace, table_name, schema, location):
    """Create an Iceberg table using the REST catalog."""
    try:
        tables = catalog.list_tables(namespace)
        if table_name not in [t[1] for t in tables]:
            print(f"Creating table '{table_name}' at location '{location}'.")
            catalog.create_table(
                identifier=(namespace, table_name),
                schema=schema,
                location=location
            )
            print(f"Created Iceberg table: '{table_name}' successfully.")
        else:
            print(f"Iceberg table '{table_name}' already exists.")
    except Exception as e:
        print(f"Failed to create or list table '{table_name}': {e}")
        raise


def load_and_append_table(branch_manager, branch_name, table_name, arrow_table):
    """Load an Iceberg table from a specific branch and append data using the PyIceberg REST catalog."""
    try:
        # Use branch manager to handle branch creation (will skip if the branch exists)
        branch_manager.create_branch(branch_name)

        # Merge from main branch to ensure the table exists on the new branch
        branch_manager.merge_branch(from_branch="main", to_branch=branch_name)

        # Reinitialize catalog for the specific branch
        catalog = initialize_rest_catalog(branch_name)

        # Load the table from the branch
        table_identifier = f"{NAMESPACE}.{table_name}"
        _table = catalog.load_table(f"{table_identifier}")

        # Append the Arrow table data to the Iceberg table
        _table.append(arrow_table)
        print(f"Appended data to table: {table_name} on branch: {branch_name}")
        return _table
    except Exception as e:
        print(f"Failed to load or append data to the table: {e}")
        raise

def generate_custom_branch_name(table_name, NAMESPACE):
    """Generate a branch name with the format: fix/bronze-timestamp-random_number."""
    # Current timestamp
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    
    # Create the branch name
    branch_name = f"{label}-{table_name}-{timestamp}"
    return branch_name


def data_quality_check(table):

    for column_name in table.schema.names:
        column = table[column_name]
        
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
    table = data[0]
    schema = table.schema
    

    print('schema', schema)

    table_name = 'sales_fact'
    branch_name = generate_custom_branch_name(table_name)
   
    arrow_schema = polars_to_pyarrow_schema(schema)
    arrow_table = polars_to_arrow_with_schema(table, arrow_schema)

    print(arrow_schema)
     # MinIO bucket setup
    client = Minio(
        "minio:9000",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    minio_bucket = BUCKET_NAME
    if not client.bucket_exists(minio_bucket):
        client.make_bucket(minio_bucket)

    # Initialize branch manager
    branch_manager = NessieBranchManager()

    # Initialize the REST catalog for Nessie and Iceberg
    main_catalog = initialize_rest_catalog('main')

    #create namespace 
    namespace = create_namespace_if_not_exists(main_catalog, NAMESPACE)

    # Create the Iceberg table if it doesn't exist
    create_iceberg_table(main_catalog, namespace, table_name, arrow_schema, f"s3a://{BUCKET_NAME}/{NAMESPACE}")
   
    # Load and append data to the table
    table = load_and_append_table(branch_manager, branch_name, table_name, arrow_table)

    _pass = data_quality_check(arrow_table)
   
    if _pass:
        branch_manager.merge_branch(from_branch=branch_name)
        branch_manager.delete_branch(branch_name)

    else:
        raise ValueError(f"Failed to pass tests for table {table_name}")



