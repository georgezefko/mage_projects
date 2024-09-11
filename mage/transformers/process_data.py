if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from typing import (
    Any,
    List,
    Optional,
    Set,
    Union,
)
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
import os
import time
import pandas as pd
import pyarrow as pa
from pyarrow import schema, field
from minio import Minio, S3Error
from pyiceberg.catalog import load_catalog, Identifier
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField
from pynessie.model import ContentKey, IcebergTable
import io
from pyiceberg.io import load_file_io
import requests
from pynessie.client import NessieClient
import pynessie
#import friendlywords as fw
MINIO_ENDPOINT = "minio:9000"  # MinIO service endpoint without http://
NESSIE_ENDPOINT = os.environ.get('NESSIE_ENDPOINT', "http://nessie:19120/iceberg/v1/")
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')  # Use base URL without /api/v1
BUCKET_NAME = "iceberg-demo-nessie" 
TABLE_NAME = "airbnb_listings-trial"
BRANCH_NAME = "full-test-2"
NAMESPACE = "tutorial"



def create_branch(nessie_client, branch_name):
    try:
        # Get the list of references (branches and tags)
        references = nessie_client.list_references().references

        # Check if the desired branch already exists
        branch = next((ref for ref in references if ref.name == branch_name), None)
        
        if branch:
            print(f"Branch '{branch_name}' already exists.")
        else:
            # Get the 'main' branch to create a new branch from it
            main_branch = next(ref for ref in references if ref.name == "main")
            
            # Create the new branch from the 'main' branch
            branch = nessie_client.create_branch(branch_name, "main", hash_on_ref=main_branch.hash_)
            print(f"Branch '{branch_name}' created successfully.")

    except Exception as e:
        print(f"Failed to create branch '{branch_name}': {str(e)}")
        raise

    return branch


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

def pandas_to_arrow(df):
    
    arrow_schema = schema(
        [
            field("id", pa.int32(), nullable=False),
            field("description", pa.string()),
            field("reviews", pa.string()),
            field("bedrooms", pa.string()),
            field("beds", pa.string()),
            field("baths", pa.string()),
        ]
    )

    return pa.Table.from_pandas(df, schema=arrow_schema)

def create_iceberg_table(catalog, namespace, table_name, schema, location):
    try:
        # List tables in the specified namespace
        tables = catalog.list_tables(namespace)
        print(f"Tables in namespace '{namespace}': {tables}")

        # Check if the table already exists
        if table_name not in [t[1] for t in tables]:
            print(f"Creating table '{table_name}' at location '{location}'.")
            catalog.create_table(
                identifier=(namespace, table_name),
                schema=schema,
                location=location,
            )
            print(f"Created Iceberg table: '{table_name}' successfully.")
        else:
            print(f"Iceberg table '{table_name}' already exists.")
    except Exception as e:
        print(f"Failed to create or list table '{table_name}': {e}")
        raise

def load_table(catalog,nessie_client, branch_name, table_name):
    """
    Load an Iceberg table from a specific branch in Nessie.
    
    :param catalog: The initialized Iceberg catalog.
    :param branch_name: The name of the branch from which to load the table.
    :param table_name: The name of the table to load.
    
    :return: The Iceberg table object.
    """
    try:
        # Get the reference (branch) from Nessie
       
        branch_ref = nessie_client.get_reference(branch_name)
        branch_hash = branch_ref.hash_
      
         # Split the table name into namespace and table name
        namespace_parts = table_name.split(".")
        print('parts',namespace_parts)
        namespace = namespace_parts[:-1]  # Everything before the last part is the namespace
        table_name_only = namespace_parts[-1]  # The last part is the table name
        print('namespace', namespace)
        print('table_name', table_name_only)
        # Create a ContentKey for the table
        content_key = ContentKey(namespace + [table_name_only])
        print('key',content_key)
        content = nessie_client.get_content(
            ref=f"{branch_name}@{branch_hash}",
            content_key=content_key  # Assuming the table name can be used as the content key
        )

        if not isinstance(content, IcebergTable):
            raise ValueError(f"Content {table_name} is not an Iceberg table.")

        # Load the table metadata from the metadata location
        metadata_location = content.metadata_location
        io = load_file_io(properties=catalog.properties, location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)

        # Return the Iceberg table object
        return Table(
            identifier=namespace + [table_name_only],
            metadata_location=metadata_location,
            metadata=metadata,
            io=io,
            catalog=catalog,
        )
    except Exception as e:
        print(f"Failed to load table '{table_name}' from branch '{branch_name}': {e}")
        raise

@transformer
def transform_custom(data, *args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #Read CSV to Arrow table
    arrow_table = pandas_to_arrow(data)

    # Define schema based on the Arrow table
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "description", StringType(), required=False),
        NestedField(3, "reviews", StringType(), required=False),
        NestedField(4, "bedrooms", StringType(), required=False),
        NestedField(5, "beds", StringType(), required=False),
        NestedField(6, "baths", StringType(), required=False)
    )

    nessie_client = pynessie.init(
        config_dict={
            'endpoint': 'http://nessie:19120/api/v1/',
            'verify': True,  # or False if you do not want SSL verification
            'default_branch': 'main',  # Set the default branch
        }
    )

    
    try:
        catalog = load_catalog(
        name="nessie",
        type="rest",
        uri="http://nessie:19120/iceberg",
        **{
            "s3.endpoint":f"http://{MINIO_ENDPOINT}",
            "s3.access-key-id":MINIO_ACCESS_KEY,
            "s3.secret-access-key":MINIO_SECRET_KEY,
            #"prefix": NAMESPACE,
            "nessie.default-branch.name": 'main',
        },

        )
        print('Catalog initialized successfully')
    except Exception as e:
        print(f"Error initializing catalog: {str(e)}")
        raise

    namespace = create_namespace_if_not_exists(catalog, NAMESPACE)
    # Create Iceberg table if not exists
    create_iceberg_table(catalog, namespace, TABLE_NAME, schema, f"s3a://{BUCKET_NAME}/{NAMESPACE}")
    
    branch = create_branch(nessie_client, BRANCH_NAME)
    print('branch',branch.name)

    try:
        catalog = load_catalog(
        name="nessie",
        type="rest",
        uri=f"http://nessie:19120/iceberg/{branch.name}",
        **{
            "s3.endpoint":f"http://{MINIO_ENDPOINT}",
            "s3.access-key-id":MINIO_ACCESS_KEY,
            "s3.secret-access-key":MINIO_SECRET_KEY,
            #"prefix": NAMESPACE,
            "nessie.default-branch.name": 'main',
        },

        )
        print('Catalog initialized successfully')
    except Exception as e:
        print(f"Error initializing catalog: {str(e)}")
        raise
    
    try:
        table = f"{namespace}.{TABLE_NAME}"
        _table = load_table(catalog, nessie_client, branch.name, table)
        _table.append(arrow_table)
        print(f"Appended data to table: {TABLE_NAME}")
    except Exception as e:
        print(f"Failed to load or append data to the table: {e}")
        raise

    scan = _table.scan().to_arrow()

    nessie_client.merge(
            from_ref=branch.name,
            onto_branch='main',
        )
    branch_ref = nessie_client.get_reference(branch.name)
    branch_hash = branch_ref.hash_
    nessie_client.delete_branch(branch.name, branch_hash)
    return


