if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import time
import pandas as pd
import pyarrow as pa
from pyarrow import schema, field
from minio import Minio, S3Error
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField
import io
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
BRANCH_NAME = "full-test"
NAMESPACE = "tutorial"


nessie_client = pynessie.init(
     config_dict={
         'endpoint': 'http://nessie:19120/api/v1/',
         'verify': True,  # or False if you do not want SSL verification
         'default_branch': 'main',  # Set the default branch
     }
 )


# # Ensure the branch exists

def create_branch(branch_name):
    try:
        # Get the list of references (branches and tags)
        references = nessie_client.list_references().references

        # Check if the desired branch already exists
        branch_exists = any(ref.name == branch_name for ref in references)
        
        if not branch_exists:
            # Get the hash of the 'main' branch to create a new branch from it
            main_branch = next(ref for ref in references if ref.name == "main")
            target_hash = main_branch.hash_
            
            # Create the new branch from the 'main' branch at the specified hash
            branch = nessie_client.create_branch(branch_name, "main", hash_on_ref=target_hash)
            print(f"Branch '{branch_name}' created successfully.")
        else:
            print(f"Branch '{branch_name}' already exists.")
            branch = next(ref for ref in references if ref.name == branch_name)  # Get the existing branch

    except NessieClientException as e:
        print(f"Failed to create branch '{branch_name}': {str(e)}")
        raise

    return branch

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
        "nessie.default-branch.name": BRANCH_NAME,
       },

    )
    print('Catalog initialized successfully')
except Exception as e:
    print(f"Error initializing catalog: {str(e)}")
    raise


def create_namespace_if_not_exists(catalog, namespace):
    try:
        # Check if the namespace exists
        print('get existing namespaces')
        existing_namespaces = catalog.list_namespaces()
        print('existing', existing_namespaces)
        if namespace not in [ns[0] for ns in existing_namespaces]:
            print(f"Namespace '{namespace}' does not exist. Creating it.")
            catalog.create_namespace(namespace)
            print(f"Namespace '{namespace}' created successfully.")
        else:
            print(f"Namespace '{namespace}' already exists.")
    except Exception as e:
        print(f"Failed to create or list namespace: {e}")
        raise

def create_dummy_data():
    data = {
        "id": [1, 2, 3, 4, 5],
        "description": ["Nice place", "Cozy spot", "Luxurious stay", "Dorm", "View"],
        "reviews": ["Great!", "Good", "Excellent", "Fair", "Fair"],
        "bedrooms": ["2", "1", "3", "1", "3"],
        "beds": ["2", "1", "3", "7", "4"],
        "baths": ["1", "1", "2", "1", "1"],
    }
    df = pd.DataFrame(data)

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

def create_iceberg_table(catalog, table_name, schema, location):
    try:
        # Ensure the namespace exists
        #create_namespace_if_not_exists(catalog, NAMESPACE)

        print(f"Listing tables in namespace: {NAMESPACE}")
        tables = catalog.list_tables(NAMESPACE)
        print(f"Tables in the catalog: {tables}")
        if not any([table_name == t[1] for t in tables]):
            print(f"Creating table {table_name} at location {location}")
            catalog.create_table(
                identifier=(NAMESPACE, table_name),
                schema=schema,
                location=location,
            )
            print(f"Created Iceberg table: {table_name}")
        else:
            print(f"Iceberg table {table_name} already exists")
    except Exception as e:
        print(f"Failed to create or list table: {e}")
        raise


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #Read CSV to Arrow table
    arrow_table = create_dummy_data()

    # Define schema based on the Arrow table
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "description", StringType(), required=False),
        NestedField(3, "reviews", StringType(), required=False),
        NestedField(4, "bedrooms", StringType(), required=False),
        NestedField(5, "beds", StringType(), required=False),
        NestedField(6, "baths", StringType(), required=False)
    )
   
    # Create Iceberg table if not exists
    create_iceberg_table(catalog, TABLE_NAME, schema, f"s3a://{BUCKET_NAME}/{NAMESPACE}")
    branch = create_branch(BRANCH_NAME)
    print('branch',branch)
    try:
        table = catalog.load_table(('tutorial.full-test', TABLE_NAME))
        print('table',table)
        table.append(arrow_table)
        print(f"Appended data to table: {TABLE_NAME}")
    except Exception as e:
        print(f"Failed to load or append data to the table: {e}")
        raise

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
