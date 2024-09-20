if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IntegerType, StringType
import pyarrow as pa
import os
from minio import Minio
from mage.utils.nessie_branch_manager import NessieBranchManager


MINIO_ENDPOINT = "minio:9000"  # MinIO service endpoint without http://
NESSIE_ENDPOINT = os.environ.get('NESSIE_ENDPOINT', "http://nessie:19120/iceberg/v1/")
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')  # Use base URL without /api/v1
BUCKET_NAME = "iceberg-demo-nessie" 
TABLE_NAME = "airbnb_listings_trial"
BRANCH_NAME = "full-test-2"
NAMESPACE = "tutorial"




def pandas_to_arrow(df):
    """Convert pandas DataFrame to Arrow table with a predefined schema."""
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("description", pa.string()),
            pa.field("reviews", pa.string()),
            pa.field("bedrooms", pa.string()),
            pa.field("beds", pa.string()),
            pa.field("baths", pa.string()),
        ]
    )
    return pa.Table.from_pandas(df, schema=arrow_schema)


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
    except Exception as e:
        print(f"Failed to load or append data to the table: {e}")
        raise


@transformer
def transform_custom(data, *args, **kwargs):
    """Main entry point to transform data."""
    
    arrow_table = pandas_to_arrow(data)

    # Define the Iceberg schema
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "description", StringType(), required=False),
        NestedField(3, "reviews", StringType(), required=False),
        NestedField(4, "bedrooms", StringType(), required=False),
        NestedField(5, "beds", StringType(), required=False),
        NestedField(6, "baths", StringType(), required=False)
    )

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

    # Create the Iceberg table if it doesn't exist
    create_iceberg_table(main_catalog, NAMESPACE, TABLE_NAME, schema, f"s3a://{BUCKET_NAME}/{NAMESPACE}")

     # Use branch manager to handle branch creation
    #new_branch = branch_manager.create_branch(BRANCH_NAME)

     # Reinitialize catalog for the specific branch
    #catalog = initialize_rest_catalog(new_branch)
    # Load and append data to the table
    load_and_append_table(branch_manager, BRANCH_NAME, TABLE_NAME, arrow_table)

    # Merge the branch back to 'main' and delete the temporary branch
    try:
        branch_manager.merge_branch(from_branch=BRANCH_NAME)
        branch_manager.delete_branch(BRANCH_NAME)
    except Exception as e:
        print(f"Failed to merge or delete the branch '{BRANCH_NAME}': {e}")
        raise

    return