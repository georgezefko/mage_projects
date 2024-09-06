if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
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
# preload the friendlywords package
import friendlywords as fw
fw.preload()

# Ensure custom patches are loaded correctly
import sys
#sys.path.append('/home/mage_code/mage/utils')
#import monkey_patch
#print('check', monkey_patch.AVAILABLE_CATALOGS)
#from pyiceberg_patch_nessie import NessieCatalog

# Configuration
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_ENDPOINT = "minio:9000"  # MinIO service endpoint without http://
NESSIE_ENDPOINT = os.environ.get('NESSIE_ENDPOINT', "http://nessie:19120/iceberg/v1/config")  # Use base URL without /api/v1
BUCKET_NAME = "iceberg-demo-nessie"  # Ensure consistent bucket name
TABLE_NAME = "airbnb_listings"
BRANCH_NAME = "main"

# Ensure environment variables are set
assert MINIO_ACCESS_KEY, "MINIO_ACCESS_KEY environment variable is not set"
assert MINIO_SECRET_KEY, "MINIO_SECRET_KEY environment variable is not set"
assert MINIO_ENDPOINT, "MINIO_ENDPOINT environment variable is not set"
assert NESSIE_ENDPOINT, "NESSIE_ENDPOINT environment variable is not set"

# Initialize MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Ensure bucket exists
#try:
#    if not minio_client.bucket_exists(BUCKET_NAME):
#        minio_client.make_bucket(BUCKET_NAME)
#        print(f"Bucket '{BUCKET_NAME}' created.")
#        # Add a delay to ensure MinIO registers the bucket creation
#        time.sleep(20)
#    else:
#        print(f"Bucket '{BUCKET_NAME}' already exists.")
#except S3Error as e:
#    print(f"Error checking or creating bucket: {e}")
#    raise

# Verify and create the warehouse directory if it doesn't exist
#try:
#    objects = minio_client.list_objects(BUCKET_NAME, prefix="warehouse", recursive=True)
#    if not any(obj.object_name.startswith("warehouse/") for obj in objects):
#        # Create an empty object to ensure the warehouse directory exists
#        minio_client.put_object(BUCKET_NAME, "warehouse/placeholder", io.BytesIO(b""), 0)
#        print(f"Warehouse directory created in bucket '{BUCKET_NAME}'")
#except S3Error as e:
#    print(f"Error verifying or creating warehouse directory: {e}")
#    raise

#print(f"Nessie endpoint: {NESSIE_ENDPOINT}")

# Verify bucket existence with logging
try:
    bucket_exists = minio_client.bucket_exists(BUCKET_NAME)
    print(f"Bucket exists verification: {bucket_exists}")
    if not bucket_exists:
        raise Exception(f"Bucket {BUCKET_NAME} does not exist.")
except Exception as e:
    print(f"Error verifying bucket existence: {e}")
    raise

# Initialize Iceberg catalog with Nessie
try:
    print('Initializing catalog...')
    catalog = load_catalog(
        name="nessie",
        type="REST",
        uri="http://nessie:19120/iceberg/",
        warehouse=f"s3a://{BUCKET_NAME}/warehouse",  # Use consistent bucket name
        #uri=f"http://{MINIO_ENDPOINT}",
        #access_key_id=MINIO_ACCESS_KEY,
        #secret_access_key=MINIO_SECRET_KEY,
        #path_style_access=True,
        prefix = BRANCH_NAME
    )
    print('Catalog initialized successfully')
except Exception as e:
    print(f"Error initializing catalog: {str(e)}")
    raise

def create_dummy_data():
    data = {
        "id": [1, 2, 3, 4],
        "description": ["Nice place", "Cozy spot", "Luxurious stay", "Dorm"],
        "reviews": ["Great!", "Good", "Excellent", "Fair"],
        "bedrooms": ["2", "1", "3", "1"],
        "beds": ["2", "1", "3", "7"],
        "baths": ["1", "1", "2", "1"],
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
        print(f"Listing tables in namespace: {BRANCH_NAME}")
        tables = catalog.list_tables(BRANCH_NAME)
        print(f"Tables in the catalog: {tables}")
        if not any([table_name == t[1] for t in tables]):
            print(f"Creating table {table_name} at location {location}")
            catalog.create_table(
                identifier=(BRANCH_NAME, table_name),
                schema=schema,
                location=location,
            )
            print(f"Created Iceberg table: {table_name}")
        else:
            print(f"Iceberg table {table_name} already exists")
    except Exception as e:
        print(f"Failed to create or list table: {e}")
        raise
def create_branch_from_main(catalog):
    """
    
    Create a random branch with a human-readable name starting from main.
    
    """
    
    branch_name = fw.generate('ppo', separator='-')
    catalog.create_branch(branch_name, 'main')

    return branch_name
@data_loader
def main(*args, **kwargs):
    

    # Read CSV to Arrow table
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
    create_iceberg_table(catalog, TABLE_NAME, schema, f"s3a://{BUCKET_NAME}/warehouse")
    
    # create a new branch in the catalog from main
    branch_name = create_branch_from_main(catalog)
    print("Branch created: {}".format(branch_name))
    # Load the table and append data
    try:
        table = catalog.load_table((branch_name, TABLE_NAME))
        with table.new_append() as append:
            for batch in arrow_table.to_batches():
                append.append_batch(batch)
            append.commit()
        print(f"Appended data to table: {TABLE_NAME}")
    except Exception as e:
        print(f"Failed to load or append data to the table: {e}")
        raise

    # Verify the data
    try:
        loaded_table = catalog.load_table((branch_name, TABLE_NAME))
        print(f"Table {TABLE_NAME} now has {loaded_table.scan().to_arrow().num_rows} rows")
    except Exception as e:
        print(f"Failed to verify data in the table: {e}")
        raise
