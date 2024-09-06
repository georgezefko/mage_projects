import os
import pandas as pd
from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType
from pynessie import init as nessie_init
# Set environment variables for MinIO access keys
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
from mage.utils.monkey_patch import monkey_patch
from mage.utils.pyiceberg_patch_nessie import NessieCatalog

# Initialize MinIO client
client = Minio(
    "minio:9000",
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create a bucket if it doesn't exist
minio_bucket = "iceberg-demo-bucket-nessie"
if not client.bucket_exists(minio_bucket):
    client.make_bucket(minio_bucket)



# Create a new branch in Nessie
try:
    nessie_client = nessie_init("http://nessie:19120/api/v1")
    # Try to perform a simple operation
    branches = nessie_client.list_branches()
    print("Successfully connected to Nessie. Branches:", branches)
except Exception as e:
    print("Failed to connect to Nessie:", str(e))


# Define Iceberg schema
# schema = Schema(
#     IntegerType().field("id"),
#     StringType().field("description"),
#     StringType().field("reviews"),
#     StringType().field("bedrooms"),
#     StringType().field("beds"),
#     StringType().field("baths")
# )
print('pass 1')
# Initialize Iceberg catalog
catalog = load_catalog(
    "nessie",
    **{"uri":"http://nessie:19120/api/v1",
    "warehouse":"s3a://iceberg-demo-bucket-nessie/warehouse",
    
        "endpoint": "http://minio:9000",
        "access_key_id": MINIO_ACCESS_KEY,
        "secret_access_key": MINIO_SECRET_KEY,
        "path_style_access": "true"
    }
)
print('pass 2')

# Function to process CSV files and write to Iceberg
@custom
def process_and_write_to_iceberg(*args, **kwargs):
    # data_folder = "mage/data"
    # for filename in os.listdir(data_folder):
    #     if filename.endswith(".csv"):
    #         file_path = os.path.join(data_folder, filename)
            
    #         # Read the CSV file into a Pandas DataFrame
    #         df = pd.read_csv(file_path)
            
    #         # Data processing similar to Spark script
    #         if 'listings' in filename:
    #             split_cols = df['name'].str.split('·', expand=True)
    #             df['description'] = split_cols[0].str.strip()
    #             df['reviews'] = split_cols[1].str.strip().str.replace('★', '', regex=False).where(split_cols[1].str.strip().str.startswith('★'))
    #             df['bedrooms'] = split_cols[2].str.strip().where(split_cols[1].str.strip().str.startswith('★'), split_cols[1].str.strip())
    #             df['beds'] = split_cols[3].str.strip().where(split_cols[1].str.strip().str.startswith('★'), split_cols[2].str.strip())
    #             df['baths'] = split_cols[4].str.strip().where(split_cols[1].str.strip().str.startswith('★'), split_cols[3].str.strip())
    #             df.drop(columns=['name', 'neighbourhood_group', 'license'], inplace=True)
            
    #         # Convert Pandas DataFrame to Iceberg table format
            
    #         table_name = f"iceberg_demo.{os.path.splitext(filename)[0]}"
            
    #         if not catalog.table_exists(table_name):
    #             catalog.create_table(table_name, schema=schema, properties={"format-version": "2"})
            
    #         table = catalog.load_table(table_name)
    #         table.append(df)
            
    return "Iceberg tables created successfully"
