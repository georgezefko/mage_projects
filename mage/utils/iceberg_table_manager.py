from pyiceberg.catalog import load_catalog
import pyarrow as pa
import polars as pl
import os

class IcebergTableManager:
    def __init__(self, catalog_type: str = "nessie"):

        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY")
        self.catalog_type = catalog_type.lower()

        if self.catalog_type not in ["rest", "nessie"]:
            raise ValueError("catalog_type must be either 'rest' or 'nessie'")


    def create_namespace_if_not_exists(self, catalog, namespace):
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

    def initialize_catalog(self, branch_name='main'):
        """Initialize the PyIceberg REST catalog to communicate with Nessie."""
        try:
            if not all([self.minio_endpoint, self.minio_access_key, self.minio_secret_key]):
                raise ValueError("MinIO environment variables are not properly set.")
            
            common_config = {
                    "s3.endpoint": f"http://{self.minio_endpoint}",
                    "s3.access-key-id": self.minio_access_key,
                    "s3.secret-access-key": self.minio_secret_key,
                    "s3.path-style-access": "true",
                }
            
            if self.catalog_type == "nessie":
                catalog = load_catalog(
                    name="nessie",
                    type="rest",
                    uri=f"http://nessie:19120/iceberg/{branch_name}",  # REST endpoint for Nessie
                    **common_config,
                    **{
                        "nessie.default-branch.name": 'main',  # Default Nessie branch
                    }
                )
                print(f"Catalog initialized successfully for branch: {branch_name}")
                return catalog
            
        except Exception as e:
            print(f"Error initializing catalog for branch '{branch_name}': {str(e)}")
            raise


    def create_iceberg_table(self, catalog, namespace, table_name, schema, location):
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
    
    # Function to convert Polars schema to PyArrow schema
    def polars_to_pyarrow_schema(self, polars_schema):
        schema_map = {
            pl.Utf8: pa.string(),
            pl.Int32: pa.int32(),
            pl.Int64: pa.int64(),
            pl.Float32: pa.float32(),
            pl.Float64: pa.float64(),
            pl.Boolean: pa.bool_(),
            pl.Datetime: pa.timestamp('ns'),
            pl.UInt32: pa.uint32(),
            pl.UInt64: pa.uint64(),
            pl.UInt8: pa.uint8(),  # You can add more mappings if needed
            pl.Date: pa.date32(),
            pl.Time: pa.time32('ms'),
            pl.Categorical: pa.dictionary(pa.int32(), pa.string())
        }


        fields = []
        for col, dtype in polars_schema.items():
            if dtype in schema_map:
                fields.append(pa.field(col, schema_map[dtype]))
            else:
                fields.append(pa.field(col, pa.string()))  # Default to string if type not found

        return pa.schema(fields)

    # Corrected function to convert Polars DataFrame to PyArrow Table with schema
    def polars_to_arrow_with_schema(self, df: pl.DataFrame, arrow_schema: pa.Schema) -> pa.Table:
        """Convert Polars DataFrame to PyArrow Table with proper type handling"""
        arrow_arrays = []
        
        for field in arrow_schema:
            col_name = field.name
            polars_series = df[col_name]
            arrow_type = field.type
            
            # Special handling for datetime types
            if isinstance(arrow_type, pa.TimestampType):
                # Convert Polars datetime to Arrow timestamp
                arrow_array = pa.array(
                    polars_series.cast(pl.Int64),  # Convert to nanoseconds
                    type=arrow_type
                )
            else:
                # Default conversion for other types
                try:
                    arrow_array = pa.array(polars_series.to_list(), type=arrow_type)
                except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
                    # Fallback to string if type conversion fails
                    arrow_array = pa.array(polars_series.cast(pl.Utf8).to_list())
                    
            arrow_arrays.append(arrow_array)
        
        return pa.Table.from_arrays(arrow_arrays, schema=arrow_schema)

