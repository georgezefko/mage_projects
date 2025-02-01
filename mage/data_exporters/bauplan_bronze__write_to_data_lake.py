if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
import bauplan
import pyarrow.fs as fs
import os
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
    

# Attempt #1: rely on environment variables
    # s3_fs = fs.S3FileSystem(
    # access_key='zefko',               # must match your MinIO user
    # secret_key='sparkTutorial',       # must match your MinIO password
    # endpoint_override='http://minio:9000',
    # scheme='http',
    # region='us-east-1'
    # )
    #info = s3_fs.get_file_info(fs.FileSelector('bauplan-demo'))
    #print('bucket info', info)
    #info_list = s3_fs.get_file_info(fs.FileSelector('iceberg-demo-nessie/ecommerce', recursive=True))
    #for info in info_list:
    #     print(info)
    # Specify your data exporting logic here
    NAMESPACE = kwargs['namespace']
    BUCKET_NAME = kwargs['bucket_name']
    DATA_LAYER = kwargs['data_layer']
    BAUPLAN_API =  os.getenv("BAUPLAN_API")
    #Create branch name
    # Currently for Bauplan you need to create branch using username.branch_name
    BRANCH_NAME = f'zefko.{DATA_LAYER}'

    # Estabslih connection with Bauplan client
    client = bauplan.Client(api_key=BAUPLAN_API)


    client.create_table(
        table='zefko_test_ecommerce',
        search_uri='s3://alpha-hello-bauplan/ecommerce/*.parquet',
        namespace=NAMESPACE,
        branch=BRANCH_NAME,
        # just in case the test table is already there for other reasons
        verbose=True,
        replace=True  
    )

    client.create_table(
        table='zefko_test',
        search_uri="s3://iceberg-demo-nessie/ecommerce/customers-bronze_c716e4e1-ccd8-40d7-beb3-9bdfcf1f6464/data/*.parquet",
        #namespace=NAMESPACE,
        branch=BRANCH_NAME,
        # just in case the test table is already there for other reasons
        verbose=True,
        replace=True  
    )

    # # Get the main branch
    # main_branch = client.get_branch('main')
    
    # # Create data layer bracnh
    # if client.has_branch(BRANCH_NAME):
    #     #raise ValueError("Branch already exists, please choose another name")
    #     pass
    # else:
    #     client.create_branch(BRANCH_NAME, from_ref='main')

    # assert client.has_branch(BRANCH_NAME), "Branch not found"

    # for data in data:
    #     table = data[0]
    #     schema = data[1]

    #     # Get table name, this is pipeline specific
    #     table_name = f"{table['table_name'][0]}-{DATA_LAYER}"
    #     print('table name', table_name)
    #     # Use Bauplan client to create table
    #     print('path',f"s3://{BUCKET_NAME}/{NAMESPACE}")
    #     if client.has_namespace(
    #             namespace=NAMESPACE,
    #             ref=BRANCH_NAME,
    #         ):
    #         pass
    #     else:
    #         client.create_namespace(
    #             namespace=NAMESPACE,
    #             branch=BRANCH_NAME,
    #         )
    #     #f"s3://{BUCKET_NAME}/{NAMESPACE}/*.parquet'"
    #     #s3://iceberg-demo-nessie/ecommerce/customers-bronze_c716e4e1-ccd8-40d7-beb3-9bdfcf1f6464/data/00000-0-cde1f5a3-b856-43b2-99a6-601d7299c564.parquet
    #     #"s3://iceberg-demo-nessie/ecommerce/customers-bronze_c716e4e1-ccd8-40d7-beb3-9bdfcf1f6464/data/*.parquet",
    #     client.create_table(
    #     table=table_name,
    #     search_uri=f"s3://{BUCKET_NAME}/{NAMESPACE}/*.parquet",
    #     namespace=NAMESPACE,
    #     branch=BRANCH_NAME,
    #     # just in case the test table is already there for other reasons
    #     verbose=True,
    #     replace=True  
    # )

