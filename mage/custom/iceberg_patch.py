if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test




import os
from time import time
# arrow imports
import pyarrow as pa
import pyarrow.parquet as pq
# data catalog imports
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import IsNull
import monkey_patch
from pyiceberg_patch_nessie import NessieCatalog
# slack imports
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
# preload the friendlywords package
import friendlywords as fw
fw.preload()


# name of the table in the Nessie catalog
# we have only one table, to which we append the new rows
# as they come in the source bucket
TABLE_NAME = 'customer_data_log'


# decorator
def measure_func(func):
    """
    
    This function shows the execution time of the function object passed -
    this is convenient when debugging to keep track of where the program
    spends its time.
    
    """
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func


@measure_func
def read_rows_into_arrow(record) -> pa.Table:
    """
    
    Read a parquet file in S3 into an arrow table.
    
    """
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    # make sure we are trying to read a parquet file
    assert key.endswith('.parquet'), "Only parquet files are supported"
    s3_path = f"s3://{bucket}/{key}"
    cnt_table = pq.read_table(s3_path)
    print("Table has {} rows".format(cnt_table.num_rows))
    
    return cnt_table


@measure_func
def create_table_if_not_exists(catalog, table_name, branch, schema, datalake_location):
    """
    
    Create a table in the Nessie catalog if it does not exist by first checking
    the tables in the main branch, and then creating the table with the schema.
    
    Note that we return True if the table is created, False otherwise (in theory,
    just on the first run the table should get created).
    
    """
    tables = catalog.list_tables('main')
    print("Tables in the catalog in {}: {}".format(branch, tables))
    # this is the result of the list_tables call when a table is there
    # [('main@2031105876d7dbd8f57fcff4820863edebf25fdbb7b75b184a915acd8cac5484', 'customer_data_log')]
    if any([table_name == t[1] for t in tables]):
        return False
    
    rt_0 = catalog.create_table(
        identifier=('main', table_name),
        schema=schema,
        location=datalake_location,
    )
    tables = catalog.list_tables('main')
    print("Now Tables in the catalog in {}: {}".format(branch, tables))
    
    return True


@measure_func
def create_branch_from_main(catalog):
    """
    
    Create a random branch with a human-readable name starting from main.
    
    """
    
    branch_name = fw.generate('ppo', separator='-')
    catalog.create_branch(branch_name, 'main')

    return branch_name


@measure_func
def append_rows_to_table_in_branch(
    catalog, 
    table_name, 
    branch_name, 
    arrow_table
    ):
    """
    
    Add the new rows in the arrow table to the table in the branch.
    
    """
    try:
        _table = catalog.load_table((branch_name, table_name))
        _table.append(arrow_table)
    except Exception as e:
        print("Error appending rows to table: {}".format(e))
        return False
    
    return True

    
@custom
def lambda_handler(*args, **kwargs):
    """
    
    This is the entry point for the lambda function. The function is triggered
    by an S3 event (every time a new file is uploaded to the source bucket).
    
    The function reads the parquet file from the source bucket, opens a branch in the data catalog,
    and commits the changes there.
    
    It then performs a quality check (simulating a furthere processing step in the pipeline) and, if the
    check is successful, merge the branch into the main table.
    
    """
    # print a copy of the event in cloudwatch
    # for debugging purposes
    print(event)
    # make sure the environment variables are set
    assert os.environ['SOURCE_BUCKET'], "Please set the SOURCE_BUCKET environment variable"
    assert os.environ['LAKE_BUCKET'], "Please set the LAKE_BUCKET environment variable"
    # get the records from the event
    records = event['Records']
    if not records:
        print("No records found in the event")
        return None
    
    # this is needed so that pynessie can write to the local filesystem
    # some configuration for the user...
    os.environ['NESSIEDIR'] = '/tmp'
    # initialize the Nessie catalog
    catalog = load_catalog(
        name='default',
        type='nessie',
        endpoint=os.environ['NESSIE_ENDPOINT'],
        default_branch='main'
    )    
    # if the target table does not exist in main (first run), create it
    datalake_location = 's3://{}'.format(os.environ['LAKE_BUCKET'])
    # loop over the records, even if it should be 1
    for record in records:
        # get the new rows out as an arrow table
        arrow_table = read_rows_into_arrow(record)
        # make sure the table in main is there (it won't be there at the first run)
        # we use the current schema to create the table for simplicity
        is_created = create_table_if_not_exists(
            catalog, 
            TABLE_NAME, 
            'main', 
            arrow_table.schema, 
            datalake_location
        )
        print("Table created: {}".format(is_created))
        # create a new branch in the catalog from main
        branch_name = create_branch_from_main(catalog)
        print("Branch created: {}".format(branch_name))
        # write the new rows to the branch
        _appended = append_rows_to_table_in_branch(
            catalog, 
            TABLE_NAME, 
            branch_name, 
            arrow_table
        )
        if not _appended:
            print("Error appending rows to branch")
            return None
        
        print("Quality check passed, merging branch: {}".format(branch_name))
        catalog.merge(branch_name, 'main')
        catalog.drop_branch(branch_name)
        print("Branch merged and dropped")
       

    return None