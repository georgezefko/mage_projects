if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import bauplan


def generate_branch(new_branch, from_branch, client):
    """
    Helper function to create branches
    """
    if client.has_branch(new_branch):
        #raise ValueError("Branch already exists, please choose another name")
        pass
    else:
        client.create_branch(new_branch, from_ref=from_branch)

    assert client.has_branch(new_branch), "Branch not found"

    return client.get_branch(new_branch)


def generate_namespace(namespace, branch, client):
    """
    Helper function to create namespaces
    """
    # Use Bauplan client to create table
    if client.has_namespace(
            namespace=namespace,
            ref=branch,
        ):
        pass
    else:
        client.create_namespace(
            namespace=namespace,
            branch=branch,
        )
    
    return client.get_namespace(namespace, branch)
    

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here
    NAMESPACE = kwargs['namespace']
    BUCKET_NAME = kwargs['bucket_name']
    DATA_LAYER = kwargs['data_layer']
    BAUPLAN_API =  os.getenv("BAUPLAN_API")
    #Create branch name
    # Currently for Bauplan you need to create branch using username.branch_name
    BRANCH_NAME = f'zefko.{DATA_LAYER}'
    BRANCH_NAME_NEW = f'zefko.{DATA_LAYER}_test'
    TABLE_NAME = "orders_fct"
    # Estabslih connection with Bauplan client
    client = bauplan.Client(api_key=BAUPLAN_API)

    # Get the main branch
    main_branch = client.get_branch('main')
    
    



    # Generate Silver branch
    silver_branch = generate_branch(BRANCH_NAME, main_branch, client)

    # Create Namespaces
    #namespace = generate_namespace(NAMESPACE, bronze_branch, client)

    # Generate Branch from Silver
    wap_branch = generate_branch(BRANCH_NAME_NEW, silver_branch, client)

    # Generate the namespace in the new branch
    wap_namespace = generate_namespace(NAMESPACE, wap_branch, client)

    
    client.create_table(
    table=TABLE_NAME,
    search_uri='s3://alpha-hello-bauplan/fakerolist/*.parquet',
    namespace=wap_namespace,
    branch=wap_branch,
    # just in case the test table is already there for other reasons
    #verbose=False,
    replace=True  
    )

    # Import the data
    #client.import_data(
    #table=TABLE_NAME,
    #search_uri='s3://alpha-hello-bauplan/green-taxi/*.parquet',
    #namespace=wap_namespace,
    #branch=wap_branch,
    #verbose=False,
    #preview='on'
    #)


    table = client.query(
    query='SELECT * FROM customers',
    max_rows = 100,
    ref=main_branch,
    namespace = 'fakerolist'
    )

    df = table.to_pandas()
    return df
    #for table in client.get_tables(ref= 'main'):
    #    print(table.name, table.kind)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
