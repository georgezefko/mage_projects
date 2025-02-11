if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import bauplan


def get_tables(tbl,tbl2, namespace, branch, client):

    
    table = client.query(
    query=f'''
        SELECT 
            t1.customer_id,
            t1.customer_unique_id,
            t1.customer_zip_code_prefix,
            t2.geolocation_city as customer_city,
            t2.geolocation_state as customer_state
        FROM {tbl} t1
        left join {tbl2} t2 ON t1.customer_zip_code_prefix = t2.geolocation_zip_code_prefix
        ''',
    max_rows = 100,
    ref=branch,
    namespace = namespace
    )

    df = table.to_pandas()
    return df

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    NAMESPACE = kwargs['namespace']
    BAUPLAN_API =  os.getenv("BAUPLAN_API")
    
    CUSTOMER_TABLE_NAME = "customers"
    GEO_TABLE_NAME = "geolocation"
    # Estabslih connection with Bauplan client
    client = bauplan.Client(api_key=BAUPLAN_API)

    # Get the main branch
    main_branch = client.get_branch('main')
  
    table = client.query(
    query=f'''
        SELECT 
            t1.customer_id,
            t1.customer_unique_id,
            t1.customer_zip_code_prefix,
            t2.geolocation_city as customer_city,
            t2.geolocation_state as customer_state
        FROM {CUSTOMER_TABLE_NAME} t1
        left join {GEO_TABLE_NAME} t2 ON t1.customer_zip_code_prefix = t2.geolocation_zip_code_prefix
        ''',
    max_rows = 100,
    ref=main_branch,
    namespace = NAMESPACE
    )

    customers_dim = table.to_pandas()

    return customers_dim 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
