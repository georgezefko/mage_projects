if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
import os
import bauplan
import polars as pl
import pyarrow
from os.path import dirname, abspath

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

    branch = client.get_branch(new_branch)
    return branch.name

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
        DATA_LAYER = kwargs['data_layer']
        BRANCH_NAME = f'zefko.{DATA_LAYER}'
        BRANCH_NAME_NEW = f'zefko.{DATA_LAYER}_test'

        # Estabslih connection with Bauplan client
        client = bauplan.Client(api_key=BAUPLAN_API)
        # Get the main branch
        main_branch = client.get_branch('main')
        
        # Generate Silver branch
        silver_branch = generate_branch(BRANCH_NAME, main_branch, client)

        # Generate Branch from Silver
        wap_branch = generate_branch(BRANCH_NAME_NEW, silver_branch, client)

        d = dirname(dirname(abspath(__file__)))
        run_state = client.run(
            project_dir = f"{d}/utils/bauplan_silver",
            ref = wap_branch,
            namespace = NAMESPACE
        )
        if run_state.job_status != "SUCCESS":
            raise Exception("Error during bauplan_tutorial!")

        table = client.query(
        query=f'''
            SELECT * from orders_fct
            ''',
        max_rows = 100,
        ref=wap_branch,
        namespace = NAMESPACE
        )

        test = table.to_pandas()
        return test 


    

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
