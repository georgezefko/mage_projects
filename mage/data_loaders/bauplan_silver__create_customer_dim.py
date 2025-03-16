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
        MODEL = kwargs['model']
        MODEL_TEST = kwargs['model_test']
        SOURCE_BRANCH = kwargs['source_branch']
        print(SOURCE_BRANCH)

        # Estabslih connection with Bauplan client
        client = bauplan.Client(api_key=BAUPLAN_API)
        # Get the main branch
        medallion_branch_source = client.get_branch(SOURCE_BRANCH)
        print(medallion_branch_source)
        
        # Generate medallion branch
        medallion_branch = generate_branch(BRANCH_NAME, medallion_branch_source, client)
        print(medallion_branch)
        # Generate Branch from Silver
        wap_branch = generate_branch(BRANCH_NAME_NEW, medallion_branch, client)
        print(wap_branch)
        d = dirname(dirname(abspath(__file__)))
        run_state = client.run(
            project_dir = f"{d}/utils/{MODEL}",
            ref = wap_branch,
            namespace = NAMESPACE
        )
        if run_state.job_status != "SUCCESS":
            raise Exception("Error during bauplan_tutorial!")

        # run the quality checks
        d = dirname(dirname(abspath(__file__)))
        run_state = client.run(
            project_dir = f"{d}/utils/{MODEL_TEST}",
            ref = wap_branch,
            namespace = NAMESPACE
        )
        if run_state.job_status != "SUCCESS":
            raise Exception("Error during bauplan_tutorial!")

        client.merge_branch(
        source_ref = wap_branch,
        into_branch = medallion_branch
        )
        table = client.query(
        query=f'''
            SELECT * from sales_summary_fct
            ''',
        max_rows = 100,
        ref = medallion_branch,
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
