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
import logging
from mage.utils.logger import setup_logger


# Setup logger
default_log_level = "INFO"
log_level_str = os.environ.get("LOG_LEVEL", default_log_level)
log_level = getattr(logging, log_level_str.upper())
logger = setup_logger(__name__, log_level=log_level)



def create_or_get_branch(client, branch_name, main_branch):
    """
    Create a new branch or retrieve an existing one.
    """
    if client.has_branch(branch_name):
        logger.info(f"Branch '{branch_name}' already exists. Retrieving...")
        branch = client.get_branch(branch_name)
    else:
        logger.info(f"Creating new branch '{branch_name}' from '{main_branch}'...")
        branch = client.create_branch(branch_name, from_ref=main_branch)

    assert client.has_branch(branch), f"{branch_name} not found"
    return branch.name

def create_namespace_if_not_exists(client, namespace, branch):
    """
    Create a namespace within a branch if it doesn't already exist.
    """
    if client.has_namespace(namespace, branch):
        logger.warning(f"Namespace '{namespace}' already exists in branch '{branch}'. Skipping creation.")
        return
    else:
        logger.info(f"Creating namespace '{namespace}' in branch '{branch}'...")
        client.create_namespace(namespace, branch)

    assert client.has_namespace(namespace, branch), f"{namespace} not found in branch {branch}"

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
        MODEL = kwargs['model']
        MODEL_TEST = kwargs['model_test']

        # Estabslih connection with Bauplan client
        client = bauplan.Client(api_key=BAUPLAN_API)
        # Get the main branch
        main_branch = client.get_branch('main')
        

        #Generate new Branch
        wap_branch = create_or_get_branch(client, BRANCH_NAME, main_branch)

        # Generate namespace
        _ = create_namespace_if_not_exists(client, NAMESPACE, wap_branch)
        
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
        into_branch= main_branch
        )

        client.delete_branch(wap_branch)
        
        # test to ensure the table merge correctly
        table = client.query(
        query=f'''
            SELECT * from {test_table}
            ''',
        max_rows = 100,
        ref=main_branch,
        namespace = NAMESPACE
        )

        test = table.to_pandas()

        return table 


    

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
