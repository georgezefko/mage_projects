if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import bauplan
import polars as pl
import pyarrow
from os.path import dirname, abspath
import logging
import time
from mage.utils.logger import setup_logger


# Setup logger
default_log_level = "INFO"
log_level_str = os.environ.get("LOG_LEVEL", default_log_level)
log_level = getattr(logging, log_level_str.upper())
logger = setup_logger(__name__, log_level=log_level)



def generate_custom_branch_name(name: str):
        """
        Generate a branch name with the format: zefko.bronze_timestamp.
        """
        # Current timestamp
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        
        # Create the branch name
        branch_name = f"{name}_{timestamp}"
        return branch_name


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

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here
    NAMESPACE = kwargs['namespace']
    BAUPLAN_API =  os.getenv("BAUPLAN_API")
    DATA_LAYER = kwargs['data_layer']
    BRANCH_NAME = f'zefko.{DATA_LAYER}'
    MODEL = kwargs['model']

    # Estabslih connection with Bauplan client
    client = bauplan.Client(api_key=BAUPLAN_API)
    # Get the main branch
    main_branch = client.get_branch('main')
    
    # Get the branch name
    branch_name = generate_custom_branch_name(BRANCH_NAME)
    
    #Generate new Branch
    wap_branch = create_or_get_branch(client, branch_name, main_branch)

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

    client.merge_branch(
    source_ref = wap_branch,
    into_branch= main_branch
    )

    client.delete_branch(wap_branch)
    
    return 