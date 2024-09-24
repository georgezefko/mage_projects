if "custom" not in globals():
    from mage_ai.data_preparation.decorators import custom
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test
import subprocess
from mage_ai.data_preparation.models.pipeline import Pipeline
from alsense_pipelines.utils.logger import setup_logger
import logging
import os

default_log_level = "INFO"
log_level_str = os.environ.get("LOG_LEVEL", default_log_level)
log_level = getattr(logging, log_level_str.upper())
logger = setup_logger(__name__, log_level=log_level)


def execute_command(command):
    try:
        # Execute the command
        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Wait for the command to complete
        output, error = process.communicate()

        # Decode output and error to utf-8
        output = output.decode("utf-8")
        error = error.decode("utf-8")

        # Get the exit code
        exit_code = process.returncode

        return output, error, exit_code

    except Exception as e:
        # Handle exceptions
        return None, str(e), -1


@custom
def remove_logs(*args, **kwargs):

    project_path = kwargs["project_path"]
    pipeline_uuids = Pipeline.get_all_pipelines(project_path)
    batch_uuids = kwargs["batch_uuids"]

    # Due to bug in Mage streaming pipelines need restarting after deleting logs
    batch_pipeline_uuids = [uuid for uuid in pipeline_uuids if uuid in batch_uuids]

    # clean old logs for batch pipelines
    for uuid in batch_pipeline_uuids:
        clean_logs = f"mage clean-old-logs {project_path} --pipeline-uuid {uuid}"
        output, error, exit_code = execute_command(clean_logs)
        logger.info(f"Clean logs outputs: {output}, {error}, {exit_code}")
    # clean chached block outputs
    clean_cached_outputs = f"mage clean-cached-variables {project_path}"
    output_cached, error_cached, exit_cached = execute_command(clean_cached_outputs)
    logger.info(f"Clean cached outputs: {output_cached}, {error_cached}, {exit_cached}")

    return
