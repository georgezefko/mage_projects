if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import bauplan
import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    client = bauplan.Client(api_key="bpln-sdk-6nat4FBXnc3V36XvMPJs2hZMqfnDKiuvYhuredjyeViv")


    main_branch = client.get_branch('main')

    table_taxi = client.get_table(
    table='taxi_fhvhv',
    ref= main_branch,
    )

    table_zones = client.get_table(
    table='taxi_zones',
    ref= main_branch,
    )

    query = client.query(
        query = "SELECT * FROM taxi_fhvhv WHERE pickup_datetime = '2023-01-01T00:00:00-05:00'",
        ref= main_branch,
    )

    df = query.to_pandas()

    #zones = table_zones.to_pandas()
    if client.has_branch('zefko.first_test'):
        raise ValueError("Branch already exists, please choose another name")

    client.create_branch('zefko.first_test', from_ref='main')

    assert client.has_branch('zefko.first_test'), "Branch not found"


    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
