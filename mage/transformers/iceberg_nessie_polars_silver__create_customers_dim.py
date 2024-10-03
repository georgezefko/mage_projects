if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import polars as pl

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    customers = data[2]
    geolocation = data[7]

    customers = customers.drop("table_name")
    geolocation =  geolocation.drop("table_name")


    customer_dim = customers.join(
    geolocation, 
    left_on='customer_zip_code_prefix', 
    right_on='geolocation_zip_code_prefix', 
    how='left'
    )

    # Selecting and renaming columns as needed
    customer_dim = customer_dim.select([
    'customer_id', 
    'customer_unique_id', 
    'customer_zip_code_prefix', 
    pl.col('geolocation_city').alias('customer_city'),  # Alias geolocation_city to customer_city
    pl.col('geolocation_state').alias('customer_state')  # Alias geolocation_state to customer_state
    ])

    return customer_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
