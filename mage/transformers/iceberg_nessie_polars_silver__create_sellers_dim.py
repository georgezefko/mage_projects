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
    sellers = data[5]
    geolocation = data[7]

    sellers = sellers.drop("table_name")
    geolocation =  geolocation.drop("table_name")


    seller_dim = sellers.join(
    geolocation, 
    left_on='seller_zip_code_prefix', 
    right_on='geolocation_zip_code_prefix', 
    how='left'
    )

    # Selecting and renaming columns as needed
    seller_dim = seller_dim.select([
    'seller_id', 
    'seller_zip_code_prefix', 
    pl.col('geolocation_city').alias('seller_city'),  # Alias geolocation_city to seller_city
    pl.col('geolocation_state').alias('seller_state')  # Alias geolocation_state to seller_state
    ])

    return seller_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
