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

    # Total revenue per order
    sales_fact = data.groupby('order_id').agg([
        pl.sum('payment_value').alias('total_revenue'),
        pl.count('order_id').alias('number_of_orders'),
        pl.mean('payment_value').alias('avg_payment_value'),
    ])

    # Aggregate number of orders and total revenue per customer
    customer_sales_fact = data.groupby('customer_id').agg([
        pl.sum('payment_value').alias('total_revenue_per_customer'),
        pl.count('order_id').alias('total_orders_per_customer'),
        pl.mean('payment_value').alias('avg_order_value_per_customer')
    ])

    return [sales_fact, customer_sales_fact]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
