if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import polars as pl

@transformer
def transform(sales_summary_fact, silver_data, *args, **kwargs):
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
    customers = silver_data[4]
    # Total revenue per order
    customer_sales_summary = sales_summary_fact.groupby('customer_id').agg([
    pl.count('order_id').alias('total_orders'),  # Count of orders per customer
    pl.sum('payment_value').alias('total_spent'),  # Total amount spent by customer
    pl.mean('review_score').alias('average_review_score'),  # Average review score
    pl.max('order_purchase_timestamp').alias('recency_of_last_order')  # Most recent order timestamp
    ])

    # Join with customer_dim to get additional customer information
    customer_sales_summary = customer_sales_summary.join(customers, on='customer_id', how='left')


    return customer_sales_summary


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
