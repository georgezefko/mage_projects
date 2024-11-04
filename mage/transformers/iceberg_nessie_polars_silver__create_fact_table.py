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
    #this isn't a best practise because if the order change then the pipeline will break
    
    payments = data[0]
    orders = data[1]
    customers = data[2]
    reviews = data[3]

    orders = orders.drop("table_name")
    customers = customers.drop("table_name")
    payments = payments.drop("table_name")
    reviews = reviews.drop("table_name")

    # Join orders with customers and payments to create an order fact table
    order_fact = orders.join(customers, on='customer_id', how='inner')
    order_fact = order_fact.join(payments, on='order_id', how='left')
    order_fact = order_fact.join(reviews, on='order_id', how='left')

    order_fact = order_fact.select(['order_id', 'customer_id', 'order_status', 'payment_value', 'order_purchase_timestamp', 'review_score'])

    return order_fact


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
