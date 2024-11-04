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

    orders_fact = data[0]
    order_items_fact = data[1]
    products_dim = data[2]
    sellers_dim = data[3]
    customers_dim = data[4]
    # Step 1: Join `order_fact` with `order_items_fact` on `order_id`
    sales_summary = orders_fact.join(order_items_fact, on='order_id', how='left')

    # Step 2: Join the result with `customer_dim` on `customer_id`
    sales_summary = sales_summary.join(customers_dim, on='customer_id', how='left')

    # Step 3: Join the result with `seller_dim` on `seller_id`
    sales_summary = sales_summary.join(sellers_dim, on='seller_id', how='left')

    # Step 4: Join the result with `product_dim` on `product_id`
    sales_summary = sales_summary.join(products_dim, on='product_id', how='left')

    print(sales_summary)
    # Step 5: Select relevant columns and perform necessary aggregations
    sales_summary_fact = sales_summary.select([
        'order_id',
        'customer_id',
        'product_id',
        'seller_id',
        'order_status',
        'order_purchase_timestamp',
        'review_score',
        'payment_value',
        'price',
        'freight_value',
        'product_quantity',
        'customer_city',
        'customer_state',
        'seller_city',
        'seller_state',
       'product_category_name'
    ])

    return sales_summary_fact



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
