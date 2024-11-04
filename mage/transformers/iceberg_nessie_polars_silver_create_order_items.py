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
    products = data[4]
    sellers = data[5]
    items = data[6]

    products = products.drop("table_name")
    sellers =  sellers.drop("table_name")
    items = items.drop("table_name")

    # Join `order_items` with `products` on `product_id`
    order_items_with_products = items.join(products, on='product_id', how='left')

    # Join `order_items_with_products` with `sellers` on `seller_id`
    order_items_fact = order_items_with_products.join(sellers, on='seller_id', how='left')

    # Selecting and renaming columns as needed
    order_items_fact = order_items_fact.select([
        'order_id', 'product_id', 'seller_id', 'price', 'freight_value', 'product_quantity', 'shipping_limit_date',
        'product_category_name', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm',
        'seller_city', 'seller_state'
    ])
    
   
    return order_items_fact


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
