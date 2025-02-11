if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import bauplan

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    NAMESPACE = kwargs['namespace']
    BAUPLAN_API =  os.getenv("BAUPLAN_API")
    
    ORDERS_TABLE_NAME = "orders"
    CUSTOMERS_TABLE_NAME = "customers"
    PAYMENTS_TABLE_NAME = "order_payments"
    REVIEWS_TABLE_NAME = "order_reviews"

    # Estabslih connection with Bauplan client
    client = bauplan.Client(api_key=BAUPLAN_API)

    # Get the main branch
    main_branch = client.get_branch('main')




    table = client.query(
    query=f'''
        SELECT 
            od.order_id,
            c.customer_id,
            od.order_status,
            p.payment_value,
            od.order_purchase_timestamp,
            r.review_score
        FROM {ORDERS_TABLE_NAME} od 
        INNER JOIN {CUSTOMERS_TABLE_NAME} c on c.customer_id = od.customer_id
        LEFT JOIN {PAYMENTS_TABLE_NAME} p on p.order_id = od.order_id
        LEFT JOIN {REVIEWS_TABLE_NAME} r on r.order_id = od.order_id
        ''',
    max_rows = 100,
    ref=main_branch,
    namespace = NAMESPACE
    )

    orders_fct = table.to_pandas()
    

    return orders_fct

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
