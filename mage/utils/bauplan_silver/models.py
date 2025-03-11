
import bauplan


# Create sellers dimension
@bauplan.python('3.11', pip={'polars': '1.19.0'})
@bauplan.model(materialization_strategy='REPLACE', name = 'sellers_dim')
def sellers_dim(
    sellers = bauplan.Model(
    'fakerolist.sellers',
    columns = ['seller_id', 'seller_zip_code_prefix'],
    ),
    geolocation = bauplan.Model('fakerolist.geolocation',
    columns = ['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state']
    ),
) :
    import polars as pl
    #from test_demo import demo
    t1 = pl.from_arrow(sellers)
    t2 = pl.from_arrow(geolocation)
    joined_df = t1.join(t2, left_on = 'seller_zip_code_prefix',right_on='geolocation_zip_code_prefix', 
                        how='left', coalesce=True).select([
    'seller_id',
    'seller_zip_code_prefix',
    'geolocation_state'
    ]
    ).to_arrow()
    #demo()
    return joined_df

# Create customers dimension
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy="REPLACE", name = 'customers_dim')
def customers_dim(
        customers = bauplan.Model('fakerolist.customers'),
        geolocation = bauplan.Model('fakerolist.geolocation'),

):
    import duckdb
    con = duckdb.connect()
    query = """
            SELECT 
                t1.customer_id,
                 t1.customer_unique_id,
                 t1.customer_zip_code_prefix,
                 t2.geolocation_city as customer_city,
                 t2.geolocation_state as customer_state
             FROM customers t1
             LEFT JOIN geolocation t2 ON t1.customer_zip_code_prefix = t2.geolocation_zip_code_prefix
    """
    data = con.execute(query).arrow()
    return data


# Create products dimension
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy="REPLACE", name = 'products_dim')
def products_dim(
        products = bauplan.Model('fakerolist.products'),
):
    import duckdb
    con = duckdb.connect()
    query = """
            SELECT 
            product_id, 
            product_category_name, 
            product_name_length, 
            product_description_length,
            product_weight_g, 
            product_length_cm, 
            product_height_cm, 
            product_width_cm
        FROM products    
     """
    data = con.execute(query).arrow()

    return data

# # Create Orders fact table
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model(materialization_strategy="REPLACE", name = 'orders_fct')
def orders_fct(
        orders = bauplan.Model('fakerolist.orders'),
        customers = bauplan.Model('fakerolist.customers'),
        payments = bauplan.Model('fakerolist.order_payments'),
        reviews = bauplan.Model('fakerolist.order_reviews'),
):
    import duckdb
    con = duckdb.connect()
    query = """
            SELECT 
            od.order_id,
            c.customer_id,
            od.order_status,
            p.payment_value,
            od.order_purchase_timestamp,
            r.review_score
        FROM orders od 
        INNER JOIN customers c on c.customer_id = od.customer_id
        LEFT JOIN payments p on p.order_id = od.order_id
        LEFT JOIN reviews r on r.order_id = od.order_id   
     """
    data = con.execute(query).arrow()

    return data

# Create Orders -Items fact table
@bauplan.python('3.11', pip={'polars': '1.19.0'})
@bauplan.model(materialization_strategy="REPLACE", name = 'order_item_fct')
def order_item_fct(
        products = bauplan.Model('fakerolist.products'),
        sellers = bauplan.Model('fakerolist.sellers'),
        items = bauplan.Model('fakerolist.order_items'),
):
    import polars as pl

    products = pl.from_arrow(products)
    sellers = pl.from_arrow(sellers)
    items = pl.from_arrow(items)

    # Join `items` with `products` on `product_id` (left join)
    order_items_with_products = items.join(products, on="product_id", how="left")

    # Join the result with `sellers` on `seller_id` (left join)
    order_items_fact = order_items_with_products.join(sellers, on="seller_id", how="left")

    # Select the desired columns
    order_items_fact = order_items_fact.select([
        'order_id', 'product_id', 'seller_id', 'price', 'freight_value', 'product_quantity', 'shipping_limit_date',
        'product_category_name', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm',
        'seller_city', 'seller_state'
    ]).to_arrow()

    return order_items_fact