
import bauplan

# Create customers dimension
@bauplan.python('3.11', pip={'polars': '1.19.0'})
@bauplan.model(materialization_strategy="REPLACE", name = 'sales_summary_fct')
def sales_summary_fct(
        orders_fact = bauplan.Model('fakerolist.orders_fct'),
        orders_item_fct = bauplan.Model('fakerolist.order_item_fct'),
        customers_dim = bauplan.Model('fakerolist.customers_dim'),
        sellers_dim = bauplan.Model('fakerolist.sellers_dim'),
        products_dim = bauplan.Model('fakerolist.products_dim'),

):
    import polars as pl


    orders_fact = pl.from_arrow(orders_fact)
    orders_item_fct = pl.from_arrow(orders_item_fct)
    customers_dim = pl.from_arrow(customers_dim)
    sellers_dim = pl.from_arrow(sellers_dim)
    products_dim = pl.from_arrow(products_dim)


    sales_summary = orders_fact.join(orders_item_fct, on='order_id', how='left')

    # Step 2: Join the result with `customer_dim` on `customer_id`
    sales_summary = sales_summary.join(customers_dim, on='customer_id', how='left')

    # Step 3: Join the result with `seller_dim` on `seller_id`
    sales_summary = sales_summary.join(sellers_dim, on='seller_id', how='left')

    # Step 4: Join the result with `product_dim` on `product_id`
    sales_summary = sales_summary.join(products_dim, on='product_id', how='left')

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
    ]).to_arrow()

    return sales_summary_fact

# # Create sellers dimension
# @bauplan.python('3.11', pip={'polars': '1.19.0'})
# @bauplan.model(materialization_strategy='REPLACE', name = 'customer_sales_summary')
# def customers_sales_summary(
#     customers = bauplan.Model(
#     'fakerolist.customers_dim',
#     ),
#     sales_fct = bauplan.Model(
#     'fakerolist.sales_summary_fct',
#     )
#     ) :
#     import polars as pl
#     sales_fct = pl.from_arrow(sales_fct)
#     customers = pl.from_arrow(customers)
#     # Total revenue per order
#     customer_sales_summary = sales_fct.groupby('customer_id').agg([
#     pl.count('order_id').alias('total_orders'),  # Count of orders per customer
#     pl.sum('payment_value').alias('total_spent'),  # Total amount spent by customer
#     pl.mean('review_score').alias('average_review_score'),  # Average review score
#     pl.max('order_purchase_timestamp').alias('recency_of_last_order')  # Most recent order timestamp
#     ])

#     # Join with customer_dim to get additional customer information
#     customer_sales_summary = customer_sales_summary.join(customers, on='customer_id', how='left').to_arrow()
    
#     return customer_sales_summary
