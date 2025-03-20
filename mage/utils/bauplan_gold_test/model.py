import bauplan


# Create customers dimension
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model()
def customers_dim_check(
        customers = bauplan.Model('fakerolist_gold.sales_summary_fct'),

):
    import duckdb
    con = duckdb.connect()
    duplicates = """
            SELECT 
                customer_id,
                order_id,
                order_purchase_timestamp,
                product_category_name,
                product_quantity,
                count(*)
            FROM customers
            GROUP BY 1,2,3,4,5
            HAVING COUNT(*) > 1
    """

    check_one = con.execute(duplicates).arrow()

    assert check_one.num_rows == 0, "customers_dim has duplicates values"
    
    
    return check_one
