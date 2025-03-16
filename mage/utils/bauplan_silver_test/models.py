import bauplan


# Create customers dimension
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
@bauplan.model()
def customers_dim_check(
        customers = bauplan.Model('fakerolist.customers_dim'),

):
    import duckdb
    con = duckdb.connect()
    duplicates = """
            SELECT 
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                count(*)
            FROM customers
            GROUP BY 1,2,3,4,5
            HAVING COUNT(*) > 1
    """

    nulls = """
            SELECT *
            FROM customers
            WHERE customer_id IS NULL
    """
    check_one = con.execute(duplicates).arrow()
    check_two = con.execute(nulls).arrow()

    assert check_one.num_rows == 0, "customers_dim has duplicates values"
    assert check_two.num_rows == 0, "customer id cannot be null"
    return check_one
