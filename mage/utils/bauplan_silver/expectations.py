import bauplan
from bauplan.standard_expectations import expect_column_no_nulls




@bauplan.expectation()
@bauplan.python('3.11',pip={'duckdb': '1.0.0'})
def customers_dim_checks(
 customers = bauplan.Model('fakerolist_silver.customers_dim'),

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
    check_one = con.execute(duplicates).arrow()
    assert expect_column_no_nulls(customers, 'customer_id')
    assert check_one == 0, "customers_dim has duplicates values"
    return True