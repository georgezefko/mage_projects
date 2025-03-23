import bauplan
from bauplan.standard_expectations import expect_column_no_nulls



@bauplan.expectation()
@bauplan.python('3.11',pip={'duckdb': '1.0.0'})
def customers_sales_checks(
 customers = bauplan.Model('fakerolist_gold.customer_sales_summary'),

):
    assert expect_column_no_nulls(customers, 'customer_id')
    return True