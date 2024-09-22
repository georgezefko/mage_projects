if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import polars as pl
from datetime import datetime

# Function to infer schema from a dictionary
def infer_schema_from_data(data):
    if not data:
        return None  # Return None if data is empty
    
    # Assuming the data is a list of dictionaries
    # Take the first dictionary
    schema = {}
    for column, value in data.items():
        first_value = value[0]
        if isinstance(first_value, str):
            schema[column] = pl.Utf8
        elif isinstance(first_value, int):
            schema[column] = pl.Int32
        elif isinstance(first_value, float):
            schema[column] = pl.Float64
        elif isinstance(first_value, datetime):
            schema[column] = pl.Datetime
        else:
            schema[column] = pl.Object  # Default to Object if type is not detected

    return schema

# Convert list of dictionaries to Polars DataFrame and infer schema
def process_data(data):
    # Infer schema
    schema = infer_schema_from_data(data)
    
    # Convert the list of dictionaries to Polars DataFrame
    df = pl.DataFrame(data)
    
    
    # Return the DataFrame and schema
    return df, schema

@transformer
def transform_data(data, *args, **kwargs):
     
    df, schema = process_data(data)  # Process the first list (table)
        
    return [df, schema]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
