if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
import time
import pandas as pd
import pyarrow as pa
from pyarrow import schema, field
from minio import Minio, S3Error
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType, NestedField
import io



def create_dummy_data():
    data = {
        "id": [1, 2, 3, 4],
        "description": ["Nice place", "Cozy spot", "Luxurious stay", "Dorm"],
        "reviews": ["Great!", "Good", "Excellent", "Fair"],
        "bedrooms": ["2", "1", "3", "1"],
        "beds": ["2", "1", "3", "7"],
        "baths": ["1", "1", "2", "1"],
    }
    df = pd.DataFrame(data)

    return df



@data_loader
def main(*args, **kwargs):
    

    # Read CSV to Arrow table
    pandas_table = create_dummy_data()

    return pandas_table

