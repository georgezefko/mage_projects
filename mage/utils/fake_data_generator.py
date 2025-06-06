### Class to generate data similar to Olist Ecommerce dataset

import os
from faker import Faker
import random
import boto3
import pandas as pd
from io import BytesIO

# Initialize faker
fake = Faker()

# Initialize S3 client
s3_client = boto3.client('s3')

# Set AWS S3 bucket name
S3_BUCKET_NAME = 'bucket-name'

def save_to_s3(df, table_name):
    """
    Save a Pandas DataFrame to AWS S3 in Parquet format.
    """
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    s3_path = f"data/{table_name}.parquet"
    s3_client.upload_fileobj(buffer, S3_BUCKET_NAME, s3_path)
    print(f"Uploaded {table_name} to S3 at {s3_path}")


def generate_customers(n):
    return {
        'customer_id': [fake.uuid4() for _ in range(n)],
        'customer_unique_id': [fake.uuid4() for _ in range(n)],
        'customer_zip_code_prefix': [fake.zipcode_in_state(state_abbr='NY') for _ in range(n)],
        'customer_city': [fake.city() for _ in range(n)],
        'customer_state': [fake.state_abbr() for _ in range(n)],
        'table_name': ['customers' for _ in range(n)]
    }


def generate_orders(customers_df, n):
    return {
        'order_id': [fake.uuid4() for _ in range(n)],
        'customer_id': random.choices(customers_df['customer_id'], k=n),
        'order_status': random.choices(['delivered', 'shipped', 'processing', 'canceled'], k=n),
        'order_purchase_timestamp': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(n)],
        'order_approved_at': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(n)],
        'order_delivered_carrier_date': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(n)],
        'order_delivered_customer_date': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(n)],
        'order_estimated_delivery_date': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(n)],
        'table_name': ['orders' for _ in range(n)]
    }

def generate_order_items(orders_df, products_df, sellers_df, n):
    return {
        'order_id': random.choices(orders_df['order_id'], k=n),  # Link to orders
        'product_id': random.choices(products_df['product_id'], k=n),  # Link to products
        'seller_id': random.choices(sellers_df['seller_id'], k=n),  # Link to sellers
        'shipping_limit_date': [fake.date_time_between(start_date='-2y', end_date='now') for _ in range(n)],
        'price': [round(random.uniform(10, 1000), 2) for _ in range(n)],  # Random price for products
        'freight_value': [round(random.uniform(5, 100), 2) for _ in range(n)],  # Random freight cost
        'order_item_id': [random.randint(1, 5) for _ in range(n)],  # Sequential number of the item in the order
        'product_quantity': [random.randint(1, 5) for _ in range(n)],  # Number of units ordered
        'table_name': ['order_items' for _ in range(n)]
    }

def generate_order_payments(orders_df, n):
    return {
        'order_id': random.choices(orders_df['order_id'], k=n),
        'payment_sequential': [random.randint(1, 3) for _ in range(n)],
        'payment_type': random.choices(['credit_card', 'cash', 'voucher', 'debit_card'], k=n),
        'payment_installments': [random.randint(1, 12) for _ in range(n)],
        'payment_value': [round(random.uniform(10, 1000), 2) for _ in range(n)],
        'table_name': ['payments' for _ in range(n)]
    }

def generate_order_reviews(orders_df, n):
    return {
        'review_id': [fake.uuid4() for _ in range(n)],
        'order_id': random.choices(orders_df['order_id'], k=n),
        'review_score': [random.randint(1, 5) for _ in range(n)],
        'review_comment_title': [fake.sentence(nb_words=6) for _ in range(n)],
        'review_comment_message': [fake.paragraph(nb_sentences=3) for _ in range(n)],
        'review_creation_date': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(n)],
        'review_answer_timestamp': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(n)],
        'table_name': ['reviews' for _ in range(n)]
    }


def generate_products(n):
    return {
        'product_id': [fake.uuid4() for _ in range(n)],
        'product_category_name': random.choices(['electronics', 'furniture', 'toys', 'clothing', 'sports'], k=n),
        'product_name_length': [random.randint(30, 100) for _ in range(n)],
        'product_description_length': [random.randint(100, 500) for _ in range(n)],
        'product_weight_g': [random.randint(500, 50000) for _ in range(n)],
        'product_length_cm': [random.randint(10, 100) for _ in range(n)],
        'product_height_cm': [random.randint(10, 100) for _ in range(n)],
        'product_width_cm': [random.randint(10, 100) for _ in range(n)],
        'table_name': ['products' for _ in range(n)]
    }


def generate_sellers(n):
    return{
        'seller_id': [fake.uuid4() for _ in range(n)],
        'seller_zip_code_prefix': [fake.zipcode_in_state('NY') for _ in range(n)],
        'seller_city': [fake.city() for _ in range(n)],
        'seller_state': [fake.state_abbr() for _ in range(n)],
        'table_name': ['sellers' for _ in range(n)]
    }


def generate_geolocation(n):
    return {
        'geolocation_zip_code_prefix': [fake.zipcode_in_state('NY') for _ in range(n)],
        'geolocation_lat': [fake.latitude() for _ in range(n)],
        'geolocation_lng': [fake.longitude() for _ in range(n)],
        'geolocation_city': [fake.city() for _ in range(n)],
        'geolocation_state': [fake.state_abbr() for _ in range(n)],
        'table_name': ['geolocation' for _ in range(n)]
    }


def main():
    n_rows = 10

    # Generate DataFrames
    customers = generate_customers(n_rows)
    orders = generate_orders(customers, n_rows)
    products = generate_products(n_rows)
    sellers = generate_sellers(n_rows)
    items = generate_order_items(orders, products, sellers, n_rows)
    payments = generate_order_payments(orders, n_rows)
    reviews = generate_order_reviews(orders, n_rows)
    geolocation = generate_geolocation(n_rows)

    datasets = {
        "customers": customers,
        "orders": orders,
        "products": products,
        "sellers": sellers,
        "order_items": items,
        "order_payments": payments,
        "order_reviews": reviews,
        "geolocation": geolocation
    }

    # Save each dataset to S3
    for name, df in datasets.items():
        save_to_s3(df, name)


if __name__ == "__main__":
    main()