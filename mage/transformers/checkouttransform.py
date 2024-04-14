from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
from confluent_kafka import Consumer,KafkaError
import json
import pandas as pd
import psycopg2


def checkout(messages: List[Dict]):
    checkouts_data = []
    for msg in messages:
        
        checkouts_data.append(msg)

    checkouts_df = pd.DataFrame(checkouts_data)

    return checkouts_df

def get_users():
    # Fetch user data from Postgres and put it in a DataFrame
    conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="postgres",
        )
    
    users_df = pd.read_sql("SELECT * FROM commerce.users", conn)
    conn.close()
    return users_df

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    
    # Assume checkouts_df and clicks_df are already populated
    print('no of messages',len(messages))
    checkouts_df = checkout(messages)
    print(checkouts_df.info())
    users_df = get_users()
    # Join checkouts with users
    joined_df = pd.merge(checkouts_df, users_df, left_on='user_id', right_on='id', how='inner')


    # Left join with clicks
    print('joined',joined_df)
    return joined_df

