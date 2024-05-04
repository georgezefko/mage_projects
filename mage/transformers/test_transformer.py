from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[Dict], *args, **kwargs):
    
from confluent_kafka import KafkaError
import json
import pandas as pd



consumer.subscribe(['clicks_topic'])

clicks_data = []
checkouts_data = []

try:
    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        topic = msg.topic()
        data = json.loads(msg.value().decode('utf-8'))

        
        clicks_data.append(data)
       

        

finally:
    consumer.close()

clicks_df = pd.DataFrame(clicks_data)


return clicks_df
