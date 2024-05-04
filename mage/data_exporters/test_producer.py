if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
from kafka import KafkaProducer
from random import random
import json


@data_exporter
def export_data(limit, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    

    topic = 'test_mage_internal'
    producer = KafkaProducer(
        bootstrap_servers='kafka:9093',
    )


    
    for i in range(34,limit):
        data = {
            'title': 'test_title',
            'director': 'Bennett Miller',
            'year': '2022',
            'rating': i,
        }
        producer.send(topic, json.dumps(data).encode('utf-8'))

   



