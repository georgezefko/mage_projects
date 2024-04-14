if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import json

from confluent_kafka import Producer

def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        key = msg.key().decode('utf-8') if msg.key() else None
        value = json.loads(msg.value().decode('utf-8')) if msg.value() else None
        click_id = value['click_id'] if value else None
        print(f"Produced event to topic {msg.topic()}: key = {key}, click_id = {click_id}")
    

@data_exporter
def export_data(data, *args, **kwargs):
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
   

    config = {
        'bootstrap.servers': 'kafka:9093',  # List of broker addresses
    
    }

    producer = Producer(
       config
    )

    # Produce data by selecting random values from these lists.
    topic = "mage.clicks"
   

    print(data[1][0]["checkout_id"])
    #clicks
    for i in range(len(data[0])):
        
        click_id =  data[0][i]["click_id"]
       
        producer.produce(topic,key=click_id, value=json.dumps(data[0][i]).encode('utf-8'), callback=delivery_callback)
        
        # Block until the messages are sent.
        producer.poll(10000)
        producer.flush()


