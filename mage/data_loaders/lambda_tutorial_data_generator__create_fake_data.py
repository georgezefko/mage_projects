if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer
import os

# Function to generate fake telemetry data
def generate_telemetry(device_id):
    return {
        "device_id": device_id,
        "timestamp": datetime.utcnow().isoformat(),
        "energy_usage": round(random.uniform(0.5, 5.0)),  # Random energy usage in kWh
        "temperature": round(random.uniform(18.0, 30.0)),  # Random temperature in °C
        "vibration": round(random.uniform(0.1, 10.0), 1)  # Random vibration in mm/s
    }

# Function to generate fake event data
def generate_event(device_id):
    event_types = ["failure", "maintenance"]
    return {
        "device_id": device_id,
        "event_timestamp": datetime.utcnow().isoformat(),
        "event_type": random.choice(event_types)
    }

# Function to deliver reports (callback)
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    # Kafka configuration
    conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BROKERS'),  # Kafka broker address
        'client.id': 'iot-data-producer',
        "on_delivery": delivery_report
    }

    # Create a Kafka producer
    producer = Producer(conf)

    # Topics to send data to
    telemetry_topic = 'iot-telemetry'
    events_topic = 'iot-events'

    # Simulate IoT devices sending data
    device_ids = [f"device_{i}" for i in range(1, 11)]  # Simulate 10 devices
    while True:
        for device_id in device_ids:
            # Generate and send telemetry data
            telemetry_data = generate_telemetry(device_id)
            producer.produce(
                telemetry_topic,
                key=device_id,
                value=json.dumps(telemetry_data),
                on_delivery=delivery_report
            )

            # Generate and send event data (less frequently)
            if random.random() < 0.1:  # 10% chance of generating an event
                event_data = generate_event(device_id)
                producer.produce(
                    events_topic,
                    key=device_id,
                    value=json.dumps(event_data),
                    on_delivery=delivery_report
                )

            producer.poll(0)
            time.sleep(1)  # Send data every second
    producer.flush()  # Ensure all messages are sent before exiting.
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
