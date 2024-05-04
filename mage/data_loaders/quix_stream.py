import os
from typing import Callable
from quixstreams import Application
from quixstreams.context import message_key
from mage_ai.streaming.sources.base_python import BasePythonSource
from mage_ai.data_preparation.decorators import streaming_source


@streaming_source
class CustomSource(BasePythonSource):
    def __init__(self):
        super().__init__()
        self.app = None

    def init_client(self):
        """
        Implement the logic of initializing the Quixstreams application.
        """
        self.app = Application(
            broker_address=os.environ.get("KAFKA_BROKERS", "localhost:9092"),
            consumer_group="temperature_alerter",
            auto_offset_reset="earliest",
        )

    def batch_read(self, handler: Callable):
        """
        Batch read the messages from the source and use handler to process the messages.
        """
        if self.app is None:
            raise ValueError("Quixstreams application not initialized. Call init_client first.")

        temperature_readings_topic = self.app.topic(name="temperature_readings")
        alerts_topic = self.app.topic(name="alerts")

        def should_alert(window_value):
            if window_value >= 90:
                print(f"Alerting for MID {message_key()}: Average Temperature {window_value}")
                return True

        sdf = self.app.dataframe(topic=temperature_readings_topic)
        sdf = sdf.apply(lambda data: data["Temperature_C"])
        sdf = sdf.hopping_window(duration_ms=5000, step_ms=1000).mean().current()
        sdf = sdf.apply(lambda result: round(result["value"], 2)).filter(should_alert)
        sdf = sdf.to_topic(alerts_topic)

        # Handler function for processing records
        def process_records(records):
            # You might need to format records based on the handler you're using
            for record in records:
                handler(record)

        # Continuously read and process records
        while True:
            records = sdf.collect()
            if records:
                process_records(records)
