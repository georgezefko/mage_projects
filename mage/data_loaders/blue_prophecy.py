from mage_ai.streaming.sources.base_python import BasePythonSource
from typing import Callable

if 'streaming_source' not in globals():
    from mage_ai.data_preparation.decorators import streaming_source
import os

from quixstreams import Application
from quixstreams.context import message_key

@streaming_source
class CustomSource(BasePythonSource):
    def init_client(self):
        """
        Implement the logic of initializing the client.
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

        # Get the temperature readings topic
        temperature_readings_topic = self.app.topic(name="temperature_readings")

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
        

        # Continuously read and process records
        while True:
            self.app.run(sdf)
