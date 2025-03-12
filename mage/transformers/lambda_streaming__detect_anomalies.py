from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Template code for a transformer block.

    Args:
        messages: List of messages in the stream.

    Returns:
        Transformed messages
    """
    # Specify your transformation logic here
    anomalies = []
    for message in messages:
        # Detect anomalies based on thresholds
        if message['energy_usage'] > 4.5:  # Example threshold for energy usage
            anomalies.append({
                "device_id": message['device_id'],
                "timestamp": message['timestamp'],
                "anomaly_type": "high_energy_usage",
                "description": f"Energy usage exceeded threshold: {message['energy_usage']} kWh"
            })
        if message['temperature'] > 28:  # Example threshold for temperature
            anomalies.append({
                "device_id": message['device_id'],
                "timestamp": message['timestamp'],
                "anomaly_type": "high_temperature",
                "description": f"Temperature exceeded threshold: {message['temperature']} °C"
            })
        print('anomalies',anomalies)
        return anomalies
