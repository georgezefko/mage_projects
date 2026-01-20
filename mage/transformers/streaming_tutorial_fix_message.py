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
    rows = []

    for msg in messages:          # messages is a LIST
        data = msg["data"]        # msg is a dict

        rows.append({
            "device_id": data["device_id"],
            "ts": data["timestamp"],
            "energy_usage": data["energy_usage"],
            "temperature": data["temperature"],
            "vibration": data["vibration"],
            "signal_strength": data["signal_strength"],
        })

    return rows

    