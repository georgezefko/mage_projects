from typing import Dict, List
from datetime import datetime, timedelta
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


STATE: Dict[str, List[Dict]] = {}

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Detect anomalies and auto‑confirm if 3 similar events
    for the same device+type occur within 1 hour.
    """
    anomalies: List[Dict] = []

    for msg in messages:
        device_id = msg["device_id"]
        ts = datetime.fromisoformat(msg["timestamp"])

        # Fetch and extend per‑device history
        history = STATE.get(device_id, [])

        def add_anomaly(a: Dict):
            history.append(a)
            anomalies.append(a)

            # Keep only the last 20 for this device
            STATE[device_id] = history[-20:]

            # Auto‑confirm if ≥3 in the past hour for *this type*
            similar = [
                h for h in history
                if h["anomaly_type"] == a["anomaly_type"]
                and ts - datetime.fromisoformat(h["timestamp"]) <= timedelta(hours=1)
            ]
            if len(similar) >= 3:
                a["is_confirmed"] = True
                a["confirmed_by"] = "system"
                a["description"] += " (Auto‑Confirmed)"

        # ---- Rule 1: High Energy Usage ----
        if msg.get("energy_usage", 0) > 4.5:
            add_anomaly({
                "device_id": device_id,
                "timestamp": msg["timestamp"],
                "anomaly_type": "high_energy_usage",
                "severity": "CRITICAL" if msg["energy_usage"] > 6.0 else "HIGH",
                "description": f"Energy usage {msg['energy_usage']} kWh (threshold: 4.5)",
                "is_confirmed": False,
                "confirmed_by": None,
            })

        # ---- Rule 2: High Temperature ----
        if msg.get("temperature", 0) > 28:
            add_anomaly({
                "device_id": device_id,
                "timestamp": msg["timestamp"],
                "anomaly_type": "high_temperature",
                "severity": "CRITICAL" if msg["temperature"] > 32 else "HIGH",
                "description": f"Temperature {msg['temperature']} °C (threshold: 28)",
                "is_confirmed": False,
                "confirmed_by": None,
            })

        # ---- Rule 3: Vibration Spike ----
        if msg.get("vibration", 0) > 3.0:
            add_anomaly({
                "device_id": device_id,
                "timestamp": msg["timestamp"],
                "anomaly_type": "high_vibration",
                "severity": "HIGH",
                "description": f"Vibration {msg['vibration']} m/s² (threshold: 3.0)",
                "is_confirmed": False,
                "confirmed_by": None,
            })

        # ---- Rule 4: Low Signal Strength ----
        signal = msg.get("signal_strength")
        if signal is not None and signal < 30:
            add_anomaly({
                "device_id": device_id,
                "timestamp": msg["timestamp"],
                "anomaly_type": "low_signal",
                "severity": "MEDIUM",
                "description": f"Signal strength {signal}% (threshold: 30)",
                "is_confirmed": False,
                "confirmed_by": None,
            })

    return anomalies
