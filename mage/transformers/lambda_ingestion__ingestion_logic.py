from typing import Dict, List

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
import pandas as pd
from minio import Minio
from io import BytesIO
from datetime import datetime
import os

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Processes messages from multiple topics:
    - iot-telemetry → telemetry-bronze bucket
    - iot-events → event-bronze bucket
    """
    # MinIO configuration
    MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
    
    client = Minio(
        "minio:9000",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Ensure bucket exists
    bucket_name = "iot-bronze"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    for msg in messages:
        try:
            # Handle both raw data and already parsed JSON
            raw_data = msg['data']
            
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode('utf-8')
            
            # Parse JSON if it's a string, otherwise use directly
            if isinstance(raw_data, str):
                try:
                    data = json.loads(raw_data)
                except json.JSONDecodeError:
                    data = raw_data  # fallback to string
            else:
                data = raw_data
            
            # Convert to DataFrame handling multiple cases
            if isinstance(data, dict):
                df = pd.DataFrame([data])  # single record
            elif isinstance(data, list):
                df = pd.DataFrame(data)  # multiple records
            else:
                df = pd.DataFrame({'value': [data]})  # fallback
            
            # Ensure device_id exists
            if 'device_id' not in df.columns:
                raise ValueError("Missing device_id in message data")
            
            device_id = df.iloc[0]["device_id"]
            date_str = datetime.now().strftime("%Y-%m-%d")
            
            # Topic routing
            topic = msg['metadata']['topic']
            if topic == 'iot-telemetry':
                prefix = "telemetry"
            elif topic == 'iot-events':
                prefix = "events"
            else:
                print(f"Unknown topic: {topic}")
                continue
             
            # Write to MinIO
            parquet_file = BytesIO()
            df.to_parquet(parquet_file, index=False)
            parquet_file.seek(0)
            
            # Unique filename with timestamp
            timestamp = datetime.now().strftime('%H%M%S')
            object_path = f"{prefix}/date={date_str}/device_id={device_id}/data_{timestamp}.parquet"
            
            client.put_object(
                bucket_name,
                object_path,
                parquet_file,
                length=parquet_file.getbuffer().nbytes,
                content_type="application/parquet"
            )
            
            print(f"Successfully processed {topic} message for device {device_id}")
            
        except Exception as e:
            print(f"Error processing message: {type(e).__name__}: {e}")
            print(f"Problematic message content: {msg.get('data')}")
            continue

    return


