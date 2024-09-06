from minio import Minio
import io
import os

# Configuration
@custom
def test(*args,**kwargs):
    
    MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
    MINIO_ENDPOINT = "minio:9000"
    BUCKET_NAME = "iceberg-demo-nessie"

    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Create a test object
    test_data = b'0' * 10 * 1024 * 1024  # 10MB data for multipart upload test

    # Ensure bucket exists
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")
    else:
        print(f"Bucket '{BUCKET_NAME}' already exists.")

    # Upload the object
    try:
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name="test_multipart_upload",
            data=io.BytesIO(test_data),
            length=len(test_data)
        )
        print("Multipart upload test completed successfully.")
    except Exception as e:
        print(f"Error during multipart upload test: {e}")
