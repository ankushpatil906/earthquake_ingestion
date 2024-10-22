from google.cloud import storage
import requests
import json

# Initialize GCS client
client = storage.Client.from_service_account_json(r"C:\Users\Ankush\PycharmProjects\GCP_Session\bwt-learning-2024-431809-feb13ad022c2.json")

# Create GCS bucket
def create_bucket(bucket_name, project_id):
    """Create a Google Cloud Storage bucket."""
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        client.create_bucket(bucket, project=project_id)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def load_api_data_to_gcs_bucket(api_url, bucket_name, destination_file_name):
    """Fetch API data and upload it to a Google Cloud Storage bucket."""
    # Fetch API data
    api_response = requests.get(api_url)

    if api_response.status_code == 200:
        # Parse the API response as JSON
        api_data = api_response.json()

        # Get the bucket
        bucket = client.bucket(bucket_name)

        # Create a blob and upload the data as JSON
        blob = bucket.blob(destination_file_name)
        blob.upload_from_string(json.dumps(api_data), content_type='application/json')

        print(f"Data loaded to GCS at gs://{bucket_name}/{destination_file_name}")
    else:
        print(f"Failed to fetch data from the API. Status code: {api_response.status_code}")
