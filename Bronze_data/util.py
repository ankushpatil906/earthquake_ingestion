from google.cloud import storage
import requests
import json

# Initialize GCS client
# This client allows interaction with Google Cloud Storage using the provided service account credentials
client = storage.Client.from_service_account_json(
    r"C:\Users\Ankush\PycharmProjects\GCP_Session\bwt-learning-2024-431809-feb13ad022c2.json")


# Create GCS bucket
def create_bucket(bucket_name, project_id):
    """
    Creates a Google Cloud Storage bucket if it does not already exist.

    Args:
    - bucket_name: Name of the GCS bucket to create.
    - project_id: Google Cloud Project ID under which the bucket should be created.
    """
    # Reference the bucket
    bucket = client.bucket(bucket_name)

    # Check if the bucket exists, if not, create it
    if not bucket.exists():
        client.create_bucket(bucket, project=project_id)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")


def load_api_data_to_gcs_bucket(api_url, bucket_name, destination_file_name):
    """
    Fetches data from an API endpoint and uploads it to a Google Cloud Storage bucket as a JSON file.

    Args:
    - api_url: The URL of the API to fetch data from.
    - bucket_name: The name of the GCS bucket where the data will be stored.
    - destination_file_name: The name of the file to be saved in GCS.
    """
    # Fetch API data using HTTP GET request
    api_response = requests.get(api_url)

    # Check if the API call was successful
    if api_response.status_code == 200:
        # Parse the API response as JSON
        api_data = api_response.json()

        # Reference the bucket
        bucket = client.bucket(bucket_name)

        # Create a blob object in GCS where the data will be stored
        blob = bucket.blob(destination_file_name)

        # Upload the JSON data to the blob in GCS
        blob.upload_from_string(json.dumps(api_data), content_type='application/json')

        print(f"Data loaded to GCS at gs://{bucket_name}/{destination_file_name}")
    else:
        # If the API request failed, print the status code
        print(f"Failed to fetch data from the API. Status code: {api_response.status_code}")


def read_data_from_gcs_bucket(bucket_name, source_file_name):
    """
    Reads data from a Google Cloud Storage bucket and returns it as a Python object (assumes JSON data).

    Args:
    - bucket_name: The name of the GCS bucket from which to read data.
    - source_file_name: The name of the file (blob) in the bucket to read from.

    Returns:
    - The parsed JSON data as a Python object (typically a dict or list).
    """
    # Initialize a new client (optional, but good for modular code)
    client = storage.Client()

    # Reference the bucket and the file (blob) within the bucket
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)

    # Download the file content as a string (expects text content, like JSON)
    source_data = blob.download_as_text()

    # Parse the JSON content into a Python object
    data = json.loads(source_data)

    # Optionally, you can print the data for debugging
    # print(f"Data read from GCS at gs://{bucket_name}/{source_file_name}")

    return data
