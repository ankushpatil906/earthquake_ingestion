import apache_beam as beam
from apache_beam.io import WriteToParquet
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import json
import requests
from datetime import datetime
import os
from google.cloud import storage
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam import Row
import pyarrow as pa
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\Ankush\PycharmProjects\GCP_Session\bwt-learning-2024-431809-a14e88a78b4e.json'

logging.basicConfig(level=logging.INFO)
options = PipelineOptions(save_main_session=True)
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'bwt-learning-2024-431809'
google_cloud_options.job_name = 'api-data-to-gcs4'
google_cloud_options.region = "asia-east1"
google_cloud_options.temp_location = 'gs://eartquake_analysis/temp_loc'
google_cloud_options.staging_location = 'gs://eartquake_analysis/stage_loc'

options.view_as(StandardOptions).runner = 'DataflowRunner'
api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
bucket_name = "eartquake_analysis"
current_date = datetime.now().strftime('%Y%m%d')
silver_output_path = f'gs://{bucket_name}/dataflow/silver/{current_date}/earthquake_raw_data'
destination_file_name = f"dataflow/landing/{current_date}/earthquake_raw_data"
table_spec = 'bwt-learning-2024-431809-433112:earthquake_ingestion_dataset.earthquake_dataflow_table'

class FetchAndUploadDataToGCS(beam.DoFn):
    def __init__(self, api_url, bucket_name, destination_file_name):
        self.api_url = api_url
        self.bucket_name = bucket_name
        self.destination_file_name = destination_file_name

    # def start_bundle(self):
    #     # Initialize the storage client once per bundle
    #     self.storage_client = storage.Client()

    def fetch_api_data(self,api_url):
        try:
            logging.info(f">> Fetching raw data from an API: {api_url}")
            response = requests.get(api_url)
            response.raise_for_status()  # Raise an error for bad status codes
            logging.info(f">> SUCCESSFUL:Raw data fetched successfully.")
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logging.error(f'HTTP error occurred: {http_err}')
            raise
        except Exception as err:
            logging.error(f'An error occurred: {err}')
            raise
    def upload_data_to_gcs(self,bucket_name,destination_file_name,data):
        try:
            from google.cloud import storage
            storage_client = storage.Client()
            logging.info(f">> Uploading raw data (fetched from URL) to GCS bucket: {bucket_name}, file: {destination_file_name}")
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_file_name)
            blob.upload_from_string(json.dumps(data), content_type='application/json')
            logging.info(f'>> SUCCESSFUL: Raw data (fetched from URL) uploaded successfully in json format to : {bucket_name}/{destination_file_name}')
        except Exception as err:
            logging.error(f'>> Failed to upload Raw data (fetched from URL) to GCS: {err}')
            raise

    def process(self,element):
        data=self.fetch_api_data(self.api_url)
        self.upload_data_to_gcs(self.bucket_name,self.destination_file_name,data)
        output_filename = f"gs://{self.bucket_name}/{self.destination_file_name}/raw_data_fetched.json"



def phase1_pipeline():
    with beam.Pipeline(options=options) as p:
        # Step 1: Fetch and upload raw JSON data to GCS
        _ = (
            p
            | 'Create Single Element' >> beam.Create([None])  # Start trigger
            | 'Fetch and Upload Data' >> beam.ParDo(
                FetchAndUploadDataToGCS(api_url, bucket_name, destination_file_name))
        )

# Define the pipeline to read the uploaded data from GCS
def pipeline_for_read_data():
    with beam.Pipeline(options=options) as p:
        # Step 2: Read JSON data from GCS
        trigger_and_data = (
            p
            | 'Wait for Upload Completion' >> beam.Create([None])
            | 'Read JSON from GCS' >> beam.io.ReadFromText(
                f"gs://{bucket_name}/{destination_file_name}")
        )

# Run the pipelines
if __name__ == '__main__':
    phase1_pipeline()
    pipeline_for_read_data()






