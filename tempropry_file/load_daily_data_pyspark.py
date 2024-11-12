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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\Ankush\PycharmProjects\GCP_Session\bwt-learning-2024-431809-a14e88a78b4e.json'

# Utility functions
class FetchAndUploadDataToGCS(beam.DoFn):
    def __init__(self, api_url, bucket_name, destination_file_name):
        self.api_url = api_url
        self.bucket_name = bucket_name
        self.destination_file_name = destination_file_name

    def start_bundle(self):
        # Initialize the storage client once per bundle
        self.storage_client = storage.Client()

    def process(self, element):
        # Fetch data from the API
        response = requests.get(self.api_url)
        if response.status_code == 200:
            data = response.json()

            # Upload data to GCS
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(self.destination_file_name)
            blob.upload_from_string(json.dumps(data), content_type='application/json')

            # Yield the data for further processing in the pipeline
            yield data
        else:
            raise Exception(f"API request failed with status code: {response.status_code}")

def convert_epoch_to_timestamp(timestamp_ms):
    if timestamp_ms is not None:
        timestamp = timestamp_ms / 1000
        return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return None

class FlattenToJSONData(beam.DoFn):
    def process(self, json_data):
        features = json_data.get("features", [])
        for feature in features:
            properties = feature["properties"]
            geometry = feature["geometry"]
            coordinates = geometry["coordinates"]

            flattened_record = {
                "place": properties.get("place"),
                "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
                "time": convert_epoch_to_timestamp(properties.get("time")),
                "updated": convert_epoch_to_timestamp(properties.get("updated")),
                "tz": properties.get("tz"),
                "url": properties.get("url"),
                "detail": properties.get("detail"),
                "felt": properties.get("felt"),
                "cdi": float(properties.get("cdi")) if properties.get("cdi") is not None else None,
                "mmi": float(properties.get("mmi")) if properties.get("mmi") is not None else None,
                "alert": properties.get("alert"),
                "status": properties.get("status"),
                "tsunami": properties.get("tsunami"),
                "sig": properties.get("sig"),
                "net": properties.get("net"),
                "code": properties.get("code"),
                "ids": properties.get("ids"),
                "sources": properties.get("sources"),
                "types": properties.get("types"),
                "nst": properties.get("nst"),
                "dmin": float(properties.get("dmin")) if properties.get("dmin") is not None else None,
                "rms": float(properties.get("rms")) if properties.get("rms") is not None else None,
                "gap": float(properties.get("gap")) if properties.get("gap") is not None else None,
                "magType": properties.get("magType"),
                "type": properties.get("type"),
                "title": properties.get("title"),
                "geometry": {
                    "longitude": coordinates[0],
                    "latitude": coordinates[1],
                    "depth": float(coordinates[2]) if coordinates[2] is not None else None
                }
            }
            yield flattened_record

class AddColumnAreaByPlace(beam.DoFn):
    def process(self, record):
        place = record["place"]
        if "of" in place:
            area = place.split("of")[1].strip()
        else:
            area = "Unknown"
        record["area"] = area
        yield record

class AddInsertDate(beam.DoFn):
    def process(self, record):
        record["insert_date"] = datetime.utcnow().strftime('%Y-%m-%d')
        yield record
# Define your schema using pyarrow
parquet_schema = pa.schema([
            ('mag', pa.float32()),  # FLOAT in BigQuery
            ('place', pa.string()),  # STRING in BigQuery
            ('time', pa.timestamp('us')),  # TIMESTAMP in BigQuery (microseconds)
            ('updated', pa.timestamp('us')),  # TIMESTAMP in BigQuery (microseconds)
            ('tz', pa.int32()),  # INTEGER in BigQuery
            ('url', pa.string()),  # STRING in BigQuery
            ('detail', pa.string()),  # STRING in BigQuery
            ('felt', pa.int32()),  # INTEGER in BigQuery
            ('cdi', pa.float32()),  # FLOAT in BigQuery
            ('mmi', pa.float32()),  # FLOAT in BigQuery
            ('alert', pa.string()),  # STRING in BigQuery
            ('status', pa.string()),  # STRING in BigQuery
            ('tsunami', pa.int32()),  # INTEGER in BigQuery
            ('sig', pa.int32()),  # INTEGER in BigQuery
            ('net', pa.string()),  # STRING in BigQuery
            ('code', pa.string()),  # STRING in BigQuery
            ('ids', pa.string()),  # STRING in BigQuery
            ('sources', pa.string()),  # STRING in BigQuery
            ('types', pa.string()),  # STRING in BigQuery
            ('nst', pa.int32()),  # INTEGER in BigQuery
            ('dmin', pa.float32()),  # FLOAT in BigQuery
            ('rms', pa.float32()),  # FLOAT in BigQuery
            ('gap', pa.float32()),  # FLOAT in BigQuery
            ('magType', pa.string()),  # STRING in BigQuery
            ('title', pa.string()),  # STRING in BigQuery
            ('geometry_type', pa.string()),  # STRING in BigQuery
            ('longitude', pa.float32()),  # FLOAT in BigQuery
            ('latitude', pa.float32()),  # FLOAT in BigQuery
            ('depth', pa.float32()),  # FLOAT in BigQuery
            ('area', pa.string()),  # STRING in BigQuery
            ('insert_date', pa.timestamp('us')),  # TIMESTAMP in BigQuery (microseconds)
        ])

def run_pipeline():
    options = PipelineOptions(save_main_session=True)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'bwt-learning-2024-431809'
    google_cloud_options.job_name = 'api-data-to-gcs3'
    google_cloud_options.region = "asia-east1"
    google_cloud_options.temp_location = 'gs://eartquake_analysis/temp_loc'
    google_cloud_options.staging_location = 'gs://eartquake_analysis/stage_loc'

    options.view_as(StandardOptions).runner = 'DataflowRunner'

    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    bucket_name = "eartquake_analysis"
    current_date = datetime.now().strftime('%Y%m%d')

    # bronze_output_path = f"gs://{bucket_name}/dataflow/landing/{current_date}/earthquake_raw_data"
    silver_output_path = f'gs://{bucket_name}/dataflow/silver/{current_date}/earthquake_raw_data'
    destination_file_name = f"dataflow/landing/{current_date}/earthquake_raw_data"
    table_spec = 'bwt-learning-2024-431809-433112:earthquake_ingestion_dataset.earthquake_dataflow_table'


    with beam.Pipeline(options=options) as p:
        raw_data = (p
                    | 'Start Pipeline' >> beam.Create([None])
                    | 'Fetch and Upload Data to GCS' >> beam.ParDo(
                    FetchAndUploadDataToGCS(api_url, bucket_name, destination_file_name))
                    )
        read_raw_data_from_gcs = (raw_data
                              | 'Read Raw Data from GCS' >> beam.io.ReadFromText(f'gs://{bucket_name}/dataflow/landing/{current_date}/earthquake_raw_data')
                              | 'Parse JSON' >> beam.Map(json.loads))

        transform_data = (read_raw_data_from_gcs
                      | 'Flatten JSON Data' >> beam.ParDo(FlattenToJSONData()))

        add_area_col = (transform_data
                    | 'Add Area Column' >> beam.ParDo(AddColumnAreaByPlace()))

        transformed_with_insert_date = (add_area_col
                                    | 'Add Insert Date' >> beam.ParDo(AddInsertDate()))


        write_proceseed_data_into_gcs=(transformed_with_insert_date | 'Write Processed Data to Parquet' >> WriteToParquet(
                                    silver_output_path, schema=parquet_schema, shard_name_template='', num_shards=1))



    # table_schema = {
        #     "fields": [
        #         {"name": "place", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "mag", "type": "FLOAT", "mode": "NULLABLE"},
        #         {"name": "time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        #         {"name": "updated", "type": "TIMESTAMP", "mode": "NULLABLE"},
        #         {"name": "tz", "type": "INTEGER", "mode": "NULLABLE"},
        #         {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "detail", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "felt", "type": "INTEGER", "mode": "NULLABLE"},
        #         {"name": "cdi", "type": "FLOAT", "mode": "NULLABLE"},
        #         {"name": "mmi", "type": "FLOAT", "mode": "NULLABLE"},
        #         {"name": "alert", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "tsunami", "type": "INTEGER", "mode": "NULLABLE"},
        #         {"name": "sig", "type": "INTEGER", "mode": "NULLABLE"},
        #         {"name": "net", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "code", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "ids", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "sources", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "types", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "nst", "type": "INTEGER", "mode": "NULLABLE"},
        #         {"name": "dmin", "type": "FLOAT", "mode": "NULLABLE"},
        #         {"name": "rms", "type": "FLOAT", "mode": "NULLABLE"},
        #         {"name": "gap", "type": "FLOAT", "mode": "NULLABLE"},
        #         {"name": "magType", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "type", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "geometry", "type": "RECORD", "mode": "NULLABLE", "fields": [
        #             {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
        #             {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
        #             {"name": "depth", "type": "FLOAT", "mode": "NULLABLE"}
        #         ]},
        #         {"name": "area", "type": "STRING", "mode": "NULLABLE"},
        #         {"name": "insert_date", "type": "DATE", "mode": "NULLABLE"}
        #     ]
        # }
        #
        # transformed_with_insert_date | 'Write to BigQuery' >> WriteToBigQuery(
        #     table=table_spec,
        #     schema=table_schema,
        #     write_disposition=BigQueryDisposition.WRITE_APPEND,
        #     create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
        # )
if __name__ == '__main__':
    run_pipeline()