import os
import json
import logging
from datetime import datetime

import requests
from google.cloud import storage
from google.cloud import bigquery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, Row

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =r"C:\Users\Ankush\PycharmProjects\EartQuakeProject\bwt-earthquake-project-a77ea1fdb8a6.json"


# Initialize clients
client = storage.Client(project="bwt-earthquake-project")
client1 = bigquery.Client(project="bwt-earthquake-project")


class Utils:

    def fetch_api_request(self, url):
        """
        Fetches data from an API URL.

        :param url: API URL
        :return: JSON OBJECT
        """
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an HTTPError if status is 4xx, 5xx
            logging.info("Connection with earthquake API established successfully.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error("Failed to establish a connection with the API: %s", e)
            raise

    def upload_to_gcs_bronze(self, data, bucket_name,destination_blob_name):
        """
        Uploads data directly to Google Cloud Storage (Bronze Layer).

        :param data: JSON data from the API
        :param bucket_name: Name of the GCS bucket
        """
        current_date = datetime.now().strftime('%Y%m%d')


        try:
            # Convert JSON data to a string and upload directly to GCS
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(json.dumps(data), content_type='application/json')
            logging.info("Data uploaded to '%s' in bucket '%s'.", destination_blob_name, bucket_name)

        except Exception as e:
            logging.error("Failed to upload data to GCS: %s", e)
            raise

    def flatten_data(self, spark, geojson_data):
        """
        Converts GeoJSON data to a PySpark DataFrame.

        :param spark: SparkSession
        :param geojson_data: A dictionary representing GeoJSON data
        :return: A PySpark DataFrame with flattened GeoJSON data
        """
        # Define schema for PySpark DataFrame
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("mag", DoubleType(), True),
            StructField("place", StringType(), True),
            StructField("time", LongType(), True),
            StructField("updated", LongType(), True),
            StructField("url", StringType(), True),
            StructField("detail", StringType(), True),
            StructField("felt", IntegerType(), True),
            StructField("cdi", DoubleType(), True),
            StructField("mmi", DoubleType(), True),
            StructField("alert", StringType(), True),
            StructField("status", StringType(), True),
            StructField("tsunami", IntegerType(), True),
            StructField("sig", IntegerType(), True),
            StructField("net", StringType(), True),
            StructField("code", StringType(), True),
            StructField("ids", StringType(), True),
            StructField("sources", StringType(), True),
            StructField("types", StringType(), True),
            StructField("nst", IntegerType(), True),
            StructField("dmin", DoubleType(), True),
            StructField("rms", DoubleType(), True),
            StructField("gap", DoubleType(), True),
            StructField("magType", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("tz", IntegerType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("depth", DoubleType(), True)
        ])

        try:
            # Prepare data for DataFrame
            rows_to_insert = []
            for feature in geojson_data['features']:
                flattened_record = Row(
                    id=feature['id'],
                    mag=float(feature['properties']['mag']),
                    place=feature['properties']['place'],
                    time=feature['properties']['time'],
                    updated=feature['properties']['updated'],
                    url=feature['properties']['url'],
                    detail=feature['properties'].get('detail'),
                    felt=feature['properties'].get('felt', 0),
                    cdi=float(feature['properties'].get('cdi', 0.0)) if feature['properties'].get('cdi') else 0.0,
                    mmi=float(feature['properties'].get('mmi')) if feature['properties'].get('mmi') else None,
                    alert=feature['properties'].get('alert'),
                    status=feature['properties']['status'],
                    tsunami=feature['properties']['tsunami'],
                    sig=feature['properties']['sig'],
                    net=feature['properties']['net'],
                    code=feature['properties']['code'],
                    ids=feature['properties'].get('ids'),
                    sources=feature['properties'].get('sources'),
                    types=feature['properties'].get('types'),
                    nst=feature['properties'].get('nst'),
                    dmin=float(feature['properties'].get('dmin', 0.0)) if feature['properties'].get('dmin') else 0.0,
                    rms=float(feature['properties'].get('rms')),
                    gap=float(feature['properties'].get('gap', 0.0)) if feature['properties'].get('gap') else 0.0,
                    magType=feature['properties']['magType'],
                    type=feature['properties']['type'],
                    title=feature['properties']['title'],
                    tz=feature['properties'].get('tz'),
                    longitude=float(feature['geometry']['coordinates'][0]),
                    latitude=float(feature['geometry']['coordinates'][1]),
                    depth=float(feature['geometry']['coordinates'][2])
                )
                rows_to_insert.append(flattened_record)

            # Create PySpark DataFrame
            df = spark.createDataFrame(rows_to_insert, schema)
            logging.info("Data flattened and PySpark DataFrame created successfully.")
            return df

        except Exception as e:
            logging.error("Failed to flatten data: %s", e)
            raise

    def upload_to_gcs_silver(self, bucket_name, destination_blob_name, data):
        """
        Uploads JSON data to a GCS bucket (Silver Layer).

        :param bucket_name: GCS bucket name
        :param destination_blob_name: Destination path in the bucket
        :param data: JSON data to upload
        """
        try:
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            json_data = json.dumps(data)
            blob.upload_from_string(json_data, content_type='application/json')
            logging.info("File uploaded to %s in bucket %s.", destination_blob_name, bucket_name)

        except Exception as e:
            logging.error("Failed to upload JSON data to GCS: %s", e)
            raise

    def read_data_from_gcs(self, bucket_name, source_blob_name):
        """
        Reads data from a GCS bucket.

        :param bucket_name: GCS bucket name
        :param source_blob_name: Source blob name in the bucket
        :return: JSON data as a dictionary
        """
        try:
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            data = blob.download_as_text()
            logging.info("Data read from GCS successfully.")
            return json.loads(data)

        except Exception as e:
            logging.error("Failed to read data from GCS: %s", e)
            raise

    def create_bigquery_dataset(self, project_id, dataset_name, location, description):
        """
        Creates a BigQuery dataset if it doesn't already exist.

        :param project_id: Project ID
        :param dataset_name: Dataset name
        :param location: Dataset location
        :param description: Dataset description
        :return: Status message about dataset creation
        """
        dataset_id = f"{project_id}.{dataset_name}"

        try:
            client1.get_dataset(dataset_id)
            logging.info("Dataset '%s' already exists.", dataset_id)
            return f"Dataset '{dataset_id}' already exists."

        except FileNotFoundError:
            try:
                dataset_ref = bigquery.Dataset(dataset_id)
                dataset_ref.location = location
                dataset_ref.description = description
                dataset = client1.create_dataset(dataset_ref)
                logging.info("Dataset created: %s", dataset_id)
                return f"Dataset created: {dataset_id}"

            except Exception as e:
                logging.error("Failed to create BigQuery dataset: %s", e)
                raise
