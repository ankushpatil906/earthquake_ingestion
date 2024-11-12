import json
import logging
import requests
from datetime import datetime
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, split

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_spark(app_name):
    logging.info(f"Initializing Spark session with app name: {app_name}")
    return SparkSession.builder.appName(app_name).getOrCreate()

def fetch_api_data(api_url):
    logging.info(f"Fetching data from API: {api_url}")
    response = requests.get(api_url)
    if response.status_code == 200:
        logging.info("Successfully fetched data from API")
        return response.json()
    else:
        logging.error(f"Failed to fetch data: {response.status_code}")
        raise Exception(f"Failed to fetch data: {response.status_code}")


def upload_to_gcs_bronze(self, destination_blob_name, data, bucket_name):
    """
    Uploads JSON data to Google Cloud Storage (Bronze Layer).

    :param destination_blob_name: Destination path in the bucket
    :param data: JSON data from the API
    :param bucket_name: Name of the GCS bucket
    """
    current_date = datetime.now().strftime('%Y%m%d')

    try:
        # Upload the JSON data directly to GCS
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'pyspark/Historical_Bronze_Data/landing/{current_date}/earthquake_raw.json'
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        logging.info("Data uploaded to GCS at '%s' in bucket '%s'.", destination_blob_name, bucket_name)

    except Exception as e:
        logging.error("Failed to upload data to GCS: %s", e)
        raise


def read_data_from_gcs(bucket_name, bronze_file_name):
    logging.info(f"Reading data from GCS bucket: {bucket_name}, file: {bronze_file_name}")
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(bronze_file_name)
    data = blob.download_as_string()
    logging.info("Successfully read data from GCS")
    return json.loads(data)

def convert_epoch_time_to_Timestamp(timestamps):
    if timestamps is not None:
        logging.debug(f"Converting timestamp: {timestamps}")
        timestamp_s = timestamps / 1000
        converted_time = datetime.utcfromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
        logging.debug(f"Converted timestamp: {converted_time}")
        return converted_time
    logging.debug("Timestamp is None")
    return None

def transform_data_to_df(spark, json_data):
    logging.info("Transforming JSON data into a DataFrame")
    features = json_data.get("features", [])
    flatten_data = []

    for feature in features:
        properties = feature["properties"]
        geometry = feature["geometry"]
        coordinates = geometry["coordinates"]

        flattened_record = {
            "place": properties.get("place"),
            "mag": float(properties.get("mag")) if properties.get("mag") is not None else None,
            "time": convert_epoch_time_to_Timestamp(properties.get("time")),
            "updated": convert_epoch_time_to_Timestamp(properties.get("updated")),
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

        flatten_data.append(flattened_record)

    logging.info("Data transformation completed, creating DataFrame")

    schema = StructType([
        StructField("place", StringType(), True),
        StructField("mag", FloatType(), True),
        StructField("time", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("tz", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", FloatType(), True),
        StructField("mmi", FloatType(), True),
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
        StructField("dmin", FloatType(), True),
        StructField("rms", FloatType(), True),
        StructField("gap", FloatType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("geometry", StructType([
            StructField("longitude", FloatType(), True),
            StructField("latitude", FloatType(), True),
            StructField("depth", FloatType(), True)
        ]))
    ])

    return spark.createDataFrame(flatten_data, schema=schema)

def add_column_area(df):
    logging.info("Adding area column to the DataFrame")
    return df.withColumn("area", split(col("place"), "of").getItem(1))

def write_df_to_gcs_as_parquet(df, output_path):
    logging.info(f"Writing DataFrame to GCS as Parquet at {output_path}")
    try:
        df.write.mode('overwrite').parquet(output_path)
        logging.info(f"Data successfully written to GCS as Parquet at {output_path}")
    except Exception as e:
        logging.error(f"Error writing DataFrame to GCS as Parquet: {e}")
        raise

def read_parquet_from_gcs(spark, bucket_name, file_path):
    gcs_path = f"gs://{bucket_name}/{file_path}"
    logging.info(f"Reading Parquet data from GCS: {gcs_path}")

    try:
        df = spark.read.parquet(gcs_path)
        logging.info("Successfully read Parquet data from GCS")
        return df
    except Exception as e:
        logging.error(f"Error reading Parquet data from GCS: {e}")
        raise

# def load_df_to_bigquery(df, project_id, dataset_id, table_id, gcs_temp_location):
#     table_ref = f"{project_id}.{dataset_id}.{table_id}"
#     logging.info(f"Loading DataFrame to BigQuery table: {table_ref}")
#
#     try:
#         df.write \
#             .format("bigquery") \
#             .option("table", table_ref) \
#             .option("temporaryGcsBucket", gcs_temp_location) \
#             .mode("overwrite") \
#             .save()
#         logging.info("Data successfully loaded into BigQuery")
#     except Exception as e:
#         logging.error(f"Failed to load data into BigQuery: {e}")
