import os
from datetime import datetime
from google.cloud import storage
import requests
import json
from pyspark.sql.functions import col, split, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize GCS client
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'./bwt-learning-2024-431809-a14e88a78b4e.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =r"C:\Users\Ankush\PycharmProjects\GCP_Session\bwt-project-2024-440809-f03da00f7efa.json"
client = storage.Client()


# Create GCS bucket
def create_bucket(bucket_name, project_id):
    client=storage.Client()
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        client.create_bucket(bucket, project=project_id,location='asia-east1')
        print(f"Bucket '{bucket_name}' created successfully in the asia-east1 region.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

# Load API data into GCS bucket
def load_api_data_to_gcs_bucket(api_url, bucket_name, destination_file_name):
    api_response = requests.get(api_url)
    print('error suru 1')
    if api_response.status_code == 200:
        print('error suru 2')
        api_data = api_response.json()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_file_name)

        blob.upload_from_string(json.dumps(api_data), content_type='application/json')

        print(f"Data loaded to GCS at gs://{bucket_name}/{destination_file_name}")
    else:
        print(f"Failed to fetch data from the API. Status code: {api_response.status_code}")

# Read data from GCS bucket
def read_data_from_gcs_bucket(bucket_name, source_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_file_name)
    source_data = blob.download_as_text()
    data = json.loads(source_data)
    return data

# Helper function to convert epoch time to timestamp
def convert_epoch_to_timestamp(epoch_time):
    if epoch_time:
        return datetime.utcfromtimestamp(epoch_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return None

# Function to transform and flatten data into PySpark DataFrame
def flatten_data_and_transform_data_to_df(spark, json_data):
    features = json_data.get("features", [])
    flatten_data = []

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

        flatten_data.append(flattened_record)

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

    df = spark.createDataFrame(flatten_data, schema=schema)
    return df

# Function to add 'area' column to the DataFrame
def add_column_area(df):
    add_column_area_df = df.withColumn("area", split(col("place"), "of").getItem(1))
    return add_column_area_df

# Function to add 'insert_dt' column (current timestamp) to DataFrame
def add_insert_dt(df):
    return df.withColumn("insert_dt", current_timestamp())

# Function to write DataFrame to GCS as JSON
# def write_df_to_gcs_as_json(df, bucket_name, output_path):
#
#     try:
#         df.write.mode('overwrite').json(output_path)
#         print(f"Data successfully written to GCS at {output_path}")
#     except Exception as e:
#         print(f"Error writing data to GCS: {e}")




