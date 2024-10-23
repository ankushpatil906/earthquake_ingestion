from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

from util import create_bucket, load_api_data_to_gcs_bucket, read_data_from_gcs_bucket, flatten_data_and_transform_data_to_df

if __name__ == '__main__':
    spark = SparkSession.builder.appName('earthquake.com').getOrCreate()

    # Define parameters
    bucket_name = "eartquake_analysis"
    project_id = "bwt-learning-2024-431809"
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    destination_file_name = "pyspark/landing/20241019.json"
    current_date = datetime.now().strftime("%Y%m%d")  # Format the date as YYYYMMDD
    # Step 1: Create a GCS bucket if it doesn't exist
    create_bucket(bucket_name, project_id)

    # Step 2: Load data from the API and save to the GCS bucket
    load_api_data_to_gcs_bucket(api_url, bucket_name, destination_file_name)

    # Step 3: Read data from the GCS bucket
    gcs_data = read_data_from_gcs_bucket(bucket_name, destination_file_name)

    # Step 4: Transform the JSON data into a PySpark DataFrame
    df = flatten_data_and_transform_data_to_df(spark, json_data=gcs_data)

    # Step 5: Show the DataFrame schema and data
    df.printSchema()
    df.show(truncate=False)
