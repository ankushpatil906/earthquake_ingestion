from pyspark.sql import SparkSession
from util import create_bucket, load_api_data_to_gcs_bucket, read_data_from_gcs_bucket

if __name__ == '__main__':
    # Initialize Spark session for further processing or analysis of data.
    # SparkSession is required when working with large-scale data processing using PySpark.
    spark = SparkSession.builder.appName('earthquake.com').getOrCreate()

    # Define parameters
    bucket_name = "eartquake_analysis"  # GCS bucket name where data will be stored.
    project_id = "bwt-learning-2024-431809"  # Google Cloud Project ID.
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"  # API endpoint to fetch earthquake data (example API).
    destination_file_name = "pyspark/landing/20241019.json"  # File path inside the GCS bucket where the data will be stored.
    file_path = "gs://eartquake_analysis/pyspark/landing/20241019.json"  # Full GCS path to the file.

    # Step 1: Create a GCS bucket if it doesn't already exist.
    # This function checks for the existence of the bucket and creates it if needed.
    create_bucket(bucket_name, project_id)

    # Step 2: Load the data from the API and save it to the GCS bucket.
    # This function fetches earthquake data from the API, converts it to JSON format, and uploads it to the GCS bucket.
    load_api_data_to_gcs_bucket(api_url, bucket_name, destination_file_name)

    # Step 3: Read the data back from the GCS bucket.
    # This function downloads the file from GCS, reads it as a string, parses the JSON data, and returns it as a Python object.
    gcs_data = read_data_from_gcs_bucket(bucket_name, destination_file_name)

    # Step 4: Print the data retrieved from GCS (this is for verification or debugging purposes).
    # This will display the earthquake data fetched from the API and stored in GCS.
    print(gcs_data)
