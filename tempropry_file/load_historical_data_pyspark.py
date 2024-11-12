import json
from datetime import datetime

from pyspark.sql import SparkSession
from Silver_data.util import create_bucket, load_api_data_to_gcs_bucket, read_data_from_gcs_bucket, \
    flatten_data_and_transform_data_to_df, add_column_area, add_insert_dt

if __name__ == '__main__':
    spark = SparkSession.builder.appName('earthquake_analysis').getOrCreate()

    # Define parameters
    bucket_name = "eartquake_analysis"
    project_id = "bwt-learning-2024-431809"
    api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    # Get current date for folder and file naming
    current_date = datetime.now().strftime("%Y%m%d")  # Format the date as YYYYMMDD
    destination_file_name =f"pyspark/silver/{current_date}/silver_data.json"
    destination_file_name = f"pyspark/landing/{current_date}.json"
    silver_data_path= f"pyspark/silver/{current_date}.parquet"


    # Step 1: Create a GCS bucket if it doesn't exist
    create_bucket(bucket_name, project_id)

    # Step 2: Load data from the API and save to the GCS bucket
    load_api_data_to_gcs_bucket(api_url, bucket_name, destination_file_name)

    # Step 3: Read data from the GCS bucket
    gcs_data = read_data_from_gcs_bucket(bucket_name, destination_file_name)

    # Step 4: Transform the JSON data into a PySpark DataFrame
    df = flatten_data_and_transform_data_to_df(spark, json_data=gcs_data)

    # Step 5: Add area column to the DataFrame
    df = add_column_area(df)
    # Step 6: Add insert_dt (current timestamp) column to the DataFrame
    df = add_insert_dt(df)

    json_data = df.toJSON().collect()  # Collects the DataFrame as a list of JSON strings
    data_to_upload = [json.loads(record) for record in json_data]  # Convert each string to a dictionary
    print('error suru ')
    load_api_data_to_gcs_bucket(data_to_upload, bucket_name, destination_file_name)
    # Step 7: Write the final DataFrame to GCS as a JSON file
    print('final')
    # df.write \
    #     .mode("overwrite") \
    #     .parquet(silver_data_path)
    #
    #
    #
    # # Print schema and show data
    # # df.printSchema()
    # # df.show(truncate=False)
