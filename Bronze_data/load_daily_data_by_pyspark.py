import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, date_format, regexp_extract, current_timestamp
from utilss import Utils

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Spark Session
if __name__ == "__main__":
    logging.info("Starting Spark Session for Earthquake Data Processing.")
    spark = SparkSession.builder.appName('Earthquake_data').master('local[*]').getOrCreate()

    # Object Creation for Utility Class
    obj_util = Utils()
    current_date = datetime.now().strftime('%Y%m%d')

    # Step 1: API Request
    logging.info("Fetching data from the Earthquake API URL.")
    url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'

    try:
        response = obj_util.fetch_api_request(url)
        logging.info("API data fetched successfully.")
    except Exception as e:
        logging.error("Error fetching data from API: %s", e)
        raise

    # Set up bucket information
    bucket_name = "eartquake_analysis1"

    # Fetch the data from the API
    data = response

    # Upload the data to GCS (Bronze Layer)
    logging.info("Uploading raw earthquake data to Google Cloud Storage (Bronze Layer).")
    try:
        destination_blob_name = f'pyspark/Historical_Bronze_Data/landing/{current_date}/earthquake_raw.json'

        obj_util.upload_to_gcs_bronze(data,bucket_name,destination_blob_name)
        logging.info("Raw data successfully uploaded to GCS Bronze Layer.")
    except Exception as e:
        logging.error("Error uploading data to GCS Bronze Layer: %s", e)
        raise

    # Step 2: Read the data from GCS and flatten it
    logging.info("Reading raw data from Google Cloud Storage and flattening.")
    try:
        input_file = obj_util.read_data_from_gcs(
            bucket_name="eartquake_analysis1",
            source_blob_name=f'pyspark/Historical_Bronze_Data/landing/{current_date}/earthquake_raw.json'

        )
        flattened_df = obj_util.flatten_data(spark, input_file)
        logging.info("Data successfully read from GCS and flattened.")
    except Exception as e:
        logging.error("Error reading and flattening data from GCS: %s", e)
        raise

    # Convert 'time' and 'updated' from milliseconds (epoch) to timestamp
    df = flattened_df.withColumn('time', from_unixtime(col('time') / 1000)) \
        .withColumn('updated', from_unixtime(col('updated') / 1000))

    # Extract area from the 'place' column
    pattern = r'of\s(.*)'
    df = df.withColumn('area', regexp_extract(col('place'), pattern, 1))
    df.show()

    # Step 3: Upload processed data to GCS in Parquet format (Silver Layer)
    destination_blob_name = f'pyspark/Historical_Silver_Data/silver/{current_date}/earthquake_silver'
    logging.info("Uploading processed data to Google Cloud Storage (Silver Layer) in Parquet format.")

    try:
        df.coalesce(1).write.mode('overwrite').parquet(f'gs://{bucket_name}/{destination_blob_name}')
        logging.info("Data successfully written to GCS Silver Layer in Parquet format.")
    except Exception as e:
        logging.error("Error uploading processed data to GCS Silver Layer: %s", e)
        raise

    # Step 4: Read data from the Parquet file in GCS
    parquet_file_path = f"gs://{bucket_name}/pyspark/Historical_Silver_Data/silver/{current_date}/earthquake_silver/*.parquet"
    logging.info("Reading data from the Parquet file in Google Cloud Storage.")

    try:
        df = spark.read.parquet(parquet_file_path)
        logging.info("Data successfully read from the Parquet file.")
    except Exception as e:
        logging.error("Error reading data from Parquet file in GCS: %s", e)
        raise

    # Step 5: Add a timestamp column and upload to BigQuery
    df1 = df.withColumn('insert_date', current_timestamp())
    df1.show(truncate=False)

    logging.info("Writing data to BigQuery table 'bwt-earthquake-project.eartquake_pyspark.historical_data_dataproc'.")
    try:
        df1.write.format("bigquery") \
            .option("table", "bwt-earthquake-project.eartquake_pyspark.historical_data_dataproc") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        logging.info("Data successfully written to BigQuery.")
    except Exception as e:
        logging.error("Error writing data to BigQuery: %s", e)
        raise

