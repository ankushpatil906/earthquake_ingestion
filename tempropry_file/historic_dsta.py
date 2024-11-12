import os
import logging
from datetime import datetime

from pyspark.sql.functions import current_timestamp

from utils import (
    fetch_api_data, initialize_spark, write_data_on_gcs,
    read_data_from_gcs, transform_data_to_df, add_column_area, write_df_to_gcs_as_parquet, read_parquet_from_gcs,

)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    logging.info("Starting the main function")

    try:
        # Configuration
        current_date = datetime.now().strftime('%Y%m%d')
        app_name = "APIDataToGCS"
        api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
        bucket_name = "eartquake_analysis1  "
        bronze_file_name = f"pyspark/Historical_Bronze_Data/landing/{current_date}/earthquake_raw.json"
        silver_file_path = f'pyspark/Historical_Silver_Data/silver/{current_date}/earthquake_silver'
        project_id = "bwt-earthquake-project"
        dataset_id = "eartquake_pyspark"
        table_id = "earthquake_data"
        gcs_temp_location = "gs://eartquake_analysis1/tmp/"

        # Initialize Spark
        spark = initialize_spark(app_name)
        logging.info("Spark session initialized successfully")

        # Fetch data from API
        data = fetch_api_data(api_url)
        logging.info("Fetched data from API")

        # Write raw data to GCS
        write_data_on_gcs(data, bucket_name, bronze_file_name)
        logging.info("Successfully written API data to GCS")

        # Read and transform data
        json_data = read_data_from_gcs(bucket_name, bronze_file_name)
        logging.info("Data read from GCS successfully")

        df = transform_data_to_df(spark, json_data)
        df.show(truncate=False)
        logging.info(f"Total Records in DataFrame: {df.count()}")

        # Add area column to the DataFrame
        add_area_column_df = add_column_area(df)
        add_area_column_df.show(truncate=False)
        logging.info("Area column added successfully")

        # Write transformed data to GCS as Parquet
        # write_df_to_gcs_as_parquet(add_area_column_df, silver_file_path)
        # logging.info("Data written to GCS as Parquet successfully")
        #
        # # Read silver data from GCS
        # silver_df = read_parquet_from_gcs(spark, bucket_name, silver_file_path)
        # # silver_df.show(truncate=False)
        # logging.info("Read silver data from GCS and displayed it successfully")
        #
        # # Add insert date to DataFrame
        # insert_date_df = silver_df.withColumn("insert_dt", current_timestamp())
        # # insert_date_df.show(truncate=False)
        # logging.info("Inserted date column added to DataFrame")
        dfs = add_area_column_df.coalesce(1).write.mode('overwrite').parquet(f'gs://{bucket_name}/{silver_file_path}')
        print(f"Data written to gs://{bucket_name}/{silver_file_path} in Parquet format.")

        parquet_file_path = "gs://pyspark/Historical_Silver_Data/silver/{current_date}/earthquake_silver/*.parquet"

        # Read the Parquet file
        dfs = spark.read.parquet(parquet_file_path)
        dfs.show()

        # ---------------------------------------------------------------------------------------------------------------

        # Insert data : insert_dt (Timestamp)

        df1 = dfs.withColumn('insert_date', current_timestamp())
        df1.show(truncate=False)
    except Exception as e:
        logging.error("An error occurred during the main execution", exc_info=True)


if __name__ == '__main__':
    main()
    logging.info("Successfully completed all operations")
