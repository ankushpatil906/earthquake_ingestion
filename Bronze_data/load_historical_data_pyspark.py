from pyspark.sql import SparkSession
from util import create_bucket,load_api_data_to_gcs_bucket
if __name__ == '__main__':

    #initialize sparksession
    spark=SparkSession.builder.appName('earthquake.com').getOrCreate()

#define_parametres
bucket_name = "eartquake_analysis"
project_id = "bwt-learning-2024-431809"
api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"  # Example API
destination_file_name = "earthquake_data/historical_api_data.json"  # Destination in GCS

#call create bucket function
create_bucket(bucket_name, project_id)
load_api_data_to_gcs_bucket(api_url,bucket_name,destination_file_name)
