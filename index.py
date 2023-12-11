from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('TLC_Trip_Record_Data').getOrCreate()

from utils.helper import data_transformation, merge_dataframes
from utils.constants import yellow_tripdata_files, green_tripdata_files

# Transform the yellow taxi trip data using custom transformations and store it in yellow_tripdata_df
yellow_tripdata_df = data_transformation(yellow_tripdata_files)

# Remove duplicate rows from yellow_tripdata_df
yellow_tripdata_df = yellow_tripdata_df.drop_duplicates()

# Display a preview of the contents in yellow_tripdata_df
yellow_tripdata_df.show()

# Transform the green taxi trip data using custom transformations and store it in green_tripdata_df
green_tripdata_df = data_transformation(green_tripdata_files)

# Remove duplicate rows from green_tripdata_df
green_tripdata_df.drop_duplicates()

# Display a preview of the contents in yellow_tripdata_df
green_tripdata_df.show()

# Read the 'taxi_zone_lookup.csv' file into the Spark DataFrame taxi_zone_lookup_df
taxi_zone_lookup_df = spark.read.csv('taxi_zone_lookup.csv', header=True)

# Remove duplicate rows from taxi_zone_df
taxi_zone_lookup_df = taxi_zone_lookup_df.drop_duplicates()

# Merge location details into the yellow trip data DataFrame (yellow_tripdata_df)
yellow_tripdata_df = merge_dataframes(yellow_tripdata_df)

# Merge location details into the green trip data DataFrame (green_tripdata_df)
green_tripdata_df = merge_dataframes(green_tripdata_df)

# Display the schema of the yellow_tripdata_df DataFrame
yellow_tripdata_df.printSchema()

# Display the schema of the green_tripdata_df DataFrame
green_tripdata_df.printSchema()

# Calculate and print the number of rows in the 'yellow_tripdata_df' dataframe
num_rows = yellow_tripdata_df.count()
print("Number of rows for yellow:", num_rows)

# Calculate and print the number of rows in the 'green_tripdata_df' dataframe
num_rows = green_tripdata_df.count()
print("Number of rows for green:", num_rows)

# Calculate and print the number of columns in the 'yellow_tripdata_df' dataframe
num_columns = len(yellow_tripdata_df.columns)
print("Number of columns for yellow:", num_columns)


