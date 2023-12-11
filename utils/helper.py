
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#

spark = SparkSession.builder.appName('TLC_Trip_Record_Data').getOrCreate()
taxi_zone_lookup_df = spark.read.csv('taxi_zone_lookup.csv', header=True)

def data_transformation(data_files):
    """
    Transform the given data files into a combined DataFrame.

    Args:
    - data_files (list): List of file paths containing Parquet data.

    Returns:
    - combined_df (DataFrame): Combined DataFrame with transformed data.
    
    This function reads Parquet data files and performs specific transformations:
    - For files containing 'yellow' in their names, it creates a DataFrame (df), drops specific columns, 
      adds a new column 'taxi_type' with a constant value 'yellow_taxi', and merges the DataFrame.
    - For other files, it performs similar operations but assigns a different constant value 'green_taxi' 
      to the 'taxi_type' column.

    Note:
    - The 'VendorID', 'RatecodeID', 'store_and_fwd_flag', and 'improvement_surcharge' columns are dropped.
    - The final output is a combined DataFrame containing the transformed data from all input files.
    """
    for data_file in data_files:
    
        if 'yellow' in data_file:
            df = spark.read.parquet(data_file, header = True)   
            columns_to_drop = ['VendorID', 'RatecodeID', 'store_and_fwd_flag', 'improvement_surcharge']
            df = df.drop(*columns_to_drop)
            constant_value = "yellow_taxi"  # value of new column
            df = df.withColumn("taxi_type", lit(constant_value)) # create new column
            combined_df = df.union(df)  #merge columns month-wise


        else:
            df = spark.read.parquet(data_file, header = True)   
            columns_to_drop = ['VendorID', 'RatecodeID', 'store_and_fwd_flag', 'improvement_surcharge']
            df = df.drop(*columns_to_drop)
            constant_value = "green_taxi"
            df = df.withColumn("taxi_type", lit(constant_value))
            combined_df = df.union(df)

        return combined_df 



def merge_dataframes(df_name):
    """
    Merge the given DataFrame with the 'taxi_zone_lookup_df' DataFrame based on pickup and dropoff location IDs.

    Args:
    - df_name (DataFrame): The DataFrame to be merged with 'taxi_zone_lookup_df'.

    Returns:
    - df (DataFrame): Merged DataFrame with pickup and dropoff location details.

    This function performs a left join between the input DataFrame (df_name) and 'taxi_zone_lookup_df' based on 
    pickup (PULocationID) and dropoff (DOLocationID) location IDs. It renames columns related to pickup location 
    information ('Borough', 'Zone', 'service_zone') as 'Pickup_Borough', 'Pickup_Zone', 'Pickup_service_zone' 
    respectively, and similarly renames columns related to dropoff location information. The 'LocationID' columns 
    are dropped after the join operation.

    Note:
    - 'taxi_zone_lookup_df' should have columns like 'LocationID', 'Borough', 'Zone', 'service_zone'.
    - The final output DataFrame contains the merged details of pickup and dropoff locations with the input DataFrame.
    """
    df = df_name
    df = df.join(taxi_zone_lookup_df, df.PULocationID == taxi_zone_lookup_df.LocationID, "left") \
            .withColumnRenamed("Borough", "Pickup_Borough") \
            .withColumnRenamed("Zone", "Pickup_Zone") \
            .withColumnRenamed("service_zone", "Pickup_service_zone") \
            .drop('LocationID') \
            .join(taxi_zone_lookup_df, df.DOLocationID == taxi_zone_lookup_df.LocationID, "left") \
            .withColumnRenamed("Borough", "Dropoff_Borough") \
            .withColumnRenamed("Zone", "Dropoff_Zone") \
            .withColumnRenamed("service_zone", "Dropoff_service_zone") \
            .drop('LocationID')
    
    return df
