# big-data-transformation-and-visualization-with-Spark
# Taxi Trip Record Data Analysis with PySpark

This project focuses on analyzing taxi trip records using PySpark, performing various transformations, merging dataframes, and analyzing data quality.

## Environment Setup

Ensure you have PySpark installed to run this code. You can install it using the following command:

```bash
pip install pyspark

Project Description
This project utilizes PySpark to perform transformations and analyze taxi trip records data. It involves the following steps:

1. Data Transformation
Transformation of yellow and green taxi trip data using custom methods.
Removal of duplicate rows.

2. Preview of Data
Displaying a preview of the transformed yellow and green trip dataframes.
Printing the schema of the dataframes.

3. Location Details
Reading the 'taxi_zone_lookup.csv' file into a Spark DataFrame for location details.
Merging location details into the trip dataframes.

Data Analysis
Calculating the number of rows and columns in the yellow and green trip dataframes.

## Code Overview
# Spark Session Initialization
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('TLC_Trip_Record_Data').getOrCreate()

# Importing necessary functions and types
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.helper import data_transformation, merge_dataframes
from utils.constants import yellow_tripdata_files, green_tripdata_files

# Data Transformation and Manipulation Steps (As per the provided code snippet)
# ...

# Displaying Dataframe Schema and Basic Stats
# ...

# Analysis and Summary
# ...

## Instructions
To run this code:

1. Ensure you have the necessary dependencies installed.
2. Replace the placeholder file paths in yellow_tripdata_files and green_tripdata_files with the actual paths to your data files.
3. Execute the code snippet in a PySpark environment or notebook.


## Data Analysis And Visualization
# Taxi Trip Data Analysis with Matplotlib and PySpark

This code performs various analysis tasks on taxi trip data using PySpark and visualizes the results using Matplotlib.

## Instructions

Ensure you have the required dependencies installed. To execute this code:

1. Set up your PySpark environment.
2. Update the paths or import statements to access your datasets.
3. Run the code in a PySpark environment or notebook.


### Analysis Tasks and Visualizations

### Task 2A
- Analyze and plot counts of trips per Pickup Borough for Yellow Taxi and Green Taxi datasets.

### Task 2B
- Analyze and visualize counts of trips per Pickup Borough for Green Taxi.

### Task 2C
- Explore and visualize counts of trips per Dropoff Borough for Yellow Taxi.

### Task 2D
- Analyze counts of trips per Dropoff Borough for Green Taxi and visualize the findings.

### Task 4
- Investigate top pickup and drop-off boroughs for Yellow and Green Taxi datasets and plot the findings.

### Task 6
- Analyze fare per mile for Yellow Taxi trips in March 2023 and plot the data.

### Task 7
- Calculate and compare the percentage of solo trips for Yellow and Green Taxi datasets.

### Task 8
- Investigate the correlation between trip duration and distance for Yellow Taxi trips in January 2023 using a scatter plot.

### Task 9
- Explore the total pickups per borough for Yellow and Green Taxis and visualize the top 10 active boroughs.

### Task 10
- Analyze monthly trip counts for Yellow and Green Taxis and plot the data.

Note: Adjust the code parameters or dataset paths as needed for your specific analysis.

## Contributions and License

Contributions are welcome under the MIT License. Feel free to modify or use this code for your projects.
