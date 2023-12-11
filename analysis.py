import matplotlib.pyplot as plt


from index import yellow_tripdata_df, green_tripdata_df
from pyspark.sql.functions import col, count, when, avg




#===== Task 2A.
# Calculate and display counts of trips per Pickup Borough for Yellow Taxi
pickup_counts_yellow_taxi = yellow_tripdata_df.groupBy("Pickup_Borough").count().orderBy("count", ascending=False)
pickup_counts_yellow_taxi.show()

# Collect aggregated counts DataFrame into a list of rows and prepare data for plotting
counts_data = pickup_counts_yellow_taxi.collect()
labels = [row["Pickup_Borough"] for row in counts_data]
values = [row["count"] for row in counts_data]

# Plot the line plot showing counts of trips per Pickup Borough for Yellow Taxi
plt.figure(figsize=(10, 6))
plt.plot(labels, values, marker='o', linestyle='-', color='b')
plt.title('Counts of Trips per Pickup Borough for Yellow Taxi')
plt.xlabel('Pickup Borough')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()

plt.show()




#===== Task 2B
# Calculate and display counts of trips per Pickup Borough for Green Taxi
pickup_counts_green_taxi = green_tripdata_df.groupBy("Pickup_Borough").count().orderBy("count", ascending=False)
pickup_counts_green_taxi.show()

# Collect aggregated counts DataFrame into a list of rows and prepare data for plotting
counts_data = pickup_counts_green_taxi.collect()
labels = [row["Pickup_Borough"] for row in counts_data]
values = [row["count"] for row in counts_data]

# Plot the line plot showing counts of trips per Pickup Borough for Green Taxi
plt.figure(figsize=(10, 6))
plt.plot(labels, values, marker='o', linestyle='-', color='b')
plt.title('Counts of Trips per Pickup Borough for Green Taxi')
plt.xlabel('Pickup Borough')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()

plt.show()




#===== Task 2C
# Calculate and display counts of trips per Dropoff Borough for Yellow Taxi
dropoff_counts_yellow_taxi = yellow_tripdata_df.groupBy("Dropoff_Borough").count().orderBy("count", ascending=False)
dropoff_counts_yellow_taxi.show()

# Collect aggregated counts DataFrame into a list of rows and prepare data for plotting
counts_data = dropoff_counts_yellow_taxi.collect()
labels = [row["Dropoff_Borough"] for row in counts_data]
values = [row["count"] for row in counts_data]

# Plot the line plot showing counts of trips per Dropoff Borough for Yellow Taxi
plt.figure(figsize=(10, 6))
plt.plot(labels, values, marker='o', linestyle='-', color='b')
plt.title('Counts of Trips per Dropoff Borough for Yellow Taxi')
plt.xlabel('Dropoff Borough')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()

plt.show()




#====== Task 2D
# Calculate and display counts of trips per Dropoff Borough for Green Taxi
dropoff_counts_green_taxi = green_tripdata_df.groupBy("Dropoff_Borough").count().orderBy("count", ascending=False)
dropoff_counts_green_taxi.show()

# Collect aggregated counts DataFrame into a list of rows and prepare data for plotting
counts_data = dropoff_counts_green_taxi.collect()
labels = [row["Dropoff_Borough"] for row in counts_data]
values = [row["count"] for row in counts_data]

# Plot the line plot showing counts of trips per Dropoff Borough for Green Taxi
plt.figure(figsize=(10, 6))
plt.plot(labels, values, marker='o', linestyle='-', color='b')
plt.title('Counts of Trips per Dropoff Borough for Green Taxi')
plt.xlabel('Dropoff Borough')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()

plt.show()




#====== Task 4
# For Yellow Taxi Dataset
yellow_pickup_counts = yellow_tripdata_df.groupBy("Pickup_Borough").count().orderBy(col("count").desc())
yellow_dropoff_counts = yellow_tripdata_df.groupBy("Dropoff_Borough").count().orderBy(col("count").desc())

# Extract top 5 pickup and drop-off boroughs for Yellow Taxi
top_5_yellow_pickup = yellow_pickup_counts.limit(5)
top_5_yellow_dropoff = yellow_dropoff_counts.limit(5)

# For Green Taxi Dataset
green_pickup_counts = green_tripdata_df.groupBy("Pickup_Borough").count().orderBy(col("count").desc())
green_dropoff_counts = green_tripdata_df.groupBy("Dropoff_Borough").count().orderBy(col("count").desc())

# Extract top 5 pickup and drop-off boroughs for Green Taxi
top_5_green_pickup = green_pickup_counts.limit(5)
top_5_green_dropoff = green_dropoff_counts.limit(5)

# Convert to Pandas for plotting
top_5_yellow_pickup = top_5_yellow_pickup.toPandas()
top_5_yellow_dropoff = top_5_yellow_dropoff.toPandas()
top_5_green_pickup = top_5_green_pickup.toPandas()
top_5_green_dropoff = top_5_green_dropoff.toPandas()

# Visualize Top 5 pickup and drop-off boroughs for Yellow Taxi
plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
plt.bar(top_5_yellow_pickup["Pickup_Borough"], top_5_yellow_pickup["count"], color='orange')
plt.title('Top 5 Pickup Boroughs for Yellow Taxi')
plt.xlabel('Borough')
plt.ylabel('Number of Trips')
plt.xticks(rotation=45)
plt.grid(axis='y')

plt.subplot(1, 2, 2)
plt.bar(top_5_yellow_dropoff["Dropoff_Borough"], top_5_yellow_dropoff["count"], color='green')
plt.title('Top 5 Drop-off Boroughs for Yellow Taxi')
plt.xlabel('Borough')
plt.ylabel('Number of Trips')
plt.xticks(rotation=45)
plt.grid(axis='y')

plt.tight_layout()
plt.show()

# Visualize Top 5 pickup and drop-off boroughs for Green Taxi
plt.figure(figsize=(12, 5))

plt.subplot(1, 2, 1)
plt.bar(top_5_green_pickup["Pickup_Borough"], top_5_green_pickup["count"], color='orange')
plt.title('Top 5 Pickup Boroughs for Green Taxi')
plt.xlabel('Borough')
plt.ylabel('Number of Trips')
plt.xticks(rotation=45)
plt.grid(axis='y')

plt.subplot(1, 2, 2)
plt.bar(top_5_green_dropoff["Dropoff_Borough"], top_5_green_dropoff["count"], color='green')
plt.title('Top 5 Drop-off Boroughs for Green Taxi')
plt.xlabel('Borough')
plt.ylabel('Number of Trips')
plt.xticks(rotation=45)
plt.grid(axis='y')

plt.tight_layout()
plt.show()



#======= Task 6
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import matplotlib.pyplot as plt

# Create a Spark session
spark = SparkSession.builder.appName("FarePerMileAnalysis").getOrCreate()

# Assuming yellow_tripdat_df contains your PySpark DataFrame

# Filter data for March 2023
yellow_tripdata_df = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").between("2023-03-01", "2023-03-31"))
)

# Calculate fare per mile
yellow_tripdata_df = yellow_tripdata_df.withColumn(
    "fare_per_mile", col("fare_amount") / col("trip_distance")
)

# Calculate average fare per mile
avg_fare_per_mile = (
    yellow_tripdata_df.agg(avg(col("fare_per_mile")).alias("avg_fare_per_mile"))
).collect()[0]["avg_fare_per_mile"]

# Filter outliers (if average calculation is successful)
if avg_fare_per_mile:
    outliers = yellow_tripdata_df.filter(col("fare_per_mile") > 2 * avg_fare_per_mile)
    outliers.show()
else:
    print("No average fare per mile calculated or found.")

# Visualization
fare_per_mile_values = (
    yellow_tripdata_df.select("fare_per_mile").collect()
)

fare_per_mile_values = [row["fare_per_mile"] for row in fare_per_mile_values]

plt.figure(figsize=(10, 6))
plt.scatter(range(len(fare_per_mile_values)), fare_per_mile_values, alpha=0.5)
if avg_fare_per_mile:
    plt.axhline(y=avg_fare_per_mile, color="r", linestyle="--", label="Average Fare per Mile")
plt.title("Fare per Mile for March 2023 Trips")
plt.xlabel("Trip Index")
plt.ylabel("Fare per Mile")
if avg_fare_per_mile:
    plt.legend()
plt.show()



#======= Task 6
# Load the Yellow Taxi data for March 2023
yellow_march_2023 = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").between("2023-03-01", "2023-03-31"))
)

# Calculate fare per mile
fare_per_mile_df = yellow_march_2023.withColumn(
    "fare_per_mile", col("fare_amount") / col("trip_distance")
).select("fare_per_mile")

# Calculate average fare per mile if it exists
average_fare_per_mile = fare_per_mile_df.agg({"fare_per_mile": "avg"}).collect()[0][0]

if average_fare_per_mile is not None:
    # Identify outlier trips with unusually high fare per mile
    outliers_df = fare_per_mile_df.filter(col("fare_per_mile") > 3 * average_fare_per_mile)

    # Prepare data for visualization
    fare_per_mile_data = fare_per_mile_df.select("fare_per_mile").rdd.flatMap(lambda x: x).collect()

    # Create a scatter plot
    plt.figure(figsize=(8, 6))
    plt.scatter(range(len(fare_per_mile_data)), fare_per_mile_data, alpha=0.5)
    plt.axhline(y=average_fare_per_mile, color='r', linestyle='-', label='Average Fare per Mile')

    # Label axes and title
    plt.xlabel('Trip')
    plt.ylabel('Fare per Mile')
    plt.title('Fare per Mile for Yellow Taxi Trips (March 2023)')
    plt.legend()
    plt.show()
else:
    print("No average fare per mile calculated or found.")



#======= Task 7
# Calculate total trip count for Yellow Taxi dataset
total_yellow_trips = yellow_tripdata_df.count()

# Calculate total trip count for Green Taxi dataset
total_green_trips = green_tripdata_df.count()

# Calculate the number of solo trips for Yellow Taxi dataset
solo_yellow_trips = yellow_tripdata_df.filter(col("passenger_count") == 1).count()

# Calculate the number of solo trips for Green Taxi dataset
solo_green_trips = green_tripdata_df.filter(col("passenger_count") == 1).count()

# Calculate percentage of solo trips for Yellow Taxi dataset
percentage_solo_yellow = (solo_yellow_trips / total_yellow_trips) * 100

# Calculate percentage of solo trips for Green Taxi dataset
percentage_solo_green = (solo_green_trips / total_green_trips) * 100

print(f"Percentage of solo trips for Yellow Taxi dataset: {percentage_solo_yellow:.2f}%")
print(f"Percentage of solo trips for Green Taxi dataset: {percentage_solo_green:.2f}%")



#======== Task 8
# Filter Yellow Taxi data for January 2023
yellow_jan_2023 = yellow_tripdata_df.filter(
    (col("tpep_pickup_datetime").between("2023-01-01 00:00:00", "2023-01-31 23:59:59"))
)

# Calculate trip duration in hours and select necessary columns
duration_distance_df = yellow_jan_2023.withColumn(
    "trip_duration_hours", (col("tpep_dropoff_datetime").cast("timestamp").cast("long") - col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 3600
).select("trip_duration_hours", "trip_distance")

# Convert to Pandas DataFrame for plotting
duration_distance_pd = duration_distance_df.toPandas()

# Create a scatter plot to visualize the correlation
plt.figure(figsize=(8, 6))
plt.scatter(duration_distance_pd['trip_duration_hours'], duration_distance_pd['trip_distance'], alpha=0.5)
plt.xlabel('Duration (hours)')
plt.ylabel('Distance Travelled')
plt.title('Correlation between Trip Duration and Distance - Yellow Taxi (Jan 2023)')
plt.grid(True)
plt.tight_layout()
plt.show()




#====== Task 9
# Calculate total pickups per borough for yellow taxis
yellow_pickups_per_borough = yellow_tripdata_df.groupBy("Pickup_Borough").agg(count("*").alias("yellow_pickups"))

# Calculate total pickups per borough for green taxis
green_pickups_per_borough = green_tripdata_df.groupBy("Pickup_Borough").agg(count("*").alias("green_pickups"))

# Join the two datasets to combine the pickups per borough for yellow and green taxis
joined_pickups = yellow_pickups_per_borough.join(green_pickups_per_borough, "Pickup_Borough", "outer").fillna(0)

# Calculate the total pickups combining yellow and green taxis
joined_pickups = joined_pickups.withColumn("total_pickups", joined_pickups["yellow_pickups"] + joined_pickups["green_pickups"])

# Get top 10 most active boroughs based on total pickups
top_10_boroughs = joined_pickups.orderBy("total_pickups", ascending=False).limit(10)

# Determine which type of taxi (green vs yellow) had the most trips for each of the top 10 boroughs
most_used_taxi_by_borough = top_10_boroughs.withColumn("most_used_taxi",
                                                       when(col("yellow_pickups") > col("green_pickups"), "Yellow")
                                                       .otherwise("Green"))

# Convert to Pandas for plotting
plot_data = most_used_taxi_by_borough.toPandas()

# Plotting the bar plot
plt.figure(figsize=(12, 6))

colors = ['yellow' if taxi == 'Yellow' else 'green' for taxi in plot_data['most_used_taxi']]

plt.bar(plot_data['Pickup_Borough'], plot_data['total_pickups'], color=colors)
plt.xlabel('Boroughs')
plt.ylabel('Total Pickups')
plt.title('Top 10 Boroughs with Most Active Taxis (Yellow and Green)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()



#========= Task 10

from pyspark.sql.functions import month, count
import matplotlib.pyplot as plt

# Extracting month-wise trip counts for yellow taxis
yellow_monthly_counts = yellow_tripdata_df.groupBy(month("tpep_pickup_datetime").alias("pickup_month")).agg(count("*").alias("trip_count")).orderBy("pickup_month").collect()

# Extracting month-wise trip counts for green taxis
green_monthly_counts = green_tripdata_df.groupBy(month("lpep_pickup_datetime").alias("pickup_month")).agg(count("*").alias("trip_count")).orderBy("pickup_month").collect()

# Creating dictionaries to hold trip counts for each month
yellow_counts_dict = {row['pickup_month']: row['trip_count'] for row in yellow_monthly_counts}
green_counts_dict = {row['pickup_month']: row['trip_count'] for row in green_monthly_counts}

# Extracting unique months from both datasets
months = set(list(yellow_counts_dict.keys()) + list(green_counts_dict.keys()))
months = sorted(list(months))

# Extracting trip counts for yellow and green taxis for each month
yellow_counts = [yellow_counts_dict.get(month, 0) for month in months]
green_counts = [green_counts_dict.get(month, 0) for month in months]

# Plotting the bar plot
plt.figure(figsize=(10, 6))

plt.bar(months, yellow_counts, color='yellow', alpha=0.7, label='Yellow Taxi')
plt.bar(months, green_counts, color='green', alpha=0.7, label='Green Taxi')

plt.xlabel('Month')
plt.ylabel('Trip Count')
plt.title('Monthly Trip Counts for Yellow and Green Taxis')
plt.legend()

plt.tight_layout()
plt.show()

