from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number, when, count, desc
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType, FloatType, DoubleType

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("DF query 1 execution") \
    .getOrCreate()

# Define the schema for the crime dataset
crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", StringType()),
    StructField("AREA", StringType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", StringType()),
    StructField("Part 1-2", StringType()),
    StructField("Crm Cd", StringType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", StringType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", StringType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", StringType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", StringType()),
    StructField("Crm Cd 2", StringType()),
    StructField("Crm Cd 3", StringType()),
    StructField("Crm Cd 4", StringType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", StringType()),
    StructField("LON", StringType())
])

# Read the CSV file and create an RDD
rdd = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema) \
    .rdd \
    .filter(lambda x: x["Premis Desc"] == "STREET")  # Filter rows where "Premis Desc" is equal to "STREET"

# Define the time segments
def get_time_segment(row):
    if 500 <= int(row["TIME OCC"]) <= 1159:
        return "Morning"
    elif 1200 <= int(row["TIME OCC"]) <= 1659:
        return "Afternoon"
    elif 1700 <= int(row["TIME OCC"]) <= 2059:
        return "Evening"
    elif 2100 <= int(row["TIME OCC"]) or int(row["TIME OCC"]) <= 359:
        return "Night"
    else:
        return None

# Map each row to a key-value pair with TimeSegment as key and 1 as value
mapped_rdd = rdd \
    .map(lambda x: (get_time_segment(x), 1)) \
    #.filter(lambda x: x[0] is not None)  # Filter out rows with None in TimeSegment

# Reduce by key to count occurrences
result_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

# Sort by count in descending order
result_rdd = result_rdd.sortBy(lambda x: x[1], ascending=False)

# Collect the result and print it
result = result_rdd.collect()
for time_segment, count in result:
    print(f"{time_segment}: {count}")

# Stop the SparkSession
spark.stop()


