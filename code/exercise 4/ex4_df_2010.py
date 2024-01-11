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

# Read the CSV file and create a PySpark DataFrame
df = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)


# Convert the Integer type columns
df = df.withColumn("DR_NO", col("DR_NO").cast(LongType()))  #Int 64
df = df.withColumn("TIME OCC", col("TIME OCC").cast(IntegerType()))
df = df.withColumn("AREA", col("AREA").cast(IntegerType()))
df = df.withColumn("Rpt Dist No", col("Rpt Dist No").cast(IntegerType()))
df = df.withColumn("Part 1-2", col("Part 1-2").cast(IntegerType()))
df = df.withColumn("Crm Cd", col("Crm Cd").cast(IntegerType()))
df = df.withColumn("Vict Age", col("Vict Age").cast(IntegerType()))

##
df = df.withColumn("Date Rptd", to_timestamp(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
df = df.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))

#Conver the Float type columns
df = df.withColumn("Premis Cd", col("Premis Cd").cast(FloatType()))
df = df.withColumn("Weapon Used Cd", col("Weapon Used Cd").cast(FloatType()))
df = df.withColumn("Crm Cd 1", col("Crm Cd 1").cast(FloatType()))
df = df.withColumn("Crm Cd 2", col("Crm Cd 2").cast(FloatType()))
df = df.withColumn("Crm Cd 3", col("Crm Cd 3").cast(FloatType()))
df = df.withColumn("Crm Cd 4", col("Crm Cd 4").cast(FloatType()))

#Convert the Double type columns
df = df.withColumn("LAT", col("LAT").cast(DoubleType()))
df = df.withColumn("LON", col("LON").cast(DoubleType()))


# Add "Year" and "Month" columns
df = df.withColumn("Year", year(col("Date Rptd")))
df = df.withColumn("Month", month(col("Date Rptd")))
#df.show(2)

# Define the time segments
morning = (col("TIME OCC") >= 500) & (col("TIME OCC") <= 1159)
afternoon = (col("TIME OCC") >= 1200) & (col("TIME OCC") <= 1659)
evening = (col("TIME OCC") >= 1700) & (col("TIME OCC") <= 2059)
night = ((col("TIME OCC") >= 2100) | (col("TIME OCC") <= 359))

# Create a new column 'TimeSegment' based on the time segments
df = df.withColumn("TimeSegment", when(morning, "Morning")
                               .when(afternoon, "Afternoon")
                               .when(evening, "Evening")
                               .when(night, "Night"))

# Filter rows where "Premis Desc" is equal to "STREET"
df_street = df.filter(col("Premis Desc") == "STREET")

# Group by TimeSegment and count the number of rows for each group
time_segment_counts = df_street.groupBy("TimeSegment").agg(count("*").alias("Count"))

# Order the result in descending order
time_segment_counts = time_segment_counts.orderBy(desc("Count"))

# Show the resulting DataFrame
time_segment_counts.show()
