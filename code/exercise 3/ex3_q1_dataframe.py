from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType, DoubleType

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("DF query 1 execution dd") \
    .getOrCreate()

# Define the schema for the crime dataset
crime_schema = StructType([
    StructField("DR_NO", IntegerType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", TimestampType()),
    StructField("AREA", IntegerType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", IntegerType()),
    StructField("Part 1-2", IntegerType()),
    StructField("Crm Cd", IntegerType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", IntegerType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", DoubleType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", DoubleType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", DoubleType()),
    StructField("Crm Cd 2", DoubleType()),
    StructField("Crm Cd 3", DoubleType()),
    StructField("Crm Cd 4", DoubleType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", FloatType()),
    StructField("LON", FloatType())
])

# Read the CSV file and create a PySpark DataFrame
#df = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)


# Read the CSV files
df_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)
df_2020_to_present = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2020_to_present.csv", header=False, schema=crime_schema)

# Union the two datasets

# Rename the 'AREA ' column in df2010 to 'AREA'
df_2010_to_2019= df_2010_to_2019.withColumnRenamed('AREA ', 'AREA')
df = df_2010_to_2019.unionByName(df_2020_to_present).distinct()

# Convert 'Date Rptd' column to timestamp format and filter out rows where it is null
df = df.withColumn("Date Rptd", to_timestamp(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
df = df.filter(col("Date Rptd").isNotNull())

df = df.withColumn("DATE OCC", to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
df = df.filter(col("DATE OCC").isNotNull())
# Add "Year" and "Month" columns
df = df.withColumn("Year", year(col("Date Rptd")))
df = df.withColumn("Month", month(col("Date Rptd")))

# Count the rows for each year and month
crimes_by_year_month = df.groupBy("Year", "Month").count().orderBy("Year", "Month")

# Add a row number column for each year
window_spec = Window.partitionBy("Year").orderBy(col("count").desc())
crimes_by_year_month = crimes_by_year_month.withColumn("#", row_number().over(window_spec))


# Rename the "count" column to "crime_total"
crimes_by_year_month = crimes_by_year_month.withColumnRenamed("count", "crime_total")


# Select the top 3 months for each year
top_3_months_by_year = crimes_by_year_month.filter(col("#") <= 3)

# Show the result
top_3_months_by_year.show()

