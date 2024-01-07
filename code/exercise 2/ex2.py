from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace,to_timestamp, to_date, year, month, row_number, when, count, desc
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType, FloatType, DoubleType

# Create a SparkSession
spark = SparkSession \
        .builder \
        .appName("DF query 3 execution") \
        .getOrCreate()

# Define the schema for the crime dataset
crime_schema = StructType([
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("Vict Age", StringType()),
    StructField("LAT", StringType()),
    StructField("LON", StringType())
])

# Read the CSV files
df_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)
df_2020_to_present = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2020_to_present.csv", header=False, schema=crime_schema)

# Union the two datasets

# Rename the 'AREA ' column in df2010 to 'AREA'
df_2010_to_2019= df_2010_to_2019.withColumnRenamed('AREA ', 'AREA')
unioned_df = df_2010_to_2019.unionByName(df_2020_to_present).distinct()


# Convert column types
unioned_df = unioned_df.withColumn("LAT", col("LAT").cast(DoubleType()))
unioned_df = unioned_df.withColumn("LON", col("LON").cast(DoubleType()))
unioned_df = unioned_df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
unioned_df = unioned_df.withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
unioned_df = unioned_df.withColumn("Vict Age", col("Vict Age").cast(IntegerType()))

# Print total number of rows
print("Total number of rows in the unioned dataset:", unioned_df.count())


# Print data types of each column
print("Data types of each column in the unioned dataset:")
unioned_df.printSchema()
