from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number, when, count, desc, udf, sqrt, pow, mean
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType, FloatType, DoubleType
from math import radians, cos, sin, asin, sqrt


# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("DF query 1 execution") \
    .getOrCreate()


@udf(returnType = DoubleType())
def distance(lat1, lon1, lat2, lon2):

    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return (c * r)

# Register the UDF
spark.udf.register("distance_udf", distance)

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

police_schema = StructType([
    StructField("X", StringType()),
    StructField("Y", StringType()),
    StructField("FID", StringType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("PREC", StringType()),
])
# Read the CSV file and create a PySpark DataFrame
# Read the CSV files
df_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)
df_2020_to_present = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2020_to_present.csv", header=False, schema=crime_schema)

# Union the two datasets

# Rename the 'AREA ' column in df2010 to 'AREA'
df_2010_to_2019= df_2010_to_2019.withColumnRenamed('AREA ', 'AREA')
df = df_2010_to_2019.unionByName(df_2020_to_present).distinct()


police_df = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/LAPD_Police_Stations.csv", header=False, schema=police_schema)


#########################################################

police_df = police_df.withColumn("X", col("X").cast(DoubleType()))
police_df = police_df.withColumn("Y", col("Y").cast(DoubleType()))
police_df = police_df.withColumn("FID", col("FID").cast(IntegerType()))
police_df = police_df.withColumn("PREC", col("PREC").cast(LongType()))

#########################################################

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
df = df.withColumn("Year", year(col("DATE OCC")))


# Register the DataFrames as SQL temporary tables
df.createOrReplaceTempView("crime_table")
police_df.createOrReplaceTempView("police_table")

# Filter the DataFrame to keep only rows where Weapon Used Cd is in the range [100, 199]
spark.sql(  """
            SELECT *
            FROM crime_table
            WHERE `Weapon Used Cd` >= 100 AND `Weapon Used Cd` <= 199
            """).createOrReplaceTempView("filtered_table")

# Join police_df and filtered_df based on AREA and PREC columns
spark.sql(  """
            SELECT p.Y, p.X, f.LAT, f.LON, f.Year
            FROM police_table p
            JOIN filtered_table f
            ON p.PREC = f.AREA
            """).createOrReplaceTempView("joined_table")

# Calculate geodesic distance
spark.sql(  """
            SELECT *, distance_udf(LAT, LON, Y, X) AS distance
            FROM joined_table
            """).createOrReplaceTempView("result_table")

# Group by "Year" and compute the mean distance and count
grouped_df = spark.sql( """
                        SELECT Year, AVG(distance) AS mean_distance, COUNT(Year) AS count_per_year
                        FROM result_table
                        GROUP BY Year
                        ORDER BY Year
                        """)

grouped_df.show()
