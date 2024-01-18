from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace,to_timestamp, year, month, row_number, when, count, desc, udf, expr
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, TimestampType, FloatType, DoubleType

# Create a SparkSession
spark = SparkSession \
    .builder \
    .appName("DF query 3 execution") \
    .getOrCreate()

#For shuffle_hash
spark.conf.set("spark.sql.BroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

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

income_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType()),
])
reverse_schema = StructType([
    StructField("LAT", StringType()),
    StructField("LON", StringType()),
    StructField("ZIPcode", StringType()),
])
# Read the CSV file and create a PySpark DataFrame
df = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)

income_df = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/LA_income_2015.csv", header=False, schema=income_schema)

reverse_df = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/revgecoding.csv", header=False, schema=reverse_schema)
# Apply the correct changes for the income_df
income_df = income_df.withColumn("Zip Code", col("Zip Code").cast(LongType()))
income_df = income_df.withColumn("Estimated Median Income", regexp_replace("Estimated Median Income", "[\$,]", ""))
income_df = income_df.withColumn("Estimated Median Income", col("Estimated Median Income").cast(IntegerType()))

reverse_df = reverse_df.withColumn("ZIPcode", col("ZIPcode").cast(LongType()))
reverse_df = reverse_df.withColumn("LON", col("LON").cast(DoubleType()))
reverse_df = reverse_df.withColumn("LAT", col("LAT").cast(DoubleType()))
#print('++++++++++++++++++++++++++++++++++++')
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


#Convert the Float type columns
df = df.withColumn("Premis Cd", col("Premis Cd").cast(FloatType()))
df = df.withColumn("Weapon Used Cd", col("Weapon Used Cd").cast(FloatType()))
df = df.withColumn("Crm Cd 1", col("Crm Cd 1").cast(FloatType()))
df = df.withColumn("Crm Cd 2", col("Crm Cd 2").cast(FloatType()))
df = df.withColumn("Crm Cd 3", col("Crm Cd 3").cast(FloatType()))
df = df.withColumn("Crm Cd 4", col("Crm Cd 4").cast(FloatType()))

#Convert the Double type columns
df = df.withColumn("LAT", col("LAT").cast(DoubleType()))
df = df.withColumn("LON", col("LON").cast(DoubleType()))
# We add column year
#########################################  Mporei na thelei Date Rptd (SOS)
df = df.withColumn("Year", year(col("DATE OCC")))


# We keep only the rows that Vict Descent in not null
df = df.filter(col("Vict Descent").isNotNull())

df = df.filter(col("Year") == 2015)

##
# Mapping Descent Codes to full names
descent_mapping = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian'
}

# Define a UDF (User Defined Function) to apply the mapping
@udf(StringType())
def map_descent_code(descent_code):
    return descent_mapping.get(descent_code, descent_code)

# Register the UDF
spark.udf.register("map_descent_code", map_descent_code)

# Apply the mapping to the DataFrame
df = df.withColumn("Full Descent", when(col("Vict Descent").isNotNull(), map_descent_code(col("Vict Descent"))))



##

#income_df.show()

#print('---------------------------------------------')

income_df = income_df.filter(col("Estimated Median Income").isNotNull())

income_df = income_df.filter(~((col("Zip Code") == 91046) | (col("Zip Code") == 91210)))
#unique_values = income_df.select("Estimated Median Income").distinct().show()
sorted_income_df = income_df.orderBy("Estimated Median Income")

first_3_rows = sorted_income_df.limit(3)
first_3_rows = first_3_rows.orderBy("Estimated Median Income", ascending=False)
last_3_rows = sorted_income_df.orderBy("Estimated Median Income", ascending=False).limit(3)

# Concatenate the DataFrames
new_df = last_3_rows.union(first_3_rows)
#new_df.show()
"""
# Select the first row
for i in range(5):
    print("----------------------------------------")
    row = income_df.head(i+1)[i]

    # Iterate over the columns and print the values
    for col_name, value in zip(income_df.columns, row):
        print(f"{col_name}: {value}")

"""

# Inner join of reverse_df and new_df on zip code
joined_df = reverse_df.hint("SHUFFLE_HASH").join(new_df.select("Zip Code"), reverse_df["ZIPcode"] == new_df["Zip Code"], "inner")
# Drop the extra col zip code
joined_df.drop("ZIPcode")


unique_zipcodes = joined_df.select("Zip Code").distinct()
# Show the unique values
#unique_zipcodes.show()
unique_lat_lon_pairs = joined_df.select("LAT", "LON").dropDuplicates()

# Show the unique pairs of LAT and LON
#unique_lat_lon_pairs.show()


#############################
# Join df with unique_lat_lon_pairs on LAT and LON columns
filtered_df = df.hint("SHUFFLE_HASH").join(unique_lat_lon_pairs, ["LAT", "LON"], "inner")

# Show the resulting DataFrame
filtered_df.show()

# Group by "Vict Descent" and count the rows for each group
vict_descent_counts = filtered_df.groupBy("Full Descent").count()
#
vict_descent_counts = vict_descent_counts.orderBy("count", ascending=False)
#
# Show the resulting DataFrame
vict_descent_counts.show()
