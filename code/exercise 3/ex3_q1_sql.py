from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, row_number
from pyspark.sql.window import Window
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
#df = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)

# Read the CSV files
df_2010_to_2019 = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2010_to_2019.csv", header=False, schema=crime_schema)
df_2020_to_present = spark.read.csv("hdfs://okeanos-master:54310/main_dataset/2020_to_present.csv", header=False, schema=crime_schema)

# Union the two datasets
# Rename the 'AREA ' column in df2010 to 'AREA'
df_2010_to_2019= df_2010_to_2019.withColumnRenamed('AREA ', 'AREA')
df = df_2010_to_2019.unionByName(df_2020_to_present).distinct()

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

# Select the second row (index 1 since PySpark uses 0-based indexing)
row = df.head(5)[4]

# Iterate over the columns and print the values
#for col_name, value in zip(df.columns, row):
#    print(f"{col_name}: {value}")

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("crime_data")


# List to store the result DataFrames
result_dfs = []

# Loop through each year from 2010 to 2024
# Loop through each year from 2010 to 2024
for current_year in range(2010, 2024):
    # SQL query to count rows for each year and month, filter for the current year, and limit to the top 3 months
    sql_query = f"""
        SELECT
            {current_year} AS Year,
            Month,
            COUNT(*) AS number_of_crimes,
            ROW_NUMBER() OVER (PARTITION BY Year, Month ORDER BY COUNT(*) DESC) AS row_num
        FROM crime_data
        WHERE Year = {current_year}
        GROUP BY Year, Month
        ORDER BY number_of_crimes DESC
        LIMIT 3
    """
    # Execute the SQL query
    result_df = spark.sql(sql_query)

    # Append the result DataFrame to the list
    result_dfs.append(result_df)

# Combine the result DataFrames for each year into a single DataFrame
final_result_df = result_dfs[0]
for result_df in result_dfs[1:]:
    final_result_df = final_result_df.union(result_df)

# Show the final result
final_result_df.show()
