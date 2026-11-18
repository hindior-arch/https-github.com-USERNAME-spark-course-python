# Importing necessary modules for the exercise
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
# Initialize a Spark session. This is an entry point to any Spark functionality.
# The master is set to local, which means Spark runs on your local machine using all available cores.
# The appName helps you identify the application in the Spark Web UI.
spark = SparkSession.builder.master("local").appName('ex2_airports').getOrCreate()
# Read a CSV file located in S3 into a Spark DataFrame.
# The parameter 'header=True' indicates that the first row of the file contains headers.
airports_raw_df = spark.read.csv('spark_course_python/data/raw/airports/', header=True)
# Transform the data:
# Cast the column 'airport_id' to IntegerType
# Select only the columns that are necessary (airport_id, city, state, name)
# Each transformation is performed using Spark SQL functions, imported as F.
# The types are used to enforce the schema, imported as T.
airports_df = airports_raw_df.select(
    F.col('airport_id').cast(T.IntegerType()).alias('airport_id'),
    F.col('city'),
    F.col('state'),
    F.col('name')
)
# Write the DataFrame back into S3 in Parquet format.
# 'mode=overwrite' means that if the directory already exists in S3, it will be replaced.
airports_df.write.parquet('spark_course_python/data/source/airports/', mode='overwrite')
# Stop the Spark session to release resources
spark.stop()