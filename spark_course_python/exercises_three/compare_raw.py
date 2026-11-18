from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex3') \
    .getOrCreate()

#flights_raw_df = spark.read.csv('spark_course_python/data/raw/flights/', header=True)
flights_raw_df = spark.read.parquet("spark_course_python/data/source/flights_raw/")

flights_df = spark.read.parquet("spark_course_python/data/source/flights/")



# Remove duplicate records from both DataFrames
flights_distinct_df = flights_df.dropDuplicates()
flights_raw_distinct_df = flights_raw_df.dropDuplicates()


matched_df = flights_df.intersect(flights_raw_df)
#matched_df.show()
# Identify unmatched records and add a new column to annotate the source DataFrame
unmatched_flights = flights_distinct_df.subtract(matched_df) \
    .withColumn('source_of_data', F.lit('flights'))
unmatched_flights_raw = flights_raw_distinct_df.subtract(matched_df) \
    .withColumn('source_of_data', F.lit('flights_raw'))
# Union the unmatched records from both DataFrames
unmatched_df = unmatched_flights.union(unmatched_flights_raw)


# Write matched and unmatched DataFrames to S3 as Parquet files
matched_df.write.parquet('spark_course_python/data/stg/flight_matched/', mode='overwrite')

unmatched_df.write.parquet('spark_course_python/data/stg/flights_unmatched/', mode='overwrite')
# Stop the Spark session to release resources
spark.stop()

