from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from datetime import datetime, timedelta

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex3') \
    .getOrCreate()


def get_dates_df():
    """
    Generate a DataFrame containing all dates in the year 2020.
    :return: A DataFrame with a single column 'flight_date' containing all dates in 2020.
    """
    # Create a dummy DataFrame with a single row to facilitate the generation of the date sequence
    dummy_df = spark.createDataFrame([Row(dummy='x')])

    date_sequence = F.sequence(F.lit("2020-01-01").cast(T.DateType()), F.lit("2020-12-31").cast(T.DateType()))

    exploded_dates = F.explode(date_sequence)
    in_dates_df = dummy_df.select(exploded_dates.alias("flight_date"))
    return in_dates_df

df_dates = get_dates_df()
#df_dates.show(5)

df_dates.write.parquet('spark_course_python/data/source/dates/', mode='overwrite')


flights_df = spark.read.parquet('spark_course_python/data/stg/flight_matched/')

df_dates = (
    df_dates
    .withColumn("day_of_week", F.dayofweek("flight_date"))
    .withColumn("day_of_month", F.dayofmonth("flight_date"))
)

max_date_df = df_dates \
    .groupBy(F.col('day_of_week'), F.col('day_of_month')) \
    .agg(F.max(F.col('flight_date')).alias('flight_date'))

max_date_df.show()

enriched_flights_df = flights_df.join(max_date_df, ['day_of_week', 'day_of_month'])

enriched_flights_df.write.parquet('spark_course_python/data/transformed/flights/',mode='overwrite')

spark.stop()