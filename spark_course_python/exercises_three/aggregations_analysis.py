from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from datetime import datetime, timedelta

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex3_aggregations') \
    .getOrCreate()


flights_df = spark.read.parquet('spark_course_python/data/transformed/flights/')
airports_df = spark.read.parquet('spark_course_python/data/source/airports/')


#flights_df.printSchema()
#flights_df.show(5)

#filtered_df = flights_df.filter(F.col("origin_airport_id") == 10299)
#filtered_df.show()

def airports_departures():
    """
    Calculate the number of departures for each airport and display the top airports by number of departures.
    """
    number_of_departures_df = flights_df \
        .groupBy('origin_airport_id') \
        .agg(F.count('*').alias('number_of_departures')) \
        .withColumnRenamed('origin_airport_id', 'airport_id') 

    top_airport_departures_df = number_of_departures_df \
        .join(
            airports_df,
            number_of_departures_df.airport_id == airports_df.airport_id,
            "inner"
        ) \
        .select(
            number_of_departures_df["airport_id"],
            number_of_departures_df["number_of_departures"],
            airports_df["name"].alias("airport_name")
        ) \
        .orderBy(F.desc("number_of_departures"))

    return top_airport_departures_df

def airports_arrivals():
    """
    Calculate the number of arrivals for each airport and display the top airports by number of arrivals.
    """   
    number_of_arrivals_df = flights_df \
            .groupBy('dest_airport_id') \
            .agg(F.count('*').alias('number_of_arrivals')) \
            .withColumnRenamed('dest_airport_id', 'airport_id') 
    
    top_airport_arrivals_df = number_of_arrivals_df \
            .join(
                airports_df,
                number_of_arrivals_df.airport_id == airports_df.airport_id,
                "inner"
            ) \
            .select(
                number_of_arrivals_df["airport_id"],
                number_of_arrivals_df["number_of_arrivals"],
                airports_df["name"].alias("airport_name")
            ) \
            .orderBy(F.desc("number_of_arrivals"))
    
    return top_airport_arrivals_df


#airports_departures().show()
airports_arrivals().show()



#airports_df.printSchema()





spark.stop()