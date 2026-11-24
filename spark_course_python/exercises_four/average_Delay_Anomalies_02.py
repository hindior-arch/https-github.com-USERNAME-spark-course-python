from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex4_anomalies_detection') \
    .getOrCreate()



all_history_window = Window.partitionBy("carrier").rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)



flights_df = spark.read.parquet('spark_course_python/data/transformed/flights/')

flights_df.cache()

delays_df = flights_df.withColumn("avg_till_now",F.avg("arr_delay").over(all_history_window) ) \
    .withColumn("avg_diff_percent",F.abs((F.col("arr_delay") / F.col("avg_till_now"))) )


delays_df.filter(F.col("avg_diff_percent") > F.lit(5))\
.show(10)

flights_df.unpersist()

spark.stop()