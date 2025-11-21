# Databricks notebook source
# MAGIC %md
# MAGIC # **Introduction To Filestore**
# MAGIC ## lets see different ways to upload files
# MAGIC ### simply python program
# MAGIC """
# MAGIC print(5+5)
# MAGIC """

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/nyctaxi

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

taxi_path = '/databricks-datasets/nyctaxi/tripdata/yellow'
display(dbutils.fs.ls(taxi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer

# COMMAND ----------

taxi_path = 'dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz'

df_raw = spark.read.csv(taxi_path)
df_raw.printSchema()
df_raw.show(5)

# COMMAND ----------

csv_path = "/databricks-datasets/learning-spark-v2/people/people-10m.delta"
df_raw = spark.read.format("delta").load(csv_path)
df_raw.printSchema()
df_raw.show(5)

# COMMAND ----------

df_raw.createOrReplaceTempView("taxi_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from taxi_raw limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from samples.nyctaxi.trips limit 10

# COMMAND ----------

df_raw = spark.table("samples.nyctaxi.trips")

df_raw.printSchema()
df_raw.select("trip_distance", "fare_amount").show(5)


# COMMAND ----------

from pyspark.sql.functions import col

df_silver=(
     df_raw
    .filter(col("trip_distance") >= 0 )
    .filter(col("fare_amount") >= 0 )
)

df_silver.printSchema()
df_silver.show(25)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_silver=(
    df_silver
    .withColumnRenamed("tpep_pickup_datetime", "pickup_time")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_time")
    .withColumn("pickup_time", to_timestamp(col("pickup_time")))
    .withColumn("dropoff_time", to_timestamp("dropoff_time"))
)
# df_silver.show(5)
display(df_silver.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

from pyspark.sql.functions import to_date, sum as _sum, count

df_gold_revenue=(
    df_silver
    .withColumn("day", to_date("pickup_time"))  #convert
    # pickup_time to date
    .groupBy("day")
    .agg(
        _sum("fare_amount").alias("total_fare_revenue"),
        count("*").alias("num_trips")
        #how many trips per day 
        # you could also add _sum("trip_distance").alias ("total_distance")
    ).orderBy("day")
)
display(df_gold_revenue)
# df_gold_revenue.show(5)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi_demo_db;
# MAGIC USE taxi_demo_db;

# COMMAND ----------

df_silver.write.mode("overwrite").format("delta").saveAsTable("taxi_demo_db.trips_silver")
df_gold_revenue.write.mode("overwrite").format("delta").saveAsTable("taxi_demo_db.daily_revenue_gold")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT day , total_fare_revenue, num_trips FROM taxi_demo_db.daily_revenue_gold
# MAGIC ORDER BY day;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- what is my current database ?
# MAGIC SELECT current_database();
# MAGIC
# MAGIC  -- LIST ALL DATABASES KNOWN TO HIVE METASTORE
# MAGIC  SHOW DATABASES;