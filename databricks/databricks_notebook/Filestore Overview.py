# Databricks notebook source
# MAGIC %sql
# MAGIC -- what is my current database ?
# MAGIC SELECT current_database();
# MAGIC
# MAGIC  -- LIST ALL DATABASES KNOWN TO HIVE METASTORE
# MAGIC  SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE if not EXISTS training_db;
# MAGIC use training_db;

# COMMAND ----------

data =[
    (1, "Ravi", "Electronics", 1500.0),
    (2, "Meena", "Grocery", 500.0),
    (3, "Arjun", "Clothing", 800.0),
    (4,"Priya","Electronics",900.0)
]
columns=["order_id", "customer_name", "category", "amount"]
df_orders=spark.createDataFrame(data, columns)
df_orders.show()


# COMMAND ----------

df_orders.write.mode("overwrite").saveAsTable("training_db.daily_orders_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE training_db;
# MAGIC SHOW TABLES;
# MAGIC
# MAGIC DESCRIBE EXTENDED daily_orders_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC # now connect to aws and store data inside the aws 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.default.orders_external2 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   amount DOUBLE
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION 's3://trgdemobucket1/raw/orders_external2';
# MAGIC