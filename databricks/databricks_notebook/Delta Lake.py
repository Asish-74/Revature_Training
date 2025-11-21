# Databricks notebook source
# MAGIC %md
# MAGIC # **STEP 1: CREATE DELTA TABLE**

# COMMAND ----------

data = [
  (1,"Ram", 50000),
  (2,"Sita", 60000),
  (3,"Laxman", 55000)
]
columns=["id", "name", "salary"]

df=spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 2: Store data as delta table**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/tmp/employees_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 3: Read Delta Table** 

# COMMAND ----------

df_delta=spark.read.format("delta").load("/Volumes/workspace/default/tmp/employees_delta")
df_delta.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP4: **

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable= DeltaTable.forPath(spark, "/Volumes/workspace/default/tmp/employees_delta")

deltaTable.update(
    condition="id=1", set={"salary" : "70000"}
)
display(deltaTable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-read table:
# MAGIC convert the delta table to data frame then we can read the table because the delta table contain a transaction address

# COMMAND ----------

display(spark.read.format("delta").load("/Volumes/workspace/default/tmp/employees_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 5: ACID TRANSACTION- (Delete)**

# COMMAND ----------

deltaTable.delete("id=3")
display(spark.read.format("delta").load("/Volumes/workspace/default/tmp/employees_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 6: ACID TRANSACTION - Merge(Upsert)**

# COMMAND ----------

new_data=[
    (2, "Sita", 65000), # updated row 
    (4, "Laxman", 62000) # new row
]
columns=["id", "name", "salary"]
df_new=spark.createDataFrame(new_data,columns)
# df_new.show()
display(df_new)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 7: ACID TRANSACTION - UPDATE**

# COMMAND ----------

deltaTable.alias("old") \
    .merge(df_new.alias("new"), "old.id = new.id") \
    .whenMatchedUpdate(set = { "salary": "new.salary" }) \
    .whenNotMatchedInsertAll() \
    .execute()
# display(spark.read.format("delta").load("/Volumes/workspace/default/tmp/employees_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Re-read Table**

# COMMAND ----------

display(spark.read.format("delta").load("/Volumes/workspace/default/tmp/employees_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 8: TIME TRAVEL DEMO (Version History)**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY "/Volumes/workspace/default/tmp/employees_delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Read Version 0**

# COMMAND ----------

df_v0= spark.read.format("delta").option("versionAsOf", 0).load("/Volumes/workspace/default/tmp/employees_delta")
display(df_v0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 9: View Delta Transaction Log**

# COMMAND ----------

# MAGIC %fs ls /Volumes/workspace/default/tmp/employees_delta/_delta_log/

# COMMAND ----------

# MAGIC %md
# MAGIC **Metadata**

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/tmp/employees_delta/_delta_log/00000000000000000000.json

# COMMAND ----------

# MAGIC %md
# MAGIC ## **CASE 1: failed to Update -> Rollback**

# COMMAND ----------

try:
    deltaTable.update(
        condition= "id = 'ABC'", # wrong type
        set= {"salary" : "90000"}
    )
    print("No exception raised")
except Exception as e:
    print("Exception raised: ", e)