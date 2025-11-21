# Databricks notebook source
# MAGIC %md
# MAGIC ## HOW to see OPTIMIZE Doing Real Work 
# MAGIC Step 1: Write many Files artificially

# COMMAND ----------

for i in range (20):
    df= spark.createDataFrame([(i, 'name'+str(i), 1000+i)], ['id', 'name', 'salary'])
    df.write.format("delta").mode("append").save("/Volumes/workspace/default/tmp/employees_delta_big")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **STEP 2: Run OPTIMIZE**

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta. `/Volumes/workspace/default/tmp/employees_delta_big`;