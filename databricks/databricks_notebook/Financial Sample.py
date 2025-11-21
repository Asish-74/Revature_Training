# Databricks notebook source
# MAGIC %md
# MAGIC ## Installing Library & Reading Excel

# COMMAND ----------

# MAGIC %md
# MAGIC # **Bronze Layer**

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting Pandas → Spark DataFrame

# COMMAND ----------

import pandas as pd
pdf= pd.read_excel("/Volumes/workspace/default/myvolume/Financial Sample.xlsx")

df=spark.createDataFrame(pdf)
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # **SILVER LAYER**

# COMMAND ----------

for col_name in df.columns:
    df=df.withColumnRenamed(col_name, col_name.replace(" ", "_"))

# COMMAND ----------

from pyspark.sql.functions import col

num_cols= [
    "Units_Sold",
    "Manufacturing_Price",
    "Sale_Price",
    "Gross_Sales",
    "Discounts",
    "_Sales",
    "COGS",
    "_Profit"
]

for c in num_cols:
    if c in df.columns:
        df=df.withColumn(c, col(c).cast("double"))

# COMMAND ----------

df_silver= df.dropna()
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

from pyspark.sql import functions as F
df_gold= df_silver.groupBy("Country").agg(
    F.sum("_Sales").alias("Total_Sales"),
    F.sum("Profit").alias("Total_Profit"),
    F.sum("COGS").alias("Total_COGS"),
    F.avg("Discounts").alias("Avg_Discounts")
    )
display(df_gold)


# COMMAND ----------

# Total Sales By Country

from pyspark.sql.functions import sum

df_country_sales={
    df_silver.groupBy("Country")
    .agg(sum("_Sales").alias("Total_Sales"))
    .orderBy("Total_Sales", ascending=False)
}

display(df_country_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC **## ✅ BLOCK 2 — Converting Pandas → Spark DataFrame**
# MAGIC Code
# MAGIC df = spark.createDataFrame(pdf)
# MAGIC display(df)
# MAGIC df.printSchema()
# MAGIC
# MAGIC ### Explanation
# MAGIC 4️⃣ spark.createDataFrame(pdf)
# MAGIC spark → SparkSession object
# MAGIC createDataFrame() → converts Pandas DataFrame → Spark DataFrame
# MAGIC pdf → your pandas data
# MAGIC df → Spark DataFrame created from the Excel data
# MAGIC ### 👉 Why need to convert?
# MAGIC Because Spark transformations require a Spark DataFrame, not Pandas.
# MAGIC
# MAGIC 5️⃣ display(df)
# MAGIC Databricks function to display Spark DataFrame in table format.
# MAGIC 6️⃣ df.printSchema()
# MAGIC Shows the schema (column names + data types).
# MAGIC
# MAGIC **# ✅ BLOCK 3 — Renaming Columns (Remove Spaces)**
# MAGIC Code
# MAGIC for col_name in df.columns:
# MAGIC     df = df.withColumnRenamed(col_name, col_name.replace(" ", "_"))
# MAGIC
# MAGIC Explanation
# MAGIC 7️⃣ for col_name in df.columns:
# MAGIC Loops through all column names in DataFrame
# MAGIC df.columns → list of column names
# MAGIC 8️⃣ df.withColumnRenamed(old, new)
# MAGIC Renames a column in Spark
# MAGIC old = original name
# MAGIC new = updated name
# MAGIC 9️⃣ col_name.replace(" ", "_")
# MAGIC Replaces spaces in column names with _.
# MAGIC Example:
# MAGIC Units Sold → Units_Sold
# MAGIC 👉 Why?
# MAGIC Spaces cause errors in Spark SQL. Underscore is safer.
# MAGIC
# MAGIC
# MAGIC **# ✅ BLOCK 4 — Casting Numeric Columns**
# MAGIC 12️⃣ df = df.withColumn(c, col(c).cast("double"))
# MAGIC col(c) → selects the column
# MAGIC .cast("double") → change datatype to double
# MAGIC withColumn → update column
# MAGIC 👉 Why casting needed?
# MAGIC Excel sometimes loads numeric values as string, and Spark cannot perform sum/avg on string values.
# MAGIC
# MAGIC ## **BLOCK 6 — Creating Gold Layer (Aggregations)**
# MAGIC Code
# MAGIC from pyspark.sql import functions as F
# MAGIC df_gold = df_silver.groupBy("Country").agg(
# MAGIC     F.sum("_Sales").alias("Total_Sales"),
# MAGIC     F.sum("Profit").alias("Total_Profit"),
# MAGIC     F.sum("COGS").alias("Total_COGS"),
# MAGIC     F.avg("Discounts").alias("Avg_Discounts")
# MAGIC )
# MAGIC display(df_gold)
# MAGIC 14️⃣ groupBy("Country")
# MAGIC Group data by Country → creates groups.
# MAGIC 15️⃣ agg()
# MAGIC Apply multiple aggregations.
# MAGIC 16️⃣ F.sum("_Sales")
# MAGIC Sum all Sales per country.
# MAGIC 17️⃣ .alias("Total_Sales")
# MAGIC Rename column.
# MAGIC 18️⃣ df_gold
# MAGIC Your final Gold layer containing aggregated metrics.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##  **FULL WORKFLOW OF DATABRICKS**
# MAGIC
# MAGIC # ⭐ WHEN YOU HAVE 2 OR MORE DATASETS — WHAT IS THE FIRST STEP?
# MAGIC
# MAGIC The first step is always:
# MAGIC
# MAGIC ### ✅ **Step 1: Bring all datasets into the BRONZE layer (Raw Layer)**
# MAGIC
# MAGIC No cleaning.
# MAGIC No changing.
# MAGIC No filtering.
# MAGIC No joining.
# MAGIC Just **store the raw files exactly as they are**.
# MAGIC
# MAGIC ### ✔ What you should do:
# MAGIC
# MAGIC * Read Dataset 1
# MAGIC * Read Dataset 2
# MAGIC * Read Dataset 3
# MAGIC * …
# MAGIC * Save all of them as **Bronze Delta Tables**
# MAGIC
# MAGIC ### ✔ Why?
# MAGIC
# MAGIC Because raw data must be preserved for audit, debugging, and reprocessing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ FULL PROCESS (VERY IMPORTANT)
# MAGIC
# MAGIC Below is your full professional pipeline:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🟫 **STEP 1: BRONZE LAYER (Raw Ingestion)**
# MAGIC
# MAGIC ### WHAT YOU DO:
# MAGIC
# MAGIC * Load Excel, CSV, JSON, Parquet, or API data
# MAGIC * No cleaning
# MAGIC * Convert to Spark DataFrame
# MAGIC * Save as raw Delta Table
# MAGIC
# MAGIC ### EXAMPLE:
# MAGIC
# MAGIC ```python
# MAGIC df1_raw = spark.read.format("csv").option("header", "true").load("path/file1.csv")
# MAGIC df2_raw = spark.read.format("csv").option("header", "true").load("path/file2.csv")
# MAGIC
# MAGIC df1_raw.write.format("delta").saveAsTable("bronze.file1")
# MAGIC df2_raw.write.format("delta").saveAsTable("bronze.file2")
# MAGIC ```
# MAGIC
# MAGIC ### PURPOSE:
# MAGIC
# MAGIC * Keep original data safe
# MAGIC * No changes to schema
# MAGIC * Acts as single-source-of-truth
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🟪 **STEP 2: SILVER LAYER (Cleaning + Standardizing)**
# MAGIC
# MAGIC ### WHAT YOU DO:
# MAGIC
# MAGIC For **each dataset separately**, clean and transform:
# MAGIC
# MAGIC ### ✔ Cleaning operations:
# MAGIC
# MAGIC * Rename columns
# MAGIC * Fix datatypes
# MAGIC * Remove null/bad rows
# MAGIC * Remove duplicates
# MAGIC * Standardize formats (dates, names, IDs)
# MAGIC * Apply data quality rules
# MAGIC
# MAGIC ### EXAMPLE:
# MAGIC
# MAGIC ```python
# MAGIC df1_silver = df1_raw \
# MAGIC     .withColumnRenamed("Units Sold", "Units_Sold") \
# MAGIC     .dropna()
# MAGIC
# MAGIC df2_silver = df2_raw \
# MAGIC     .withColumn("Price", col("Price").cast("double"))
# MAGIC ```
# MAGIC
# MAGIC ### PURPOSE:
# MAGIC
# MAGIC * Make data usable
# MAGIC * Prepare for joining
# MAGIC * Ensure schema consistency
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ **WHEN DO I JOIN TWO DATASETS?**
# MAGIC
# MAGIC JOINING ALWAYS HAPPENS IN **SILVER or GOLD**, not Bronze.
# MAGIC
# MAGIC ### If you want:
# MAGIC
# MAGIC * Cleaned + Joined data → **Silver**
# MAGIC * Business metrics → **Gold**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # 🟨 **STEP 3: GOLD LAYER (Business Aggregations + KPIs)**
# MAGIC
# MAGIC ### WHAT YOU DO:
# MAGIC
# MAGIC * Join Dataset 1 + Dataset 2 using common keys
# MAGIC * Create reports, summaries, dashboards
# MAGIC * Calculate KPIs
# MAGIC
# MAGIC ### EXAMPLE:
# MAGIC
# MAGIC ```python
# MAGIC df_gold = df1_silver.join(df2_silver, "Customer_ID") \
# MAGIC     .groupBy("Country") \
# MAGIC     .agg(sum("Sales").alias("Total_Sales"))
# MAGIC ```
# MAGIC
# MAGIC ### PURPOSE:
# MAGIC
# MAGIC * Ready for BI tools (Power BI, Tableau)
# MAGIC * Business analytics layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ **FULL VISUAL PIPELINE**
# MAGIC
# MAGIC ```
# MAGIC           DATASET 1    DATASET 2    DATASET 3
# MAGIC
# MAGIC                 ↓          ↓           ↓
# MAGIC         ┌──────────────────────────────────────────┐
# MAGIC         │               🟫 BRONZE LAYER             │
# MAGIC         │     Raw files (no cleaning, no join)     │
# MAGIC         └──────────────────────────────────────────┘
# MAGIC                          ↓
# MAGIC         ┌──────────────────────────────────────────┐
# MAGIC         │               🟪 SILVER LAYER             │
# MAGIC         │ Cleaning, fixing schema, datatypes,      │
# MAGIC         │ removing nulls, JOINING datasets          │
# MAGIC         └──────────────────────────────────────────┘
# MAGIC                          ↓
# MAGIC         ┌──────────────────────────────────────────┐
# MAGIC         │               🟨 GOLD LAYER               │
# MAGIC         │ Business reports, KPIs, aggregations     │
# MAGIC         └──────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ⭐ WHAT YOU MUST ALWAYS DO FIRST WHEN YOU HAVE 2 DATASETS
# MAGIC
# MAGIC ### 🔥 **Step 1: Bring both datasets into BRONZE exactly as they are**
# MAGIC
# MAGIC ✔ Dataset 1 → Bronze
# MAGIC ✔ Dataset 2 → Bronze
# MAGIC
# MAGIC AFTER THAT:
# MAGIC
# MAGIC ### ✔ Step 2: Clean them individually → Silver
# MAGIC
# MAGIC ### ✔ Step 3: Join if needed → Silver/Gold
# MAGIC
# MAGIC ### ✔ Step 4: Create KPIs → Gold
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # **Data Lake**
# MAGIC
# MAGIC ## Q: What is a Delta Table?
# MAGIC - A Delta Table is an enhanced Parquet-based storage format that provides ACID transactions and versioning.
# MAGIC - It uses a transaction log (_delta_log) to ensure reliability and consistency.
# MAGIC - Supports time travel, schema enforcement, schema evolution, upsert, merge, and delete operations.
# MAGIC - Enables scalable ETL, analytics, streaming, and real-time pipelines in Lakehouse architecture.
# MAGIC - Delta is the default and most powerful table format in Databricks.
# MAGIC
# MAGIC **Delta Table = parquet + ACID transaction**
# MAGIC
# MAGIC 1. ACID Transactions
# MAGIC Reliable writes & updates even under failures.
# MAGIC 2. Schema Enforcement
# MAGIC Prevents bad data from entering your table.
# MAGIC 3. Schema Evolution
# MAGIC Allows adding new columns automatically.
# MAGIC 4. Time Travel
# MAGIC Query previous versions of data.
# MAGIC 5. Upserts (MERGE)
# MAGIC Support for UPDATE + INSERT in one operation.
# MAGIC 6. Deletes & Updates
# MAGIC Hard to do in Parquet — easy in Delta.
# MAGIC 7. Audit & Versioning
# MAGIC Every write becomes a new version.
# MAGIC
# MAGIC ### **What Databricks does:**
# MAGIC df1_raw.write.format("delta").saveAsTable("bronze.file1")
# MAGIC 🔹 Step 1
# MAGIC Writes DataFrame as Parquet files
# MAGIC 🔹 Step 2
# MAGIC Creates _delta_log folder
# MAGIC 🔹 Step 3
# MAGIC Registers table inside Hive Metastore under the name bronze.file1
# MAGIC 🔹 Step 4
# MAGIC
# MAGIC bronze.file1/
# MAGIC    ├── part-000.parquet
# MAGIC    ├── part-001.parquet
# MAGIC    └── _delta_log/
# MAGIC          ├── 000000000000.json
# MAGIC          ├── 000000000001.json
# MAGIC
# MAGIC
# MAGIC