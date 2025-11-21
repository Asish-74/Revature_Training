# Databricks notebook source
# MAGIC %md
# MAGIC # Create Raw Data in Databricks 

# COMMAND ----------

raw_products = [
    ("P001", "Dove Shampoo", "Beauty", 199.50),
    ("P002", "Samsung Earbuds", "Electronics", 399.00),
    ("P003", "Dell Mouse", "Electronics", 149.00),
    ("P004", "Lakme Face Wash", "Beauty", 249.00),
    ("P005", "Boat Headphones", "Electronics", 1299.00),
    ("P006", "Pepsodent Toothpaste", "Grocery", 89.00),
    ("P007", "Sunfeast Biscuits", "Grocery", 35.00),
    ("P008", "Amul Butter", "Grocery", 55.00),
    ("P009", "Parle-G Biscuits", "Grocery", 25.00),
    ("P010", "Tata Salt", "Grocery", 45.00),
    ("P011", "iPhone Charger", "Electronics", 1599.00),
    ("P012", "Nivea Cream", "Beauty", 199.00),
    ("P013", "Britannia Cake", "Grocery", 45.00),
    ("P014", "USB Type-C Cable", "Electronics", 299.00),
    ("P015", "Himalaya Shampoo", "Beauty", 179.00),
    ("P016", "Colgate Paste", "Grocery", 98.00),
    ("P017", "Kellogg’s Cornflakes", "Grocery", 299.00),
    ("P018", "Dabur Honey", "Grocery", 199.00),
    ("P019", "Mi Power Bank", "Electronics", 999.00),
    ("P020", "Blue Heaven Kajal", "Beauty", 99.00),
] * 8  # ➜ This makes 160 rows


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ 150 Records — raw_customers

# COMMAND ----------

raw_customers = [
    ("C001", "Akash", "Delhi"),
    ("C002", "Priya", "Chennai"),
    ("C003", "Ravi", "Mumbai"),
    ("C004", "Suresh", "Hyderabad"),
    ("C005", "Meena", "Bangalore"),
    ("C006", "Karthik", "Pune"),
    ("C007", "Aisha", "Kolkata"),
    ("C008", "Rahul", "Jaipur"),
    ("C009", "Sneha", "Nagpur"),
    ("C010", "Vikram", "Indore"),
    ("C011", "Harini", "Coimbatore"),
    ("C012", "Nitin", "Ahmedabad"),
    ("C013", "Tejas", "Surat"),
    ("C014", "Deepika", "Patna"),
    ("C015", "Gopal", "Lucknow"),
] * 10  # ➜ 150 customers


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ 150+ Records — raw_sales
# MAGIC - ⚡Generated with random-like pattern
# MAGIC - ⚡ Order dates: Jan 2024 – Mar 2024
# MAGIC - ⚡ Customers rotate from C001–C015
# MAGIC - ⚡ Products rotate P001–P020
# MAGIC - ⚡ Quantities vary 1–5
# MAGIC - ⚡ Prices are picked from product price list

# COMMAND ----------


raw_sales = []

import random
from datetime import datetime, timedelta

product_ids = ["P00" + str(i) if i < 10 else "P0" + str(i) for i in range(1, 21)]
customer_ids = ["C00" + str(i) if i < 10 else "C0" + str(i) for i in range(1, 16)]
base_date = datetime(2024, 1, 1)

prices = {
    "P001":199.50, "P002":399.00, "P003":149.00, "P004":249.00, "P005":1299.00,
    "P006":89.00,  "P007":35.00,  "P008":55.00,  "P009":25.00,  "P010":45.00,
    "P011":1599.00,"P012":199.00, "P013":45.00,  "P014":299.00, "P015":179.00,
    "P016":98.00,  "P017":299.00, "P018":199.00, "P019":999.00, "P020":99.00
}

order_id_counter = 1001

for i in range(160):  # ➜ 160 sales records
    cust = random.choice(customer_ids)
    prod = random.choice(product_ids)
    qty = random.randint(1, 5)
    price = prices.get(prod, 100.00)
    order_date = base_date + timedelta(days=random.randint(0, 75))

    raw_sales.append((
        str(order_id_counter),
        cust,
        prod,
        order_date.strftime("%Y-%m-%d"),
        qty,
        price
    ))

    order_id_counter += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✔️ Create DataFrames

# COMMAND ----------

sales_df = spark.createDataFrame(
    raw_sales,
    ["order_id","customer_id","product_id","order_date","quantity","price"]
)

products_df = spark.createDataFrame(
    raw_products,
    ["product_id","product_name","category","unit_price"]
)

customers_df = spark.createDataFrame(
    raw_customers,
    ["customer_id","customer_name","city"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Save to Bronze (Delta Lake)

# COMMAND ----------

sales_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/bronze/sales")
products_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/bronze/products")
customers_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/bronze/customers")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🥈 SILVER LAYER (Cleaning + Joining)
# MAGIC 🎯 Goal
# MAGIC Convert raw data → trusted data.
# MAGIC
# MAGIC ✔ What We Will Do
# MAGIC - Remove nulls
# MAGIC - Validate data types
# MAGIC - Remove duplicates
# MAGIC - Join sales with products
# MAGIC - Join sales with customers
# MAGIC - Enrich with product category & customer city

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Read Bronze

# COMMAND ----------


bronze_sales = spark.read.format("delta").load("/Volumes/workspace/default/bronze/sales")
bronze_products = spark.read.format("delta").load("/Volumes/workspace/default/bronze/products")
bronze_customers = spark.read.format("delta").load("/Volumes/workspace/default/bronze/customers")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Clean + Join

# COMMAND ----------

from pyspark.sql.functions import *

silver_sales = bronze_sales \
    .dropDuplicates(["order_id"]) \
    .filter("quantity > 0") \
    .filter("price > 0") \
    .join(bronze_products, "product_id", "left") \
    .join(bronze_customers, "customer_id", "left") \
    .withColumn("total_amount", col("quantity") * col("unit_price"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save Silver Layer as Delta

# COMMAND ----------

silver_sales.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/silver/sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Output
# MAGIC You now have a clean, validated, and enriched sales table with:
# MAGIC - customer name
# MAGIC - city
# MAGIC - product name
# MAGIC - category
# MAGIC - total amount

# COMMAND ----------

# MAGIC %md
# MAGIC - 🥇 GOLD LAYER (Business Aggregations)
# MAGIC
# MAGIC Goal:
# MAGIC - Create business-ready KPIs for dashboards.
# MAGIC ## Step 6: Load Silver Table

# COMMAND ----------

sales_silver = spark.read.format("delta").load("/Volumes/workspace/default/silver/sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⭐ Business KPIs
# MAGIC ### 1. Daily Revenue

# COMMAND ----------

daily_revenue = sales_silver.groupBy("order_date").agg(sum("total_amount").alias("revenue"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Revenue by Category

# COMMAND ----------

rev_category = sales_silver.groupBy("category").agg(sum("total_amount").alias("category_revenue"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Top Cities by Sales

# COMMAND ----------

rev_city = sales_silver.groupBy("city").agg(sum("total_amount").alias("city_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Step 7: Save Gold Layer

# COMMAND ----------

daily_revenue.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/gold/daily_revenue")
rev_category.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/gold/rev_category")
rev_city.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/gold/rev_city")

# COMMAND ----------

