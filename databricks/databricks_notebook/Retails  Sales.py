# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer 

# COMMAND ----------

customer_df=spark.read.csv("/Volumes/workspace/default/retail_sales/retail_customers.csv",header=True,inferSchema=True)
orders_df=spark.read.csv("/Volumes/workspace/default/retail_sales/retail_orders.csv", header=True, inferSchema=True)
products_df=spark.read.csv("/Volumes/workspace/default/retail_sales/retail_products.csv", header=True, inferSchema=True)
order_items_df=spark.read.csv("/Volumes/workspace/default/retail_sales/retail_order_items.csv", header=True, inferSchema=True)

# COMMAND ----------

# store into delta table 
customer_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/retail_sales/customer_delta")
orders_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/retail_sales/orders_delta")
products_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/retail_sales/products_delta")
order_items_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/retail_sales/order_items_delta")

# COMMAND ----------

# now read the delta tables 
customer_bronze=spark.read.format("delta").load("/Volumes/workspace/default/retail_sales/customer_delta")
orders_bronze=spark.read.format("delta").load("/Volumes/workspace/default/retail_sales/orders_delta")
products_bronze=spark.read.format("delta").load("/Volumes/workspace/default/retail_sales/products_delta")
order_items_bronze=spark.read.format("delta").load("/Volumes/workspace/default/retail_sales/order_items_delta")
# display(customer_bronze)
# display(orders_bronze)
# display(products_bronze)
# display(order_items_bronze)

# COMMAND ----------

# count rows of each bronze table
print("customer_bronze count: ", customer_bronze.count())
print("orders_bronze count: ", orders_bronze.count())
print("products_bronze count: ", products_bronze.count())
print("order_items_bronze count: ", order_items_bronze.count())
# display(customer_bronze.count())
# display(orders_bronze.count())
# display(products_bronze.count())
# display(order_items_bronze.count())

display(orders_bronze)

# COMMAND ----------

from pyspark.sql.functions import trim, col, lower
def invalid_pk(df, pk):
    return df.filter(
        col(pk).isNull() |
        (trim(col(pk)) == "") |
        (lower(trim(col(pk))) == "null") |
        (lower(trim(col(pk))) == "none")
    )

# COMMAND ----------

# display(invalid_pk(customer_bronze, "customer_id"))
# display(invalid_pk(orders_bronze, "order_id"))
# display(invalid_pk(products_bronze, "product_id"))
# display(invalid_pk(order_items_bronze, "order_id"))

from pyspark.sql.functions import trim, col, lower

display(invalid_pk(orders_bronze, "customer_id"))


# COMMAND ----------

# Find negative quantities in order_items.
display(order_items_bronze.filter("quantity <0"))
# if order_items_bronze.filter("quantity <0"):
#     order_items_bronze.quantity =0
# else:
#     print("no negative quantities")

# display(order_items_bronze.filter("quantity <0"))

# COMMAND ----------

# Identify incorrect date formats in orders. 
from pyspark.sql.functions import try_to_date, col

incorrect_dates = orders_bronze.filter(
    try_to_date(col("order_date"), "yyyy-MM-dd").isNull()
)
display(incorrect_dates)
