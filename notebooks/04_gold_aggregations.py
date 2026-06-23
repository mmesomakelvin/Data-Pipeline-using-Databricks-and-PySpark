# Databricks notebook source
# Stage 6 for Databricks: rebuild Gold Delta tables from the Silver table.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("database", "online_retail_pipeline")

database = dbutils.widgets.get("database")
silver_table = f"{database}.silver_online_retail"

weekly_revenue_table = f"{database}.gold_weekly_revenue"
top_products_table = f"{database}.gold_top_5_products_by_revenue"
top_customers_table = f"{database}.gold_top_5_customers_by_revenue"
revenue_by_country_table = f"{database}.gold_revenue_by_country"

non_product_stock_codes = [
    "AMAZONFEE",
    "BANK CHARGES",
    "C2",
    "CRUK",
    "D",
    "DCGS0003",
    "DCGS0004",
    "DCGS0055",
    "DCGS0057",
    "DCGS0069",
    "DCGS0070",
    "DCGS0076",
    "DCGSSBOY",
    "DCGSSGIRL",
    "DOT",
    "M",
    "POST",
    "S",
]

silver_df = spark.table(silver_table)

# COMMAND ----------

weekly_revenue_df = (
    silver_df.withColumn("WeekStartDate", F.to_date(F.date_trunc("week", "InvoiceTimestamp")))
    .groupBy("WeekStartDate")
    .agg(F.round(F.sum("Revenue"), 2).alias("WeeklyRevenue"))
    .orderBy("WeekStartDate")
)

product_sales_df = silver_df.filter(~F.col("StockCode").isin(non_product_stock_codes))

top_products_df = (
    product_sales_df.groupBy("StockCode", "Description")
    .agg(
        F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
        F.sum("Quantity").alias("TotalQuantitySold"),
    )
    .orderBy(F.desc("TotalRevenue"))
    .limit(5)
)

top_customers_df = (
    silver_df.filter(F.col("CustomerID").isNotNull())
    .groupBy("CustomerID")
    .agg(F.round(F.sum("Revenue"), 2).alias("TotalRevenue"))
    .orderBy(F.desc("TotalRevenue"))
    .limit(5)
)

revenue_by_country_df = (
    silver_df.groupBy("Country")
    .agg(F.round(F.sum("Revenue"), 2).alias("TotalRevenue"))
    .orderBy(F.desc("TotalRevenue"))
)

# COMMAND ----------

weekly_revenue_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(weekly_revenue_table)

top_products_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(top_products_table)

top_customers_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(top_customers_table)

revenue_by_country_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(revenue_by_country_table)

dbutils.notebook.exit("Gold tables rebuilt.")
