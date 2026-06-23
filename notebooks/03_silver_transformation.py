# Databricks notebook source
# Stage 5 for Databricks: clean new Bronze rows into the Silver Delta table.

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("database", "online_retail_pipeline")

database = dbutils.widgets.get("database")
bronze_table = f"{database}.bronze_online_retail"
silver_table = f"{database}.silver_online_retail"
processed_table = f"{database}.silver_processed_files"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {processed_table} (
        source_file STRING,
        processed_at TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------

processed_files = {
    row.source_file for row in spark.table(processed_table).select("source_file").collect()
}

bronze_df = spark.table(bronze_table)

new_source_files = [
    row._source_file
    for row in bronze_df.select("_source_file").distinct().orderBy("_source_file").collect()
    if row._source_file not in processed_files
]

if not new_source_files:
    dbutils.notebook.exit("No new Bronze files to transform.")

# COMMAND ----------

silver_df = (
    bronze_df.filter(F.col("_source_file").isin(new_source_files))
    .withColumn("InvoiceNo", F.trim(F.col("InvoiceNo")))
    .withColumn("StockCode", F.trim(F.col("StockCode")))
    .withColumn("Description", F.trim(F.col("Description")))
    .withColumn("Country", F.trim(F.col("Country")))
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
    .withColumn("CustomerID", F.col("CustomerID").cast("double").cast("long").cast("string"))
    .withColumn(
        "InvoiceTimestamp",
        F.coalesce(
            F.to_timestamp("InvoiceDate"),
            F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"),
        ),
    )
    .filter(F.col("InvoiceNo").isNotNull())
    .filter(~F.col("InvoiceNo").startswith("C"))
    .filter(F.col("StockCode").isNotNull())
    .filter(F.col("Quantity") > 0)
    .filter(F.col("UnitPrice") > 0)
    .filter(F.col("InvoiceTimestamp").isNotNull())
    .withColumn("Revenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    .withColumn("_silver_processed_at_utc", F.current_timestamp())
    .select(
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceTimestamp",
        "UnitPrice",
        "CustomerID",
        "Country",
        "Revenue",
        "_source_file",
        "_ingested_at_utc",
        "_silver_processed_at_utc",
    )
)

silver_df.write.mode("append").format("delta").saveAsTable(silver_table)

processed_df = spark.createDataFrame([(file_name,) for file_name in new_source_files], ["source_file"])
processed_df.withColumn("processed_at", F.current_timestamp()).write.mode("append").format(
    "delta"
).saveAsTable(processed_table)

dbutils.notebook.exit(f"Transformed {len(new_source_files)} file(s) into {silver_table}")
