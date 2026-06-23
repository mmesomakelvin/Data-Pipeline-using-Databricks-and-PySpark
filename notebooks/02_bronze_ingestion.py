# Databricks notebook source
# Stage 4 for Databricks: append new landing files into the Bronze Delta table.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


dbutils.widgets.text("base_path", "dbfs:/FileStore/online_retail_pipeline")
dbutils.widgets.text("database", "online_retail_pipeline")

base_path = dbutils.widgets.get("base_path").rstrip("/")
database = dbutils.widgets.get("database")
landing_path = f"{base_path}/landing"

bronze_table = f"{database}.bronze_online_retail"
processed_table = f"{database}.bronze_processed_files"

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

source_columns = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]

schema = StructType([StructField(column, StringType(), True) for column in source_columns])

processed_files = {
    row.source_file for row in spark.table(processed_table).select("source_file").collect()
}

landing_files = sorted(
    [file for file in dbutils.fs.ls(landing_path) if file.name.startswith("sales_")],
    key=lambda file: file.name,
)

new_files = [file for file in landing_files if file.name not in processed_files]

if not new_files:
    dbutils.notebook.exit("No new landing files to ingest.")

# COMMAND ----------

new_file_paths = [file.path for file in new_files]

bronze_df = (
    spark.read.option("header", True)
    .schema(schema)
    .csv(new_file_paths)
    .withColumn("_source_path", F.input_file_name())
    .withColumn("_source_file", F.regexp_extract("_source_path", r"([^/]+)$", 1))
    .withColumn("_ingested_at_utc", F.current_timestamp())
    .drop("_source_path")
)

bronze_df.write.mode("append").format("delta").saveAsTable(bronze_table)

processed_df = spark.createDataFrame([(file.name,) for file in new_files], ["source_file"])
processed_df.withColumn("processed_at", F.current_timestamp()).write.mode("append").format(
    "delta"
).saveAsTable(processed_table)

dbutils.notebook.exit(f"Ingested {len(new_files)} file(s) into {bronze_table}")
