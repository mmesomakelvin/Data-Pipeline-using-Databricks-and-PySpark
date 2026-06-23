# Databricks notebook source
# Optional Databricks runner notebook. In production, prefer Databricks Workflows
# with separate tasks for notebooks 01 through 04.

# COMMAND ----------

dbutils.widgets.text("base_path", "dbfs:/FileStore/online_retail_pipeline")
dbutils.widgets.text("database", "online_retail_pipeline")

base_path = dbutils.widgets.get("base_path")
database = dbutils.widgets.get("database")

timeout_seconds = 0

dbutils.notebook.run("01_move_next_file", timeout_seconds, {"base_path": base_path})
dbutils.notebook.run(
    "02_bronze_ingestion",
    timeout_seconds,
    {"base_path": base_path, "database": database},
)
dbutils.notebook.run("03_silver_transformation", timeout_seconds, {"database": database})
dbutils.notebook.run("04_gold_aggregations", timeout_seconds, {"database": database})

dbutils.notebook.exit("Workflow run completed.")
