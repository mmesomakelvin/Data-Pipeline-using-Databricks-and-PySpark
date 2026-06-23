# Databricks notebook source
# Stage 3 for Databricks: move one monthly archive file into landing.

# COMMAND ----------

dbutils.widgets.text("base_path", "dbfs:/FileStore/online_retail_pipeline")

base_path = dbutils.widgets.get("base_path").rstrip("/")
archives_path = f"{base_path}/archives"
landing_path = f"{base_path}/landing"

dbutils.fs.mkdirs(landing_path)

archive_files = sorted(
    [file for file in dbutils.fs.ls(archives_path) if file.name.startswith("sales_")],
    key=lambda file: file.name,
)

if not archive_files:
    dbutils.notebook.exit("No archive files left to move.")

next_file = archive_files[0]
destination_path = f"{landing_path}/{next_file.name}"

existing_landing_files = {file.name for file in dbutils.fs.ls(landing_path)}
if next_file.name in existing_landing_files:
    raise FileExistsError(f"Landing file already exists: {destination_path}")

dbutils.fs.mv(next_file.path, destination_path)
dbutils.notebook.exit(f"Moved {next_file.path} to {destination_path}")
