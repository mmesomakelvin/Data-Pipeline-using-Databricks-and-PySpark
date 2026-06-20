# End-to-End Incremental Data Pipeline using Databricks and PySpark

This capstone project builds an automated incremental data pipeline for the UCI Online Retail dataset. It uses Databricks, PySpark, Delta Lake, and the medallion architecture to process newly arrived sales data without reprocessing historical files.

## Business Requirements

The final Gold layer and dashboard will answer four questions:

1. How much revenue is generated weekly?
2. What are the top five selling products by revenue?
3. Who are the top five customers by spending?
4. What is the total revenue per country?

## Pipeline Architecture

```text
UCI Online Retail dataset
          |
          v
Monthly CSV files in archives/
          |
          v
Hourly file movement to landing/
          |
          v
Bronze Delta table (raw incremental ingestion)
          |
          v
Silver Delta table (cleaned and validated sales)
          |
          v
Gold Delta tables (business aggregations)
          |
          v
Databricks SQL dashboard
```

## Technology Stack

- Python 3.14
- PySpark 4.1.2
- Java 21 LTS
- Databricks
- Apache Spark
- Delta Lake
- Databricks Workflows
- Spark SQL
- Git and GitHub

## Planned Workflow

1. Download the Online Retail dataset from the UCI Machine Learning Repository.
2. Split the source dataset into monthly CSV files using PySpark.
3. Store the files in `archives/` using names such as `sales_2010_12.csv`.
4. Schedule hourly movement of one archived file into `landing/`.
5. Incrementally append new data to the Bronze layer.
6. Clean and transform only newly ingested data in the Silver layer.
7. Update four Gold aggregation tables.
8. Refresh the Databricks dashboard after each pipeline run.

## Local Setup

Create and activate a virtual environment in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install PySpark:

```powershell
python -m pip install pyspark==4.1.2
```

Confirm the installed version:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

## Deliverables

- Databricks notebook source files
- Data workflow screenshot
- Screenshots of the first and final successful pipeline runs
- Screenshots of the dashboard after the first and final runs
- GitHub repository containing the complete project

## Project Status

Local environment setup is complete. A Spark 4.1.2 session has been tested successfully with Python 3.14 and Java 21. Pipeline implementation will be added incrementally as each stage is developed and tested.
