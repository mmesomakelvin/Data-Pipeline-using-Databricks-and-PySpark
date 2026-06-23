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

## Project Structure

```text
.
|-- src/                 # Local PySpark scripts
|-- notebooks/           # Databricks notebook source files
|-- docs/
|   `-- screenshots/     # Workflow, pipeline run, and dashboard evidence
|-- requirements.txt     # Local Python dependencies
|-- .gitignore           # Files excluded from version control
`-- README.md            # Project documentation
```

Local datasets, landing files, checkpoints, and Spark-generated files are excluded from Git.

## Stage 2: Prepare Monthly Archives

The raw UCI file is downloaded as an Excel workbook. Locally, the project first converts that workbook into a CSV file and then uses PySpark to split the dataset into monthly archive files.

Run the Stage 2 script from the project root:

```powershell
python .\src\prepare_monthly_archives.py
```

Expected output files:

```text
archives/sales_2010_12.csv
archives/sales_2011_01.csv
...
archives/sales_2011_12.csv
```

Stage 2 has been run locally and produced 13 monthly archive files with 541,909 total rows. The generated `archives/` files are intentionally ignored by Git because they are reproducible data outputs.

On Windows, Spark may print `winutils.exe` warnings during local runs. For this project step, those warnings are acceptable as long as the script finishes and creates the monthly CSV files.

## Stage 3: Simulate File Arrival

The pipeline is incremental, so it should not process every archive file at once. Stage 3 simulates an hourly scheduled job by moving the next monthly CSV file from `archives/` into `landing/`.

Preview the next file movement:

```powershell
python .\src\move_next_archive_to_landing.py --dry-run
```

Move the next file:

```powershell
python .\src\move_next_archive_to_landing.py
```

After the first run, `landing/` should contain:

```text
landing/sales_2010_12.csv
```

Stage 3 has been tested locally. The first simulated arrival moved `sales_2010_12.csv` into `landing/`, leaving 12 monthly files in `archives/`.

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

Install project dependencies:

```powershell
python -m pip install -r requirements.txt
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

Local environment setup is complete. A Spark 4.1.2 session has been tested successfully with Python 3.14 and Java 21. Stage 2 prepared the reproducible monthly archive files from the source dataset. Stage 3 simulated the first file arrival by moving `sales_2010_12.csv` into `landing/`. Pipeline implementation will be added incrementally as each stage is developed and tested.
