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
|-- Dashboard/            # Exported Databricks dashboard visual screenshots
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

## Stage 4: Bronze Ingestion

Bronze is the raw ingestion layer. It stores the landed sales rows with audit columns so we know which file each row came from and when it was loaded.

Preview new landing files before ingestion:

```powershell
python .\src\ingest_bronze.py --dry-run
```

Ingest new landing files into the local Bronze output:

```powershell
python .\src\ingest_bronze.py
```

Local Bronze output:

```text
data/bronze/online_retail_bronze.csv
```

The script uses `checkpoints/bronze_processed_files.txt` to avoid ingesting the same landing file twice.

Stage 4 has been tested locally. The first Bronze ingestion loaded `sales_2010_12.csv` with 42,481 rows, wrote audit columns `_source_file` and `_ingested_at_utc`, and confirmed that rerunning the script does not reprocess the same file.

## Stage 5: Silver Transformation

Silver is the cleaned and typed layer. It converts raw Bronze strings into useful data types, removes invalid sales rows, and calculates revenue.

Preview new Bronze files before Silver transformation:

```powershell
python .\src\transform_silver.py --dry-run
```

Transform new Bronze rows into the local Silver output:

```powershell
python .\src\transform_silver.py
```

Local Silver output:

```text
data/silver/online_retail_silver.csv
```

The Silver script:

- converts `Quantity` to integer
- converts `UnitPrice` to decimal
- converts `InvoiceDate` to `InvoiceTimestamp`
- removes cancellation invoices and non-positive sales rows
- creates `Revenue = Quantity * UnitPrice`
- uses `checkpoints/silver_processed_files.txt` to avoid transforming the same source file twice

Stage 5 has been tested locally. The first Silver transformation processed `sales_2010_12.csv`, read 42,481 Bronze rows, and wrote 41,480 cleaned Silver rows. Rerunning the script confirms that the same source file is not transformed twice.

## Stage 6: Gold Aggregations

Gold is the business-ready reporting layer. It aggregates the clean Silver data into the outputs needed for dashboards and business questions.

Preview Gold table row counts:

```powershell
python .\src\build_gold.py --dry-run
```

Build the local Gold outputs:

```powershell
python .\src\build_gold.py
```

Local Gold outputs:

```text
data/gold/weekly_revenue.csv
data/gold/top_5_products_by_revenue.csv
data/gold/top_5_customers_by_revenue.csv
data/gold/revenue_by_country.csv
```

The Gold outputs answer:

- weekly revenue
- top five products by revenue
- top five customers by revenue
- total revenue by country

For the top-products output, known service and fee stock codes such as `DOT`, `POST`, and `AMAZONFEE` are excluded so the result focuses on actual products.

Stage 6 has been tested locally using the first Silver batch. It read 41,480 Silver rows and created four Gold outputs: 4 weekly revenue rows, 5 top-product rows, 5 top-customer rows, and 23 country revenue rows.

## Stage 7: Local Pipeline Orchestration

Stage 7 runs one full incremental cycle with a single command. It moves the next archive file into `landing/`, ingests new landing files into Bronze, transforms new Bronze rows into Silver, and rebuilds the Gold aggregation outputs.

Run one local incremental cycle:

```powershell
python .\src\run_incremental_pipeline.py
```

The runner executes these scripts in order:

1. `src/move_next_archive_to_landing.py`
2. `src/ingest_bronze.py`
3. `src/transform_silver.py`
4. `src/build_gold.py`

Stage 7 has been tested locally with the second monthly file, `sales_2011_01.csv`. After the run, Bronze contained 77,628 rows, Silver contained 75,786 rows, weekly Gold contained 9 rows, and country Gold contained 28 rows.

## Stage 8: Databricks Notebook Sources

The local scripts now have Databricks notebook source equivalents in `notebooks/`.

```text
notebooks/01_move_next_file.py
notebooks/02_bronze_ingestion.py
notebooks/03_silver_transformation.py
notebooks/04_gold_aggregations.py
notebooks/05_run_workflow.py
```

These notebooks use Databricks volume paths, Delta tables, and `dbutils`, which are available inside Databricks. Local scripts in `src/` still use local project folders and are separate from the Databricks notebook paths. The recommended Databricks Workflow task order is:

1. `01_move_next_file`
2. `02_bronze_ingestion`
3. `03_silver_transformation`
4. `04_gold_aggregations`

The optional `05_run_workflow` notebook can run the same sequence manually, but the final project should show a Databricks Workflow screenshot.

## Stage 9: Databricks Setup Guide

The Databricks deployment guide is available at:

```text
docs/databricks_setup.md
```

It covers notebook import, Unity Catalog volume upload paths, Workflow task order, table validation SQL, dashboard requirements, and the screenshots needed for final submission.

## Stage 10: Databricks SQL Dashboard Queries

Dashboard SQL queries are available in:

```text
docs/sql/
```

These queries are designed for Databricks SQL and read from the Gold Delta tables. PostgreSQL is not required.

## Stage 11: Submission Checklist

The final evidence tracker is available at:

```text
docs/submission_checklist.md
```

It lists completed GitHub work, remaining Databricks tasks, required screenshots, final validation queries, and final submission checks.

## Stage 12: First Databricks Dashboard Evidence

The first Databricks run has been completed with one monthly file, `sales_2010_12.csv`, processed through Bronze, Silver, and Gold.

Validation results from Databricks SQL:

```text
Bronze rows: 42,481
Silver rows: 41,480
Weekly revenue rows: 4
Revenue by country rows: 23
Top products rows: 5
Top customers rows: 5
```

Four dashboard visual exports are saved in the `Dashboard/` folder:

```text
Dashboard/Country by Total Revenue.png
Dashboard/Customer ID by Total Revenue.png
Dashboard/Product Description by Total Revenue.png
Dashboard/Weekly start date by weekly revenue.png
```

These images show the first-run Gold reporting outputs for revenue by country, top customers, top products, and weekly revenue. The next Databricks step is to run additional incremental cycles so the row counts and dashboard update as more monthly files move from `archives/` into `landing/`.

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

Local environment setup is complete. A Spark 4.1.2 session has been tested successfully with Python 3.14 and Java 21. Stage 2 prepared the reproducible monthly archive files from the source dataset. Stage 3 simulated file arrival by moving monthly files into `landing/`. Stage 4 loaded landed files into the local Bronze output with duplicate protection. Stage 5 cleaned and typed Bronze data into the local Silver output. Stage 6 created the local Gold business aggregations for dashboard reporting. Stage 7 added and tested a single-command local pipeline runner. Stage 8 added Databricks notebook source files for the workflow implementation. Stage 9 added the Databricks setup guide for deployment, validation, and final evidence collection. Stage 10 added Databricks SQL dashboard queries. Stage 11 added the final submission checklist. Stage 12 captured the first Databricks SQL dashboard evidence after one successful incremental run.
