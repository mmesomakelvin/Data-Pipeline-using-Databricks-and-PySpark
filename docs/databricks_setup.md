# Databricks Setup Guide

This guide explains how to run the incremental Online Retail pipeline in Databricks after the local scripts and notebook sources have been prepared.

## 1. Import Notebook Source Files

Import these files from the GitHub repository into your Databricks workspace:

```text
notebooks/01_move_next_file.py
notebooks/02_bronze_ingestion.py
notebooks/03_silver_transformation.py
notebooks/04_gold_aggregations.py
notebooks/05_run_workflow.py
```

Recommended workspace folder:

```text
/Workspace/Users/<your-email>/online-retail-pipeline
```

The final Databricks Workflow should use notebooks `01` through `04` as separate tasks. Notebook `05` is optional and is only for manually running the sequence from one notebook.

## 2. Upload Monthly Archive Files

The local Stage 2 script creates monthly files in:

```text
archives/
```

Upload those files to this DBFS path:

```text
dbfs:/FileStore/online_retail_pipeline/archives/
```

Create the landing folder:

```text
dbfs:/FileStore/online_retail_pipeline/landing/
```

After upload, the archive folder should contain files like:

```text
sales_2010_12.csv
sales_2011_01.csv
sales_2011_02.csv
...
sales_2011_12.csv
```

## 3. Shared Parameters

Use these values in the Databricks Workflow task parameters:

```text
base_path = dbfs:/FileStore/online_retail_pipeline
database = online_retail_pipeline
```

`base_path` controls where the archive and landing files live.

`database` controls where Delta tables are created.

## 4. Create Databricks Workflow

Create a Databricks Workflow with four tasks in this order:

| Task key | Notebook | Depends on | Parameters |
| --- | --- | --- | --- |
| `move_next_file` | `01_move_next_file` | None | `base_path` |
| `bronze_ingestion` | `02_bronze_ingestion` | `move_next_file` | `base_path`, `database` |
| `silver_transformation` | `03_silver_transformation` | `bronze_ingestion` | `database` |
| `gold_aggregations` | `04_gold_aggregations` | `silver_transformation` | `database` |

Set the schedule to hourly if the project requires automated arrival simulation.

## 5. Delta Tables Created

The notebooks create these Delta tables:

```text
online_retail_pipeline.bronze_online_retail
online_retail_pipeline.bronze_processed_files
online_retail_pipeline.silver_online_retail
online_retail_pipeline.silver_processed_files
online_retail_pipeline.gold_weekly_revenue
online_retail_pipeline.gold_top_5_products_by_revenue
online_retail_pipeline.gold_top_5_customers_by_revenue
online_retail_pipeline.gold_revenue_by_country
```

Bronze and Silver use processed-file tables to avoid processing the same source file twice.

Gold tables are rebuilt from the latest Silver table after every successful workflow run.

## 6. Validate Tables

Run these SQL checks in Databricks SQL or a notebook:

```sql
SELECT COUNT(*) FROM online_retail_pipeline.bronze_online_retail;
SELECT COUNT(*) FROM online_retail_pipeline.silver_online_retail;
SELECT * FROM online_retail_pipeline.gold_weekly_revenue ORDER BY WeekStartDate;
SELECT * FROM online_retail_pipeline.gold_top_5_products_by_revenue;
SELECT * FROM online_retail_pipeline.gold_top_5_customers_by_revenue;
SELECT * FROM online_retail_pipeline.gold_revenue_by_country ORDER BY TotalRevenue DESC;
```

After the first successful run, only `sales_2010_12.csv` should be processed.

After the final successful run, all monthly files should be processed and `archives/` should be empty.

## 7. Dashboard Requirements

Create a Databricks SQL dashboard with four visualizations:

1. Weekly revenue from `gold_weekly_revenue`
2. Top five products by revenue from `gold_top_5_products_by_revenue`
3. Top five customers by revenue from `gold_top_5_customers_by_revenue`
4. Revenue by country from `gold_revenue_by_country`

Good chart choices:

- line chart for weekly revenue
- bar chart for top products
- bar chart for top customers
- bar chart or map for revenue by country

## 8. Screenshots To Capture

Save screenshots in:

```text
docs/screenshots/
```

Required evidence:

1. Databricks Workflow showing all four tasks
2. First successful workflow run
3. Final successful workflow run after all archive files are processed
4. Dashboard after the first run
5. Dashboard after the final run

Do not commit private credentials, Databricks tokens, or workspace secrets.

## 9. Common Issues

If `01_move_next_file` says no archive files are left, check that files exist in:

```text
dbfs:/FileStore/online_retail_pipeline/archives/
```

If Bronze or Silver says there are no new files, check the processed-file tables:

```sql
SELECT * FROM online_retail_pipeline.bronze_processed_files;
SELECT * FROM online_retail_pipeline.silver_processed_files;
```

If you need to restart testing from scratch, use a fresh database name such as:

```text
online_retail_pipeline_test
```

That avoids deleting useful evidence from earlier successful runs.
