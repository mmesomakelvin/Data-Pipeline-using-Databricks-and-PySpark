# Submission Checklist

Use this checklist to track what is already complete and what remains before final submission.

## Completed In GitHub

- [x] Project README
- [x] Local Python virtual environment instructions
- [x] Local PySpark archive preparation script
- [x] Local file-arrival simulation script
- [x] Local Bronze ingestion script
- [x] Local Silver transformation script
- [x] Local Gold aggregation script
- [x] Local one-command incremental pipeline runner
- [x] Databricks notebook source files
- [x] Databricks setup guide
- [x] Databricks SQL dashboard queries

## Local Evidence Already Produced

- [x] Monthly archive files generated from the UCI Online Retail dataset
- [x] First file moved into `landing/`
- [x] Bronze ingestion tested
- [x] Silver transformation tested
- [x] Gold aggregation tested
- [x] Second monthly file processed through the local pipeline runner

Local runtime folders such as `data/`, `archives/`, `landing/`, and `checkpoints/` are intentionally ignored by Git because they are generated outputs.

## Remaining Databricks Work

- [ ] Import notebook source files from `notebooks/` into Databricks
- [ ] Upload all monthly archive CSV files to `dbfs:/FileStore/online_retail_pipeline/archives/`
- [ ] Create a Databricks Workflow with four tasks:
  - `01_move_next_file`
  - `02_bronze_ingestion`
  - `03_silver_transformation`
  - `04_gold_aggregations`
- [ ] Set workflow parameters:
  - `base_path = dbfs:/FileStore/online_retail_pipeline`
  - `database = online_retail_pipeline`
- [ ] Run the workflow once and confirm only the first monthly file is processed
- [ ] Run the workflow until all monthly files are processed
- [ ] Create Databricks SQL dashboard using queries from `docs/sql/`

## Screenshots To Save

Save screenshots in `docs/screenshots/`.

Recommended filenames:

```text
workflow_tasks.png
workflow_first_successful_run.png
workflow_final_successful_run.png
dashboard_first_run.png
dashboard_final_run.png
```

Required screenshot evidence:

- [ ] Databricks Workflow showing all four tasks
- [ ] First successful workflow run
- [ ] Final successful workflow run
- [ ] Dashboard after first run
- [ ] Dashboard after final run

## Final Validation Queries

Run these in Databricks SQL before submission:

```sql
SELECT COUNT(*) AS bronze_rows
FROM online_retail_pipeline.bronze_online_retail;

SELECT COUNT(*) AS silver_rows
FROM online_retail_pipeline.silver_online_retail;

SELECT COUNT(*) AS processed_bronze_files
FROM online_retail_pipeline.bronze_processed_files;

SELECT COUNT(*) AS processed_silver_files
FROM online_retail_pipeline.silver_processed_files;
```

Expected final file counts:

```text
processed_bronze_files = 13
processed_silver_files = 13
```

## Final Submission

- [ ] Confirm GitHub repository is public or accessible to the reviewer
- [ ] Confirm README explains the project clearly
- [ ] Confirm screenshots are committed or attached as required by the submission portal
- [ ] Submit the GitHub repository link
