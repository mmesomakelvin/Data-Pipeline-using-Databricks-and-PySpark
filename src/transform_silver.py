from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_FILE = PROJECT_ROOT / "data" / "bronze" / "online_retail_bronze.csv"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
SILVER_FILE = SILVER_DIR / "online_retail_silver.csv"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
PROCESSED_FILES_LOG = CHECKPOINT_DIR / "silver_processed_files.txt"


def configure_pyspark_python() -> None:
    python_command = sys.executable if " " not in sys.executable else "python"
    os.environ["PYSPARK_PYTHON"] = python_command
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_command


def read_processed_files() -> set[str]:
    if not PROCESSED_FILES_LOG.exists():
        return set()

    return {
        line.strip()
        for line in PROCESSED_FILES_LOG.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def record_processed_files(file_names: list[str]) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    with PROCESSED_FILES_LOG.open("a", encoding="utf-8") as file:
        for file_name in file_names:
            file.write(f"{file_name}\n")


def build_silver_dataframe():
    configure_pyspark_python()

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if not BRONZE_FILE.exists():
        raise FileNotFoundError(f"Missing Bronze file: {BRONZE_FILE}")

    spark = (
        SparkSession.builder.appName("transform-silver")
        .master("local[*]")
        .getOrCreate()
    )

    processed_files = read_processed_files()
    silver_processed_at = datetime.now(timezone.utc).isoformat()

    bronze_df = spark.read.option("header", True).csv(str(BRONZE_FILE))

    new_source_files = [
        row["_source_file"]
        for row in bronze_df.select("_source_file")
        .distinct()
        .orderBy("_source_file")
        .collect()
        if row["_source_file"] not in processed_files
    ]

    if not new_source_files:
        spark.stop()
        return None, [], 0, 0

    new_bronze_df = bronze_df.filter(F.col("_source_file").isin(new_source_files))
    input_rows = new_bronze_df.count()

    cleaned_df = (
        new_bronze_df.withColumn("InvoiceNo", F.trim(F.col("InvoiceNo")))
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
        .withColumn("_silver_processed_at_utc", F.lit(silver_processed_at))
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

    output_rows = cleaned_df.count()
    return spark, new_source_files, input_rows, output_rows, cleaned_df


def run_silver_transformation(dry_run: bool) -> None:
    result = build_silver_dataframe()

    if result[0] is None:
        print("No new Bronze files to transform into Silver.")
        return

    spark, new_source_files, input_rows, output_rows, silver_df = result

    try:
        if dry_run:
            print("Dry run: new Bronze files ready for Silver transformation:")
            for file_name in new_source_files:
                print(f"- {file_name}")
            print(f"Input Bronze rows: {input_rows:,}")
            print(f"Rows that would pass Silver validation: {output_rows:,}")
            return

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        silver_df.toPandas().to_csv(SILVER_FILE, mode="a", header=not SILVER_FILE.exists(), index=False)
        record_processed_files(new_source_files)

        print(f"Silver files processed: {len(new_source_files)}")
        print(f"Input Bronze rows: {input_rows:,}")
        print(f"Silver rows added: {output_rows:,}")
        print(f"Silver output: {SILVER_FILE.relative_to(PROJECT_ROOT)}")
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform new Bronze rows into the local Silver output."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview new Bronze files without writing Silver output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_silver_transformation(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
