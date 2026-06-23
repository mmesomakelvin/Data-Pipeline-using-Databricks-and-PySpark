from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LANDING_DIR = PROJECT_ROOT / "landing"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
BRONZE_FILE = BRONZE_DIR / "online_retail_bronze.csv"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
PROCESSED_FILES_LOG = CHECKPOINT_DIR / "bronze_processed_files.txt"

SOURCE_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


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


def record_processed_file(file_name: str) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    with PROCESSED_FILES_LOG.open("a", encoding="utf-8") as file:
        file.write(f"{file_name}\n")


def get_new_landing_files() -> list[Path]:
    if not LANDING_DIR.exists():
        raise FileNotFoundError(f"Missing landing folder: {LANDING_DIR}")

    processed_files = read_processed_files()
    landing_files = sorted(LANDING_DIR.glob("sales_*.csv"))

    return [path for path in landing_files if path.name not in processed_files]


def ingest_file_to_bronze(path: Path) -> int:
    configure_pyspark_python()

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(column, StringType(), True) for column in SOURCE_COLUMNS])

    spark = (
        SparkSession.builder.appName("ingest-bronze")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        ingestion_time = datetime.now(timezone.utc).isoformat()

        bronze_df = (
            spark.read.option("header", True)
            .schema(schema)
            .csv(str(path))
            .withColumn("_source_file", F.lit(path.name))
            .withColumn("_ingested_at_utc", F.lit(ingestion_time))
        )

        row_count = bronze_df.count()
        pdf = bronze_df.toPandas()

        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        pdf.to_csv(BRONZE_FILE, mode="a", header=not BRONZE_FILE.exists(), index=False)

        return row_count
    finally:
        spark.stop()


def run_bronze_ingestion(dry_run: bool) -> None:
    new_files = get_new_landing_files()

    if not new_files:
        print("No new landing files to ingest.")
        return

    if dry_run:
        print("Dry run: new files ready for Bronze ingestion:")
        for path in new_files:
            print(f"- {path.relative_to(PROJECT_ROOT)}")
        return

    total_rows = 0
    for path in new_files:
        row_count = ingest_file_to_bronze(path)
        record_processed_file(path.name)
        total_rows += row_count
        print(f"Ingested {path.relative_to(PROJECT_ROOT)}: {row_count:,} rows")

    print(f"Bronze rows added: {total_rows:,}")
    print(f"Bronze output: {BRONZE_FILE.relative_to(PROJECT_ROOT)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append new landing CSV files into the local Bronze output."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview new landing files without writing Bronze output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_bronze_ingestion(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
