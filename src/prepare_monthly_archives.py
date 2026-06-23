from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_EXCEL = PROJECT_ROOT / "data" / "raw" / "Online Retail.xlsx"
INTERIM_DIR = PROJECT_ROOT / "data" / "interim"
INTERIM_CSV = INTERIM_DIR / "online_retail.csv"
ARCHIVES_DIR = PROJECT_ROOT / "archives"


def convert_excel_to_csv() -> None:
    if not RAW_EXCEL.exists():
        raise FileNotFoundError(f"Missing source file: {RAW_EXCEL}")

    INTERIM_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Reading Excel file: {RAW_EXCEL}")
    pdf = pd.read_excel(RAW_EXCEL)
    print(f"Rows read from Excel: {len(pdf):,}")

    pdf.to_csv(INTERIM_CSV, index=False)
    print(f"Created Spark-readable CSV: {INTERIM_CSV}")


def create_spark_session() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    return (
        SparkSession.builder.appName("prepare-monthly-archives")
        .master("local[*]")
        .getOrCreate()
    )


def split_csv_into_monthly_archives() -> None:
    ARCHIVES_DIR.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    try:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(INTERIM_CSV))
        )

        df = df.withColumn(
            "InvoiceTimestamp",
            F.coalesce(
                F.to_timestamp("InvoiceDate"),
                F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"),
            ),
        )

        bad_dates = df.filter(F.col("InvoiceTimestamp").isNull()).count()
        if bad_dates:
            raise ValueError(f"Found {bad_dates:,} rows with invalid InvoiceDate values")

        df = df.withColumn("year", F.year("InvoiceTimestamp")).withColumn(
            "month", F.month("InvoiceTimestamp")
        )

        months = [
            (row["year"], row["month"])
            for row in df.select("year", "month").distinct().orderBy("year", "month").collect()
        ]

        original_columns = [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ]

        for year, month in months:
            monthly_df = df.filter((F.col("year") == year) & (F.col("month") == month)).select(
                *original_columns
            )
            final_file = ARCHIVES_DIR / f"sales_{year}_{month:02d}.csv"

            if final_file.exists():
                final_file.unlink()

            # On local Windows, Spark's native CSV writer requires Hadoop winutils.exe.
            # We still use Spark for the split logic, then write each small monthly file with pandas.
            monthly_df.toPandas().to_csv(final_file, index=False)
            print(f"Created {final_file.relative_to(PROJECT_ROOT)}")

        print(f"Monthly archive files created: {len(months)}")
    finally:
        spark.stop()


def main() -> None:
    convert_excel_to_csv()
    split_csv_into_monthly_archives()


if __name__ == "__main__":
    main()
