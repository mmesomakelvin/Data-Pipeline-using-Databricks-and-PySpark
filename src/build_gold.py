from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "online_retail_silver.csv"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

WEEKLY_REVENUE_FILE = GOLD_DIR / "weekly_revenue.csv"
TOP_PRODUCTS_FILE = GOLD_DIR / "top_5_products_by_revenue.csv"
TOP_CUSTOMERS_FILE = GOLD_DIR / "top_5_customers_by_revenue.csv"
REVENUE_BY_COUNTRY_FILE = GOLD_DIR / "revenue_by_country.csv"

NON_PRODUCT_STOCK_CODES = {
    "AMAZONFEE",
    "BANK CHARGES",
    "C2",
    "CRUK",
    "D",
    "DCGS0003",
    "DCGS0004",
    "DCGS0055",
    "DCGS0057",
    "DCGS0069",
    "DCGS0070",
    "DCGS0076",
    "DCGSSBOY",
    "DCGSSGIRL",
    "DOT",
    "M",
    "POST",
    "S",
}


def configure_pyspark_python() -> None:
    python_command = sys.executable if " " not in sys.executable else "python"
    os.environ["PYSPARK_PYTHON"] = python_command
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_command


def create_spark_session():
    configure_pyspark_python()

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("build-gold")
        .master("local[*]")
        .getOrCreate()
    )


def save_csv(df, path: Path) -> int:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    row_count = df.count()
    df.toPandas().to_csv(path, index=False)
    return row_count


def build_gold_tables(dry_run: bool) -> None:
    from pyspark.sql import functions as F

    if not SILVER_FILE.exists():
        raise FileNotFoundError(f"Missing Silver file: {SILVER_FILE}")

    spark = create_spark_session()

    try:
        silver_df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(SILVER_FILE))
            .withColumn("InvoiceTimestamp", F.to_timestamp("InvoiceTimestamp"))
            .withColumn("Revenue", F.col("Revenue").cast("double"))
        )

        silver_rows = silver_df.count()

        weekly_revenue_df = (
            silver_df.withColumn("WeekStartDate", F.to_date(F.date_trunc("week", "InvoiceTimestamp")))
            .groupBy("WeekStartDate")
            .agg(F.round(F.sum("Revenue"), 2).alias("WeeklyRevenue"))
            .orderBy("WeekStartDate")
        )

        product_sales_df = silver_df.filter(~F.col("StockCode").isin(NON_PRODUCT_STOCK_CODES))

        top_products_df = (
            product_sales_df.groupBy("StockCode", "Description")
            .agg(
                F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
                F.sum("Quantity").alias("TotalQuantitySold"),
            )
            .orderBy(F.desc("TotalRevenue"))
            .limit(5)
        )

        top_customers_df = (
            silver_df.filter(F.col("CustomerID").isNotNull())
            .groupBy("CustomerID")
            .agg(F.round(F.sum("Revenue"), 2).alias("TotalRevenue"))
            .orderBy(F.desc("TotalRevenue"))
            .limit(5)
        )

        revenue_by_country_df = (
            silver_df.groupBy("Country")
            .agg(F.round(F.sum("Revenue"), 2).alias("TotalRevenue"))
            .orderBy(F.desc("TotalRevenue"))
        )

        if dry_run:
            print(f"Silver rows available for Gold: {silver_rows:,}")
            print(f"Weekly revenue rows: {weekly_revenue_df.count():,}")
            print(f"Top products rows: {top_products_df.count():,}")
            print(f"Top customers rows: {top_customers_df.count():,}")
            print(f"Revenue by country rows: {revenue_by_country_df.count():,}")
            return

        weekly_rows = save_csv(weekly_revenue_df, WEEKLY_REVENUE_FILE)
        product_rows = save_csv(top_products_df, TOP_PRODUCTS_FILE)
        customer_rows = save_csv(top_customers_df, TOP_CUSTOMERS_FILE)
        country_rows = save_csv(revenue_by_country_df, REVENUE_BY_COUNTRY_FILE)

        print(f"Gold input Silver rows: {silver_rows:,}")
        print(f"Created {WEEKLY_REVENUE_FILE.relative_to(PROJECT_ROOT)}: {weekly_rows:,} rows")
        print(f"Created {TOP_PRODUCTS_FILE.relative_to(PROJECT_ROOT)}: {product_rows:,} rows")
        print(f"Created {TOP_CUSTOMERS_FILE.relative_to(PROJECT_ROOT)}: {customer_rows:,} rows")
        print(f"Created {REVENUE_BY_COUNTRY_FILE.relative_to(PROJECT_ROOT)}: {country_rows:,} rows")
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build local Gold aggregation CSV files from the Silver output."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview Gold table row counts without writing output files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_gold_tables(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
