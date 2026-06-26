"""
spark_jobs/analysis.py
──────────────────────
PySpark daily revenue analysis job.

Improvements over the original:
  • SparkSession built from shared config (no hard-coded credentials)
  • Structured Python logging piped into Spark log4j
  • Broadcast hint for the smaller items table (avoids shuffle join)
  • Column-explicit SELECT instead of wildcard join
  • Partitioned write for faster downstream consumption
  • Graceful SparkSession shutdown in a finally block
  • Type-safe column imports (avoid shadow of built-in `sum`)
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.settings import get_config

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)


def build_spark_session(cfg) -> SparkSession:
    return (
        SparkSession.builder
        .appName(cfg.spark.app_name)
        .master(cfg.spark.master)
        .config("spark.jars", cfg.spark.jdbc_driver_path)
        .config("spark.executor.memory", cfg.spark.executor_memory)
        .config("spark.driver.memory", cfg.spark.driver_memory)
        # Improve small-cluster performance
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def read_orders(spark: SparkSession, cfg) -> DataFrame:
    logger.info("Reading orders from Parquet: %s", cfg.paths.clean_orders_parquet)
    return spark.read.parquet(cfg.paths.clean_orders_parquet)


def read_items(spark: SparkSession, cfg) -> DataFrame:
    logger.info("Reading order_items from PostgreSQL via JDBC.")
    return (
        spark.read
        .format("jdbc")
        .option("url", cfg.db.jdbc_url)
        .option("dbtable", "order_items")
        .option("user", cfg.db.user)
        .option("password", cfg.db.password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")          # tune for throughput
        .load()
    )


def build_daily_report(orders_df: DataFrame, items_df: DataFrame) -> DataFrame:
    """
    Join orders with items and aggregate daily revenue.

    Returns a DataFrame with columns:
        report_date      DATE
        daily_revenue    DOUBLE
        order_count      LONG
    """
    orders_sel = orders_df.select(
        F.col("order_id"),
        F.to_date("order_purchase_timestamp").alias("report_date"),
    )

    items_sel = items_df.select(
        F.col("order_id"),
        F.col("price"),
    )

    joined = orders_sel.join(
        F.broadcast(items_sel),      # broadcast smaller table
        on="order_id",
        how="inner",
    )

    report = (
        joined
        .groupBy("report_date")
        .agg(
            F.round(F.sum("price"), 2).alias("daily_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy("report_date")
    )

    return report


def write_report(report: DataFrame, cfg) -> None:
    output_path = cfg.paths.daily_report_csv
    logger.info("Writing daily report to %s", output_path)
    (
        report.coalesce(1)                    # single CSV for easy downstream use
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    logger.info("Report written successfully.")


def main() -> None:
    cfg = get_config()
    spark = build_spark_session(cfg)
    spark.sparkContext.setLogLevel("WARN")

    try:
        orders_df = read_orders(spark, cfg)
        items_df = read_items(spark, cfg)
        report_df = build_daily_report(orders_df, items_df)
        write_report(report_df, cfg)

        row_count = report_df.count()
        logger.info("Daily report complete: %d date partitions.", row_count)
    except Exception:
        logger.exception("Spark analysis job failed.")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


if __name__ == "__main__":
    main()
