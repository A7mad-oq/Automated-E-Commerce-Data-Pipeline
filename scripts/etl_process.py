"""
scripts/etl_process.py
──────────────────────
Pipeline orchestrator for the E-Commerce ETL.

This file is intentionally thin — it delegates every step to the
dedicated modules in the same package:

    extract.py   →  download from Kaggle + read CSVs
    transform.py →  clean, coerce types, validate
    load.py      →  write Parquet + UPSERT to PostgreSQL

The Airflow DAG (and any CLI caller) imports run_etl() from here.
"""

from __future__ import annotations

import logging

from extract import download_kaggle_data, extract
from transform import transform
from load import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

_DIVIDER = "═" * 60


def run_etl() -> None:
    """
    Execute the full ETL pipeline end-to-end:

      1. Extract  — download from Kaggle (if needed) and read CSVs.
      2. Transform — clean, coerce types, apply business rules, validate.
      3. Load      — write Parquet checkpoint and UPSERT into PostgreSQL.
    """
    logger.info(_DIVIDER)
    logger.info("ETL pipeline started.")
    logger.info(_DIVIDER)

    # ── 1. Extract ────────────────────────────────────────────────────────────
    logger.info("[1/3] EXTRACT")
    download_kaggle_data()
    orders_raw, items_raw = extract()

    # ── 2. Transform ──────────────────────────────────────────────────────────
    logger.info("[2/3] TRANSFORM")
    orders_clean, items_clean = transform(orders_raw, items_raw)

    # ── 3. Load ───────────────────────────────────────────────────────────────
    logger.info("[3/3] LOAD")
    load(orders_clean, items_clean)

    logger.info(_DIVIDER)
    logger.info("ETL pipeline finished successfully.")
    logger.info("  orders      : %d rows", len(orders_clean))
    logger.info("  order_items : %d rows", len(items_clean))
    logger.info(_DIVIDER)


if __name__ == "__main__":
    run_etl()
