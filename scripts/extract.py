"""
scripts/extract.py
──────────────────
Extraction layer for the E-Commerce ETL pipeline.

Responsibilities:
  1. Download the Brazilian E-Commerce dataset from Kaggle
     (idempotent — skips if files already exist, retries on failure).
  2. Read raw CSVs from disk and return them as DataFrames.
"""

from __future__ import annotations

import logging
import os
import time

import pandas as pd

from config.settings import get_config
from exceptions import DataDownloadError

logger = logging.getLogger(__name__)

# ─── Kaggle download ──────────────────────────────────────────────────────────

def download_kaggle_data(*, retries: int = 3, backoff: float = 5.0) -> None:
    """
    Download the Olist Brazilian E-Commerce dataset from Kaggle.

    Skips silently if the sentinel file already exists on disk.
    Retries up to `retries` times with exponential back-off on
    transient failures.

    Args:
        retries:  Maximum number of download attempts.
        backoff:  Base sleep seconds between retries (multiplied by attempt#).

    Raises:
        DataDownloadError: If all retry attempts are exhausted.
    """
    cfg = get_config()
    sentinel = cfg.paths.raw_orders

    if os.path.exists(sentinel):
        logger.info("Kaggle data already present at '%s' — skipping download.", sentinel)
        return

    # Inject credentials so the kaggle library picks them up
    os.environ.setdefault("KAGGLE_USERNAME", cfg.kaggle.username)
    os.environ.setdefault("KAGGLE_KEY", cfg.kaggle.key)

    from kaggle.api.kaggle_api_extended import KaggleApi  # lazy import

    api = KaggleApi()
    api.authenticate()

    for attempt in range(1, retries + 1):
        try:
            logger.info(
                "Downloading dataset '%s' (attempt %d / %d)…",
                cfg.kaggle.dataset_slug, attempt, retries,
            )
            api.dataset_download_files(
                cfg.kaggle.dataset_slug,
                path=cfg.paths.data_dir,
                unzip=True,
            )
            logger.info("Download complete → %s", cfg.paths.data_dir)
            return
        except Exception as exc:
            logger.warning("Attempt %d failed: %s", attempt, exc)
            if attempt < retries:
                sleep_for = backoff * attempt
                logger.info("Retrying in %.0f seconds…", sleep_for)
                time.sleep(sleep_for)
            else:
                raise DataDownloadError(
                    f"Could not download Kaggle dataset after {retries} attempts."
                ) from exc


# ─── CSV extraction ───────────────────────────────────────────────────────────

def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read the raw orders and order-items CSVs from disk.

    Returns:
        A (orders, items) tuple of raw DataFrames, unmodified.

    Raises:
        FileNotFoundError: If either CSV is missing (run download first).
    """
    cfg = get_config().paths

    logger.info("Reading orders CSV  → %s", cfg.raw_orders)
    orders = pd.read_csv(cfg.raw_orders, low_memory=False)
    logger.info("orders: %d rows, %d columns loaded.", *orders.shape)

    logger.info("Reading items CSV   → %s", cfg.raw_items)
    items = pd.read_csv(cfg.raw_items, low_memory=False)
    logger.info("order_items: %d rows, %d columns loaded.", *items.shape)

    return orders, items
