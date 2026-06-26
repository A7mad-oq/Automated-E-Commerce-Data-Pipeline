"""
scripts/transform.py
────────────────────
Transformation layer for the E-Commerce ETL pipeline.

Responsibilities:
  1. Parse and coerce data types (timestamps, numerics).
  2. Apply business rules (e.g. fill missing delivery dates).
  3. Select only the columns required by the database schema.
  4. Run data-quality checks and raise DataQualityError on violations.
"""

from __future__ import annotations

import logging

import pandas as pd

from exceptions import DataQualityError

logger = logging.getLogger(__name__)

# ─── Schema column definitions ────────────────────────────────────────────────

# Columns written to the ecommerce.orders table
_ORDERS_SCHEMA_COLS: list[str] = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
]

# All timestamp columns present in the raw orders CSV
_ORDERS_TIMESTAMP_COLS: list[str] = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

# Columns written to the ecommerce.order_items table
_ITEMS_SCHEMA_COLS: list[str] = [
    "order_id",
    "product_id",
    "price",
    "freight_value",
]

# ─── Quality checks ───────────────────────────────────────────────────────────

def _validate_orders(df: pd.DataFrame) -> None:
    """
    Assert critical quality invariants on the orders DataFrame.

    Raises:
        DataQualityError: On any violation.
    """
    if df["order_id"].isna().any():
        raise DataQualityError("orders.order_id contains NULL values.")

    if df["customer_id"].isna().any():
        raise DataQualityError("orders.customer_id contains NULL values.")

    if df["order_status"].isna().any():
        raise DataQualityError("orders.order_status contains NULL values.")

    dup_count = df["order_id"].duplicated().sum()
    if dup_count:
        raise DataQualityError(
            f"orders.order_id has {dup_count:,} duplicate value(s)."
        )

    logger.info("orders: all quality checks passed (%d rows).", len(df))


def _validate_items(df: pd.DataFrame) -> None:
    """
    Assert critical quality invariants on the order_items DataFrame.

    Raises:
        DataQualityError: On any hard violation.
    """
    null_prices = df["price"].isna().sum()
    if null_prices:
        # Soft warning — rows were already dropped in transform_items()
        logger.warning(
            "order_items: %d row(s) with unparseable price were dropped.", null_prices
        )

    negative_prices = (df["price"] < 0).sum()
    if negative_prices:
        raise DataQualityError(
            f"order_items.price has {negative_prices:,} negative value(s)."
        )

    logger.info("order_items: all quality checks passed (%d rows).", len(df))


# ─── Transformation functions ─────────────────────────────────────────────────

def transform_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the orders DataFrame.

    Steps:
      • Parse all timestamp columns to datetime.
      • Fill missing delivery dates with the purchase timestamp (business rule).
      • Select only the columns defined in the DB schema.
      • Run data-quality validation.

    Args:
        orders: Raw orders DataFrame from extract().

    Returns:
        Cleaned orders DataFrame ready for loading.
    """
    df = orders.copy()

    # Parse timestamps
    for col in _ORDERS_TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Business rule: use purchase timestamp as fallback for missing delivery date
    df["order_delivered_customer_date"] = df["order_delivered_customer_date"].fillna(
        df["order_purchase_timestamp"]
    )

    # Narrow to schema columns
    df = df[_ORDERS_SCHEMA_COLS].copy()

    _validate_orders(df)
    return df


def transform_items(items: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the order_items DataFrame.

    Steps:
      • Coerce price and freight_value to numeric (bad values → NaN).
      • Drop rows with unparseable prices.
      • Drop rows with negative prices.
      • Select only the columns defined in the DB schema.
      • Run data-quality validation.

    Args:
        items: Raw order_items DataFrame from extract().

    Returns:
        Cleaned order_items DataFrame ready for loading.
    """
    df = items.copy()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["price"])
    df = df[df["price"] >= 0]
    dropped = before - len(df)
    if dropped:
        logger.warning("order_items: dropped %d invalid row(s).", dropped)

    df = df[_ITEMS_SCHEMA_COLS].copy()

    _validate_items(df)
    return df


def transform(
    orders: pd.DataFrame,
    items: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Entry-point: transform both DataFrames in one call.

    Args:
        orders: Raw orders DataFrame.
        items:  Raw order_items DataFrame.

    Returns:
        (clean_orders, clean_items) tuple.
    """
    logger.info("Transforming orders…")
    clean_orders = transform_orders(orders)

    logger.info("Transforming order_items…")
    clean_items = transform_items(items)

    return clean_orders, clean_items
