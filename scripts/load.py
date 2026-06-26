"""
scripts/load.py
───────────────
Loading layer for the E-Commerce ETL pipeline.

Responsibilities:
  1. Write the cleaned orders DataFrame to a Parquet file
     (used as input by the downstream PySpark job).
  2. UPSERT orders into PostgreSQL via a staging table
     (idempotent — safe to re-run without duplicating data).
  3. Reload order_items in chunks
     (avoids OOM on large datasets).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import get_config
from exceptions import DatabaseLoadError

logger = logging.getLogger(__name__)

# ─── Engine factory ───────────────────────────────────────────────────────────

def _build_engine() -> Engine:
    """Create a SQLAlchemy engine from the shared config."""
    cfg = get_config()
    return create_engine(
        cfg.db.sqlalchemy_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,             # discard stale connections
        connect_args={"connect_timeout": 10},
    )


@contextmanager
def _engine_ctx() -> Generator[Engine, None, None]:
    """Context manager that disposes the engine on exit."""
    engine = _build_engine()
    try:
        yield engine
    finally:
        engine.dispose()


# ─── Parquet checkpoint ───────────────────────────────────────────────────────

def save_parquet(orders: pd.DataFrame) -> None:
    """
    Persist the clean orders DataFrame as a Parquet file.

    This file is the input for the downstream PySpark analysis job,
    so it is always written before the PostgreSQL load.

    Args:
        orders: Cleaned orders DataFrame from transform().
    """
    path = get_config().paths.clean_orders_parquet
    orders.to_parquet(path, index=False)
    logger.info("Parquet checkpoint written → %s", path)


# ─── Database loaders ─────────────────────────────────────────────────────────

def _upsert_orders(df: pd.DataFrame, engine: Engine, chunk_size: int = 10_000) -> None:
    """
    Load orders into PostgreSQL via a temporary staging table.

    Pattern:
      1. Create a TEMP TABLE with the same structure as `orders`.
      2. Bulk-insert all rows into staging (in chunks).
      3. UPSERT from staging → orders using ON CONFLICT (order_id).

    This is fully idempotent: re-running does not create duplicates.

    Args:
        df:         Cleaned orders DataFrame.
        engine:     Active SQLAlchemy engine.
        chunk_size: Rows per INSERT batch (tunes memory vs. round-trips).
    """
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TEMP TABLE orders_staging
                (LIKE ecommerce.orders INCLUDING ALL)
            ON COMMIT DROP;
        """))

        for start in range(0, len(df), chunk_size):
            df.iloc[start: start + chunk_size].to_sql(
                "orders_staging",
                conn,
                if_exists="append",
                index=False,
                method="multi",
            )

        conn.execute(text("""
            INSERT INTO ecommerce.orders
                (order_id, customer_id, order_status,
                 order_purchase_timestamp, order_delivered_customer_date)
            SELECT order_id, customer_id, order_status,
                   order_purchase_timestamp, order_delivered_customer_date
            FROM orders_staging
            ON CONFLICT (order_id) DO UPDATE
              SET customer_id                   = EXCLUDED.customer_id,
                  order_status                  = EXCLUDED.order_status,
                  order_purchase_timestamp      = EXCLUDED.order_purchase_timestamp,
                  order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
                  updated_at                    = NOW();
        """))

    logger.info("orders: UPSERT complete (%d rows).", len(df))


def _load_items(df: pd.DataFrame, engine: Engine, chunk_size: int = 10_000) -> None:
    """
    Load order_items into PostgreSQL in chunks.

    order_items has no natural primary key for conflict detection, so the
    table is replaced on the first chunk then appended for subsequent ones.

    Args:
        df:         Cleaned order_items DataFrame.
        engine:     Active SQLAlchemy engine.
        chunk_size: Rows per INSERT batch.
    """
    for i, start in enumerate(range(0, len(df), chunk_size)):
        chunk = df.iloc[start: start + chunk_size]
        chunk.to_sql(
            "order_items",
            engine,
            schema="ecommerce",
            if_exists="replace" if i == 0 else "append",
            index=False,
            method="multi",
        )
        logger.debug("order_items: chunk %d written (%d rows).", i + 1, len(chunk))

    logger.info("order_items: load complete (%d rows).", len(df))


# ─── Public entry point ───────────────────────────────────────────────────────

def load(orders: pd.DataFrame, items: pd.DataFrame) -> None:
    """
    Persist both cleaned DataFrames.

    Order of operations:
      1. Write Parquet checkpoint (Spark input).
      2. UPSERT orders into PostgreSQL.
      3. Load order_items into PostgreSQL.

    Args:
        orders: Cleaned orders DataFrame from transform().
        items:  Cleaned order_items DataFrame from transform().

    Raises:
        DatabaseLoadError: Wraps any SQLAlchemy / DB error.
    """
    save_parquet(orders)

    with _engine_ctx() as engine:
        try:
            _upsert_orders(orders, engine)
            _load_items(items, engine)
        except Exception as exc:
            raise DatabaseLoadError(
                "Failed to load data into PostgreSQL."
            ) from exc
