"""
config/settings.py
──────────────────
Centralised, environment-driven configuration for the
E-Commerce Data Engineering Pipeline.

All secrets are read from environment variables (set via .env
or Docker secrets).  No credentials are ever hard-coded here.
"""

import os
from dataclasses import dataclass, field
from functools import lru_cache


@dataclass(frozen=True)
class DatabaseConfig:
    host: str = field(default_factory=lambda: os.environ["POSTGRES_HOST"])
    port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    user: str = field(default_factory=lambda: os.environ["POSTGRES_USER"])
    password: str = field(default_factory=lambda: os.environ["POSTGRES_PASSWORD"])
    db: str = field(default_factory=lambda: os.environ["POSTGRES_DB"])

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"


@dataclass(frozen=True)
class PathConfig:
    data_dir: str = "/opt/airflow/data"
    raw_orders: str = field(init=False)
    raw_items: str = field(init=False)
    clean_orders_parquet: str = field(init=False)
    daily_report_csv: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "raw_orders",
                           os.path.join(self.data_dir, "olist_orders_dataset.csv"))
        object.__setattr__(self, "raw_items",
                           os.path.join(self.data_dir, "olist_order_items_dataset.csv"))
        object.__setattr__(self, "clean_orders_parquet",
                           os.path.join(self.data_dir, "orders_clean.parquet"))
        object.__setattr__(self, "daily_report_csv",
                           os.path.join(self.data_dir, "daily_summary_report"))


@dataclass(frozen=True)
class KaggleConfig:
    username: str = field(default_factory=lambda: os.environ["KAGGLE_USERNAME"])
    key: str = field(default_factory=lambda: os.environ["KAGGLE_KEY"])
    dataset_slug: str = "olistbr/brazilian-ecommerce"


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = "EcommerceAnalysis"
    master: str = "local[*]"
    jdbc_driver_path: str = "/opt/spark/jars/postgresql-42.6.0.jar"
    executor_memory: str = "2g"
    driver_memory: str = "1g"


@dataclass(frozen=True)
class AppConfig:
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    kaggle: KaggleConfig = field(default_factory=KaggleConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """Return a singleton AppConfig, built once per process."""
    return AppConfig()
