"""
scripts/exceptions.py
─────────────────────
Shared exception hierarchy for the ETL pipeline.
Imported by extract.py, transform.py, and load.py to avoid
circular dependencies.
"""


class ETLError(Exception):
    """Base class for all ETL pipeline errors."""


class DataDownloadError(ETLError):
    """Raised when the Kaggle dataset cannot be downloaded."""


class DataQualityError(ETLError):
    """Raised when a data-quality check fails during transformation."""


class DatabaseLoadError(ETLError):
    """Raised when writing to PostgreSQL fails."""
