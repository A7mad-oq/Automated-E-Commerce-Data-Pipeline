"""
airflow_dags/main_dag.py
────────────────────────
Production Airflow DAG for the E-Commerce Data Engineering Pipeline.

Improvements over the original:
  • Retries and exponential back-off on every task
  • SLA alert callback (log warning — hook up email/Slack in production)
  • Email-on-failure / on-retry flags
  • Explicit task-level timeouts
  • Health-check task before ETL
  • Spark analysis task included in the pipeline
  • DAG documented with description and tags
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from etl_process import run_etl

logger = logging.getLogger(__name__)

# ─── SLA miss callback ────────────────────────────────────────────────────────
def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.warning(
        "SLA missed for DAG '%s'. Tasks: %s",
        dag.dag_id,
        [str(t) for t in task_list],
    )
    # TODO: integrate with PagerDuty / Slack / email here


# ─── Default task arguments ───────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@example.com"],      # Replace with real address
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# ─── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_pipeline",
    description=(
        "Ingests Brazilian E-Commerce data from Kaggle, runs ETL via pandas, "
        "and generates a daily revenue report with PySpark."
    ),
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",      # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    default_args=default_args,
    sla_miss_callback=_sla_miss_callback,
    tags=["ecommerce", "etl", "spark"],
    doc_md="""
## E-Commerce ETL Pipeline

### Flow
```
check_data_files >> run_etl >> run_spark_analysis >> verify_report
```

### Tasks
| Task | Description |
|------|-------------|
| `check_data_files` | Verifies the data directory is accessible |
| `run_etl` | Downloads, cleans, and loads data to PostgreSQL |
| `run_spark_analysis` | Joins orders + items; writes daily revenue CSV |
| `verify_report` | Confirms the report CSV was produced |
""",
) as dag:

    # ── Task 1: Verify data directory is mounted ──────────────────────────────
    check_data_files = BashOperator(
        task_id="check_data_files",
        bash_command=(
            "ls /opt/airflow/data/ && "
            "echo 'Data directory is accessible.'"
        ),
        execution_timeout=timedelta(minutes=2),
        doc_md="Verify the data volume is mounted before starting ETL.",
    )

    # ── Task 2: ETL (extract, transform, load) ────────────────────────────────
    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
        execution_timeout=timedelta(hours=1, minutes=30),
        doc_md=(
            "Downloads Kaggle data if absent, cleans it, and loads "
            "orders + order_items into PostgreSQL."
        ),
    )

    # ── Task 3: PySpark analysis ──────────────────────────────────────────────
    run_spark_analysis = BashOperator(
        task_id="run_spark_analysis",
        bash_command="python /opt/airflow/spark_jobs/analysis.py",
        execution_timeout=timedelta(hours=1),
        doc_md="Runs the PySpark daily revenue aggregation job.",
    )

    # ── Task 4: Sanity-check the output report ────────────────────────────────
    verify_report = BashOperator(
        task_id="verify_report",
        bash_command=(
            "ls /opt/airflow/data/daily_summary_report/ | grep -q '.csv' && "
            "echo 'Report verified.' || "
            "(echo 'ERROR: report CSV not found!' && exit 1)"
        ),
        execution_timeout=timedelta(minutes=5),
        doc_md="Fails the DAG if no report CSV was produced by Spark.",
    )

    # ── Pipeline ──────────────────────────────────────────────────────────────
    check_data_files >> run_etl_task >> run_spark_analysis >> verify_report
