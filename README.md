# E-Commerce Data Engineering Pipeline

A production-ready data pipeline that ingests the **Brazilian E-Commerce (Olist)** dataset from Kaggle, cleans and loads it into PostgreSQL via Apache Airflow, and generates a daily revenue report with PySpark.

---

## Architecture

```
Kaggle API
    │
    ▼
[Airflow DAG]
    ├─ check_data_files    →  verify data volume is mounted
    ├─ run_etl             →  Extract → Transform → Load (pandas + PostgreSQL)
    ├─ run_spark_analysis  →  Distributed join & aggregation (PySpark)
    └─ verify_report       →  Sanity-check output CSV
                                       │
                              PostgreSQL (ecommerce schema)
                              ├─ orders
                              └─ order_items
                                       │
                              /data/daily_summary_report/*.csv
```

---

## Quick Start

### 1. Clone & configure

```bash
git clone <your-repo>
cd Data_Engineering_Project

# Create your local secrets file (never commit this)
cp .env.example .env
# Edit .env and fill in all CHANGE_ME values
```

### 2. Generate a Fernet key

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste output into AIRFLOW_FERNET_KEY in .env
```

### 3. Set your Kaggle credentials

Go to https://www.kaggle.com/settings → API → **Create New Token**.  
Set `KAGGLE_USERNAME` and `KAGGLE_KEY` in `.env`.

### 4. Start the stack

```bash
docker compose up -d --build
```

| Service | URL |
|---------|-----|
| Airflow Webserver | http://localhost:8080 |
| pgAdmin | http://localhost:5050 |
| PostgreSQL | localhost:5432 |

### 5. Trigger the DAG

In the Airflow UI, enable and trigger `ecommerce_pipeline`.  
Alternatively:

```bash
docker compose exec airflow-webserver airflow dags trigger ecommerce_pipeline
```

---

## Project Structure

```
.
├── airflow_dags/
│   └── main_dag.py          # Airflow DAG (4-task pipeline)
├── config/
│   └── settings.py          # Centralised, env-driven configuration
├── scripts/
│   └── etl_process.py       # ETL: download → clean → PostgreSQL
├── spark_jobs/
│   └── analysis.py          # PySpark daily revenue aggregation
├── sql/
│   └── schema.sql           # DB schema (auto-applied on first start)
├── .env.example             # Secret template — copy to .env
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
└── requirements.txt
```

---

## Key Production Enhancements

| Area | Original | Production |
|------|----------|------------|
| **Secrets** | Hard-coded credentials in source files | All secrets via `.env` / environment variables |
| **Error handling** | None | Custom exceptions + structured logging |
| **DB writes** | `if_exists='replace'` (destructive) | UPSERT via staging table (idempotent) |
| **Data quality** | None | Type checks, NULL checks, duplicate checks |
| **Retries** | None | 3 retries with exponential back-off (Airflow + download) |
| **Chunked I/O** | Full DataFrame to DB | 10 000-row chunks to avoid OOM |
| **Spark** | No config, no shutdown | Config-driven, broadcast hint, graceful stop |
| **SQL schema** | No indexes or constraints | PK, FK, CHECK, indexes, audit columns, view |
| **Docker** | Single-stage, root user | Multi-stage build, non-root `airflow` user |
| **Compose** | Plain env vars | `env_file`, resource limits, health checks |
| **Logging** | `print()` statements | `logging` module with timestamps and levels |

---

## Stopping the Stack

```bash
docker compose down          # keep volumes
docker compose down -v       # also delete all data volumes
```
