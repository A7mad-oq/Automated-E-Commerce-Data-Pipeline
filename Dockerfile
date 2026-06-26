# ─── Stage 1: dependency builder ──────────────────────────────────────────────
# Pin the base image to a specific digest in production for reproducibility.
FROM apache/airflow:2.7.1-python3.10 AS builder

USER root

# Install Java (required by PySpark) and clean up in a single layer
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download the PostgreSQL JDBC driver into a well-known location
RUN mkdir -p /opt/spark/jars \
 && curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar" \
    -o /opt/spark/jars/postgresql-42.6.0.jar

# ─── Stage 2: runtime image ───────────────────────────────────────────────────
FROM apache/airflow:2.7.1-python3.10

USER root

# Copy Java runtime and JDBC jar from builder
COPY --from=builder /usr/lib/jvm/java-17-openjdk-amd64 \
                    /usr/lib/jvm/java-17-openjdk-amd64
COPY --from=builder /opt/spark/jars /opt/spark/jars

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Drop back to the airflow user — never run as root in production
USER airflow

WORKDIR /opt/airflow

# Copy pinned requirements and install
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt

# Copy application source
COPY airflow_dags/  /opt/airflow/dags/
COPY scripts/       /opt/airflow/scripts/
COPY spark_jobs/    /opt/airflow/spark_jobs/
COPY config/        /opt/airflow/config/
COPY sql/           /opt/airflow/sql/

# Tell Python where to find the project packages
ENV PYTHONPATH="/opt/airflow/scripts:/opt/airflow"

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
