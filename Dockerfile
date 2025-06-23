# Use the official Airflow image as base
FROM apache/airflow:2.10.2-python3.11

# Switch to root to install system packages
USER root

# Create shared directory with proper permissions
RUN mkdir -p /opt/airflow/shared && \
    chown -R airflow:root /opt/airflow/shared && \
    chmod -R 775 /opt/airflow/shared

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy your ETL code
COPY --chown=airflow:root ./etl /opt/airflow/etl
COPY --chown=airflow:root ./.env /opt/airflow/.env

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"