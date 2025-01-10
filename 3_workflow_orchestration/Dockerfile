# Use official Airflow image as base
FROM apache/airflow:2.10.4

ENV AIRFLOW_HOME=/opt/airflow

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Switch to root user for installing packages
USER root

# Install additional system dependencies
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends \ 
        wget \
        vim \
    && apt-get clean \ 
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/airflow/downloads \
    && chown -R airflow:root /opt/airflow/downloads \
    && chmod -R 775 /opt/airflow/downloads

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Install Python dependencies as airflow user
USER airflow
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and configs
COPY ./dags /opt/airflow/dags
COPY ./config /opt/airflow/config

WORKDIR $AIRFLOW_HOME