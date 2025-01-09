# Use official Airflow image as base
FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Switch to root user for installing packages
USER root

# Install additional system dependencies
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends \ 
        wget \
        vim \
        # Add any other apt packages you need
    && apt-get clean \ 
    && rm -rf /var/lib/apt/lists/* 

# Install additional Python packages individually
# Copy requirements.txt and install dependencies
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir pandas && pip install --no-cache-dir -r /requirements.txt

# Copy DAGs if you want to embed them in the image
COPY ./dags /opt/airflow/dags

# Optional: Copy custom scripts or configurations
COPY ./scripts /opt/airflow/scripts
COPY ./config /opt/airflow/config

WORKDIR $AIRFLOW_HOME

# Switch back to airflow user for security
USER airflow

# Set any additional environment variables if needed
ENV PYTHONPATH=/opt/airflow