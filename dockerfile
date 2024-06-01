FROM apache/airflow:2.3.0

ENV AIRFLOW_HOME=/opt/airflow

# Install additional packages
RUN pip install --no-cache-dir pyspark==3.4.0 pandas

# Set the entrypoint
ENTRYPOINT ["/entrypoint"]

# Switch to the user airflow
USER airflow

# Set the working directory
WORKDIR /opt/airflow
