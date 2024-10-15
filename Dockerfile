# Use the official Airflow image
FROM apache/airflow:2.9.1

# Set environment variables (these should match your docker-compose.yml file)
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
    AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow \
    AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0 \
    AIRFLOW__CORE__FERNET_KEY='' \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true \
    AIRFLOW__CORE__LOAD_EXAMPLES=true \
    AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.basic_auth

# Install any additional dependencies you might need
# For example, if you're using Google Cloud providers, you can install them like this:
# RUN pip install apache-airflow-providers-google
RUN pip install apache-airflow-providers-google
# Alternatively, use the _PIP_ADDITIONAL_REQUIREMENTS variable to install additional packages
# ENV _PIP_ADDITIONAL_REQUIREMENTS="apache-airflow-providers-google"
ENV _PIP_ADDITIONAL_REQUIREMENTS="apache-airflow-providers-google"
# Set the working directory
WORKDIR /opt/airflow

# Copy in the DAGs, plugins, and any custom scripts or configuration files
COPY ./dags /opt/airflow/dags
COPY ./plugins /opt/airflow/plugins
COPY ./config /opt/airflow/config
# Set the user to run Airflow processes
USER ${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-50000}

# Default entrypoint and command for running airflow commands
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]

# Expose necessary ports
EXPOSE 8080
