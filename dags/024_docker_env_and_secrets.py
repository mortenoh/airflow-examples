"""DAG 24: Docker Environment Variables and Secrets.

Demonstrates passing environment variables to Docker containers.
Shows static env vars, Airflow template-rendered vars, and
how to structure configuration for containerized tasks.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="024_docker_env_and_secrets",
    default_args=DEFAULT_ARGS,
    description="Environment variables and configuration in Docker tasks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    # Static environment variables
    static_env = DockerOperator(
        task_id="static_env",
        image="alpine:3.20",
        command=(
            'sh -c "'
            "echo APP_NAME=$APP_NAME && "
            "echo APP_ENV=$APP_ENV && "
            "echo LOG_LEVEL=$LOG_LEVEL && "
            "echo BATCH_SIZE=$BATCH_SIZE"
            '"'
        ),
        environment={
            "APP_NAME": "airflow-demo",
            "APP_ENV": "staging",
            "LOG_LEVEL": "INFO",
            "BATCH_SIZE": "500",
        },
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Template-rendered environment variables (Airflow injects values at runtime)
    templated_env = DockerOperator(
        task_id="templated_env",
        image="alpine:3.20",
        command=(
            'sh -c "'
            "echo EXECUTION_DATE=$EXECUTION_DATE && "
            "echo DAG_ID=$DAG_ID && "
            "echo RUN_ID=$RUN_ID"
            '"'
        ),
        environment={
            "EXECUTION_DATE": "{{ ds }}",
            "DAG_ID": "{{ dag.dag_id }}",
            "RUN_ID": "{{ run_id }}",
        },
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Simulated config-driven task
    config_driven = DockerOperator(
        task_id="config_driven",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import os; "
            "config = {k: v for k, v in os.environ.items() if k.startswith('APP_')}; "
            "print('Application config:'); "
            "[print(f'  {k} = {v}') for k, v in sorted(config.items())]; "
            "\""
        ),
        environment={
            "APP_DATABASE_HOST": "postgres",
            "APP_DATABASE_PORT": "5432",
            "APP_CACHE_TTL": "3600",
            "APP_WORKERS": "4",
            "APP_DEBUG": "false",
        },
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    static_env >> templated_env >> config_driven
