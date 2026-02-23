"""DAG 21: Docker Hello World.

The simplest possible DockerOperator DAG. Runs a single command
inside an Alpine Linux container. Demonstrates the minimal
configuration needed to execute tasks in Docker containers.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="021_docker_hello",
    default_args=DEFAULT_ARGS,
    description="Minimal DockerOperator: run a command in a container",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    hello = DockerOperator(
        task_id="hello",
        image="alpine:3.20",
        command='echo "Hello from inside a Docker container!"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    uname = DockerOperator(
        task_id="uname",
        image="alpine:3.20",
        command="uname -a",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    hello >> uname
