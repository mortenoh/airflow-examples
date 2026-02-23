"""DAG 31: Docker Dynamic Image Mapping.

Demonstrates dynamic task mapping with DockerOperator, running the
same command across different container images. Shows how to
parallelize work across multiple runtime environments.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

IMAGES = [
    "python:3.12-slim",
    "python:3.11-slim",
    "node:22-alpine",
    "ruby:3.3-alpine",
]


@task
def list_images() -> list[str]:
    """Return a list of Docker images to test."""
    print(f"[{timestamp()}] Will run version checks on {len(IMAGES)} images")
    return IMAGES


with DAG(
    dag_id="031_docker_dynamic_images",
    default_args=DEFAULT_ARGS,
    description="Dynamic task mapping across different Docker images",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    start = BashOperator(task_id="start", bash_command='echo "Starting multi-image test"')

    images = list_images()

    # Dynamically create a Docker task for each image
    version_check = DockerOperator.partial(
        task_id="version_check",
        command='sh -c "echo Image: $(cat /etc/os-release 2>/dev/null | head -1 || echo unknown) && uname -a"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    ).expand(image=images)

    done = BashOperator(task_id="done", bash_command='echo "All images tested"')

    start >> images >> version_check >> done
