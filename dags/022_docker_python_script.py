"""DAG 22: Docker Python Script.

Runs Python code inside a container using the official Python image.
Demonstrates running inline scripts, passing arguments, and
capturing output from containerized Python execution.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="022_docker_python_script",
    default_args=DEFAULT_ARGS,
    description="Run Python scripts inside Docker containers",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    # Run an inline Python script
    inline_script = DockerOperator(
        task_id="inline_script",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import sys, platform; "
            "print(f'Python {sys.version}'); "
            "print(f'Platform: {platform.platform()}'); "
            "print(f'Result: {sum(range(100))}'); "
            "\""
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Run a multi-line script using heredoc-style input
    data_processing = DockerOperator(
        task_id="data_processing",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; "
            "squared = [x**2 for x in data]; "
            "evens = [x for x in squared if x % 2 == 0]; "
            "print(f'Input: {data}'); "
            "print(f'Squared: {squared}'); "
            "print(f'Even squares: {evens}'); "
            "print(f'Sum of even squares: {sum(evens)}'); "
            "\""
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Install a package and use it (bash -c wraps the entire command)
    with_package = DockerOperator(
        task_id="with_package",
        image="python:3.12-slim",
        command=(
            "bash -c '"
            "pip install --quiet humanize && "
            'python -c "import humanize; '
            "print(humanize.naturalsize(1024**3)); "
            "print(humanize.intcomma(1000000)); "
            'print(humanize.naturaltime(3600))"'
            "'"
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    inline_script >> data_processing >> with_package
