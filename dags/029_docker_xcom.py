"""DAG 29: Docker XCom Communication.

Demonstrates passing data between Docker containers via XCom.
DockerOperator can push its stdout to XCom, and downstream tasks
can pull that data. Shows the ``xcom_all`` parameter and how to
structure container output for inter-task communication.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def show_docker_output(**context: object) -> None:
    """Pull and display XCom values pushed by Docker tasks."""
    ti = context["ti"]  # type: ignore[index]
    generate_output = ti.xcom_pull(task_ids="generate_data")
    compute_output = ti.xcom_pull(task_ids="compute_stats")

    print(f"[{timestamp()}] Data from generate_data container:")
    print(f"  {generate_output}")
    print(f"[{timestamp()}] Data from compute_stats container:")
    print(f"  {compute_output}")


with DAG(
    dag_id="029_docker_xcom",
    default_args=DEFAULT_ARGS,
    description="Passing data between Docker containers via XCom",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    # Generate data and push to XCom via stdout
    # When retrieve_output=True, the last line of stdout is pushed to XCom
    generate_data = DockerOperator(
        task_id="generate_data",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import json; "
            "data = {'values': [10, 20, 30, 40, 50], 'source': 'docker'}; "
            "print(json.dumps(data)); "
            "\""
        ),
        retrieve_output=True,
        retrieve_output_path="/tmp/xcom_output",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Compute stats and push result to XCom
    compute_stats = DockerOperator(
        task_id="compute_stats",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import json; "
            "values = [10, 20, 30, 40, 50]; "
            "stats = {'mean': sum(values)/len(values), 'min': min(values), 'max': max(values)}; "
            "print(json.dumps(stats)); "
            "\""
        ),
        retrieve_output=True,
        retrieve_output_path="/tmp/xcom_output",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Python task reads XCom values from both Docker tasks
    show_results = PythonOperator(
        task_id="show_results",
        python_callable=show_docker_output,
    )

    [generate_data, compute_stats] >> show_results
