"""DAG 30: Mixed Pipeline (Docker + Python + Bash).

Demonstrates combining DockerOperator with PythonOperator and
BashOperator in a single DAG. Real-world pipelines often mix
execution modes: some tasks need container isolation, others
run fine natively.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def validate_locally(**context: object) -> None:
    """Run validation in the Airflow process (no container needed)."""
    print(f"[{timestamp()}] Local Python validation:")
    print(f"  DAG: {context.get('dag_id')}")
    print(f"  Task: {context.get('task_id')}")
    print("  Validation passed (no container overhead needed)")


def aggregate_results(**context: object) -> None:
    """Aggregate results from upstream tasks."""
    print(f"[{timestamp()}] Aggregating results from mixed pipeline:")
    print("  Docker extraction: complete")
    print("  Local validation: complete")
    print("  Bash check: complete")
    print("  Docker transform: complete")
    print("  Pipeline finished successfully")


with DAG(
    dag_id="030_docker_mixed_pipeline",
    default_args=DEFAULT_ARGS,
    description="Mixed pipeline combining Docker, Python, and Bash operators",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker", "pipeline"],
) as dag:
    # Step 1: Docker -- extract data (needs specific dependencies)
    docker_extract = DockerOperator(
        task_id="docker_extract",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import json; "
            "data = [{'sensor': f's{i}', 'reading': i * 3.14} for i in range(5)]; "
            "print(f'Extracted {len(data)} sensor readings'); "
            "[print(f'  {d}') for d in data]; "
            "\""
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Step 2: Python -- validate locally (lightweight, no container needed)
    python_validate = PythonOperator(
        task_id="python_validate",
        python_callable=validate_locally,
    )

    # Step 3: Bash -- quick system check
    bash_check = BashOperator(
        task_id="bash_check",
        bash_command=(
            'echo "System check: $(uname -s) $(uname -m),'
            ' disk: $(df -h / | tail -1 | awk \'{print $4}\') free"'
        ),
    )

    # Step 4: Docker -- transform in isolated environment
    docker_transform = DockerOperator(
        task_id="docker_transform",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import math; "
            "readings = [i * 3.14 for i in range(5)]; "
            "normalized = [round(math.sin(r), 4) for r in readings]; "
            "print(f'Transformed {len(readings)} readings:'); "
            "[print(f'  {r:.2f} -> {n}') for r, n in zip(readings, normalized)]; "
            "\""
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Step 5: Python -- aggregate (runs natively)
    python_aggregate = PythonOperator(
        task_id="python_aggregate",
        python_callable=aggregate_results,
    )

    docker_extract >> python_validate >> bash_check >> docker_transform >> python_aggregate
