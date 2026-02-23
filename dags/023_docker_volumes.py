"""DAG 23: Docker Volumes.

Demonstrates bind mounts for sharing data between Docker tasks.
Each container step reads from and writes to a shared volume,
enabling multi-step pipelines where output from one container
becomes input for the next.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

from airflow_examples.config import DEFAULT_ARGS

SHARED_MOUNT = Mount(source="airflow-docker-demo", target="/data", type="volume")

with DAG(
    dag_id="023_docker_volumes",
    default_args=DEFAULT_ARGS,
    description="Bind mounts for sharing data between Docker tasks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    # Step 1: Create the shared directory and write initial data
    create_data = DockerOperator(
        task_id="create_data",
        image="alpine:3.20",
        command=(
            'sh -c "'
            "mkdir -p /data && "
            "echo 'Alice,85' > /data/scores.csv && "
            "echo 'Bob,92' >> /data/scores.csv && "
            "echo 'Charlie,78' >> /data/scores.csv && "
            "echo 'Created scores.csv:' && cat /data/scores.csv"
            '"'
        ),
        mounts=[SHARED_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Step 2: Process data in a Python container (reads from same mount)
    process_data = DockerOperator(
        task_id="process_data",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import csv; "
            "rows = list(csv.reader(open('/data/scores.csv'))); "
            "total = sum(int(r[1]) for r in rows); "
            "avg = total / len(rows); "
            "out = open('/data/summary.txt', 'w'); "
            "out.write(f'Total: {total}\\n'); "
            "out.write(f'Average: {avg:.1f}\\n'); "
            "out.write(f'Count: {len(rows)}\\n'); "
            "out.close(); "
            "print(f'Processed {len(rows)} records, avg={avg:.1f}'); "
            "\""
        ),
        mounts=[SHARED_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Step 3: Read the summary produced by the previous step
    read_summary = DockerOperator(
        task_id="read_summary",
        image="alpine:3.20",
        command='sh -c "echo \'Summary from previous step:\' && cat /data/summary.txt"',
        mounts=[SHARED_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Step 4: Cleanup
    cleanup = DockerOperator(
        task_id="cleanup",
        image="alpine:3.20",
        command='sh -c "rm -rf /data/* && echo \'Cleaned up shared volume\'"',
        mounts=[SHARED_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    create_data >> process_data >> read_summary >> cleanup
