"""DAG 25: Docker ETL Pipeline.

A complete multi-step ETL pipeline where every stage runs in its
own Docker container. Demonstrates a realistic pattern: download
raw data, validate, transform, and generate a report -- all with
different container images suited to each task.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG
from docker.types import Mount

from airflow_examples.config import DEFAULT_ARGS

PIPELINE_MOUNT = Mount(source="airflow-etl-pipeline", target="/pipeline", type="volume")

with DAG(
    dag_id="025_docker_pipeline",
    default_args=DEFAULT_ARGS,
    description="Multi-step ETL pipeline with each stage in a Docker container",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker", "pipeline"],
) as dag:
    # Stage 1: Generate raw data (simulates download)
    extract = DockerOperator(
        task_id="extract",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import json; "
            "data = ["
            "{'id': i, 'temp_c': 15 + i * 0.5, 'humidity': 60 + i} "
            "for i in range(20)"
            "]; "
            "json.dump(data, open('/pipeline/raw.json', 'w'), indent=2); "
            "print(f'Extracted {len(data)} records to raw.json'); "
            "\""
        ),
        mounts=[PIPELINE_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Stage 2: Validate data
    validate = DockerOperator(
        task_id="validate",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import json; "
            "data = json.load(open('/pipeline/raw.json')); "
            "valid = [r for r in data if 0 <= r['temp_c'] <= 50 and 0 <= r['humidity'] <= 100]; "
            "invalid = len(data) - len(valid); "
            "json.dump(valid, open('/pipeline/validated.json', 'w'), indent=2); "
            "print(f'Validated: {len(valid)} ok, {invalid} rejected'); "
            "\""
        ),
        mounts=[PIPELINE_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Stage 3: Transform (convert Celsius to Fahrenheit, add derived fields)
    # Uses list comprehension with dict.update() for one-liner in python -c
    transform = DockerOperator(
        task_id="transform",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import json; "
            "data = json.load(open('/pipeline/validated.json')); "
            "[r.update({'temp_f': round(r['temp_c'] * 9/5 + 32, 1), "
            "'heat_index': round(r['temp_c'] + 0.05 * r['humidity'], 1)}) for r in data]; "
            "json.dump(data, open('/pipeline/transformed.json', 'w'), indent=2); "
            "print(f'Transformed {len(data)} records (added temp_f, heat_index)'); "
            "\""
        ),
        mounts=[PIPELINE_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Stage 4: Generate summary report
    report = DockerOperator(
        task_id="report",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import json; "
            "data = json.load(open('/pipeline/transformed.json')); "
            "temps = [r['temp_c'] for r in data]; "
            "report = f'Pipeline Report\\n' "
            "f'Records: {len(data)}\\n' "
            "f'Temp range: {min(temps):.1f} - {max(temps):.1f} C\\n' "
            "f'Avg temp: {sum(temps)/len(temps):.1f} C\\n'; "
            "open('/pipeline/report.txt', 'w').write(report); "
            "print(report); "
            "\""
        ),
        mounts=[PIPELINE_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Stage 5: Cleanup
    cleanup = DockerOperator(
        task_id="cleanup",
        image="alpine:3.20",
        command='sh -c "rm -rf /pipeline/* && echo \'Pipeline artifacts cleaned up\'"',
        mounts=[PIPELINE_MOUNT],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    extract >> validate >> transform >> report >> cleanup
