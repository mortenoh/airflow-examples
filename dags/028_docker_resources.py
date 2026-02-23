"""DAG 28: Docker Resource Constraints.

Demonstrates setting CPU and memory limits on Docker containers.
Shows how to control resource allocation to prevent runaway tasks
from consuming all host resources.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="028_docker_resources",
    default_args=DEFAULT_ARGS,
    description="CPU/memory limits and resource constraints for Docker tasks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    # Task with memory limit
    memory_limited = DockerOperator(
        task_id="memory_limited",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import resource; "
            "print('Memory-limited container:'); "
            "data = list(range(100000)); "
            "print(f'  Allocated list with {len(data)} items'); "
            "usage = resource.getrusage(resource.RUSAGE_SELF); "
            "print(f'  Max RSS: {usage.ru_maxrss} KB'); "
            "\""
        ),
        mem_limit="128m",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Task with CPU limit
    cpu_limited = DockerOperator(
        task_id="cpu_limited",
        image="python:3.12-slim",
        command=(
            "python -c \""
            "import time, os; "
            "print('CPU-limited container:'); "
            "print(f'  CPU count visible: {os.cpu_count()}'); "
            "start = time.monotonic(); "
            "total = sum(i**2 for i in range(500000)); "
            "elapsed = time.monotonic() - start; "
            "print(f'  Computation took {elapsed:.3f}s'); "
            "print(f'  Result: {total}'); "
            "\""
        ),
        cpus=0.5,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Task with both CPU and memory limits
    fully_constrained = DockerOperator(
        task_id="fully_constrained",
        image="alpine:3.20",
        command=(
            'sh -c "'
            "echo 'Fully constrained container:' && "
            "echo '  Memory limit: 64MB' && "
            "echo '  CPU limit: 0.25 cores' && "
            "echo '  Available memory:' && free -m 2>/dev/null || "
            "echo '  (free not available, using cat /proc/meminfo)' && "
            "head -3 /proc/meminfo 2>/dev/null || echo '  (meminfo not available)'"
            '"'
        ),
        mem_limit="64m",
        cpus=0.25,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    memory_limited >> cpu_limited >> fully_constrained
