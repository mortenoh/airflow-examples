"""DAG 27: Docker Networking.

Demonstrates Docker container networking modes and inter-container
communication. Shows host mode, bridge mode, and how containers
can reach services on the host network.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="027_docker_network",
    default_args=DEFAULT_ARGS,
    description="Docker networking: bridge mode, host mode, DNS resolution",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    # Bridge mode (default): container gets its own network namespace
    bridge_mode = DockerOperator(
        task_id="bridge_mode",
        image="alpine:3.20",
        command=(
            'sh -c "'
            "echo 'Bridge mode networking:' && "
            "echo 'Hostname:' $(hostname) && "
            "echo 'IP address:' $(hostname -i) && "
            "echo 'DNS resolv.conf:' && cat /etc/resolv.conf"
            '"'
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Check connectivity to the Docker host
    host_connectivity = DockerOperator(
        task_id="host_connectivity",
        image="alpine:3.20",
        command=(
            'sh -c "'
            "echo 'Testing network connectivity:' && "
            "echo 'Pinging gateway...' && "
            "ping -c 1 -W 2 host.docker.internal 2>/dev/null && "
            "echo 'Gateway reachable' || echo 'Gateway not reachable (expected in some environments)'"
            '"'
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # DNS resolution inside container
    dns_resolution = DockerOperator(
        task_id="dns_resolution",
        image="alpine:3.20",
        command=(
            'sh -c "'
            "echo 'DNS resolution test:' && "
            "nslookup dns.google 2>/dev/null && "
            "echo 'DNS working' || echo 'DNS resolution test complete'"
            '"'
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    bridge_mode >> host_connectivity >> dns_resolution
