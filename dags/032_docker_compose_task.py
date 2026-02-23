"""DAG 32: Docker Compose from Airflow.

Demonstrates running Docker containers from ``BashOperator`` and
inspecting the Docker environment using the Python Docker SDK.
Shows how to run one-off containers, list running services, and
get system information -- all from within an Airflow DAG.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="032_docker_compose_task",
    default_args=DEFAULT_ARGS,
    description="Docker environment inspection and one-off containers from DAGs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:

    @task
    def docker_info() -> dict[str, str]:
        """Get Docker system info using the Python Docker SDK."""
        import docker

        client = docker.from_env()
        info = client.info()
        result = {
            "server_version": info.get("ServerVersion", "unknown"),
            "os": info.get("OperatingSystem", "unknown"),
            "arch": info.get("Architecture", "unknown"),
            "cpus": str(info.get("NCPU", 0)),
            "containers": str(info.get("Containers", 0)),
            "images": str(info.get("Images", 0)),
        }
        for key, val in result.items():
            print(f"  {key}: {val}")
        client.close()
        return result

    @task
    def list_containers() -> list[str]:
        """List running containers using the Python Docker SDK."""
        import docker

        client = docker.from_env()
        containers = client.containers.list()
        names = []
        for c in containers:
            name = c.name
            status = c.status
            image = c.image.tags[0] if c.image.tags else "untagged"
            print(f"  {name}: {image} ({status})")
            names.append(name)
        client.close()
        return names

    # Run a one-off container via DockerOperator
    run_container = DockerOperator(
        task_id="run_one_off",
        image="alpine:3.20",
        command='sh -c "echo \'One-off container from DockerOperator\' && hostname && date -u"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "Docker environment inspection complete"',
    )

    docker_info() >> list_containers() >> run_container >> done
