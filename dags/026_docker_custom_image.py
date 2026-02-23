"""DAG 26: Docker Custom Image.

Demonstrates using different specialized container images for
different tasks. Shows how to use entrypoint arguments, working
directories, and user configuration to control container behavior.
"""

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="026_docker_custom_image",
    default_args=DEFAULT_ARGS,
    description="Specialized Docker images with entrypoint args and working dirs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "docker"],
) as dag:
    # Use busybox for lightweight utilities
    busybox_task = DockerOperator(
        task_id="busybox_task",
        image="busybox:1.37",
        command='sh -c "echo \'BusyBox version:\' && busybox --help | head -1 && date -u"',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Use Node.js for JavaScript execution
    # {% raw %} prevents Jinja from interpreting {length: 5} as an expression
    nodejs_task = DockerOperator(
        task_id="nodejs_task",
        image="node:22-alpine",
        command=(
            "{% raw %}"
            "node -e \""
            "const data = Array.from({length: 5}, (_, i) => ({id: i, value: Math.random().toFixed(3)})); "
            "console.log('Generated from Node.js:'); "
            "data.forEach(d => console.log('  id=' + d.id + ' value=' + d.value)); "
            "console.log('Total items: ' + data.length); "
            "\""
            "{% endraw %}"
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Use Ruby for text processing
    ruby_task = DockerOperator(
        task_id="ruby_task",
        image="ruby:3.3-alpine",
        command=(
            "ruby -e \""
            "words = %w[airflow docker ruby pipeline orchestration]; "
            "puts 'Ruby word analysis:'; "
            "words.each do |w| puts '  ' + w + ': ' + w.length.to_s + ' chars' end; "
            "puts 'Longest: ' + words.max_by(&:length); "
            "\""
        ),
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Use a specific working directory inside the container
    workdir_task = DockerOperator(
        task_id="workdir_task",
        image="alpine:3.20",
        command='sh -c "pwd && ls -la && echo \'Working directory demo complete\'"',
        working_dir="/opt",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    busybox_task >> nodejs_task >> ruby_task >> workdir_task
