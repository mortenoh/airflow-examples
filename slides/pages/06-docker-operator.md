# DockerOperator

Run tasks in isolated containers -- any image, any language

```python {all|1-8|10-17}
from airflow.providers.docker.operators.docker import DockerOperator

hello = DockerOperator(
    task_id="hello",
    image="alpine:3.20",
    command='echo "Hello from inside a Docker container!"',
    auto_remove="success",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
)

uname = DockerOperator(
    task_id="uname",
    image="alpine:3.20",
    command="uname -a",
    auto_remove="success",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
)
```

Also supports: `volumes`, `environment`, `mem_limit`, `cpus`, custom `entrypoint`

<span class="text-sm opacity-60">DAG 021 -- docker_hello.py</span>
