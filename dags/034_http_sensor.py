"""DAG 34: HTTP Sensor.

Demonstrates ``HttpSensor`` for polling an HTTP endpoint until
a condition is met. Shows both poke mode and deferrable mode
(which releases the worker slot while waiting).
"""

from datetime import datetime

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="034_http_sensor",
    default_args=DEFAULT_ARGS,
    description="HttpSensor: poll an endpoint until condition is met",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "http"],
) as dag:
    # Poke mode: holds worker slot, checks periodically
    wait_for_api_poke = HttpSensor(
        task_id="wait_for_api_poke",
        http_conn_id="http_default",
        endpoint="/status/200",
        response_check=lambda response: response.status_code == 200,
        poke_interval=2,
        timeout=15,
        mode="poke",
    )

    after_poke = BashOperator(
        task_id="after_poke",
        bash_command='echo "API is available (poke mode confirmed)"',
    )

    # Deferrable mode: releases worker slot while waiting
    wait_for_api_defer = HttpSensor(
        task_id="wait_for_api_defer",
        http_conn_id="http_default",
        endpoint="/status/200",
        response_check=lambda response: response.status_code == 200,
        poke_interval=2,
        timeout=15,
        deferrable=True,
    )

    after_defer = BashOperator(
        task_id="after_defer",
        bash_command='echo "API is available (deferrable mode confirmed)"',
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "Both sensor modes validated"',
    )

    wait_for_api_poke >> after_poke >> done
    wait_for_api_defer >> after_defer >> done
