"""DAG 41: Deferrable Sensors.

Demonstrates deferrable (async) execution using ``deferrable=True``.
Deferrable tasks release their worker slot while waiting, then
resume when the trigger fires. This is Airflow 3.x's approach
to efficient sensor execution (replaces the old *Async classes).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="041_deferrable_sensors",
    default_args=DEFAULT_ARGS,
    description="Deferrable sensors that release worker slots while waiting",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "deferrable"],
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting deferrable sensor demo"',
    )

    # Traditional (poke) sensor: holds worker slot
    poke_sensor = TimeDeltaSensor(
        task_id="poke_sensor",
        delta=timedelta(seconds=0),
        poke_interval=2,
        timeout=10,
        mode="poke",
    )

    after_poke = BashOperator(
        task_id="after_poke",
        bash_command='echo "Poke sensor completed (held worker slot the entire time)"',
    )

    # Deferrable sensor: releases worker slot, triggerer handles the wait
    defer_sensor = TimeDeltaSensor(
        task_id="defer_sensor",
        delta=timedelta(seconds=0),
        poke_interval=2,
        timeout=10,
        deferrable=True,
    )

    after_defer = BashOperator(
        task_id="after_defer",
        bash_command='echo "Deferrable sensor completed (worker slot was free while waiting)"',
    )

    done = BashOperator(
        task_id="done",
        bash_command=(
            'echo "Both sensor modes completed" && '
            'echo "Key difference:" && '
            'echo "  poke mode: worker occupied during wait" && '
            'echo "  deferrable: worker freed, triggerer handles it"'
        ),
    )

    start >> poke_sensor >> after_poke >> done
    start >> defer_sensor >> after_defer >> done
