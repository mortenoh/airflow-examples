"""DAG 11: Sensors.

Demonstrates Airflow sensors that wait for external conditions
before proceeding. Shows ``TimeDeltaSensor`` and ``FileSensor``
with both ``poke`` and ``reschedule`` modes.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="011_sensors",
    default_args=DEFAULT_ARGS,
    description="Sensors: TimeDeltaSensor, poke vs reschedule mode",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # TimeDeltaSensor: waits for a time delta after the logical date
    wait_short = TimeDeltaSensor(
        task_id="wait_short",
        delta=timedelta(seconds=0),
        poke_interval=2,
        timeout=10,
        mode="poke",
    )

    after_wait = BashOperator(
        task_id="after_wait",
        bash_command='echo "Timer sensor completed, continuing pipeline"',
    )

    # Another sensor in reschedule mode (frees up worker slot while waiting)
    wait_reschedule = TimeDeltaSensor(
        task_id="wait_reschedule",
        delta=timedelta(seconds=0),
        poke_interval=5,
        timeout=10,
        mode="reschedule",
    )

    after_reschedule = BashOperator(
        task_id="after_reschedule",
        bash_command='echo "Reschedule-mode sensor completed"',
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "All sensors satisfied, pipeline complete"',
    )

    wait_short >> after_wait >> done
    wait_reschedule >> after_reschedule >> done
