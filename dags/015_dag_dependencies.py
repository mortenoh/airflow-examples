"""DAG 15: DAG Dependencies.

Demonstrates cross-DAG orchestration using ``TriggerDagRunOperator``
to trigger another DAG, and ``ExternalTaskSensor`` to wait for a
task in another DAG to complete before proceeding.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

# --- DAG A: triggers DAG B and waits for it --------------------------------

with DAG(
    dag_id="015_dag_dependencies",
    default_args=DEFAULT_ARGS,
    description="Cross-DAG orchestration: TriggerDagRunOperator + ExternalTaskSensor",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting cross-DAG orchestration"',
    )

    # Trigger another DAG (001_hello_world) from this DAG
    trigger_hello = TriggerDagRunOperator(
        task_id="trigger_hello_world",
        trigger_dag_id="001_hello_world",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Wait for task "say_hello" in the triggered DAG to finish.
    # ExternalTaskSensor polls the metadata DB for the task state.
    wait_for_hello = ExternalTaskSensor(
        task_id="wait_for_hello",
        external_dag_id="001_hello_world",
        external_task_id="say_hello",
        timeout=60,
        poke_interval=5,
        mode="poke",
        allowed_states=["success"],
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "001_hello_world completed, orchestration done"',
    )

    start >> trigger_hello >> wait_for_hello >> done
