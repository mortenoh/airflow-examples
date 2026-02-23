"""DAG 50: FileSensor.

Demonstrates ``FileSensor`` for waiting until a file appears on disk
before proceeding with downstream tasks. Shows both ``poke`` and
``reschedule`` modes, wildcard patterns with ``glob``, and the
practical pattern of one task creating a file that a sensor detects.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="050_file_sensor",
    default_args=DEFAULT_ARGS,
    description="FileSensor: wait for files on disk, poke vs reschedule mode",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "sensor"],
) as dag:
    # Create a file that the sensor will detect
    create_file = BashOperator(
        task_id="create_file",
        bash_command="""
            mkdir -p /opt/airflow/target/sensor_test
            echo '{"status": "ready", "rows": 1000}' > /opt/airflow/target/sensor_test/data_ready.json
            echo "Created /opt/airflow/target/sensor_test/data_ready.json"
        """,
    )

    # FileSensor in poke mode: holds the worker slot while polling.
    # Use for short waits where you expect the file to appear quickly.
    wait_for_file = FileSensor(
        task_id="wait_for_file_poke",
        filepath="/opt/airflow/target/sensor_test/data_ready.json",
        poke_interval=2,
        timeout=30,
        mode="poke",
    )

    process_file = BashOperator(
        task_id="process_file",
        bash_command="""
            echo "=== Processing detected file ==="
            cat /opt/airflow/target/sensor_test/data_ready.json
        """,
    )

    # Create another file for the reschedule-mode sensor
    create_csv = BashOperator(
        task_id="create_csv",
        bash_command="""
            echo "station,temp,humidity" > /opt/airflow/target/sensor_test/weather.csv
            echo "oslo_01,12.5,68.0" >> /opt/airflow/target/sensor_test/weather.csv
            echo "Created /opt/airflow/target/sensor_test/weather.csv"
        """,
    )

    # FileSensor in reschedule mode: releases the worker slot between polls.
    # Use for longer waits to avoid blocking a worker slot.
    wait_for_csv = FileSensor(
        task_id="wait_for_csv_reschedule",
        filepath="/opt/airflow/target/sensor_test/weather.csv",
        poke_interval=5,
        timeout=30,
        mode="reschedule",
    )

    process_csv = BashOperator(
        task_id="process_csv",
        bash_command="""
            echo "=== Processing CSV ==="
            cat /opt/airflow/target/sensor_test/weather.csv
            echo "Lines: $(wc -l < /opt/airflow/target/sensor_test/weather.csv)"
        """,
    )

    # Cleanup
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command="rm -rf /opt/airflow/target/sensor_test && echo 'Cleaned up'",
    )

    # File created first, then sensor detects it
    create_file >> wait_for_file >> process_file
    create_csv >> wait_for_csv >> process_csv
    [process_file, process_csv] >> cleanup
