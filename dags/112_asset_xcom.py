"""DAG 112: Asset + XCom producer/consumer.

Shows how Assets (scheduling signal) and XCom (data passing) work
together across DAGs. The producer updates an asset and pushes a value
to XCom. The consumer is scheduled by the asset and pulls that value
via cross-DAG XCom.

Note: XCom is scoped to a DAG run, so the consumer must explicitly
reference the producer's ``dag_id`` and ``task_ids`` when pulling.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

from airflow_examples.config import DEFAULT_ARGS, timestamp

PRODUCER_DAG_ID = "112_asset_xcom_producer"
PRODUCER_TASK_ID = "produce_number"

scored_data = Asset("s3://example-bucket/scored_data.json")


def produce_number() -> int:
    """Produce a number and push it to XCom via return value."""
    value = 42
    print(f"[{timestamp()}] Producing scored data with value={value}")
    print(f"[{timestamp()}] Updated asset: scored_data.json")
    return value


def consume_number(**context: object) -> None:
    """Pull the number produced by the other DAG via cross-DAG XCom.

    ``include_prior_dates=True`` is required because the consumer runs in
    a different DAG run than the producer, so the logical dates differ.
    When multiple prior values exist it returns a list; we take the last
    (most recent) entry.
    """
    ti = context["ti"]  # type: ignore[index]
    raw = ti.xcom_pull(
        dag_id=PRODUCER_DAG_ID,
        task_ids=PRODUCER_TASK_ID,
        key="return_value",
        include_prior_dates=True,
    )
    # include_prior_dates may return a list when multiple runs exist
    value = raw[-1] if isinstance(raw, list) else raw
    print(f"[{timestamp()}] Pulled value from producer: {value}")
    if value is not None:
        result = value * 2
        print(f"[{timestamp()}] Computed result: {value} * 2 = {result}")
    else:
        print(f"[{timestamp()}] No XCom value found (producer has not run yet?)")


# --- Producer DAG --------------------------------------------------------
with DAG(
    dag_id=PRODUCER_DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Asset + XCom producer: pushes a number and updates an asset",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "assets"],
) as producer_dag:
    PythonOperator(
        task_id=PRODUCER_TASK_ID,
        python_callable=produce_number,
        outlets=[scored_data],
    )


# --- Consumer DAG --------------------------------------------------------
with DAG(
    dag_id="112_asset_xcom_consumer",
    default_args=DEFAULT_ARGS,
    description="Asset + XCom consumer: triggered by asset, reads cross-DAG XCom",
    start_date=datetime(2024, 1, 1),
    schedule=[scored_data],
    catchup=False,
    tags=["example", "assets"],
) as consumer_dag:
    PythonOperator(
        task_id="consume_number",
        python_callable=consume_number,
    )
