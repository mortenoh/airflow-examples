"""DAG 33: HTTP Requests.

Demonstrates ``HttpOperator`` for making HTTP requests from Airflow.
Shows GET requests, POST with JSON body, response handling, and
chaining HTTP calls. Uses a local httpbin service.
"""

from datetime import datetime

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def show_response(**context: object) -> None:
    """Pull and display HTTP response from XCom."""
    ti = context["ti"]  # type: ignore[index]
    response = ti.xcom_pull(task_ids="get_request")
    print(f"[{timestamp()}] GET /get response (first 200 chars):")
    print(f"  {str(response)[:200]}")


with DAG(
    dag_id="033_http_requests",
    default_args=DEFAULT_ARGS,
    description="HttpOperator for GET/POST requests to httpbin",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "http"],
) as dag:
    # Simple GET request
    get_request = HttpOperator(
        task_id="get_request",
        http_conn_id="http_default",
        method="GET",
        endpoint="/get",
        log_response=True,
    )

    # POST request with JSON body
    post_request = HttpOperator(
        task_id="post_request",
        http_conn_id="http_default",
        method="POST",
        endpoint="/post",
        data='{"sensor": "temp_01", "value": 23.5}',
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    # GET with query parameters via endpoint
    get_with_params = HttpOperator(
        task_id="get_with_params",
        http_conn_id="http_default",
        method="GET",
        endpoint="/get?city=oslo&units=metric",
        log_response=True,
    )

    # Show response via PythonOperator
    show = PythonOperator(
        task_id="show_response",
        python_callable=show_response,
    )

    get_request >> post_request >> get_with_params >> show
