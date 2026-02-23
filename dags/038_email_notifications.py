"""DAG 38: Email Notifications.

Demonstrates ``EmailOperator`` for sending email notifications from
Airflow tasks. Uses a local Mailpit SMTP server (viewable at
http://localhost:8025). Shows plain emails, HTML emails, and
callback-triggered notifications.
"""

from datetime import datetime

from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="038_email_notifications",
    default_args=DEFAULT_ARGS,
    description="EmailOperator notifications via Mailpit SMTP",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "email"],
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting email notification demo"',
    )

    # Simple plain-text email
    send_plain = EmailOperator(
        task_id="send_plain",
        conn_id="smtp_default",
        to="team@example.com",
        subject="Airflow: Pipeline started ({{ ds }})",
        html_content="<p>The daily pipeline has started for execution date {{ ds }}.</p>",
        from_email="airflow@example.com",
    )

    # HTML email with formatting
    send_html = EmailOperator(
        task_id="send_html",
        conn_id="smtp_default",
        to="team@example.com",
        subject="Airflow: Pipeline report ({{ ds }})",
        html_content="""
        <h2>Pipeline Execution Report</h2>
        <table border="1" cellpadding="5">
            <tr><th>Field</th><th>Value</th></tr>
            <tr><td>DAG</td><td>{{ dag.dag_id }}</td></tr>
            <tr><td>Execution Date</td><td>{{ ds }}</td></tr>
            <tr><td>Run ID</td><td>{{ run_id }}</td></tr>
        </table>
        <p>View details in the <a href="http://localhost:8080">Airflow UI</a>.</p>
        """,
        from_email="airflow@example.com",
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "Emails sent. View at http://localhost:8025"',
    )

    start >> send_plain >> send_html >> done
