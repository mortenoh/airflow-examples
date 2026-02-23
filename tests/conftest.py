"""Test configuration for airflow-examples.

Sets Airflow environment variables for lightweight SQLite-based testing
before any airflow imports occur. No Docker infrastructure needed.
"""

import os

os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////tmp/test_airflow.db"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = os.path.join(os.path.dirname(__file__), "..", "dags")
