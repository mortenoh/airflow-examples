"""DAG 44: BashOperator Environment Variables.

Demonstrates environment variable handling in ``BashOperator``: the
``env`` parameter for static variables, Jinja-templated variables,
``append_env=True`` for extending (not replacing) the base environment,
and passing data between tasks via environment variables and XCom.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="044_bash_environment",
    default_args=DEFAULT_ARGS,
    description="BashOperator env vars: env, append_env, templated vars",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "bash"],
    params={"region": "europe", "debug": "true"},
) as dag:
    # --- Static environment variables -----------------------------------
    # The `env` parameter passes a dict of environment variables to the
    # bash subprocess. By default this REPLACES the entire environment
    # (PATH, HOME, etc. are NOT inherited).
    static_env = BashOperator(
        task_id="static_env",
        bash_command="""
            echo "APP_NAME:    $APP_NAME"
            echo "APP_VERSION: $APP_VERSION"
            echo "LOG_LEVEL:   $LOG_LEVEL"
        """,
        env={
            "APP_NAME": "climate-pipeline",
            "APP_VERSION": "2.1.0",
            "LOG_LEVEL": "INFO",
        },
        append_env=True,
    )

    # --- append_env=True ------------------------------------------------
    # With append_env=True, the env dict is MERGED into the existing
    # environment. This preserves PATH, HOME, etc. while adding custom vars.
    append_env_demo = BashOperator(
        task_id="append_env_demo",
        bash_command="""
            echo "Custom var:  $PIPELINE_STAGE"
            echo "PATH exists: $(test -n "$PATH" && echo yes || echo no)"
            echo "HOME exists: $(test -n "$HOME" && echo yes || echo no)"
            echo "USER exists: $(test -n "$USER" && echo yes || echo no)"
        """,
        env={"PIPELINE_STAGE": "transform"},
        append_env=True,
    )

    # --- Jinja-templated environment variables --------------------------
    # Values in the env dict support Jinja templates, giving access to
    # Airflow macros, params, and context variables.
    templated_env = BashOperator(
        task_id="templated_env",
        bash_command="""
            echo "EXEC_DATE:  $EXEC_DATE"
            echo "DAG_ID:     $DAG_ID"
            echo "RUN_ID:     $RUN_ID"
            echo "REGION:     $REGION"
            echo "DEBUG:      $DEBUG"
        """,
        env={
            "EXEC_DATE": "{{ ds }}",
            "DAG_ID": "{{ dag.dag_id }}",
            "RUN_ID": "{{ run_id }}",
            "REGION": "{{ params.region }}",
            "DEBUG": "{{ params.debug }}",
        },
        append_env=True,
    )

    # --- Multiple configuration profiles -------------------------------
    # Use environment variables to simulate different configs.
    dev_config = BashOperator(
        task_id="dev_config",
        bash_command="""
            echo "=== $ENV_NAME Configuration ==="
            echo "  Database: $DB_HOST:$DB_PORT/$DB_NAME"
            echo "  Log level: $LOG_LEVEL"
            echo "  Debug: $DEBUG_MODE"
        """,
        env={
            "ENV_NAME": "Development",
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_NAME": "dev_db",
            "LOG_LEVEL": "DEBUG",
            "DEBUG_MODE": "true",
        },
        append_env=True,
    )

    prod_config = BashOperator(
        task_id="prod_config",
        bash_command="""
            echo "=== $ENV_NAME Configuration ==="
            echo "  Database: $DB_HOST:$DB_PORT/$DB_NAME"
            echo "  Log level: $LOG_LEVEL"
            echo "  Debug: $DEBUG_MODE"
        """,
        env={
            "ENV_NAME": "Production",
            "DB_HOST": "db.prod.internal",
            "DB_PORT": "5432",
            "DB_NAME": "prod_db",
            "LOG_LEVEL": "WARNING",
            "DEBUG_MODE": "false",
        },
        append_env=True,
    )

    # --- Passing data between tasks via XCom + env ----------------------
    # Task 1 pushes output to XCom (last line of stdout).
    generate_config = BashOperator(
        task_id="generate_config",
        bash_command='echo "batch_size=500,timeout=30,retries=3"',
    )

    # Task 2 pulls that XCom value into an environment variable.
    use_config = BashOperator(
        task_id="use_config",
        bash_command="""
            echo "Received config: $PIPELINE_CONFIG"
            IFS=',' read -ra PARAMS <<< "$PIPELINE_CONFIG"
            for param in "${PARAMS[@]}"; do
                echo "  $param"
            done
        """,
        env={
            "PIPELINE_CONFIG": "{{ ti.xcom_pull(task_ids='generate_config') }}",
        },
        append_env=True,
    )

    # --- Variable expansion patterns ------------------------------------
    variable_patterns = BashOperator(
        task_id="variable_patterns",
        # {% raw %} prevents Jinja from interpreting ${#VAR} as a comment tag
        bash_command="""{% raw %}
            # Default values with ${VAR:-default}
            echo "WITH_DEFAULT:    ${MISSING_VAR:-fallback_value}"
            echo "SET_VAR:         ${SET_VAR:-fallback_value}"

            # String length
            echo "Length of SET_VAR: ${#SET_VAR}"

            # Substring extraction
            FILENAME="weather_data_2024_oslo.csv"
            echo "Full name:  $FILENAME"
            echo "Extension:  ${FILENAME##*.}"
            echo "Base name:  ${FILENAME%.*}"
            echo "Replace:    ${FILENAME/oslo/bergen}"
        {% endraw %}""",
        env={"SET_VAR": "hello_airflow"},
        append_env=True,
    )

    # Flow
    static_env >> append_env_demo >> templated_env
    templated_env >> [dev_config, prod_config]
    templated_env >> generate_config >> use_config
    templated_env >> variable_patterns
