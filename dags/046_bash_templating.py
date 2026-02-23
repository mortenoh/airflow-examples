"""DAG 46: BashOperator with Jinja Templating.

Demonstrates Jinja template rendering inside ``BashOperator`` commands:
Airflow macros (``ds``, ``logical_date``, ``macros.ds_add``), DAG params,
XCom references, dynamic file paths, and conditional logic within
templates. Builds on DAG 08 with deeper bash-specific patterns.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="046_bash_templating",
    default_args=DEFAULT_ARGS,
    description="BashOperator Jinja templates: macros, params, XCom, dynamic paths",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "bash"],
    params={
        "station": "oslo_01",
        "threshold": 10.0,
        "format": "csv",
    },
) as dag:
    # --- Core template variables ----------------------------------------
    # bash_command is a templated field: Jinja expressions are rendered
    # before the command is executed.
    core_macros = BashOperator(
        task_id="core_macros",
        bash_command="""
            echo "=== Airflow Template Variables ==="
            echo "ds (YYYY-MM-DD):     {{ ds }}"
            echo "ds_nodash:           {{ ds_nodash }}"
            echo "logical_date:        {{ logical_date }}"
            echo "dag_id:              {{ dag.dag_id }}"
            echo "task_id:             {{ task.task_id }}"
            echo "run_id:              {{ run_id }}"
            echo "try_number:          {{ task_instance.try_number }}"
        """,
    )

    # --- Date arithmetic with macros ------------------------------------
    date_macros = BashOperator(
        task_id="date_macros",
        bash_command="""
            echo "=== Date Arithmetic ==="
            echo "Today:          {{ ds }}"
            echo "Yesterday:      {{ macros.ds_add(ds, -1) }}"
            echo "Tomorrow:       {{ macros.ds_add(ds, 1) }}"
            echo "One week ago:   {{ macros.ds_add(ds, -7) }}"
            echo "One month ago:  {{ macros.ds_add(ds, -30) }}"
            echo ""
            echo "=== Formatted dates ==="
            echo "ds_nodash:      {{ ds_nodash }}"
            echo "Year:           {{ logical_date.strftime('%Y') }}"
            echo "Month:          {{ logical_date.strftime('%m') }}"
            echo "Day:            {{ logical_date.strftime('%d') }}"
            echo "ISO format:     {{ logical_date.isoformat() }}"
        """,
    )

    # --- DAG params in templates ----------------------------------------
    # Access DAG-level params defined in the DAG constructor.
    params_in_bash = BashOperator(
        task_id="params_in_bash",
        bash_command="""
            echo "=== DAG Parameters ==="
            echo "Station:    {{ params.station }}"
            echo "Threshold:  {{ params.threshold }}"
            echo "Format:     {{ params.format }}"
            echo ""
            echo "Processing station {{ params.station }} with threshold {{ params.threshold }}"
        """,
    )

    # --- Dynamic file paths ---------------------------------------------
    # Build file paths that incorporate execution date and params.
    dynamic_paths = BashOperator(
        task_id="dynamic_paths",
        bash_command="""
            # Construct date-partitioned paths from templates
            BASE="/opt/airflow/target/data"
            INGEST_PATH="$BASE/raw/{{ ds_nodash }}/{{ params.station }}"
            OUTPUT_PATH="$BASE/processed/{{ logical_date.strftime('%Y') }}/{{ logical_date.strftime('%m') }}"
            ARCHIVE_PATH="$BASE/archive/{{ ds }}"

            echo "=== Dynamic File Paths ==="
            echo "Ingest:  $INGEST_PATH"
            echo "Output:  $OUTPUT_PATH"
            echo "Archive: $ARCHIVE_PATH"

            # Actually create and use the directories
            mkdir -p "$INGEST_PATH" "$OUTPUT_PATH" "$ARCHIVE_PATH"

            echo '{"station":"{{ params.station }}","date":"{{ ds }}","temp":12.5}' \
                > "$INGEST_PATH/data.json"
            echo "Wrote: $INGEST_PATH/data.json"

            cat "$INGEST_PATH/data.json"

            # Cleanup
            rm -rf "$BASE"
        """,
    )

    # --- XCom in templates ----------------------------------------------
    # Push a value from one task, pull it in another using Jinja.
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="""
            echo "Generating station report..."
            echo "station={{ params.station }},temp=12.5,humidity=68.0,status=ok"
        """,
    )

    # Pull XCom value inline in bash_command template.
    use_xcom = BashOperator(
        task_id="use_xcom",
        bash_command="""
            RAW="{{ ti.xcom_pull(task_ids='generate_data') }}"
            echo "=== Received from generate_data ==="
            echo "Raw: $RAW"

            # Parse the key=value pairs
            IFS=',' read -ra FIELDS <<< "$RAW"
            for field in "${FIELDS[@]}"; do
                KEY="${field%%=*}"
                VALUE="${field#*=}"
                echo "  $KEY -> $VALUE"
            done
        """,
    )

    # --- Jinja control structures in bash_command -----------------------
    # You can use Jinja {% if %}, {% for %}, {% set %} inside bash_command.
    jinja_control = BashOperator(
        task_id="jinja_control",
        bash_command="""
            echo "=== Jinja Control Structures ==="

            # Jinja for-loop generates bash commands at render time
            {% for city in ["oslo", "bergen", "tromso", "stavanger"] %}
            echo "Processing city: {{ city }}"
            {% endfor %}

            echo ""

            # Jinja if/else conditional
            {% if params.format == "csv" %}
            echo "Output format: CSV (comma-separated)"
            FORMAT_EXT="csv"
            {% elif params.format == "json" %}
            echo "Output format: JSON"
            FORMAT_EXT="json"
            {% else %}
            echo "Output format: {{ params.format }} (custom)"
            FORMAT_EXT="{{ params.format }}"
            {% endif %}
            echo "File extension: $FORMAT_EXT"

            echo ""

            # Jinja set to define template variables
            {% set num_stations = 4 %}
            echo "Number of stations: {{ num_stations }}"

            # Jinja filters
            echo "Station uppercase: {{ params.station | upper }}"
            echo "Station length:    {{ params.station | length }}"
        """,
    )

    # --- Templated output filenames for reports -------------------------
    report_generator = BashOperator(
        task_id="report_generator",
        bash_command="""
            REPORT_DIR="/opt/airflow/target/reports"
            mkdir -p "$REPORT_DIR"

            # Generate report with templated filename
            REPORT_FILE="$REPORT_DIR/{{ params.station }}_{{ ds_nodash }}.{{ params.format }}"
            echo "=== Generating Report ==="
            echo "File: $REPORT_FILE"

            {% if params.format == "csv" %}
            cat > "$REPORT_FILE" << 'EOF'
            station,date,temp_c,humidity,status
            {{ params.station }},{{ ds }},12.5,68.0,active
EOF
            {% else %}
            cat > "$REPORT_FILE" << EOF
            {"station": "{{ params.station }}", "date": "{{ ds }}", "temp_c": 12.5}
EOF
            {% endif %}

            echo "Contents:"
            cat "$REPORT_FILE"

            # Cleanup
            rm -rf "$REPORT_DIR"
            echo "Report file: $REPORT_FILE"
        """,
    )

    # --- Combining everything: templated pipeline -----------------------
    full_pipeline = BashOperator(
        task_id="full_pipeline",
        bash_command="""
            set -euo pipefail

            echo "=== Templated Pipeline ==="
            echo "Station:   {{ params.station }}"
            echo "Date:      {{ ds }}"
            echo "Threshold: {{ params.threshold }}"

            WORKDIR=$(mktemp -d)

            # Generate data using template params
            {% for i in range(1, 11) %}
            echo "{{ params.station }},{{ i }},$(echo "scale=1; $RANDOM % 300 / 10" | bc)" \
                >> "$WORKDIR/data.csv"
            {% endfor %}

            echo ""
            echo "Generated $(wc -l < "$WORKDIR/data.csv") records"

            # Filter using template threshold
            echo "Filtering with threshold > {{ params.threshold }}"
            KEPT=0
            while IFS=',' read -r station id temp; do
                if [ "$(echo "$temp > {{ params.threshold }}" | bc 2>/dev/null)" = "1" ]; then
                    echo "  KEEP: station=$station id=$id temp=$temp"
                    KEPT=$((KEPT + 1))
                fi
            done < "$WORKDIR/data.csv"

            echo "Kept $KEPT records above {{ params.threshold }}C"

            rm -rf "$WORKDIR"
            echo "Pipeline complete for {{ params.station }} on {{ ds }}"
        """,
    )

    # Flow
    core_macros >> date_macros >> params_in_bash
    params_in_bash >> dynamic_paths
    params_in_bash >> generate_data >> use_xcom
    params_in_bash >> jinja_control >> report_generator
    [dynamic_paths, use_xcom, report_generator] >> full_pipeline
