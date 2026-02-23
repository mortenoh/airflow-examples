"""DAG 45: BashOperator Scripting Patterns.

Demonstrates advanced bash scripting within ``BashOperator``: error
handling with ``set -euo pipefail``, functions, loops, conditionals,
file I/O, temporary files, and structured data processing.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="045_bash_scripting",
    default_args=DEFAULT_ARGS,
    description="BashOperator advanced scripting: functions, loops, error handling",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "bash"],
) as dag:
    # --- Strict mode and error handling ---------------------------------
    # set -e: exit on error, -u: error on undefined vars,
    # -o pipefail: catch pipe failures
    strict_mode = BashOperator(
        task_id="strict_mode",
        bash_command="""
            set -euo pipefail

            echo "=== Strict Mode Demo ==="

            # This function validates input (would fail with set -u if var missing)
            validate() {
                local value="$1"
                local name="$2"
                if [ -z "$value" ]; then
                    echo "ERROR: $name is empty"
                    return 1
                fi
                echo "OK: $name = $value"
            }

            validate "oslo" "city"
            validate "12.5" "temperature"
            validate "2024-01-01" "date"
            echo "All validations passed"
        """,
    )

    # --- Bash functions -------------------------------------------------
    # Define and call functions within a single BashOperator.
    bash_functions = BashOperator(
        task_id="bash_functions",
        bash_command="""
            # Function to format a log message with timestamp
            log() {
                local level="$1"
                shift
                echo "[$(date '+%H:%M:%S')] [$level] $*"
            }

            # Function to calculate a simple statistic (using awk instead of bc)
            compute_mean() {
                local sum=0
                local count=0
                for val in "$@"; do
                    sum=$(awk "BEGIN {print $sum + $val}")
                    count=$((count + 1))
                done
                awk "BEGIN {printf \\"%.2f\\", $sum / $count}"
            }

            log "INFO" "Starting computation"
            TEMPS="12.5 13.1 9.8 3.2 11.0"
            log "INFO" "Input temperatures: $TEMPS"

            MEAN=$(compute_mean $TEMPS)
            log "INFO" "Mean temperature: $MEAN"

            log "INFO" "Computation complete"
        """,
    )

    # --- Loops and iteration --------------------------------------------
    loops = BashOperator(
        task_id="loops",
        bash_command="""
            echo "=== For loop over list ==="
            CITIES="oslo bergen tromso stavanger"
            for city in $CITIES; do
                echo "  Processing: $city"
            done

            echo ""
            echo "=== C-style for loop ==="
            for ((i=1; i<=5; i++)); do
                echo "  Iteration $i: value=$((i * 10))"
            done

            echo ""
            echo "=== While loop with counter ==="
            COUNT=0
            while [ $COUNT -lt 3 ]; do
                echo "  Count: $COUNT"
                COUNT=$((COUNT + 1))
            done

            echo ""
            echo "=== Sequence generation ==="
            seq 1 5 | while read -r n; do
                echo "  Square of $n = $((n * n))"
            done
        """,
    )

    # --- Conditionals and case statements -------------------------------
    conditionals = BashOperator(
        task_id="conditionals",
        bash_command="""
            TEMP=15
            echo "Temperature: ${TEMP}C"

            # if/elif/else
            if [ "$TEMP" -lt 0 ]; then
                CATEGORY="freezing"
            elif [ "$TEMP" -lt 10 ]; then
                CATEGORY="cold"
            elif [ "$TEMP" -lt 20 ]; then
                CATEGORY="mild"
            elif [ "$TEMP" -lt 30 ]; then
                CATEGORY="warm"
            else
                CATEGORY="hot"
            fi
            echo "Category: $CATEGORY"

            # case statement
            SENSOR_TYPE="temperature"
            case "$SENSOR_TYPE" in
                temperature)
                    UNIT="celsius"
                    RANGE="-50 to 60"
                    ;;
                humidity)
                    UNIT="percent"
                    RANGE="0 to 100"
                    ;;
                pressure)
                    UNIT="hPa"
                    RANGE="870 to 1084"
                    ;;
                *)
                    UNIT="unknown"
                    RANGE="N/A"
                    ;;
            esac
            echo "Sensor: $SENSOR_TYPE, Unit: $UNIT, Range: $RANGE"
        """,
    )

    # --- Temporary files and file I/O -----------------------------------
    file_operations = BashOperator(
        task_id="file_operations",
        bash_command="""
            set -euo pipefail

            # Create a temp directory (cleaned up automatically)
            WORKDIR=$(mktemp -d)
            echo "Working in: $WORKDIR"

            # Write CSV data
            cat > "$WORKDIR/weather.csv" << 'CSVDATA'
            station,temp_c,humidity
            oslo_01,12.5,68.0
            bergen_01,9.8,82.0
            tromso_01,3.2,71.0
            stavanger_01,11.0,75.0
CSVDATA

            echo "=== Raw data ==="
            cat "$WORKDIR/weather.csv"

            echo ""
            echo "=== Stations with temp > 10 ==="
            tail -n +2 "$WORKDIR/weather.csv" | while IFS=',' read -r station temp humidity; do
                station=$(echo "$station" | tr -d ' ')
                temp=$(echo "$temp" | tr -d ' ')
                if [ "$(awk "BEGIN {print ($temp > 10) ? 1 : 0}")" -eq 1 ]; then
                    echo "  $station: ${temp}C, ${humidity}% humidity"
                fi
            done

            echo ""
            echo "=== Line and word counts ==="
            echo "  Lines: $(wc -l < "$WORKDIR/weather.csv")"
            echo "  Words: $(wc -w < "$WORKDIR/weather.csv")"

            # Cleanup
            rm -rf "$WORKDIR"
            echo "Temp directory cleaned up"
        """,
    )

    # --- Arrays and structured data -------------------------------------
    # {% raw %} prevents Jinja from interpreting ${#ARRAY[@]} as a comment tag
    arrays = BashOperator(
        task_id="arrays",
        bash_command="""{% raw %}
            # Indexed array
            STATIONS=("oslo_01" "bergen_01" "tromso_01" "stavanger_01")
            echo "=== Stations (${#STATIONS[@]} total) ==="
            for i in "${!STATIONS[@]}"; do
                echo "  [$i] ${STATIONS[$i]}"
            done

            # Parallel arrays (poor man's records)
            CITIES=("Oslo" "Bergen" "Tromso")
            TEMPS=(12.5 9.8 3.2)
            HUMIDITIES=(68 82 71)

            echo ""
            echo "=== Weather Report ==="
            printf "%-10s %8s %10s\n" "City" "Temp(C)" "Humidity(%)"
            printf "%-10s %8s %10s\n" "----------" "--------" "-----------"
            for i in "${!CITIES[@]}"; do
                printf "%-10s %8s %10s\n" "${CITIES[$i]}" "${TEMPS[$i]}" "${HUMIDITIES[$i]}"
            done

            # Associative array (bash 4+)
            declare -A CAPITALS
            CAPITALS["Norway"]="Oslo"
            CAPITALS["Sweden"]="Stockholm"
            CAPITALS["Denmark"]="Copenhagen"
            CAPITALS["Finland"]="Helsinki"

            echo ""
            echo "=== Nordic Capitals ==="
            for country in "${!CAPITALS[@]}"; do
                echo "  $country -> ${CAPITALS[$country]}"
            done
        {% endraw %}""",
    )

    # --- String processing and text manipulation ------------------------
    # {% raw %} prevents Jinja from interpreting ${#TEXT} as a comment tag
    text_processing = BashOperator(
        task_id="text_processing",
        bash_command="""{% raw %}
            DATA="oslo:12.5:68|bergen:9.8:82|tromso:3.2:71"

            echo "=== Parsing delimited data ==="
            IFS='|' read -ra RECORDS <<< "$DATA"
            for record in "${RECORDS[@]}"; do
                IFS=':' read -r city temp humidity <<< "$record"
                echo "  City: $city, Temp: ${temp}C, Humidity: ${humidity}%"
            done

            echo ""
            echo "=== String operations ==="
            TEXT="  Hello Airflow World  "
            echo "  Original:    '$TEXT'"
            echo "  Uppercase:   '$(echo "$TEXT" | tr '[:lower:]' '[:upper:]')'"
            echo "  Lowercase:   '$(echo "$TEXT" | tr '[:upper:]' '[:lower:]')'"
            echo "  Trimmed:     '$(echo "$TEXT" | xargs)'"
            echo "  Length:      ${#TEXT}"

            echo ""
            echo "=== Pattern matching ==="
            FILENAME="weather_oslo_2024-01-15.nc"
            echo "  File: $FILENAME"
            echo "  City: $(echo "$FILENAME" | sed 's/weather_\\(.*\\)_[0-9]*.*/\\1/')"
            echo "  Date: $(echo "$FILENAME" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')"
            echo "  Ext:  ${FILENAME##*.}"
        {% endraw %}""",
    )

    # --- Multi-step data pipeline in pure bash --------------------------
    # Uses awk instead of bc (bc is not available in the Airflow container)
    bash_pipeline = BashOperator(
        task_id="bash_pipeline",
        bash_command="""
            set -euo pipefail

            WORKDIR=$(mktemp -d)

            # Step 1: Generate sample data
            echo "Step 1: Generating data"
            for i in $(seq 1 20); do
                TEMP=$(awk "BEGIN {printf \\"%.1f\\", ($RANDOM % 300) / 10}")
                HUMIDITY=$(awk "BEGIN {printf \\"%d\\", 50 + ($RANDOM % 50)}")
                echo "$i,$TEMP,$HUMIDITY"
            done > "$WORKDIR/raw.csv"
            echo "  Generated $(wc -l < "$WORKDIR/raw.csv") records"

            # Step 2: Filter (temp > 10)
            echo "Step 2: Filtering records"
            while IFS=',' read -r id temp hum; do
                if [ "$(awk "BEGIN {print ($temp > 10) ? 1 : 0}")" -eq 1 ]; then
                    echo "$id,$temp,$hum"
                fi
            done < "$WORKDIR/raw.csv" > "$WORKDIR/filtered.csv"
            echo "  Kept $(wc -l < "$WORKDIR/filtered.csv") records"

            # Step 3: Compute summary
            echo "Step 3: Computing summary"
            COUNT=$(wc -l < "$WORKDIR/filtered.csv")
            SUM=$(awk -F',' '{s+=$2} END {print s}' "$WORKDIR/filtered.csv")
            MEAN=$(awk "BEGIN {printf \\"%.2f\\", $SUM / $COUNT}")
            echo "  Count: $COUNT"
            echo "  Sum:   $SUM"
            echo "  Mean:  $MEAN"

            # Cleanup
            rm -rf "$WORKDIR"
            echo "Pipeline complete"

            # Push summary to XCom (last line)
            echo "$MEAN"
        """,
    )

    # Flow
    strict_mode >> bash_functions >> loops
    loops >> conditionals
    loops >> file_operations >> arrays
    loops >> text_processing
    file_operations >> bash_pipeline
