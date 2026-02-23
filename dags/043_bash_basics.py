"""DAG 43: BashOperator Basics.

Demonstrates fundamental ``BashOperator`` usage: single commands,
multi-line scripts, exit codes, working directories, and capturing
output. BashOperator is Airflow's workhorse for running shell commands
directly on the worker node.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="043_bash_basics",
    default_args=DEFAULT_ARGS,
    description="BashOperator fundamentals: commands, scripts, exit codes, cwd",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "bash"],
) as dag:
    # --- Single commands ------------------------------------------------
    # The simplest BashOperator: run a single shell command.
    single_command = BashOperator(
        task_id="single_command",
        bash_command='echo "Hello from BashOperator"',
    )

    # Multiple commands chained with && (all must succeed).
    chained_commands = BashOperator(
        task_id="chained_commands",
        bash_command=(
            'echo "Step 1: check date" && '
            "date '+%Y-%m-%d %H:%M:%S' && "
            'echo "Step 2: check hostname" && '
            "hostname && "
            'echo "Step 3: check user" && '
            "whoami"
        ),
    )

    # --- Multi-line scripts ---------------------------------------------
    # Use triple-quoted strings for readable multi-line bash scripts.
    # The script runs in a temporary file via `bash -c`.
    multiline_script = BashOperator(
        task_id="multiline_script",
        bash_command="""
            echo "=== System Information ==="
            echo "Date:     $(date '+%Y-%m-%d %H:%M:%S')"
            echo "Hostname: $(hostname)"
            echo "User:     $(whoami)"
            echo "Shell:    $SHELL"
            echo "PWD:      $(pwd)"
            echo "Uptime:   $(uptime)"
            echo "========================="
        """,
    )

    # --- Working directory ----------------------------------------------
    # cwd sets the working directory for the bash command.
    working_directory = BashOperator(
        task_id="working_directory",
        bash_command='echo "Working directory: $(pwd)" && ls -la',
        cwd="/tmp",
    )

    # --- Exit codes -----------------------------------------------------
    # BashOperator fails the task if the command returns a non-zero exit code.
    # Use `|| true` to prevent failure on non-zero exit, or check explicitly.
    exit_code_success = BashOperator(
        task_id="exit_code_success",
        bash_command='echo "This succeeds (exit 0)" && exit 0',
    )

    # skip_on_exit_code: if the command exits with this code, the task is
    # marked SKIPPED instead of FAILED. Useful for conditional execution.
    conditional_skip = BashOperator(
        task_id="conditional_skip",
        bash_command="""
            HOUR=$(date +%H)
            echo "Current hour: $HOUR"
            # Skip if hour is odd (just as a demonstration)
            if [ $((HOUR % 2)) -eq 1 ]; then
                echo "Odd hour -- skipping"
                exit 99
            fi
            echo "Even hour -- proceeding"
        """,
        skip_on_exit_code=99,
    )

    # --- Capturing output via XCom --------------------------------------
    # When do_xcom_push=True (default for BashOperator), the last line
    # of stdout is pushed to XCom with key "return_value".
    produce_output = BashOperator(
        task_id="produce_output",
        bash_command="""
            echo "This line is NOT captured (not the last line)"
            echo "42"
        """,
    )

    # Pull the XCom value from the previous task using Jinja.
    consume_output = BashOperator(
        task_id="consume_output",
        bash_command=(
            'VALUE="{{ ti.xcom_pull(task_ids=\'produce_output\') }}" && '
            'echo "Value from previous task: $VALUE"'
        ),
    )

    # --- Piping and subshells -------------------------------------------
    piping_example = BashOperator(
        task_id="piping_example",
        bash_command="""
            echo "=== Process list (top 5 by memory) ==="
            ps aux | head -1
            ps aux --sort=-%mem 2>/dev/null | head -5 || ps aux | head -5
            echo ""
            echo "=== Disk usage ==="
            df -h | head -5
            echo ""
            echo "=== Word count of /etc/hostname ==="
            cat /etc/hostname | wc -c
        """,
    )

    # --- Command substitution -------------------------------------------
    command_substitution = BashOperator(
        task_id="command_substitution",
        bash_command="""
            TIMESTAMP=$(date '+%Y-%m-%dT%H:%M:%S')
            HOSTNAME=$(hostname)
            KERNEL=$(uname -r)
            echo "Report generated at $TIMESTAMP on $HOSTNAME (kernel $KERNEL)"
        """,
    )

    # Dependencies: run in logical groups
    single_command >> chained_commands >> multiline_script
    multiline_script >> working_directory
    multiline_script >> exit_code_success >> conditional_skip
    multiline_script >> produce_output >> consume_output
    multiline_script >> piping_example >> command_substitution
