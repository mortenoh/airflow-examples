"""DAG 39: SSH Commands.

Demonstrates ``SSHOperator`` for executing commands on a remote
host via SSH. Uses a local OpenSSH server container. Shows
single commands, multi-command scripts, and environment passing.
"""

from datetime import datetime

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="039_ssh_commands",
    default_args=DEFAULT_ARGS,
    description="SSHOperator for remote command execution",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "ssh"],
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting SSH command demo"',
    )

    # Simple remote command
    remote_uname = SSHOperator(
        task_id="remote_uname",
        ssh_conn_id="ssh_default",
        command="uname -a",
        cmd_timeout=10,
    )

    # Remote system info
    remote_info = SSHOperator(
        task_id="remote_info",
        ssh_conn_id="ssh_default",
        command="echo 'Hostname:' $(hostname) && echo 'Uptime:' $(uptime) && echo 'Disk:' $(df -h / | tail -1)",
        cmd_timeout=10,
    )

    # Multi-command script
    remote_script = SSHOperator(
        task_id="remote_script",
        ssh_conn_id="ssh_default",
        command=(
            "echo '--- Remote Script Start ---' && "
            "echo 'User: '$(whoami) && "
            "echo 'PWD: '$(pwd) && "
            "echo 'Date: '$(date) && "
            "echo 'Files in home:' && ls -la ~ && "
            "echo '--- Remote Script End ---'"
        ),
        cmd_timeout=10,
    )

    # Create and read a file remotely
    remote_file = SSHOperator(
        task_id="remote_file",
        ssh_conn_id="ssh_default",
        command=(
            "echo 'Writing file on remote...' && "
            "echo 'Hello from Airflow SSH' > /tmp/airflow_test.txt && "
            "echo 'Reading back:' && cat /tmp/airflow_test.txt && "
            "rm /tmp/airflow_test.txt && "
            "echo 'File cleaned up'"
        ),
        cmd_timeout=10,
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "All SSH commands executed successfully"',
    )

    start >> remote_uname >> remote_info >> remote_script >> remote_file >> done
