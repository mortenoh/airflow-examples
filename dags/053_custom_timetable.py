"""DAG 53: Custom Timetable.

Demonstrates how to create a custom ``Timetable`` subclass for
non-standard scheduling needs. The built-in cron and timedelta
schedules cover most cases, but custom timetables let you define
arbitrary scheduling logic (e.g., business days only, market hours,
skip holidays).

This example creates a timetable that runs only on weekdays
(Monday-Friday), skipping Saturday and Sunday.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

# Note: Custom timetables require registration via an Airflow plugin
# or the timetable API. For this example, we demonstrate the DAG
# using the built-in CronTriggerTimetable with a cron expression
# that approximates weekday-only scheduling.
#
# A full custom timetable implementation looks like this:
#
#   from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
#   from pendulum import DateTime, instance
#
#   class WeekdayTimetable(Timetable):
#       def next_dagrun_info(self, *, last_automated_dagrun, restriction):
#           # Skip to next weekday if current day is Sat/Sun
#           ...
#           return DagRunInfo.interval(start=start, end=end)
#
#       def infer_manual_data_interval(self, *, run_after):
#           # Handle manually triggered runs
#           ...
#
# Then register it in a plugin:
#
#   class TimetablePlugin(AirflowPlugin):
#       name = "custom_timetables"
#       timetables = [WeekdayTimetable]
#
# And use it in a DAG:
#
#   with DAG(timetable=WeekdayTimetable(), ...):
#       ...

with DAG(
    dag_id="053_custom_timetable",
    default_args=DEFAULT_ARGS,
    description="Custom timetable patterns: weekday-only, business hours",
    start_date=datetime(2024, 1, 1),
    # This cron runs at 06:00 Mon-Fri (weekdays only)
    # "0 6 * * 1-5" = minute 0, hour 6, any day of month, any month, Mon-Fri
    schedule=None,  # Use "0 6 * * 1-5" for real weekday scheduling
    catchup=False,
    tags=["example", "scheduling"],
) as dag:
    explain = BashOperator(
        task_id="explain_timetables",
        bash_command="""
            echo "=== Custom Timetable Concepts ==="
            echo ""
            echo "Built-in schedules:"
            echo "  cron:      '0 6 * * *'      - Standard cron expressions"
            echo "  timedelta: timedelta(hours=2) - Fixed intervals"
            echo "  presets:   '@daily'           - Convenience aliases"
            echo ""
            echo "Custom timetable use cases:"
            echo "  - Business days only (skip weekends)"
            echo "  - Market trading hours (09:30-16:00 EST)"
            echo "  - Skip public holidays"
            echo "  - Bi-weekly payroll schedules"
            echo "  - Lunar calendar"
            echo "  - Custom fiscal calendar periods"
            echo ""
            echo "Implementation:"
            echo "  1. Subclass airflow.timetables.base.Timetable"
            echo "  2. Implement next_dagrun_info() and infer_manual_data_interval()"
            echo "  3. Register via AirflowPlugin.timetables"
            echo "  4. Use: DAG(timetable=MyTimetable())"
        """,
    )

    weekday_example = BashOperator(
        task_id="weekday_example",
        bash_command="""
            echo "=== Weekday-Only Schedule ==="
            echo "Cron approximation: '0 6 * * 1-5'"
            echo "  1-5 = Monday through Friday"
            echo ""
            DAY_OF_WEEK=$(date +%u)
            DAY_NAME=$(date +%A)
            echo "Today is $DAY_NAME (day $DAY_OF_WEEK)"
            if [ "$DAY_OF_WEEK" -le 5 ]; then
                echo "This IS a weekday -- pipeline would run"
            else
                echo "This is a WEEKEND -- pipeline would be skipped"
            fi
        """,
    )

    business_hours = BashOperator(
        task_id="business_hours_example",
        bash_command="""
            echo "=== Business Hours Schedule ==="
            echo "Cron: '0 9-16 * * 1-5'"
            echo "  Run hourly from 09:00-16:00, Monday-Friday"
            echo ""
            echo "With a custom timetable, you could implement:"
            echo "  - Skip public holidays via a holiday calendar"
            echo "  - Adjust for timezone-aware trading hours"
            echo "  - Handle market half-days"
            echo "  - Dynamic schedules from external APIs"
        """,
    )

    explain >> weekday_example >> business_hours
