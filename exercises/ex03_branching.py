"""Exercise 03: Branching.

Goal: Add a BranchPythonOperator to route execution based on a condition.

Instructions:
1. Create a @task.branch function that checks the day of the week
2. If it's a weekday (Mon-Fri), route to the "weekday_report" task
3. If it's a weekend (Sat-Sun), route to the "weekend_summary" task
4. Both branches should join at a "done" task

Hint: Use `datetime.now().weekday()` -- returns 0 (Mon) through 6 (Sun).
Hint: The branch function should return the task_id string of the next task.
"""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task


# TODO: Create a @task.branch function called `choose_branch` that:
# - Gets the current day of week with datetime.now().weekday()
# - Returns "weekday_report" if weekday (0-4)
# - Returns "weekend_summary" if weekend (5-6)

# TODO: Create a @task function called `weekday_report` that prints
# "Running weekday report..."

# TODO: Create a @task function called `weekend_summary` that prints
# "Running weekend summary..."

# TODO: Create a DAG with dag_id="ex03_branching" and wire:
# choose_branch() >> [weekday_report(), weekend_summary()] >> done
# Use EmptyOperator for the "done" task with trigger_rule="none_failed_min_one_success"


# SOLUTION (uncomment to check your work):
# -----------------------------------------------------------------------
# @task.branch
# def choose_branch() -> str:
#     """Route based on day of week."""
#     day = datetime.now().weekday()
#     if day < 5:
#         return "weekday_report"
#     return "weekend_summary"
#
#
# @task
# def weekday_report() -> None:
#     """Run the weekday report."""
#     print("Running weekday report...")
#
#
# @task
# def weekend_summary() -> None:
#     """Run the weekend summary."""
#     print("Running weekend summary...")
#
#
# with DAG(
#     dag_id="ex03_branching",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise"],
# ) as dag:
#     branch = choose_branch()
#     wd = weekday_report()
#     we = weekend_summary()
#     done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
#     branch >> [wd, we] >> done
