# Exercises

Hands-on exercises to practice building Airflow DAGs. Each file is a skeleton
with `TODO` markers -- fill in the blanks, then check your work against the
commented-out solution at the bottom.

## How to use

1. Copy an exercise file into the `dags/` directory
2. Fill in the `TODO` sections
3. Run `make run` and trigger your DAG from the Airflow UI
4. Check the task logs to see if your DAG works correctly
5. Compare your solution with the `SOLUTION` block at the bottom of the file

## Exercises

| # | File | Difficulty | Topic |
|---|------|------------|-------|
| 1 | `ex01_first_dag.py` | Beginner | PythonOperator tasks and dependencies |
| 2 | `ex02_taskflow.py` | Beginner | Convert PythonOperator to @task decorators |
| 3 | `ex03_branching.py` | Intermediate | Branch execution based on conditions |
| 4 | `ex04_api_pipeline.py` | Intermediate | Fetch API data, transform, write CSV |
| 5 | `ex05_quality_check.py` | Advanced | Quality validation with branching |

## Tips

- Start with Exercise 1 if you are new to Airflow
- Read the module docstring at the top of each file for context
- The `SOLUTION` block at the bottom is commented out -- uncomment it to test
- Compare the DAG graph in the Airflow UI with what you expected
