# Getting Started

## Before We Start: Things You Might Not Know Yet

If you already know what Docker and Bash are, skip to [Airflow 101](#airflow-101-core-concepts).

### What is Bash?

Bash is the default command-line shell on most Linux and macOS systems. When you open a terminal
and type `ls` or `cd`, you are using Bash. It is just a way to run commands by typing them.

In Airflow, `BashOperator` lets you run a terminal command as a task. Think of it as:

```python
# Instead of this (in a terminal):
#   echo "Hello from Airflow"
#
# You write this (in a DAG):
task = BashOperator(
    task_id="say_hello",
    bash_command='echo "Hello from Airflow"',
)
```

You do not need to be a Bash expert. Most examples use simple one-liners like `echo`, `rm`, or
`python my_script.py`. If you can type a command in a terminal, you can use `BashOperator`.

### What is Docker?

Docker runs your code inside an isolated "container" -- a lightweight mini-computer with its own
operating system, files, and installed packages. Think of it like a virtual environment (`venv`)
but for *everything*, not just Python packages.

**Why does this project use Docker?** Airflow itself has several pieces that need to run together:
a web server, a scheduler, a database. Docker Compose starts all of these with a single command
(`make run`) so you do not have to install PostgreSQL, Redis, or Airflow system-wide.

```
Your machine
  +-- Docker ------------------------------------------+
  |                                                    |
  |  [PostgreSQL]  [Airflow Scheduler]  [Web Server]   |
  |       |              |                   |         |
  |       +--- all talk to each other -------+         |
  |                                                    |
  +----------------------------------------------------+
```

You interact with Docker through two commands:

- `docker compose up` -- start everything
- `docker compose down` -- stop everything

The `Makefile` wraps these for you, so `make run` is all you need. DAGs 21-32 also show how
to run *your own code* inside Docker containers as Airflow tasks -- but that is an advanced
topic you can skip on your first read.

### What is an Operator?

An **operator** is just a Python class that knows how to do one specific thing. Airflow ships
with many built-in operators so you do not have to write the plumbing yourself:

| Operator | What it does | Python equivalent |
|----------|-------------|-------------------|
| `BashOperator` | Runs a terminal command | `subprocess.run(["echo", "hello"])` |
| `PythonOperator` | Calls a Python function | `my_function()` |
| `DockerOperator` | Runs code in a Docker container | `docker run python:3.12 python script.py` |
| `HttpOperator` | Makes an HTTP request | `requests.get("https://api.example.com")` |
| `SQLExecuteQueryOperator` | Runs a SQL query | `cursor.execute("SELECT * FROM ...")` |
| `EmailOperator` | Sends an email | `smtplib.SMTP(...).send_message(...)` |

You can also skip operators entirely and use the `@task` decorator, which lets you write
plain Python functions and Airflow handles the rest:

```python
@task
def add_numbers(a, b):
    return a + b

result = add_numbers(3, 4)  # Airflow tracks this as a task
```

Most examples in this project use the `@task` style because it feels natural if you know Python.

---

## Airflow 101: Core Concepts

### What is Airflow?

Imagine you have a Python script that:

1. Downloads weather data from an API
2. Cleans and transforms it with pandas
3. Saves the results to a database

You could run it manually, or set up a cron job. But what happens when step 1 fails because
the API is down? What if step 2 takes longer than expected? How do you re-run just the failed
step without re-downloading everything?

Airflow solves these problems. You describe your workflow as a **DAG** (Directed Acyclic Graph) --
which is just a fancy name for "a list of tasks and the order they run in." Airflow then:

- **Runs tasks in the right order**, respecting dependencies
- **Retries failed tasks** automatically (with configurable delays)
- **Shows you what happened** in a web dashboard with logs, timing, and status
- **Schedules runs** on a timer, or triggers them when new data arrives
- **Scales** from a laptop to a cluster of machines

### Architecture (How the Pieces Fit Together)

When you run `make run`, Docker starts five processes that work together:

```
You (browser)
    |
    v
+------------------+
|   Web Server     |  The dashboard you see at localhost:8081
+------------------+
        |
+------------------+
|   Scheduler      |  Reads your DAG files, decides what to run and when
+------------------+
        |
+------------------+
|   Executor       |  Actually runs your tasks (on this machine, or on workers)
+------------------+
        |
+------------------+
|   PostgreSQL     |  A database that remembers task status, logs, and results
+------------------+
        |
+------------------+
|   Triggerer      |  Handles tasks that are waiting for something (optional)
+------------------+
```

You do not need to configure or think about these -- `make run` sets everything up. But it
helps to know they exist when you read log messages or debug a problem.

### Key Terminology

Do not memorize this table. Come back to it when you see a term you do not recognize.

| Term | Plain English |
|------|--------------|
| **DAG** | Your workflow -- a Python file that describes tasks and their order. "Directed Acyclic Graph" just means tasks flow in one direction and never loop back. |
| **Task** | A single step in your workflow. "Download the data", "Clean the data", "Save the data" are three tasks. |
| **Operator** | The *type* of a task. `BashOperator` means "this task runs a terminal command." `@task` means "this task runs a Python function." |
| **Sensor** | A task that waits. "Wait until the file exists", "wait until the API responds." Then it lets downstream tasks continue. |
| **XCom** | How tasks pass data to each other. Task A returns a dict, Task B receives it. Short for "cross-communication." |
| **DAG Run** | One execution of your workflow. If your DAG runs daily, each day creates a new DAG Run. |
| **Connection** | Saved credentials (database password, API key, SSH login) so you do not hardcode them in your DAGs. |
| **Variable** | A global key-value setting, like `{"env": "staging"}`, accessible from any DAG. |
| **Pool** | A limit on how many tasks can run at once. Useful when an API only allows 3 concurrent requests. |
| **Asset** | A data dependency. "Run this DAG whenever the weather data is updated." New in Airflow 3.x. |
| **Backfill** | Running your DAG for past dates it missed. Useful when you deploy a new daily pipeline mid-month. |
