# Dynamic Tasks & Branching

Runtime fan-out with `.expand()` and conditional paths with `@task.branch`

<div class="grid grid-cols-2 gap-4">
<div>

### Dynamic mapping

```python
@task
def generate_files() -> list[str]:
    return ["file_01.csv", "file_02.csv",
            "file_03.csv", "file_04.csv"]

@task
def process_file(filename: str):
    return {"file": filename, "rows": 100}

files = generate_files()
# one task instance per file
processed = process_file.expand(
    filename=files
)
```

<span class="text-sm opacity-60">DAG 010</span>

</div>
<div>

### Branching

```python
@task.branch
def choose_branch() -> str:
    return random.choice([
        "fast_path",
        "slow_path",
        "skip_path",
    ])

branch >> [fast, slow, skip] >> join

# join uses trigger_rule:
# "none_failed_min_one_success"
```

<span class="text-sm opacity-60">DAG 006</span>

</div>
</div>
