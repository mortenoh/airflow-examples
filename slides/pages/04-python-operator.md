# PythonOperator

Run Python callables with `op_args` / `op_kwargs` -- return values auto-push to XCom

```python {all|1-4|6-9|11-21}
def greet(name: str, greeting: str = "Hello") -> str:
    msg = f"{greeting}, {name}!"
    print(msg)
    return msg                          # auto-pushed to XCom

def compute_sum(a: int, b: int) -> int:
    result = a + b
    print(f"{a} + {b} = {result}")
    return result                       # auto-pushed to XCom

greet_task = PythonOperator(
    task_id="greet",
    python_callable=greet,
    op_args=["Airflow"],
    op_kwargs={"greeting": "Welcome"},
)

sum_task = PythonOperator(
    task_id="compute_sum",
    python_callable=compute_sum,
    op_args=[10, 32],
)
```

<span class="text-sm opacity-60">DAG 002 -- python_operator.py</span>
