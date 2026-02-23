"""DAG 79: Local DAG Testing with dag.test().

Demonstrates the ``dag.test()`` method for running DAGs without the
scheduler. Includes an ``if __name__ == "__main__"`` block so the file
can be run directly with ``python dags/79_dag_test_runner.py``.
Shows the local development workflow for quick iteration.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


@task
def generate_data() -> dict[str, object]:
    """Create sample data dict."""
    data = {
        "stations": ["oslo_01", "bergen_01", "tromso_01"],
        "temperatures": [5.2, 8.1, -3.4],
        "humidity": [72.0, 85.0, 65.0],
        "record_count": 3,
    }
    print(f"[{timestamp()}] Generated {data['record_count']} records")
    return data


@task
def process(data: dict[str, object]) -> dict[str, object]:
    """Transform data: compute averages and categorize."""
    temps_raw = data["temperatures"]
    temps: list[float] = temps_raw if isinstance(temps_raw, list) else []
    humidity_raw = data["humidity"]
    humidity: list[float] = humidity_raw if isinstance(humidity_raw, list) else []

    avg_temp = sum(temps) / len(temps) if temps else 0
    avg_hum = sum(humidity) / len(humidity) if humidity else 0

    result = {
        "avg_temperature": round(avg_temp, 1),
        "avg_humidity": round(avg_hum, 1),
        "category": "cold" if avg_temp < 5 else "mild" if avg_temp < 15 else "warm",
        "record_count": data["record_count"],
    }
    print(f"[{timestamp()}] Processed: avg_temp={result['avg_temperature']}, category={result['category']}")
    return result


@task
def validate_output(result: dict[str, object]) -> dict[str, object]:
    """Assert output meets expectations."""
    count = result.get("record_count", 0)
    assert isinstance(count, int) and count > 0, f"Expected positive row count, got {count}"

    avg_temp = result.get("avg_temperature")
    assert avg_temp is not None, "Missing avg_temperature"

    category = result.get("category")
    assert category in ("cold", "mild", "warm"), f"Invalid category: {category}"

    print(f"[{timestamp()}] Validation passed:")
    print(f"  record_count: {count} > 0")
    print(f"  avg_temperature: {avg_temp} (not null)")
    print(f"  category: {category} (valid)")
    return result


@task
def summary(result: dict[str, object]) -> None:
    """Print final results."""
    print(f"[{timestamp()}] === DAG Test Runner Complete ===")
    print(f"  Records: {result['record_count']}")
    print(f"  Avg temperature: {result['avg_temperature']}")
    print(f"  Avg humidity: {result['avg_humidity']}")
    print(f"  Category: {result['category']}")
    print("\n  This DAG can also be run with:")
    print("    python dags/79_dag_test_runner.py")


with DAG(
    dag_id="079_dag_test_runner",
    default_args=DEFAULT_ARGS,
    description="Local DAG testing with dag.test() and __main__ pattern",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "testing"],
) as dag:
    data = generate_data()
    processed = process(data=data)
    validated = validate_output(result=processed)
    summary(result=validated)


if __name__ == "__main__":
    dag.test()
