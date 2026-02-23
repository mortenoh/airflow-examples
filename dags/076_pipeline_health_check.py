"""DAG 76: Pipeline Health Monitoring.

A meta-DAG that checks pipeline health by verifying output artifacts:
file existence, freshness, and size. Sends a webhook alert if status
is degraded or critical. Shows meta-monitoring and file-based liveness
probes.
"""

import os
import time
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

HEALTH_DIR = str(OUTPUT_BASE / "health_check")
WEBHOOK_URL = "http://httpbin:8080/post"

# Expected pipeline outputs to monitor
EXPECTED_OUTPUTS = [
    {"path": f"{HEALTH_DIR}/pipeline_a_output.csv", "max_age_s": 3600, "min_size": 100},
    {"path": f"{HEALTH_DIR}/pipeline_b_output.parquet", "max_age_s": 3600, "min_size": 200},
    {"path": f"{HEALTH_DIR}/pipeline_c_output.json", "max_age_s": 3600, "min_size": 50},
]


@task
def setup_test_files() -> list[dict[str, object]]:
    """Create sample output files simulating previous pipeline runs."""
    os.makedirs(HEALTH_DIR, exist_ok=True)

    # Fresh, good-sized file
    path_a = f"{HEALTH_DIR}/pipeline_a_output.csv"
    with open(path_a, "w") as f:
        f.write("station,date,temp\n" + "oslo_01,2024-01-01,5.0\n" * 20)

    # Stale file (2 hours old)
    path_b = f"{HEALTH_DIR}/pipeline_b_output.parquet"
    with open(path_b, "wb") as f:
        f.write(b"x" * 300)  # Fake parquet content for size check
    old_time = time.time() - 7200
    os.utime(path_b, (old_time, old_time))

    # Missing file (pipeline_c_output.json not created)

    print(f"[{timestamp()}] Setup: 1 fresh file, 1 stale file, 1 missing file")
    return EXPECTED_OUTPUTS


@task
def check_file_existence(expected: list[dict[str, object]]) -> list[dict[str, object]]:
    """Verify expected output files exist at expected paths."""
    results: list[dict[str, object]] = []
    for spec in expected:
        path = str(spec["path"])
        exists = os.path.exists(path)
        results.append({"path": path, "exists": exists})
        status = "FOUND" if exists else "MISSING"
        print(f"[{timestamp()}] {status}: {os.path.basename(path)}")
    return results


@task
def check_file_freshness(expected: list[dict[str, object]]) -> list[dict[str, object]]:
    """Verify files were modified within expected time window."""
    results: list[dict[str, object]] = []
    for spec in expected:
        path = str(spec["path"])
        max_age = float(spec["max_age_s"])  # type: ignore[arg-type]
        if not os.path.exists(path):
            results.append({"path": path, "fresh": False, "age_s": -1, "reason": "missing"})
            continue

        age = time.time() - os.path.getmtime(path)
        fresh = age <= max_age
        results.append({"path": path, "fresh": fresh, "age_s": round(age), "reason": "" if fresh else "stale"})
        status = "FRESH" if fresh else "STALE"
        print(f"[{timestamp()}] {status}: {os.path.basename(path)} (age={age:.0f}s, max={max_age:.0f}s)")
    return results


@task
def check_file_sizes(expected: list[dict[str, object]]) -> list[dict[str, object]]:
    """Verify files are non-empty and within expected size bounds."""
    results: list[dict[str, object]] = []
    for spec in expected:
        path = str(spec["path"])
        min_size = int(spec["min_size"])  # type: ignore[arg-type]
        if not os.path.exists(path):
            results.append({"path": path, "size_ok": False, "size": 0, "reason": "missing"})
            continue

        size = os.path.getsize(path)
        ok = size >= min_size
        results.append({"path": path, "size_ok": ok, "size": size, "reason": "" if ok else "too_small"})
        status = "OK" if ok else "TOO SMALL"
        print(f"[{timestamp()}] Size {status}: {os.path.basename(path)} ({size} bytes, min={min_size})")
    return results


@task
def health_summary(
    existence: list[dict[str, object]],
    freshness: list[dict[str, object]],
    sizes: list[dict[str, object]],
) -> str:
    """Compute overall health status and print dashboard."""
    issues = 0
    total = len(existence) * 3  # 3 checks per file

    for e in existence:
        if not e["exists"]:
            issues += 1
    for f in freshness:
        if not f["fresh"]:
            issues += 1
    for s in sizes:
        if not s["size_ok"]:
            issues += 1

    if issues == 0:
        status = "HEALTHY"
    elif issues <= 2:
        status = "DEGRADED"
    else:
        status = "CRITICAL"

    print(f"[{timestamp()}] === Pipeline Health Dashboard ===")
    print(f"  Status: {status}")
    print(f"  Issues: {issues}/{total} checks failed")
    print(f"\n  {'File':<35s} {'Exists':>8s} {'Fresh':>8s} {'Size':>8s}")
    print(f"  {'-' * 63}")

    for i, e in enumerate(existence):
        basename = os.path.basename(str(e["path"]))
        exists = "YES" if e["exists"] else "NO"
        fresh = "YES" if freshness[i]["fresh"] else "NO"
        size_ok = "YES" if sizes[i]["size_ok"] else "NO"
        print(f"  {basename:<35s} {exists:>8s} {fresh:>8s} {size_ok:>8s}")

    return status


@task
def alert_if_unhealthy(status: str) -> None:
    """POST webhook to httpbin if status is degraded or critical."""
    import httpx

    if status == "HEALTHY":
        print(f"[{timestamp()}] All healthy -- no alert needed")
        return

    payload = {
        "event": "pipeline_health_alert",
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "message": f"Pipeline health is {status} -- attention required",
    }
    resp = httpx.post(WEBHOOK_URL, json=payload, timeout=10)
    print(f"[{timestamp()}] Health alert sent: HTTP {resp.status_code}")
    print(f"[{timestamp()}] Status: {status}")


with DAG(
    dag_id="076_pipeline_health_check",
    default_args=DEFAULT_ARGS,
    description="Meta-monitoring: file existence, freshness, size, and health alerting",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "alerting"],
) as dag:
    files = setup_test_files()
    existence = check_file_existence(expected=files)
    freshness = check_file_freshness(expected=files)
    sizes = check_file_sizes(expected=files)
    status = health_summary(existence=existence, freshness=freshness, sizes=sizes)
    alert = alert_if_unhealthy(status=status)

    files >> [existence, freshness, sizes] >> status >> alert  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {HEALTH_DIR} && echo 'Cleaned up'",
    )
    alert >> cleanup
