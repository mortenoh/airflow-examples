"""DAG integrity tests.

Validates that all DAGs in the dags/ directory can be imported
without errors, have at least one task, contain no cycles,
and follow the naming convention.
"""

import os
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    """Load all DAGs from the dags directory."""
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_all_dags_importable(dagbag: DagBag) -> None:
    """All DAG files should import without errors."""
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"


def test_all_dags_have_tasks(dagbag: DagBag) -> None:
    """Every DAG should have at least one task."""
    for dag_id, dag in dagbag.dags.items():
        assert len(dag.tasks) > 0, f"DAG '{dag_id}' has no tasks"


def test_no_dag_cycles(dagbag: DagBag) -> None:
    """No DAG should contain circular dependencies."""
    for dag_id, dag in dagbag.dags.items():
        # DagBag validates this during import, but check explicitly
        result = dag.check_cycle()
        assert not result, f"DAG '{dag_id}' contains a cycle: {result}"


def test_dag_ids_match_filenames(dagbag: DagBag) -> None:
    """DAG IDs should follow the numbered naming convention.

    Each DAG ID must start with the same number prefix (e.g., "14_", "55_")
    as one of the files in dags/. Files can define multiple DAGs with
    suffixed IDs (e.g., 14_assets.py -> 14_assets_producer, 14_assets_consumer).
    """
    dag_files = sorted(Path(DAGS_DIR).glob("*.py"))
    file_stems = {f.stem for f in dag_files}

    for dag_id in dagbag.dag_ids:
        # Extract the numeric prefix (e.g., "14" from "14_assets_producer")
        prefix = dag_id.split("_", 1)[0]
        # Check if any file stem starts with the same prefix
        has_match = dag_id in file_stems or any(stem.startswith(prefix + "_") for stem in file_stems)
        assert has_match, f"DAG '{dag_id}' does not match any file in dags/"


def test_minimum_dag_count(dagbag: DagBag) -> None:
    """There should be at least 107 DAGs (some files define multiple)."""
    assert len(dagbag.dags) >= 107, f"Expected >= 107 DAGs, got {len(dagbag.dags)}"
