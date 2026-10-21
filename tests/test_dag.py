import ast
from pathlib import Path

DAG = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "nhs_prescribing_monthly.py"


def test_dag_file_parses_and_declares_tasks():
    tree = ast.parse(DAG.read_text())
    task_ids = {
        kw.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for kw in node.keywords
        if kw.arg == "task_id" and isinstance(kw.value, ast.Constant)
    }
    assert task_ids == {"ingest", "bronze", "silver_with_quality_checks", "gold"}
