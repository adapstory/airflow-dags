from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "dags" / "serp_public_docs_context_benchmark.py"


def test_context_benchmark_dag_is_scheduled_in_cluster_and_never_dispatches_to_github_actions() -> (
    None
):
    source = DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DAG_FILE))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    dag_call = next(
        call
        for call in calls
        if isinstance(call.func, ast.Name)
        and call.func.id == "DAG"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and call.args[0].value == "serp_public_docs_context_benchmark"
    )
    keywords = {keyword.arg: keyword.value for keyword in dag_call.keywords}

    assert isinstance(keywords["schedule"], ast.Constant)
    assert keywords["schedule"].value == "15 3 * * *"
    assert isinstance(keywords["is_paused_upon_creation"], ast.Constant)
    assert keywords["is_paused_upon_creation"].value is False
    assert "workflow_dispatch" not in source
    assert "COMPETITOR_BENCHMARK_URL" not in source
    assert [
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "PythonOperator"
        for keyword in node.keywords
        if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant)
    ] == [
        "write_context_benchmark_plan",
        "execute_context_benchmark",
        "submit_context_benchmark_bc21_runs",
        "publish_context_benchmark_github_status",
        "enforce_context_benchmark_gate",
    ]
