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


def test_context_benchmark_scopes_github_status_token_to_its_single_task() -> None:
    source = DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DAG_FILE))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    config = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "GITHUB_STATUS_EXECUTOR_CONFIG"
            for target in node.targets
        )
    )
    assert isinstance(config.value, ast.Call)
    assert isinstance(config.value.func, ast.Name)
    assert config.value.func.id == "task_secret_executor_config"
    config_keywords = {keyword.arg: keyword.value for keyword in config.value.keywords}
    assert isinstance(config_keywords["name"], ast.Constant)
    assert config_keywords["name"].value == "ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN"
    assert isinstance(config_keywords["secret_name"], ast.Constant)
    assert config_keywords["secret_name"].value == "airflow-serp-github-status"
    assert isinstance(config_keywords["secret_key"], ast.Constant)
    assert config_keywords["secret_key"].value == "token"

    publish_call = next(
        call
        for call in calls
        if isinstance(call.func, ast.Name)
        and call.func.id == "PythonOperator"
        and any(
            keyword.arg == "task_id"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "publish_context_benchmark_github_status"
            for keyword in call.keywords
        )
    )
    publish_keywords = {keyword.arg: keyword.value for keyword in publish_call.keywords}
    assert isinstance(publish_keywords["executor_config"], ast.Name)
    assert publish_keywords["executor_config"].id == "GITHUB_STATUS_EXECUTOR_CONFIG"

    for call in calls:
        if not (isinstance(call.func, ast.Name) and call.func.id == "PythonOperator"):
            continue
        keywords = {keyword.arg: keyword.value for keyword in call.keywords}
        task_id = keywords.get("task_id")
        if not isinstance(task_id, ast.Constant) or not isinstance(task_id.value, str):
            continue
        if task_id.value != "publish_context_benchmark_github_status":
            assert "executor_config" not in keywords
