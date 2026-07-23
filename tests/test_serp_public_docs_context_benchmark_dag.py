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


def test_context_benchmark_assigns_least_privilege_workload_identity_to_each_external_task() -> (
    None
):
    source = DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DAG_FILE))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    configs = {
        target.id: node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    evidence_config = configs["CONTEXT_EVIDENCE_EXECUTOR_CONFIG"]
    assert isinstance(evidence_config, ast.Call)
    assert isinstance(evidence_config.func, ast.Name)
    assert evidence_config.func.id == "minio_web_identity_executor_config"

    bc21_config = configs["CONTEXT_BC21_EXECUTOR_CONFIG"]
    assert isinstance(bc21_config, ast.Call)
    assert isinstance(bc21_config.func, ast.Name)
    assert bc21_config.func.id == "bc21_authorized_minio_executor_config"

    github_config = configs["GITHUB_STATUS_EXECUTOR_CONFIG"]
    assert isinstance(github_config, ast.Call)
    assert isinstance(github_config.func, ast.Name)
    assert github_config.func.id == "minio_web_identity_executor_config"
    github_keywords = {keyword.arg: keyword.value for keyword in github_config.keywords}
    additional_env_vars = github_keywords["additional_env_vars"]
    assert isinstance(additional_env_vars, ast.List)
    assert len(additional_env_vars.elts) == 4
    secret_env_var = additional_env_vars.elts[0]
    assert isinstance(secret_env_var, ast.Call)
    assert isinstance(secret_env_var.func, ast.Name)
    assert secret_env_var.func.id == "task_secret_env_var"
    secret_keywords = {keyword.arg: keyword.value for keyword in secret_env_var.keywords}
    assert isinstance(secret_keywords["name"], ast.Constant)
    assert secret_keywords["name"].value == "ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN"
    assert isinstance(secret_keywords["secret_name"], ast.Constant)
    assert secret_keywords["secret_name"].value == "airflow-serp-github-status"
    assert isinstance(secret_keywords["secret_key"], ast.Constant)
    assert secret_keywords["secret_key"].value == "token"
    proxy_env = {}
    for env_var in additional_env_vars.elts[1:]:
        assert isinstance(env_var, ast.Call)
        assert isinstance(env_var.func, ast.Attribute)
        assert isinstance(env_var.func.value, ast.Name)
        assert env_var.func.value.id == "k8s"
        assert env_var.func.attr == "V1EnvVar"
        env_keywords = {keyword.arg: keyword.value for keyword in env_var.keywords}
        assert isinstance(env_keywords["name"], ast.Constant)
        assert isinstance(env_keywords["value"], ast.Constant)
        proxy_env[env_keywords["name"].value] = env_keywords["value"].value
    assert proxy_env == {
        "HTTP_PROXY": "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
        "HTTPS_PROXY": "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
        "NO_PROXY": "localhost,127.0.0.1,::1,.svc,.svc.cluster.local,cluster.local",
    }

    task_executor_configs = {}
    for call in calls:
        if not (isinstance(call.func, ast.Name) and call.func.id == "PythonOperator"):
            continue
        keywords = {keyword.arg: keyword.value for keyword in call.keywords}
        task_id = keywords.get("task_id")
        if not isinstance(task_id, ast.Constant) or not isinstance(task_id.value, str):
            continue
        executor_config = keywords.get("executor_config")
        task_executor_configs[task_id.value] = (
            executor_config.id if isinstance(executor_config, ast.Name) else None
        )

    assert task_executor_configs == {
        "write_context_benchmark_plan": "CONTEXT_EVIDENCE_EXECUTOR_CONFIG",
        "execute_context_benchmark": "CONTEXT_EVIDENCE_EXECUTOR_CONFIG",
        "submit_context_benchmark_bc21_runs": "CONTEXT_BC21_EXECUTOR_CONFIG",
        "publish_context_benchmark_github_status": "GITHUB_STATUS_EXECUTOR_CONFIG",
        "enforce_context_benchmark_gate": None,
    }

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
