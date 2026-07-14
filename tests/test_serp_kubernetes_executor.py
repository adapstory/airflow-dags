from __future__ import annotations

from dags.serp_kubernetes_executor import task_secret_executor_config


def test_task_secret_executor_config_projects_only_the_requested_secret() -> None:
    config = task_secret_executor_config(
        name="ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN",
        secret_name="airflow-serp-github-status",
        secret_key="token",
    )

    pod = config["pod_override"]
    assert pod.spec is not None
    assert pod.spec.automount_service_account_token is False
    assert pod.spec.service_account_name is None
    assert pod.spec.containers is not None
    env = pod.spec.containers[0].env
    assert env is not None
    assert len(env) == 1
    assert env[0].name == "ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN"
    assert env[0].value_from is not None
    assert env[0].value_from.secret_key_ref is not None
    assert env[0].value_from.secret_key_ref.name == "airflow-serp-github-status"
    assert env[0].value_from.secret_key_ref.key == "token"
