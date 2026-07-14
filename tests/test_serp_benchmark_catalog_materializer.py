from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dags.serp_benchmark_catalog_materializer import (
    materialize_benchmark_catalog_receipt,
)
from dags.serp_benchmark_catalog_workload import (
    BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    benchmark_catalog_acquisition_container_security_context,
    benchmark_catalog_acquisition_env_vars,
    benchmark_catalog_acquisition_pod_security_context,
)


def test_catalog_materializer_seals_catalog_snapshot_in_immutable_receipt() -> None:
    plan: dict[str, Any] = {
        "artifact_paths": {
            "benchmark_catalog": "s3://airflow-serp-evidence/serp-evals/op/catalog.json",
            "benchmark_catalog_receipt": "s3://airflow-serp-evidence/serp-evals/op/catalog-receipt.json",
        },
        "dag_id": "serp_nightly_regression_suite",
        "operation_id": "op-1",
    }
    captured: dict[str, Any] = {}

    def materializer(observed_plan: Mapping[str, Any] | str) -> dict[str, Any]:
        assert observed_plan == plan
        return {
            "artifactPath": plan["artifact_paths"]["benchmark_catalog"],
            "artifactSha256": "a" * 64,
            "artifactVersionId": "catalog-v1",
            "catalogStatus": "blocked",
            "blockingSuiteIds": ["APIBench"],
            "objectLockMode": "COMPLIANCE",
        }

    def receipt_writer(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "artifactPath": kwargs["artifact_path"],
            "artifactVersionId": "receipt-v1",
            "objectLockMode": "COMPLIANCE",
        }

    receipt = materialize_benchmark_catalog_receipt(
        plan,
        catalog_materializer=materializer,
        receipt_writer=receipt_writer,
    )

    assert captured == {
        "artifact_path": plan["artifact_paths"]["benchmark_catalog_receipt"],
        "artifact_type": "benchmark_catalog_materialization_receipt",
        "operation_id": "op-1",
        "payload": {
            "catalogSnapshot": {
                "artifactPath": plan["artifact_paths"]["benchmark_catalog"],
                "artifactSha256": "a" * 64,
                "artifactVersionId": "catalog-v1",
                "blockingSuiteIds": ["APIBench"],
                "catalogStatus": "blocked",
                "objectLockMode": "COMPLIANCE",
            },
            "contractVersion": "serp-benchmark-catalog-materializer/v1",
            "dagId": "serp_nightly_regression_suite",
            "operationId": "op-1",
        },
    }
    assert receipt["artifactVersionId"] == "receipt-v1"


def test_catalog_acquisition_workload_has_minimal_proxy_and_evidence_contract(
    monkeypatch: Any,
) -> None:
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio.env-prod.svc:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE": "true",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS": "365",
        "ADAPSTORY_SERP_SOURCE_PROXY_URL": "http://forward-proxy.forward-proxy.svc:3128",
    }.items():
        monkeypatch.setenv(name, value)

    env_vars = benchmark_catalog_acquisition_env_vars()

    assert BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT == (
        "airflow-serp-benchmark-acquisition"
    )
    assert BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS == {
        "adapstory.com/serp-evidence-workload": "true",
        "adapstory.com/serp-network-profile": "benchmark-acquisition",
        "component": "worker",
        "release": "airflow",
        "tier": "airflow",
    }
    assert [env_var.name for env_var in env_vars] == [
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
        "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
        "ADAPSTORY_SERP_SOURCE_PROXY_URL",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY",
    ]
    literal_env = {env_var.name: env_var.value for env_var in env_vars if env_var.value is not None}
    assert literal_env["HTTP_PROXY"] == "http://forward-proxy.forward-proxy.svc:3128"
    assert literal_env["HTTPS_PROXY"] == "http://forward-proxy.forward-proxy.svc:3128"
    assert ".svc.cluster.local" in literal_env["NO_PROXY"]
    assert BENCHMARK_CATALOG_ACQUISITION_RESOURCES.to_dict() == {
        "claims": None,
        "limits": {"cpu": "500m", "memory": "1Gi"},
        "requests": {"cpu": "250m", "memory": "256Mi"},
    }
    assert BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS == 90
    assert benchmark_catalog_acquisition_pod_security_context().to_dict() == {
        "app_armor_profile": None,
        "fs_group": None,
        "fs_group_change_policy": None,
        "run_as_group": None,
        "run_as_non_root": True,
        "run_as_user": 50000,
        "se_linux_change_policy": None,
        "se_linux_options": None,
        "seccomp_profile": {"localhost_profile": None, "type": "RuntimeDefault"},
        "supplemental_groups": None,
        "supplemental_groups_policy": None,
        "sysctls": None,
        "windows_options": None,
    }
    assert benchmark_catalog_acquisition_container_security_context().to_dict() == {
        "allow_privilege_escalation": False,
        "app_armor_profile": None,
        "capabilities": {"add": None, "drop": ["ALL"]},
        "privileged": None,
        "proc_mount": None,
        "read_only_root_filesystem": None,
        "run_as_group": None,
        "run_as_non_root": True,
        "run_as_user": 50000,
        "se_linux_options": None,
        "seccomp_profile": None,
        "windows_options": None,
    }
