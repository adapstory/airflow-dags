from __future__ import annotations

import sys
from collections.abc import Mapping
from types import ModuleType
from typing import Any


def _test_literal(value: str) -> str:
    return value


if "airflow.sdk" not in sys.modules:
    airflow_module = ModuleType("airflow")
    airflow_sdk_module = ModuleType("airflow.sdk")
    airflow_sdk_module.__dict__["literal"] = _test_literal
    sys.modules["airflow"] = airflow_module
    sys.modules["airflow.sdk"] = airflow_sdk_module

from dags.serp_benchmark_catalog_materializer import (  # noqa: E402
    materialize_benchmark_catalog_receipt,
)
from dags.serp_benchmark_catalog_workload import (  # noqa: E402
    BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    benchmark_catalog_acquisition_container_security_context,
    benchmark_catalog_acquisition_env_vars,
    benchmark_catalog_acquisition_pod_security_context,
    benchmark_catalog_acquisition_web_identity_volume_mounts,
    benchmark_catalog_acquisition_web_identity_volumes,
)
from dags.serp_eval_contracts import MANDATORY_SERP_BENCHMARK_SUITES  # noqa: E402

# This unit module deliberately imports the catalog helper without the full
# Airflow distribution. Remove the temporary import-only modules so the DAG
# import-contract tests can install their own isolated Airflow/Kubernetes stubs.
sys.modules.pop("dags.serp_evidence_workload_identity", None)
sys.modules.pop("airflow.sdk", None)
sys.modules.pop("airflow", None)


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
            "catalogStatus": "ready",
            "blockingSuiteIds": [],
            "objectLockMode": "COMPLIANCE",
            "suiteSummary": [
                {
                    "distributionRule": "internal-only-no-redistribution",
                    "executionStatus": "ready",
                    "rightsStatus": "rights-unverified",
                    "suiteId": suite_id,
                }
                for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
            ],
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
                "blockingSuiteIds": [],
                "catalogStatus": "ready",
                "objectLockMode": "COMPLIANCE",
                "suiteSummary": [
                    {
                        "distributionRule": "internal-only-no-redistribution",
                        "executionStatus": "ready",
                        "rightsStatus": "rights-unverified",
                        "suiteId": suite_id,
                    }
                    for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
                ],
            },
            "contractVersion": "serp-benchmark-catalog-materializer/v3",
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
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
        "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
        "ADAPSTORY_SERP_SOURCE_PROXY_URL",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    ]
    literal_env = {env_var.name: env_var.value for env_var in env_vars if env_var.value is not None}
    assert literal_env["ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT"] == (
        "http://minio.env-prod.svc:9000"
    )
    assert literal_env["ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE"] == "true"
    assert literal_env["ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION"] == "us-east-1"
    assert literal_env["ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE"] == (
        "/var/run/secrets/adapstory/minio-web-identity/token"
    )
    assert literal_env["ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS"] == '"900"'
    assert literal_env["ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS"] == '"365"'
    assert literal_env["ADAPSTORY_SERP_SOURCE_PROXY_URL"] == (
        "http://forward-proxy.forward-proxy.svc:3128"
    )
    assert literal_env["HTTP_PROXY"] == "http://forward-proxy.forward-proxy.svc:3128"
    assert literal_env["HTTPS_PROXY"] == "http://forward-proxy.forward-proxy.svc:3128"
    assert ".svc.cluster.local" in literal_env["NO_PROXY"]
    assert all(env_var.value_from is None for env_var in env_vars)
    assert benchmark_catalog_acquisition_web_identity_volumes()[0].to_dict() == {
        "aws_elastic_block_store": None,
        "azure_disk": None,
        "azure_file": None,
        "cephfs": None,
        "cinder": None,
        "config_map": None,
        "csi": None,
        "downward_api": None,
        "empty_dir": None,
        "ephemeral": None,
        "fc": None,
        "flex_volume": None,
        "flocker": None,
        "gce_persistent_disk": None,
        "git_repo": None,
        "glusterfs": None,
        "host_path": None,
        "image": None,
        "iscsi": None,
        "name": "minio-web-identity-token",
        "nfs": None,
        "persistent_volume_claim": None,
        "photon_persistent_disk": None,
        "portworx_volume": None,
        "projected": {
            "default_mode": None,
            "sources": [
                {
                    "cluster_trust_bundle": None,
                    "config_map": None,
                    "downward_api": None,
                    "pod_certificate": None,
                    "secret": None,
                    "service_account_token": {
                        "audience": "minio",
                        "expiration_seconds": 900,
                        "path": "token",
                    },
                }
            ],
        },
        "quobyte": None,
        "rbd": None,
        "scale_io": None,
        "secret": None,
        "storageos": None,
        "vsphere_volume": None,
    }
    assert benchmark_catalog_acquisition_web_identity_volume_mounts()[0].to_dict() == {
        "mount_path": "/var/run/secrets/adapstory/minio-web-identity",
        "mount_propagation": None,
        "name": "minio-web-identity-token",
        "read_only": True,
        "recursive_read_only": None,
        "sub_path": None,
        "sub_path_expr": None,
    }
    assert BENCHMARK_CATALOG_ACQUISITION_RESOURCES.to_dict() == {
        "claims": None,
        "limits": {"cpu": "1000m", "memory": "3Gi"},
        "requests": {"cpu": "500m", "memory": "1Gi"},
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
