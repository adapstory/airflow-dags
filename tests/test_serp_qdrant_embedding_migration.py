from __future__ import annotations

import ast
import importlib
import json
import os
import subprocess
import sys
import types
from collections.abc import Callable, Mapping
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from urllib.parse import quote

import pytest

import dags.serp_qdrant_embedding_migration_remote_runner as remote_runner


def test_validate_qdrant_embedding_migration_conf_rejects_versioned_source_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(monkeypatch)
    conf = _valid_conf()
    conf["source_collection"] = "serp_vectors_bge_m3_1024_v20260101"

    with pytest.raises(ValueError, match="physical source collection"):
        module.validate_qdrant_embedding_migration_conf(conf)


def test_validate_conf_rejects_operation_outside_backfill_evidence_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(monkeypatch)
    conf = _valid_conf()
    conf["operation_id"] = "unrelated-operation"

    with pytest.raises(ValueError, match="qdrant-embedding-backfill prefix"):
        module.validate_qdrant_embedding_migration_conf(conf)


@pytest.mark.parametrize(
    "profile_version",
    ("bge-m3-1024", "bge-m3-1024@", "@2026.07.2", "bge-m3-1024@@2026.07.2"),
)
def test_validate_conf_rejects_noncanonical_embedding_profile_version(
    monkeypatch: pytest.MonkeyPatch,
    profile_version: str,
) -> None:
    module = _load_migration_module(monkeypatch)
    conf = _valid_conf()
    conf["embedding_profile_version"] = profile_version

    with pytest.raises(ValueError, match="profile@version"):
        module.validate_qdrant_embedding_migration_conf(conf)


def test_validate_plan_derives_stable_operation_identity_from_airflow_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(monkeypatch)
    conf = _valid_conf()
    for field_name in ("generated_at", "correlation_id", "operation_id"):
        conf.pop(field_name, None)
    dag_run = SimpleNamespace(
        conf=conf,
        run_id="manual__2026-08-02T20:00:00+00:00",
        logical_date=module.datetime(2026, 8, 2, 20, 0, tzinfo=module.UTC),
    )

    first = module.validate_qdrant_embedding_migration_plan(dag_run=dag_run)
    retried = module.validate_qdrant_embedding_migration_plan(dag_run=dag_run)

    assert first == retried
    assert first["generated_at"] == "2026-08-02T20:00:00Z"
    run_identity = sha256(
        b"serp_qdrant_embedding_migration\0manual__2026-08-02T20:00:00+00:00"
    ).hexdigest()[:24]
    assert first["correlation_id"] == f"airflow:serp_qdrant_embedding_migration:{run_identity}"
    assert first["operation_id"] == f"qdrant-embedding-backfill-{run_identity}"


@pytest.mark.parametrize(
    "forbidden_field",
    ("minimum_target_points", "target_ready_threshold", "fail_when_not_ready"),
)
def test_validate_qdrant_embedding_migration_conf_rejects_threshold_shortcuts(
    monkeypatch: pytest.MonkeyPatch,
    forbidden_field: str,
) -> None:
    module = _load_migration_module(monkeypatch)
    conf = _valid_conf()
    conf[forbidden_field] = 1025 if forbidden_field != "fail_when_not_ready" else False

    with pytest.raises(ValueError, match="threshold shortcut"):
        module.validate_qdrant_embedding_migration_conf(conf)


def test_dispatch_qdrant_embedding_backfill_handoff_preserves_canonical_s3_receipt_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(monkeypatch)
    plan = module.validate_qdrant_embedding_migration_conf(_valid_conf())
    plan_handle = module.write_qdrant_embedding_migration_plan(
        plan,
        snapshot_writer=_snapshot_writer(),
    )

    cli_spec = module.dispatch_qdrant_embedding_backfill_handoff_from_snapshot(plan_handle)

    receipt_path = plan["artifact_paths"]["backfill_receipt"]
    assert cli_spec["status"] == "ready_for_pipeline_cli_runner"
    assert cli_spec["input_paths"] == [plan["artifact_paths"]["backfill_plan"]]
    assert cli_spec["receipt_uri"] == receipt_path
    assert cli_spec["plan_evidence"] == plan_handle["evidence"]
    receipt_index = cli_spec["argv"].index("--receipt-uri") + 1
    assert cli_spec["argv"][receipt_index] == receipt_path
    assert cli_spec["argv"][:3] == [
        "python",
        "-m",
        "adapstory_serp_pipeline.migration.qdrant_embedding_backfill_cli",
    ]


def test_snapshot_final_receipt_writes_immutable_evidence_from_canonical_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(monkeypatch)
    plan = module.validate_qdrant_embedding_migration_conf(_valid_conf())
    plan_handle = module.write_qdrant_embedding_migration_plan(
        plan,
        snapshot_writer=_snapshot_writer(),
    )
    receipt_payload = {
        "receipt_version": "qdrant-embedding-backfill-receipt/v1",
        "source_collection": plan["source_collection"],
        "target_collection": plan["target_collection"],
        "embedding_profile_version": plan["embedding_profile_version"],
        "source_snapshot_point_count": 482910,
        "processed_point_count": 482910,
        "next_offset": None,
        "target_point_count": 482910,
        "completed": True,
    }
    receipt_bytes = _canonical_json(receipt_payload).encode("utf-8")
    receipt_bucket, receipt_key = plan["artifact_paths"]["backfill_receipt"].removeprefix("s3://").split(
        "/", 1
    )
    storage = {(receipt_bucket, receipt_key): receipt_bytes}
    monkeypatch.setattr(
        module,
        "_operation_prefix_read_s3_client",
        lambda *, artifact_uris: _FakeS3Client(storage),
    )

    receipt_handle = module.snapshot_qdrant_embedding_backfill_receipt_from_snapshot(
        plan_handle,
        snapshot_writer=_snapshot_writer(),
    )

    assert receipt_handle["artifactType"] == "qdrant_embedding_backfill_receipt"
    assert receipt_handle["evidence"]["s3Uri"] == plan["artifact_paths"]["backfill_receipt_evidence"]
    assert receipt_handle["summary"] == {
        "completed": True,
        "processedPointCount": 482910,
        "targetPointCount": 482910,
    }


def test_remote_runner_passes_canonical_s3_receipt_uri_to_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_payload = {
        "artifact_paths": {
            "backfill_plan": "s3://airflow-serp-evidence/serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-plan.json",
            "backfill_receipt": "s3://airflow-serp-evidence/serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-receipt.json",
            "backfill_receipt_evidence": "s3://airflow-serp-evidence/serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-receipt-evidence.json",
        },
        "dag_id": "serp_qdrant_embedding_migration",
        "embedding_profile_version": "bge-m3-1024@2026.07.2",
        "operation_id": "qdrant-embedding-backfill-001",
        "source_collection": "serp_vectors_prod",
        "target_collection": "serp_vectors_bge_m3_1024_v20260716",
    }
    plan_bytes = _canonical_json(plan_payload).encode("utf-8")
    plan_evidence = {
        "s3Uri": plan_payload["artifact_paths"]["backfill_plan"],
        "sha256": "sha256:" + sha256(plan_bytes).hexdigest(),
        "versionId": "plan-version-7",
    }
    receipt_payload = {
        "completed": True,
        "embedding_profile_version": "bge-m3-1024@2026.07.2",
        "processed_point_count": 482910,
        "receipt_version": "qdrant-embedding-backfill-receipt/v1",
        "source_collection": "serp_vectors_prod",
        "source_snapshot_point_count": 482910,
        "target_collection": "serp_vectors_bge_m3_1024_v20260716",
        "target_point_count": 482910,
    }
    receipt_bytes = _canonical_json(receipt_payload).encode("utf-8")
    receipt_path = plan_payload["artifact_paths"]["backfill_receipt"]
    storage = {
        ("airflow-serp-evidence", "serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-plan.json"): plan_bytes,
        ("airflow-serp-evidence", "serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-receipt.json"): receipt_bytes,
    }
    spec = {
        "argv": [
            "python",
            "-m",
            "adapstory_serp_pipeline.migration.qdrant_embedding_backfill_cli",
            "--source-collection",
            "serp_vectors_prod",
            "--target-collection",
            "serp_vectors_bge_m3_1024_v20260716",
            "--receipt-uri",
            receipt_path,
            "--correlation-id",
            "airflow:dag:run-qdrant-backfill",
            "--route-environment-prefix",
            "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING",
            "--batch-size",
            "128",
        ],
        "contract_version": remote_runner.PIPELINE_CLI_CONTRACT_VERSION,
        "dag_id": "serp_qdrant_embedding_migration",
        "input_paths": [plan_evidence["s3Uri"]],
        "operation_id": "qdrant-embedding-backfill-001",
        "plan_evidence": plan_evidence,
        "receipt_uri": receipt_path,
        "status": "ready_for_pipeline_cli_runner",
        "task_id": "qdrant_embedding_backfill_pipeline",
    }
    encoded_spec = quote(json.dumps(spec, separators=(",", ":"), sort_keys=True), safe="")
    monkeypatch.setenv(remote_runner.PIPELINE_CLI_SPEC_ENV, encoded_spec)
    monkeypatch.setattr(
        remote_runner,
        "_operation_prefix_read_s3_client",
        lambda *, artifact_uris: _FakeS3Client(storage),
    )

    observed: dict[str, object] = {}

    def fake_run(argv: list[str], **_kwargs: object) -> SimpleNamespace:
        observed["receipt_uri"] = argv[argv.index("--receipt-uri") + 1]
        return SimpleNamespace(returncode=0, stderr="", stdout=receipt_bytes.decode("utf-8"))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert remote_runner.main() == 0
    assert observed["receipt_uri"] == receipt_path
    assert os.environ[remote_runner.PIPELINE_CLI_SPEC_ENV] == encoded_spec


def test_remote_runner_rejects_noncanonical_receipt_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_payload = {
        "artifact_paths": {
            "backfill_plan": "s3://airflow-serp-evidence/serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-plan.json",
            "backfill_receipt": "s3://airflow-serp-evidence/serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-receipt.json",
            "backfill_receipt_evidence": "s3://airflow-serp-evidence/serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-receipt-evidence.json",
        },
        "dag_id": "serp_qdrant_embedding_migration",
        "embedding_profile_version": "bge-m3-1024@2026.07.2",
        "operation_id": "qdrant-embedding-backfill-001",
        "source_collection": "serp_vectors_prod",
        "target_collection": "serp_vectors_bge_m3_1024_v20260716",
    }
    plan_bytes = _canonical_json(plan_payload).encode("utf-8")
    plan_evidence = {
        "s3Uri": plan_payload["artifact_paths"]["backfill_plan"],
        "sha256": "sha256:" + sha256(plan_bytes).hexdigest(),
        "versionId": "plan-version-7",
    }
    storage = {
        ("airflow-serp-evidence", "serp-evals/qdrant-embedding-backfill-001/qdrant-embedding-backfill-plan.json"): plan_bytes,
    }
    monkeypatch.setenv(
        remote_runner.PIPELINE_CLI_SPEC_ENV,
        quote(
            json.dumps(
                {
                    "argv": [
                        "python",
                        "-m",
                        "adapstory_serp_pipeline.migration.qdrant_embedding_backfill_cli",
                        "--receipt-uri",
                        "/tmp/not-allowed.json",
                    ],
                    "contract_version": remote_runner.PIPELINE_CLI_CONTRACT_VERSION,
                    "dag_id": "serp_qdrant_embedding_migration",
                    "input_paths": [plan_evidence["s3Uri"]],
                    "operation_id": "qdrant-embedding-backfill-001",
                    "plan_evidence": plan_evidence,
                    "receipt_uri": plan_payload["artifact_paths"]["backfill_receipt"],
                    "status": "ready_for_pipeline_cli_runner",
                    "task_id": "qdrant_embedding_backfill_pipeline",
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            safe="",
        ),
    )
    monkeypatch.setattr(
        remote_runner,
        "_operation_prefix_read_s3_client",
        lambda *, artifact_uris: _FakeS3Client(storage),
    )

    with pytest.raises(ValueError, match="canonical receipt URI"):
        remote_runner.main()


def test_dag_source_uses_single_runner_with_kubernetes_reliability_contracts() -> None:
    source = (
        Path(__file__).resolve().parent.parent / "dags" / "serp_qdrant_embedding_migration.py"
    ).read_text(encoding="utf-8")

    assert 'cmds=["python", "-m", "dags.serp_qdrant_embedding_migration_remote_runner"]' in source
    assert "max_active_runs=1" in source
    assert "reattach_on_restart=True" in source
    assert 'on_kill_action="keep_pod"' in source
    assert 'schedule=None' in source
    assert "TriggerDagRunOperator" not in source


def test_evidence_tasks_use_dedicated_minio_identity_while_kpo_keeps_launcher_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_migration_module(monkeypatch)

    expected_evidence_config = {
        "minio_identity": module.QDRANT_EMBEDDING_BACKFILL_SERVICE_ACCOUNT,
        "labels": module.QDRANT_EMBEDDING_BACKFILL_LABELS,
    }
    for operator in (
        module.validate_plan,
        module.write_plan,
        module.dispatch_handoff,
        module.snapshot_receipt,
    ):
        assert operator.kwargs["executor_config"] == expected_evidence_config
    run_config = module.run_qdrant_embedding_backfill.kwargs["executor_config"]
    assert run_config == {"pod_launcher": True}


def _load_migration_module(monkeypatch: pytest.MonkeyPatch) -> Any:
    module_name = "dags.serp_qdrant_embedding_migration"
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    monkeypatch.delitem(sys.modules, "dags.serp_evidence_workload_identity", raising=False)

    class FakeDAG:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            self.kwargs = kwargs

    class FakeOperator:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: object) -> object:
            return other

        def __rrshift__(self, _other: object) -> FakeOperator:
            return self

    class FakeConf:
        @staticmethod
        def get(section: str, key: str) -> str:
            values = {
                ("kubernetes_executor", "namespace"): "airflow",
                ("kubernetes_executor", "worker_container_repository"): "harbor/airflow",
                ("kubernetes_executor", "worker_container_tag"): "test",
            }
            return values[(section, key)]

    class FakeKubernetesModel:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            self.kwargs = kwargs

    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_ROOT": "s3://airflow-serp-evidence/serp-evals",
        "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS": "30",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "https://minio.example.test",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE": "true",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-west-1",
        "ADAPSTORY_BC10_GATEWAY_URL": "https://bc10.example.test",
        "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_BUDGET_POLICY_ID": "budget-policy",
        "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_MODEL_VERSION_ID": "model-version",
        "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_PROMPT_TEMPLATE_VERSION": "prompt-version",
        "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_ROUTE_ID": "route-id",
        "ADAPSTORY_SERP_QDRANT_TIMEOUT_SECONDS": "15",
        "ADAPSTORY_SERP_QDRANT_URL": "https://qdrant.example.test",
    }.items():
        monkeypatch.setenv(name, value)

    modules = {
        "airflow": types.ModuleType("airflow"),
        "airflow.configuration": types.ModuleType("airflow.configuration"),
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.cncf": types.ModuleType("airflow.providers.cncf"),
        "airflow.providers.cncf.kubernetes": types.ModuleType("airflow.providers.cncf.kubernetes"),
        "airflow.providers.cncf.kubernetes.operators": types.ModuleType(
            "airflow.providers.cncf.kubernetes.operators"
        ),
        "airflow.providers.cncf.kubernetes.operators.pod": types.ModuleType(
            "airflow.providers.cncf.kubernetes.operators.pod"
        ),
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": types.ModuleType(
            "airflow.providers.standard.operators"
        ),
        "airflow.providers.standard.operators.python": types.ModuleType(
            "airflow.providers.standard.operators.python"
        ),
        "airflow.sdk": types.ModuleType("airflow.sdk"),
        "kubernetes": types.ModuleType("kubernetes"),
        "kubernetes.client": types.ModuleType("kubernetes.client"),
        "kubernetes.client.models": types.ModuleType("kubernetes.client.models"),
    }
    cast(Any, modules["airflow.configuration"]).conf = FakeConf()
    cast(Any, modules["airflow.providers.standard.operators.python"]).PythonOperator = FakeOperator
    cast(Any, modules["airflow.providers.cncf.kubernetes.operators.pod"]).KubernetesPodOperator = (
        FakeOperator
    )
    cast(Any, modules["airflow.sdk"]).DAG = FakeDAG
    for attr in (
        "V1Capabilities",
        "V1Container",
        "V1EnvVar",
        "V1EnvVarSource",
        "V1ProjectedVolumeSource",
        "V1ResourceRequirements",
        "V1SecretKeySelector",
        "V1SecurityContext",
        "V1ServiceAccountTokenProjection",
        "V1Volume",
        "V1VolumeMount",
        "V1VolumeProjection",
    ):
        setattr(cast(Any, modules["kubernetes.client.models"]), attr, FakeKubernetesModel)

    monkeypatch.setitem(
        sys.modules,
        "dags.serp_evidence_workload_identity",
        _fake_workload_identity_module(),
    )
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    return importlib.import_module(module_name)


def _fake_workload_identity_module() -> types.ModuleType:
    module = types.ModuleType("dags.serp_evidence_workload_identity")
    module.bc10_workload_env_vars = lambda: [SimpleNamespace(kwargs={"name": "ADAPSTORY_BC10_TOKEN_PATH", "value": "/token"})]
    module.bc10_workload_volume_mounts = lambda: [SimpleNamespace(kwargs={"name": "bc10"})]
    module.bc10_workload_volumes = lambda: [SimpleNamespace(kwargs={"name": "bc10"})]
    module.kubernetes_pod_launcher_executor_config = lambda: {"pod_launcher": True}
    module.minio_web_identity_executor_config = (
        lambda *, service_account_name, labels: {
            "minio_identity": service_account_name,
            "labels": labels,
        }
    )
    module.minio_web_identity_env_vars = lambda names: [
        SimpleNamespace(kwargs={"name": name, "value": "value"}) for name in names
    ]
    module.minio_web_identity_volume_mounts = lambda: [SimpleNamespace(kwargs={"name": "minio"})]
    module.minio_web_identity_volumes = lambda: [SimpleNamespace(kwargs={"name": "minio"})]
    module.operation_prefix_read_s3_client = lambda *, artifact_uris: _FakeS3Client({})
    return module


def _snapshot_writer() -> Callable[..., dict[str, str]]:
    def writer(
        artifact_path: str,
        *,
        artifact_type: str,
        operation_id: str,
        payload: Mapping[str, Any],
        s3_client: object | None = None,
    ) -> dict[str, str]:
        del s3_client
        payload_bytes = _canonical_json(payload).encode("utf-8")
        return {
            "artifactPath": artifact_path,
            "artifactSha256": sha256(payload_bytes).hexdigest(),
            "artifactType": artifact_type,
            "artifactVersionId": f"{operation_id}-{artifact_type}",
        }

    return writer


def _valid_conf() -> dict[str, object]:
    return {
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "batch_size": 128,
        "correlation_id": "airflow:dag:run-qdrant-backfill",
        "embedding_profile_version": "bge-m3-1024@2026.07.2",
        "generated_at": "2026-08-02T19:00:00Z",
        "route_environment_prefix": "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING",
        "source_collection": "serp_vectors_prod",
        "target_collection": "serp_vectors_bge_m3_1024_v20260716",
    }


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload


class _FakeS3Client:
    def __init__(self, storage: dict[tuple[str, str], bytes]) -> None:
        self.storage = storage

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, _Body]:
        return {"Body": _Body(self.storage[(Bucket, Key)])}
