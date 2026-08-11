from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from dags.serp_eval_contracts import write_immutable_evidence_snapshot
from dags.serp_evidence_workload_identity import (
    bc10_workload_env_vars,
    bc10_workload_volume_mounts,
    bc10_workload_volumes,
    kubernetes_pod_launcher_executor_config,
    minio_web_identity_env_vars,
    minio_web_identity_executor_config,
    minio_web_identity_volume_mounts,
    minio_web_identity_volumes,
)
from dags.serp_evidence_workload_identity import (
    operation_prefix_read_s3_client as _operation_prefix_read_s3_client,
)

DAG_ID = "serp_qdrant_embedding_migration"
PIPELINE_TASK_ID = "qdrant_embedding_backfill_pipeline"
PLAN_CONTRACT_VERSION = "serp-qdrant-embedding-migration-plan/v1"
PIPELINE_CLI_CONTRACT_VERSION = "serp-qdrant-embedding-migration-pipeline-bridge/v1"
TASK_ARTIFACT_HANDLE_SCHEMA = "serp-qdrant-embedding-migration-task-artifact-handle/v1"
PLAN_ARTIFACT_TYPE = "qdrant_embedding_backfill_plan"
RECEIPT_ARTIFACT_TYPE = "qdrant_embedding_backfill_receipt"
PIPELINE_CLI_SPEC_ENV = "ADAPSTORY_SERP_PIPELINE_CLI_SPEC_URLENCODED"
MAX_PLAN_BYTES = 64 * 1024
_VERSIONED_COLLECTION_RE = re.compile(r"[a-z0-9_]+_v\d{8,}\Z")
_SAFE_TOKEN_RE = re.compile(r"[A-Za-z0-9._:/-]+\Z")
_EMBEDDING_PROFILE_VERSION_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:/-]*@[A-Za-z0-9][A-Za-z0-9._:/-]*\Z"
)
_ROUTE_PREFIX_RE = re.compile(r"[A-Z0-9_]+\Z")
_PIPELINE_RUNNER_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_BC10_GATEWAY_URL",
    "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_BUDGET_POLICY_ID",
    "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_MODEL_VERSION_ID",
    "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_PROMPT_TEMPLATE_VERSION",
    "ADAPSTORY_BC10_SERP_CONTEXT_EMBEDDING_ROUTE_ID",
    "ADAPSTORY_SERP_EMBEDDING_BATCH_SIZE",
    "ADAPSTORY_SERP_EMBEDDING_DIMENSION",
    "ADAPSTORY_SERP_EMBEDDING_PROFILE_VERSION",
    "ADAPSTORY_SERP_QDRANT_TIMEOUT_SECONDS",
    "ADAPSTORY_SERP_QDRANT_URL",
)
QDRANT_EMBEDDING_BACKFILL_SERVICE_ACCOUNT = "airflow-serp-qdrant-embedding-backfill"
QDRANT_EMBEDDING_BACKFILL_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "qdrant-embedding-backfill",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
QDRANT_EMBEDDING_BACKFILL_EVIDENCE_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name=QDRANT_EMBEDDING_BACKFILL_SERVICE_ACCOUNT,
    labels=QDRANT_EMBEDDING_BACKFILL_LABELS,
)
QDRANT_EMBEDDING_BACKFILL_RUNTIME_VOLUMES = [
    *minio_web_identity_volumes(),
    *bc10_workload_volumes(),
]
QDRANT_EMBEDDING_BACKFILL_RUNTIME_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    *bc10_workload_volume_mounts(),
]
QDRANT_EMBEDDING_BACKFILL_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "2000m", "memory": "4Gi"},
)
_THRESHOLD_SHORTCUT_FIELDS = frozenset(
    {"minimum_target_points", "target_ready_threshold", "fail_when_not_ready"}
)


def current_airflow_runtime_image() -> str:
    repository = conf.get("kubernetes_executor", "worker_container_repository").strip()
    tag = conf.get("kubernetes_executor", "worker_container_tag").strip()
    if not repository or not tag:
        raise ValueError("KubernetesExecutor worker image configuration is required")
    return f"{repository}:{tag}"


def pipeline_runner_runtime_env_vars() -> list[k8s.V1EnvVar]:
    values = [
        *minio_web_identity_env_vars(_PIPELINE_RUNNER_ENV_NAMES),
        *bc10_workload_env_vars(),
    ]
    values.append(
        k8s.V1EnvVar(
            name="ADAPSTORY_SERP_QDRANT_API_KEY",
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name="qdrant-prod-write-client",
                    key="api-key",
                )
            ),
        )
    )
    return values


def pipeline_runner_env_vars(cli_spec_task_id: str) -> list[k8s.V1EnvVar]:
    values = pipeline_runner_runtime_env_vars()
    values.append(
        k8s.V1EnvVar(
            name=PIPELINE_CLI_SPEC_ENV,
            value=("{{ ti.xcom_pull(task_ids='" + cli_spec_task_id + "') | tojson | urlencode }}"),
        )
    )
    return values


def validate_qdrant_embedding_migration_plan(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    raw_conf = getattr(dag_run, "conf", None) or {}
    if not isinstance(raw_conf, Mapping):
        raise ValueError("dag run config must be an object")
    run_id = getattr(dag_run, "run_id", None)
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Airflow dag_run.run_id is required")
    logical_date = getattr(dag_run, "logical_date", None)
    if not isinstance(logical_date, datetime):
        raise ValueError("Airflow dag_run.logical_date is required")
    run_identity = sha256(f"{DAG_ID}\0{run_id}".encode()).hexdigest()
    conf_value = dict(raw_conf)
    conf_value.setdefault(
        "generated_at",
        logical_date.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )
    conf_value.setdefault("correlation_id", f"airflow:{DAG_ID}:{run_identity[:24]}")
    conf_value.setdefault("operation_id", f"qdrant-embedding-backfill-{run_identity[:24]}")
    return validate_qdrant_embedding_migration_conf(conf_value)


def validate_qdrant_embedding_migration_conf(conf_value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(conf_value, Mapping):
        raise ValueError("dag run config must be an object")
    unknown = set(conf_value) - {
        "artifact_root_path",
        "batch_size",
        "correlation_id",
        "embedding_profile_version",
        "generated_at",
        "operation_id",
        "route_environment_prefix",
        "source_collection",
        "target_collection",
        *_THRESHOLD_SHORTCUT_FIELDS,
    }
    if unknown:
        raise ValueError(f"unsupported qdrant embedding migration config fields: {sorted(unknown)}")
    shortcut_fields = [
        field_name
        for field_name in _THRESHOLD_SHORTCUT_FIELDS
        if conf_value.get(field_name) is not None
    ]
    if shortcut_fields:
        raise ValueError(
            "threshold shortcut fields are forbidden; use exact parity receipt gating instead"
        )
    generated_at = _required_datetime_string(
        conf_value.get("generated_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "generated_at",
    )
    artifact_root_path = _required_s3_uri(
        str(
            conf_value.get("artifact_root_path")
            or os.environ.get("ADAPSTORY_AIRFLOW_ARTIFACT_ROOT")
            or ""
        ),
        "artifact_root_path",
    ).rstrip("/")
    source_collection = _required_token(conf_value.get("source_collection"), "source_collection")
    target_collection = _required_token(conf_value.get("target_collection"), "target_collection")
    if _VERSIONED_COLLECTION_RE.fullmatch(source_collection):
        raise ValueError(
            "source_collection must stay a physical source collection, not a versioned target"
        )
    if source_collection == target_collection:
        raise ValueError("source_collection and target_collection must differ")
    if not _VERSIONED_COLLECTION_RE.fullmatch(target_collection):
        raise ValueError("target_collection must be a versioned immutable collection name")
    embedding_profile_version = _required_embedding_profile_version(
        conf_value.get("embedding_profile_version"),
        "embedding_profile_version",
    )
    route_environment_prefix = _required_route_prefix(
        conf_value.get("route_environment_prefix"),
        "route_environment_prefix",
    )
    correlation_id = _required_token(conf_value.get("correlation_id"), "correlation_id")
    batch_size = _required_positive_int(conf_value.get("batch_size"), "batch_size")
    operation_id = _required_token(
        conf_value.get("operation_id")
        or _operation_id(
            "qdrant-embedding-backfill",
            generated_at,
            source_collection,
            target_collection,
            embedding_profile_version,
        ),
        "operation_id",
    )
    if not operation_id.startswith("qdrant-embedding-backfill-"):
        raise ValueError("operation_id must use the qdrant-embedding-backfill prefix")
    artifact_paths = _artifact_paths(artifact_root_path, operation_id)
    plan = {
        "artifact_paths": artifact_paths,
        "artifact_root_path": artifact_root_path,
        "batch_size": batch_size,
        "contract_version": PLAN_CONTRACT_VERSION,
        "correlation_id": correlation_id,
        "dag_id": DAG_ID,
        "embedding_profile_version": embedding_profile_version,
        "generated_at": generated_at,
        "operation_id": operation_id,
        "receipt_uri": artifact_paths["backfill_receipt"],
        "route_environment_prefix": route_environment_prefix,
        "source_collection": source_collection,
        "status": "validated",
        "target_collection": target_collection,
        "task_id": PIPELINE_TASK_ID,
    }
    payload_bytes = _canonical_json(plan).encode("utf-8")
    if len(payload_bytes) > MAX_PLAN_BYTES:
        raise ValueError(
            "qdrant embedding migration plan exceeds the governed byte ceiling: "
            f"bytes={len(payload_bytes)} limit={MAX_PLAN_BYTES}"
        )
    return plan


def write_qdrant_embedding_migration_plan(
    plan_json: Mapping[str, Any] | str,
    *,
    snapshot_writer: Any = write_immutable_evidence_snapshot,
) -> dict[str, Any]:
    plan = _plan_payload(plan_json)
    written = snapshot_writer(
        plan["artifact_paths"]["backfill_plan"],
        artifact_type=PLAN_ARTIFACT_TYPE,
        operation_id=plan["operation_id"],
        payload=plan,
    )
    return _task_artifact_handle(
        written,
        payload=plan,
        summary={
            "batchSize": int(plan["batch_size"]),
            "sourceCollection": str(plan["source_collection"]),
            "targetCollection": str(plan["target_collection"]),
        },
    )


def dispatch_qdrant_embedding_backfill_handoff_from_snapshot(
    plan_handle: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _task_artifact_payload(plan_handle, PLAN_ARTIFACT_TYPE)
    receipt_path = plan["artifact_paths"]["backfill_receipt"]
    return {
        "argv": [
            "python",
            "-m",
            "adapstory_serp_pipeline.migration.qdrant_embedding_backfill_cli",
            "--source-collection",
            plan["source_collection"],
            "--target-collection",
            plan["target_collection"],
            "--receipt-uri",
            receipt_path,
            "--correlation-id",
            plan["correlation_id"],
            "--route-environment-prefix",
            plan["route_environment_prefix"],
            "--batch-size",
            str(plan["batch_size"]),
        ],
        "contract_version": PIPELINE_CLI_CONTRACT_VERSION,
        "dag_id": DAG_ID,
        "input_paths": [plan["artifact_paths"]["backfill_plan"]],
        "operation_id": plan["operation_id"],
        "plan_evidence": _task_artifact_evidence(plan_handle),
        "receipt_uri": receipt_path,
        "source_collection": plan["source_collection"],
        "status": "ready_for_pipeline_cli_runner",
        "target_collection": plan["target_collection"],
        "task_id": PIPELINE_TASK_ID,
    }


def snapshot_qdrant_embedding_backfill_receipt_from_snapshot(
    plan_handle: Mapping[str, Any] | str,
    *,
    snapshot_writer: Any = write_immutable_evidence_snapshot,
) -> dict[str, Any]:
    plan = _task_artifact_payload(plan_handle, PLAN_ARTIFACT_TYPE)
    receipt_path = plan["artifact_paths"]["backfill_receipt"]
    s3_client = _operation_prefix_read_s3_client(
        artifact_uris=(plan["artifact_paths"]["backfill_plan"], receipt_path)
    )
    receipt_payload = _read_json_artifact(receipt_path, s3_client=s3_client)
    if (
        _required_token(receipt_payload.get("source_collection"), "source_collection")
        != plan["source_collection"]
    ):
        raise ValueError("backfill receipt source_collection does not match the immutable plan")
    if (
        _required_token(receipt_payload.get("target_collection"), "target_collection")
        != plan["target_collection"]
    ):
        raise ValueError("backfill receipt target_collection does not match the immutable plan")
    if (
        _required_embedding_profile_version(
            receipt_payload.get("embedding_profile_version"),
            "embedding_profile_version",
        )
        != plan["embedding_profile_version"]
    ):
        raise ValueError(
            "backfill receipt embedding_profile_version does not match the immutable plan"
        )
    written = snapshot_writer(
        plan["artifact_paths"]["backfill_receipt_evidence"],
        artifact_type=RECEIPT_ARTIFACT_TYPE,
        operation_id=plan["operation_id"],
        payload=receipt_payload,
    )
    return _task_artifact_handle(
        written,
        payload=receipt_payload,
        summary={
            "completed": bool(receipt_payload.get("completed")),
            "processedPointCount": _required_non_negative_int(
                receipt_payload.get("processed_point_count"),
                "processed_point_count",
            ),
            "targetPointCount": _required_non_negative_int(
                receipt_payload.get("target_point_count"),
                "target_point_count",
            ),
        },
    )


def _artifact_paths(artifact_root_path: str, operation_id: str) -> dict[str, str]:
    operation_root = f"{artifact_root_path.rstrip('/')}/{operation_id}"
    return {
        "backfill_plan": f"{operation_root}/qdrant-embedding-backfill-plan.json",
        "backfill_receipt": f"{operation_root}/qdrant-embedding-backfill-receipt.json",
        "backfill_receipt_evidence": (
            f"{operation_root}/qdrant-embedding-backfill-receipt-evidence.json"
        ),
    }


def _task_artifact_handle(
    written: Mapping[str, Any],
    *,
    payload: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "artifactType": _required_token(written.get("artifactType"), "artifactType"),
        "evidence": {
            "s3Uri": _required_s3_uri(written.get("artifactPath"), "artifactPath"),
            "sha256": "sha256:"
            + _required_sha256_hex(written.get("artifactSha256"), "artifactSha256"),
            "versionId": _required_token(written.get("artifactVersionId"), "artifactVersionId"),
        },
        "payload": dict(payload),
        "schema": TASK_ARTIFACT_HANDLE_SCHEMA,
        "summary": dict(summary),
    }


def _task_artifact_payload(
    raw_handle: Mapping[str, Any] | str, expected_type: str
) -> dict[str, Any]:
    handle = _json_object(raw_handle, "task_artifact_handle")
    if _required_token(handle.get("schema"), "schema") != TASK_ARTIFACT_HANDLE_SCHEMA:
        raise ValueError("qdrant embedding migration task artifact schema is unsupported")
    if _required_token(handle.get("artifactType"), "artifactType") != expected_type:
        raise ValueError("qdrant embedding migration task artifact type is unsupported")
    payload = handle.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("qdrant embedding migration task artifact payload is required")
    return dict(payload)


def _task_artifact_evidence(raw_handle: Mapping[str, Any] | str) -> dict[str, str]:
    handle = _json_object(raw_handle, "task_artifact_handle")
    evidence = handle.get("evidence")
    if not isinstance(evidence, Mapping):
        raise ValueError("qdrant embedding migration evidence is required")
    return {
        "s3Uri": _required_s3_uri(evidence.get("s3Uri"), "plan_evidence.s3Uri"),
        "sha256": "sha256:"
        + _required_sha256_hex(
            str(evidence.get("sha256", "")).removeprefix("sha256:"),
            "plan_evidence.sha256",
        ),
        "versionId": _required_token(evidence.get("versionId"), "plan_evidence.versionId"),
    }


def _plan_payload(value: Mapping[str, Any] | str) -> dict[str, Any]:
    plan = _json_object(value, "plan_json")
    if _required_token(plan.get("contract_version"), "contract_version") != PLAN_CONTRACT_VERSION:
        raise ValueError("qdrant embedding migration plan contract version is unsupported")
    if _required_token(plan.get("dag_id"), "dag_id") != DAG_ID:
        raise ValueError("qdrant embedding migration plan dag_id is unsupported")
    return dict(plan)


def _read_json_artifact(path: str, *, s3_client: Any) -> dict[str, Any]:
    bucket, key = _s3_bucket_key(path)
    payload = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{path} must contain valid JSON") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} must contain a JSON object")
    return dict(value)


def _json_object(value: Mapping[str, Any] | str, field_name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a JSON object or mapping")
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if not isinstance(loaded, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    return dict(loaded)


def _s3_bucket_key(path: str) -> tuple[str, str]:
    normalized = _required_s3_uri(path, "artifact_path").removeprefix("s3://")
    bucket, key = normalized.split("/", 1)
    return bucket, key


def _operation_id(prefix: str, *parts: object) -> str:
    material = "|".join(str(part) for part in parts)
    return f"{prefix}-{sha256(material.encode('utf-8')).hexdigest()[:32]}"


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _required_s3_uri(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.startswith("s3://") or value.count("/") < 3:
        raise ValueError(f"{field_name} must be an s3:// artifact path")
    return value.rstrip("/")


def _required_token(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or not _SAFE_TOKEN_RE.fullmatch(value):
        raise ValueError(f"{field_name} is required")
    return value


def _required_embedding_profile_version(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not _EMBEDDING_PROFILE_VERSION_RE.fullmatch(value):
        raise ValueError(f"{field_name} must use canonical profile@version syntax")
    return value


def _required_route_prefix(value: object, field_name: str) -> str:
    token = _required_token(value, field_name)
    if not _ROUTE_PREFIX_RE.fullmatch(token):
        raise ValueError(f"{field_name} must be an uppercase environment-prefix token")
    return token


def _required_positive_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _required_non_negative_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _required_datetime_string(value: object, field_name: str) -> str:
    token = _required_token(value, field_name)
    try:
        datetime.fromisoformat(token.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 datetime") from exc
    return token


def _required_sha256_hex(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field_name} must be a 64-character lowercase sha256 hex digest")
    return value


default_args = {
    "owner": "serp-qdrant-embedding-migration",
    "start_date": datetime(2026, 8, 2, tzinfo=UTC),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="SERP governed Qdrant embedding backfill without alias mutation",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["serp", "qdrant", "migration", "bc10"],
)

validate_plan = PythonOperator(
    task_id="validate_qdrant_embedding_migration_plan",
    python_callable=validate_qdrant_embedding_migration_plan,
    executor_config=QDRANT_EMBEDDING_BACKFILL_EVIDENCE_EXECUTOR_CONFIG,
    dag=dag,
)

write_plan = PythonOperator(
    task_id="write_qdrant_embedding_migration_plan",
    python_callable=write_qdrant_embedding_migration_plan,
    op_args=["{{ ti.xcom_pull(task_ids='validate_qdrant_embedding_migration_plan') }}"],
    executor_config=QDRANT_EMBEDDING_BACKFILL_EVIDENCE_EXECUTOR_CONFIG,
    dag=dag,
)

dispatch_handoff = PythonOperator(
    task_id="dispatch_qdrant_embedding_backfill_handoff",
    python_callable=dispatch_qdrant_embedding_backfill_handoff_from_snapshot,
    op_args=["{{ ti.xcom_pull(task_ids='write_qdrant_embedding_migration_plan') }}"],
    executor_config=QDRANT_EMBEDDING_BACKFILL_EVIDENCE_EXECUTOR_CONFIG,
    dag=dag,
)

run_qdrant_embedding_backfill = KubernetesPodOperator(
    task_id="run_qdrant_embedding_backfill",
    name="serp-qdrant-embedding-backfill",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "dags.serp_qdrant_embedding_migration_remote_runner"],
    env_vars=pipeline_runner_env_vars("dispatch_qdrant_embedding_backfill_handoff"),
    service_account_name=QDRANT_EMBEDDING_BACKFILL_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    labels=QDRANT_EMBEDDING_BACKFILL_LABELS,
    volumes=QDRANT_EMBEDDING_BACKFILL_RUNTIME_VOLUMES,
    volume_mounts=QDRANT_EMBEDDING_BACKFILL_RUNTIME_VOLUME_MOUNTS,
    container_resources=QDRANT_EMBEDDING_BACKFILL_RESOURCES,
    container_security_context=k8s.V1SecurityContext(
        allow_privilege_escalation=False,
        capabilities=k8s.V1Capabilities(drop=["ALL"]),
    ),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=1,
    retry_delay=timedelta(seconds=5),
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

snapshot_receipt = PythonOperator(
    task_id="snapshot_qdrant_embedding_backfill_receipt",
    python_callable=snapshot_qdrant_embedding_backfill_receipt_from_snapshot,
    op_args=["{{ ti.xcom_pull(task_ids='write_qdrant_embedding_migration_plan') }}"],
    executor_config=QDRANT_EMBEDDING_BACKFILL_EVIDENCE_EXECUTOR_CONFIG,
    dag=dag,
)

validate_plan >> write_plan >> dispatch_handoff >> run_qdrant_embedding_backfill >> snapshot_receipt
