from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Mapping
from hashlib import sha256
from typing import Any
from urllib.parse import unquote

from dags.serp_evidence_workload_identity import (
    operation_prefix_read_s3_client as _operation_prefix_read_s3_client,
)

PIPELINE_CLI_SPEC_ENV = "ADAPSTORY_SERP_PIPELINE_CLI_SPEC_URLENCODED"
PIPELINE_CLI_CONTRACT_VERSION = "serp-qdrant-embedding-migration-pipeline-bridge/v1"
_DAG_ID = "serp_qdrant_embedding_migration"
_TASK_ID = "qdrant_embedding_backfill_pipeline"


def main() -> int:
    encoded_spec = os.environ.get(PIPELINE_CLI_SPEC_ENV)
    if encoded_spec is None or not encoded_spec.strip():
        raise ValueError(f"{PIPELINE_CLI_SPEC_ENV} is required")
    spec = _json_object(unquote(encoded_spec), PIPELINE_CLI_SPEC_ENV)
    if _required_token(spec.get("contract_version"), "contract_version") != PIPELINE_CLI_CONTRACT_VERSION:
        raise ValueError("qdrant embedding remote runner cli spec contract version is unsupported")
    if _required_token(spec.get("dag_id"), "dag_id") != _DAG_ID:
        raise ValueError("qdrant embedding remote runner dag_id is unsupported")
    if _required_token(spec.get("task_id"), "task_id") != _TASK_ID:
        raise ValueError("qdrant embedding remote runner task_id is unsupported")
    if _required_token(spec.get("status"), "status") != "ready_for_pipeline_cli_runner":
        raise ValueError("qdrant embedding remote runner cli spec is not ready")
    argv = _required_str_list(spec.get("argv"), "argv")
    if any(token in {";", "&&", "|"} for token in argv):
        raise ValueError("qdrant embedding remote runner argv must not contain shell operators")
    receipt_path = _required_s3_uri(spec.get("receipt_uri"), "receipt_uri")
    supplied_receipt_path = _required_cli_option_value(argv, "--receipt-uri")
    if supplied_receipt_path != receipt_path:
        raise ValueError("qdrant embedding remote runner must pass the canonical receipt URI to CLI")
    plan_evidence = _validated_evidence_handle(spec.get("plan_evidence"), "plan_evidence")
    input_paths = _required_str_list(spec.get("input_paths"), "input_paths")
    if input_paths != [plan_evidence["s3Uri"]]:
        raise ValueError("qdrant embedding remote runner input_paths must reference exact plan evidence")
    s3_client = _operation_prefix_read_s3_client(
        artifact_uris=(plan_evidence["s3Uri"], receipt_path)
    )
    plan_payload = _read_json_artifact(plan_evidence["s3Uri"], s3_client=s3_client)
    if "sha256:" + sha256(_canonical_json(plan_payload).encode("utf-8")).hexdigest() != plan_evidence["sha256"]:
        raise ValueError("qdrant embedding remote runner plan evidence digest does not match")
    if _required_token(plan_payload.get("operation_id"), "operation_id") != _required_token(spec.get("operation_id"), "operation_id"):
        raise ValueError("qdrant embedding remote runner operation_id does not match the immutable plan")
    if _required_token(plan_payload.get("source_collection"), "source_collection") != _required_cli_option_value(argv, "--source-collection"):
        raise ValueError("qdrant embedding remote runner source_collection does not match the immutable plan")
    if _required_token(plan_payload.get("target_collection"), "target_collection") != _required_cli_option_value(argv, "--target-collection"):
        raise ValueError("qdrant embedding remote runner target_collection does not match the immutable plan")
    plan_receipt_path = plan_payload.get("receipt_uri")
    if plan_receipt_path is None:
        artifact_paths = plan_payload.get("artifact_paths")
        if isinstance(artifact_paths, Mapping):
            plan_receipt_path = artifact_paths.get("backfill_receipt")
    if _required_s3_uri(plan_receipt_path, "receipt_uri") != receipt_path:
        raise ValueError("qdrant embedding remote runner receipt_uri does not match the immutable plan")

    completed = subprocess.run(argv, capture_output=True, check=False, text=True)
    if completed.returncode != 0:
        raise ValueError(
            "qdrant embedding remote runner failed: "
            f"returncode={completed.returncode} "
            f"stderr_sha256={sha256(completed.stderr.encode('utf-8')).hexdigest()}"
        )

    stdout_payload = _json_object(completed.stdout, "pipeline_cli_stdout")
    receipt_payload = _read_json_artifact(receipt_path, s3_client=s3_client)
    if stdout_payload != receipt_payload:
        raise ValueError("qdrant embedding remote runner stdout does not match the canonical receipt")
    return 0


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


def _validated_evidence_handle(value: object, field_name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return {
        "s3Uri": _required_s3_uri(value.get("s3Uri"), f"{field_name}.s3Uri"),
        "sha256": _required_prefixed_sha256(value.get("sha256"), f"{field_name}.sha256"),
        "versionId": _required_token(value.get("versionId"), f"{field_name}.versionId"),
    }


def _required_cli_option_value(argv: list[str], option_name: str) -> str:
    try:
        option_index = argv.index(option_name)
    except ValueError as exc:
        raise ValueError(f"{option_name} is required") from exc
    value_index = option_index + 1
    if value_index >= len(argv):
        raise ValueError(f"{option_name} requires a value")
    return argv[value_index]


def _json_object(raw_value: str, field_name: str) -> dict[str, Any]:
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    return dict(value)


def _required_str_list(value: object, field_name: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list")
    result = []
    for item in value:
        result.append(_required_token(item, field_name))
    return result


def _required_token(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _required_s3_uri(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.startswith("s3://") or value.count("/") < 3:
        raise ValueError(f"{field_name} must use s3://")
    return value


def _required_prefixed_sha256(value: object, field_name: str) -> str:
    token = _required_token(value, field_name)
    if not token.startswith("sha256:") or len(token) != len("sha256:") + 64:
        raise ValueError(f"{field_name} must be sha256:<64 lowercase hex>")
    return token


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _s3_bucket_key(path: str) -> tuple[str, str]:
    normalized = path.removeprefix("s3://")
    bucket, key = normalized.split("/", 1)
    return bucket, key


if __name__ == "__main__":
    raise SystemExit(main())
