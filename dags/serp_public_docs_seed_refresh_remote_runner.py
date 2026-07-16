from __future__ import annotations

import importlib as importlib
import json
import os
from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from urllib.parse import quote, unquote

from dags.serp_eval_contracts import (
    PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES as PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES,
)
from dags.serp_eval_contracts import (
    _read_public_docs_exact_evidence_bytes,
    _s3_client,
    _validated_public_docs_exact_evidence_handle,
)

PIPELINE_CLI_SPEC_ENV = "ADAPSTORY_SERP_PIPELINE_CLI_SPEC_URLENCODED"
PIPELINE_REMOTE_RUNNER_MODULE = "adapstory_serp_pipeline.orchestration.seed_refresh_remote_runner"


def main() -> int:
    encoded_spec = os.environ.get(PIPELINE_CLI_SPEC_ENV)
    if encoded_spec is None or not encoded_spec.strip():
        raise ValueError(f"{PIPELINE_CLI_SPEC_ENV} is required")
    spec = _json_object(unquote(encoded_spec), PIPELINE_CLI_SPEC_ENV)
    operation_id = _required_str(spec, "operation_id")
    status = _required_str(spec, "status")
    evidence = _validated_public_docs_exact_evidence_handle(
        _required_mapping(spec, "refresh_plan_evidence"),
        "refresh_plan_evidence",
    )
    payload_bytes = _read_public_docs_exact_evidence_bytes(
        evidence,
        field_name="public docs seed refresh plan evidence",
        s3_client=_s3_client(evidence["s3Uri"]),
        max_bytes=PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES,
    )
    refresh_plan = _json_object(
        payload_bytes.decode("utf-8"),
        "public docs seed refresh plan evidence",
    )
    if _required_str(refresh_plan, "operation_id") != operation_id:
        raise ValueError("public docs seed refresh plan operation_id does not match cli spec")

    expected_refresh_status = {
        "no_due_sources": "no_due_sources",
        "ready_for_pipeline_cli_runner": "ready_for_pipeline_dispatch",
    }.get(status)
    if expected_refresh_status is None:
        raise ValueError("public docs seed refresh cli spec status is unsupported")
    if _required_str(refresh_plan, "status") != expected_refresh_status:
        raise ValueError("public docs seed refresh plan status does not match cli spec")

    delegated_spec = dict(spec)
    if status == "ready_for_pipeline_cli_runner":
        with TemporaryDirectory(prefix="serp-d20-refresh-plan-") as temp_dir:
            local_path = Path(temp_dir) / "public-docs-seed-refresh-plan.json"
            local_path.write_bytes(payload_bytes)
            delegated_spec = _materialize_refresh_plan(delegated_spec, evidence, local_path)
            return _delegate(encoded_spec, delegated_spec)
    return _delegate(encoded_spec, delegated_spec)


def _materialize_refresh_plan(
    spec: Mapping[str, Any],
    evidence: Mapping[str, str],
    local_path: Path,
) -> dict[str, Any]:
    argv = _required_str_list(spec, "argv")
    if argv.count("--refresh-plan") != 1:
        raise ValueError("public docs seed refresh cli argv must contain one --refresh-plan")
    option_index = argv.index("--refresh-plan")
    if option_index + 1 >= len(argv) or argv[option_index + 1] != evidence["s3Uri"]:
        raise ValueError("public docs seed refresh cli argv must reference exact refresh evidence")
    input_paths = _required_str_list(spec, "input_paths")
    if input_paths != [evidence["s3Uri"]]:
        raise ValueError(
            "public docs seed refresh input_paths must reference exact refresh evidence"
        )
    materialized_argv = list(argv)
    materialized_argv[option_index + 1] = str(local_path)
    return {
        **spec,
        "argv": materialized_argv,
        "input_paths": [str(local_path)],
    }


def _delegate(original_encoded_spec: str, delegated_spec: Mapping[str, Any]) -> int:
    os.environ[PIPELINE_CLI_SPEC_ENV] = quote(
        json.dumps(delegated_spec, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        safe="",
    )
    try:
        runner = importlib.import_module(PIPELINE_REMOTE_RUNNER_MODULE)
        result = runner.main()
        if isinstance(result, bool) or not isinstance(result, int):
            raise TypeError("public docs pipeline remote runner main must return an integer")
        return cast(int, result)
    finally:
        os.environ[PIPELINE_CLI_SPEC_ENV] = original_encoded_spec


def _json_object(raw_value: str, field_name: str) -> dict[str, Any]:
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return cast(dict[str, Any], value)


def _required_mapping(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    field = value.get(field_name)
    if not isinstance(field, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return field


def _required_str(value: Mapping[str, Any], field_name: str) -> str:
    field = value.get(field_name)
    if not isinstance(field, str) or not field.strip():
        raise ValueError(f"{field_name} is required")
    return field


def _required_str_list(value: Mapping[str, Any], field_name: str) -> list[str]:
    fields = value.get(field_name)
    if not isinstance(fields, list) or not fields:
        raise ValueError(f"{field_name} must be a non-empty list")
    if any(not isinstance(field, str) or not field.strip() for field in fields):
        raise ValueError(f"{field_name} must contain non-empty strings")
    return cast(list[str], fields)


if __name__ == "__main__":
    raise SystemExit(main())
