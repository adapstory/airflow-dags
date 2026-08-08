"""Durable Airflow ownership and terminal outbox contract for D19 releases."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

D19_AIRFLOW_OWNERSHIP_SCHEMA = "SerpD19AirflowOwnership/v1"
D19_TERMINAL_OUTBOX_SCHEMA = "SerpD19TerminalOutbox/v1"
D19_DAG_ID = "serp_benchmark_improvement_wave"
D19_RUN_COUNT = 4
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")


def _timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"D19 handoff {field_name} must be RFC3339 UTC")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"D19 handoff {field_name} must be RFC3339 UTC") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"D19 handoff {field_name} must be timezone-aware")
    return parsed.astimezone(UTC)


def _required_string(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"D19 handoff {field_name} is required")
    return value


def validate_release_ownership(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the complete immutable run plan accepted by Airflow."""

    if (
        set(value)
        != {
            "schema",
            "correlationId",
            "measurementTargetAt",
            "noProgressTimeoutSeconds",
            "runs",
        }
        or value.get("schema") != D19_AIRFLOW_OWNERSHIP_SCHEMA
    ):
        raise ValueError("D19 Airflow ownership has an invalid shape/schema")
    correlation_id = _required_string(value.get("correlationId"), "correlationId")
    try:
        if str(UUID(correlation_id)) != correlation_id:
            raise ValueError
    except ValueError as exc:
        raise ValueError("D19 handoff correlationId must be a canonical UUID") from exc
    target = _timestamp(value.get("measurementTargetAt"), "measurementTargetAt")
    timeout = value.get("noProgressTimeoutSeconds")
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("D19 handoff noProgressTimeoutSeconds must be positive")
    runs = value.get("runs")
    if not isinstance(runs, list) or len(runs) != D19_RUN_COUNT:
        raise ValueError("D19 Airflow ownership must contain four ordered runs")
    normalized_runs: list[dict[str, Any]] = []
    for index, raw in enumerate(runs):
        if not isinstance(raw, Mapping) or set(raw) != {
            "role",
            "ordinal",
            "runId",
            "logicalDate",
        }:
            raise ValueError("D19 Airflow ownership run has an invalid shape")
        expected_role = "canary" if index == 0 else "governance"
        if raw.get("ordinal") != index or raw.get("role") != expected_role:
            raise ValueError("D19 Airflow ownership runs must be ordered")
        run_id = _required_string(raw.get("runId"), "runId")
        logical_date = _timestamp(raw.get("logicalDate"), "logicalDate")
        normalized_runs.append(
            {
                "role": expected_role,
                "ordinal": index,
                "runId": run_id,
                "logicalDate": logical_date.isoformat().replace("+00:00", "Z"),
            }
        )
    if len({run["runId"] for run in normalized_runs}) != D19_RUN_COUNT:
        raise ValueError("D19 Airflow ownership run IDs must be unique")
    if [run["logicalDate"] for run in normalized_runs] != sorted(
        run["logicalDate"] for run in normalized_runs
    ):
        raise ValueError("D19 Airflow ownership logical dates must be ordered")
    return {
        "schema": D19_AIRFLOW_OWNERSHIP_SCHEMA,
        "correlationId": correlation_id,
        "measurementTargetAt": target.isoformat().replace("+00:00", "Z"),
        "noProgressTimeoutSeconds": timeout,
        "runs": normalized_runs,
    }


def _validate_qualification(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping) or set(value) != {
        "suiteCount",
        "packCount",
        "workItemCount",
    }:
        raise ValueError("D19 canary qualification has an invalid shape")
    expected = {"suiteCount": 9, "packCount": 18, "workItemCount": 90}
    if dict(value) != expected:
        raise ValueError("D19 canary qualification is incomplete")
    return expected


def _validate_outbox_evidence(value: Mapping[str, Any]) -> dict[str, str]:
    if set(value) != {
        "artifactPath",
        "artifactVersionId",
        "artifactSha256",
        "objectLockMode",
        "retainUntil",
    }:
        raise ValueError("D19 terminal outbox evidence has an invalid shape")
    path = _required_string(value.get("artifactPath"), "outbox artifactPath")
    version = _required_string(value.get("artifactVersionId"), "outbox artifactVersionId")
    digest = _required_string(value.get("artifactSha256"), "outbox artifactSha256")
    if not path.startswith("s3://") or _SHA256.fullmatch(digest) is None:
        raise ValueError("D19 terminal outbox evidence is invalid")
    if value.get("objectLockMode") != "COMPLIANCE":
        raise ValueError("D19 terminal outbox evidence must be COMPLIANCE locked")
    retain_until = _timestamp(value.get("retainUntil"), "outbox retainUntil")
    return {
        "artifactPath": path,
        "artifactVersionId": version,
        "artifactSha256": digest,
        "objectLockMode": "COMPLIANCE",
        "retainUntil": retain_until.isoformat().replace("+00:00", "Z"),
    }


def build_terminal_outbox(
    *,
    ownership: Mapping[str, Any],
    current_run_id: str,
    verification_pointer: Mapping[str, Any],
    canary_qualification: Mapping[str, Any] | None,
    observed_at: datetime,
    artifact_root_path: str,
    writer: Callable[[str, dict[str, Any]], Mapping[str, Any]],
) -> dict[str, Any]:
    """Seal one terminal event and return the sole allowed next transition."""

    normalized = validate_release_ownership(ownership)
    matching = [run for run in normalized["runs"] if run["runId"] == current_run_id]
    if len(matching) != 1:
        raise ValueError("current D19 run is outside the Airflow ownership plan")
    current = matching[0]
    ordinal = int(current["ordinal"])
    airflow_run = verification_pointer.get("airflowRun")
    receipt_status = verification_pointer.get("receiptStatus")
    if (
        not isinstance(airflow_run, Mapping)
        or airflow_run.get("dagId") != D19_DAG_ID
        or airflow_run.get("runId") != current_run_id
        or receipt_status not in {"accepted", "rejected"}
    ):
        raise ValueError("D19 terminal verification pointer is mismatched")
    if ordinal == 0:
        qualification = _validate_qualification(canary_qualification)
    else:
        qualification = None
        if receipt_status != "accepted":
            raise ValueError("D19 governance receipt must be accepted")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("D19 terminal outbox observation must be timezone-aware")
    root = artifact_root_path.rstrip("/")
    if not root.startswith("s3://"):
        raise ValueError("D19 terminal outbox root must use s3://")
    next_run = normalized["runs"][ordinal + 1] if ordinal + 1 < D19_RUN_COUNT else None
    event = {
        "schema": D19_TERMINAL_OUTBOX_SCHEMA,
        "status": "RUN_TERMINAL",
        "correlationId": normalized["correlationId"],
        "observedAt": observed_at.astimezone(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "run": current,
        "receiptStatus": receipt_status,
        "verificationPointer": dict(verification_pointer),
        "canaryQualification": qualification,
        "nextRun": next_run,
    }
    path = f"{root}/d19-release-handoff/{normalized['correlationId']}/terminal-{ordinal:02d}.json"
    evidence = _validate_outbox_evidence(writer(path, event))
    return {
        "terminal": next_run is None,
        "nextRun": next_run,
        "outboxEvidence": evidence,
        "event": event,
    }
