from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from dags.serp_d19_release_handoff import (
    D19_AIRFLOW_OWNERSHIP_SCHEMA,
    D19_TERMINAL_OUTBOX_SCHEMA,
    build_terminal_outbox,
    validate_release_ownership,
)


def _ownership() -> dict[str, Any]:
    return {
        "schema": D19_AIRFLOW_OWNERSHIP_SCHEMA,
        "correlationId": "1a4cae22-41e1-4ed2-b1ef-531821a78b72",
        "measurementTargetAt": "2026-09-19T07:00:00Z",
        "noProgressTimeoutSeconds": 1800,
        "runs": [
            {
                "role": "canary" if index == 0 else "governance",
                "ordinal": index,
                "runId": f"run-{index}",
                "logicalDate": f"2026-08-08T07:0{index}:00Z",
            }
            for index in range(4)
        ],
    }


def _pointer(status: str = "accepted") -> dict[str, Any]:
    return {
        "receiptStatus": status,
        "requestId": "request-1",
        "airflowRun": {
            "dagId": "serp_benchmark_improvement_wave",
            "runId": "run-0",
            "logicalDate": "2026-08-08T07:00:00Z",
            "runType": "manual",
        },
        "pairedEvaluationVerificationEvidence": {
            "s3Uri": "s3://airflow-serp-evidence/serp-evals/op/receipt.json",
            "versionId": "version-1",
            "sha256": "sha256:" + "1" * 64,
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-08-08T07:00:00Z",
        },
        "observedNormalizedScoreCellsEvidence": {
            "artifactPath": "s3://airflow-serp-evidence/serp-evals/op/cells.json",
            "artifactVersionId": "version-2",
            "artifactSha256": "sha256:" + "2" * 64,
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-08-08T12:00:00Z",
        },
    }


def test_canary_outbox_advances_the_next_run_only_after_exact_qualification() -> None:
    writes: list[tuple[str, dict[str, Any]]] = []

    result = build_terminal_outbox(
        ownership=_ownership(),
        current_run_id="run-0",
        verification_pointer=_pointer("rejected"),
        canary_qualification={"suiteCount": 9, "packCount": 18, "workItemCount": 90},
        observed_at=datetime(2026, 8, 8, 8, 0, tzinfo=UTC),
        artifact_root_path="s3://airflow-serp-evidence/serp-evals/op",
        writer=lambda path, payload: writes.append((path, payload))
        or {
            "artifactPath": path,
            "artifactVersionId": "outbox-version",
            "artifactSha256": "sha256:" + "3" * 64,
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-08-08T12:00:00Z",
        },
    )

    assert writes[0][1]["schema"] == D19_TERMINAL_OUTBOX_SCHEMA
    assert writes[0][1]["status"] == "RUN_TERMINAL"
    assert result["nextRun"] == _ownership()["runs"][1]
    assert result["terminal"] is False
    assert result["outboxEvidence"]["artifactVersionId"] == "outbox-version"


def test_last_governance_outbox_is_terminal_and_has_no_next_run() -> None:
    result = build_terminal_outbox(
        ownership=_ownership(),
        current_run_id="run-3",
        verification_pointer={
            **_pointer(),
            "airflowRun": {**_pointer()["airflowRun"], "runId": "run-3"},
        },
        canary_qualification=None,
        observed_at=datetime(2026, 8, 8, 12, 0, tzinfo=UTC),
        artifact_root_path="s3://airflow-serp-evidence/serp-evals/op",
        writer=lambda path, _payload: {
            "artifactPath": path,
            "artifactVersionId": "terminal-version",
            "artifactSha256": "sha256:" + "4" * 64,
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-08-08T12:00:00Z",
        },
    )

    assert result["terminal"] is True
    assert result["nextRun"] is None


def test_governance_rejection_cannot_advance_the_state_machine() -> None:
    with pytest.raises(ValueError, match="governance receipt must be accepted"):
        build_terminal_outbox(
            ownership=_ownership(),
            current_run_id="run-1",
            verification_pointer={
                **_pointer("rejected"),
                "airflowRun": {**_pointer()["airflowRun"], "runId": "run-1"},
            },
            canary_qualification=None,
            observed_at=datetime(2026, 8, 8, 9, 0, tzinfo=UTC),
            artifact_root_path="s3://airflow-serp-evidence/serp-evals/op",
            writer=lambda _path, _payload: {},
        )


def test_ownership_rejects_duplicate_or_reordered_runs() -> None:
    ownership = _ownership()
    ownership["runs"][2]["ordinal"] = 1

    with pytest.raises(ValueError, match="ordered"):
        validate_release_ownership(ownership)
