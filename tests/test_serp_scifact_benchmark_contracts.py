from __future__ import annotations

from hashlib import sha256
from typing import Any

from dags.serp_scifact_benchmark_contracts import (
    SCIFACT_ARCHIVE_URL,
    activate_scifact_benchmark_pack,
    build_scifact_benchmark_plan,
    materialize_scifact_archive,
    prepare_scifact_benchmark_registry,
    submit_scifact_pipeline_state,
)


def test_scifact_plan_binds_a_dedicated_benchmark_pack_to_versioned_evidence() -> None:
    plan = build_scifact_benchmark_plan(
        {
            "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
            "generated_at": "2026-07-13T12:00:00Z",
        },
        bc21_base_url="http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )

    assert plan["dag_id"] == "serp_beir_scifact_live_benchmark"
    assert plan["archive_source_url"] == SCIFACT_ARCHIVE_URL
    assert plan["pack_slug"] == "benchmark-beir-scifact"
    assert plan["workflow_scope"] == {
        "tenant_mode": "benchmark",
        "tenant_scope": "private",
        "workflow_code": "search_context",
    }
    assert plan["artifact_paths"]["archive"].endswith("/scifact.zip")
    assert plan["artifact_paths"]["index_evidence"].endswith("/scifact-indexing.json")
    assert plan["artifact_paths"]["run_evidence"].endswith("/scifact-live-run.json")


def test_scifact_archive_materialization_binds_bytes_and_s3_version_before_indexing() -> None:
    plan = build_scifact_benchmark_plan(
        {
            "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
            "generated_at": "2026-07-13T12:00:00Z",
        },
        bc21_base_url="http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    archive = b"PK\\x03\\x04scifact"
    calls: list[dict[str, object]] = []

    def snapshot_writer(**kwargs: object) -> dict[str, str]:
        calls.append(kwargs)
        return {
            "artifactETag": "archive-etag",
            "artifactPath": str(kwargs["artifact_path"]),
            "artifactSha256": sha256(archive).hexdigest(),
            "artifactType": "beir_scifact_archive",
            "artifactVersionId": "version-scifact",
            "objectLockMode": "COMPLIANCE",
        }

    snapshot = materialize_scifact_archive(
        plan,
        fetch_bytes=lambda url: archive if url == SCIFACT_ARCHIVE_URL else b"",
        snapshot_writer=snapshot_writer,
    )

    assert snapshot["archiveSha256"] == sha256(archive).hexdigest()
    assert snapshot["archiveVersionId"] == "version-scifact"
    assert snapshot["objectLockMode"] == "COMPLIANCE"
    assert calls[0]["artifact_path"] == plan["artifact_paths"]["archive"]
    assert calls[0]["content_type"] == "application/zip"


def test_scifact_registry_setup_creates_or_reuses_only_a_dedicated_pack_and_source() -> None:
    plan = build_scifact_benchmark_plan(
        {
            "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
            "generated_at": "2026-07-13T12:00:00Z",
        },
        bc21_base_url="http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    archive_snapshot = {
        "archiveETag": "archive-etag",
        "archivePath": plan["artifact_paths"]["archive"],
        "archiveSha256": "a" * 64,
        "archiveVersionId": "version-scifact",
        "objectLockMode": "COMPLIANCE",
        "sourceUrl": SCIFACT_ARCHIVE_URL,
    }
    submissions: list[dict[str, Any]] = []

    def post_json(
        _base_url: str,
        endpoint: str,
        *,
        body: dict[str, object],
        headers: dict[str, str],
        error_label: str,
    ) -> dict[str, str]:
        submissions.append(
            {"body": body, "endpoint": endpoint, "error_label": error_label, "headers": headers}
        )
        if endpoint == "/api/bc-21/serp/v1/sources":
            return {"resourceId": "00000000-0000-4000-a000-000000000111"}
        return {"resourceId": "00000000-0000-4000-a000-000000000222"}

    registry = prepare_scifact_benchmark_registry(
        plan,
        archive_snapshot,
        list_resources=lambda _kind: [],
        post_json=post_json,
    )

    assert registry["source_id"] == "00000000-0000-4000-a000-000000000111"
    assert registry["pack_id"] == "00000000-0000-4000-a000-000000000222"
    assert registry["pack_version_id"]
    assert registry["pipeline_run_id"]
    assert registry["workflow_scope"] == plan["workflow_scope"]
    assert [submission["endpoint"] for submission in submissions] == [
        "/api/bc-21/serp/v1/sources",
        "/api/bc-21/serp/v1/packs",
    ]
    assert submissions[0]["body"]["sourceType"] == "website"
    assert submissions[0]["body"]["dataClass"] == "PUBLIC"
    assert submissions[1]["body"]["slug"] == "benchmark-beir-scifact"


def test_scifact_pack_activation_requires_index_receipt_and_records_selection() -> None:
    plan = build_scifact_benchmark_plan(
        {
            "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
            "generated_at": "2026-07-13T12:00:00Z",
        },
        bc21_base_url="http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    registry = {
        "archive_sha256": "a" * 64,
        "pack_id": "00000000-0000-4000-a000-000000000222",
        "pack_version_id": "00000000-0000-4000-a000-000000000333",
        "tenant_id": plan["tenant_id"],
        "workflow_scope": plan["workflow_scope"],
    }
    pipeline_receipt = {
        "response": {
            "evidenceBundleId": "00000000-0000-4000-a000-000000000444",
            "evidenceSealHash": "sha256:" + "b" * 64,
            "resourceId": registry["pack_id"],
            "runId": "00000000-0000-4000-a000-000000000555",
            "tenantId": plan["tenant_id"],
        },
        "status": "accepted",
    }
    submissions: list[dict[str, Any]] = []

    def post_json(
        _base_url: str,
        endpoint: str,
        *,
        body: dict[str, object],
        headers: dict[str, str],
        error_label: str,
    ) -> dict[str, str]:
        submissions.append({"body": body, "endpoint": endpoint, "headers": headers})
        if endpoint.endswith("autonomous-approval-decisions"):
            return {
                "approvalDecision": "approve",
                "approvalState": "approved",
                "autonomousRunId": "00000000-0000-4000-a000-000000000666",
                "packId": registry["pack_id"],
                "policyDecision": "approved",
                "tenantId": plan["tenant_id"],
            }
        if endpoint.endswith("publish-activations"):
            return {
                "activationState": "active",
                "evidenceBundleId": "00000000-0000-4000-a000-000000000444",
                "packId": registry["pack_id"],
                "packVersionId": registry["pack_version_id"],
                "tenantId": plan["tenant_id"],
            }
        return {
            "evidenceBundleId": "00000000-0000-4000-a000-000000000444",
            "packId": registry["pack_id"],
            "selectionState": "active",
            "tenantId": plan["tenant_id"],
        }

    result = activate_scifact_benchmark_pack(
        plan,
        registry,
        pipeline_receipt,
        post_json=post_json,
    )

    assert [submission["endpoint"] for submission in submissions] == [
        "/api/bc-21/serp/v1/governance/autonomous-approval-decisions",
        f"/api/bc-21/serp/v1/packs/{registry['pack_id']}/publish-activations",
        "/api/bc-21/serp/v1/packs/workflow-selections",
    ]
    approval = submissions[0]
    assert approval["body"] == {
        "actorId": plan["actor_id"],
        "dataClass": "PUBLIC",
        "freshnessState": "fresh",
        "licenseObligationState": "public_share_allowed",
        "packId": registry["pack_id"],
        "policyVersion": "beir-scifact-license-policy@2026.07.13",
        "sourceType": "website",
        "trustState": "trusted",
    }
    activation = submissions[1]
    assert activation["body"] == {
        "activationReasonCode": "beir_scifact_indexed_evidence_approved",
        "approvalRunId": "00000000-0000-4000-a000-000000000666",
        "evidenceBundleId": "00000000-0000-4000-a000-000000000444",
        "evidenceSealHash": "sha256:" + "b" * 64,
        "indexedRunId": "00000000-0000-4000-a000-000000000555",
        "packVersionId": registry["pack_version_id"],
    }
    selection = submissions[2]
    assert selection["body"]["actorId"] == plan["actor_id"]
    assert selection["body"]["evidenceBundleId"] == "00000000-0000-4000-a000-000000000444"
    assert selection["body"]["policyBundleSha256"].startswith("sha256:")
    for submission in submissions:
        headers = submission["headers"]
        assert headers["X-Adapstory-Tenant-Id"] == plan["tenant_id"]
        assert headers["X-Adapstory-Actor-Id"] == plan["actor_id"]
        assert headers["X-Adapstory-Trusted-Actor-Id"] == plan["actor_id"]
        assert headers["X-Adapstory-Trusted-Tenant-Id"] == plan["tenant_id"]
    assert result["active_pack_version_id"] == registry["pack_version_id"]
    assert result["workflow_selection"]["selectionState"] == "active"


def test_scifact_pipeline_state_submission_binds_the_index_evidence_to_a_worm_receipt() -> None:
    plan = build_scifact_benchmark_plan(
        {
            "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
            "generated_at": "2026-07-13T12:00:00Z",
        },
        bc21_base_url="http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    registry = {
        "archive_sha256": "a" * 64,
        "archive_version_id": "version-scifact",
        "pack_id": "00000000-0000-4000-a000-000000000222",
        "pack_version_id": "00000000-0000-4000-a000-000000000333",
        "source_id": "00000000-0000-4000-a000-000000000111",
        "tenant_id": plan["tenant_id"],
    }
    evidence = {
        "archive_snapshot": {
            "artifact_path": plan["artifact_paths"]["archive"],
            "artifact_sha256": "sha256:" + "a" * 64,
            "artifact_version_id": registry["archive_version_id"],
            "object_lock_mode": "COMPLIANCE",
        },
        "artifact_type": "beir_scifact_indexing_evidence",
        "pack_id": registry["pack_id"],
        "pack_version_id": registry["pack_version_id"],
        "source_id": registry["source_id"],
        "status": "indexed",
        "tenant_id": plan["tenant_id"],
        "pipeline_state_submission": {
            "body": {
                "packVersionId": registry["pack_version_id"],
                "resourceId": registry["pack_id"],
                "runId": "00000000-0000-4000-a000-000000000555",
                "sourceId": registry["source_id"],
                "status": "indexed",
            },
            "endpointPath": "/api/bc-21/serp/v1/runs/pipeline-state",
            "headers": {
                "X-Adapstory-Actor-Id": plan["actor_id"],
                "X-Adapstory-Tenant-Id": plan["tenant_id"],
            },
        },
    }
    submitted: list[dict[str, Any]] = []

    def post_json(
        _base_url: str,
        endpoint: str,
        *,
        body: dict[str, object],
        headers: dict[str, str],
        error_label: str,
    ) -> dict[str, str]:
        submitted.append({"body": body, "endpoint": endpoint, "headers": headers})
        return {
            "evidenceBundleId": "00000000-0000-4000-a000-000000000444",
            "evidenceSealHash": "sha256:" + "b" * 64,
            "resourceId": registry["pack_id"],
            "runId": body["runId"],
            "status": "indexed",
            "tenantId": plan["tenant_id"],
        }

    receipt = submit_scifact_pipeline_state(
        plan,
        registry,
        evidence_reader=lambda _path, _field: evidence,
        post_json=post_json,
        snapshot_writer=lambda **kwargs: {
            "artifactETag": "pipeline-state-etag",
            "artifactPath": kwargs["artifact_path"],
            "artifactSha256": "c" * 64,
            "artifactType": kwargs["artifact_type"],
            "artifactVersionId": "pipeline-state-version",
            "objectLockMode": "COMPLIANCE",
        },
    )

    assert submitted[0]["endpoint"] == "/api/bc-21/serp/v1/runs/pipeline-state"
    assert receipt["status"] == "accepted"
    assert receipt["response"]["evidenceBundleId"] == "00000000-0000-4000-a000-000000000444"
    assert receipt["snapshot"]["artifactVersionId"] == "pipeline-state-version"
