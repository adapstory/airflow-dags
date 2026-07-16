from __future__ import annotations

import ast
from hashlib import sha256
from pathlib import Path
from typing import Any

from dags.serp_scifact_benchmark_contracts import (
    SCIFACT_ARCHIVE_URL,
    activate_scifact_benchmark_pack,
    build_scifact_benchmark_plan,
    materialize_scifact_archive,
    prepare_scifact_benchmark_registry,
    seal_scifact_activation_evidence,
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
    assert plan["actor_id"] == "airflow-serp-benchmark-builder"
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


def test_scifact_dag_assigns_each_phase_only_its_required_workload_identity() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_beir_scifact_live_benchmark.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assignments = {
        target.id: node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name) and target.id.endswith("_EXECUTOR_CONFIG")
    }
    assert {
        "SCIFACT_ACQUISITION_EXECUTOR_CONFIG",
        "SCIFACT_BUILDER_EXECUTOR_CONFIG",
        "SCIFACT_ACTIVATOR_EXECUTOR_CONFIG",
        "SCIFACT_AGGREGATOR_EXECUTOR_CONFIG",
    } <= set(assignments)
    for name in (
        "SCIFACT_BUILDER_EXECUTOR_CONFIG",
        "SCIFACT_ACTIVATOR_EXECUTOR_CONFIG",
    ):
        assignment = assignments[name]
        assert isinstance(assignment, ast.Call)
        assert isinstance(assignment.func, ast.Name)
        assert assignment.func.id == "bc21_authorized_minio_executor_config"
    for name in (
        "SCIFACT_ACQUISITION_EXECUTOR_CONFIG",
        "SCIFACT_AGGREGATOR_EXECUTOR_CONFIG",
    ):
        assignment = assignments[name]
        assert isinstance(assignment, ast.Call)
        assert isinstance(assignment.func, ast.Name)
        assert assignment.func.id == "minio_web_identity_executor_config"

    task_executor_configs = {}
    for call in ast.walk(tree):
        if not (isinstance(call, ast.Call) and isinstance(call.func, ast.Name)):
            continue
        if call.func.id != "PythonOperator":
            continue
        task_id = next(
            keyword.value.value
            for keyword in call.keywords
            if keyword.arg == "task_id"
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, str)
        )
        executor_config = next(
            keyword.value for keyword in call.keywords if keyword.arg == "executor_config"
        )
        assert isinstance(executor_config, ast.Name)
        task_executor_configs[task_id] = executor_config.id

    assert task_executor_configs == {
        "build_scifact_benchmark_plan": "SCIFACT_AGGREGATOR_EXECUTOR_CONFIG",
        "materialize_scifact_archive": "SCIFACT_ACQUISITION_EXECUTOR_CONFIG",
        "prepare_scifact_benchmark_registry": "SCIFACT_BUILDER_EXECUTOR_CONFIG",
        "submit_scifact_pipeline_state": "SCIFACT_BUILDER_EXECUTOR_CONFIG",
        "activate_scifact_benchmark_pack": "SCIFACT_ACTIVATOR_EXECUTOR_CONFIG",
        "seal_scifact_activation_evidence": "SCIFACT_AGGREGATOR_EXECUTOR_CONFIG",
    }


def test_scifact_kubernetes_tasks_use_separate_indexer_and_evaluation_identities() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_beir_scifact_live_benchmark.py"
    ).read_text(encoding="utf-8")

    assert "SCIFACT_INDEXER_WORKLOAD_SERVICE_ACCOUNT" in source
    assert "SCIFACT_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT" in source
    assert "SCIFACT_INDEXER_WORKLOAD_LABELS" in source
    assert "SCIFACT_EVALUATOR_WORKLOAD_LABELS" in source
    assert '"adapstory.com/serp-network-profile": "benchmark-indexer"' in source
    assert "service_account_name=SCIFACT_INDEXER_WORKLOAD_SERVICE_ACCOUNT" in source
    assert "service_account_name=SCIFACT_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT" in source


def test_scifact_evaluator_gets_only_its_evidence_and_governed_search_contract() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_beir_scifact_live_benchmark.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    evaluator = next(
        call
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "KubernetesPodOperator"
        and any(
            keyword.arg == "task_id"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "evaluate_scifact_live_gateway"
            for keyword in call.keywords
        )
    )
    evaluator_env = next(
        keyword.value for keyword in evaluator.keywords if keyword.arg == "env_vars"
    )
    assert isinstance(evaluator_env, ast.Call)
    assert isinstance(evaluator_env.func, ast.Name)
    assert evaluator_env.func.id == "scifact_evaluator_env_vars"
    assert "volumes=SCIFACT_EVALUATOR_WEB_IDENTITY_VOLUMES" in source
    assert "volume_mounts=SCIFACT_EVALUATOR_WEB_IDENTITY_VOLUME_MOUNTS" in source
    assert "def scifact_evaluator_env_vars()" in source
    evaluator_contract = source.split("def scifact_evaluator_env_vars()", 1)[1].split(
        "def build_scifact_plan_from_dag_run", 1
    )[0]
    assert "ADAPSTORY_SERP_NEO4J_PASSWORD" not in evaluator_contract
    assert "airflow-serp-evidence-store" not in evaluator_contract
    assert "return minio_web_identity_env_vars(_SCIFACT_EVALUATOR_ENV_NAMES)" in evaluator_contract


def test_scifact_kubernetes_tasks_use_only_their_required_projected_tokens() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_beir_scifact_live_benchmark.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert "bc21_authorized_minio_executor_config" in source
    assert "SCIFACT_BUILDER_EXECUTOR_CONFIG" in source

    kubernetes_tasks = {
        next(
            keyword.value.value
            for keyword in call.keywords
            if keyword.arg == "task_id"
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, str)
        ): call
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "KubernetesPodOperator"
    }
    acquisition_automount = next(
        keyword.value
        for keyword in kubernetes_tasks["index_scifact_live_dataset"].keywords
        if keyword.arg == "automount_service_account_token"
    )
    assert isinstance(acquisition_automount, ast.Constant)
    assert acquisition_automount.value is False

    evaluator_automount = next(
        keyword.value
        for keyword in kubernetes_tasks["evaluate_scifact_live_gateway"].keywords
        if keyword.arg == "automount_service_account_token"
    )
    assert isinstance(evaluator_automount, ast.Constant)
    assert evaluator_automount.value is False


def test_bc21_authorized_executor_projects_a_bounded_authorization_token() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_evidence_workload_identity.py"
    ).read_text(encoding="utf-8")

    assert 'BC21_WORKLOAD_TOKEN_FILE = "/var/run/secrets/adapstory/bc21-workload/token"' in source
    assert 'BC21_WORKLOAD_TOKEN_VOLUME_NAME = "bc21-workload-token"' in source
    assert 'BC21_WORKLOAD_TOKEN_AUDIENCE = "https://kubernetes.default.svc.cluster.local"' in source
    assert "def bc21_authorized_minio_executor_config(" in source
    assert 'name="ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH"' in source
    assert "audience=BC21_WORKLOAD_TOKEN_AUDIENCE" in source
    assert "automount_service_account_token=False" in source


def test_scifact_indexer_uses_minio_and_bc10_projected_identities() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_beir_scifact_live_benchmark.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    indexer = next(
        call
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Name)
        and call.func.id == "KubernetesPodOperator"
        and any(
            keyword.arg == "task_id"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "index_scifact_live_dataset"
            for keyword in call.keywords
        )
    )
    indexer_env = next(keyword.value for keyword in indexer.keywords if keyword.arg == "env_vars")
    assert isinstance(indexer_env, ast.Call)
    assert isinstance(indexer_env.func, ast.Name)
    assert indexer_env.func.id == "scifact_indexer_env_vars"
    assert "volumes=SCIFACT_INDEXER_RUNTIME_VOLUMES" in source
    assert "volume_mounts=SCIFACT_INDEXER_RUNTIME_VOLUME_MOUNTS" in source
    assert "*bc10_workload_volumes()" in source
    assert "*bc10_workload_volume_mounts()" in source
    indexer_contract = source.split("def scifact_indexer_env_vars()", 1)[1].split(
        "def scifact_evaluator_env_vars()", 1
    )[0]
    assert "return pipeline_runner_runtime_env_vars()" in indexer_contract
    assert "ADAPSTORY_OLLAMA_BASE_URL" not in source
    assert "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY" not in source
    assert "ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY" not in source


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
        "policyVersion": "source-approval@2026.07.1",
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
        assert "X-Adapstory-Trusted-Actor-Id" not in headers
        assert "X-Adapstory-Trusted-Tenant-Id" not in headers
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


def test_scifact_activation_receipts_are_written_as_distinct_compliance_snapshots() -> None:
    plan = build_scifact_benchmark_plan(
        {
            "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
            "generated_at": "2026-07-13T12:00:00Z",
        },
        bc21_base_url="http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    writes: list[dict[str, object]] = []

    def writer(**kwargs: object) -> dict[str, str]:
        writes.append(kwargs)
        return {
            "artifactETag": "evidence-etag",
            "artifactPath": str(kwargs["artifact_path"]),
            "artifactSha256": "d" * 64,
            "artifactType": str(kwargs["artifact_type"]),
            "artifactVersionId": "evidence-version-" + str(len(writes)),
            "objectLockMode": "COMPLIANCE",
        }

    receipt = seal_scifact_activation_evidence(
        plan,
        {
            "active_pack_version_id": "00000000-0000-4000-a000-000000000333",
            "activation": {"activationState": "active"},
            "approval": {"approvalState": "approved"},
            "workflow_selection": {"selectionState": "active"},
        },
        snapshot_writer=writer,
    )

    assert [write["artifact_path"] for write in writes] == [
        plan["artifact_paths"]["activation_receipt"],
        plan["artifact_paths"]["workflow_selection_receipt"],
    ]
    assert receipt["activation_snapshot"]["objectLockMode"] == "COMPLIANCE"
    assert receipt["selection_snapshot"]["objectLockMode"] == "COMPLIANCE"
