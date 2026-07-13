"""Canonical orchestration contract for a live BEIR/SciFact retrieval run."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, uuid5

from dags.serp_eval_contracts import (
    _bc21_workload_authorization_headers,
    _fetch_https_bytes,
    build_evidence_artifact_paths,
    post_bc21_json,
    read_evidence_artifact,
    write_immutable_evidence_bytes_snapshot,
    write_immutable_evidence_snapshot,
)

SCIFACT_ARCHIVE_URL = (
    "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip"
)
SCIFACT_BENCHMARK_DAG_ID = "serp_beir_scifact_live_benchmark"
SCIFACT_BENCHMARK_CONTRACT_VERSION = "beir-scifact-airflow/v1"
SCIFACT_TENANT_ID = "00000000-0000-4000-a000-000000000001"
SCIFACT_ACTOR_ID = "airflow-serp-beir-scifact"
SCIFACT_GATEWAY_ACTOR_ID = "00000000-0000-4000-a000-000000000203"
SCIFACT_PACK_SLUG = "benchmark-beir-scifact"
SCIFACT_POLICY_VERSION = "source-approval@2026.07.1"
SCIFACT_WORKFLOW_SCOPE = {
    "tenant_mode": "benchmark",
    "tenant_scope": "private",
    "workflow_code": "search_context",
}


def build_scifact_benchmark_plan(
    conf: Mapping[str, Any],
    *,
    bc21_base_url: str,
) -> dict[str, Any]:
    """Build a no-input-spoofing plan for one immutable SciFact run."""

    _reject_unknown_conf(conf)
    generated_at = _required_datetime(conf, "generated_at")
    artifact_root_path = _required_s3_uri(conf, "artifact_root_path")
    if not isinstance(bc21_base_url, str) or not bc21_base_url.startswith("http"):
        raise ValueError("bc21_base_url must be an absolute HTTP URL")
    operation_id = (
        "beir-scifact-"
        + sha256(f"{SCIFACT_BENCHMARK_CONTRACT_VERSION}|{generated_at}".encode()).hexdigest()[:32]
    )
    return {
        "actor_id": SCIFACT_ACTOR_ID,
        "archive_source_url": SCIFACT_ARCHIVE_URL,
        "artifact_paths": build_evidence_artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("archive", "scifact.zip"),
                ("index_evidence", "scifact-indexing.json"),
                ("pipeline_state_receipt", "scifact-pipeline-state.json"),
                ("activation_receipt", "scifact-activation.json"),
                ("workflow_selection_receipt", "scifact-workflow-selection.json"),
                ("run_evidence", "scifact-live-run.json"),
            ),
        ),
        "bc21_base_url": bc21_base_url.rstrip("/"),
        "contract_version": SCIFACT_BENCHMARK_CONTRACT_VERSION,
        "dag_id": SCIFACT_BENCHMARK_DAG_ID,
        "generated_at": generated_at,
        "gateway_actor_id": SCIFACT_GATEWAY_ACTOR_ID,
        "operation_id": operation_id,
        "pack_slug": SCIFACT_PACK_SLUG,
        "tenant_id": SCIFACT_TENANT_ID,
        "workflow_scope": dict(SCIFACT_WORKFLOW_SCOPE),
    }


def materialize_scifact_archive(
    plan: Mapping[str, Any],
    *,
    fetch_bytes: Callable[[str], bytes] | None = None,
    snapshot_writer: Callable[..., Mapping[str, object]] | None = None,
) -> dict[str, str]:
    """Fetch the canonical distribution and store exact archive bytes in WORM S3."""

    _validate_plan(plan)
    archive = (fetch_bytes or _fetch_https_bytes)(_required_str(plan, "archive_source_url"))
    if not isinstance(archive, bytes) or not archive:
        raise ValueError("SciFact archive fetch returned no bytes")
    writer = snapshot_writer or write_immutable_evidence_bytes_snapshot
    snapshot = dict(
        writer(
            artifact_path=_artifact_path(plan, "archive"),
            artifact_type="beir_scifact_archive",
            operation_id=_required_str(plan, "operation_id"),
            payload=archive,
            content_type="application/zip",
        )
    )
    _validate_immutable_snapshot(snapshot, archive)
    return {
        "archiveETag": _required_str(snapshot, "artifactETag"),
        "archivePath": _required_str(snapshot, "artifactPath"),
        "archiveSha256": sha256(archive).hexdigest(),
        "archiveVersionId": _required_str(snapshot, "artifactVersionId"),
        "objectLockMode": _required_str(snapshot, "objectLockMode"),
        "sourceUrl": _required_str(plan, "archive_source_url"),
    }


def prepare_scifact_benchmark_registry(
    plan: Mapping[str, Any],
    archive_snapshot: Mapping[str, Any],
    *,
    list_resources: Callable[[str], list[Mapping[str, Any]]] | None = None,
    post_json: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Ensure the archive is indexed only in an isolated SciFact benchmark pack."""

    _validate_plan(plan)
    archive_path = _required_str(archive_snapshot, "archivePath")
    archive_sha256 = _required_sha256(archive_snapshot, "archiveSha256")
    archive_version_id = _required_str(archive_snapshot, "archiveVersionId")
    if _required_str(archive_snapshot, "objectLockMode") != "COMPLIANCE":
        raise ValueError("SciFact registry setup requires a COMPLIANCE archive snapshot")
    tenant_id = _required_str(plan, "tenant_id")
    actor_id = _required_str(plan, "actor_id")
    operation_id = _required_str(plan, "operation_id")
    base_url = _required_str(plan, "bc21_base_url")
    resources = list_resources or (
        lambda kind: _list_bc21_resources(base_url, tenant_id=tenant_id, kind=kind)
    )
    submit = post_json or post_bc21_json
    source_uri_hash = (
        "sha256:"
        + sha256(f"BeIR/scifact|{archive_sha256}|{archive_version_id}".encode()).hexdigest()
    )
    source_id = _resource_id_by_field(resources("sources"), "sourceUriHash", source_uri_hash)
    if source_id is None:
        source_body = {
            "accessScope": "internal",
            "dataClass": "PUBLIC",
            "displayName": "BEIR SciFact CC-BY-SA-4.0",
            "ownerActorId": actor_id,
            "sourceType": "website",
            "sourceUriHash": source_uri_hash,
        }
        source_id = _acceptance_resource_id(
            submit(
                base_url,
                "/api/bc-21/serp/v1/sources",
                body=source_body,
                headers=_mutation_headers(
                    tenant_id,
                    uuid5(NAMESPACE_URL, operation_id + "|scifact-source"),
                    source_body,
                    actor_id=actor_id,
                ),
                error_label="BEIR/SciFact source registration",
            )
        )
    pack_slug = _required_str(plan, "pack_slug")
    pack_id = _resource_id_by_field(resources("packs"), "slug", pack_slug)
    if pack_id is None:
        pack_body = {"ownerActorId": actor_id, "slug": pack_slug, "visibility": "private"}
        pack_id = _acceptance_resource_id(
            submit(
                base_url,
                "/api/bc-21/serp/v1/packs",
                body=pack_body,
                headers=_mutation_headers(
                    tenant_id,
                    uuid5(NAMESPACE_URL, operation_id + "|scifact-pack"),
                    pack_body,
                    actor_id=actor_id,
                ),
                error_label="BEIR/SciFact pack registration",
            )
        )
    version_material = "|".join((operation_id, archive_sha256, archive_version_id, pack_id))
    return {
        "archive_artifact_uri": archive_path,
        "archive_sha256": archive_sha256,
        "archive_version_id": archive_version_id,
        "fetch_run_id": str(uuid5(NAMESPACE_URL, version_material + "|fetch")),
        "idempotency_key": str(uuid5(NAMESPACE_URL, version_material + "|index-idempotency")),
        "pack_id": pack_id,
        "pack_version_id": str(uuid5(NAMESPACE_URL, version_material + "|pack-version")),
        "parse_run_id": str(uuid5(NAMESPACE_URL, version_material + "|parse")),
        "pipeline_run_id": str(uuid5(NAMESPACE_URL, version_material + "|pipeline")),
        "source_id": source_id,
        "tenant_id": tenant_id,
        "workflow_scope": dict(SCIFACT_WORKFLOW_SCOPE),
    }


def activate_scifact_benchmark_pack(
    plan: Mapping[str, Any],
    registry: Mapping[str, Any],
    pipeline_receipt: Mapping[str, Any],
    *,
    post_json: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Approve, activate, and select the isolated SciFact pack version."""

    _validate_plan(plan)
    tenant_id = _required_str(plan, "tenant_id")
    actor_id = _required_str(plan, "actor_id")
    base_url = _required_str(plan, "bc21_base_url")
    pack_id = _required_str(registry, "pack_id")
    pack_version_id = _required_str(registry, "pack_version_id")
    if _required_str(registry, "tenant_id") != tenant_id:
        raise ValueError("SciFact registry tenant must match benchmark plan")
    if pipeline_receipt.get("status") != "accepted":
        raise ValueError("SciFact pipeline receipt must be accepted before activation")
    receipt = pipeline_receipt.get("response")
    if not isinstance(receipt, Mapping):
        raise ValueError("SciFact pipeline receipt response must be an object")
    evidence_bundle_id = _required_str(receipt, "evidenceBundleId")
    evidence_seal_hash = _required_prefixed_sha256(receipt, "evidenceSealHash")
    if _required_str(receipt, "tenantId") != tenant_id:
        raise ValueError("SciFact pipeline receipt tenant does not match benchmark plan")
    if _required_str(receipt, "resourceId") != pack_id:
        raise ValueError("SciFact pipeline receipt resource does not match benchmark pack")
    indexed_run_id = _required_str(receipt, "runId")
    submit = post_json or post_bc21_json

    approval_body = {
        "actorId": actor_id,
        "dataClass": "PUBLIC",
        "freshnessState": "fresh",
        "licenseObligationState": "public_share_allowed",
        "packId": pack_id,
        "policyVersion": SCIFACT_POLICY_VERSION,
        "sourceType": "website",
        "trustState": "trusted",
    }
    approval = dict(
        submit(
            base_url,
            "/api/bc-21/serp/v1/governance/autonomous-approval-decisions",
            body=approval_body,
            headers=_mutation_headers(
                tenant_id,
                uuid5(NAMESPACE_URL, _required_str(plan, "operation_id") + "|scifact-approval"),
                approval_body,
                actor_id=actor_id,
            ),
            error_label="BEIR/SciFact autonomous approval",
        )
    )
    if _required_str(approval, "tenantId") != tenant_id:
        raise ValueError("SciFact benchmark approval tenant does not match benchmark plan")
    if _required_str(approval, "packId") != pack_id:
        raise ValueError("SciFact benchmark approval pack does not match registry")
    if _required_str(approval, "policyDecision") != "approved":
        raise ValueError("SciFact benchmark policy decision was not approved")
    if _required_str(approval, "approvalDecision") != "approve":
        raise ValueError("SciFact benchmark approval decision was not approve")
    if _required_str(approval, "approvalState") != "approved":
        raise ValueError("SciFact benchmark approval was not approved")

    activation_body = {
        "activationReasonCode": "beir_scifact_indexed_evidence_approved",
        "approvalRunId": _required_str(approval, "autonomousRunId"),
        "evidenceBundleId": evidence_bundle_id,
        "evidenceSealHash": evidence_seal_hash,
        "indexedRunId": indexed_run_id,
        "packVersionId": pack_version_id,
    }
    activation = dict(
        submit(
            base_url,
            f"/api/bc-21/serp/v1/packs/{pack_id}/publish-activations",
            body=activation_body,
            headers=_mutation_headers(
                tenant_id,
                uuid5(NAMESPACE_URL, _required_str(plan, "operation_id") + "|scifact-activation"),
                activation_body,
                actor_id=actor_id,
            ),
            error_label="BEIR/SciFact pack activation",
        )
    )
    if _required_str(activation, "tenantId") != tenant_id:
        raise ValueError("SciFact activation tenant does not match benchmark plan")
    if _required_str(activation, "packId") != pack_id:
        raise ValueError("SciFact activation pack does not match registry")
    if _required_str(activation, "packVersionId") != pack_version_id:
        raise ValueError("SciFact activation pack version does not match registry")
    if _required_str(activation, "activationState") != "active":
        raise ValueError("SciFact activation was not active")

    workflow_scope = _required_mapping(plan, "workflow_scope")
    selection_body = {
        "actorId": actor_id,
        "evidenceBundleId": evidence_bundle_id,
        "packId": pack_id,
        "policyBundleSha256": "sha256:"
        + sha256(
            "|".join(
                (
                    _required_sha256(registry, "archive_sha256"),
                    evidence_bundle_id,
                    pack_version_id,
                )
            ).encode()
        ).hexdigest(),
        "policyVersion": SCIFACT_POLICY_VERSION,
        "selectionReasonCode": "beir_scifact_live_benchmark",
        "tenantMode": _required_str(workflow_scope, "tenant_mode"),
        "tenantScope": _required_str(workflow_scope, "tenant_scope"),
        "workflowCode": _required_str(workflow_scope, "workflow_code"),
    }
    selection = dict(
        submit(
            base_url,
            "/api/bc-21/serp/v1/packs/workflow-selections",
            body=selection_body,
            headers=_mutation_headers(
                tenant_id,
                uuid5(NAMESPACE_URL, _required_str(plan, "operation_id") + "|scifact-selection"),
                selection_body,
                actor_id=actor_id,
            ),
            error_label="BEIR/SciFact workflow selection",
        )
    )
    if _required_str(selection, "tenantId") != tenant_id:
        raise ValueError("SciFact workflow selection tenant does not match benchmark plan")
    if _required_str(selection, "packId") != pack_id:
        raise ValueError("SciFact workflow selection pack does not match registry")
    if _required_str(selection, "selectionState") != "active":
        raise ValueError("SciFact workflow selection was not activated")
    return {
        "active_pack_version_id": pack_version_id,
        "activation": activation,
        "approval": approval,
        "workflow_selection": selection,
    }


def submit_scifact_pipeline_state(
    plan: Mapping[str, Any],
    registry: Mapping[str, Any],
    *,
    evidence_reader: Callable[[str, str], Mapping[str, Any]] | None = None,
    post_json: Callable[..., Mapping[str, Any]] | None = None,
    snapshot_writer: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Submit exactly the version-bound indexing transition and seal its receipt."""

    _validate_plan(plan)
    tenant_id = _required_str(plan, "tenant_id")
    registry_tenant_id = _required_str(registry, "tenant_id")
    if registry_tenant_id != tenant_id:
        raise ValueError("SciFact registry tenant must match benchmark plan")
    reader = evidence_reader or read_evidence_artifact
    evidence = dict(reader(_artifact_path(plan, "index_evidence"), "scifact_indexing_evidence"))
    if _required_str(evidence, "artifact_type") != "beir_scifact_indexing_evidence":
        raise ValueError("SciFact index evidence artifact type is invalid")
    if _required_str(evidence, "status") != "indexed":
        raise ValueError("SciFact index evidence must have indexed status")
    _require_matching_registry_identity(evidence, registry, tenant_id=tenant_id)
    archive_snapshot = _required_mapping(evidence, "archive_snapshot")
    if _required_str(archive_snapshot, "artifact_path") != _artifact_path(plan, "archive"):
        raise ValueError("SciFact index evidence archive path does not match plan")
    if _required_prefixed_sha256(archive_snapshot, "artifact_sha256") != (
        "sha256:" + _required_sha256(registry, "archive_sha256")
    ):
        raise ValueError("SciFact index evidence archive checksum does not match registry")
    if _required_str(archive_snapshot, "artifact_version_id") != _required_str(
        registry, "archive_version_id"
    ):
        raise ValueError("SciFact index evidence archive version does not match registry")
    if _required_str(archive_snapshot, "object_lock_mode") != "COMPLIANCE":
        raise ValueError("SciFact index evidence archive must use COMPLIANCE lock")

    submission = _required_mapping(evidence, "pipeline_state_submission")
    endpoint_path = _required_str(submission, "endpointPath")
    if endpoint_path != "/api/bc-21/serp/v1/runs/pipeline-state":
        raise ValueError("SciFact pipeline-state endpoint is invalid")
    body = dict(_required_mapping(submission, "body"))
    headers = {
        str(name): str(value) for name, value in _required_mapping(submission, "headers").items()
    }
    if _required_str(body, "resourceId") != _required_str(registry, "pack_id"):
        raise ValueError("SciFact pipeline-state resource must match benchmark pack")
    if _required_str(body, "packVersionId") != _required_str(registry, "pack_version_id"):
        raise ValueError("SciFact pipeline-state pack version must match registry")
    if _required_str(body, "sourceId") != _required_str(registry, "source_id"):
        raise ValueError("SciFact pipeline-state source must match registry")
    if _required_str(body, "status") != "indexed":
        raise ValueError("SciFact pipeline-state submission must be indexed")
    if headers.get("X-Adapstory-Tenant-Id") != tenant_id:
        raise ValueError("SciFact pipeline-state tenant header does not match plan")
    if headers.get("X-Adapstory-Actor-Id") != _required_str(plan, "actor_id"):
        raise ValueError("SciFact pipeline-state actor header does not match plan")

    response = dict(
        (post_json or post_bc21_json)(
            _required_str(plan, "bc21_base_url"),
            endpoint_path,
            body=body,
            headers=headers,
            error_label="BEIR/SciFact pipeline-state submission",
        )
    )
    if _required_str(response, "tenantId") != tenant_id:
        raise ValueError("SciFact pipeline-state response tenant does not match plan")
    if _required_str(response, "resourceId") != _required_str(registry, "pack_id"):
        raise ValueError("SciFact pipeline-state response resource does not match registry")
    if _required_str(response, "runId") != _required_str(body, "runId"):
        raise ValueError("SciFact pipeline-state response run does not match submission")
    if _required_str(response, "status") != "indexed":
        raise ValueError("SciFact pipeline-state response is not indexed")
    _required_str(response, "evidenceBundleId")
    _required_prefixed_sha256(response, "evidenceSealHash")

    receipt = {
        "archive_snapshot": dict(archive_snapshot),
        "contract_version": SCIFACT_BENCHMARK_CONTRACT_VERSION,
        "index_evidence_path": _artifact_path(plan, "index_evidence"),
        "operation_id": _required_str(plan, "operation_id"),
        "registry": {
            "pack_id": _required_str(registry, "pack_id"),
            "pack_version_id": _required_str(registry, "pack_version_id"),
            "source_id": _required_str(registry, "source_id"),
            "tenant_id": tenant_id,
        },
        "response": response,
        "status": "accepted",
    }
    snapshot = dict(
        (snapshot_writer or write_immutable_evidence_snapshot)(
            artifact_path=_artifact_path(plan, "pipeline_state_receipt"),
            artifact_type="beir_scifact_pipeline_state_receipt",
            operation_id=_required_str(plan, "operation_id"),
            payload=receipt,
        )
    )
    _validate_immutable_json_snapshot(snapshot, "beir_scifact_pipeline_state_receipt")
    return {**receipt, "snapshot": snapshot}


def seal_scifact_activation_evidence(
    plan: Mapping[str, Any],
    activation: Mapping[str, Any],
    *,
    snapshot_writer: Callable[..., Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Persist approval, activation, and server-resolved selection as WORM evidence."""

    _validate_plan(plan)
    if not isinstance(activation.get("activation"), Mapping):
        raise ValueError("SciFact activation response is required")
    if not isinstance(activation.get("approval"), Mapping):
        raise ValueError("SciFact approval response is required")
    if not isinstance(activation.get("workflow_selection"), Mapping):
        raise ValueError("SciFact workflow selection response is required")
    writer = snapshot_writer or write_immutable_evidence_snapshot
    payload = {
        "activation": dict(_required_mapping(activation, "activation")),
        "active_pack_version_id": _required_str(activation, "active_pack_version_id"),
        "approval": dict(_required_mapping(activation, "approval")),
        "contract_version": SCIFACT_BENCHMARK_CONTRACT_VERSION,
        "operation_id": _required_str(plan, "operation_id"),
        "workflow_selection": dict(_required_mapping(activation, "workflow_selection")),
    }
    activation_snapshot = dict(
        writer(
            artifact_path=_artifact_path(plan, "activation_receipt"),
            artifact_type="beir_scifact_activation_receipt",
            operation_id=_required_str(plan, "operation_id"),
            payload=payload,
        )
    )
    _validate_immutable_json_snapshot(activation_snapshot, "beir_scifact_activation_receipt")
    selection_snapshot = dict(
        writer(
            artifact_path=_artifact_path(plan, "workflow_selection_receipt"),
            artifact_type="beir_scifact_workflow_selection_receipt",
            operation_id=_required_str(plan, "operation_id"),
            payload={
                "contract_version": SCIFACT_BENCHMARK_CONTRACT_VERSION,
                "operation_id": _required_str(plan, "operation_id"),
                "workflow_selection": dict(_required_mapping(activation, "workflow_selection")),
            },
        )
    )
    _validate_immutable_json_snapshot(selection_snapshot, "beir_scifact_workflow_selection_receipt")
    return {
        **payload,
        "activation_snapshot": activation_snapshot,
        "selection_snapshot": selection_snapshot,
    }


def _validate_plan(plan: Mapping[str, Any]) -> None:
    if _required_str(plan, "dag_id") != SCIFACT_BENCHMARK_DAG_ID:
        raise ValueError("SciFact plan dag_id is invalid")
    if _required_str(plan, "contract_version") != SCIFACT_BENCHMARK_CONTRACT_VERSION:
        raise ValueError("SciFact plan contract_version is invalid")
    _required_datetime(plan, "generated_at")
    _required_str(plan, "operation_id")
    _required_str(plan, "tenant_id")
    _required_str(plan, "actor_id")
    _required_str(plan, "bc21_base_url")
    _artifact_path(plan, "archive")


def _validate_immutable_snapshot(snapshot: Mapping[str, object], archive: bytes) -> None:
    if _required_str(snapshot, "objectLockMode") != "COMPLIANCE":
        raise ValueError("SciFact archive must use COMPLIANCE object lock")
    if _required_str(snapshot, "artifactSha256") != sha256(archive).hexdigest():
        raise ValueError("SciFact archive snapshot SHA-256 does not match fetched bytes")
    if not _required_str(snapshot, "artifactPath").startswith("s3://"):
        raise ValueError("SciFact archive snapshot must be stored in S3")
    _required_str(snapshot, "artifactVersionId")


def _validate_immutable_json_snapshot(snapshot: Mapping[str, Any], artifact_type: str) -> None:
    if _required_str(snapshot, "artifactType") != artifact_type:
        raise ValueError("SciFact immutable receipt has an unexpected artifact type")
    if _required_str(snapshot, "objectLockMode") != "COMPLIANCE":
        raise ValueError("SciFact immutable receipt must use COMPLIANCE object lock")
    if not _required_str(snapshot, "artifactPath").startswith("s3://"):
        raise ValueError("SciFact immutable receipt must be stored in S3")
    _required_sha256(snapshot, "artifactSha256")
    _required_str(snapshot, "artifactVersionId")
    _required_str(snapshot, "artifactETag")


def _require_matching_registry_identity(
    evidence: Mapping[str, Any],
    registry: Mapping[str, Any],
    *,
    tenant_id: str,
) -> None:
    required_pairs = (
        ("tenant_id", tenant_id),
        ("source_id", _required_str(registry, "source_id")),
        ("pack_id", _required_str(registry, "pack_id")),
        ("pack_version_id", _required_str(registry, "pack_version_id")),
    )
    for field_name, expected_value in required_pairs:
        if _required_str(evidence, field_name) != expected_value:
            raise ValueError(f"SciFact index evidence {field_name} does not match registry")


def _list_bc21_resources(
    base_url: str,
    *,
    tenant_id: str,
    kind: str,
) -> list[Mapping[str, Any]]:
    path_by_kind = {"packs": "/packs", "sources": "/sources"}
    try:
        path = path_by_kind[kind]
    except KeyError as exc:
        raise ValueError(f"unsupported BC-21 resource list: {kind}") from exc
    request = Request(
        base_url.rstrip("/") + "/api/bc-21/serp/v1" + path,
        headers={
            "Accept": "application/json",
            "X-Adapstory-Tenant-Id": tenant_id,
            **_bc21_workload_authorization_headers(),
        },
    )
    try:
        with urlopen(request, timeout=10.0) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except OSError as exc:
        raise ValueError(f"BC-21 {kind} listing failed") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"BC-21 {kind} listing must be an object")
    items = payload.get("items")
    if not isinstance(items, list) or not all(isinstance(item, Mapping) for item in items):
        raise ValueError(f"BC-21 {kind} listing must contain items")
    return [dict(item) for item in items]


def _resource_id_by_field(
    resources: list[Mapping[str, Any]],
    field_name: str,
    expected_value: str,
) -> str | None:
    matching = [resource for resource in resources if resource.get(field_name) == expected_value]
    if not matching:
        return None
    if len(matching) != 1:
        raise ValueError(f"BC-21 has ambiguous resource identity for {field_name}")
    identifier_field = "packId" if field_name == "slug" else "sourceId"
    return _required_str(matching[0], identifier_field)


def _acceptance_resource_id(payload: Mapping[str, Any]) -> str:
    return _required_str(payload, "resourceId")


def _mutation_headers(
    tenant_id: str,
    idempotency_key: object,
    body: Mapping[str, Any],
    *,
    actor_id: str,
) -> dict[str, str]:
    return {
        "X-Adapstory-Actor-Id": actor_id,
        "X-Adapstory-Tenant-Id": tenant_id,
        "X-Fingerprint": "sha256:"
        + sha256(
            json.dumps(body, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest(),
        "X-Idempotency-Key": str(idempotency_key),
    }


def _artifact_path(plan: Mapping[str, Any], name: str) -> str:
    paths = plan.get("artifact_paths")
    if not isinstance(paths, Mapping):
        raise ValueError("SciFact plan artifact_paths must be an object")
    return _required_str(paths, name)


def _reject_unknown_conf(conf: Mapping[str, Any]) -> None:
    unknown = set(conf) - {"artifact_root_path", "generated_at"}
    if unknown:
        raise ValueError("unsupported SciFact benchmark config: " + ", ".join(sorted(unknown)))


def _required_datetime(value: Mapping[str, Any], field_name: str) -> str:
    raw = _required_str(value, field_name)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _required_s3_uri(value: Mapping[str, Any], field_name: str) -> str:
    uri = _required_str(value, field_name).rstrip("/")
    if not uri.startswith("s3://"):
        raise ValueError(f"{field_name} must be an s3:// URI")
    return uri


def _required_sha256(value: Mapping[str, Any], field_name: str) -> str:
    digest = _required_str(value, field_name)
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"{field_name} must be a SHA-256 hex digest")
    return digest


def _required_prefixed_sha256(value: Mapping[str, Any], field_name: str) -> str:
    digest = _required_str(value, field_name)
    if not digest.startswith("sha256:"):
        raise ValueError(f"{field_name} must be a sha256-prefixed digest")
    _required_sha256({field_name: digest.removeprefix("sha256:")}, field_name)
    return digest


def _required_mapping(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    nested = value.get(field_name)
    if not isinstance(nested, Mapping):
        raise ValueError(f"{field_name} is required")
    return nested


def _required_str(value: Mapping[str, object], field_name: str) -> str:
    nested = value.get(field_name)
    if not isinstance(nested, str) or not nested.strip():
        raise ValueError(f"{field_name} is required")
    return nested
