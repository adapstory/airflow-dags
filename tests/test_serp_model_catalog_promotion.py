from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    build_benchmark_improvement_wave_plan,
    build_model_catalog_promotion_plan,
    load_governed_model_releases,
    load_model_catalog_promotion_snapshot,
    write_model_catalog_promotion_receipt,
)

TENANT_ID = "00000000-0000-4000-a000-000000000001"
RESOURCE_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"


def _immutable_evidence(path_suffix: str) -> dict[str, str]:
    return {
        "artifactPath": (
            f"s3://airflow-serp-evidence/serp-evals/model-releases/{path_suffix}.json"
        ),
        "artifactSha256": "sha256:" + "a" * 64,
        "artifactVersionId": f"version-{path_suffix}",
    }


def _promotion_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-model-governance",
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "baseline_release_evidence": _immutable_evidence("baseline"),
        "candidate_release_evidence": _immutable_evidence("candidate"),
        "generated_at": "2026-07-15T05:00:00Z",
        "promotion_id": "public-docs-reranker-eval-2026-07-15",
        "registry_resource_id": RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
    }


def _d19_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "generated_at": "2026-07-15T05:10:00Z",
        "improvement_spec_id": "improve-public-retrieval-reranker-v2",
        "max_benchmark_runs": 12,
        "model_promotion_evidence": _immutable_evidence("d17-promotion"),
        "registry_resource_id": RESOURCE_ID,
        "registry_resource_type": "workflow",
        "rollback_policy_ref": "policy://rollback/last-validated-baseline@v1",
        "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
        "tenant_id": TENANT_ID,
    }


def test_d17_plan_allows_only_version_bound_release_evidence() -> None:
    plan = build_model_catalog_promotion_plan(_promotion_conf())

    assert plan.payload["dag_id"] == "serp_model_catalog_promotion"
    assert plan.payload["baseline_release_evidence"] == _immutable_evidence("baseline")
    assert plan.payload["candidate_release_evidence"] == _immutable_evidence("candidate")
    assert set(plan.payload["artifact_paths"]) == {"airflow_plan", "promotion_receipt"}
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_model_catalog_promotion_plan",
        "load_governed_model_releases",
        "write_model_catalog_promotion_receipt",
        "notify_governance_eval_surfaces",
    ]

    invalid = _promotion_conf()
    invalid["candidate_release_evidence"] = {
        "artifactPath": "s3://airflow-serp-evidence/model-releases/candidate.json",
        "artifactSha256": "sha256:" + "a" * 64,
    }
    with pytest.raises(ValueError, match="artifactVersionId"):
        build_model_catalog_promotion_plan(invalid)


def test_d19_requires_d17_promotion_evidence_and_rejects_legacy_selection_fields() -> None:
    plan = build_benchmark_improvement_wave_plan(_d19_conf())

    assert plan.payload["model_promotion_evidence"] == _immutable_evidence("d17-promotion")
    assert "baseline_run_id" not in plan.payload
    assert "candidate_id" not in plan.payload
    assert "replay_context" not in plan.payload

    invalid = _d19_conf()
    invalid["candidate_id"] = "untrusted-candidate"
    with pytest.raises(ValueError, match="legacy D19 selection field"):
        build_benchmark_improvement_wave_plan(invalid)


def test_d17_reloads_exact_worm_release_manifests_before_sealing_receipt() -> None:
    baseline = _release_manifest("baseline-reranker", "baseline-run", "reranker@2026.07.1")
    candidate = _release_manifest("candidate-reranker", "candidate-run", "reranker@2026.07.2")
    conf = _promotion_conf()
    conf["baseline_release_evidence"] = {
        **_immutable_evidence("baseline"),
        "artifactSha256": "sha256:" + sha256(baseline).hexdigest(),
    }
    conf["candidate_release_evidence"] = {
        **_immutable_evidence("candidate"),
        "artifactSha256": "sha256:" + sha256(candidate).hexdigest(),
    }
    plan = build_model_catalog_promotion_plan(conf)
    client = _FakeS3(
        {
            (
                "airflow-serp-evidence",
                "serp-evals/model-releases/baseline.json",
                "version-baseline",
            ): baseline,
            (
                "airflow-serp-evidence",
                "serp-evals/model-releases/candidate.json",
                "version-candidate",
            ): candidate,
        }
    )

    releases = load_governed_model_releases(plan.to_canonical_json(), s3_client=client)
    assert releases["baselineRelease"]["release"]["releaseId"] == "baseline-reranker"
    assert releases["candidateRelease"]["release"]["releaseId"] == "candidate-reranker"

    written: list[dict[str, object]] = []

    def writer(**kwargs: object) -> dict[str, str]:
        written.append(dict(kwargs))
        payload = kwargs["payload"]
        assert isinstance(payload, dict)
        return {
            "artifactPath": str(kwargs["artifact_path"]),
            "artifactSha256": sha256(
                json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
            ).hexdigest(),
            "artifactType": str(kwargs["artifact_type"]),
            "artifactVersionId": "promotion-version",
            "objectLockMode": "COMPLIANCE",
            "status": "written",
        }

    receipt = write_model_catalog_promotion_receipt(
        plan.to_canonical_json(), releases, snapshot_writer=writer
    )

    assert receipt["payload"]["contractVersion"] == "serp-model-catalog-promotion/v1"
    assert receipt["payload"]["status"] == "approved-for-evaluation"
    assert receipt["payload"]["baselineRelease"]["release"]["evaluationRunId"] == "baseline-run"
    assert receipt["payload"]["candidateRelease"]["release"]["evaluationRunId"] == "candidate-run"
    assert written[0]["artifact_type"] == "serp_model_catalog_promotion_receipt"

    receipt_bytes = json.dumps(receipt["payload"], separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    d19_conf = _d19_conf()
    d19_conf["model_promotion_evidence"] = {
        "artifactPath": "s3://airflow-serp-evidence/serp-evals/model-releases/d17-promotion.json",
        "artifactSha256": "sha256:" + sha256(receipt_bytes).hexdigest(),
        "artifactVersionId": "d17-promotion-version",
    }
    d19_plan = build_benchmark_improvement_wave_plan(d19_conf)
    d19_client = _FakeS3(
        {
            **client._objects,
            (
                "airflow-serp-evidence",
                "serp-evals/model-releases/d17-promotion.json",
                "d17-promotion-version",
            ): receipt_bytes,
        }
    )

    promotion = load_model_catalog_promotion_snapshot(
        d19_plan.to_canonical_json(), s3_client=d19_client
    )

    promoted_releases = promotion["promotion"]
    assert promoted_releases["baselineRelease"]["release"]["releaseId"] == "baseline-reranker"
    assert promoted_releases["candidateRelease"]["release"]["releaseId"] == "candidate-reranker"


def _release_manifest(release_id: str, run_id: str, reranker_profile: str) -> bytes:
    payload = {
        "apiVersion": "serp.adapstory.ai/v1alpha1",
        "component": "reranker-profile-public-docs",
        "contractVersion": "serp-model-release/v1",
        "evaluationRunId": run_id,
        "kind": "ModelRelease",
        "model": {
            "catalogEntryId": "model-catalog://serp/judge-serp-rubric@2026.07.1",
            "modelId": "judge-serp-rubric",
            "modelVersion": "judge@2026.07.1",
        },
        "registryResourceId": RESOURCE_ID,
        "registryResourceType": "workflow",
        "releaseId": release_id,
        "replay": {
            "featureFlags": ["serp.d19.dry_run"],
            "guardrailBundleVersion": "guardrails@2026.07.1",
            "judgeModelId": "judge-serp-rubric",
            "judgeModelVersion": "judge@2026.07.1",
            "judgePromptTemplateVersion": "judge-template@2026.07.1",
            "policyBundleVersion": "policy@2026.07.1",
            "providerRouteId": "llm-gateway://eval/judge-serp-rubric",
            "rerankerProfileVersion": reranker_profile,
            "retrievalProfileVersion": "hybrid@2026.07.1",
        },
        "runtime": {
            "imageDigest": "sha256:" + "b" * 64,
            "sourceRevision": "c" * 40,
        },
        "status": "approved-for-evaluation",
        "tenantId": TENANT_ID,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


class _FakeS3:
    def __init__(self, objects: dict[tuple[str, str, str], bytes]) -> None:
        self._objects = objects

    def head_object(self, **kwargs: str) -> dict[str, object]:
        key = (kwargs["Bucket"], kwargs["Key"], kwargs["VersionId"])
        assert key in self._objects
        return {
            "ObjectLockMode": "COMPLIANCE",
            "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=365),
            "VersionId": kwargs["VersionId"],
        }

    def get_object(self, **kwargs: str) -> dict[str, object]:
        key = (kwargs["Bucket"], kwargs["Key"], kwargs["VersionId"])
        return {"Body": io.BytesIO(self._objects[key])}
