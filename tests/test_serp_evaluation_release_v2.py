from __future__ import annotations

import io
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import pytest

import dags.serp_eval_contracts as serp_eval_contracts
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    build_benchmark_improvement_wave_plan,
    build_model_catalog_promotion_plan,
    load_governed_model_releases,
    load_model_catalog_promotion_snapshot,
    write_model_catalog_promotion_receipt,
    write_paired_eval_request_artifact,
)

TENANT_ID = "00000000-0000-4000-a000-000000000001"
RESOURCE_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
BINDING_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96701"


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _handle(
    name: str,
    payload: Mapping[str, Any],
    objects: dict[tuple[str, str, str], bytes],
) -> dict[str, str]:
    body = _canonical_bytes(payload)
    version_id = f"version-{name}"
    key = f"serp-evals/evaluation-releases/{name}.json"
    objects[("airflow-serp-evidence", key, version_id)] = body
    return {
        "s3Uri": f"s3://airflow-serp-evidence/{key}",
        "versionId": version_id,
        "sha256": "sha256:" + sha256(body).hexdigest(),
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
    }


def _release_pair(
    *,
    activation_status: str = "ready-for-evaluation",
    legacy_treatment: bool = False,
    same_treatment: bool = False,
) -> tuple[dict[str, Any], dict[tuple[str, str, str], bytes]]:
    objects: dict[tuple[str, str, str], bytes] = {}
    component_cache: dict[bytes, dict[str, str]] = {}

    def component(name: str, payload: Mapping[str, Any]) -> dict[str, str]:
        body = _canonical_bytes(payload)
        cached = component_cache.get(body)
        if cached is not None:
            return cached
        evidence = _handle(name, payload, objects)
        component_cache[body] = evidence
        return evidence

    runtime_evidence = {
        side: component(
            f"runtime-{side}",
            {
                "digest": "sha256:" + digest * 64,
                "jenkinsBuildUrl": f"https://jenkins.adapstory.com/job/infra-build/{build}/",
                "result": "SUCCESS",
                "sourceRevision": digest * 40,
            },
        )
        for side, digest, build in (("baseline", "b", 160), ("candidate", "c", 161))
    }

    def profiles(side: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for index, suite_id in enumerate(MANDATORY_SERP_BENCHMARK_SUITES):
            slug = suite_id.lower().replace(" ", "-")
            selected_chunks = 2 if side == "baseline" or same_treatment else index + 3
            treatment_side = "baseline" if side == "baseline" or same_treatment else "candidate"
            profile: dict[str, Any] = {
                "schema": "SuiteEvaluationProfile/v2",
                "suiteId": suite_id,
                "profileId": f"serp-{side}-{slug}",
                "profileVersion": "2026.07.1",
                "profileSha256": "sha256:"
                + sha256(f"{side}:{suite_id}:{selected_chunks}".encode()).hexdigest(),
                "evaluatorRunnerEvidence": component(
                    f"runner-{slug}", {"runnerVersion": f"official/{slug}@v1"}
                ),
                "officialScorerEvidence": component(
                    f"scorer-{slug}",
                    {
                        "bindingStatus": "verified",
                        "repositoryUrl": f"https://github.com/upstream/{slug}",
                        "revision": "d" * 40,
                        "entrypoint": f"official.{slug}.score",
                        "profile": "official",
                    },
                ),
                "retrievalProfileEvidence": component(
                    f"retrieval-{side}-{slug}-{selected_chunks}",
                    {
                        "profileVersion": "hybrid-rrf-profile@2026.07.3",
                        "algorithm": "hybrid-rrf",
                        "maxSelectedChunks": selected_chunks,
                    },
                ),
                "rerankerProfileEvidence": component(
                    f"reranker-{treatment_side}",
                    {
                        "profileVersion": f"term-reranker-{treatment_side}@2026.07.3",
                        "algorithm": f"deterministic-term-reranker-{treatment_side}",
                    },
                ),
                "modelRouteEvidence": component(
                    f"model-route-{treatment_side}",
                    {
                        "providerRouteId": f"in-cluster-{treatment_side}@2026.07.1",
                        "embeddingModelId": f"ollama:nomic-embed-text-{treatment_side}@2026.06.25",
                        "embeddingModelVersion": "e" * 64,
                        "judgeModelId": "deterministic-validator@2026.07.1",
                        "judgeModelVersion": "deterministic-validator@2026.07.1",
                        "promptTemplateVersion": "prompt@2026.07.1",
                    },
                ),
                "metricProfileEvidence": component(
                    f"metric-{slug}", {"profileVersion": f"{slug}-metrics@2026.07.1"}
                ),
                "partitionManifestEvidence": component(
                    f"partition-{slug}", {"manifestVersion": f"{slug}-partition@2026.07.1"}
                ),
                "executionEnvelopeEvidence": component(
                    "execution-envelope",
                    {
                        "seedSchedule": [1701, 1702, 1703, 1704, 1705],
                        "networkPolicy": "evaluation-read-only@2026.07.1",
                    },
                ),
                "packBuildProfileEvidence": component(
                    "pack-build", {"profileVersion": "native-benchmark-pack-build@2026.07.1"}
                ),
            }
            if side == "candidate":
                dimensions = [
                    {
                        "dimension": "retrievalProfile",
                        "changedFields": ["maxSelectedChunks"],
                    },
                    {
                        "dimension": "rerankerProfile",
                        "changedFields": ["algorithm", "profileVersion"],
                    },
                    {
                        "dimension": "modelRoute",
                        "changedFields": ["embeddingModelId", "providerRouteId"],
                    },
                ]
                profile["treatmentDelta"] = (
                    dimensions[0] if legacy_treatment else {"dimensions": dimensions}
                )
            result.append(profile)
        return result

    releases: dict[str, dict[str, Any]] = {}
    release_handles: dict[str, dict[str, str]] = {}
    for side in ("baseline", "candidate"):
        suite_profiles = profiles(side)
        profile_set_evidence = component(
            f"profile-set-{side}",
            {
                "schema": "SuiteEvaluationProfileSet/v2",
                "profileSetId": f"serp-{side}-profile-set-2026.07.1",
                "suiteProfiles": suite_profiles,
            },
        )
        core = {
            "schema": "EvaluationRelease/v2",
            "activationStatus": activation_status,
            "releaseId": f"serp-{side}-release-2026.07.1",
            "runtimeEvidence": runtime_evidence[side],
            "profileSetEvidence": profile_set_evidence,
            "suiteProfiles": suite_profiles,
        }
        release = {**core, "releaseDigest": "sha256:" + sha256(_canonical_bytes(core)).hexdigest()}
        releases[side] = release
        release_handles[side] = _handle(f"release-{side}", release, objects)

    metric_matrix_evidence = _handle(
        "metric-compatibility-matrix",
        {"schema": "MetricCompatibilityMatrix/v1", "suiteMetricFamilies": []},
        objects,
    )
    objective_evidence = _handle(
        "paired-evaluation-objective",
        {
            "metricCells": [],
            "schema": "PairedEvaluationObjectiveSpecification/v1",
        },
        objects,
    )
    bundle = {
        "apiVersion": "serp.adapstory.ai/v2alpha1",
        "contractVersion": "serp-ci-evaluation-release-evidence/v3",
        "kind": "EvaluationReleaseEvidence",
        "metricCompatibilityMatrixEvidence": metric_matrix_evidence,
        "objectiveSpecificationEvidence": objective_evidence,
        "operationId": "ci-evaluation-release-161",
        "registryResourceId": RESOURCE_ID,
        "registryResourceType": "workflow",
        "status": "sealed" if activation_status == "ready-for-evaluation" else activation_status,
        "tenantId": TENANT_ID,
        "baselineRelease": releases["baseline"],
        "candidateRelease": releases["candidate"],
        "baselineReleaseEvidence": release_handles["baseline"],
        "candidateReleaseEvidence": release_handles["candidate"],
    }
    return bundle, objects


def _promotion_conf(bundle: Mapping[str, Any]) -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-model-governance",
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "evaluation_release_evidence": dict(bundle),
        "generated_at": "2026-07-15T05:00:00Z",
        "promotion_id": "all-nine-eval-2026-07-15",
        "registry_resource_id": RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
    }


def _reference(name: str, digest: str = "a") -> dict[str, str]:
    return {
        "s3Uri": f"s3://airflow-serp-evidence/serp-evals/{name}.json",
        "versionId": f"version-{name}",
        "sha256": "sha256:" + digest * 64,
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
    }


def _d19_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "evaluation_release_promotion_evidence": _reference("d17-promotion", "c"),
        "generated_at": "2026-07-15T05:10:00Z",
        "registry_resource_id": RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
    }


def test_d17_consumes_ci_v3_bundle_and_seals_governed_v2_promotion() -> None:
    bundle, objects = _release_pair()
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    assert plan.payload["baseline_release_evidence"] == bundle["baselineReleaseEvidence"]
    assert plan.payload["candidate_release_evidence"] == bundle["candidateReleaseEvidence"]
    assert (
        plan.payload["metric_compatibility_matrix_evidence"]
        == bundle["metricCompatibilityMatrixEvidence"]
    )
    assert (
        plan.payload["objective_specification_evidence"] == bundle["objectiveSpecificationEvidence"]
    )
    assert "evaluation_release_evidence" not in plan.payload

    releases = load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))
    assert [
        profile["suiteId"] for profile in releases["candidateRelease"]["release"]["suiteProfiles"]
    ] == list(MANDATORY_SERP_BENCHMARK_SUITES)

    receipt = write_model_catalog_promotion_receipt(
        plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    payload = receipt["payload"]
    assert payload["schema"] == "EvaluationReleasePromotionReceipt/v3"
    assert payload["status"] == "approved-for-evaluation"
    assert payload["baselineRelease"] == {
        "evidence": bundle["baselineReleaseEvidence"],
        "releaseDigest": bundle["baselineRelease"]["releaseDigest"],
    }
    assert payload["candidateRelease"] == {
        "evidence": bundle["candidateReleaseEvidence"],
        "releaseDigest": bundle["candidateRelease"]["releaseDigest"],
    }
    assert (
        payload["metricCompatibilityMatrixEvidence"] == bundle["metricCompatibilityMatrixEvidence"]
    )
    assert payload["objectiveSpecificationEvidence"] == bundle["objectiveSpecificationEvidence"]
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in ("suiteProfiles", "replay", "ModelRelease/v1"):
        assert forbidden not in serialized


def test_d17_rejects_blocked_activation_and_missing_treatment() -> None:
    blocked_bundle, blocked_objects = _release_pair(
        activation_status="blocked-official-scorer-evidence"
    )
    blocked_plan = build_model_catalog_promotion_plan(_promotion_conf(blocked_bundle))
    with pytest.raises(ValueError, match="activationStatus must be ready-for-evaluation"):
        load_governed_model_releases(
            blocked_plan.to_canonical_json(), s3_client=_FakeS3(blocked_objects)
        )

    same_bundle, same_objects = _release_pair(same_treatment=True)
    same_plan = build_model_catalog_promotion_plan(_promotion_conf(same_bundle))
    with pytest.raises(ValueError, match="genuine treatment"):
        load_governed_model_releases(same_plan.to_canonical_json(), s3_client=_FakeS3(same_objects))


def test_d17_rejects_legacy_single_dimension_treatment_delta() -> None:
    bundle, objects = _release_pair(legacy_treatment=True)
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="treatmentDelta fields are unsupported"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_component_evidence_tampering() -> None:
    bundle, objects = _release_pair()
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    release = bundle["candidateRelease"]
    scorer = release["suiteProfiles"][0]["officialScorerEvidence"]
    bucket = "airflow-serp-evidence"
    key = scorer["s3Uri"].removeprefix(f"s3://{bucket}/")
    objects[(bucket, key, scorer["versionId"])] = b"{}"

    with pytest.raises(ValueError, match="SHA-256"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_uses_a_read_only_session_for_the_exact_ci_release_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, objects = _release_pair()
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    scopes: list[tuple[str, ...]] = []

    def read_client(*artifact_paths: str) -> _FakeS3:
        scopes.append(artifact_paths)
        return _FakeS3(objects)

    monkeypatch.setattr(serp_eval_contracts, "_s3_read_client", read_client)
    load_governed_model_releases(plan.to_canonical_json())

    assert scopes == [
        (
            bundle["baselineReleaseEvidence"]["s3Uri"],
            bundle["candidateReleaseEvidence"]["s3Uri"],
        )
    ]


def test_d19_rereads_the_v2_promotion_and_both_release_manifests() -> None:
    bundle, objects = _release_pair()
    d17_plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(
        d17_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    receipt = write_model_catalog_promotion_receipt(
        d17_plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    receipt_evidence = receipt["promotionEvidence"]
    receipt_body = _canonical_bytes(receipt["payload"])
    bucket = "airflow-serp-evidence"
    receipt_key = receipt_evidence["s3Uri"].removeprefix(f"s3://{bucket}/")
    objects[(bucket, receipt_key, receipt_evidence["versionId"])] = receipt_body

    conf = _d19_conf()
    conf["evaluation_release_promotion_evidence"] = receipt_evidence
    d19_plan = build_benchmark_improvement_wave_plan(conf)
    snapshot = load_model_catalog_promotion_snapshot(
        d19_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )

    assert snapshot["promotion"]["schema"] == "EvaluationReleasePromotionReceipt/v3"
    assert (
        snapshot["promotion"]["baselineRelease"]["releaseDigest"]
        == bundle["baselineRelease"]["releaseDigest"]
    )
    assert (
        snapshot["promotion"]["candidateRelease"]["releaseDigest"]
        == bundle["candidateRelease"]["releaseDigest"]
    )
    assert (
        snapshot["promotion"]["metricCompatibilityMatrixEvidence"]
        == bundle["metricCompatibilityMatrixEvidence"]
    )
    assert (
        snapshot["promotion"]["objectiveSpecificationEvidence"]
        == bundle["objectiveSpecificationEvidence"]
    )


def test_d19_builds_scoreless_reference_only_paired_request_v2(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = build_benchmark_improvement_wave_plan(_d19_conf())
    baseline = _reference("release-baseline", "f")
    candidate = _reference("release-candidate", "1")
    promotion = {
        "promotionEvidence": plan.payload["evaluation_release_promotion_evidence"],
        "promotion": {
            "schema": "EvaluationReleasePromotionReceipt/v3",
            "promotionId": "all-nine-eval-2026-07-15",
            "tenantId": TENANT_ID,
            "registryResourceId": RESOURCE_ID,
            "registryResourceType": "workflow",
            "baselineRelease": {"evidence": baseline, "releaseDigest": "sha256:" + "2" * 64},
            "candidateRelease": {"evidence": candidate, "releaseDigest": "sha256:" + "3" * 64},
            "metricCompatibilityMatrixEvidence": _reference("metric-matrix", "d"),
            "objectiveSpecificationEvidence": _reference("objective-spec", "e"),
        },
    }
    monkeypatch.setattr(
        "dags.serp_eval_contracts.write_immutable_evidence_snapshot", _snapshot_writer
    )
    lifecycle_result = {
        "schema": "BC21AllNineBenchmarkPackLifecycleResult/v1",
        "tenantId": TENANT_ID,
        "evaluationBindingId": BINDING_ID,
        "evaluationBindingEvidence": _reference("evaluation-binding", "b"),
        "bindingFingerprint": "sha256:" + "f" * 64,
        "expiresAt": "2026-07-15T07:10:00Z",
        "evaluationReleasePromotionEvidence": promotion["promotionEvidence"],
        "baselineReleaseEvidence": baseline,
        "candidateReleaseEvidence": candidate,
        "baselineReleaseDigest": promotion["promotion"]["baselineRelease"]["releaseDigest"],
        "candidateReleaseDigest": promotion["promotion"]["candidateRelease"]["releaseDigest"],
        "packMaterialBindings": [
            {"suiteId": suite_id} for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
        "suiteExecutionBindings": [
            {"suiteId": suite_id} for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
        "indexedReceiptCount": 18,
        "productionActivationRequested": False,
    }
    artifact = write_paired_eval_request_artifact(
        plan.to_canonical_json(), _catalog_snapshot(plan), promotion, lifecycle_result
    )
    request = artifact["payload"]

    assert request["schema"] == "PairedEvaluationRequest/v2"
    assert (
        request["evaluationReleasePromotionEvidence"]
        == _d19_conf()["evaluation_release_promotion_evidence"]
    )
    assert request["baselineReleaseEvidence"] == baseline
    assert request["candidateReleaseEvidence"] == candidate
    assert request["evaluationBindingId"] == BINDING_ID
    assert request["evaluationBindingEvidence"] == lifecycle_result["evaluationBindingEvidence"]
    assert (
        request["metricCompatibilityMatrixEvidence"]
        == promotion["promotion"]["metricCompatibilityMatrixEvidence"]
    )
    assert (
        request["objectiveSpecificationEvidence"]
        == promotion["promotion"]["objectiveSpecificationEvidence"]
    )
    forbidden = {
        "suiteProfiles",
        "suiteBindings",
        "profileId",
        "packId",
        "packVersionId",
        "metricValue",
        "score",
        "scores",
        "caseResults",
        "replay",
    }
    assert forbidden.isdisjoint(_all_keys(request))


@pytest.mark.parametrize(
    "field",
    (
        "candidate_id",
        "evaluation_binding_evidence",
        "evaluation_binding_id",
        "profileId",
        "packVersionId",
        "score",
        "caseResults",
    ),
)
def test_d19_rejects_inline_selection_or_scoring_fields(field: str) -> None:
    conf = _d19_conf()
    conf[field] = "caller-controlled"
    with pytest.raises(ValueError, match="inline D19 field is forbidden"):
        build_benchmark_improvement_wave_plan(conf)


@pytest.mark.parametrize(
    "field",
    ("metric_compatibility_matrix_evidence", "objective_specification_evidence"),
)
def test_d19_rejects_caller_supplied_metric_authority(field: str) -> None:
    conf = _d19_conf()
    conf[field] = _reference(field, "9")

    with pytest.raises(ValueError, match="inline D19 field is forbidden"):
        build_benchmark_improvement_wave_plan(conf)


def _catalog_snapshot(plan: Any) -> dict[str, object]:
    return {
        "artifactPath": plan.payload["artifact_paths"]["benchmark_catalog"],
        "artifactSha256": "a" * 64,
        "artifactVersionId": "catalog-version-001",
        "blockingSuiteIds": [],
        "catalogReceiptPath": plan.payload["artifact_paths"]["benchmark_catalog_receipt"],
        "catalogReceiptSha256": "b" * 64,
        "catalogReceiptVersionId": "catalog-receipt-version-001",
        "catalogStatus": "ready",
        "objectLockMode": "COMPLIANCE",
        "suiteSummary": [
            {
                "distributionRule": "internal-only",
                "executionStatus": "ready",
                "rightsStatus": "attested",
                "suiteId": suite_id,
            }
            for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
    }


def _snapshot_writer(artifact_path: object | None = None, **kwargs: object) -> dict[str, str]:
    payload = kwargs["payload"]
    assert isinstance(payload, Mapping)
    return {
        "artifactPath": str(artifact_path or kwargs["artifact_path"]),
        "artifactSha256": sha256(_canonical_bytes(payload)).hexdigest(),
        "artifactType": str(kwargs["artifact_type"]),
        "artifactVersionId": "written-version-001",
        "objectLockMode": "COMPLIANCE",
        "objectLockRetainUntil": "2027-07-15T00:00:00Z",
        "status": "written",
    }


def _all_keys(value: object) -> set[str]:
    if isinstance(value, Mapping):
        return set(value) | {key for item in value.values() for key in _all_keys(item)}
    if isinstance(value, list):
        return {key for item in value for key in _all_keys(item)}
    return set()


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
