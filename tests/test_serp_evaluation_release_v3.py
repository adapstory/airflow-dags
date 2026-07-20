from __future__ import annotations

import io
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

import pytest
import rfc8785
from adapstory_serp_pipeline.benchmark.native_suite_scoring import suite_metric_profile
from adapstory_serp_pipeline.registry.evaluation_release_contract import (
    BENCHMARK_EXECUTION_SUBSTRATE_ROLE_CONTRACTS,
)

import dags.serp_eval_contracts as serp_eval_contracts
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    build_benchmark_improvement_wave_plan,
    build_d17_event_d6_plan,
    build_d17_event_d6_trigger_conf,
    build_model_catalog_promotion_plan,
    load_benchmark_pack_lifecycle_result_snapshot,
    load_governed_model_releases,
    load_model_catalog_promotion_snapshot,
    validate_d17_event_d6_airflow_run,
    write_model_catalog_promotion_receipt,
    write_paired_eval_request_artifact,
)

TENANT_ID = "00000000-0000-4000-a000-000000000001"
RESOURCE_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
BINDING_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96701"


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return rfc8785.dumps(payload)


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


def _benchmark_substrate_source_set_handle(
    substrate_id: int,
    payload: Mapping[str, Any],
    objects: dict[tuple[str, str, str], bytes],
    *,
    canonical_uri: bool = True,
    extra_handle_fields: Mapping[str, str] | None = None,
    omit_retain_until: bool = False,
) -> dict[str, str]:
    body = _canonical_bytes(payload)
    if canonical_uri:
        key = f"serp-evals/ci-benchmark-substrates-{substrate_id}/source-set.json"
    else:
        key = f"serp-evals/substrates-{substrate_id}/source-set.json"
    version_id = f"substrate-source-set-{substrate_id}"
    objects[("airflow-serp-evidence", key, version_id)] = body
    handle = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/{key}",
        "sha256": "sha256:" + sha256(body).hexdigest(),
        "versionId": version_id,
        **dict(extra_handle_fields or {}),
    }
    if omit_retain_until:
        handle.pop("retainUntil")
    return handle


def _release_pair(
    *,
    activation_status: str = "ready-for-evaluation",
    legacy_treatment: bool = False,
    same_treatment: bool = False,
    include_runtime_source_set: bool = True,
    runtime_source_set_schema: str = "BenchmarkExecutionSubstrateSourceSet/v7",
    different_runtime_source_set_handles: bool = False,
    canonical_runtime_source_set_uri: bool = True,
    runtime_source_set_extra_handle_fields: Mapping[str, str] | None = None,
    runtime_source_set_omit_retain_until: bool = False,
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

    def source_set_payload(substrate_id: int) -> dict[str, Any]:
        source_root = (
            "s3://airflow-serp-evidence/serp-evals/" f"ci-benchmark-substrates-{substrate_id}"
        )

        def source_evidence(relative_path: str) -> dict[str, str]:
            return {
                "objectLockMode": "COMPLIANCE",
                "retainUntil": "2027-07-15T00:00:00Z",
                "s3Uri": f"{source_root}/{relative_path}",
                "sha256": "sha256:"
                + sha256(f"{substrate_id}:{relative_path}".encode()).hexdigest(),
                "versionId": "substrate-"
                f"{substrate_id}-" + sha256(relative_path.encode()).hexdigest()[:16],
            }

        return {
            "checkoutProvenance": {
                "buildUrl": (
                    "https://jenkins.adapstory.com/job/serp-benchmark-sandbox-supply/"
                    f"{substrate_id}/"
                ),
                "commit": "d" * 40,
                "origin": "https://github.com/adapstory/Adapstory-GitOps.git",
                "pipelinePath": (
                    "infra/ci/jenkins/pipelines/serp-benchmark-sandbox-supply.jenkinsfile"
                ),
                "schema": "GitOpsCheckoutProvenance/v1",
                "tree": "e" * 40,
            },
            "ds1000BaseImageProvenanceEvidence": source_evidence(
                "base-images/ds1000/provenance.json"
            ),
            "ds1000DatasetProvenanceEvidence": source_evidence(
                "datasets/ds1000/simplified-provenance.json"
            ),
            "ds1000WheelhouseResolutionEvidence": source_evidence(
                "wheelhouses/ds1000/resolution.json"
            ),
            "schema": runtime_source_set_schema,
            "suites": [
                {
                    "roles": [
                        {
                            "evidence": source_evidence(f"roles/{file_name}"),
                            "role": role,
                        }
                    ],
                    "suiteId": suite_id,
                }
                for suite_id, role, file_name in BENCHMARK_EXECUTION_SUBSTRATE_ROLE_CONTRACTS
            ],
            "supplyAttestationsEvidence": source_evidence("supply-attestations.json"),
        }

    source_set_evidence = {
        "baseline": _benchmark_substrate_source_set_handle(
            1,
            source_set_payload(1),
            objects,
            canonical_uri=canonical_runtime_source_set_uri,
            extra_handle_fields=runtime_source_set_extra_handle_fields,
            omit_retain_until=runtime_source_set_omit_retain_until,
        ),
        "candidate": _benchmark_substrate_source_set_handle(
            2 if different_runtime_source_set_handles else 1,
            source_set_payload(2 if different_runtime_source_set_handles else 1),
            objects,
            canonical_uri=canonical_runtime_source_set_uri,
            extra_handle_fields=runtime_source_set_extra_handle_fields,
            omit_retain_until=runtime_source_set_omit_retain_until,
        ),
    }

    def runtime_terminal_binding(side: str) -> dict[str, str]:
        runtime_side = "baseline" if not different_runtime_source_set_handles else side
        build = 160 if runtime_side == "baseline" else 161
        draft: dict[str, Any] = {
            "contractVersion": "AirflowRuntimeBuildDraft/v1",
            "repository": "harbor.adapstory.com/adapstory/airflow",
            "tag": f"3.3.0-runtime-{runtime_side}",
            "digest": "sha256:" + ("b" if runtime_side == "baseline" else "c") * 64,
            "manifestMediaType": "application/vnd.oci.image.manifest.v1+json",
            "configMediaType": "application/vnd.oci.image.config.v1+json",
            "airflowDagsRef": "a" * 40,
            "serpMcpGatewayRef": "b" * 40,
            "serpPipelineContractSourceBundleSha256": "sha256:" + "8" * 64,
            "serpPipelineRef": "c" * 40,
            "serpContextBenchmarkRef": "d" * 40,
            "jenkinsBuildUrl": f"https://jenkins.adapstory.com/job/infra-build/{build}/",
        }
        if include_runtime_source_set:
            draft["benchmarkSubstrateSourceSetEvidence"] = source_set_evidence[runtime_side]
        draft_evidence = component(f"runtime-draft-{runtime_side}", draft)
        draft_transit_evidence = component(
            f"runtime-draft-transit-{runtime_side}",
            {"schema": "ArtifactSignatureAttestationReceipt/v2", "subject": draft_evidence},
        )
        terminal_attestation = {
            "schema": "AirflowRuntimeTerminalAttestation/v1",
            "runtimeBuildDraftEvidence": draft_evidence,
            "runtimeBuildDraftSha256": draft_evidence["sha256"],
            "gitopsCommit": "f" * 40,
            "jenkinsBuildUrl": draft["jenkinsBuildUrl"],
            "serpPipelineContractSourceBundleSha256": "sha256:" + "8" * 64,
            "terminalResult": "SUCCESS",
            "writtenAt": "2026-07-15T05:00:00Z",
        }
        terminal_evidence = component(
            f"runtime-terminal-attestation-{runtime_side}", terminal_attestation
        )
        terminal_transit_evidence = component(
            f"runtime-terminal-transit-{runtime_side}",
            {"schema": "ArtifactSignatureAttestationReceipt/v2", "subject": terminal_evidence},
        )
        return component(
            f"runtime-terminal-binding-{runtime_side}",
            {
                "schema": "RuntimeTerminalActivationBinding/v1",
                "runtimeBuildDraftEvidence": draft_evidence,
                "runtimeBuildDraftVaultTransitEvidence": draft_transit_evidence,
                "terminalBuildAttestationEvidence": terminal_evidence,
                "terminalBuildAttestationVaultTransitEvidence": terminal_transit_evidence,
            },
        )

    runtime_evidence = {side: runtime_terminal_binding(side) for side in ("baseline", "candidate")}

    def profiles(side: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for index, suite_id in enumerate(MANDATORY_SERP_BENCHMARK_SUITES):
            slug = suite_id.lower().replace(" ", "-")
            selected_chunks = 2 if side == "baseline" or same_treatment else index + 3
            treatment_side = "baseline" if side == "baseline" or same_treatment else "candidate"
            profile: dict[str, Any] = {
                "schema": "SuiteEvaluationProfile/v3",
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
            profile["mcpAuthorizationEvidence"] = component(
                "mcp-authorization-control",
                {
                    "actorId": TENANT_ID,
                    "capabilities": ["benchmark.execute", "benchmark.read"],
                    "workloadIdentity": "airflow-serp-eval-runner",
                },
            )
            profile["mcpRuntimeAdmissionEvidence"] = component(
                "mcp-runtime-admission-control",
                {
                    "authorityId": "serp-evaluation-admission",
                    "granularity": "tenant",
                    "limit": 100,
                    "reservedCapacity": 1,
                },
            )
            profile["mcpPolicyVersionEvidence"] = component(
                "mcp-policy-control",
                {
                    "correctivePolicyVersion": "serp-corrective-policy@2026.07.1",
                    "queryTransformBudgetPolicyId": "serp-query-budget@2026.07.1",
                    "queryTransformRouteId": "serp-query-route@2026.07.1",
                },
            )
            profile["mcpExecutionContextEvidence"] = component(
                f"mcp-execution-context-{side}-{slug}",
                {
                    "protocolVersion": "serp-mcp-runtime@2026.07.1",
                    "transport": "streamable-http",
                    "retrievalProfileSha256": profile["retrievalProfileEvidence"]["sha256"],
                    "rerankerProfileSha256": profile["rerankerProfileEvidence"]["sha256"],
                    "modelRouteSha256": profile["modelRouteEvidence"]["sha256"],
                },
            )
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
    release_authorities = {
        "baseline": {
            "canaryState": "passed",
            "modelId": "serp-all-nine-baseline-router@2026.07.2",
            "provider": "adapstory-model-gateway",
            "purpose": "serp-benchmark-baseline",
        },
        "candidate": {
            "canaryState": "passed",
            "modelId": "serp-all-nine-candidate-router@2026.07.3",
            "provider": "adapstory-model-gateway",
            "purpose": "serp-benchmark-candidate",
        },
    }
    for side in ("baseline", "candidate"):
        suite_profiles = profiles(side)
        profile_set_evidence = component(
            f"profile-set-{side}",
            {
                "schema": "SuiteEvaluationProfileSet/v3",
                "profileSetId": f"serp-{side}-profile-set-2026.07.1",
                "suiteProfiles": suite_profiles,
            },
        )
        core = {
            "schema": "EvaluationRelease/v4",
            "activationStatus": activation_status,
            "releaseId": f"serp-{side}-release-2026.07.1",
            "runtimeEvidence": runtime_evidence[side],
            "profileSetEvidence": profile_set_evidence,
            "releaseAuthority": release_authorities[side],
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
    metric_cells: list[dict[str, Any]] = []
    for suite_id in MANDATORY_SERP_BENCHMARK_SUITES:
        primary_metric = suite_metric_profile(suite_id)["primaryMetric"]
        assert isinstance(primary_metric, Mapping)
        metric_cell = {
            "aggregation": primary_metric["aggregation"],
            "maximumScore": primary_metric["maximumScore"],
            "metricFamily": primary_metric["metricFamily"],
            "metricId": primary_metric["metricId"],
            "referenceAuthority": "official",
            "referenceEvidence": component(
                f"evaluation-metric-reference-{suite_id}",
                {
                    "aggregation": primary_metric["aggregation"],
                    "maximumScore": primary_metric["maximumScore"],
                    "metricFamily": primary_metric["metricFamily"],
                    "metricId": primary_metric["metricId"],
                    "referenceAuthority": "official",
                    "referenceScore": 0.9,
                    "schema": "EvaluationMetricReference/v1",
                    "suiteId": suite_id,
                },
            ),
            "referenceScore": 0.9,
            "suiteId": suite_id,
        }
        metric_cells.append(metric_cell)
    reference_set_evidence = component(
        "evaluation-reference-set",
        {"metricReferences": metric_cells, "schema": "EvaluationReferenceSet/v1"},
    )
    evaluation_objective_attestation_evidence = _handle(
        "evaluation-objective-attestation",
        {"schema": "ArtifactSignatureAttestationReceipt/v2"},
        objects,
    )
    evaluation_objective = {
        "bootstrapConfidenceLevel": 0.95,
        "bootstrapSampleCount": 10_000,
        "metricCells": metric_cells,
        "minimumCandidateNormalizedLcb95": 0.9,
        "minimumCandidateNormalizedMean": 0.9,
        "minimumBaselineRetentionLcb95ToMean": 0.9,
        "minimumPairedNormalizedDeltaLcb95": 0.0,
        "objectiveId": "serp-all-nine-quality",
        "pairedRunCount": 5,
        "referenceAuthority": {
            "authorityId": "serp-all-nine-reference-set",
            "evidence": reference_set_evidence,
            "hardcoded": False,
            "kind": "official-harness",
            "referenceScore": 0.9,
            "scoreOrigin": "official-harness-result",
            "validationStatus": "passed",
            "version": reference_set_evidence["versionId"],
        },
        "referenceSetEvidence": reference_set_evidence,
        "referenceSetAttestationEvidence": evaluation_objective_attestation_evidence,
        "requiredConsecutiveAcceptedEvaluations": 3,
        "schema": "EvaluationObjective/v6",
    }
    evaluation_objective["version"] = (
        "serp-all-nine-quality-"
        + sha256(_canonical_bytes(evaluation_objective)).hexdigest()
        + ".v6"
    )
    evaluation_objective_evidence = _handle("evaluation-objective", evaluation_objective, objects)
    bundle = {
        "apiVersion": "serp.adapstory.ai/v2alpha1",
        "contractVersion": "serp-ci-evaluation-release-evidence/v8",
        "kind": "EvaluationReleaseEvidence",
        "metricCompatibilityMatrixEvidence": metric_matrix_evidence,
        "evaluationObjectiveEvidence": evaluation_objective_evidence,
        "evaluationObjectiveAttestationEvidence": (evaluation_objective_attestation_evidence),
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


def _evaluation_objective_v6_fixture() -> dict[str, Any]:
    reference_set_evidence = _reference("evaluation-reference-set", "4")
    cells: list[dict[str, Any]] = []
    for suite_id in MANDATORY_SERP_BENCHMARK_SUITES:
        primary_metric = suite_metric_profile(suite_id)["primaryMetric"]
        assert isinstance(primary_metric, Mapping)
        cells.append(
            {
                "aggregation": primary_metric["aggregation"],
                "maximumScore": primary_metric["maximumScore"],
                "metricFamily": primary_metric["metricFamily"],
                "metricId": primary_metric["metricId"],
                "referenceAuthority": "official",
                "referenceEvidence": _reference(
                    f"evaluation-reference-{suite_id}", format(len(cells), "x")
                ),
                "referenceScore": 0.9,
                "suiteId": suite_id,
            }
        )
    payload: dict[str, Any] = {
        "bootstrapConfidenceLevel": 0.95,
        "bootstrapSampleCount": 10_000,
        "metricCells": cells,
        "minimumCandidateNormalizedLcb95": 0.9,
        "minimumCandidateNormalizedMean": 0.9,
        "minimumBaselineRetentionLcb95ToMean": 0.9,
        "minimumPairedNormalizedDeltaLcb95": 0.0,
        "objectiveId": "serp-all-nine-quality",
        "pairedRunCount": 5,
        "referenceAuthority": {
            "authorityId": "serp-all-nine-reference-set",
            "evidence": reference_set_evidence,
            "hardcoded": False,
            "kind": "official-harness",
            "referenceScore": 0.9,
            "scoreOrigin": "official-harness-result",
            "validationStatus": "passed",
            "version": reference_set_evidence["versionId"],
        },
        "referenceSetEvidence": reference_set_evidence,
        "referenceSetAttestationEvidence": _reference("evaluation-reference-set-attestation", "5"),
        "requiredConsecutiveAcceptedEvaluations": 3,
        "schema": "EvaluationObjective/v6",
    }
    payload["version"] = (
        "serp-all-nine-quality-" + sha256(_canonical_bytes(payload)).hexdigest() + ".v6"
    )
    return payload


def _worm_object(
    objects: Mapping[tuple[str, str, str], bytes], evidence: Mapping[str, str]
) -> dict[str, Any]:
    bucket = "airflow-serp-evidence"
    key = evidence["s3Uri"].removeprefix(f"s3://{bucket}/")
    decoded = json.loads(objects[(bucket, key, evidence["versionId"])])
    if not isinstance(decoded, dict):
        raise AssertionError("WORM fixture payload must be a JSON object")
    return {str(field): value for field, value in decoded.items()}


def _rewrite_worm_object(
    objects: dict[tuple[str, str, str], bytes],
    evidence: dict[str, str],
    payload: Mapping[str, Any],
) -> None:
    body = _canonical_bytes(payload)
    bucket = "airflow-serp-evidence"
    key = evidence["s3Uri"].removeprefix(f"s3://{bucket}/")
    objects[(bucket, key, evidence["versionId"])] = body
    evidence["sha256"] = "sha256:" + sha256(body).hexdigest()


def _rewrite_release_side(
    bundle: dict[str, Any],
    objects: dict[tuple[str, str, str], bytes],
    *,
    side: str,
) -> None:
    release = bundle[f"{side}Release"]
    profile_set_evidence = release["profileSetEvidence"]
    profile_set = _worm_object(objects, profile_set_evidence)
    profile_set["suiteProfiles"] = release["suiteProfiles"]
    _rewrite_worm_object(objects, profile_set_evidence, profile_set)
    core = dict(release)
    core.pop("releaseDigest")
    release["releaseDigest"] = "sha256:" + sha256(_canonical_bytes(core)).hexdigest()
    _rewrite_worm_object(objects, bundle[f"{side}ReleaseEvidence"], release)


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


def test_d17_consumes_ci_v7_bundle_and_seals_governed_v7_promotion() -> None:
    bundle, objects = _release_pair()
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    assert plan.payload["baseline_release_evidence"] == bundle["baselineReleaseEvidence"]
    assert plan.payload["candidate_release_evidence"] == bundle["candidateReleaseEvidence"]
    assert (
        plan.payload["metric_compatibility_matrix_evidence"]
        == bundle["metricCompatibilityMatrixEvidence"]
    )
    assert plan.payload["evaluation_objective_evidence"] == bundle["evaluationObjectiveEvidence"]
    assert (
        plan.payload["evaluation_objective_attestation_evidence"]
        == bundle["evaluationObjectiveAttestationEvidence"]
    )
    assert "evaluation_release_evidence" not in plan.payload
    assert plan.payload["ci_evaluation_release_contract_version"] == (
        "serp-ci-evaluation-release-evidence/v8"
    )

    releases = load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))
    assert [
        profile["suiteId"] for profile in releases["candidateRelease"]["release"]["suiteProfiles"]
    ] == list(MANDATORY_SERP_BENCHMARK_SUITES)

    receipt = write_model_catalog_promotion_receipt(
        plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    payload = receipt["payload"]
    assert payload["schema"] == "EvaluationReleasePromotionReceipt/v8"
    assert payload["evaluationReleaseContractVersion"] == "serp-ci-evaluation-release-evidence/v8"
    assert payload["status"] == "approved-for-evaluation"
    assert payload["baselineRelease"] == {
        "evidence": bundle["baselineReleaseEvidence"],
        "releaseDigest": bundle["baselineRelease"]["releaseDigest"],
    }
    assert payload["candidateRelease"] == {
        "evidence": bundle["candidateReleaseEvidence"],
        "releaseDigest": bundle["candidateRelease"]["releaseDigest"],
    }
    assert payload["candidateReleaseAuthority"] == {
        **bundle["candidateRelease"]["releaseAuthority"],
        "evidence": bundle["candidateReleaseEvidence"],
        "releaseDigest": bundle["candidateRelease"]["releaseDigest"],
        "releaseId": bundle["candidateRelease"]["releaseId"],
    }
    assert (
        payload["metricCompatibilityMatrixEvidence"] == bundle["metricCompatibilityMatrixEvidence"]
    )
    assert payload["evaluationObjectiveEvidence"] == bundle["evaluationObjectiveEvidence"]
    assert (
        payload["evaluationObjectiveAttestationEvidence"]
        == bundle["evaluationObjectiveAttestationEvidence"]
    )
    assert receipt["promotionEvidence"]["retainUntil"] == "2027-07-15T00:00:00Z"
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in ("suiteProfiles", "replay", "ModelRelease/v1"):
        assert forbidden not in serialized


def test_d17_receipt_derives_one_strict_event_d6_then_native_d19_conf() -> None:
    bundle, objects = _release_pair()
    d17_plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(
        d17_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    receipt = write_model_catalog_promotion_receipt(
        d17_plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    source_run = {
        "dagId": "serp_model_catalog_promotion",
        "logicalDate": "2026-07-15T05:00:00Z",
        "runId": "manual__d17-release-20260715T050000Z",
        "runType": "manual",
    }

    event_conf = build_d17_event_d6_trigger_conf(d17_plan.to_canonical_json(), receipt, source_run)
    event_plan = build_d17_event_d6_plan(event_conf)

    assert event_conf == build_d17_event_d6_trigger_conf(
        d17_plan.to_canonical_json(), receipt, source_run
    )
    assert set(event_conf) == {
        "actor_id",
        "artifact_root_path",
        "eventD6RunId",
        "generated_at",
        "promotionEvidence",
        "registry_resource_id",
        "registry_resource_type",
        "schema",
        "sourceD17",
        "tenant_id",
    }
    assert event_conf["schema"] == "D17EventD6Trigger/v1"
    assert event_conf["sourceD17"] == {
        "dagId": "serp_model_catalog_promotion",
        "logicalDate": "2026-07-15T05:00:00Z",
        "operationId": d17_plan.payload["operation_id"],
        "promotionId": d17_plan.payload["promotion_id"],
        "runId": source_run["runId"],
        "runType": "manual",
    }
    assert event_conf["promotionEvidence"] == receipt["promotionEvidence"]
    assert (
        event_conf["promotionEvidence"]["s3Uri"]
        == d17_plan.payload["artifact_paths"]["promotion_receipt"]
    )

    assert event_plan.payload["dag_id"] == "serp_model_promotion_regression_suite"
    assert [task["task_id"] for task in d17_plan.payload["tasks"]] == [
        "validate_model_catalog_promotion_plan",
        "load_governed_model_releases",
        "write_model_catalog_promotion_receipt",
        "build_d17_event_d6_trigger_conf",
        "trigger_model_promotion_regression_suite",
    ]
    assert [task["task_id"] for task in event_plan.payload["tasks"]] == [
        "validate_d17_event_d6_plan",
        "trigger_benchmark_improvement_wave",
    ]
    assert event_plan.payload["event_d6_run_id"] == event_conf["eventD6RunId"]
    assert event_plan.payload["d19_trigger_run_id"] == (
        "event_d6_d19__" + event_conf["eventD6RunId"].removeprefix("event_d6__")
    )
    assert event_plan.payload["d19_trigger_conf"] == {
        "actor_id": d17_plan.payload["actor_id"],
        "artifact_root_path": d17_plan.payload["artifact_root_path"],
        "evaluation_release_promotion_evidence": receipt["promotionEvidence"],
        "generated_at": d17_plan.payload["generated_at"],
        "registry_resource_id": d17_plan.payload["registry_resource_id"],
        "registry_resource_type": d17_plan.payload["registry_resource_type"],
        "tenant_id": d17_plan.payload["tenant_id"],
    }
    d19_plan = build_benchmark_improvement_wave_plan(event_plan.payload["d19_trigger_conf"])
    assert d19_plan.payload["evaluation_release_promotion_evidence"] == receipt["promotionEvidence"]
    assert d19_plan.payload["generated_at"] == d17_plan.payload["generated_at"]

    admitted = validate_d17_event_d6_airflow_run(
        event_plan.to_canonical_json(),
        {
            "dagId": "serp_model_promotion_regression_suite",
            "logicalDate": d17_plan.payload["generated_at"],
            "runId": event_conf["eventD6RunId"],
            "runType": "manual",
        },
    )
    assert admitted == {
        "dagId": "serp_model_promotion_regression_suite",
        "logicalDate": d17_plan.payload["generated_at"],
        "runId": event_conf["eventD6RunId"],
        "runType": "manual",
    }


def test_d17_promotion_writer_uses_canonical_retain_until_not_executor_receipt_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, objects = _release_pair()
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))

    def canonical_writer(artifact_path: str, **kwargs: object) -> dict[str, str]:
        return {
            "artifactETag": "a" * 64,
            "artifactPath": artifact_path,
            "artifactSha256": "a" * 64,
            "artifactType": str(kwargs["artifact_type"]),
            "artifactVersionId": "canonical-writer-version-001",
            "contractVersion": "serp-airflow-artifact-writer/v1",
            "objectLockMode": "COMPLIANCE",
            "operationId": str(kwargs["operation_id"]),
            "retainUntil": "2027-07-15T00:00:00Z",
            "retentionDays": "365",
            "status": "written",
        }

    monkeypatch.setattr(serp_eval_contracts, "write_immutable_evidence_snapshot", canonical_writer)
    receipt = write_model_catalog_promotion_receipt(plan.to_canonical_json(), releases)

    assert receipt["promotionEvidence"]["retainUntil"] == "2027-07-15T00:00:00Z"


def test_d17_event_d6_rejects_mismatched_receipt_manual_scheduled_mixes_and_inline_d19() -> None:
    bundle, objects = _release_pair()
    d17_plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(
        d17_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    receipt = write_model_catalog_promotion_receipt(
        d17_plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    source_run = {
        "dagId": "serp_model_catalog_promotion",
        "logicalDate": "2026-07-15T05:00:00Z",
        "runId": "manual__d17-release-20260715T050000Z",
        "runType": "manual",
    }
    mismatched_receipt = dict(receipt)
    mismatched_receipt["promotionEvidence"] = {
        **receipt["promotionEvidence"],
        "s3Uri": "s3://airflow-serp-evidence/serp-evals/foreign/promotion.json",
    }

    with pytest.raises(ValueError, match="promotionEvidence artifact path does not match D17 plan"):
        build_d17_event_d6_trigger_conf(
            d17_plan.to_canonical_json(), mismatched_receipt, source_run
        )
    with pytest.raises(ValueError, match="D17 source runType must be manual"):
        build_d17_event_d6_trigger_conf(
            d17_plan.to_canonical_json(), receipt, {**source_run, "runType": "scheduled"}
        )

    event_conf = build_d17_event_d6_trigger_conf(d17_plan.to_canonical_json(), receipt, source_run)
    with pytest.raises(ValueError, match="inline D19 field is forbidden"):
        build_d17_event_d6_plan({**event_conf, "candidate_evaluation": {"score": 1.0}})
    for legacy_field in (
        "bc21_base_url",
        "benchmark_suite_inputs",
        "evaluation_release_promotion_evidence",
    ):
        with pytest.raises(ValueError, match="D17 event-D6 trigger fields are unsupported"):
            build_d17_event_d6_plan({**event_conf, legacy_field: "caller-controlled"})

    event_plan = build_d17_event_d6_plan(event_conf)
    with pytest.raises(ValueError, match="event D6 only admits trigger-created manual DagRuns"):
        validate_d17_event_d6_airflow_run(
            event_plan.to_canonical_json(),
            {
                "dagId": "serp_model_promotion_regression_suite",
                "logicalDate": d17_plan.payload["generated_at"],
                "runId": event_conf["eventD6RunId"],
                "runType": "scheduled",
            },
        )
    with pytest.raises(ValueError, match="event D6 DagRun runId does not match D17 trigger"):
        validate_d17_event_d6_airflow_run(
            event_plan.to_canonical_json(),
            {
                "dagId": "serp_model_promotion_regression_suite",
                "logicalDate": d17_plan.payload["generated_at"],
                "runId": "manual__caller-controlled",
                "runType": "manual",
            },
        )

    d19_conf = dict(event_plan.payload["d19_trigger_conf"])
    d19_conf["candidate_evaluation"] = {"score": 1.0}
    with pytest.raises(ValueError, match="inline D19 field is forbidden"):
        build_benchmark_improvement_wave_plan(d19_conf)


def test_d17_rejects_ci_v5_bundle_without_compatibility_fallback() -> None:
    bundle, _objects = _release_pair()
    bundle["contractVersion"] = "serp-ci-evaluation-release-evidence/v5"

    with pytest.raises(ValueError, match="contractVersion is unsupported"):
        build_model_catalog_promotion_plan(_promotion_conf(bundle))


def test_d17_rejects_terminal_draft_without_benchmark_substrate_source_set_evidence() -> None:
    bundle, objects = _release_pair(include_runtime_source_set=False)
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="AirflowRuntimeBuildDraft/v1"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_receipt_writer_requires_loaded_benchmark_substrate_source_set_evidence() -> None:
    bundle, objects = _release_pair()
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))
    releases["baselineRelease"]["release"].pop("benchmarkSubstrateSourceSetEvidence")

    with pytest.raises(ValueError, match="benchmarkSubstrateSourceSetEvidence"):
        write_model_catalog_promotion_receipt(
            plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
        )


def test_d17_rejects_runtime_source_set_outside_ci_benchmark_substrates_prefix() -> None:
    bundle, objects = _release_pair(canonical_runtime_source_set_uri=False)
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="ci-benchmark-substrates"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_runtime_source_set_with_unsupported_schema() -> None:
    bundle, objects = _release_pair(
        runtime_source_set_schema="BenchmarkExecutionSubstrateSourceSet/v2"
    )
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(
        ValueError, match="benchmarkSubstrateSourceSetEvidence schema is unsupported"
    ):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_tampered_runtime_source_set_bytes() -> None:
    bundle, objects = _release_pair()
    runtime_evidence = bundle["baselineRelease"]["runtimeEvidence"]
    binding = _worm_object(objects, runtime_evidence)
    runtime = _worm_object(objects, binding["runtimeBuildDraftEvidence"])
    source_set_evidence = runtime["benchmarkSubstrateSourceSetEvidence"]
    source_set_key = source_set_evidence["s3Uri"].removeprefix("s3://airflow-serp-evidence/")
    objects[("airflow-serp-evidence", source_set_key, source_set_evidence["versionId"])] = b"{}"
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="benchmarkSubstrateSourceSetEvidence.*SHA-256"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_runtime_source_set_handle_with_extra_fields() -> None:
    bundle, objects = _release_pair(
        runtime_source_set_extra_handle_fields={"unexpected": "unsupported"}
    )
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(
        ValueError, match="benchmarkSubstrateSourceSetEvidence fields are unsupported"
    ):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_pair_with_different_runtime_source_set_handles() -> None:
    bundle, objects = _release_pair(different_runtime_source_set_handles=True)
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="same benchmark substrate source set"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


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


def test_d17_rejects_legacy_profile_schema_and_missing_mcp_evidence() -> None:
    for mutation, expected_error in (
        ("legacy-schema", "schema is unsupported"),
        ("missing-mcp", "fields are unsupported"),
    ):
        bundle, objects = _release_pair()
        profile = bundle["candidateRelease"]["suiteProfiles"][0]
        if mutation == "legacy-schema":
            profile["schema"] = "SuiteEvaluationProfile/v2"
        else:
            profile.pop("mcpAuthorizationEvidence")
        _rewrite_release_side(bundle, objects, side="candidate")
        plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

        with pytest.raises(ValueError, match=expected_error):
            load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_unbound_or_unfrozen_mcp_context() -> None:
    bundle, objects = _release_pair()
    candidate_profile = bundle["candidateRelease"]["suiteProfiles"][0]
    context_evidence = candidate_profile["mcpExecutionContextEvidence"]
    context = _worm_object(objects, context_evidence)
    context["modelRouteSha256"] = "sha256:" + "0" * 64
    _rewrite_worm_object(objects, context_evidence, context)
    _rewrite_release_side(bundle, objects, side="candidate")
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="modelRouteSha256 is not component-bound"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))

    bundle, objects = _release_pair()
    candidate_profile = bundle["candidateRelease"]["suiteProfiles"][0]
    authorization_evidence = candidate_profile["mcpAuthorizationEvidence"]
    authorization = _worm_object(objects, authorization_evidence)
    authorization["workloadIdentity"] = "changed-evaluator-identity"
    candidate_profile["mcpAuthorizationEvidence"] = _handle(
        "candidate-mcp-authorization-control", authorization, objects
    )
    _rewrite_release_side(bundle, objects, side="candidate")
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="MCP fairness control is not frozen"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_noncanonical_corrective_policy_identifier() -> None:
    bundle, objects = _release_pair()
    candidate_profile = bundle["candidateRelease"]["suiteProfiles"][0]
    policy_evidence = candidate_profile["mcpPolicyVersionEvidence"]
    policy = _worm_object(objects, policy_evidence)
    policy["correctivePolicyVersion"] = "serp-corrective-policy/2026.07.1"
    candidate_profile["mcpPolicyVersionEvidence"] = _handle(
        "candidate-invalid-mcp-policy", policy, objects
    )
    _rewrite_release_side(bundle, objects, side="candidate")
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="versioned policy identifier"):
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


def test_d17_rejects_noncanonical_release_bytes_even_when_digest_matches() -> None:
    bundle, objects = _release_pair()
    release_evidence = bundle["baselineReleaseEvidence"]
    body = json.dumps(bundle["baselineRelease"], sort_keys=False).encode("utf-8")
    bucket = "airflow-serp-evidence"
    key = release_evidence["s3Uri"].removeprefix(f"s3://{bucket}/")
    objects[(bucket, key, release_evidence["versionId"])] = body
    release_evidence["sha256"] = "sha256:" + sha256(body).hexdigest()
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="canonical RFC 8785"):
        load_governed_model_releases(plan.to_canonical_json(), s3_client=_FakeS3(objects))


def test_d17_rejects_an_opaque_or_incomplete_evaluation_objective_v6() -> None:
    bundle, objects = _release_pair()
    objective_evidence = bundle["evaluationObjectiveEvidence"]
    objective = _worm_object(objects, objective_evidence)
    objective["metricCells"] = []
    versionless = dict(objective)
    versionless.pop("version")
    objective["version"] = (
        "serp-all-nine-quality-" + sha256(_canonical_bytes(versionless)).hexdigest() + ".v6"
    )
    body = _canonical_bytes(objective)
    bucket = "airflow-serp-evidence"
    key = objective_evidence["s3Uri"].removeprefix(f"s3://{bucket}/")
    objects[(bucket, key, objective_evidence["versionId"])] = body
    bundle["evaluationObjectiveEvidence"] = {
        **objective_evidence,
        "sha256": "sha256:" + sha256(body).hexdigest(),
    }
    plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))

    with pytest.raises(ValueError, match="exact canonical nine cells"):
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
            bundle["evaluationObjectiveEvidence"]["s3Uri"],
        )
    ]


def test_d19_rereads_the_v7_promotion_and_both_release_manifests() -> None:
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

    assert snapshot["promotion"]["schema"] == "EvaluationReleasePromotionReceipt/v8"
    assert snapshot["promotion"]["evaluationReleaseContractVersion"] == (
        "serp-ci-evaluation-release-evidence/v8"
    )
    assert (
        snapshot["promotion"]["baselineRelease"]["releaseDigest"]
        == bundle["baselineRelease"]["releaseDigest"]
    )
    assert (
        snapshot["promotion"]["candidateRelease"]["releaseDigest"]
        == bundle["candidateRelease"]["releaseDigest"]
    )
    assert snapshot["promotion"]["candidateReleaseAuthority"] == {
        **bundle["candidateRelease"]["releaseAuthority"],
        "evidence": bundle["candidateReleaseEvidence"],
        "releaseDigest": bundle["candidateRelease"]["releaseDigest"],
        "releaseId": bundle["candidateRelease"]["releaseId"],
    }
    assert (
        snapshot["promotion"]["metricCompatibilityMatrixEvidence"]
        == bundle["metricCompatibilityMatrixEvidence"]
    )
    assert (
        snapshot["promotion"]["evaluationObjectiveEvidence"]
        == bundle["evaluationObjectiveEvidence"]
    )
    assert (
        snapshot["promotion"]["evaluationObjectiveAttestationEvidence"]
        == bundle["evaluationObjectiveAttestationEvidence"]
    )


def test_d19_rejects_v5_promotion_receipt_without_compatibility_fallback() -> None:
    bundle, objects = _release_pair()
    d17_plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(
        d17_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    receipt = write_model_catalog_promotion_receipt(
        d17_plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    legacy_receipt = dict(receipt["payload"])
    legacy_receipt["schema"] = "EvaluationReleasePromotionReceipt/v5"
    legacy_evidence = _handle("legacy-v5-promotion", legacy_receipt, objects)
    conf = _d19_conf()
    conf["evaluation_release_promotion_evidence"] = legacy_evidence
    d19_plan = build_benchmark_improvement_wave_plan(conf)

    with pytest.raises(ValueError, match="D17 promotion receipt schema is unsupported"):
        load_model_catalog_promotion_snapshot(
            d19_plan.to_canonical_json(), s3_client=_FakeS3(objects)
        )


def test_d19_rejects_an_opaque_or_incomplete_evaluation_objective_v6() -> None:
    bundle, objects = _release_pair()
    d17_plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(
        d17_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    receipt = write_model_catalog_promotion_receipt(
        d17_plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    objective_evidence = bundle["evaluationObjectiveEvidence"]
    objective = _worm_object(objects, objective_evidence)
    objective["metricCells"] = []
    versionless = dict(objective)
    versionless.pop("version")
    objective["version"] = (
        "serp-all-nine-quality-" + sha256(_canonical_bytes(versionless)).hexdigest() + ".v6"
    )
    body = _canonical_bytes(objective)
    bucket = "airflow-serp-evidence"
    objective_key = objective_evidence["s3Uri"].removeprefix(f"s3://{bucket}/")
    objects[(bucket, objective_key, objective_evidence["versionId"])] = body
    tampered_objective_evidence = {
        **objective_evidence,
        "sha256": "sha256:" + sha256(body).hexdigest(),
    }
    tampered_receipt = json.loads(json.dumps(receipt["payload"]))
    tampered_receipt["evaluationObjectiveEvidence"] = tampered_objective_evidence
    tampered_receipt_evidence = _handle("opaque-objective-promotion", tampered_receipt, objects)
    conf = _d19_conf()
    conf["evaluation_release_promotion_evidence"] = tampered_receipt_evidence
    d19_plan = build_benchmark_improvement_wave_plan(conf)

    with pytest.raises(ValueError, match="exact canonical nine cells"):
        load_model_catalog_promotion_snapshot(
            d19_plan.to_canonical_json(), s3_client=_FakeS3(objects)
        )


def test_d19_rejects_duplicate_promotion_member_even_when_digest_matches() -> None:
    bundle, objects = _release_pair()
    d17_plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(
        d17_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    receipt = write_model_catalog_promotion_receipt(
        d17_plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    canonical_body = _canonical_bytes(receipt["payload"])
    body = b'{"schema":"EvaluationReleasePromotionReceipt/v8",' + canonical_body[1:]
    receipt_evidence = dict(receipt["promotionEvidence"])
    receipt_evidence["sha256"] = "sha256:" + sha256(body).hexdigest()
    bucket = "airflow-serp-evidence"
    receipt_key = receipt_evidence["s3Uri"].removeprefix(f"s3://{bucket}/")
    objects[(bucket, receipt_key, receipt_evidence["versionId"])] = body
    conf = _d19_conf()
    conf["evaluation_release_promotion_evidence"] = receipt_evidence
    d19_plan = build_benchmark_improvement_wave_plan(conf)

    with pytest.raises(ValueError, match="canonical RFC 8785"):
        load_model_catalog_promotion_snapshot(
            d19_plan.to_canonical_json(), s3_client=_FakeS3(objects)
        )


def test_d19_rejects_noncanonical_lifecycle_number_even_when_digest_matches() -> None:
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
    promotion_snapshot = load_model_catalog_promotion_snapshot(
        d19_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    promotion = promotion_snapshot["promotion"]
    lifecycle_result = {
        "schema": "BC21AllNineBenchmarkPackLifecycleResult/v1",
        "tenantId": TENANT_ID,
        "evaluationBindingId": BINDING_ID,
        "evaluationBindingEvidence": _reference("evaluation-binding", "b"),
        "bindingFingerprint": "sha256:" + "f" * 64,
        "expiresAt": "2026-07-15T07:10:00Z",
        "evaluationReleasePromotionEvidence": promotion_snapshot["promotionEvidence"],
        "baselineReleaseEvidence": promotion["baselineRelease"]["evidence"],
        "candidateReleaseEvidence": promotion["candidateRelease"]["evidence"],
        "baselineReleaseDigest": promotion["baselineRelease"]["releaseDigest"],
        "candidateReleaseDigest": promotion["candidateRelease"]["releaseDigest"],
        "packMaterialBindings": [
            {"suiteId": suite_id} for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
        "suiteExecutionBindings": [
            {"suiteId": suite_id} for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
        "indexedReceiptCount": 18,
        "productionActivationRequested": False,
    }
    body = _canonical_bytes(lifecycle_result).replace(
        b'"indexedReceiptCount":18', b'"indexedReceiptCount":1.8e1'
    )
    lifecycle_path = d19_plan.payload["artifact_paths"]["benchmark_pack_lifecycle_result"]
    lifecycle_version = "lifecycle-version-001"
    lifecycle_key = lifecycle_path.removeprefix(f"s3://{bucket}/")
    objects[(bucket, lifecycle_key, lifecycle_version)] = body
    lifecycle_pointer = {
        "lifecycleResultEvidence": {
            "artifactPath": lifecycle_path,
            "artifactSha256": "sha256:" + sha256(body).hexdigest(),
            "artifactVersionId": lifecycle_version,
        }
    }

    with pytest.raises(ValueError, match="canonical RFC 8785"):
        load_benchmark_pack_lifecycle_result_snapshot(
            d19_plan.to_canonical_json(),
            promotion_snapshot,
            lifecycle_pointer,
            s3_client=_FakeS3(objects),
        )


def test_d19_rejects_tampered_candidate_release_authority() -> None:
    bundle, objects = _release_pair()
    d17_plan = build_model_catalog_promotion_plan(_promotion_conf(bundle))
    releases = load_governed_model_releases(
        d17_plan.to_canonical_json(), s3_client=_FakeS3(objects)
    )
    receipt = write_model_catalog_promotion_receipt(
        d17_plan.to_canonical_json(), releases, snapshot_writer=_snapshot_writer
    )
    tampered = json.loads(json.dumps(receipt["payload"]))
    tampered["candidateReleaseAuthority"]["modelId"] = "caller-selected-model"
    tampered_evidence = _handle("tampered-promotion", tampered, objects)
    conf = _d19_conf()
    conf["evaluation_release_promotion_evidence"] = tampered_evidence
    d19_plan = build_benchmark_improvement_wave_plan(conf)

    with pytest.raises(ValueError, match="governed release authority"):
        load_model_catalog_promotion_snapshot(
            d19_plan.to_canonical_json(), s3_client=_FakeS3(objects)
        )


def test_d19_builds_scoreless_reference_only_paired_request_v5(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = build_benchmark_improvement_wave_plan(_d19_conf())
    baseline = _reference("release-baseline", "f")
    candidate = _reference("release-candidate", "1")
    promotion = {
        "promotionEvidence": plan.payload["evaluation_release_promotion_evidence"],
        "evaluationObjective": _evaluation_objective_v6_fixture(),
        "promotion": {
            "schema": "EvaluationReleasePromotionReceipt/v8",
            "evaluationReleaseContractVersion": "serp-ci-evaluation-release-evidence/v8",
            "promotionId": "all-nine-eval-2026-07-15",
            "tenantId": TENANT_ID,
            "registryResourceId": RESOURCE_ID,
            "registryResourceType": "workflow",
            "baselineRelease": {"evidence": baseline, "releaseDigest": "sha256:" + "2" * 64},
            "candidateRelease": {"evidence": candidate, "releaseDigest": "sha256:" + "3" * 64},
            "candidateReleaseAuthority": {
                "canaryState": "passed",
                "evidence": candidate,
                "modelId": "serp-all-nine-candidate-router@2026.07.3",
                "provider": "adapstory-model-gateway",
                "purpose": "serp-benchmark-candidate",
                "releaseDigest": "sha256:" + "3" * 64,
                "releaseId": "serp-candidate-release-2026.07.1",
            },
            "metricCompatibilityMatrixEvidence": _reference("metric-matrix", "d"),
            "evaluationObjectiveEvidence": _reference("evaluation-objective", "e"),
            "evaluationObjectiveAttestationEvidence": _reference(
                "evaluation-objective-attestation", "c"
            ),
        },
    }
    monkeypatch.setattr(
        "dags.serp_eval_contracts.write_immutable_evidence_snapshot", _snapshot_writer
    )
    mcp_objects: dict[tuple[str, str, str], bytes] = {}
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
        "packMaterialBindings": _d19_pack_material_bindings(mcp_objects),
        "suiteExecutionBindings": [
            {"suiteId": suite_id} for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
        "indexedReceiptCount": 18,
        "productionActivationRequested": False,
    }
    artifact = write_paired_eval_request_artifact(
        plan.to_canonical_json(),
        _catalog_snapshot(plan),
        promotion,
        lifecycle_result,
        s3_client=_FakeS3(mcp_objects),
    )
    request = artifact["payload"]

    assert request["schema"] == "PairedEvaluationRequest/v6"
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
        request["evaluationObjectiveEvidence"]
        == promotion["promotion"]["evaluationObjectiveEvidence"]
    )
    assert (
        request["evaluationObjectiveAttestationEvidence"]
        == promotion["promotion"]["evaluationObjectiveAttestationEvidence"]
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
    ("metric_compatibility_matrix_evidence", "evaluation_objective_evidence"),
)
def test_d19_rejects_caller_supplied_metric_authority(field: str) -> None:
    conf = _d19_conf()
    conf[field] = _reference(field, "9")

    with pytest.raises(ValueError, match="inline D19 field is forbidden"):
        build_benchmark_improvement_wave_plan(conf)


def _d19_pack_material_bindings(
    objects: dict[tuple[str, str, str], bytes],
) -> list[dict[str, object]]:
    bindings: list[dict[str, object]] = []
    for index, suite_id in enumerate(MANDATORY_SERP_BENCHMARK_SUITES):
        bindings.append(
            {
                "suiteId": suite_id,
                "baseline": _d19_pack_material_side(
                    suite_id,
                    "baseline",
                    index,
                    objects,
                ),
                "candidate": _d19_pack_material_side(
                    suite_id,
                    "candidate",
                    index,
                    objects,
                ),
            }
        )
    return bindings


def _d19_pack_material_side(
    suite_id: str,
    side: str,
    index: int,
    objects: dict[tuple[str, str, str], bytes],
) -> dict[str, object]:
    side_digit = "1" if side == "baseline" else "2"
    digest_digit = "a" if side == "baseline" else "b"
    profile_digit = "c" if side == "baseline" else "d"
    pack_id = f"00000000-0000-4000-a000-{side_digit}{index:011d}"
    pack_version_id = f"00000000-0000-4000-a000-{side_digit}{index + 100:011d}"
    profile_sha256 = "sha256:" + profile_digit * 64
    receipt_evidence = _handle(
        f"pack-receipt-{suite_id.casefold().replace(' ', '-')}-{side}",
        {
            "packId": pack_id,
            "packVersionId": pack_version_id,
            "profileSha256": profile_sha256,
            "schema": "BenchmarkPackBuildReceipt/v1",
            "suiteId": suite_id,
        },
        objects,
    )
    snapshot_id = f"pack-snapshot:v2:{suite_id.casefold().replace(' ', '-')}:{side}"
    snapshot_evidence = _handle(
        f"hermetic-snapshot-{suite_id.casefold().replace(' ', '-')}-{side}",
        {
            "contract_version": "SerpMcpHermeticPackSnapshot/v2",
            "pack_build_receipt_sha256": receipt_evidence["sha256"],
            "pack_id": pack_id,
            "pack_snapshot_id": snapshot_id,
            "pack_version_id": pack_version_id,
            "tenant_id": TENANT_ID,
        },
        objects,
    )
    runtime_binding_evidence = _handle(
        f"mcp-runtime-binding-{suite_id.casefold().replace(' ', '-')}-{side}",
        {
            "contractVersion": "BenchmarkPackMcpRuntimeBinding/v1",
            "mcpRuntimeContractVersion": "SerpMcpHermeticBenchmarkRuntime/v1",
            "packBuildReceiptEvidence": receipt_evidence,
            "packBuildReceiptSha256": receipt_evidence["sha256"],
            "packId": pack_id,
            "packSnapshotId": snapshot_id,
            "packSnapshotSha256": snapshot_evidence["sha256"],
            "packVersionId": pack_version_id,
            "snapshotContractVersion": "SerpMcpHermeticPackSnapshot/v2",
            "snapshotEvidence": snapshot_evidence,
        },
        objects,
    )
    return {
        "executionSubstrateSha256": "sha256:" + digest_digit * 64,
        "mcpRuntimeBindingEvidence": runtime_binding_evidence,
        "metricProfileSha256": profile_sha256,
        "officialHarnessIdentitySha256": "sha256:" + "e" * 64,
        "packBuildReceiptEvidence": receipt_evidence,
        "packBuildReceiptSha256": receipt_evidence["sha256"],
        "packId": pack_id,
        "packProfileEvidence": _reference(
            f"pack-profile-{suite_id.casefold().replace(' ', '-')}-{side}",
            profile_digit,
        ),
        "packProfileSha256": profile_sha256,
        "packVersionId": pack_version_id,
        "releaseManifestSha256": "sha256:" + "f" * 64,
        "side": side,
        "suiteId": suite_id,
    }


def _catalog_snapshot(plan: Any) -> dict[str, object]:
    return {
        "artifactPath": plan.payload["artifact_paths"]["benchmark_catalog"],
        "artifactSha256": "a" * 64,
        "artifactVersionId": "catalog-version-001",
        "blockingSuiteIds": [],
        "catalogReceiptPath": plan.payload["artifact_paths"]["benchmark_catalog_receipt"],
        "catalogReceiptSha256": "b" * 64,
        "catalogReceiptVersionId": "catalog-receipt-version-001",
        "catalogReceiptRetainUntil": "2027-07-15T00:00:00Z",
        "catalogRetainUntil": "2027-07-15T00:00:00Z",
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
        "retainUntil": "2027-07-15T00:00:00Z",
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
