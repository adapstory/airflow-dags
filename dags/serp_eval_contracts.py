from __future__ import annotations

import importlib
import json
import logging
import math
import os
import re
import subprocess
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from time import perf_counter, sleep
from typing import Any, NoReturn, cast
from urllib.error import HTTPError, URLError
from urllib.parse import ParseResult, parse_qsl, unquote, urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen
from uuid import NAMESPACE_URL, UUID, uuid5

import rfc8785

from dags.public_docs_crawler import CrawlResponse, crawl_public_docs
from dags.serp_public_docs_seed_catalog import (
    PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH,
    STACK_INVENTORY_SOURCE_PATH,
    p0_public_docs_sources,
)

_LOG = logging.getLogger(__name__)

MANDATORY_SERP_BENCHMARK_SUITES = (
    "APIBench",
    "ARES",
    "BEIR",
    "CodeRAG-Bench",
    "RAGBench",
    "RepoQA",
    "SWE-bench Verified",
    "cwd-benchmark-data",
    "rusBEIR",
)
_D19_CODE_SANDBOX_SUITES = frozenset({"CodeRAG-Bench", "SWE-bench Verified"})
SERP_NORMALIZED_GATE_FLOOR = 0.90
GATEWAY_CLI_MODULE = "adapstory_serp_mcp_gateway.airflow_eval_cli"
GATEWAY_CLI_PYTHON = "python"
PIPELINE_CLI_MODULE = "adapstory_serp_pipeline.orchestration.seed_refresh_cli"
PIPELINE_PUBLISH_ACTIVATION_CLI_MODULE = "adapstory_serp_pipeline.registry.publish_activation_cli"
PIPELINE_RETIRED_PACK_CLEANUP_CLI_MODULE = (
    "adapstory_serp_pipeline.orchestration.retired_pack_cleanup_cli"
)

_RESOURCE_TYPES = frozenset({"pack", "tenant", "workflow"})
_GATEWAY_CLI_CONTRACT_VERSION = "serp-airflow-gateway-cli-bridge/v1"
_PIPELINE_CLI_CONTRACT_VERSION = "serp-airflow-pipeline-cli-bridge/v1"
_AIRFLOW_ARTIFACT_CONTRACT_VERSION = "serp-airflow-artifact-writer/v1"
_EVAL_CONTRACT_VERSION = "2026.07.2"
_METRIC_COMPATIBILITY_CONTRACT_VERSION = "serp-suite-metric-compatibility/v1"
_CI_EVALUATION_RELEASE_CONTRACT_VERSION = "serp-ci-evaluation-release-evidence/v5"
_EVALUATION_RELEASE_SCHEMA = "EvaluationRelease/v3"
_EVALUATION_RELEASE_PROMOTION_SCHEMA = "EvaluationReleasePromotionReceipt/v5"
_PAIRED_EVALUATION_REQUEST_SCHEMA = "PairedEvaluationRequest/v5"
_PAIRED_EVALUATION_RECEIPT_CONTRACT_VERSION = "serp-paired-eval-receipt/v9"
_PAIRED_EVALUATION_VERIFICATION_EVIDENCE_SCHEMA = "PairedEvaluationVerificationEvidence/v1"
_PAIRED_EVALUATION_FINAL_RECEIPT_PURPOSE = "serp-paired-evaluation-final-receipt"
_D19_RUN_HISTORY_OBSERVATION_PURPOSE = "serp-d19-run-history-observation"
_PAIRED_EVALUATION_ATTESTATION_PURPOSES = {
    "evaluationObjective": "serp-evaluation-objective",
    "evaluationReferenceSet": "serp-evaluation-reference-set",
    "executionManifest": "serp-evaluation-execution-manifest",
}
_PAIRED_EVALUATION_PURPOSE_TRANSIT_KEYS = {
    "serp-evaluation-objective": "serp-evaluation-authority",
    "serp-evaluation-reference-set": "serp-evaluation-authority",
    "serp-evaluation-execution-manifest": "serp-evaluation-runtime",
    _PAIRED_EVALUATION_FINAL_RECEIPT_PURPOSE: "serp-evaluation-runtime",
    _D19_RUN_HISTORY_OBSERVATION_PURPOSE: "serp-d19-history-observation",
}
_BENCHMARK_CATALOG_PACK_ACTIVATION_SCHEMA = "BenchmarkCatalogPackActivation/v1"
_MODEL_PROMOTION_DAG_ID = "serp_model_catalog_promotion"
_D19_DAG_ID = "serp_benchmark_improvement_wave"
_D19_VERIFICATION_EVIDENCE_MAX_BYTES = 16_000_000
_SCHEDULED_D6_DAG_ID = "serp_nightly_regression_suite"
_SCHEDULED_D6_RECEIPT_SCHEMA = "ScheduledD6RegressionReceipt/v1"
_D19_RUN_HISTORY_OBSERVATION_SCHEMA = "D19RunHistoryObservation/v1"
_D19_RUN_HISTORY_OBSERVER_SERVICE_ACCOUNT = "airflow-serp-d19-history-observer"
_D19_RUN_HISTORY_OBSERVER_NAMESPACE = "airflow"
_SCHEDULED_D6_PRIOR_STREAK_LENGTH = 3
_EVALUATION_PROFILE_EVIDENCE_FIELDS = (
    "evaluatorRunnerEvidence",
    "officialScorerEvidence",
    "retrievalProfileEvidence",
    "rerankerProfileEvidence",
    "modelRouteEvidence",
    "metricProfileEvidence",
    "partitionManifestEvidence",
    "executionEnvelopeEvidence",
    "packBuildProfileEvidence",
)
_EVALUATION_TREATMENT_EVIDENCE_FIELDS = {
    "retrievalProfile": "retrievalProfileEvidence",
    "rerankerProfile": "rerankerProfileEvidence",
    "modelRoute": "modelRouteEvidence",
}
_EVALUATION_RELEASE_AUTHORITIES = {
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
_BENCHMARK_METRIC_FAMILIES = ("retrieval", "answer-quality", "citation", "policy")
_STRICT_PRIMARY_RETRIEVAL_METRICS = frozenset({"MRR@10", "nDCG@10"})
_STRICT_PAIRED_RUN_COUNT = 5
_ALLOWED_DATASET_DISTRIBUTION_RULES = {
    "internal-only",
    "internal-only-no-redistribution",
    "no-redistribution",
    "public-share-allowed",
    "review-required",
    "snippets-only",
}
_UNATTESTED_LICENSE_MARKERS = ("pending", "unknown", "noassertion", "unlicensed")
_DATASET_RIGHTS_STATUSES = frozenset({"attested", "rights-unverified"})
_RIGHTS_UNVERIFIED_DISTRIBUTION_RULE = "internal-only-no-redistribution"
_CATALOG_EXECUTION_STATUSES = frozenset(
    {
        "corpus-evidence-blocked",
        "execution-substrate-blocked",
        "ready",
        "rights-policy-blocked",
    }
)
_FORBIDDEN_INLINE_D19_FIELDS = frozenset(
    {
        "baseline_run_id",
        "candidate_id",
        "candidate_evaluation",
        "candidateEvaluation",
        "candidate_run_id",
        "evaluation_binding_evidence",
        "evaluation_binding_id",
        "feature_flags",
        "guardrail_bundle_version",
        "judge_model_id",
        "judge_model_version",
        "judge_prompt_template_version",
        "model_catalog_entry_id",
        "model_governance",
        "policy_bundle_version",
        "provider_route_id",
        "replay_context",
        "reranker_profile_version",
        "retrieval_profile_version",
        "caseResults",
        "evaluationObjectiveEvidence",
        "evaluationObjectiveAttestationEvidence",
        "evaluation_objective_evidence",
        "evaluation_objective_attestation_evidence",
        "metric",
        "metric_compatibility_matrix_evidence",
        "metricValue",
        "objectiveSpecificationEvidence",
        "objective_specification_evidence",
        "packId",
        "packVersionId",
        "profileId",
        "score",
        "scores",
        "selected_suite_ids",
        "suiteBindings",
        "suiteProfiles",
    }
)
_BENCHMARK_NAMESPACE = UUID("018f5e13-2d73-7a77-a052-8d1bcbf96599")
_PUBLIC_DOCS_NAMESPACE = UUID("018f5e13-2d73-7a77-a052-8d1bcbf96600")
_PUBLIC_DOCS_EXECUTABLE_SOURCE_TYPES = frozenset({"git", "openapi", "pdf", "website"})
_PUBLIC_DOCS_DATA_CLASSES = frozenset({"PUBLIC", "INTERNAL_EXTERNAL_OK"})
_PUBLIC_DOCS_DISTRIBUTION_RULES = frozenset({"cite-and-cache", "cite-only", "internal-cache-only"})
_PUBLIC_DOCS_FRESHNESS_STATUSES = frozenset(
    {"failed", "indexed", "never_indexed", "partial_failure", "quarantined"}
)
_PUBLIC_DOCS_INDEX_MODES = frozenset({"evidence-only", "live"})
_PUBLIC_DOCS_EMBEDDING_MODES = frozenset({"deterministic-dev", "live-gateway"})
_PUBLIC_DOCS_LOCALE_PATH_SEGMENT = re.compile(r"^[a-z]{2}(?:-[a-z0-9]{2,8})?$", re.IGNORECASE)
_PUBLIC_DOCS_DEFAULT_TENANT_ID = "00000000-0000-4000-a000-000000000001"
_PUBLIC_DOCS_DEFAULT_PACK_ID = "00000000-0000-4000-a000-000000000201"
_PUBLIC_DOCS_DEFAULT_PACK_VERSION_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
_PUBLIC_DOCS_DEFAULT_ACTOR_ID = "airflow-serp-public-docs-acquisition"
_PUBLIC_DOCS_DEFAULT_SEARCH_SERVE_ACTOR_ID = "00000000-0000-4000-a000-000000000203"
_PUBLIC_DOCS_SEARCH_SERVE_SMOKE_SUBJECT_ID = "00000000-0000-4000-a000-000000000202"
_PUBLIC_DOCS_DEFAULT_ARTIFACT_ROOT = "/var/opt/adapstory/serp-public-docs-refresh"
_PUBLIC_DOCS_STACK_INVENTORY_PATH = STACK_INVENTORY_SOURCE_PATH
_PUBLIC_DOCS_SITEMAP_FETCH_TIMEOUT_SECONDS = 8
_PUBLIC_DOCS_CRAWLER_FETCH_ATTEMPTS = 3
_PUBLIC_DOCS_CRAWLER_RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_HUGGINGFACE_PROXY_CONNECT_TIMEOUT_SECONDS = 30.0
_HUGGINGFACE_PROXY_READ_TIMEOUT_SECONDS = 120.0
_HUGGINGFACE_PROXY_WRITE_TIMEOUT_SECONDS = 30.0
_HUGGINGFACE_PROXY_POOL_TIMEOUT_SECONDS = 30.0
_PUBLIC_DOCS_MAX_SITEMAP_INDEX_CHILDREN = 3
_PUBLIC_DOCS_MAX_OPTIONAL_FRONTIER_SOURCES_ENV = (
    "ADAPSTORY_SERP_PUBLIC_DOCS_MAX_OPTIONAL_FRONTIER_SOURCES"
)
_PUBLIC_DOCS_MAX_OPTIONAL_FRONTIER_PER_SEED_ENV = (
    "ADAPSTORY_SERP_PUBLIC_DOCS_MAX_OPTIONAL_FRONTIER_PER_SEED"
)
_PUBLIC_DOCS_DEFAULT_MAX_OPTIONAL_FRONTIER_SOURCES = 96
_PUBLIC_DOCS_DEFAULT_MAX_OPTIONAL_FRONTIER_PER_SEED = 4
_PUBLIC_DOCS_CRAWLER_DISCOVERY_WORKERS_ENV = "ADAPSTORY_SERP_PUBLIC_DOCS_CRAWLER_WORKERS"
_PUBLIC_DOCS_DEFAULT_CRAWLER_DISCOVERY_WORKERS = 6
_PUBLIC_DOCS_MAX_CRAWLER_DISCOVERY_WORKERS = 16
PUBLIC_DOCS_MAX_SEED_COUNT = 128
PUBLIC_DOCS_MAX_SEED_EVIDENCE_BYTES = 2_000_000
PUBLIC_DOCS_MAX_TOTAL_SEED_EVIDENCE_BYTES = 16_000_000
PUBLIC_DOCS_MAX_COMPACT_PLAN_BYTES = 256_000
PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES = 16_000_000
PUBLIC_DOCS_MAX_XCOM_BYTES = 16_000
_PUBLIC_DOCS_AIRFLOW_PLAN_HANDLE_SCHEMA = "PublicDocsAirflowPlanHandle/v1"
_PUBLIC_DOCS_AIRFLOW_PLAN_SNAPSHOT_SCHEMA = "PublicDocsAirflowPlanSnapshot/v1"
_PUBLIC_DOCS_SEED_EVIDENCE_SCHEMA = "PublicDocsSeedEvidence/v1"
_PUBLIC_DOCS_TASK_ARTIFACT_HANDLE_SCHEMA = "PublicDocsTaskArtifactHandle/v1"
_ARTIFACT_ROOT_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_ROOT"
_PUBLIC_DOCS_SEARCH_SERVE_BASE_URL_ENV = "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL"
_PUBLIC_DOCS_SEARCH_SERVE_ACTOR_ID_ENV = "ADAPSTORY_SERP_SEARCH_SERVE_ACTOR_ID"
_PUBLIC_DOCS_SEARCH_SERVE_DEFAULT_BASE_URL = (
    "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
)
_PUBLIC_DOCS_INDEX_MODE_ENV = "ADAPSTORY_SERP_PUBLIC_DOCS_INDEX_MODE"
_PUBLIC_DOCS_EMBEDDING_MODE_ENV = "ADAPSTORY_SERP_PUBLIC_DOCS_EMBEDDING_MODE"
_PUBLIC_DOCS_QDRANT_COLLECTION_ENV = "ADAPSTORY_SERP_PUBLIC_DOCS_QDRANT_COLLECTION"
_PUBLIC_DOCS_OPENSEARCH_INDEX_ENV = "ADAPSTORY_SERP_PUBLIC_DOCS_OPENSEARCH_INDEX"
_PUBLIC_DOCS_NEO4J_DATABASE_ENV = "ADAPSTORY_SERP_PUBLIC_DOCS_NEO4J_DATABASE"
_PUBLIC_DOCS_SOURCE_PROXY_URL_ENV = "ADAPSTORY_SERP_SOURCE_PROXY_URL"
_BC21_BASE_URL_ENV = "ADAPSTORY_SERP_BC21_BASE_URL"
_BC21_SERVICE_ACCOUNT_TOKEN_PATH_ENV = "ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH"
_NIGHTLY_REGRESSION_RUNTIME_ENV = {
    "actor_id": "ADAPSTORY_SERP_D6_ACTOR_ID",
    "evaluation_release_promotion_evidence": (
        "ADAPSTORY_SERP_D6_EVALUATION_RELEASE_PROMOTION_EVIDENCE"
    ),
    "registry_resource_id": "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_ID",
    "registry_resource_type": "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_TYPE",
    "tenant_id": "ADAPSTORY_SERP_D6_TENANT_ID",
}
_LEGACY_NIGHTLY_PRIOR_VERIFICATION_ENV = (
    "ADAPSTORY_SERP_D6_PRIOR_PAIRED_EVALUATION_VERIFICATION_EVIDENCE"
)
_NIGHTLY_REGRESSION_SCHEDULE_PROBE_AT = "2026-01-01T00:00:00Z"
_DEFAULT_SERVICE_ACCOUNT_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
_PUBLIC_DOCS_DEFAULT_QDRANT_COLLECTION = "serp_vectors_dev"
_PUBLIC_DOCS_DEFAULT_OPENSEARCH_INDEX = "serp_lexical_dev"
_PUBLIC_DOCS_DEFAULT_NEO4J_DATABASE = "serp_graph_dev"
_PUBLIC_DOCS_RETRIEVAL_GOLDEN_MIN_CASES = 30
_PUBLIC_DOCS_RETRIEVAL_GOLDEN_MAX_FRESHNESS_HOURS = 24
_PUBLIC_DOCS_RETRIEVAL_GOLDEN_P95_SLO_SECONDS = 2.0
_ARTIFACT_S3_ENDPOINT_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT"
_ARTIFACT_S3_REGION_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION"
_EVIDENCE_RETENTION_DAYS_ENV = "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS"
_RAW_SECRET_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "authorization",
        "client_secret",
        "connector_secret",
        "credential",
        "password",
        "private_key",
        "secret",
        "secret_value",
        "token",
    }
)
_SECRET_VALUE_PATTERNS = (
    re.compile(r"(?i)^bearer\s+[a-z0-9._-]+$"),
    re.compile(r"(?i)^sk-[a-z0-9_-]{16,}$"),
)
_PIPELINE_CLI_FAILURE_EXCERPT_MAX_CHARS = 2048
_PIPELINE_CLI_FAILURE_SECRET_ASSIGNMENT_RE = re.compile(
    r"""(?ix)
    \b
    (?P<field>
        (?:[a-z0-9]+[_-])*(?:
            access[_-]?token|api[_-]?key|apikey|authorization|client[_-]?secret|
            connector[_-]?secret|credential|password|private[_-]?key|
            secret(?:[_-]?value)?|token
        )
    )
    \b
    [\"']?
    (?P<separator>\s*[:=]\s*)
    (?P<value>[^,;\r\n]+)
    """
)
_PIPELINE_CLI_FAILURE_BEARER_RE = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_PIPELINE_CLI_FAILURE_BASIC_RE = re.compile(r"(?i)\bbasic\s+[^\s,;]+")
_PIPELINE_CLI_FAILURE_URL_CREDENTIALS_RE = re.compile(
    r"(?i)(?P<scheme>https?://)(?P<username>[^\s/:@]+):(?P<password>[^\s/@]+)@"
)
_PIPELINE_CLI_FAILURE_SK_RE = re.compile(r"(?i)\bsk-[a-z0-9_-]{16,}\b")

PublicDocsSitemapFrontierDiscoverer = Callable[
    [str, Mapping[str, Any], int],
    Sequence[str] | Mapping[str, Any],
]


@dataclass(frozen=True, slots=True)
class SerpDagPlan:
    payload: dict[str, Any]

    def to_canonical_json(self) -> str:
        return _canonical_json(self.payload)

    def operation_sha256(self) -> str:
        return sha256(self.to_canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class _ArtifactRef:
    location: str
    kind: str
    local_path: str | None = None
    bucket: str | None = None
    key: str | None = None


def _validate_dataset_rights(
    *,
    license_id: str,
    distribution_rule: str,
    rights_status: str,
    error_prefix: str,
) -> None:
    """Enforce executable but non-redistributable rights-unverified evidence."""

    if rights_status not in _DATASET_RIGHTS_STATUSES:
        raise ValueError(f"{error_prefix} dataset rights status is unsupported")
    if distribution_rule not in _ALLOWED_DATASET_DISTRIBUTION_RULES:
        raise ValueError(f"{error_prefix} dataset distribution rule is unsupported")
    if rights_status == "attested":
        if any(marker in license_id.casefold() for marker in _UNATTESTED_LICENSE_MARKERS):
            raise ValueError(f"{error_prefix} dataset license is not attested")
        return
    if not license_id.startswith("LicenseRef-"):
        raise ValueError(f"{error_prefix} rights-unverified dataset must use LicenseRef")
    if distribution_rule != _RIGHTS_UNVERIFIED_DISTRIBUTION_RULE:
        raise ValueError(
            f"{error_prefix} rights-unverified dataset must be internal-only-no-redistribution"
        )


def default_nightly_regression_conf(
    *,
    generated_at: str,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return the reference-only D6 parent envelope from GitOps configuration."""

    values = os.environ if environment is None else environment
    if values.get(_LEGACY_NIGHTLY_PRIOR_VERIFICATION_ENV):
        raise ValueError("legacy D6 prior verification env pointers are unsupported")
    return {
        "actor_id": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["actor_id"]
        ),
        "artifact_root_path": _required_environment_value(values, _ARTIFACT_ROOT_ENV),
        "evaluation_release_promotion_evidence": _required_worm_environment_evidence(
            values,
            _NIGHTLY_REGRESSION_RUNTIME_ENV["evaluation_release_promotion_evidence"],
            "evaluation_release_promotion_evidence",
        ),
        "generated_at": _required_datetime_string({"generated_at": generated_at}, "generated_at"),
        "registry_resource_id": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["registry_resource_id"]
        ),
        "registry_resource_type": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["registry_resource_type"]
        ),
        "tenant_id": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["tenant_id"]
        ),
    }


def nightly_regression_runtime_ready(
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Report whether a scheduled D6 run has a complete, valid runtime envelope."""

    try:
        build_nightly_regression_plan(
            default_nightly_regression_conf(
                generated_at=_NIGHTLY_REGRESSION_SCHEDULE_PROBE_AT,
                environment=environment,
            )
        )
    except ValueError:
        return False
    return True


def _required_environment_value(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _required_worm_environment_evidence(
    environment: Mapping[str, str],
    name: str,
    field_name: str,
) -> dict[str, str]:
    raw = _required_environment_value(environment, name)
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must be a JSON object") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a JSON object")
    try:
        return _worm_evidence_reference({field_name: value}, field_name)
    except ValueError as exc:
        raise ValueError(f"{name} is invalid: {exc}") from exc


def build_nightly_regression_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    """Build scheduled D6 as a fenced parent of one new native D19 run."""

    payload = _payload(conf)
    _reject_raw_secrets(payload)
    if "d19_run_history_observation_evidence" in payload:
        raise ValueError("D19 history observation is runtime-produced, never caller-supplied")
    legacy_fields = {
        "bc21_base_url",
        "benchmark_suite_inputs",
        "candidateReleaseEvidence",
        "evaluationObjectiveEvidence",
        "pack_version_ids",
        "pairedEvaluationReceiptEvidence",
        "pairedEvaluationVerificationEvidence",
        "prior_paired_evaluation_verification_evidence",
        "reranker_profile_version",
        "retrieval_profile_version",
        "selected_suite_ids",
    }
    supplied_legacy = sorted(set(payload).intersection(legacy_fields))
    if supplied_legacy:
        raise ValueError(f"legacy D6 scorer fields are unsupported: {supplied_legacy}")
    tenant_id = _required_uuid(payload, "tenant_id")
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    raw_artifact_root_path = _required_str(payload, "artifact_root_path")
    if not raw_artifact_root_path.startswith("s3://"):
        raise ValueError("scheduled D6 requires an s3:// artifact_root_path")
    artifact_root_path = _required_artifact_root_path(payload)
    promotion_evidence = _worm_evidence_reference(
        payload,
        "evaluation_release_promotion_evidence",
    )
    _require_worm_evidence_within_artifact_root(
        promotion_evidence,
        artifact_root_path,
        "evaluation_release_promotion_evidence",
    )
    operation_id = _operation_id(
        "serp-airflow-scheduled-d6-parent",
        tenant_id,
        generated_at,
        promotion_evidence["sha256"],
    )
    artifact_paths = _artifact_paths(
        artifact_root_path,
        operation_id,
        (
            ("airflow_plan", "airflow-plan.json"),
            (
                "d19_run_history_observation",
                "d19-run-history-observation.json",
            ),
            (
                "d19_run_history_observation_attestation",
                "d19-run-history-observation.attestation.json",
            ),
            (
                "scheduled_regression_receipt",
                "scheduled-d6-regression-receipt.json",
            ),
        ),
    )
    actor_id = _required_str(payload, "actor_id")
    d19_trigger_conf = {
        "actor_id": actor_id,
        "artifact_root_path": artifact_root_path,
        "evaluation_release_promotion_evidence": promotion_evidence,
        "generated_at": generated_at,
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "tenant_id": str(tenant_id),
    }
    plan_payload = {
        "actor_id": actor_id,
        "artifact_root_path": artifact_root_path,
        "artifact_paths": artifact_paths,
        "d19_trigger_conf": d19_trigger_conf,
        "dag_id": _SCHEDULED_D6_DAG_ID,
        "evaluation_release_promotion_evidence": promotion_evidence,
        "generated_at": generated_at,
        "operation_id": operation_id,
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "tasks": _tasks(
            (
                "validate_nightly_regression_plan",
                "produce_d19_run_history_observation",
                "trigger_benchmark_improvement_wave",
                "load_triggered_d19_verification",
                "observe_triggered_d19_run",
                "write_scheduled_d6_regression_receipt",
                "release_d19_history_fence",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


def produce_d19_run_history_observation(
    plan_json: Mapping[str, Any] | str,
    parent_airflow_run: Mapping[str, Any] | str,
    *,
    history_client: Any | None = None,
    fence_client: Any | None = None,
    clock: Callable[[], datetime] | None = None,
    snapshot_writer: Callable[..., dict[str, Any]] | None = None,
    attestation_sealer: Callable[..., tuple[dict[str, str], dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Fence D19, snapshot complete pre-window history, and Transit-attest it."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != _SCHEDULED_D6_DAG_ID:
        raise ValueError("plan dag_id does not match the scheduled D6 history producer")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "d19_run_history_observation",
            "d19_run_history_observation_attestation",
        ),
    )
    parent = _normalized_scheduled_d6_airflow_run(parent_airflow_run)
    history_reader = history_client or _default_d19_history_client()
    fence_manager = fence_client or _default_d19_history_fence_client()
    writer = snapshot_writer or write_immutable_evidence_snapshot
    sealer = attestation_sealer or _default_d19_history_attestation_sealer()
    now_fn = clock or (lambda: datetime.now(UTC))
    fence: dict[str, Any] | None = None
    try:
        raw_fence = fence_manager.acquire(parent_airflow_run=parent)
        fence = _normalized_d19_history_fence(raw_fence, parent_airflow_run=parent)
        initial_history = _normalized_d19_history_client_result(
            history_reader.collect(parent_logical_date=parent["logicalDate"]),
            parent_airflow_run=parent,
            artifact_root_path=_required_str(plan, "artifact_root_path"),
        )
        fence = _normalized_d19_history_fence(
            fence_manager.require_active(fence),
            parent_airflow_run=parent,
        )
        final_history = _normalized_d19_history_client_result(
            history_reader.collect(parent_logical_date=parent["logicalDate"]),
            parent_airflow_run=parent,
            artifact_root_path=_required_str(plan, "artifact_root_path"),
        )
        if final_history != initial_history:
            raise ValueError("D19 history changed while the D19 fence was active")
        generated_at_dt = now_fn()
        if generated_at_dt.tzinfo is None:
            raise ValueError("D19 history observer clock must be timezone-aware")
        generated_at = generated_at_dt.astimezone(UTC)
        parent_start = _datetime_value(parent["startDate"], "parentAirflowRun.startDate")
        if generated_at < parent_start or generated_at - parent_start > timedelta(minutes=5):
            raise ValueError(
                "D19 history observation must be produced within five minutes of parent start"
            )
        fence = _normalized_d19_history_fence(
            fence,
            parent_airflow_run=parent,
            observed_at=generated_at,
        )
        payload = {
            **final_history,
            "fence": fence,
            "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
            "parentAirflowRun": parent,
            "producer": {
                "namespace": _D19_RUN_HISTORY_OBSERVER_NAMESPACE,
                "serviceAccount": _D19_RUN_HISTORY_OBSERVER_SERVICE_ACCOUNT,
            },
            "schema": _D19_RUN_HISTORY_OBSERVATION_SCHEMA,
        }
        observation_path = artifact_paths["d19_run_history_observation"]
        written = writer(
            artifact_path=observation_path,
            artifact_type="d19_run_history_observation",
            operation_id=_required_str(plan, "operation_id"),
            payload=payload,
        )
        observation_evidence = _written_worm_evidence_reference(
            written,
            observation_path,
            "D19 run history observation",
        )
        attestation_evidence, verification = sealer(
            written,
            purpose=_D19_RUN_HISTORY_OBSERVATION_PURPOSE,
        )
        try:
            normalized_attestation = _worm_evidence_reference(
                {"attestationEvidence": attestation_evidence},
                "attestationEvidence",
            )
            if (
                normalized_attestation["s3Uri"]
                != artifact_paths["d19_run_history_observation_attestation"]
            ):
                raise ValueError("D19 history Transit attestation path does not match the plan")
            normalized_verification = _normalized_d19_history_attestation_verification(
                verification,
                expected_subject=observation_evidence,
                expected_attestation=normalized_attestation,
            )
        except (KeyError, ValueError) as exc:
            raise ValueError("D19 history requires a valid Transit attestation") from exc
        d19_trigger_conf = dict(_required_mapping(plan, "d19_trigger_conf"))
        d19_trigger_conf["scheduled_d6_fence"] = fence
        return {
            "d19RunHistoryObservationAttestationEvidence": normalized_attestation,
            "d19RunHistoryObservationEvidence": observation_evidence,
            "d19RunHistoryObservationVerification": normalized_verification,
            "d19TriggerConf": d19_trigger_conf,
            "fence": fence,
        }
    except Exception:
        if fence is not None:
            fence_manager.release(fence)
        raise


def _normalized_scheduled_d6_airflow_run(
    airflow_run: Mapping[str, Any] | str,
) -> dict[str, str]:
    metadata = _json_object(airflow_run, "parent_airflow_run")
    _reject_raw_secrets(metadata)
    if set(metadata) != {"dagId", "logicalDate", "runId", "runType", "startDate"}:
        raise ValueError("scheduled D6 parentAirflowRun fields are unsupported")
    if _required_str(metadata, "dagId") != _SCHEDULED_D6_DAG_ID:
        raise ValueError("scheduled D6 parentAirflowRun dagId does not match")
    if _required_str(metadata, "runType") != "scheduled":
        raise ValueError("scheduled D6 parentAirflowRun runType must be scheduled")
    logical_date = _required_datetime_string(metadata, "logicalDate")
    start_date = _required_datetime_string(metadata, "startDate")
    if _datetime_value(start_date, "parentAirflowRun.startDate") < _datetime_value(
        logical_date,
        "parentAirflowRun.logicalDate",
    ):
        raise ValueError("scheduled D6 parent startDate precedes logicalDate")
    return {
        "dagId": _SCHEDULED_D6_DAG_ID,
        "logicalDate": logical_date,
        "runId": _required_str(metadata, "runId"),
        "runType": "scheduled",
        "startDate": start_date,
    }


def _normalized_d19_history_client_result(
    result: Mapping[str, Any],
    *,
    parent_airflow_run: Mapping[str, str],
    artifact_root_path: str,
) -> dict[str, Any]:
    if set(result) != {
        "acceptedRunVerifications",
        "activeRunQuery",
        "api",
        "pagination",
        "query",
        "runs",
        "verificationPointerQuery",
    }:
        raise ValueError("D19 history client result fields are unsupported")
    active = _required_mapping(result, "activeRunQuery")
    if set(active) != {"dagId", "states", "totalEntries"}:
        raise ValueError("D19 active-run query fields are unsupported")
    if _required_str(active, "dagId") != _D19_DAG_ID:
        raise ValueError("D19 active-run query dagId does not match")
    if _required_str_list(active, "states") != ["queued", "running"]:
        raise ValueError("D19 active-run query states are unsupported")
    if _required_non_negative_int(active, "totalEntries") != 0:
        raise ValueError("D19 history fence cannot admit active D19 runs")
    api = _required_mapping(result, "api")
    if set(api) != {"apiVersion", "airflowVersion", "serverAuthority"}:
        raise ValueError("D19 history API fields are unsupported")
    expected_api = {
        "apiVersion": "v2",
        "airflowVersion": "3.1.6",
        "serverAuthority": "airflow-api-server.airflow.svc.cluster.local:8080",
    }
    if {field: _required_str(api, field) for field in expected_api} != expected_api:
        raise ValueError("D19 history API authority/version is unsupported")
    query = _required_mapping(result, "query")
    if set(query) != {"apiPath", "dagId", "logicalDateLt", "orderBy"}:
        raise ValueError("D19 history query fields are unsupported")
    expected_query = {
        "apiPath": f"/api/v2/dags/{_D19_DAG_ID}/dagRuns",
        "dagId": _D19_DAG_ID,
        "logicalDateLt": parent_airflow_run["logicalDate"],
        "orderBy": ["logical_date", "run_id"],
    }
    normalized_query = {
        "apiPath": _required_str(query, "apiPath"),
        "dagId": _required_str(query, "dagId"),
        "logicalDateLt": _required_datetime_string(query, "logicalDateLt"),
        "orderBy": _required_str_list(query, "orderBy"),
    }
    if normalized_query != expected_query:
        if normalized_query["logicalDateLt"] != expected_query["logicalDateLt"]:
            raise ValueError("D19 history query logicalDateLt must match parent logicalDate")
        raise ValueError("D19 history query is unsupported")
    raw_runs = result.get("runs")
    if not isinstance(raw_runs, list) or not all(isinstance(run, Mapping) for run in raw_runs):
        raise ValueError("D19 history runs must be a list of objects")
    runs = [
        _normalized_d19_history_run(run, parent_logical_date=parent_airflow_run["logicalDate"])
        for run in raw_runs
    ]
    run_order = [(run["logicalDate"], run["runId"]) for run in runs]
    if run_order != sorted(run_order) or len(set(run_order)) != len(run_order):
        raise ValueError("D19 history runs must be unique and canonically ordered")
    pagination = _required_mapping(result, "pagination")
    if set(pagination) != {
        "complete",
        "observedEntries",
        "pageCount",
        "pageLimit",
        "totalEntries",
    }:
        raise ValueError("D19 history pagination fields are unsupported")
    if pagination.get("complete") is not True:
        raise ValueError("D19 history requires complete pagination")
    observed_entries = _required_non_negative_int(pagination, "observedEntries")
    total_entries = _required_non_negative_int(pagination, "totalEntries")
    page_count = _required_non_negative_int(pagination, "pageCount")
    page_limit = _required_positive_int(pagination, "pageLimit")
    if page_limit > 500:
        raise ValueError("D19 history pageLimit exceeds the supported bound")
    expected_pages = (total_entries + page_limit - 1) // page_limit
    if observed_entries != len(runs) or total_entries != len(runs) or page_count != expected_pages:
        raise ValueError("D19 history requires complete pagination with matching totals")
    verification_pointer_query = _normalized_d19_verification_pointer_query(
        _required_mapping(result, "verificationPointerQuery")
    )
    accepted_verifications = _normalized_d19_history_accepted_verifications(
        result.get("acceptedRunVerifications"),
        history_runs=runs,
        artifact_root_path=artifact_root_path,
    )
    return {
        "acceptedRunVerifications": accepted_verifications,
        "activeRunQuery": {
            "dagId": _D19_DAG_ID,
            "states": ["queued", "running"],
            "totalEntries": 0,
        },
        "api": expected_api,
        "pagination": {
            "complete": True,
            "observedEntries": observed_entries,
            "pageCount": page_count,
            "pageLimit": page_limit,
            "totalEntries": total_entries,
        },
        "query": expected_query,
        "runs": runs,
        "verificationPointerQuery": verification_pointer_query,
    }


def _normalized_d19_history_run(
    run: Mapping[str, Any],
    *,
    parent_logical_date: str,
) -> dict[str, str]:
    if set(run) != {"dagId", "logicalDate", "runId", "runType", "state"}:
        raise ValueError("D19 history run fields are unsupported")
    if _required_str(run, "dagId") != _D19_DAG_ID:
        raise ValueError("D19 history contains a foreign dagId")
    logical_date = _required_datetime_string(run, "logicalDate")
    if _datetime_value(logical_date, "D19 history logicalDate") >= _datetime_value(
        parent_logical_date,
        "parentAirflowRun.logicalDate",
    ):
        raise ValueError("D19 history run is outside the pre-parent window")
    run_type = _required_str(run, "runType")
    if run_type not in {"asset_triggered", "backfill", "manual", "scheduled"}:
        raise ValueError("D19 history runType is unsupported")
    state = _required_str(run, "state")
    if state not in {"failed", "queued", "running", "success"}:
        raise ValueError("D19 history state is unsupported")
    return {
        "dagId": _D19_DAG_ID,
        "logicalDate": logical_date,
        "runId": _required_str(run, "runId"),
        "runType": run_type,
        "state": state,
    }


def _normalized_d19_verification_pointer_query(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "apiPathTemplate": (
            f"/api/v2/dags/{_D19_DAG_ID}/dagRuns/{{dagRunId}}/"
            "taskInstances/persist_paired_evaluation_verification_evidence/"
            "xcomEntries/return_value"
        ),
        "deserialize": True,
        "mapIndex": -1,
        "stringify": False,
        "taskId": "persist_paired_evaluation_verification_evidence",
        "xcomKey": "return_value",
    }
    if set(value) != set(expected):
        raise ValueError("D19 verification pointer query fields are unsupported")
    normalized = {
        "apiPathTemplate": _required_str(value, "apiPathTemplate"),
        "deserialize": value.get("deserialize"),
        "mapIndex": value.get("mapIndex"),
        "stringify": value.get("stringify"),
        "taskId": _required_str(value, "taskId"),
        "xcomKey": _required_str(value, "xcomKey"),
    }
    if normalized != expected:
        raise ValueError("D19 verification pointer query is unsupported")
    return expected


def _normalized_d19_history_accepted_verifications(
    value: Any,
    *,
    history_runs: Sequence[Mapping[str, str]],
    artifact_root_path: str,
) -> list[dict[str, Any]]:
    if (
        not isinstance(value, list)
        or len(value) != _SCHEDULED_D6_PRIOR_STREAK_LENGTH
        or not all(isinstance(item, Mapping) for item in value)
    ):
        raise ValueError("D19 history requires exactly three accepted verification pointers")
    if len(history_runs) < _SCHEDULED_D6_PRIOR_STREAK_LENGTH:
        raise ValueError("D19 history requires at least three prior runs")
    expected_runs = list(history_runs[-_SCHEDULED_D6_PRIOR_STREAK_LENGTH:])
    if any(run["state"] != "success" or run["runType"] != "manual" for run in expected_runs):
        raise ValueError("last three historical D19 runs must be successful admitted runs")
    logical_dates = [
        _datetime_value(run["logicalDate"], "D19 history logicalDate") for run in expected_runs
    ]
    if logical_dates != sorted(logical_dates) or len(set(logical_dates)) != len(logical_dates):
        raise ValueError("last three historical D19 logical dates must be strictly increasing")
    normalized: list[dict[str, Any]] = []
    for index, (raw_pointer, expected_run) in enumerate(
        zip(value, expected_runs, strict=True),
        start=1,
    ):
        pointer = cast(Mapping[str, Any], raw_pointer)
        if set(pointer) != {
            "airflowRun",
            "pairedEvaluationVerificationEvidence",
            "receiptStatus",
            "requestId",
        }:
            raise ValueError("D19 history verification pointer fields are unsupported")
        airflow_run = _normalized_d19_airflow_run(_required_mapping(pointer, "airflowRun"))
        expected_airflow_run = {
            key: expected_run[key] for key in ("dagId", "logicalDate", "runId", "runType")
        }
        if airflow_run != expected_airflow_run:
            raise ValueError("D19 history verification pointer does not match its run")
        if _required_str(pointer, "receiptStatus") != "accepted":
            raise ValueError("D19 history verification pointer must be accepted")
        evidence = _worm_evidence_reference(
            pointer,
            "pairedEvaluationVerificationEvidence",
        )
        _require_worm_evidence_within_artifact_root(
            evidence,
            artifact_root_path,
            f"acceptedRunVerifications[{index}]",
        )
        normalized.append(
            {
                "airflowRun": airflow_run,
                "pairedEvaluationVerificationEvidence": evidence,
                "receiptStatus": "accepted",
                "requestId": str(_required_uuid(pointer, "requestId")),
            }
        )
    request_ids = [item["requestId"] for item in normalized]
    if len(set(request_ids)) != len(request_ids):
        raise ValueError("D19 history verification pointer requestId values must be unique")
    evidence_identities = [
        (
            item["pairedEvaluationVerificationEvidence"]["s3Uri"],
            item["pairedEvaluationVerificationEvidence"]["versionId"],
        )
        for item in normalized
    ]
    if len(set(evidence_identities)) != len(evidence_identities):
        raise ValueError("D19 history verification WORM handles must be unique")
    return normalized


def _normalized_d19_history_fence(
    fence: Mapping[str, Any],
    *,
    parent_airflow_run: Mapping[str, str],
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    expected_fields = {
        "acquiredAt",
        "expiresAt",
        "holderIdentity",
        "leaseDurationSeconds",
        "leaseName",
        "namespace",
        "parentDagId",
        "parentRunId",
        "resourceVersion",
        "schema",
    }
    if set(fence) != expected_fields:
        raise ValueError("D19 history fence fields are unsupported")
    if _required_str(fence, "schema") != "D19HistoryFence/v1":
        raise ValueError("D19 history fence schema is unsupported")
    if _required_str(fence, "leaseName") != "serp-d19-history-fence":
        raise ValueError("D19 history fence leaseName is unsupported")
    if _required_str(fence, "namespace") != _D19_RUN_HISTORY_OBSERVER_NAMESPACE:
        raise ValueError("D19 history fence namespace is unsupported")
    if _required_str(fence, "parentDagId") != parent_airflow_run["dagId"]:
        raise ValueError("D19 history fence parentDagId does not match")
    if _required_str(fence, "parentRunId") != parent_airflow_run["runId"]:
        raise ValueError("D19 history fence parentRunId does not match")
    expected_holder = f"d6:{parent_airflow_run['runId']}"
    if _required_str(fence, "holderIdentity") != expected_holder:
        raise ValueError("D19 history fence holderIdentity does not match parent")
    acquired = _datetime_value(
        _required_datetime_string(fence, "acquiredAt"),
        "D19 history fence acquiredAt",
    )
    expires = _datetime_value(
        _required_datetime_string(fence, "expiresAt"),
        "D19 history fence expiresAt",
    )
    if observed_at is not None and not acquired <= observed_at < expires:
        raise ValueError("D19 history fence must remain active through observation")
    duration = _required_positive_int(fence, "leaseDurationSeconds")
    if duration > 86_400 or expires - acquired != timedelta(seconds=duration):
        raise ValueError("D19 history fence duration is unsupported")
    parent_start = _datetime_value(parent_airflow_run["startDate"], "parent startDate")
    if acquired < parent_start:
        raise ValueError("D19 history fence must be acquired after parent start")
    return {
        "acquiredAt": acquired.isoformat().replace("+00:00", "Z"),
        "expiresAt": expires.isoformat().replace("+00:00", "Z"),
        "holderIdentity": expected_holder,
        "leaseDurationSeconds": duration,
        "leaseName": "serp-d19-history-fence",
        "namespace": _D19_RUN_HISTORY_OBSERVER_NAMESPACE,
        "parentDagId": parent_airflow_run["dagId"],
        "parentRunId": parent_airflow_run["runId"],
        "resourceVersion": _required_str(fence, "resourceVersion"),
        "schema": "D19HistoryFence/v1",
    }


def _normalized_d19_history_attestation_verification(
    verification: Mapping[str, Any],
    *,
    expected_subject: Mapping[str, str],
    expected_attestation: Mapping[str, str],
) -> dict[str, Any]:
    try:
        normalized, _ = _paired_evaluation_verification_descriptor(
            verification,
            field_name="D19 history Transit attestation verification",
            expected_purpose=_D19_RUN_HISTORY_OBSERVATION_PURPOSE,
            expected_subject=expected_subject,
            expected_attestation=expected_attestation,
        )
    except (KeyError, ValueError) as exc:
        raise ValueError("D19 history requires a valid Transit attestation") from exc
    signer = _required_mapping(normalized, "signer")
    expected_signer = {
        "authRole": "serp-d19-history-observer-attestor-role",
        "serviceAccountName": _D19_RUN_HISTORY_OBSERVER_SERVICE_ACCOUNT,
        "serviceAccountNamespace": _D19_RUN_HISTORY_OBSERVER_NAMESPACE,
        "tokenPolicy": "serp-d19-history-observer-attestor",
    }
    for field_name, expected in expected_signer.items():
        if _required_str(signer, field_name) != expected:
            raise ValueError("D19 history Transit attestation signer identity is unsupported")
    return normalized


def write_scheduled_d6_regression_receipt(
    plan_json: Mapping[str, Any] | str,
    history_result: Mapping[str, Any] | str,
    triggered_verification: Mapping[str, Any] | str,
    current_run_observation: Mapping[str, Any] | str,
    *,
    evidence_reader: Callable[[Mapping[str, str], str], Mapping[str, Any]] | None = None,
    snapshot_writer: Callable[..., dict[str, Any]] | None = None,
    s3_client: Any | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Seal the scheduled D6 receipt from exact WORM history and D19 evidence."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != _SCHEDULED_D6_DAG_ID:
        raise ValueError("plan dag_id does not match scheduled D6 receipt writer")
    artifact_paths = _required_artifact_paths(plan, ("scheduled_regression_receipt",))
    history_output = _json_object(history_result, "history_result")
    _reject_raw_secrets(history_output)
    expected_history_fields = {
        "d19RunHistoryObservationAttestationEvidence",
        "d19RunHistoryObservationEvidence",
        "d19RunHistoryObservationVerification",
        "d19TriggerConf",
        "fence",
    }
    if set(history_output) != expected_history_fields:
        raise ValueError("scheduled D6 history result fields are unsupported")
    history_evidence = _worm_evidence_reference(
        history_output,
        "d19RunHistoryObservationEvidence",
    )
    history_attestation_evidence = _worm_evidence_reference(
        history_output,
        "d19RunHistoryObservationAttestationEvidence",
    )
    history_verification = _normalized_d19_history_attestation_verification(
        _required_mapping(history_output, "d19RunHistoryObservationVerification"),
        expected_subject=history_evidence,
        expected_attestation=history_attestation_evidence,
    )

    def read_json(evidence: Mapping[str, str], field_name: str) -> dict[str, Any]:
        if evidence_reader is not None:
            value = evidence_reader(evidence, field_name)
            if not isinstance(value, Mapping):
                raise ValueError(f"{field_name} reader must return an object")
            return dict(value)
        client = s3_client or _s3_read_client(evidence["s3Uri"])
        payload = _read_exact_worm_evidence_bytes(
            evidence,
            field_name=field_name,
            s3_client=client,
        )
        return dict(_canonical_json_object_bytes(payload, field_name))

    promotion_evidence = _worm_evidence_reference(
        plan,
        "evaluation_release_promotion_evidence",
    )
    promotion_receipt = _validated_evaluation_release_promotion_receipt(
        read_json(promotion_evidence, "D17 evaluation release promotion"),
        plan,
    )
    history_observation = _normalized_d19_history_observation(
        read_json(history_evidence, "D19 run history observation"),
        expected_evidence=history_evidence,
        expected_fence=_required_mapping(history_output, "fence"),
        artifact_root_path=_required_str(plan, "artifact_root_path"),
    )
    _validate_d19_history_attestation_receipt(
        read_json(
            history_attestation_evidence,
            "D19 run history observation attestation",
        ),
        expected_subject=history_evidence,
        expected_verification=history_verification,
    )
    parent_run = cast(dict[str, str], history_observation["parentAirflowRun"])
    plan_fence = _normalized_d19_history_fence(
        _required_mapping(history_output, "fence"),
        parent_airflow_run=parent_run,
    )
    if plan_fence != history_observation["fence"]:
        raise ValueError("scheduled D6 history result fence does not match its WORM snapshot")
    trigger_conf = dict(_required_mapping(history_output, "d19TriggerConf"))
    if trigger_conf != {
        **dict(_required_mapping(plan, "d19_trigger_conf")),
        "scheduled_d6_fence": plan_fence,
    }:
        raise ValueError("scheduled D6 D19 trigger conf does not match its fenced plan")

    prior_pointers = cast(
        list[dict[str, Any]],
        history_observation["acceptedRunVerifications"],
    )
    prior_verifications: list[dict[str, Any]] = []
    for index, pointer in enumerate(prior_pointers, start=1):
        handle = _worm_evidence_reference(
            pointer,
            "pairedEvaluationVerificationEvidence",
        )
        verification = _normalized_paired_evaluation_verification_evidence(
            read_json(handle, f"prior paired evaluation verification {index}"),
            evidence_handle=handle,
        )
        if (
            verification["airflowRun"] != pointer["airflowRun"]
            or verification["requestId"] != pointer["requestId"]
            or pointer["receiptStatus"] != "accepted"
        ):
            raise ValueError("D19 history pointer does not match its WORM verification evidence")
        prior_verifications.append(verification)
    history_runs = cast(list[dict[str, str]], history_observation["runs"])
    expected_prior_runs = [verification["airflowRun"] for verification in prior_verifications]
    if len(history_runs) < _SCHEDULED_D6_PRIOR_STREAK_LENGTH or history_runs[-3:] != [
        {**run, "state": "success"} for run in expected_prior_runs
    ]:
        raise ValueError(
            "last three historical D19 runs must exactly match prior accepted evidence"
        )

    triggered = _json_object(triggered_verification, "triggered_verification")
    _reject_raw_secrets(triggered)
    if set(triggered) != {
        "airflowRun",
        "pairedEvaluationVerificationEvidence",
        "receiptStatus",
        "requestId",
    }:
        raise ValueError("triggered D19 verification result fields are unsupported")
    current_handle = _worm_evidence_reference(
        triggered,
        "pairedEvaluationVerificationEvidence",
    )
    current_verification = _normalized_paired_evaluation_verification_evidence(
        read_json(current_handle, "triggered paired evaluation verification"),
        evidence_handle=current_handle,
    )
    if (
        _normalized_d19_airflow_run(_required_mapping(triggered, "airflowRun"))
        != current_verification["airflowRun"]
        or _required_str(triggered, "requestId") != current_verification["requestId"]
        or _required_str(triggered, "receiptStatus") != "accepted"
    ):
        raise ValueError("triggered D19 verification result does not match its WORM evidence")
    current_observation = _normalized_d19_current_run_observation(current_run_observation)
    child_run = cast(dict[str, str], current_verification["airflowRun"])
    if {key: current_observation[key] for key in ("dagId", "logicalDate", "runId")} != {
        key: child_run[key] for key in ("dagId", "logicalDate", "runId")
    }:
        raise ValueError("current D19 observation does not match the triggered child")
    if child_run["logicalDate"] != parent_run["logicalDate"]:
        raise ValueError("triggered D19 logicalDate must match scheduled D6 parent")

    all_verifications = [*prior_verifications, current_verification]
    receipt_records = [
        _normalized_accepted_v9_receipt(
            read_json(
                verification["receiptPointer"]["receiptEvidence"],
                f"paired evaluation receipt {index}",
            ),
            verification=verification,
        )
        for index, verification in enumerate(all_verifications, start=1)
    ]
    request_ids = [str(item["requestId"]) for item in all_verifications]
    if len(set(request_ids)) != len(request_ids):
        raise ValueError("scheduled D6 requestId values must be unique")
    run_identities = [
        (item["airflowRun"]["logicalDate"], item["airflowRun"]["runId"])
        for item in all_verifications
    ]
    if len(set(run_identities)) != len(run_identities):
        raise ValueError("scheduled D6 D19 run identities must be unique")
    verification_identities = [
        (item["verificationEvidence"]["s3Uri"], item["verificationEvidence"]["versionId"])
        for item in all_verifications
    ]
    if len(set(verification_identities)) != len(verification_identities):
        raise ValueError("scheduled D6 verification S3 identities must be unique")
    receipt_identities = [
        (
            item["receiptPointer"]["receiptEvidence"]["s3Uri"],
            item["receiptPointer"]["receiptEvidence"]["versionId"],
        )
        for item in all_verifications
    ]
    if len(set(receipt_identities)) != len(receipt_identities):
        raise ValueError("scheduled D6 receipt S3 identities must be unique")
    authorities = [record["authority"] for record in receipt_records]
    if any(authority != authorities[0] for authority in authorities[1:]):
        raise ValueError("scheduled D6 evaluation authority must remain identical")
    expected_promotion_authority = {
        "baselineReleaseEvidence": promotion_receipt["baselineRelease"]["evidence"],
        "candidateReleaseEvidence": promotion_receipt["candidateRelease"]["evidence"],
        "evaluationObjectiveAttestationEvidence": promotion_receipt[
            "evaluationObjectiveAttestationEvidence"
        ],
        "evaluationObjectiveEvidence": promotion_receipt["evaluationObjectiveEvidence"],
        "evaluationReleasePromotionEvidence": promotion_evidence,
        "metricCompatibilityMatrixEvidence": promotion_receipt["metricCompatibilityMatrixEvidence"],
    }
    observed_promotion_authority = {
        field_name: authorities[0][field_name] for field_name in expected_promotion_authority
    }
    if observed_promotion_authority != expected_promotion_authority:
        raise ValueError("scheduled D6 evaluation authority does not match the D17 promotion")

    now = _aware_datetime(clock or (lambda: datetime.now(UTC)), "scheduled D6 receipt clock")
    observed_at = _datetime_value(current_observation["observedAt"], "observedAt")
    fence_expires = _datetime_value(plan_fence["expiresAt"], "fence expiresAt")
    if not observed_at <= now < fence_expires:
        raise ValueError("scheduled D6 receipt must be written while its fence is active")
    entries = [
        {
            "airflowRun": verification["airflowRun"],
            "receiptEvidence": verification["receiptPointer"]["receiptEvidence"],
            "requestId": verification["requestId"],
            "verificationEvidence": verification["verificationEvidence"],
        }
        for verification in all_verifications
    ]
    payload = {
        "acceptedStreakLength": 4,
        "authority": authorities[0],
        "currentRunObservation": current_observation,
        "generatedAt": now.isoformat().replace("+00:00", "Z"),
        "historyObservationAttestationEvidence": history_attestation_evidence,
        "historyObservationEvidence": history_evidence,
        "historyObservationVerification": history_verification,
        "operationId": _required_str(plan, "operation_id"),
        "parentAirflowRun": parent_run,
        "priorAcceptedEvaluations": entries[:3],
        "schema": _SCHEDULED_D6_RECEIPT_SCHEMA,
        "status": "accepted",
        "triggeredEvaluation": entries[3],
    }
    writer = snapshot_writer or write_immutable_evidence_snapshot
    output_path = artifact_paths["scheduled_regression_receipt"]
    written = writer(
        artifact_path=output_path,
        artifact_type="scheduled_d6_regression_receipt",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
        s3_client=s3_client,
    )
    output_evidence = _written_worm_evidence_reference(
        written,
        output_path,
        "scheduled D6 regression receipt",
    )
    persisted = read_json(output_evidence, "scheduled D6 regression receipt")
    if persisted != payload:
        raise ValueError("scheduled D6 regression receipt readback does not match")
    return {
        "operationId": _required_str(plan, "operation_id"),
        "scheduledD6RegressionEvidence": output_evidence,
        "status": "accepted",
    }


def _aware_datetime(clock: Callable[[], datetime], field_name: str) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _normalized_d19_history_observation(
    observation: Mapping[str, Any],
    *,
    expected_evidence: Mapping[str, str],
    expected_fence: Mapping[str, Any],
    artifact_root_path: str,
) -> dict[str, Any]:
    expected_fields = {
        "acceptedRunVerifications",
        "activeRunQuery",
        "api",
        "fence",
        "generatedAt",
        "pagination",
        "parentAirflowRun",
        "producer",
        "query",
        "runs",
        "schema",
        "verificationPointerQuery",
    }
    if set(observation) != expected_fields:
        raise ValueError("D19 run history observation fields are unsupported")
    if _required_str(observation, "schema") != _D19_RUN_HISTORY_OBSERVATION_SCHEMA:
        raise ValueError("D19 run history observation schema is unsupported")
    parent = _normalized_scheduled_d6_airflow_run(
        _required_mapping(observation, "parentAirflowRun")
    )
    generated_at = _datetime_value(
        _required_datetime_string(observation, "generatedAt"),
        "D19 history generatedAt",
    )
    parent_start = _datetime_value(parent["startDate"], "parent startDate")
    if not parent_start <= generated_at <= parent_start + timedelta(minutes=5):
        raise ValueError("D19 run history observation is not runtime-fresh")
    producer = _required_mapping(observation, "producer")
    if set(producer) != {"namespace", "serviceAccount"} or dict(producer) != {
        "namespace": _D19_RUN_HISTORY_OBSERVER_NAMESPACE,
        "serviceAccount": _D19_RUN_HISTORY_OBSERVER_SERVICE_ACCOUNT,
    }:
        raise ValueError("D19 run history observation producer is unsupported")
    history = _normalized_d19_history_client_result(
        {
            key: observation[key]
            for key in (
                "acceptedRunVerifications",
                "activeRunQuery",
                "api",
                "pagination",
                "query",
                "runs",
                "verificationPointerQuery",
            )
        },
        parent_airflow_run=parent,
        artifact_root_path=artifact_root_path,
    )
    fence = _normalized_d19_history_fence(
        _required_mapping(observation, "fence"),
        parent_airflow_run=parent,
        observed_at=generated_at,
    )
    normalized_expected_fence = _normalized_d19_history_fence(
        expected_fence,
        parent_airflow_run=parent,
    )
    if fence != normalized_expected_fence:
        raise ValueError("D19 run history observation fence does not match")
    _worm_evidence_reference({"evidence": expected_evidence}, "evidence")
    return {
        **history,
        "fence": fence,
        "generatedAt": generated_at.isoformat().replace("+00:00", "Z"),
        "parentAirflowRun": parent,
        "producer": dict(producer),
        "schema": _D19_RUN_HISTORY_OBSERVATION_SCHEMA,
    }


def _validate_d19_history_attestation_receipt(
    receipt: Mapping[str, Any],
    *,
    expected_subject: Mapping[str, str],
    expected_verification: Mapping[str, Any],
) -> None:
    expected_fields = {
        "domain",
        "purpose",
        "schema",
        "signatureProvider",
        "signer",
        "statementSha256",
        "subject",
        "transit",
    }
    if set(receipt) != expected_fields:
        raise ValueError("D19 history attestation receipt fields are unsupported")
    if (
        _required_str(receipt, "domain") != "serp.adapstory.ai/evaluation-governance/v1"
        or _required_str(receipt, "purpose") != _D19_RUN_HISTORY_OBSERVATION_PURPOSE
        or _required_str(receipt, "schema") != "ArtifactSignatureAttestationReceipt/v2"
        or _required_str(receipt, "signatureProvider") != "vault-transit"
    ):
        raise ValueError("D19 history attestation receipt trust contract is unsupported")
    if _worm_evidence_reference(receipt, "subject") != dict(expected_subject):
        raise ValueError("D19 history attestation receipt subject does not match")
    if dict(_required_mapping(receipt, "signer")) != expected_verification["signer"]:
        raise ValueError("D19 history attestation receipt signer does not match")
    if (
        _required_sha256_prefixed(receipt, "statementSha256")
        != expected_verification["statementSha256"]
    ):
        raise ValueError("D19 history attestation receipt statement does not match")
    transit = _required_mapping(receipt, "transit")
    expected_transit = expected_verification["transit"]
    for receipt_field, verification_field in (
        ("key", "key"),
        ("keyVersion", "keyVersion"),
        ("signature", "signature"),
        ("verifyRequestId", "verifyRequestId"),
    ):
        if transit.get(receipt_field) != expected_transit[verification_field]:
            raise ValueError("D19 history attestation receipt Transit proof does not match")


def _normalized_paired_evaluation_verification_evidence(
    payload: Mapping[str, Any],
    *,
    evidence_handle: Mapping[str, str],
) -> dict[str, Any]:
    if set(payload) != {"airflowRun", "operationId", "receiptPointer", "requestId", "schema"}:
        raise ValueError("paired evaluation verification evidence fields are unsupported")
    if _required_str(payload, "schema") != _PAIRED_EVALUATION_VERIFICATION_EVIDENCE_SCHEMA:
        raise ValueError("paired evaluation verification evidence schema is unsupported")
    operation_id = _required_str(payload, "operationId")
    if _required_str(payload, "requestId") != operation_id:
        raise ValueError("paired evaluation verification requestId does not match operationId")
    pointer = _required_mapping(payload, "receiptPointer")
    if set(pointer) != {
        "receiptAttestationEvidence",
        "receiptEvidence",
        "receiptStatus",
        "receiptVerification",
    }:
        raise ValueError("paired evaluation receipt pointer fields are unsupported")
    if _required_str(pointer, "receiptStatus") != "accepted":
        raise ValueError("scheduled D6 requires accepted paired evaluation receipts")
    receipt_evidence = _worm_evidence_reference(pointer, "receiptEvidence")
    attestation_evidence = _worm_evidence_reference(pointer, "receiptAttestationEvidence")
    receipt_verification, _ = _paired_evaluation_verification_descriptor(
        _required_mapping(pointer, "receiptVerification"),
        field_name="scheduled D6 receiptVerification",
        expected_purpose=_PAIRED_EVALUATION_FINAL_RECEIPT_PURPOSE,
        expected_subject=receipt_evidence,
        expected_attestation=attestation_evidence,
    )
    return {
        "airflowRun": _normalized_d19_airflow_run(_required_mapping(payload, "airflowRun")),
        "operationId": operation_id,
        "receiptPointer": {
            "receiptAttestationEvidence": attestation_evidence,
            "receiptEvidence": receipt_evidence,
            "receiptStatus": "accepted",
            "receiptVerification": receipt_verification,
        },
        "requestId": operation_id,
        "schema": _PAIRED_EVALUATION_VERIFICATION_EVIDENCE_SCHEMA,
        "verificationEvidence": dict(evidence_handle),
    }


def _normalized_d19_current_run_observation(
    payload: Mapping[str, Any] | str,
) -> dict[str, Any]:
    value = _json_object(payload, "current_run_observation")
    _reject_raw_secrets(value)
    expected_fields = {
        "dagId",
        "logicalDate",
        "observedAt",
        "runId",
        "sameLogicalDateRunCount",
        "sameLogicalDateSuccessCount",
        "schema",
        "state",
    }
    if set(value) != expected_fields:
        raise ValueError("D19 current run observation fields are unsupported")
    if _required_str(value, "schema") != "D19CurrentRunObservation/v1":
        raise ValueError("D19 current run observation schema is unsupported")
    if _required_str(value, "dagId") != _D19_DAG_ID:
        raise ValueError("D19 current run observation dagId is unsupported")
    if _required_str(value, "state") != "success":
        raise ValueError("D19 current run observation state must be success")
    if _required_non_negative_int(value, "sameLogicalDateRunCount") != 1:
        raise ValueError("D19 sameLogicalDateRunCount must equal one")
    if _required_non_negative_int(value, "sameLogicalDateSuccessCount") != 1:
        raise ValueError("D19 sameLogicalDateSuccessCount must equal one")
    return {
        "dagId": _D19_DAG_ID,
        "logicalDate": _required_datetime_string(value, "logicalDate"),
        "observedAt": _required_datetime_string(value, "observedAt"),
        "runId": _required_str(value, "runId"),
        "sameLogicalDateRunCount": 1,
        "sameLogicalDateSuccessCount": 1,
        "schema": "D19CurrentRunObservation/v1",
        "state": "success",
    }


def _normalized_accepted_v9_receipt(
    receipt: Mapping[str, Any],
    *,
    verification: Mapping[str, Any],
) -> dict[str, Any]:
    expected_fields = {
        "attestationVerifications",
        "baselineReleaseEvidence",
        "candidateReleaseEvidence",
        "contractVersion",
        "evaluationBindingEvidence",
        "evaluationBindingId",
        "evaluationObjectiveAttestationEvidence",
        "evaluationObjectiveEvidence",
        "evaluationReleasePromotionEvidence",
        "metricCompatibilityMatrixEvidence",
        "pairedEvaluation",
        "requestEvidence",
        "requestId",
        "status",
    }
    if set(receipt) != expected_fields:
        raise ValueError("paired evaluation v9 receipt fields are unsupported")
    if _required_str(receipt, "contractVersion") != _PAIRED_EVALUATION_RECEIPT_CONTRACT_VERSION:
        raise ValueError("paired evaluation receipt contract must be v9")
    request_id = _required_str(receipt, "requestId")
    if request_id != verification["requestId"] or _required_str(receipt, "status") != "accepted":
        raise ValueError("scheduled D6 requires the exact accepted paired evaluation receipt")
    paired = _required_mapping(receipt, "pairedEvaluation")
    if (
        _required_str(paired, "contractVersion") != "serp-paired-evaluation/v5"
        or _required_str(paired, "operationId") != request_id
        or _required_str(paired, "status") != "accepted"
    ):
        raise ValueError("scheduled D6 pairedEvaluation result is unsupported")
    return {
        "authority": {
            "baselineReleaseEvidence": _worm_evidence_reference(receipt, "baselineReleaseEvidence"),
            "candidateReleaseEvidence": _worm_evidence_reference(
                receipt, "candidateReleaseEvidence"
            ),
            "evaluationBindingEvidence": _worm_evidence_reference(
                receipt, "evaluationBindingEvidence"
            ),
            "evaluationBindingId": str(_required_uuid(receipt, "evaluationBindingId")),
            "evaluationObjectiveAttestationEvidence": _worm_evidence_reference(
                receipt, "evaluationObjectiveAttestationEvidence"
            ),
            "evaluationObjectiveEvidence": _worm_evidence_reference(
                receipt, "evaluationObjectiveEvidence"
            ),
            "evaluationReleasePromotionEvidence": _worm_evidence_reference(
                receipt, "evaluationReleasePromotionEvidence"
            ),
            "metricCompatibilityMatrixEvidence": _worm_evidence_reference(
                receipt, "metricCompatibilityMatrixEvidence"
            ),
        },
        "requestId": request_id,
    }


def _default_d19_history_client() -> Any:
    from dags.serp_d19_history_observer import AirflowD19HistoryClient

    return AirflowD19HistoryClient.from_environment()


def _default_d19_history_fence_client() -> Any:
    from dags.serp_d19_history_observer import KubernetesD19HistoryFenceClient

    return KubernetesD19HistoryFenceClient.from_environment()


def _default_d19_history_attestation_sealer() -> Callable[..., Any]:
    from dags.serp_d19_history_observer import seal_d19_history_observation_attestation

    return seal_d19_history_observation_attestation


def build_mandatory_benchmark_dataset_evidence_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    """Build the WORM snapshot plan without pretending to run benchmark adapters.

    Dataset acquisition and benchmark scoring are distinct contracts.  This
    narrowly scoped plan snapshots every canonical source, dataset byte stream,
    and licensing evidence before an adapter is available; it never accepts
    caller-provided suite inputs or emits a benchmark score.
    """

    payload = _payload(conf)
    _reject_raw_secrets(payload)
    for field_name in ("benchmark_suite_inputs", "selected_suite_ids"):
        if field_name in payload:
            raise ValueError(f"{field_name} is fixed by the mandatory dataset evidence contract")
    generated_at = _required_datetime_string(payload, "generated_at")
    artifact_root_path = _required_artifact_root_path(payload)
    if not artifact_root_path.startswith("s3://"):
        raise ValueError("mandatory dataset evidence requires an s3:// artifact_root_path")
    operation_id = _operation_id(
        "serp-airflow-mandatory-dataset-evidence",
        generated_at,
        ",".join(MANDATORY_SERP_BENCHMARK_SUITES),
        "serp-benchmark-catalog/v2",
    )
    return SerpDagPlan(
        {
            "artifact_root_path": artifact_root_path,
            "artifact_paths": _artifact_paths(
                artifact_root_path,
                operation_id,
                (
                    ("airflow_plan", "airflow-plan.json"),
                    ("benchmark_catalog", "benchmark-catalog.json"),
                    (
                        "benchmark_catalog_receipt",
                        "benchmark-catalog-materialization-receipt.json",
                    ),
                ),
            ),
            "dag_id": "serp_mandatory_benchmark_dataset_evidence_snapshot",
            "generated_at": generated_at,
            "operation_id": operation_id,
            "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
            "tasks": _tasks(
                (
                    "validate_mandatory_benchmark_dataset_evidence_plan",
                    "materialize_mandatory_benchmark_dataset_evidence",
                )
            ),
        }
    )


def build_tenant_golden_regression_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    payload = _payload(conf)
    _reject_raw_secrets(payload)
    tenant_id = _required_uuid(payload, "tenant_id")
    changed_pack_version_ids = tuple(_required_uuid_list(payload, "changed_pack_version_ids"))
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    workflow_id = _required_str(payload, "workflow_id")
    golden_set_id = _required_str(payload, "golden_set_id")
    golden_set_version = _required_str(payload, "golden_set_version")
    artifact_root_path = _required_artifact_root_path(payload)
    operation_id = _operation_id(
        "serp-airflow-tenant-golden-plan",
        tenant_id,
        workflow_id,
        golden_set_id,
        golden_set_version,
        generated_at,
        ",".join(str(value) for value in changed_pack_version_ids),
    )
    plan_payload = {
        "actor_id": _required_str(payload, "actor_id"),
        "artifact_root_path": artifact_root_path,
        "artifact_paths": _artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("airflow_plan", "airflow-plan.json"),
                ("golden_set", "golden-set.json"),
                ("tenant_golden_report", "tenant-golden-report.json"),
                (
                    "tenant_golden_registry_submissions",
                    "tenant-golden-registry-submissions.json",
                ),
            ),
        ),
        "changed_pack_version_ids": [str(value) for value in changed_pack_version_ids],
        "dag_id": "serp_tenant_golden_set_regression",
        "generated_at": generated_at,
        "golden_set_id": golden_set_id,
        "golden_set_version": golden_set_version,
        "operation_id": operation_id,
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "tasks": _tasks(
            (
                "validate_tenant_golden_regression_plan",
                "run_tenant_golden_set_cases",
                "build_tenant_golden_registry_submissions",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
        "workflow_id": workflow_id,
    }
    return SerpDagPlan(plan_payload)


def build_online_eval_rollup_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    payload = _payload(conf)
    _reject_raw_secrets(payload)
    tenant_id = _required_uuid(payload, "tenant_id")
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    reports = _required_object_list(payload, "reports")
    normalized_gate_floor = _optional_unit_interval(
        payload,
        "normalized_gate_floor",
        SERP_NORMALIZED_GATE_FLOOR,
    )
    artifact_root_path = _required_artifact_root_path(payload)
    report_hashes = ",".join(
        sha256(_canonical_json(report).encode("utf-8")).hexdigest() for report in reports
    )
    operation_id = _operation_id(
        "serp-airflow-online-eval-rollup-plan",
        tenant_id,
        registry_resource_type,
        registry_resource_id,
        generated_at,
        report_hashes,
    )
    plan_payload = {
        "actor_id": _required_str(payload, "actor_id"),
        "artifact_root_path": artifact_root_path,
        "artifact_paths": _artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("airflow_plan", "airflow-plan.json"),
                ("online_eval_rollup_plan", "online-eval-rollup-plan.json"),
                ("online_eval_rollup", "online-eval-rollup.json"),
                (
                    "online_eval_registry_submissions",
                    "online-eval-registry-submissions.json",
                ),
            ),
        ),
        "capacity_readiness_state": "ready_for_po_capacity_approval",
        "dag_id": "serp_online_eval_rollup",
        "generated_at": generated_at,
        "normalized_gate_floor": normalized_gate_floor,
        "operation_id": operation_id,
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "reports": [dict(report) for report in reports],
        "tasks": _tasks(
            (
                "validate_online_eval_rollup_plan",
                "write_online_eval_rollup_plan",
                "build_online_eval_rollup",
                "build_online_eval_registry_submissions",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


def build_model_catalog_promotion_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    """Create D17 from the canonical CI exact-nine release bundle only."""

    payload = _payload(conf)
    _reject_raw_secrets(payload)
    tenant_id = _required_uuid(payload, "tenant_id")
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    promotion_id = _required_str(payload, "promotion_id")
    artifact_root_path = _required_artifact_root_path(payload)
    if not artifact_root_path.startswith("s3://"):
        raise ValueError("model catalog promotion requires an s3:// artifact_root_path")
    for legacy_field in ("baseline_release_evidence", "candidate_release_evidence"):
        if legacy_field in payload:
            raise ValueError(f"{legacy_field} is forbidden; use evaluation_release_evidence")
    ci_bundle = _normalized_ci_evaluation_release_bundle(
        _required_mapping(payload, "evaluation_release_evidence")
    )
    for field_name, plan_field in (
        ("tenantId", str(tenant_id)),
        ("registryResourceId", str(registry_resource_id)),
        ("registryResourceType", registry_resource_type),
    ):
        if _required_str(ci_bundle, field_name) != plan_field:
            raise ValueError(f"evaluation_release_evidence {field_name} does not match D17 plan")
    baseline_release_evidence = _worm_evidence_reference(ci_bundle, "baselineReleaseEvidence")
    candidate_release_evidence = _worm_evidence_reference(ci_bundle, "candidateReleaseEvidence")
    metric_matrix_evidence = _worm_evidence_reference(
        ci_bundle, "metricCompatibilityMatrixEvidence"
    )
    evaluation_objective_evidence = _worm_evidence_reference(
        ci_bundle, "evaluationObjectiveEvidence"
    )
    evaluation_objective_attestation_evidence = _worm_evidence_reference(
        ci_bundle, "evaluationObjectiveAttestationEvidence"
    )
    for field_name, evidence in (
        ("baseline_release_evidence", baseline_release_evidence),
        ("candidate_release_evidence", candidate_release_evidence),
        ("metric_compatibility_matrix_evidence", metric_matrix_evidence),
        ("evaluation_objective_evidence", evaluation_objective_evidence),
        (
            "evaluation_objective_attestation_evidence",
            evaluation_objective_attestation_evidence,
        ),
    ):
        _require_worm_evidence_within_artifact_root(evidence, artifact_root_path, field_name)
    if baseline_release_evidence == candidate_release_evidence:
        raise ValueError("baseline and candidate release evidence must be distinct")
    operation_id = _operation_id(
        "serp-airflow-model-catalog-promotion",
        tenant_id,
        promotion_id,
        baseline_release_evidence["sha256"],
        candidate_release_evidence["sha256"],
        metric_matrix_evidence["sha256"],
        evaluation_objective_evidence["sha256"],
        evaluation_objective_attestation_evidence["sha256"],
        generated_at,
    )
    return SerpDagPlan(
        {
            "actor_id": _required_str(payload, "actor_id"),
            "artifact_root_path": artifact_root_path,
            "artifact_paths": _artifact_paths(
                artifact_root_path,
                operation_id,
                (
                    ("airflow_plan", "airflow-plan.json"),
                    ("promotion_receipt", "model-catalog-promotion-receipt.json"),
                ),
            ),
            "baseline_release_evidence": baseline_release_evidence,
            "candidate_release_evidence": candidate_release_evidence,
            "ci_evaluation_release_contract_version": _CI_EVALUATION_RELEASE_CONTRACT_VERSION,
            "dag_id": _MODEL_PROMOTION_DAG_ID,
            "generated_at": generated_at,
            "operation_id": operation_id,
            "metric_compatibility_matrix_evidence": metric_matrix_evidence,
            "evaluation_objective_evidence": evaluation_objective_evidence,
            "evaluation_objective_attestation_evidence": (
                evaluation_objective_attestation_evidence
            ),
            "promotion_id": promotion_id,
            "registry_resource_id": str(registry_resource_id),
            "registry_resource_type": registry_resource_type,
            "tasks": _tasks(
                (
                    "validate_model_catalog_promotion_plan",
                    "load_governed_model_releases",
                    "write_model_catalog_promotion_receipt",
                    "notify_governance_eval_surfaces",
                )
            ),
            "tenant_id": str(tenant_id),
        }
    )


def build_benchmark_improvement_wave_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    """Create D19 from immutable v5 promotion authority only."""

    payload = _payload(conf)
    _reject_raw_secrets(payload)
    _reject_inline_d19_fields(payload)
    tenant_id = _required_uuid(payload, "tenant_id")
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    promotion_evidence = _worm_evidence_reference(payload, "evaluation_release_promotion_evidence")
    artifact_root_path = _required_artifact_root_path(payload)
    if not artifact_root_path.startswith("s3://"):
        raise ValueError("benchmark improvement wave requires an s3:// artifact_root_path")
    _require_worm_evidence_within_artifact_root(
        promotion_evidence,
        artifact_root_path,
        "evaluation_release_promotion_evidence",
    )
    operation_id = _operation_id(
        "serp-airflow-benchmark-improvement-wave",
        tenant_id,
        promotion_evidence["sha256"],
        generated_at,
    )
    plan_payload = {
        "actor_id": _required_str(payload, "actor_id"),
        "artifact_root_path": artifact_root_path,
        "artifact_paths": _artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("airflow_plan", "airflow-plan.json"),
                ("benchmark_catalog", "benchmark-catalog.json"),
                (
                    "benchmark_catalog_receipt",
                    "benchmark-catalog-materialization-receipt.json",
                ),
                (
                    "benchmark_catalog_pack_activation",
                    "benchmark-catalog-pack-activation.json",
                ),
                ("paired_eval_request", "paired-eval-request.json"),
                ("paired_eval_receipt", "paired-eval-receipt.json"),
                (
                    "paired_evaluation_verification_evidence",
                    "paired-evaluation-verification-evidence.json",
                ),
                ("benchmark_pack_build_result", "benchmark-pack-build-result.json"),
                (
                    "benchmark_pack_lifecycle_result",
                    "benchmark-pack-lifecycle-result.json",
                ),
                (
                    "paired_evaluation_assembly_plan",
                    "paired-evaluation-assembly-plan.json",
                ),
                ("paired_execution_manifest", "paired-execution-manifest.json"),
            ),
        ),
        "dag_id": _D19_DAG_ID,
        "generated_at": generated_at,
        "evaluation_release_promotion_evidence": promotion_evidence,
        "normalized_gate_floor": SERP_NORMALIZED_GATE_FLOOR,
        "operation_id": operation_id,
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "tasks": _tasks(
            (
                "validate_d19_fence_admission",
                "validate_benchmark_improvement_wave_plan",
                "materialize_live_benchmark_catalog",
                "load_materialized_benchmark_catalog",
                "load_model_catalog_promotion",
                "build_exact_nine_benchmark_packs",
                "register_exact_nine_evaluation_binding",
                "load_exact_nine_evaluation_binding",
                "write_paired_eval_request",
                "materialize_official_harness_work_items",
                *_d19_harness_task_ids(),
                "write_paired_evaluation_assembly_plan",
                "assemble_paired_execution_manifest",
                "run_paired_benchmark_evaluation",
                "persist_paired_evaluation_verification_evidence",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


def _d19_harness_task_ids() -> tuple[str, ...]:
    task_ids: list[str] = []
    for suite_id in MANDATORY_SERP_BENCHMARK_SUITES:
        slug = suite_id.casefold().replace(" ", "_").replace("-", "_")
        for repetition in range(1, 6):
            for side in ("baseline", "candidate"):
                if suite_id in _D19_CODE_SANDBOX_SUITES:
                    task_ids.extend(
                        f"{phase}_code_sandbox_{slug}_{side}_{repetition}"
                        for phase in (
                            "prepare",
                            "fanout",
                            "execute",
                            "result_set_plan",
                            "seal",
                        )
                    )
                else:
                    task_ids.append(f"run_official_harness_{slug}_{side}_{repetition}")
    return tuple(task_ids)


def _immutable_evidence_reference(payload: Mapping[str, Any], field_name: str) -> dict[str, str]:
    """Normalize a pointer without trusting caller-declared lock metadata."""

    evidence = _required_mapping(payload, field_name)
    return {
        "artifactPath": _artifact_path(
            f"{field_name}.artifactPath", _required_str(evidence, "artifactPath")
        ),
        "artifactSha256": _required_sha256_prefixed(evidence, "artifactSha256"),
        "artifactVersionId": _required_str(evidence, "artifactVersionId"),
    }


def _worm_evidence_reference(payload: Mapping[str, Any], field_name: str) -> dict[str, str]:
    evidence = _required_mapping(payload, field_name)
    expected = {"s3Uri", "versionId", "sha256", "objectLockMode", "retainUntil"}
    if set(evidence) != expected:
        raise ValueError(f"{field_name} must define exactly {sorted(expected)}")
    s3_uri = _artifact_path(f"{field_name}.s3Uri", _required_str(evidence, "s3Uri"))
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"{field_name}.s3Uri must use s3://")
    if _required_str(evidence, "objectLockMode") != "COMPLIANCE":
        raise ValueError(f"{field_name} must declare COMPLIANCE object lock")
    _required_datetime_string(evidence, "retainUntil")
    return {
        "s3Uri": s3_uri,
        "versionId": _required_str(evidence, "versionId"),
        "sha256": _required_sha256_prefixed(evidence, "sha256"),
        "objectLockMode": "COMPLIANCE",
        "retainUntil": _required_str(evidence, "retainUntil"),
    }


def _normalized_ci_evaluation_release_bundle(payload: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "apiVersion",
        "baselineRelease",
        "baselineReleaseEvidence",
        "candidateRelease",
        "candidateReleaseEvidence",
        "contractVersion",
        "kind",
        "metricCompatibilityMatrixEvidence",
        "evaluationObjectiveEvidence",
        "evaluationObjectiveAttestationEvidence",
        "operationId",
        "registryResourceId",
        "registryResourceType",
        "status",
        "tenantId",
    }
    if set(payload) != expected:
        raise ValueError(f"evaluation_release_evidence must define exactly {sorted(expected)}")
    if _required_str(payload, "apiVersion") != "serp.adapstory.ai/v2alpha1":
        raise ValueError("evaluation_release_evidence apiVersion is unsupported")
    if _required_str(payload, "contractVersion") != _CI_EVALUATION_RELEASE_CONTRACT_VERSION:
        raise ValueError("evaluation_release_evidence contractVersion is unsupported")
    if _required_str(payload, "kind") != "EvaluationReleaseEvidence":
        raise ValueError("evaluation_release_evidence kind is unsupported")
    status = _required_str(payload, "status")
    if status != "sealed" and not status.startswith("blocked-"):
        raise ValueError("evaluation_release_evidence status is unsupported")
    baseline = _required_mapping(payload, "baselineRelease")
    candidate = _required_mapping(payload, "candidateRelease")
    for side, release in (("baseline", baseline), ("candidate", candidate)):
        if _required_str(release, "schema") != _EVALUATION_RELEASE_SCHEMA:
            raise ValueError(f"{side} release schema is unsupported")
    return {
        **dict(payload),
        "baselineReleaseEvidence": _worm_evidence_reference(payload, "baselineReleaseEvidence"),
        "candidateReleaseEvidence": _worm_evidence_reference(payload, "candidateReleaseEvidence"),
        "metricCompatibilityMatrixEvidence": _worm_evidence_reference(
            payload, "metricCompatibilityMatrixEvidence"
        ),
        "evaluationObjectiveEvidence": _worm_evidence_reference(
            payload, "evaluationObjectiveEvidence"
        ),
        "evaluationObjectiveAttestationEvidence": _worm_evidence_reference(
            payload, "evaluationObjectiveAttestationEvidence"
        ),
        "registryResourceId": str(_required_uuid(payload, "registryResourceId")),
        "registryResourceType": _required_resource_type(payload, "registryResourceType"),
        "tenantId": str(_required_uuid(payload, "tenantId")),
    }


def _reject_inline_d19_fields(payload: Mapping[str, Any]) -> None:
    supplied = sorted(_FORBIDDEN_INLINE_D19_FIELDS.intersection(payload))
    if supplied:
        raise ValueError("inline D19 field is forbidden: " + ", ".join(supplied))


def _require_evidence_within_artifact_root(
    evidence: Mapping[str, str], artifact_root_path: str, field_name: str
) -> None:
    root = artifact_root_path.rstrip("/") + "/"
    if not evidence["artifactPath"].startswith(root):
        raise ValueError(f"{field_name} must be stored under artifact_root_path")


def _require_worm_evidence_within_artifact_root(
    evidence: Mapping[str, str], artifact_root_path: str, field_name: str
) -> None:
    root = artifact_root_path.rstrip("/") + "/"
    if not evidence["s3Uri"].startswith(root):
        raise ValueError(f"{field_name} must be stored under artifact_root_path")


def load_governed_model_releases(
    plan_json: Mapping[str, Any] | str,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Read and verify both exact-nine releases and every component handle."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != _MODEL_PROMOTION_DAG_ID:
        raise ValueError("plan dag_id does not match model catalog promotion loader")
    if _required_str(plan, "ci_evaluation_release_contract_version") != (
        _CI_EVALUATION_RELEASE_CONTRACT_VERSION
    ):
        raise ValueError("D17 CI evaluation release contract is unsupported")
    baseline_evidence = _worm_evidence_reference(plan, "baseline_release_evidence")
    candidate_evidence = _worm_evidence_reference(plan, "candidate_release_evidence")
    client = s3_client or _s3_read_client(baseline_evidence["s3Uri"], candidate_evidence["s3Uri"])
    baseline = _load_governed_evaluation_release(
        baseline_evidence, field_name="baseline_release_evidence", s3_client=client
    )
    candidate = _load_governed_evaluation_release(
        candidate_evidence, field_name="candidate_release_evidence", s3_client=client
    )
    _validate_evaluation_release_pair(baseline["release"], candidate["release"])
    return {"baselineRelease": baseline, "candidateRelease": candidate}


def write_model_catalog_promotion_receipt(
    plan_json: Mapping[str, Any] | str,
    releases: Mapping[str, Any],
    *,
    snapshot_writer: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Seal the only D17 authority which D19 is allowed to consume."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != _MODEL_PROMOTION_DAG_ID:
        raise ValueError("plan dag_id does not match model catalog promotion receipt writer")
    artifact_paths = _required_artifact_paths(plan, ("promotion_receipt",))
    baseline = _validated_evaluation_release_binding(
        _required_mapping(releases, "baselineRelease"), "baselineRelease"
    )
    candidate = _validated_evaluation_release_binding(
        _required_mapping(releases, "candidateRelease"), "candidateRelease"
    )
    if baseline["evidence"] != _worm_evidence_reference(plan, "baseline_release_evidence"):
        raise ValueError("D17 baseline release evidence does not match plan")
    if candidate["evidence"] != _worm_evidence_reference(plan, "candidate_release_evidence"):
        raise ValueError("D17 candidate release evidence does not match plan")
    _validate_evaluation_release_pair(baseline["release"], candidate["release"])
    payload = _model_promotion_receipt_payload(plan, baseline, candidate)
    writer = snapshot_writer or write_immutable_evidence_snapshot
    snapshot = writer(
        artifact_path=artifact_paths["promotion_receipt"],
        artifact_type="serp_model_catalog_promotion_receipt",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )
    evidence = _written_worm_evidence_reference(
        snapshot, artifact_paths["promotion_receipt"], "model catalog promotion receipt"
    )
    return {
        **_artifact_result(
            artifact_paths["promotion_receipt"],
            artifact_type="model_catalog_promotion_receipt",
            operation_id=_required_str(plan, "operation_id"),
            payload=payload,
        ),
        "promotionEvidence": evidence,
    }


def load_model_catalog_promotion_snapshot(
    plan_json: Mapping[str, Any] | str,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Resolve a D17 receipt and re-check both of its model release manifests."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_benchmark_improvement_wave":
        raise ValueError("plan dag_id does not match D19 promotion receipt loader")
    receipt_evidence = _worm_evidence_reference(plan, "evaluation_release_promotion_evidence")
    receipt_client = s3_client or _s3_read_client(receipt_evidence["s3Uri"])
    receipt = _load_worm_json_evidence(
        receipt_evidence,
        field_name="evaluation_release_promotion_evidence",
        s3_client=receipt_client,
    )
    normalized = _validated_evaluation_release_promotion_receipt(receipt, plan)
    release_evidence = tuple(
        (
            role,
            _worm_evidence_reference(normalized[role], "evidence"),
        )
        for role in ("baselineRelease", "candidateRelease")
    )
    release_client = s3_client or _s3_read_client(
        *(evidence["s3Uri"] for _, evidence in release_evidence)
    )
    actual_releases: dict[str, Mapping[str, Any]] = {}
    for role, evidence in release_evidence:
        recorded = normalized[role]
        actual = _load_governed_evaluation_release(
            evidence, field_name=f"{role}.evidence", s3_client=release_client
        )
        actual_releases[role] = actual["release"]
        if _required_str(actual["release"], "releaseDigest") != _required_str(
            recorded, "releaseDigest"
        ):
            raise ValueError(f"D17 {role} digest no longer matches its immutable manifest")
    _validate_evaluation_release_pair(
        actual_releases["baselineRelease"], actual_releases["candidateRelease"]
    )
    expected_candidate_authority = {
        **_normalized_evaluation_release_authority(
            _required_mapping(actual_releases["candidateRelease"], "releaseAuthority"),
            "candidateRelease.releaseAuthority",
        ),
        "evidence": dict(release_evidence[1][1]),
        "releaseDigest": _required_str(actual_releases["candidateRelease"], "releaseDigest"),
        "releaseId": _required_str(actual_releases["candidateRelease"], "releaseId"),
    }
    if normalized["candidateReleaseAuthority"] != expected_candidate_authority:
        raise ValueError("D17 candidateReleaseAuthority no longer matches its immutable manifest")
    return {"promotionEvidence": receipt_evidence, "promotion": normalized}


def load_benchmark_pack_lifecycle_result_snapshot(
    plan_json: Mapping[str, Any] | str,
    promotion_snapshot: Mapping[str, Any],
    lifecycle_pointer: Mapping[str, Any],
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Read and validate the server-owned exact-nine BC21 binding result."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_benchmark_improvement_wave":
        raise ValueError("plan dag_id does not match D19 lifecycle result loader")
    evidence = _immutable_evidence_reference(lifecycle_pointer, "lifecycleResultEvidence")
    artifact_paths = _required_artifact_paths(plan, ("benchmark_pack_lifecycle_result",))
    if evidence["artifactPath"] != artifact_paths["benchmark_pack_lifecycle_result"]:
        raise ValueError("D19 lifecycle result artifact path does not match plan")
    client = s3_client or _s3_read_client(evidence["artifactPath"])
    payload, observed_version_id, _ = _read_compliance_locked_s3_bytes(
        client,
        evidence["artifactPath"],
        field_name="benchmark pack lifecycle result",
        version_id=evidence["artifactVersionId"],
    )
    if observed_version_id != evidence["artifactVersionId"]:
        raise ValueError("D19 lifecycle result VersionId does not match evidence")
    if "sha256:" + sha256(payload).hexdigest() != evidence["artifactSha256"]:
        raise ValueError("D19 lifecycle result SHA-256 does not match evidence")
    lifecycle_result = _canonical_json_object_bytes(payload, "lifecycle_result")
    promotion = _validated_d19_promotion_snapshot(plan, promotion_snapshot)
    return _validated_d19_lifecycle_result(plan, promotion, lifecycle_result)


def _load_governed_evaluation_release(
    evidence: Mapping[str, Any],
    *,
    field_name: str,
    s3_client: Any,
) -> dict[str, Any]:
    normalized_evidence = _worm_evidence_reference({field_name: evidence}, field_name)
    payload = _load_worm_json_evidence(
        normalized_evidence,
        field_name=field_name,
        s3_client=s3_client,
    )
    return {
        "evidence": normalized_evidence,
        "release": _normalize_evaluation_release(
            payload, field_name=field_name, s3_client=s3_client
        ),
    }


def _load_worm_json_evidence(
    evidence: Mapping[str, str],
    *,
    field_name: str,
    s3_client: Any,
) -> Mapping[str, Any]:
    payload, observed_version_id, _ = _read_compliance_locked_s3_bytes(
        s3_client,
        evidence["s3Uri"],
        field_name=field_name,
        version_id=evidence["versionId"],
    )
    if observed_version_id != evidence["versionId"]:
        raise ValueError(f"{field_name} VersionId does not match immutable evidence")
    if "sha256:" + sha256(payload).hexdigest() != evidence["sha256"]:
        raise ValueError(f"{field_name} SHA-256 does not match immutable evidence")
    return _canonical_json_object_bytes(payload, field_name)


def _normalize_evaluation_release(
    payload: Mapping[str, Any], *, field_name: str, s3_client: Any
) -> dict[str, Any]:
    expected = {
        "schema",
        "activationStatus",
        "releaseId",
        "releaseDigest",
        "runtimeEvidence",
        "profileSetEvidence",
        "releaseAuthority",
        "suiteProfiles",
    }
    if set(payload) != expected:
        raise ValueError(f"{field_name} must define exactly {sorted(expected)}")
    if _required_str(payload, "schema") != _EVALUATION_RELEASE_SCHEMA:
        raise ValueError(f"{field_name} schema is unsupported")
    if _required_str(payload, "activationStatus") != "ready-for-evaluation":
        raise ValueError(f"{field_name} activationStatus must be ready-for-evaluation")
    suite_profiles_raw = _required_object_list(payload, "suiteProfiles")
    suites = [_required_str(item, "suiteId") for item in suite_profiles_raw]
    if suites != list(MANDATORY_SERP_BENCHMARK_SUITES):
        raise ValueError(f"{field_name} suiteProfiles must use the exact canonical order")
    suite_profiles = [
        _normalize_suite_evaluation_profile(
            profile,
            field_name=f"{field_name}.suiteProfiles[{index}]",
            s3_client=s3_client,
        )
        for index, profile in enumerate(suite_profiles_raw)
    ]
    profile_ids = [_required_str(item, "profileId") for item in suite_profiles]
    profile_digests = [_required_str(item, "profileSha256") for item in suite_profiles]
    if len(set(profile_ids)) != len(profile_ids):
        raise ValueError(f"{field_name} suite profile IDs must be unique")
    if len(set(profile_digests)) != len(profile_digests):
        raise ValueError(f"{field_name} suite profile digests must be unique")
    runtime_evidence = _worm_evidence_reference(payload, "runtimeEvidence")
    runtime = _load_worm_json_evidence(
        runtime_evidence, field_name=f"{field_name}.runtimeEvidence", s3_client=s3_client
    )
    if _required_str(runtime, "result") != "SUCCESS":
        raise ValueError(f"{field_name} runtime evidence is not a successful signed build")
    if not _required_str(runtime, "jenkinsBuildUrl").startswith(
        "https://jenkins.adapstory.com/job/infra-build/"
    ):
        raise ValueError(f"{field_name} runtime evidence is not owned by Jenkins")
    _required_sha256_prefixed(runtime, "digest")
    profile_set_evidence = _worm_evidence_reference(payload, "profileSetEvidence")
    profile_set = _load_worm_json_evidence(
        profile_set_evidence,
        field_name=f"{field_name}.profileSetEvidence",
        s3_client=s3_client,
    )
    if set(profile_set) != {"schema", "profileSetId", "suiteProfiles"}:
        raise ValueError(f"{field_name} profile set fields are unsupported")
    if _required_str(profile_set, "schema") != "SuiteEvaluationProfileSet/v2":
        raise ValueError(f"{field_name} profile set schema is unsupported")
    if _canonical_json(
        {"suiteProfiles": _required_object_list(profile_set, "suiteProfiles")}
    ) != _canonical_json({"suiteProfiles": suite_profiles}):
        raise ValueError(f"{field_name} suite profiles do not match profileSetEvidence")
    release_authority = _normalized_evaluation_release_authority(
        _required_mapping(payload, "releaseAuthority"),
        f"{field_name}.releaseAuthority",
    )
    release_core = {
        "schema": _EVALUATION_RELEASE_SCHEMA,
        "activationStatus": "ready-for-evaluation",
        "releaseId": _required_str(payload, "releaseId"),
        "runtimeEvidence": runtime_evidence,
        "profileSetEvidence": profile_set_evidence,
        "releaseAuthority": release_authority,
        "suiteProfiles": suite_profiles,
    }
    release_digest = _required_sha256_prefixed(payload, "releaseDigest")
    expected_digest = "sha256:" + sha256(_canonical_json(release_core).encode("utf-8")).hexdigest()
    if release_digest != expected_digest:
        raise ValueError(f"{field_name} releaseDigest does not match canonical release bytes")
    return {**release_core, "releaseDigest": release_digest}


def _normalize_suite_evaluation_profile(
    payload: Mapping[str, Any], *, field_name: str, s3_client: Any
) -> dict[str, Any]:
    base_fields = {
        "schema",
        "suiteId",
        "profileId",
        "profileVersion",
        "profileSha256",
        *_EVALUATION_PROFILE_EVIDENCE_FIELDS,
    }
    if set(payload) not in (base_fields, base_fields | {"treatmentDelta"}):
        raise ValueError(f"{field_name} fields are unsupported")
    if _required_str(payload, "schema") != "SuiteEvaluationProfile/v2":
        raise ValueError(f"{field_name} schema is unsupported")
    normalized: dict[str, Any] = {
        "schema": "SuiteEvaluationProfile/v2",
        "suiteId": _required_str(payload, "suiteId"),
        "profileId": _required_str(payload, "profileId"),
        "profileVersion": _required_str(payload, "profileVersion"),
        "profileSha256": _required_sha256_prefixed(payload, "profileSha256"),
    }
    component_payloads: dict[str, Mapping[str, Any]] = {}
    for evidence_field in _EVALUATION_PROFILE_EVIDENCE_FIELDS:
        evidence = _worm_evidence_reference(payload, evidence_field)
        normalized[evidence_field] = evidence
        component = _load_worm_json_evidence(
            evidence, field_name=f"{field_name}.{evidence_field}", s3_client=s3_client
        )
        _reject_placeholder_profile_values(component, f"{field_name}.{evidence_field}")
        component_payloads[evidence_field] = component
    scorer = component_payloads["officialScorerEvidence"]
    if _required_str(scorer, "bindingStatus") != "verified":
        raise ValueError(f"{field_name} official scorer evidence is not verified")
    revision = _required_str(scorer, "revision")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError(f"{field_name} official scorer revision must be a full Git SHA")
    for scorer_field in ("repositoryUrl", "entrypoint", "profile"):
        _required_str(scorer, scorer_field)
    if "treatmentDelta" in payload:
        treatment = _required_mapping(payload, "treatmentDelta")
        if set(treatment) != {"dimensions"}:
            raise ValueError(f"{field_name} treatmentDelta fields are unsupported")
        dimensions = _required_object_list(treatment, "dimensions")
        normalized_dimensions: list[dict[str, Any]] = []
        observed_dimensions: list[str] = []
        for index, item in enumerate(dimensions):
            if set(item) != {"dimension", "changedFields"}:
                raise ValueError(
                    f"{field_name} treatmentDelta.dimensions[{index}] fields are unsupported"
                )
            dimension = _required_str(item, "dimension")
            if dimension not in _EVALUATION_TREATMENT_EVIDENCE_FIELDS:
                raise ValueError(f"{field_name} treatmentDelta dimension is unsupported")
            changed_fields = _required_str_list(item, "changedFields")
            if len(set(changed_fields)) != len(changed_fields):
                raise ValueError(f"{field_name} treatmentDelta changedFields must be unique")
            observed_dimensions.append(dimension)
            normalized_dimensions.append({"dimension": dimension, "changedFields": changed_fields})
        expected_order = tuple(_EVALUATION_TREATMENT_EVIDENCE_FIELDS)
        if len(set(observed_dimensions)) != len(observed_dimensions):
            raise ValueError(f"{field_name} treatmentDelta dimensions must be unique")
        if tuple(observed_dimensions) != tuple(
            dimension for dimension in expected_order if dimension in observed_dimensions
        ):
            raise ValueError(f"{field_name} treatmentDelta dimensions are not canonical")
        normalized["treatmentDelta"] = {"dimensions": normalized_dimensions}
    return normalized


def _reject_placeholder_profile_values(value: object, field_name: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_placeholder_profile_values(item, f"{field_name}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _reject_placeholder_profile_values(item, f"{field_name}[{index}]")
        return
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"latest", "not-applicable", "not-configured"} or normalized.startswith(
            ("latest@", "not-applicable@", "not-configured@")
        ):
            raise ValueError(f"{field_name} contains a forbidden placeholder")


def _validated_evaluation_release_binding(
    binding: Mapping[str, Any], field_name: str
) -> dict[str, Any]:
    if set(binding) != {"evidence", "release"}:
        raise ValueError(f"{field_name} fields are unsupported")
    release = _required_mapping(binding, "release")
    if _required_str(release, "schema") != _EVALUATION_RELEASE_SCHEMA:
        raise ValueError(f"{field_name}.release schema is unsupported")
    return {
        "evidence": _worm_evidence_reference(binding, "evidence"),
        "release": dict(release),
    }


def _validate_evaluation_release_pair(
    baseline: Mapping[str, Any], candidate: Mapping[str, Any]
) -> None:
    if (
        _required_mapping(baseline, "releaseAuthority")
        != _EVALUATION_RELEASE_AUTHORITIES["baseline"]
    ):
        raise ValueError("baseline releaseAuthority is not the governed baseline authority")
    if (
        _required_mapping(candidate, "releaseAuthority")
        != _EVALUATION_RELEASE_AUTHORITIES["candidate"]
    ):
        raise ValueError("candidate releaseAuthority is not the governed candidate authority")
    if _required_str(baseline, "releaseId") == _required_str(candidate, "releaseId"):
        raise ValueError("baseline and candidate releaseId must differ")
    if _required_str(baseline, "releaseDigest") == _required_str(candidate, "releaseDigest"):
        raise ValueError("baseline and candidate releaseDigest must differ")
    baseline_profiles = _required_object_list(baseline, "suiteProfiles")
    candidate_profiles = _required_object_list(candidate, "suiteProfiles")
    if len(baseline_profiles) != len(candidate_profiles):
        raise ValueError("baseline and candidate must bind the same exact-nine suites")
    for baseline_profile, candidate_profile in zip(
        baseline_profiles, candidate_profiles, strict=True
    ):
        suite_id = _required_str(baseline_profile, "suiteId")
        if _required_str(candidate_profile, "suiteId") != suite_id:
            raise ValueError("baseline and candidate suite order must match")
        if _required_str(baseline_profile, "profileSha256") == _required_str(
            candidate_profile, "profileSha256"
        ):
            raise ValueError(f"candidate {suite_id} must have a distinct profile digest")
        treatment = _required_mapping(candidate_profile, "treatmentDelta")
        dimensions = _required_object_list(treatment, "dimensions")
        treatment_fields: set[str] = set()
        for item in dimensions:
            dimension = _required_str(item, "dimension")
            _required_str_list(item, "changedFields")
            treatment_field = _EVALUATION_TREATMENT_EVIDENCE_FIELDS.get(dimension)
            if treatment_field is None:
                raise ValueError(f"candidate {suite_id} treatment dimension is unsupported")
            treatment_fields.add(treatment_field)
        changed_evidence_fields = {
            field
            for field in _EVALUATION_PROFILE_EVIDENCE_FIELDS
            if _worm_evidence_reference(baseline_profile, field)
            != _worm_evidence_reference(candidate_profile, field)
        }
        if not treatment_fields or changed_evidence_fields != treatment_fields:
            raise ValueError(f"candidate {suite_id} must have genuine treatment component deltas")


def _model_promotion_receipt_payload(
    plan: Mapping[str, Any],
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_release = _required_mapping(candidate, "release")
    candidate_evidence = dict(_required_mapping(candidate, "evidence"))
    return {
        "schema": _EVALUATION_RELEASE_PROMOTION_SCHEMA,
        "baselineRelease": {
            "evidence": dict(_required_mapping(baseline, "evidence")),
            "releaseDigest": _required_str(_required_mapping(baseline, "release"), "releaseDigest"),
        },
        "candidateRelease": {
            "evidence": candidate_evidence,
            "releaseDigest": _required_str(candidate_release, "releaseDigest"),
        },
        "candidateReleaseAuthority": {
            **_normalized_evaluation_release_authority(
                _required_mapping(candidate_release, "releaseAuthority"),
                "candidateRelease.releaseAuthority",
            ),
            "evidence": candidate_evidence,
            "releaseDigest": _required_str(candidate_release, "releaseDigest"),
            "releaseId": _required_str(candidate_release, "releaseId"),
        },
        "metricCompatibilityMatrixEvidence": dict(
            _required_mapping(plan, "metric_compatibility_matrix_evidence")
        ),
        "evaluationObjectiveEvidence": dict(
            _required_mapping(plan, "evaluation_objective_evidence")
        ),
        "evaluationObjectiveAttestationEvidence": dict(
            _required_mapping(plan, "evaluation_objective_attestation_evidence")
        ),
        "dagId": _MODEL_PROMOTION_DAG_ID,
        "generatedAt": _required_str(plan, "generated_at"),
        "operationId": _required_str(plan, "operation_id"),
        "promotionId": _required_str(plan, "promotion_id"),
        "registryResourceId": _required_str(plan, "registry_resource_id"),
        "registryResourceType": _required_resource_type(plan, "registry_resource_type"),
        "status": "approved-for-evaluation",
        "tenantId": _required_str(plan, "tenant_id"),
    }


def _validated_evaluation_release_promotion_receipt(
    receipt: Mapping[str, Any], plan: Mapping[str, Any]
) -> dict[str, Any]:
    expected = {
        "schema",
        "baselineRelease",
        "candidateRelease",
        "candidateReleaseAuthority",
        "metricCompatibilityMatrixEvidence",
        "evaluationObjectiveEvidence",
        "evaluationObjectiveAttestationEvidence",
        "dagId",
        "generatedAt",
        "operationId",
        "promotionId",
        "registryResourceId",
        "registryResourceType",
        "status",
        "tenantId",
    }
    if set(receipt) != expected:
        raise ValueError("D17 promotion receipt fields are unsupported")
    if _required_str(receipt, "schema") != _EVALUATION_RELEASE_PROMOTION_SCHEMA:
        raise ValueError("D17 promotion receipt schema is unsupported")
    if _required_str(receipt, "dagId") != _MODEL_PROMOTION_DAG_ID:
        raise ValueError("D17 promotion receipt dagId is unsupported")
    if _required_str(receipt, "status") != "approved-for-evaluation":
        raise ValueError("D17 promotion receipt is not approved for evaluation")
    for field_name, plan_field in (
        ("tenantId", "tenant_id"),
        ("registryResourceId", "registry_resource_id"),
        ("registryResourceType", "registry_resource_type"),
    ):
        if _required_str(receipt, field_name) != _required_str(plan, plan_field):
            raise ValueError(f"D17 promotion receipt {field_name} does not match D19 plan")
    baseline = _validated_promoted_release_reference(
        _required_mapping(receipt, "baselineRelease"), "baselineRelease"
    )
    candidate = _validated_promoted_release_reference(
        _required_mapping(receipt, "candidateRelease"), "candidateRelease"
    )
    if baseline["evidence"] == candidate["evidence"]:
        raise ValueError("D17 promotion release evidence must be distinct")
    if baseline["releaseDigest"] == candidate["releaseDigest"]:
        raise ValueError("D17 promotion release digests must be distinct")
    candidate_authority = _validated_candidate_release_authority(
        _required_mapping(receipt, "candidateReleaseAuthority")
    )
    if candidate_authority["evidence"] != candidate["evidence"]:
        raise ValueError("D17 candidateReleaseAuthority evidence does not match candidateRelease")
    if candidate_authority["releaseDigest"] != candidate["releaseDigest"]:
        raise ValueError("D17 candidateReleaseAuthority digest does not match candidateRelease")
    return {
        "schema": _EVALUATION_RELEASE_PROMOTION_SCHEMA,
        "baselineRelease": baseline,
        "candidateRelease": candidate,
        "candidateReleaseAuthority": candidate_authority,
        "metricCompatibilityMatrixEvidence": _worm_evidence_reference(
            receipt, "metricCompatibilityMatrixEvidence"
        ),
        "evaluationObjectiveEvidence": _worm_evidence_reference(
            receipt, "evaluationObjectiveEvidence"
        ),
        "evaluationObjectiveAttestationEvidence": _worm_evidence_reference(
            receipt, "evaluationObjectiveAttestationEvidence"
        ),
        "operationId": _required_str(receipt, "operationId"),
        "promotionId": _required_str(receipt, "promotionId"),
        "registryResourceId": _required_str(receipt, "registryResourceId"),
        "registryResourceType": _required_str(receipt, "registryResourceType"),
        "tenantId": _required_str(receipt, "tenantId"),
    }


def _validated_promoted_release_reference(
    binding: Mapping[str, Any], field_name: str
) -> dict[str, Any]:
    if set(binding) != {"evidence", "releaseDigest"}:
        raise ValueError(f"{field_name} fields are unsupported")
    return {
        "evidence": _worm_evidence_reference(binding, "evidence"),
        "releaseDigest": _required_sha256_prefixed(binding, "releaseDigest"),
    }


def _normalized_evaluation_release_authority(
    authority: Mapping[str, Any], field_name: str
) -> dict[str, str]:
    expected_fields = {"canaryState", "modelId", "provider", "purpose"}
    if set(authority) != expected_fields:
        raise ValueError(f"{field_name} must define exactly {sorted(expected_fields)}")
    normalized = {field: _required_str(authority, field) for field in sorted(expected_fields)}
    if normalized["canaryState"] != "passed":
        raise ValueError(f"{field_name}.canaryState must be passed")
    if normalized not in _EVALUATION_RELEASE_AUTHORITIES.values():
        raise ValueError(f"{field_name} is not a governed release authority")
    return normalized


def _validated_candidate_release_authority(
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    expected_fields = {
        "canaryState",
        "evidence",
        "modelId",
        "provider",
        "purpose",
        "releaseDigest",
        "releaseId",
    }
    if set(authority) != expected_fields:
        raise ValueError(
            "candidateReleaseAuthority must define exactly " f"{sorted(expected_fields)}"
        )
    normalized_authority = _normalized_evaluation_release_authority(
        {field: authority[field] for field in ("canaryState", "modelId", "provider", "purpose")},
        "candidateReleaseAuthority",
    )
    if normalized_authority != _EVALUATION_RELEASE_AUTHORITIES["candidate"]:
        raise ValueError("candidateReleaseAuthority is not the governed candidate authority")
    return {
        **normalized_authority,
        "evidence": _worm_evidence_reference(authority, "evidence"),
        "releaseDigest": _required_sha256_prefixed(authority, "releaseDigest"),
        "releaseId": _required_str(authority, "releaseId"),
    }


def _written_worm_evidence_reference(
    snapshot: Mapping[str, Any], artifact_path: str, field_name: str
) -> dict[str, str]:
    if _required_str(snapshot, "artifactPath") != artifact_path:
        raise ValueError(f"{field_name} writer path does not match plan")
    if _required_str(snapshot, "objectLockMode") != "COMPLIANCE":
        raise ValueError(f"{field_name} writer did not apply COMPLIANCE retention")
    return {
        "s3Uri": artifact_path,
        "sha256": "sha256:" + _required_sha256_hex(snapshot, "artifactSha256"),
        "versionId": _required_str(snapshot, "artifactVersionId"),
        "objectLockMode": "COMPLIANCE",
        "retainUntil": _required_datetime_string(snapshot, "retainUntil"),
    }


def build_public_docs_publish_activation_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    payload = _payload(conf)
    _reject_raw_secrets(payload)
    tenant_id = _required_uuid(payload, "tenant_id")
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    pack_id = _required_uuid(payload, "pack_id")
    pack_version_id = _required_uuid(payload, "pack_version_id")
    approval_idempotency_key = _required_uuid(payload, "approval_idempotency_key")
    evidence_bundle_id = _required_uuid(payload, "evidence_bundle_id")
    activation_idempotency_key = _required_uuid(payload, "activation_idempotency_key")
    evidence_seal_hash = _required_sha256_prefixed(payload, "evidence_seal_hash")
    benchmark_gate_export_sha256 = _required_sha256_prefixed(
        payload,
        "benchmark_gate_export_sha256",
    )
    seed_refresh_result_path = _artifact_path(
        "public_docs_seed_refresh_result_path",
        _required_str(payload, "public_docs_seed_refresh_result_path"),
    )
    seed_refresh_plan_path = _artifact_path(
        "public_docs_seed_refresh_plan_path",
        _required_str(payload, "public_docs_seed_refresh_plan_path"),
    )
    raw_seed_refresh_plan_evidence = payload.get("public_docs_seed_refresh_plan_evidence")
    seed_refresh_plan_evidence: dict[str, str] | None = None
    if raw_seed_refresh_plan_evidence is not None:
        if not isinstance(raw_seed_refresh_plan_evidence, Mapping):
            raise ValueError("public_docs_seed_refresh_plan_evidence must be an object")
        seed_refresh_plan_evidence = _validated_public_docs_exact_evidence_handle(
            raw_seed_refresh_plan_evidence,
            "public docs seed refresh plan evidence",
        )
        if seed_refresh_plan_evidence["s3Uri"] != seed_refresh_plan_path:
            raise ValueError("public docs seed refresh plan evidence URI does not match its path")
    if seed_refresh_plan_path.startswith("s3://") and seed_refresh_plan_evidence is None:
        raise ValueError("public_docs_seed_refresh_plan_evidence is required for S3 plans")
    seed_refresh_identity = _public_docs_seed_refresh_result_identity(seed_refresh_result_path)
    if seed_refresh_identity["tenant_id"] != str(tenant_id):
        raise ValueError("public_docs_seed_refresh_result identity must match tenant_id")
    if seed_refresh_identity["pack_id"] != str(pack_id):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_id")
    if seed_refresh_identity["pack_version_id"] != str(pack_version_id):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_version_id")
    activation_reason_code = _required_str(payload, "activation_reason_code")
    artifact_root_path = _required_artifact_root_path(payload)
    crawl_state_path = _public_docs_crawl_state_path(payload, artifact_root_path)
    previous_active_pack_version_id = _optional_previous_active_pack_version_id(payload)
    if previous_active_pack_version_id == str(pack_version_id):
        raise ValueError("previous_active_pack_version_id must not equal candidate pack_version_id")
    qdrant_collection = _public_docs_store_name(
        payload,
        "qdrant_collection",
        env_name=_PUBLIC_DOCS_QDRANT_COLLECTION_ENV,
        default=_PUBLIC_DOCS_DEFAULT_QDRANT_COLLECTION,
    )
    opensearch_index = _public_docs_store_name(
        payload,
        "opensearch_index",
        env_name=_PUBLIC_DOCS_OPENSEARCH_INDEX_ENV,
        default=_PUBLIC_DOCS_DEFAULT_OPENSEARCH_INDEX,
    )
    neo4j_database = _public_docs_store_name(
        payload,
        "neo4j_database",
        env_name=_PUBLIC_DOCS_NEO4J_DATABASE_ENV,
        default=_PUBLIC_DOCS_DEFAULT_NEO4J_DATABASE,
    )
    search_serve_base_url = _public_docs_search_serve_base_url(payload)
    search_serve_actor_id = _public_docs_search_serve_actor_id(payload)
    operation_id = _operation_id(
        "serp-airflow-publish-signed-pack",
        tenant_id,
        registry_resource_type,
        registry_resource_id,
        pack_id,
        pack_version_id,
        generated_at,
        previous_active_pack_version_id or "",
        seed_refresh_plan_path,
        _canonical_json(seed_refresh_plan_evidence) if seed_refresh_plan_evidence else "",
        seed_refresh_result_path,
        approval_idempotency_key,
        evidence_bundle_id,
        evidence_seal_hash,
        benchmark_gate_export_sha256,
        search_serve_actor_id,
    )
    plan_payload = {
        "activation_idempotency_key": str(activation_idempotency_key),
        "activation_reason_code": activation_reason_code,
        "actor_id": _required_str(payload, "actor_id"),
        "approval_idempotency_key": str(approval_idempotency_key),
        "artifact_root_path": artifact_root_path,
        "artifact_paths": _artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("airflow_plan", "airflow-plan.json"),
                (
                    "public_docs_publish_activation_request",
                    "public-docs-publish-activation-request.json",
                ),
                (
                    "public_docs_publish_activation_receipt",
                    "public-docs-publish-activation-receipt.json",
                ),
                (
                    "public_docs_search_serve_smoke",
                    "public-docs-search-serve-smoke.json",
                ),
                (
                    "public_docs_retrieval_golden",
                    "public-docs-retrieval-golden.json",
                ),
                (
                    "public_docs_post_activation_rollback",
                    "public-docs-post-activation-rollback.json",
                ),
                (
                    "public_docs_coverage_proof",
                    "public-docs-coverage-proof.json",
                ),
                (
                    "public_docs_crawl_state_commit_receipt",
                    "public-docs-crawl-state-commit-receipt.json",
                ),
                (
                    "public_docs_retired_pack_cleanup",
                    "public-docs-retired-pack-cleanup.json",
                ),
            ),
        ),
        "benchmark_gate_export_sha256": benchmark_gate_export_sha256,
        "bc21_base_url": _required_bc21_base_url(payload),
        "dag_id": "serp_publish_signed_pack",
        "evidence_bundle_id": str(evidence_bundle_id),
        "evidence_seal_hash": evidence_seal_hash,
        "generated_at": generated_at,
        "operation_id": operation_id,
        "pack_id": str(pack_id),
        "pack_version_id": str(pack_version_id),
        "qdrant_collection": qdrant_collection,
        "opensearch_index": opensearch_index,
        "neo4j_database": neo4j_database,
        "policy_data_class": _required_str(payload, "policy_data_class"),
        "policy_freshness_state": _required_str(payload, "policy_freshness_state"),
        "policy_license_obligation_state": _required_str(
            payload,
            "policy_license_obligation_state",
        ),
        "policy_source_type": _required_str(payload, "policy_source_type"),
        "policy_trust_state": _required_str(payload, "policy_trust_state"),
        "policy_version": _required_str(payload, "policy_version"),
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
        "public_docs_seed_refresh_plan_path": seed_refresh_plan_path,
        **(
            {"public_docs_seed_refresh_plan_evidence": seed_refresh_plan_evidence}
            if seed_refresh_plan_evidence is not None
            else {}
        ),
        "public_docs_crawl_state_path": crawl_state_path,
        **(
            {"previous_active_pack_version_id": previous_active_pack_version_id}
            if previous_active_pack_version_id is not None
            else {}
        ),
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "search_serve_actor_id": search_serve_actor_id,
        "search_serve_base_url": search_serve_base_url,
        "status": "ready_for_publish_activation_handoff",
        "tasks": _tasks(
            (
                "validate_publish_signed_pack_plan",
                "dispatch_publish_activation_handoff",
                "run_publish_activation_handoff",
                "dispatch_publish_activation_submit",
                "submit_publish_activation_to_bc21",
                "verify_public_docs_search_serve",
                "run_public_docs_retrieval_golden",
                "rollback_public_docs_post_activation_failure",
                "write_public_docs_coverage_proof",
                "commit_public_docs_crawl_state",
                "build_retired_public_docs_pack_cleanup",
                "cleanup_retired_public_docs_pack_versions",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


def build_public_docs_seed_refresh_plan(
    conf: Mapping[str, Any],
    *,
    sitemap_frontier_discoverer: PublicDocsSitemapFrontierDiscoverer | None = None,
) -> SerpDagPlan:
    payload = _payload(conf)
    _reject_raw_secrets(payload)
    actor_id = _required_str(payload, "actor_id")
    if actor_id != _PUBLIC_DOCS_DEFAULT_ACTOR_ID:
        raise ValueError("actor_id must match the public-docs acquisition workload identity")
    tenant_id = _required_uuid(payload, "tenant_id")
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    pack_id = _required_uuid(payload, "pack_id")
    pack_version_id = _required_uuid(payload, "pack_version_id")
    artifact_root_path = _required_artifact_root_path(payload)
    crawl_state_path = _public_docs_crawl_state_path(payload, artifact_root_path)
    active_pack_version_id = _optional_public_docs_active_pack_version_id(payload)
    crawl_state_recovery = _optional_public_docs_crawl_state_recovery(
        payload,
        active_pack_version_id=active_pack_version_id,
    )
    index_mode = _public_docs_index_mode(payload)
    embedding_mode = _public_docs_embedding_mode(payload, index_mode)
    qdrant_collection = _public_docs_store_name(
        payload,
        "qdrant_collection",
        env_name=_PUBLIC_DOCS_QDRANT_COLLECTION_ENV,
        default=_PUBLIC_DOCS_DEFAULT_QDRANT_COLLECTION,
    )
    opensearch_index = _public_docs_store_name(
        payload,
        "opensearch_index",
        env_name=_PUBLIC_DOCS_OPENSEARCH_INDEX_ENV,
        default=_PUBLIC_DOCS_DEFAULT_OPENSEARCH_INDEX,
    )
    neo4j_database = _public_docs_store_name(
        payload,
        "neo4j_database",
        env_name=_PUBLIC_DOCS_NEO4J_DATABASE_ENV,
        default=_PUBLIC_DOCS_DEFAULT_NEO4J_DATABASE,
    )
    bc21_base_url = _optional_bc21_base_url(payload)
    refresh_mode, refresh_reason = _public_docs_refresh_mode(payload)
    frontier_budget = _public_docs_frontier_budget(payload, generated_at=generated_at)
    crawler_discovery_workers = _public_docs_crawler_discovery_workers(
        payload,
        seed_count=len(_required_object_list(payload, "seed_registry")),
    )
    seeds = _public_docs_seed_registry(
        payload,
        crawler_discovery_workers=crawler_discovery_workers,
        sitemap_frontier_discoverer=sitemap_frontier_discoverer,
    )
    seed_registry_sha256 = sha256(
        _canonical_json({"seed_registry": seeds}).encode("utf-8")
    ).hexdigest()
    source_type_counts = _source_type_counts(seeds)
    operation_id = _operation_id(
        "serp-web-seed-crawl-refresh",
        tenant_id,
        registry_resource_type,
        registry_resource_id,
        pack_id,
        pack_version_id,
        generated_at,
        seed_registry_sha256,
        active_pack_version_id or "",
        _canonical_json(crawl_state_recovery) if crawl_state_recovery is not None else "",
        index_mode,
        embedding_mode,
        bc21_base_url or "",
        refresh_mode,
        refresh_reason or "",
        _canonical_json({"frontier_budget": frontier_budget}),
        crawler_discovery_workers,
    )
    plan_payload = {
        "actor_id": actor_id,
        **(
            {"previous_active_pack_version_id": active_pack_version_id}
            if active_pack_version_id is not None
            else {}
        ),
        **(
            {"public_docs_crawl_state_recovery": crawl_state_recovery}
            if crawl_state_recovery is not None
            else {}
        ),
        "artifact_root_path": artifact_root_path,
        "artifact_paths": _artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("airflow_plan", "airflow-plan.json"),
                ("public_docs_seed_registry", "public-docs-seed-registry.json"),
                (
                    "public_docs_seed_refresh_plan",
                    "public-docs-seed-refresh-plan.json",
                ),
                (
                    "public_docs_seed_refresh_result",
                    "public-docs-seed-refresh-result.json",
                ),
                (
                    "public_docs_publish_activation_trigger_conf",
                    "public-docs-publish-activation-trigger-conf.json",
                ),
                (
                    "public_docs_bc21_pipeline_state_receipt",
                    "public-docs-bc21-pipeline-state-receipt.json",
                ),
            ),
        ),
        **({"bc21_base_url": bc21_base_url} if bc21_base_url else {}),
        "contract_version": _EVAL_CONTRACT_VERSION,
        "crawler_discovery_workers": crawler_discovery_workers,
        "dag_id": "serp_web_seed_crawl_refresh",
        "generated_at": generated_at,
        "embedding_mode": embedding_mode,
        "frontier_budget": frontier_budget,
        "index_mode": index_mode,
        "neo4j_database": neo4j_database,
        "operation_id": operation_id,
        "opensearch_index": opensearch_index,
        "pack_id": str(pack_id),
        "pack_version_id": str(pack_version_id),
        "public_docs_crawl_state_path": crawl_state_path,
        "qdrant_collection": qdrant_collection,
        "refresh_mode": refresh_mode,
        **({"refresh_reason": refresh_reason} if refresh_reason is not None else {}),
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "seed_count": len(seeds),
        "seed_registry": seeds,
        "seed_registry_sha256": seed_registry_sha256,
        "source_type_counts": source_type_counts,
        "status": "ready_for_public_docs_seed_refresh",
        "tasks": _tasks(
            (
                "validate_public_docs_seed_registry",
                "write_public_docs_seed_registry",
                "build_public_docs_seed_refresh_plan",
                "dispatch_pipeline_seed_refresh_handoff",
                "run_public_docs_seed_refresh_pipeline",
                "submit_public_docs_bc21_pipeline_state",
                "write_public_docs_publish_activation_trigger_conf",
                "prepare_public_docs_d5_dispatch",
                "trigger_public_docs_d5_publish_activation",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


def load_public_docs_crawl_state_conf(conf: Mapping[str, Any]) -> dict[str, Any]:
    """Overlay the last D5-committed crawl state onto a D20 source catalog.

    The stable state object is intentionally read before planning and only written
    after D5 activates a fully covered pack.  A failed candidate therefore cannot
    advance validators or turn changed content into a false no-op on the next run.
    """

    payload = _payload(conf)
    _reject_raw_secrets(payload)
    artifact_root_path = _required_artifact_root_path(payload)
    state_path = _public_docs_crawl_state_path(payload, artifact_root_path)
    raw_state = _read_optional_json_file(state_path, "public_docs_crawl_state")
    if raw_state is None:
        return _recover_public_docs_active_pack_from_bc21(payload, state_path)
    _validate_public_docs_crawl_state_identity(raw_state, payload)
    active_pack_version_id = str(_required_uuid(raw_state, "active_pack_version_id"))
    persisted_seeds = _required_mapping(raw_state, "seeds")
    hydrated_registry: list[dict[str, Any]] = []
    for raw_seed in _required_object_list(payload, "seed_registry"):
        seed = dict(raw_seed)
        seed_id = _required_seed_id(seed)
        persisted_seed = persisted_seeds.get(seed_id)
        if persisted_seed is not None:
            if not isinstance(persisted_seed, Mapping):
                raise ValueError("public_docs_crawl_state seed must be an object")
            freshness_state = _required_mapping(persisted_seed, "freshness_state")
            # Validate before the persisted state becomes planner input.
            seed["freshness_state"] = _public_docs_freshness_state(
                {"freshness_state": freshness_state}
            )
        hydrated_registry.append(seed)
    return {
        **payload,
        "active_pack_version_id": active_pack_version_id,
        "public_docs_crawl_state_path": state_path,
        "seed_registry": hydrated_registry,
    }


def _recover_public_docs_active_pack_from_bc21(
    payload: Mapping[str, Any],
    state_path: str,
) -> dict[str, Any]:
    """Recover only the active predecessor when the durable crawl snapshot is absent.

    The snapshot owns crawler freshness state and is never synthesized from the
    registry.  BC-21 is consulted solely to preserve a known active pack as the
    rollback target during a one-time recovery or after artifact loss.
    """

    if payload.get("active_pack_version_id") is not None:
        return {**payload, "public_docs_crawl_state_path": state_path}
    bc21_base_url = _optional_bc21_base_url(payload)
    if bc21_base_url is None:
        return {**payload, "public_docs_crawl_state_path": state_path}

    tenant_id = str(_required_uuid(payload, "tenant_id"))
    pack_id = str(_required_uuid(payload, "pack_id"))
    endpoint = bc21_base_url.rstrip("/") + f"/api/bc-21/serp/v1/packs/{pack_id}/active-version"
    response = _bc21_json_request(
        endpoint,
        method="GET",
        body=None,
        headers={
            "X-Adapstory-Tenant-Id": tenant_id,
        },
        error_label="public docs active pack resolution",
        allow_conflict=True,
    )
    if response is None:
        return {**payload, "public_docs_crawl_state_path": state_path}
    if _required_str(response, "tenantId") != tenant_id:
        raise ValueError("public docs active pack resolution tenantId does not match plan")
    if _required_str(response, "packId") != pack_id:
        raise ValueError("public docs active pack resolution packId does not match plan")
    if _required_str(response, "activationState") != "active":
        raise ValueError("public docs active pack resolution activationState must be active")
    if _required_str(response, "versionState") != "active":
        raise ValueError("public docs active pack resolution versionState must be active")
    active_pack_version_id = str(_required_uuid(response, "packVersionId"))
    activation_run_id = str(_required_uuid(response, "activationRunId"))
    recovery = {
        "active_pack_version_id": active_pack_version_id,
        "activation_run_id": activation_run_id,
        "method": "bc21_active_pack_resolution",
    }
    return {
        **payload,
        "active_pack_version_id": active_pack_version_id,
        "public_docs_crawl_state_path": state_path,
        "public_docs_crawl_state_recovery": recovery,
    }


def default_public_docs_seed_refresh_conf(
    *,
    generated_at: str,
    artifact_root_path: str | None = None,
) -> dict[str, Any]:
    generated_at = _required_datetime_string({"generated_at": generated_at}, "generated_at")
    root_path = artifact_root_path or os.environ.get(
        _ARTIFACT_ROOT_ENV,
        _PUBLIC_DOCS_DEFAULT_ARTIFACT_ROOT,
    )
    seed_registry = _default_public_docs_seed_registry()
    pack_version_id = _default_public_docs_pack_version_id(
        generated_at=generated_at,
        seed_registry=seed_registry,
    )
    conf = {
        "actor_id": _PUBLIC_DOCS_DEFAULT_ACTOR_ID,
        "artifact_root_path": root_path,
        "generated_at": generated_at,
        "pack_id": _PUBLIC_DOCS_DEFAULT_PACK_ID,
        "pack_version_id": pack_version_id,
        "registry_resource_id": pack_version_id,
        "registry_resource_type": "pack",
        "seed_registry": seed_registry,
        "tenant_id": _PUBLIC_DOCS_DEFAULT_TENANT_ID,
    }
    bc21_base_url = _optional_bc21_base_url(conf)
    if bc21_base_url:
        conf["bc21_base_url"] = bc21_base_url
    return conf


def _default_public_docs_pack_version_id(
    *,
    generated_at: str,
    seed_registry: Sequence[Mapping[str, Any]],
) -> str:
    seed_registry_sha256 = sha256(
        _canonical_json({"seed_registry": seed_registry}).encode("utf-8")
    ).hexdigest()
    return str(
        uuid5(
            _PUBLIC_DOCS_NAMESPACE,
            f"public-docs-pack-version:{generated_at}:{seed_registry_sha256}",
        )
    )


def write_airflow_plan_artifact(plan: SerpDagPlan) -> str:
    plan_json = plan.to_canonical_json()
    artifact_paths = _required_artifact_paths(
        plan.payload,
        ("airflow_plan",),
    )
    _write_json_artifact(artifact_paths["airflow_plan"], plan.payload)
    return plan_json


def build_evidence_artifact_paths(
    artifact_root_path: str,
    operation_id: str,
    filenames: Sequence[tuple[str, str]],
) -> dict[str, str]:
    """Build validated, immutable evidence paths for a single operation."""
    return _artifact_paths(artifact_root_path, operation_id, filenames)


def write_evidence_artifact(
    artifact_path: str,
    *,
    artifact_type: str,
    operation_id: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist a secret-free evidence payload to local storage or the configured S3 store."""
    _reject_raw_secrets(payload)
    normalized_path = _artifact_path("artifact_path", artifact_path)
    _write_json_artifact(normalized_path, payload)
    return _artifact_result(
        normalized_path,
        artifact_type=artifact_type,
        operation_id=operation_id,
        payload=payload,
    )


def write_immutable_evidence_snapshot(
    artifact_path: str,
    *,
    artifact_type: str,
    operation_id: str,
    payload: Mapping[str, Any],
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Write one version-bound WORM evidence object to the dedicated S3 bucket.

    Plain ``s3://bucket/key`` references are mutable logical names.  A benchmark
    assertion must bind its content checksum to the specific S3 version created
    under COMPLIANCE retention, otherwise a later overwrite could rewrite the
    apparent provenance without changing the report path.
    """

    _reject_raw_secrets(payload)
    payload_json = _canonical_json(payload)
    return write_immutable_evidence_bytes_snapshot(
        artifact_path,
        artifact_type=artifact_type,
        operation_id=operation_id,
        payload=payload_json.encode("utf-8"),
        content_type="application/json",
        s3_client=s3_client,
    )


def write_immutable_evidence_bytes_snapshot(
    artifact_path: str,
    *,
    artifact_type: str,
    operation_id: str,
    payload: bytes,
    content_type: str,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Write opaque dataset or evidence bytes as a version-bound WORM object.

    Dataset archives cannot be truthfully represented by a mutable URL or by a
    digest stored only in a report.  This primitive binds the exact raw bytes to
    the S3 ``VersionId`` created under COMPLIANCE retention; JSON evidence uses
    the same path through :func:`write_immutable_evidence_snapshot`.
    """

    _require_non_empty("artifact_type", artifact_type)
    _require_non_empty("operation_id", operation_id)
    if not isinstance(payload, bytes) or not payload:
        raise ValueError("immutable evidence payload must be non-empty bytes")
    _require_non_empty("content_type", content_type)
    artifact = _artifact_ref("artifact_path", artifact_path)
    if artifact.kind != "s3":
        raise ValueError("immutable evidence snapshots require an s3:// artifact path")
    retention_days = _required_positive_int_env(_EVIDENCE_RETENTION_DAYS_ENV)
    client = s3_client or _s3_client(artifact_path)
    written_at = datetime.now(UTC)
    response = client.put_object(
        Bucket=_required_str_ref(artifact.bucket),
        Key=_required_str_ref(artifact.key),
        Body=payload,
        ContentType=content_type,
    )
    if not isinstance(response, Mapping):
        raise ValueError("immutable evidence S3 response is invalid")
    version_id = response.get("VersionId")
    if not isinstance(version_id, str) or not version_id.strip():
        raise ValueError("immutable evidence S3 response is missing VersionId")
    retain_until = _verify_compliance_locked_evidence_version(
        client,
        bucket=_required_str_ref(artifact.bucket),
        key=_required_str_ref(artifact.key),
        version_id=version_id,
        retention_days=retention_days,
        written_at=written_at,
    )
    etag = response.get("ETag")
    if not isinstance(etag, str) or not etag.strip():
        raise ValueError("immutable evidence S3 response is missing ETag")
    return {
        "artifactETag": etag.strip('"'),
        "artifactPath": artifact.location,
        "artifactSha256": sha256(payload).hexdigest(),
        "artifactType": artifact_type,
        "artifactVersionId": version_id,
        "contractVersion": _AIRFLOW_ARTIFACT_CONTRACT_VERSION,
        "objectLockMode": "COMPLIANCE",
        "operationId": operation_id,
        "retainUntil": retain_until,
        "retentionDays": retention_days,
        "status": "written",
    }


def _verify_compliance_locked_evidence_version(
    s3_client: Any,
    *,
    bucket: str,
    key: str,
    version_id: str,
    retention_days: int,
    written_at: datetime,
) -> str:
    """Fail closed unless bucket policy applied COMPLIANCE retention to this version.

    Evidence writers intentionally lack ``s3:PutObjectRetention``.  The
    GitOps-owned evidence bucket supplies its default COMPLIANCE retention, and
    this read-after-write check binds that protection to the exact VersionId
    returned by the upload before any provenance record is accepted.
    """

    response = s3_client.head_object(Bucket=bucket, Key=key, VersionId=version_id)
    if not isinstance(response, Mapping):
        raise ValueError("immutable evidence HeadObject response is invalid")
    if response.get("VersionId") != version_id:
        raise ValueError("immutable evidence HeadObject VersionId does not match upload")
    if response.get("ObjectLockMode") != "COMPLIANCE":
        raise ValueError("immutable evidence HeadObject must report COMPLIANCE retention")
    retain_until = response.get("ObjectLockRetainUntilDate")
    if not isinstance(retain_until, datetime) or retain_until.tzinfo is None:
        raise ValueError("immutable evidence HeadObject must include retention timestamp")
    minimum_retention = written_at + timedelta(days=retention_days) - timedelta(minutes=1)
    if retain_until.astimezone(UTC) < minimum_retention:
        raise ValueError("immutable evidence retention is shorter than the required policy")
    return retain_until.astimezone(UTC).isoformat().replace("+00:00", "Z")


def write_public_docs_airflow_plan_snapshot(
    plan: SerpDagPlan,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Seal D20 seed evidence separately and return a bounded XCom handle.

    Crawler evidence is deliberately absent from the returned object and from
    the compact Airflow plan. Each seed is stored as its own exact-version
    COMPLIANCE object so every downstream task can independently replay and
    verify it without materializing a multi-megabyte XCom value.
    """

    payload = plan.payload
    _reject_raw_secrets(payload)
    if _required_str(payload, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("public docs plan snapshot only supports the D20 DAG")
    seeds = _required_object_list(payload, "seed_registry")
    if not seeds:
        raise ValueError("public docs plan snapshot requires at least one seed")
    if len(seeds) > PUBLIC_DOCS_MAX_SEED_COUNT:
        raise ValueError(
            "public docs seed count exceeds the governed ceiling: "
            f"{len(seeds)} > {PUBLIC_DOCS_MAX_SEED_COUNT}"
        )
    operation_id = _required_str(payload, "operation_id")
    artifact_root_path = _required_artifact_root_path(payload)
    if not artifact_root_path.startswith("s3://"):
        raise ValueError("public docs plan snapshots require an s3:// artifact root")
    artifact_paths = _required_artifact_paths(payload, ("airflow_plan",))
    client = s3_client or _s3_client(artifact_paths["airflow_plan"])
    seed_evidence: list[dict[str, Any]] = []
    total_seed_bytes = 0
    seen_seed_ids: set[str] = set()
    for index, seed in enumerate(seeds):
        seed_id = _required_str(seed, "seed_id")
        if seed_id in seen_seed_ids:
            raise ValueError(f"duplicate public docs seed_id in snapshot: {seed_id}")
        seen_seed_ids.add(seed_id)
        _validate_public_docs_seed_evidence_limits(seed)
        seed_snapshot = {
            "operationId": operation_id,
            "schema": _PUBLIC_DOCS_SEED_EVIDENCE_SCHEMA,
            "seed": dict(seed),
            "seedId": seed_id,
        }
        seed_bytes = _canonical_json(seed_snapshot).encode("utf-8")
        if len(seed_bytes) > PUBLIC_DOCS_MAX_SEED_EVIDENCE_BYTES:
            raise ValueError(
                "public docs seed evidence exceeds the governed byte ceiling: "
                f"seed_id={seed_id} bytes={len(seed_bytes)} "
                f"limit={PUBLIC_DOCS_MAX_SEED_EVIDENCE_BYTES}"
            )
        total_seed_bytes += len(seed_bytes)
        if total_seed_bytes > PUBLIC_DOCS_MAX_TOTAL_SEED_EVIDENCE_BYTES:
            raise ValueError(
                "public docs aggregate seed evidence exceeds the governed byte ceiling: "
                f"bytes={total_seed_bytes} limit={PUBLIC_DOCS_MAX_TOTAL_SEED_EVIDENCE_BYTES}"
            )
        seed_filename = (
            "public-docs-seed-evidence/"
            f"{index:04d}-{sha256(seed_id.encode('utf-8')).hexdigest()[:16]}.json"
        )
        seed_path = _artifact_paths(
            artifact_root_path,
            operation_id,
            (("seed", seed_filename),),
        )["seed"]
        written = write_immutable_evidence_snapshot(
            seed_path,
            artifact_type="public_docs_seed_evidence",
            operation_id=operation_id,
            payload=seed_snapshot,
            s3_client=client,
        )
        _LOG.info(
            "public docs seed WORM snapshot written operation_id=%s seed_ordinal=%d/%d "
            "seed_id_sha256=%s version_id=%s",
            operation_id,
            index + 1,
            len(seeds),
            sha256(seed_id.encode("utf-8")).hexdigest(),
            _required_str(written, "artifactVersionId"),
        )
        seed_evidence.append(
            {
                "evidence": _public_docs_exact_evidence_handle(written),
                "summary": _public_docs_seed_evidence_summary(seed),
            }
        )

    compact_plan = dict(payload)
    compact_plan.pop("seed_registry")
    plan_snapshot = {
        "plan": compact_plan,
        "schema": _PUBLIC_DOCS_AIRFLOW_PLAN_SNAPSHOT_SCHEMA,
        "seedEvidence": seed_evidence,
    }
    plan_snapshot_bytes = _canonical_json(plan_snapshot).encode("utf-8")
    if len(plan_snapshot_bytes) > PUBLIC_DOCS_MAX_COMPACT_PLAN_BYTES:
        raise ValueError(
            "public docs compact plan exceeds the governed byte ceiling: "
            f"bytes={len(plan_snapshot_bytes)} limit={PUBLIC_DOCS_MAX_COMPACT_PLAN_BYTES}"
        )
    written_plan = write_immutable_evidence_snapshot(
        artifact_paths["airflow_plan"],
        artifact_type="public_docs_airflow_plan_snapshot",
        operation_id=operation_id,
        payload=plan_snapshot,
        s3_client=client,
    )
    _LOG.info(
        "public docs compact plan WORM snapshot written operation_id=%s seed_count=%d "
        "version_id=%s",
        operation_id,
        len(seeds),
        _required_str(written_plan, "artifactVersionId"),
    )
    handle = {
        "planEvidence": _public_docs_exact_evidence_handle(written_plan),
        "schema": _PUBLIC_DOCS_AIRFLOW_PLAN_HANDLE_SCHEMA,
        "summary": {
            "generatedAt": _required_datetime_string(payload, "generated_at"),
            "operationId": operation_id,
            "seedCount": len(seeds),
            "sourceTypeCounts": dict(_required_mapping(payload, "source_type_counts")),
        },
    }
    handle_size = len(_canonical_json(handle).encode("utf-8"))
    if handle_size > PUBLIC_DOCS_MAX_XCOM_BYTES:
        raise ValueError(
            "public docs XCom handle exceeds the governed byte ceiling: "
            f"bytes={handle_size} limit={PUBLIC_DOCS_MAX_XCOM_BYTES}"
        )
    return handle


def load_public_docs_airflow_plan_snapshot(
    handle: Mapping[str, Any] | str,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Re-read a D20 plan and every seed by exact VersionId and SHA-256."""

    plan, _snapshot = _load_public_docs_airflow_plan_snapshot_bundle(
        handle,
        s3_client=s3_client,
    )
    return plan


def _load_public_docs_airflow_plan_snapshot_bundle(
    handle: Mapping[str, Any] | str,
    *,
    s3_client: Any | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_handle = _json_object(handle, "public_docs_plan_handle")
    if set(normalized_handle) != {"planEvidence", "schema", "summary"}:
        raise ValueError("public docs plan handle shape is invalid")
    if _required_str(normalized_handle, "schema") != _PUBLIC_DOCS_AIRFLOW_PLAN_HANDLE_SCHEMA:
        raise ValueError("public docs plan handle schema is unsupported")
    if len(_canonical_json(normalized_handle).encode("utf-8")) > PUBLIC_DOCS_MAX_XCOM_BYTES:
        raise ValueError("public docs plan handle exceeds the governed XCom byte ceiling")
    plan_evidence = _validated_public_docs_exact_evidence_handle(
        _required_mapping(normalized_handle, "planEvidence"),
        "public docs plan evidence",
    )
    client = s3_client or _s3_client(plan_evidence["s3Uri"])
    plan_bytes = _read_public_docs_exact_evidence_bytes(
        plan_evidence,
        field_name="public docs plan evidence",
        s3_client=client,
        max_bytes=PUBLIC_DOCS_MAX_COMPACT_PLAN_BYTES,
    )
    try:
        raw_snapshot = json.loads(plan_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("public docs plan evidence is not valid JSON") from exc
    if not isinstance(raw_snapshot, Mapping):
        raise ValueError("public docs plan evidence must be a JSON object")
    if set(raw_snapshot) != {"plan", "schema", "seedEvidence"}:
        raise ValueError("public docs plan evidence shape is invalid")
    if _required_str(raw_snapshot, "schema") != _PUBLIC_DOCS_AIRFLOW_PLAN_SNAPSHOT_SCHEMA:
        raise ValueError("public docs plan evidence schema is unsupported")
    compact_plan = dict(_required_mapping(raw_snapshot, "plan"))
    if "seed_registry" in compact_plan:
        raise ValueError("public docs compact plan must not contain inline seed evidence")
    if _required_str(compact_plan, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("public docs compact plan dag_id is unsupported")
    operation_id = _required_str(compact_plan, "operation_id")
    artifact_paths = _required_artifact_paths(compact_plan, ("airflow_plan",))
    if plan_evidence["s3Uri"] != artifact_paths["airflow_plan"]:
        raise ValueError("public docs plan evidence URI does not match the canonical plan path")
    seed_evidence = raw_snapshot.get("seedEvidence")
    if not isinstance(seed_evidence, list) or not seed_evidence:
        raise ValueError("public docs plan evidence must contain seed handles")
    if len(seed_evidence) > PUBLIC_DOCS_MAX_SEED_COUNT:
        raise ValueError("public docs plan evidence seed count exceeds the governed ceiling")
    if len(seed_evidence) != _required_positive_int(compact_plan, "seed_count"):
        raise ValueError("public docs plan evidence seed count does not match the plan")
    seed_prefix = (
        _required_artifact_root_path(compact_plan).rstrip("/")
        + "/"
        + operation_id
        + "/public-docs-seed-evidence/"
    )
    seeds: list[dict[str, Any]] = []
    normalized_seed_evidence: list[dict[str, Any]] = []
    total_seed_bytes = 0
    for raw_entry in seed_evidence:
        if not isinstance(raw_entry, Mapping) or set(raw_entry) != {"evidence", "summary"}:
            raise ValueError("public docs seed evidence entry shape is invalid")
        evidence = _validated_public_docs_exact_evidence_handle(
            _required_mapping(raw_entry, "evidence"),
            "public docs seed evidence",
        )
        if not evidence["s3Uri"].startswith(seed_prefix):
            raise ValueError("public docs seed evidence URI escapes the operation prefix")
        summary = dict(_required_mapping(raw_entry, "summary"))
        seed_bytes = _read_public_docs_exact_evidence_bytes(
            evidence,
            field_name="public docs seed evidence",
            s3_client=client,
            max_bytes=PUBLIC_DOCS_MAX_SEED_EVIDENCE_BYTES,
        )
        total_seed_bytes += len(seed_bytes)
        if total_seed_bytes > PUBLIC_DOCS_MAX_TOTAL_SEED_EVIDENCE_BYTES:
            raise ValueError(
                "public docs aggregate seed evidence exceeds the governed byte ceiling"
            )
        try:
            raw_seed_snapshot = json.loads(seed_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("public docs seed evidence is not valid JSON") from exc
        if not isinstance(raw_seed_snapshot, Mapping) or set(raw_seed_snapshot) != {
            "operationId",
            "schema",
            "seed",
            "seedId",
        }:
            raise ValueError("public docs seed evidence shape is invalid")
        if _required_str(raw_seed_snapshot, "schema") != _PUBLIC_DOCS_SEED_EVIDENCE_SCHEMA:
            raise ValueError("public docs seed evidence schema is unsupported")
        if _required_str(raw_seed_snapshot, "operationId") != operation_id:
            raise ValueError("public docs seed evidence operationId does not match the plan")
        seed = dict(_required_mapping(raw_seed_snapshot, "seed"))
        seed_id = _required_str(seed, "seed_id")
        if _required_str(raw_seed_snapshot, "seedId") != seed_id:
            raise ValueError("public docs seed evidence seedId does not match its payload")
        if summary != _public_docs_seed_evidence_summary(seed):
            raise ValueError("public docs seed evidence summary does not match its payload")
        _validate_public_docs_seed_evidence_limits(seed)
        seeds.append(seed)
        normalized_seed_evidence.append({"evidence": evidence, "summary": summary})
    if len({_required_str(seed, "seed_id") for seed in seeds}) != len(seeds):
        raise ValueError("public docs seed evidence contains duplicate seed_id values")
    expected_registry_sha256 = sha256(
        _canonical_json({"seed_registry": seeds}).encode("utf-8")
    ).hexdigest()
    if _required_str(compact_plan, "seed_registry_sha256") != expected_registry_sha256:
        raise ValueError("public docs seed registry digest does not match exact WORM evidence")
    hydrated_plan = {**compact_plan, "seed_registry": seeds}
    _validate_public_docs_plan_handle_summary(normalized_handle, hydrated_plan)
    return hydrated_plan, {
        "planEvidence": plan_evidence,
        "seedEvidence": normalized_seed_evidence,
    }


def _public_docs_exact_evidence_handle(written: Mapping[str, Any]) -> dict[str, str]:
    return {
        "s3Uri": _artifact_path("artifactPath", _required_str(written, "artifactPath")),
        "sha256": "sha256:" + _required_sha256_hex(written, "artifactSha256"),
        "versionId": _required_str(written, "artifactVersionId"),
    }


def _validated_public_docs_exact_evidence_handle(
    handle: Mapping[str, Any],
    field_name: str,
) -> dict[str, str]:
    if set(handle) != {"s3Uri", "sha256", "versionId"}:
        raise ValueError(f"{field_name} must contain exactly s3Uri, sha256, and versionId")
    s3_uri = _artifact_path(f"{field_name}.s3Uri", _required_str(handle, "s3Uri"))
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"{field_name}.s3Uri must use s3://")
    return {
        "s3Uri": s3_uri,
        "sha256": _required_sha256_prefixed(handle, "sha256"),
        "versionId": _required_str(handle, "versionId"),
    }


def _read_public_docs_exact_evidence_bytes(
    evidence: Mapping[str, str],
    *,
    field_name: str,
    s3_client: Any,
    max_bytes: int,
) -> bytes:
    payload, observed_version, _ = _read_compliance_locked_s3_bytes(
        s3_client,
        evidence["s3Uri"],
        field_name=field_name,
        version_id=evidence["versionId"],
        max_bytes=max_bytes,
    )
    if observed_version != evidence["versionId"]:
        raise ValueError(f"{field_name} VersionId does not match the requested version")
    if "sha256:" + sha256(payload).hexdigest() != evidence["sha256"]:
        raise ValueError(f"{field_name} digest does not match exact WORM evidence")
    return payload


def _public_docs_seed_evidence_summary(seed: Mapping[str, Any]) -> dict[str, Any]:
    crawl_evidence = _required_mapping(seed, "crawl_policy").get("crawl_evidence")
    crawl_summary: dict[str, Any] = {"status": "not_applicable"}
    if crawl_evidence is not None:
        if not isinstance(crawl_evidence, Mapping):
            raise ValueError("public docs crawl evidence must be an object")
        raw_summary = crawl_evidence.get("summary", {})
        if not isinstance(raw_summary, Mapping):
            raise ValueError("public docs crawl evidence summary must be an object")
        crawl_summary = {
            "counts": {
                key: _required_non_negative_int(raw_summary, key)
                for key in ("blocked", "changed", "deleted", "failed", "unchanged")
            },
            "status": _required_str(crawl_evidence, "status"),
        }
    return {
        "crawlEvidence": crawl_summary,
        "seedId": _required_str(seed, "seed_id"),
        "sourceId": _required_str(seed, "source_id"),
        "sourceType": _required_str(seed, "source_type"),
    }


def _validate_public_docs_seed_evidence_limits(seed: Mapping[str, Any]) -> None:
    crawl_policy = _required_mapping(seed, "crawl_policy")
    max_pages = _required_positive_int(crawl_policy, "max_pages")
    if max_pages > 500:
        raise ValueError("public docs crawl max_pages exceeds the governed ceiling")
    for state_field in ("previous_state",):
        state = crawl_policy.get(state_field, {})
        if not isinstance(state, Mapping) or len(state) > max_pages:
            raise ValueError(f"public docs {state_field} exceeds max_pages")
    freshness_state = seed.get("freshness_state")
    if isinstance(freshness_state, Mapping):
        page_state = freshness_state.get("page_state", {})
        if not isinstance(page_state, Mapping) or len(page_state) > max_pages:
            raise ValueError("public docs freshness page_state exceeds max_pages")
    crawl_evidence = crawl_policy.get("crawl_evidence")
    if crawl_evidence is None:
        return
    if not isinstance(crawl_evidence, Mapping):
        raise ValueError("public docs crawl evidence must be an object")
    for field_name in (
        "blocked_urls",
        "changed_urls",
        "deleted_urls",
        "failed_urls",
        "unchanged_urls",
    ):
        values = crawl_evidence.get(field_name, [])
        if not isinstance(values, list) or len(values) > max_pages:
            raise ValueError(f"public docs crawl evidence {field_name} exceeds max_pages")
    pages = crawl_evidence.get("pages", {})
    if not isinstance(pages, Mapping) or len(pages) > max_pages * 2:
        raise ValueError("public docs crawl evidence pages exceeds the governed ceiling")
    state = crawl_evidence.get("state", {})
    if not isinstance(state, Mapping) or len(state) > max_pages:
        raise ValueError("public docs crawl evidence state exceeds max_pages")


def _validate_public_docs_plan_handle_summary(
    handle: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> None:
    summary = _required_mapping(handle, "summary")
    if set(summary) != {"generatedAt", "operationId", "seedCount", "sourceTypeCounts"}:
        raise ValueError("public docs plan handle summary shape is invalid")
    expected = {
        "generatedAt": _required_datetime_string(plan, "generated_at"),
        "operationId": _required_str(plan, "operation_id"),
        "seedCount": len(_required_object_list(plan, "seed_registry")),
        "sourceTypeCounts": dict(_required_mapping(plan, "source_type_counts")),
    }
    if dict(summary) != expected:
        raise ValueError("public docs plan handle summary does not match exact WORM evidence")


def _public_docs_task_artifact_handle(
    written: Mapping[str, Any],
    *,
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    handle = {
        "artifactType": _required_str(written, "artifactType"),
        "evidence": _public_docs_exact_evidence_handle(written),
        "schema": _PUBLIC_DOCS_TASK_ARTIFACT_HANDLE_SCHEMA,
        "summary": dict(summary),
    }
    if len(_canonical_json(handle).encode("utf-8")) > PUBLIC_DOCS_MAX_XCOM_BYTES:
        raise ValueError("public docs task artifact handle exceeds the governed XCom byte ceiling")
    return handle


def _read_public_docs_task_artifact_payload(
    raw_handle: Mapping[str, Any] | str,
    *,
    expected_artifact_type: str,
    max_bytes: int,
) -> tuple[dict[str, Any], dict[str, str]]:
    handle = _json_object(raw_handle, "public_docs_task_artifact_handle")
    if set(handle) != {"artifactType", "evidence", "schema", "summary"}:
        raise ValueError("public docs task artifact handle shape is invalid")
    if _required_str(handle, "schema") != _PUBLIC_DOCS_TASK_ARTIFACT_HANDLE_SCHEMA:
        raise ValueError("public docs task artifact handle schema is unsupported")
    if _required_str(handle, "artifactType") != expected_artifact_type:
        raise ValueError("public docs task artifact type is unsupported")
    if len(_canonical_json(handle).encode("utf-8")) > PUBLIC_DOCS_MAX_XCOM_BYTES:
        raise ValueError("public docs task artifact handle exceeds the governed XCom byte ceiling")
    evidence = _validated_public_docs_exact_evidence_handle(
        _required_mapping(handle, "evidence"),
        f"{expected_artifact_type} evidence",
    )
    payload_bytes = _read_public_docs_exact_evidence_bytes(
        evidence,
        field_name=f"{expected_artifact_type} evidence",
        s3_client=_s3_client(evidence["s3Uri"]),
        max_bytes=max_bytes,
    )
    try:
        payload = json.loads(payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{expected_artifact_type} evidence is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"{expected_artifact_type} evidence must be a JSON object")
    _reject_raw_secrets(payload)
    return dict(payload), evidence


def _public_docs_compact_seed_registry(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        _public_docs_compact_seed_descriptor(seed)
        for seed in _required_object_list(plan, "seed_registry")
    ]


def _public_docs_compact_seed_descriptor(seed: Mapping[str, Any]) -> dict[str, Any]:
    descriptor = dict(seed)
    crawl_policy = dict(_required_mapping(seed, "crawl_policy"))
    crawl_policy.pop("crawl_evidence", None)
    crawl_policy.pop("previous_state", None)
    descriptor["crawl_policy"] = crawl_policy
    freshness_state = dict(_required_mapping(seed, "freshness_state"))
    freshness_state.pop("page_state", None)
    descriptor["freshness_state"] = freshness_state
    return descriptor


def _compact_public_docs_seed_refresh_payload(
    payload: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    seed_evidence: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    compact = dict(payload)
    compact_registry = _public_docs_compact_seed_registry(plan)
    evidence_by_seed_id: dict[str, Mapping[str, Any]] = {}
    normalized_evidence: list[dict[str, Any]] = []
    for raw_entry in seed_evidence:
        entry = dict(raw_entry)
        summary = _required_mapping(entry, "summary")
        seed_id = _required_str(summary, "seedId")
        if seed_id in evidence_by_seed_id:
            raise ValueError("public docs refresh plan contains duplicate seed evidence")
        evidence_by_seed_id[seed_id] = entry
        normalized_evidence.append(entry)
    if set(evidence_by_seed_id) != {_required_str(seed, "seed_id") for seed in compact_registry}:
        raise ValueError("public docs refresh plan seed evidence does not match its registry")
    compact["seed_evidence"] = normalized_evidence
    compact["seed_registry"] = compact_registry
    compact_requests: list[dict[str, Any]] = []
    for raw_request in _required_object_list(payload, "source_fetch_requests"):
        request = dict(raw_request)
        source_metadata = dict(_required_mapping(request, "source_metadata"))
        frontier = _required_mapping(source_metadata, "frontier")
        parent_seed_id = _required_str(frontier, "parent_seed_id")
        evidence_entry = evidence_by_seed_id.get(parent_seed_id)
        if evidence_entry is None:
            raise ValueError("public docs source request is missing parent seed evidence")
        crawl_policy = dict(_required_mapping(source_metadata, "crawl_policy"))
        crawl_policy.pop("crawl_evidence", None)
        crawl_policy.pop("previous_state", None)
        source_metadata["crawl_policy"] = crawl_policy
        source_metadata["crawl_evidence_reference"] = {
            "evidence": dict(_required_mapping(evidence_entry, "evidence")),
            "summary": dict(
                _required_mapping(_required_mapping(evidence_entry, "summary"), "crawlEvidence")
            ),
        }
        request["source_metadata"] = source_metadata
        compact_requests.append(request)
    compact["source_fetch_requests"] = compact_requests
    return compact


def _read_public_docs_refresh_plan_for_d5(plan: Mapping[str, Any]) -> dict[str, Any]:
    refresh_plan_path = _artifact_path(
        "public_docs_seed_refresh_plan_path",
        _required_str(plan, "public_docs_seed_refresh_plan_path"),
    )
    raw_evidence = plan.get("public_docs_seed_refresh_plan_evidence")
    if refresh_plan_path.startswith("s3://"):
        if not isinstance(raw_evidence, Mapping):
            raise ValueError("public docs S3 refresh plan requires exact WORM evidence")
        evidence = _validated_public_docs_exact_evidence_handle(
            raw_evidence,
            "public docs seed refresh plan evidence",
        )
        if evidence["s3Uri"] != refresh_plan_path:
            raise ValueError("public docs seed refresh plan evidence URI does not match its path")
        payload_bytes = _read_public_docs_exact_evidence_bytes(
            evidence,
            field_name="public docs seed refresh plan evidence",
            s3_client=_s3_client(refresh_plan_path),
            max_bytes=PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES,
        )
        try:
            payload = json.loads(payload_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("public docs seed refresh plan evidence is not valid JSON") from exc
        if not isinstance(payload, Mapping):
            raise ValueError("public docs seed refresh plan evidence must be a JSON object")
        return dict(payload)
    if raw_evidence is not None:
        raise ValueError("local public docs refresh plans must not declare S3 WORM evidence")
    return dict(_read_json_file(refresh_plan_path, "public_docs_seed_refresh_plan"))


def _public_docs_seed_registry_from_refresh_plan(
    refresh_plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    compact_registry = _required_object_list(refresh_plan, "seed_registry")
    raw_seed_evidence = refresh_plan.get("seed_evidence")
    if raw_seed_evidence is None:
        return [dict(seed) for seed in compact_registry]
    if not isinstance(raw_seed_evidence, list) or not raw_seed_evidence:
        raise ValueError("public docs refresh plan seed_evidence must be a non-empty list")
    if len(raw_seed_evidence) != len(compact_registry):
        raise ValueError("public docs refresh plan seed evidence does not match its registry")
    operation_id = _required_str(refresh_plan, "operation_id")
    airflow_plan_path = _required_artifact_paths(refresh_plan, ("airflow_plan",))["airflow_plan"]
    seed_prefix = (
        _artifact_parent_path(airflow_plan_path).rstrip("/") + "/public-docs-seed-evidence/"
    )
    compact_by_seed_id = {_required_str(seed, "seed_id"): dict(seed) for seed in compact_registry}
    if len(compact_by_seed_id) != len(compact_registry):
        raise ValueError("public docs refresh plan registry contains duplicate seed_id values")
    hydrated: list[dict[str, Any]] = []
    total_seed_bytes = 0
    for raw_entry in raw_seed_evidence:
        if not isinstance(raw_entry, Mapping) or set(raw_entry) != {"evidence", "summary"}:
            raise ValueError("public docs refresh plan seed evidence entry shape is invalid")
        evidence = _validated_public_docs_exact_evidence_handle(
            _required_mapping(raw_entry, "evidence"),
            "public docs refresh seed evidence",
        )
        if not evidence["s3Uri"].startswith(seed_prefix):
            raise ValueError("public docs refresh seed evidence escapes its operation prefix")
        seed_bytes = _read_public_docs_exact_evidence_bytes(
            evidence,
            field_name="public docs refresh seed evidence",
            s3_client=_s3_client(evidence["s3Uri"]),
            max_bytes=PUBLIC_DOCS_MAX_SEED_EVIDENCE_BYTES,
        )
        total_seed_bytes += len(seed_bytes)
        if total_seed_bytes > PUBLIC_DOCS_MAX_TOTAL_SEED_EVIDENCE_BYTES:
            raise ValueError("public docs refresh seed evidence exceeds aggregate byte ceiling")
        try:
            seed_snapshot = json.loads(seed_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("public docs refresh seed evidence is not valid JSON") from exc
        if not isinstance(seed_snapshot, Mapping) or set(seed_snapshot) != {
            "operationId",
            "schema",
            "seed",
            "seedId",
        }:
            raise ValueError("public docs refresh seed evidence shape is invalid")
        if _required_str(seed_snapshot, "schema") != _PUBLIC_DOCS_SEED_EVIDENCE_SCHEMA:
            raise ValueError("public docs refresh seed evidence schema is unsupported")
        if _required_str(seed_snapshot, "operationId") != operation_id:
            raise ValueError("public docs refresh seed evidence operationId is mismatched")
        seed = dict(_required_mapping(seed_snapshot, "seed"))
        seed_id = _required_str(seed, "seed_id")
        if _required_str(seed_snapshot, "seedId") != seed_id:
            raise ValueError("public docs refresh seed evidence seedId is mismatched")
        if dict(_required_mapping(raw_entry, "summary")) != _public_docs_seed_evidence_summary(
            seed
        ):
            raise ValueError("public docs refresh seed evidence summary is mismatched")
        if compact_by_seed_id.get(seed_id) != _public_docs_compact_seed_descriptor(seed):
            raise ValueError("public docs compact seed descriptor is mismatched")
        _validate_public_docs_seed_evidence_limits(seed)
        hydrated.append(seed)
    expected_digest = sha256(
        _canonical_json({"seed_registry": hydrated}).encode("utf-8")
    ).hexdigest()
    if _required_str(refresh_plan, "seed_registry_sha256") != expected_digest:
        raise ValueError("public docs refresh registry digest is mismatched")
    return hydrated


_BENCHMARK_SUBSTRATE_SOURCE_SET_ENV = "ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE"


def _load_execution_substrate_source_set(
    source_set_evidence: Mapping[str, Any],
    *,
    s3_client: Any | None = None,
) -> dict[str, dict[str, bytes]]:
    """Load one exact WORM source set and every exact WORM role it declares."""

    from dags.serp_benchmark_catalog import EXTERNAL_EXECUTION_SUBSTRATE_ROLES

    source_handle = _validated_compact_worm_handle(
        source_set_evidence, "benchmark execution substrate source set"
    )
    source_client = s3_client or _s3_client(source_handle["s3Uri"])
    source_bytes, _, _ = _read_compliance_locked_s3_bytes(
        source_client,
        source_handle["s3Uri"],
        field_name="benchmark execution substrate source set",
        version_id=source_handle["versionId"],
    )
    if "sha256:" + sha256(source_bytes).hexdigest() != source_handle["sha256"]:
        raise ValueError("benchmark execution substrate source set digest is mismatched")
    try:
        source_set = json.loads(source_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("benchmark execution substrate source set is not valid JSON") from exc
    if (
        not isinstance(source_set, Mapping)
        or set(source_set) != {"schema", "suites", "supplyAttestationsEvidence"}
        or source_set.get("schema") != "BenchmarkExecutionSubstrateSourceSet/v2"
    ):
        raise ValueError("benchmark execution substrate source set shape is invalid")
    _load_benchmark_supply_attestations(
        source_set.get("supplyAttestationsEvidence"), s3_client=s3_client
    )
    raw_suites = source_set.get("suites")
    if not isinstance(raw_suites, list) or len(raw_suites) != len(
        EXTERNAL_EXECUTION_SUBSTRATE_ROLES
    ):
        raise ValueError("benchmark execution substrate source set suites are incomplete")
    result: dict[str, dict[str, bytes]] = {}
    for index, (expected_suite_id, expected_roles) in enumerate(
        EXTERNAL_EXECUTION_SUBSTRATE_ROLES.items()
    ):
        suite = raw_suites[index]
        if (
            not isinstance(suite, Mapping)
            or set(suite) != {"roles", "suiteId"}
            or suite.get("suiteId") != expected_suite_id
        ):
            raise ValueError("benchmark execution substrate source set suite order is invalid")
        raw_roles = suite.get("roles")
        if not isinstance(raw_roles, list) or len(raw_roles) != len(expected_roles):
            raise ValueError(
                f"benchmark execution substrate source roles are incomplete: {expected_suite_id}"
            )
        loaded_roles: dict[str, bytes] = {}
        for role_index, expected_role in enumerate(expected_roles):
            role = raw_roles[role_index]
            if (
                not isinstance(role, Mapping)
                or set(role) != {"evidence", "role"}
                or role.get("role") != expected_role
            ):
                raise ValueError(
                    "benchmark execution substrate source role order is invalid: "
                    f"{expected_suite_id}"
                )
            handle = _validated_compact_worm_handle(
                role.get("evidence"),
                f"benchmark execution substrate {expected_suite_id}/{expected_role}",
            )
            role_client = s3_client or _s3_client(handle["s3Uri"])
            payload, _, _ = _read_compliance_locked_s3_bytes(
                role_client,
                handle["s3Uri"],
                field_name=f"benchmark execution substrate {expected_suite_id}/{expected_role}",
                version_id=handle["versionId"],
            )
            if "sha256:" + sha256(payload).hexdigest() != handle["sha256"]:
                raise ValueError(
                    f"benchmark execution substrate role digest is mismatched: "
                    f"{expected_suite_id}/{expected_role}"
                )
            loaded_roles[expected_role] = payload
        result[expected_suite_id] = loaded_roles
    return result


def _load_benchmark_supply_attestations(evidence: object, *, s3_client: Any | None = None) -> None:
    handle = _validated_compact_worm_handle(evidence, "benchmark substrate supply attestations")
    client = s3_client or _s3_client(handle["s3Uri"])
    payload, _, _ = _read_compliance_locked_s3_bytes(
        client,
        handle["s3Uri"],
        field_name="benchmark substrate supply attestations",
        version_id=handle["versionId"],
    )
    if "sha256:" + sha256(payload).hexdigest() != handle["sha256"]:
        raise ValueError("benchmark substrate supply attestations digest is mismatched")
    manifest = _canonical_json_object_bytes(payload, "benchmark substrate supply attestations")
    if (
        set(manifest) != {"ds1000", "schema", "sweBench"}
        or manifest.get("schema") != "BenchmarkSubstrateSupplyAttestations/v1"
    ):
        raise ValueError("benchmark substrate supply attestations shape is invalid")
    ds1000 = manifest.get("ds1000")
    if (
        not isinstance(ds1000, Mapping)
        or set(ds1000) != {"imageReference", "sbomEvidence", "signatureStatus"}
        or ds1000.get("signatureStatus") != "signed-and-verified"
        or not isinstance(ds1000.get("imageReference"), str)
        or not re.fullmatch(
            r"harbor\.adapstory\.com/benchmark-sandboxes/ds1000@sha256:[0-9a-f]{64}",
            ds1000["imageReference"],
        )
    ):
        raise ValueError("DS-1000 supply attestation is invalid")
    _validated_compact_worm_handle(ds1000.get("sbomEvidence"), "DS-1000 SBOM")

    swe_bench = manifest.get("sweBench")
    if (
        not isinstance(swe_bench, Mapping)
        or set(swe_bench) != {"datasetRevision", "images"}
        or not isinstance(swe_bench.get("datasetRevision"), str)
        or not re.fullmatch(r"[0-9a-f]{40}", swe_bench["datasetRevision"])
        or not isinstance(swe_bench.get("images"), list)
        or len(swe_bench["images"]) != 500
    ):
        raise ValueError("SWE-bench supply attestations are incomplete")
    instance_ids: list[str] = []
    for image in swe_bench["images"]:
        if (
            not isinstance(image, Mapping)
            or set(image)
            != {
                "imageReference",
                "instanceId",
                "sbomEvidence",
                "signatureStatus",
            }
            or image.get("signatureStatus") != "signed-and-verified"
            or not isinstance(image.get("instanceId"), str)
            or not isinstance(image.get("imageReference"), str)
            or not re.fullmatch(
                r"harbor\.adapstory\.com/benchmark-sandboxes/swe-bench/"
                r"[a-z0-9._/-]+@sha256:[0-9a-f]{64}",
                image["imageReference"],
            )
        ):
            raise ValueError("SWE-bench supply attestation is invalid")
        _validated_compact_worm_handle(
            image.get("sbomEvidence"),
            f"SWE-bench {image['instanceId']} SBOM",
        )
        instance_ids.append(image["instanceId"])
    if instance_ids != sorted(instance_ids) or len(set(instance_ids)) != 500:
        raise ValueError("SWE-bench supply attestation identities are invalid")


def _validated_compact_worm_handle(value: object, field_name: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {
        "objectLockMode",
        "s3Uri",
        "sha256",
        "versionId",
    }:
        raise ValueError(f"{field_name} evidence shape is invalid")
    handle = {key: _required_str(value, key) for key in value}
    if handle["objectLockMode"] != "COMPLIANCE":
        raise ValueError(f"{field_name} must declare COMPLIANCE object lock")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", handle["sha256"]):
        raise ValueError(f"{field_name} digest must be sha256:<64 lowercase hex>")
    _artifact_ref(field_name, handle["s3Uri"])
    return handle


def _execution_substrate_source_set_from_env() -> dict[str, dict[str, bytes]]:
    raw = os.environ.get(_BENCHMARK_SUBSTRATE_SOURCE_SET_ENV, "").strip()
    if not raw:
        return {}
    return _load_execution_substrate_source_set(
        _json_object(raw, _BENCHMARK_SUBSTRATE_SOURCE_SET_ENV)
    )


def materialize_live_benchmark_catalog_artifact(
    plan_json: Mapping[str, Any] | str,
    *,
    fetch_bytes: Callable[[str], bytes] | None = None,
    snapshot_writer: Callable[..., dict[str, Any]] | None = None,
    snapshot_bytes_writer: Callable[..., dict[str, Any]] | None = None,
    native_adapter_materializer: Callable[
        [str, Mapping[str, bytes], Mapping[str, Mapping[str, object]]], Mapping[str, object]
    ]
    | None = None,
    native_corpus_materializer: Callable[
        [str, Mapping[str, bytes], Mapping[str, Mapping[str, object]]], Mapping[str, object]
    ]
    | None = None,
    execution_substrate_materializer: Callable[
        [
            str,
            Mapping[str, bytes],
            Mapping[str, Mapping[str, object]],
            Mapping[str, bytes],
            Mapping[str, Mapping[str, object]],
            Mapping[str, bytes],
        ],
        Mapping[str, bytes],
    ]
    | None = None,
    execution_substrate_role_payload_loader: Callable[[], Mapping[str, Mapping[str, bytes]]]
    | None = None,
) -> dict[str, Any]:
    """Capture all upstream benchmark licensing evidence in immutable storage.

    This is deliberately a separate D6 task.  The following suite-materializer
    may only consume this version-bound snapshot; it must never trust values
    supplied through ``dag_run.conf`` or a mutable dataset-card URL.
    """

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") not in {
        "serp_nightly_regression_suite",
        "serp_mandatory_benchmark_dataset_evidence_snapshot",
        "serp_benchmark_improvement_wave",
    }:
        raise ValueError("plan dag_id does not match benchmark catalog materializer")
    artifact_paths = _required_artifact_paths(plan, ("benchmark_catalog",))
    from dags.serp_benchmark_catalog import build_live_benchmark_catalog_evidence

    artifact_root_path = _artifact_parent_path(artifact_paths["benchmark_catalog"])
    bytes_writer = (
        write_immutable_evidence_bytes_snapshot
        if snapshot_bytes_writer is None
        else snapshot_bytes_writer
    )

    def snapshot_bytes(
        suite_id: str,
        evidence_type: str,
        source_url: str,
        payload: bytes,
    ) -> dict[str, Any]:
        artifact_key = sha256(f"{suite_id}:{evidence_type}:{source_url}".encode()).hexdigest()
        return bytes_writer(
            artifact_path=(
                f"{artifact_root_path}/benchmark-catalog-inputs/{artifact_key}-{evidence_type}.bin"
            ),
            artifact_type=f"benchmark_catalog_{evidence_type}",
            operation_id=_required_str(plan, "operation_id"),
            payload=payload,
            content_type="application/octet-stream",
        )

    evidence = build_live_benchmark_catalog_evidence(
        observed_at=_required_datetime_string(plan, "generated_at"),
        fetch_bytes=_fetch_https_bytes if fetch_bytes is None else fetch_bytes,
        snapshot_bytes=snapshot_bytes,
        native_adapter_materializer=(
            _native_adapter_materializer
            if native_adapter_materializer is None
            else native_adapter_materializer
        ),
        native_corpus_materializer=(
            _native_corpus_materializer
            if native_corpus_materializer is None
            else native_corpus_materializer
        ),
        execution_substrate_materializer=(
            _execution_substrate_materializer
            if execution_substrate_materializer is None
            else execution_substrate_materializer
        ),
        execution_substrate_role_payloads=(
            _execution_substrate_source_set_from_env()
            if execution_substrate_role_payload_loader is None
            else execution_substrate_role_payload_loader()
        ),
    )
    writer = write_immutable_evidence_snapshot if snapshot_writer is None else snapshot_writer
    snapshot = writer(
        artifact_path=artifact_paths["benchmark_catalog"],
        artifact_type="benchmark_catalog",
        operation_id=_required_str(plan, "operation_id"),
        payload=evidence,
    )
    if not isinstance(snapshot, Mapping):
        raise ValueError("benchmark catalog snapshot writer returned an invalid result")
    result = dict(snapshot)
    result["catalogStatus"] = _required_str(evidence, "catalog_status")
    suites = _required_object_list(evidence, "suites")
    result["blockingSuiteIds"] = [
        _required_str(suite, "suite_id")
        for suite in suites
        if _required_str(suite, "execution_status") != "ready"
    ]
    result["officialHarnessLineage"] = _catalog_official_harness_lineage(suites)
    result["suiteSummary"] = _catalog_suite_summary(suites)
    return result


def _native_adapter_materializer(
    suite_id: str,
    dataset_payloads: Mapping[str, bytes],
    dataset_snapshots: Mapping[str, Mapping[str, object]],
) -> Mapping[str, object]:
    """Load the image-owned adapter only inside the isolated catalog workload."""

    from adapstory_serp_pipeline.benchmark.native_adapters import (  # type: ignore[import-not-found]
        build_native_case_manifest,
    )

    return cast(
        Mapping[str, object],
        build_native_case_manifest(
            suite_id=suite_id,
            dataset_payloads=dataset_payloads,
            dataset_snapshots=dataset_snapshots,
        ),
    )


def _native_corpus_materializer(
    suite_id: str,
    dataset_payloads: Mapping[str, bytes],
    dataset_snapshots: Mapping[str, Mapping[str, object]],
) -> Mapping[str, object]:
    """Derive only query-independent corpus bytes inside the isolated workload."""

    from adapstory_serp_pipeline.benchmark.native_corpus import (  # type: ignore[import-not-found]
        derive_native_benchmark_corpus,
    )

    materialization = derive_native_benchmark_corpus(
        suite_id=suite_id,
        dataset_payloads=dataset_payloads,
        dataset_snapshots=dataset_snapshots,
    )
    return {
        "manifest": dict(materialization.manifest),
        "payloads": dict(materialization.payloads),
    }


def _execution_substrate_materializer(
    suite_id: str,
    dataset_payloads: Mapping[str, bytes],
    dataset_snapshots: Mapping[str, Mapping[str, object]],
    corpus_payloads: Mapping[str, bytes],
    corpus_snapshots: Mapping[str, Mapping[str, object]],
    official_harness_payloads: Mapping[str, bytes],
) -> Mapping[str, bytes]:
    """Materialize exact role bytes only inside the isolated catalog workload."""

    from adapstory_serp_pipeline.benchmark.execution_substrate_materialization import (  # type: ignore[import-not-found]
        materialize_execution_substrate_role_payloads,
    )

    return cast(
        Mapping[str, bytes],
        materialize_execution_substrate_role_payloads(
            suite_id,
            dataset_payloads,
            dataset_snapshots,
            corpus_payloads,
            corpus_snapshots,
            official_harness_payloads,
        ),
    )


def load_materialized_benchmark_catalog_snapshot(
    plan_json: Mapping[str, Any] | str,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Load a KPO-produced catalog receipt and bind the catalog's exact S3 version.

    The scheduler never trusts pod stdout as benchmark provenance.  It reads the
    receipt from the isolated executor's WORM path, then checks the receipt's
    catalog VersionId, SHA-256, status, and blocking suites against the exact
    catalog object before handing it to a D6 suite-plan or D19 paired-eval writer.
    """

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") not in {
        "serp_nightly_regression_suite",
        "serp_mandatory_benchmark_dataset_evidence_snapshot",
        "serp_benchmark_improvement_wave",
    }:
        raise ValueError("plan dag_id does not match benchmark catalog receipt loader")
    artifact_paths = _required_artifact_paths(
        plan,
        ("benchmark_catalog", "benchmark_catalog_receipt"),
    )
    client = s3_client or _s3_client(*artifact_paths.values())
    receipt_bytes, receipt_version_id, receipt_retain_until = _read_compliance_locked_s3_bytes(
        client,
        artifact_paths["benchmark_catalog_receipt"],
        field_name="benchmark_catalog_receipt",
    )
    receipt = _canonical_json_object_bytes(
        receipt_bytes,
        "benchmark_catalog_materialization_receipt",
    )
    if _required_str(receipt, "contractVersion") != "serp-benchmark-catalog-materializer/v5":
        raise ValueError("benchmark catalog materialization receipt contract is unsupported")
    if _required_str(receipt, "dagId") != _required_str(plan, "dag_id"):
        raise ValueError("benchmark catalog materialization receipt dagId does not match plan")
    if _required_str(receipt, "operationId") != _required_str(plan, "operation_id"):
        raise ValueError(
            "benchmark catalog materialization receipt operationId does not match plan"
        )
    catalog_snapshot = dict(_required_mapping(receipt, "catalogSnapshot"))
    if _required_str(catalog_snapshot, "artifactPath") != artifact_paths["benchmark_catalog"]:
        raise ValueError("benchmark catalog receipt must match the plan artifact path")
    catalog_version_id = _required_str(catalog_snapshot, "artifactVersionId")
    if _required_str(catalog_snapshot, "objectLockMode") != "COMPLIANCE":
        raise ValueError("benchmark catalog receipt must use COMPLIANCE object lock")
    catalog_bytes, observed_catalog_version_id, catalog_retain_until = (
        _read_compliance_locked_s3_bytes(
            client,
            artifact_paths["benchmark_catalog"],
            field_name="benchmark_catalog",
            version_id=catalog_version_id,
        )
    )
    if observed_catalog_version_id != catalog_version_id:
        raise ValueError("benchmark catalog receipt VersionId does not match catalog object")
    if sha256(catalog_bytes).hexdigest() != _required_str(catalog_snapshot, "artifactSha256"):
        raise ValueError("benchmark catalog receipt SHA-256 does not match catalog object")
    catalog = _canonical_json_object_bytes(catalog_bytes, "benchmark_catalog")
    if _required_str(catalog, "catalog_status") != _required_str(catalog_snapshot, "catalogStatus"):
        raise ValueError("benchmark catalog receipt status does not match catalog object")
    if _required_str(catalog, "contract_version") != "serp-benchmark-catalog/v5":
        raise ValueError("benchmark catalog contract is unsupported")
    if set(catalog) != {"catalog_status", "contract_version", "observed_at", "suites"}:
        raise ValueError("benchmark catalog object has an invalid v5 shape")
    suites = _required_object_list(catalog, "suites")
    suite_ids = [_required_str(suite, "suite_id") for suite in suites]
    if suite_ids != list(MANDATORY_SERP_BENCHMARK_SUITES):
        raise ValueError(
            "benchmark catalog object must contain mandatory suites in canonical order"
        )
    _validate_benchmark_catalog_suite_shapes(suites)
    _validate_benchmark_catalog_corpus_evidence(suites)
    _validate_benchmark_catalog_execution_substrate_artifacts(suites)
    if normalize_benchmark_catalog_suite_summary(
        catalog_snapshot.get("suiteSummary")
    ) != _catalog_suite_summary(suites):
        raise ValueError("benchmark catalog receipt suite summary does not match catalog object")
    if normalize_benchmark_catalog_official_harness_lineage(
        catalog_snapshot.get("officialHarnessLineage")
    ) != _catalog_official_harness_lineage(suites):
        raise ValueError(
            "benchmark catalog receipt official harness lineage does not match catalog object"
        )
    actual_blocking_suite_ids = [
        _required_str(suite, "suite_id")
        for suite in suites
        if _required_str(suite, "execution_status") != "ready"
    ]
    if actual_blocking_suite_ids != _required_str_list_allow_empty(
        catalog_snapshot, "blockingSuiteIds"
    ):
        raise ValueError("benchmark catalog receipt blocking suites do not match catalog object")
    blocking_reason_by_suite = _catalog_blocking_reason_by_suite(suites)
    if list(blocking_reason_by_suite) != actual_blocking_suite_ids:
        raise ValueError("catalog-evidence-invalid: blocking reasons do not match catalog suites")
    return {
        **catalog_snapshot,
        "blockingReasonBySuite": blocking_reason_by_suite,
        "catalogReceiptPath": artifact_paths["benchmark_catalog_receipt"],
        "catalogReceiptSha256": sha256(receipt_bytes).hexdigest(),
        "catalogReceiptVersionId": receipt_version_id,
        "catalogReceiptRetainUntil": receipt_retain_until,
        "catalogRetainUntil": catalog_retain_until,
    }


def normalize_benchmark_catalog_suite_summary(value: object) -> list[dict[str, str]]:
    """Validate the safe, self-describing nine-suite receipt summary.

    This deliberately excludes URLs, dataset bytes, hashes, and credentials.
    The immutable catalog remains the detailed provenance source; the summary
    makes a receipt and task log independently auditable without expanding
    access to protected evidence.
    """

    if not isinstance(value, list):
        raise ValueError("benchmark catalog suite summary must be a list")
    normalized: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {
            "distributionRule",
            "executionStatus",
            "rightsStatus",
            "suiteId",
        }:
            raise ValueError("benchmark catalog suite summary has an invalid shape")
        execution_status = _required_str(item, "executionStatus")
        if execution_status not in _CATALOG_EXECUTION_STATUSES:
            raise ValueError("benchmark catalog suite summary has an unsupported execution status")
        rights_status = _required_str(item, "rightsStatus")
        if rights_status not in _DATASET_RIGHTS_STATUSES:
            raise ValueError("benchmark catalog suite summary has an unsupported rights status")
        distribution_rule = _required_str(item, "distributionRule")
        if distribution_rule not in _ALLOWED_DATASET_DISTRIBUTION_RULES:
            raise ValueError("benchmark catalog suite summary has an unsupported distribution rule")
        if (
            rights_status == "rights-unverified"
            and distribution_rule != _RIGHTS_UNVERIFIED_DISTRIBUTION_RULE
        ):
            raise ValueError(
                "benchmark catalog suite summary must keep unverified rights internal-only"
            )
        normalized.append(
            {
                "distributionRule": distribution_rule,
                "executionStatus": execution_status,
                "rightsStatus": rights_status,
                "suiteId": _required_str(item, "suiteId"),
            }
        )
    if [item["suiteId"] for item in normalized] != list(MANDATORY_SERP_BENCHMARK_SUITES):
        raise ValueError("benchmark catalog suite summary must use canonical suite order")
    return normalized


def normalize_benchmark_catalog_official_harness_lineage(
    value: object,
) -> list[dict[str, str]]:
    """Validate the exact official harness source and code-license lineage."""

    required_fields = {
        "entrypoint",
        "harnessLicenseId",
        "harnessLicenseSha256",
        "harnessLicenseStatus",
        "harnessSourceArchiveSha256",
        "revision",
        "suiteId",
    }
    if not isinstance(value, list):
        raise ValueError("benchmark catalog official harness lineage must be a list")
    normalized: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != required_fields:
            raise ValueError("benchmark catalog official harness lineage has an invalid shape")
        revision = _required_str(item, "revision")
        if len(revision) != 40 or any(
            character not in "0123456789abcdef" for character in revision
        ):
            raise ValueError("benchmark catalog official harness revision must be a git SHA")
        license_status = _required_str(item, "harnessLicenseStatus")
        if license_status not in {"ATTESTED", "UNDECLARED"}:
            raise ValueError("benchmark catalog official harness license status is unsupported")
        normalized.append(
            {
                "entrypoint": _required_str(item, "entrypoint"),
                "harnessLicenseId": _required_str(item, "harnessLicenseId"),
                "harnessLicenseSha256": _normalized_catalog_sha256_digest(
                    _required_str(item, "harnessLicenseSha256"),
                    "harnessLicenseSha256",
                ),
                "harnessLicenseStatus": license_status,
                "harnessSourceArchiveSha256": _normalized_catalog_sha256_digest(
                    _required_str(item, "harnessSourceArchiveSha256"),
                    "harnessSourceArchiveSha256",
                ),
                "revision": revision,
                "suiteId": _required_str(item, "suiteId"),
            }
        )
    if [item["suiteId"] for item in normalized] != list(MANDATORY_SERP_BENCHMARK_SUITES):
        raise ValueError("benchmark catalog official harness lineage must use canonical order")
    return normalized


def _normalized_catalog_sha256_digest(value: str, field_name: str) -> str:
    normalized = value.removeprefix("sha256:")
    if not re.fullmatch(r"[a-f0-9]{64}", normalized):
        raise ValueError(f"benchmark catalog {field_name} must be a SHA-256 digest")
    return f"sha256:{normalized}"


def _catalog_suite_summary(suites: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    return normalize_benchmark_catalog_suite_summary(
        [
            {
                "distributionRule": _required_str(suite, "distribution_rule"),
                "executionStatus": _required_str(suite, "execution_status"),
                "rightsStatus": _required_str(suite, "rights_status"),
                "suiteId": _required_str(suite, "suite_id"),
            }
            for suite in suites
        ]
    )


def _catalog_official_harness_lineage(
    suites: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    lineage: list[dict[str, str]] = []
    for suite in suites:
        official_harness = _required_mapping(suite, "official_harness")
        source_archive_snapshot = _required_mapping(official_harness, "source_archive_snapshot")
        license_snapshot = _required_mapping(official_harness, "license_snapshot")
        lineage.append(
            {
                "entrypoint": _required_str(official_harness, "entrypoint"),
                "harnessLicenseId": _required_str(official_harness, "license_id"),
                "harnessLicenseSha256": _required_str(license_snapshot, "sha256"),
                "harnessLicenseStatus": _required_str(official_harness, "license_status"),
                "harnessSourceArchiveSha256": _required_str(source_archive_snapshot, "sha256"),
                "revision": _required_str(official_harness, "revision"),
                "suiteId": _required_str(suite, "suite_id"),
            }
        )
    return normalize_benchmark_catalog_official_harness_lineage(lineage)


def _validate_benchmark_catalog_corpus_evidence(
    suites: Sequence[Mapping[str, Any]],
) -> None:
    """Reject a ready suite unless its query-independent corpus is WORM-bound."""

    for suite in suites:
        suite_id = _required_str(suite, "suite_id")
        execution_status = _required_str(suite, "execution_status")
        corpus_snapshots = _required_mapping(suite, "corpus_snapshots")
        native_manifest = _required_mapping(suite, "native_adapter_manifest")
        if execution_status == "corpus-evidence-blocked":
            if corpus_snapshots:
                raise ValueError(
                    "benchmark catalog corpus-blocked suite cannot expose corpus snapshots: "
                    + suite_id
                )
            if "corpusManifest" in native_manifest or "corpusEvidence" in native_manifest:
                raise ValueError(
                    "benchmark catalog corpus-blocked suite cannot expose corpus lineage: "
                    + suite_id
                )
            continue
        if not corpus_snapshots:
            raise ValueError(
                f"benchmark catalog runnable suite requires corpus snapshots: {suite_id}"
            )
        corpus_manifest = _required_mapping(native_manifest, "corpusManifest")
        if _required_str(corpus_manifest, "schema") != "NativeBenchmarkCorpusManifest/v1":
            raise ValueError(f"benchmark catalog corpus schema is unsupported: {suite_id}")
        if _required_str(corpus_manifest, "suiteId") != suite_id:
            raise ValueError(f"benchmark catalog corpus suite identity mismatch: {suite_id}")
        if _required_str(corpus_manifest, "status") != "materialized":
            raise ValueError(f"benchmark catalog corpus is not materialized: {suite_id}")
        sources = _required_object_list(corpus_manifest, "sources")
        source_ids = [_required_str(source, "sourceId") for source in sources]
        if source_ids != list(corpus_snapshots):
            raise ValueError(f"benchmark catalog corpus source order mismatch: {suite_id}")
        expected_evidence: list[dict[str, str]] = []
        for source, source_id in zip(sources, source_ids, strict=True):
            snapshot = _required_mapping(corpus_snapshots, source_id)
            corpus_role = _required_str(snapshot, "corpus_role")
            if _required_str(source, "corpusRole") != corpus_role:
                raise ValueError(f"benchmark catalog corpus role mismatch: {suite_id}")
            digest = _normalized_catalog_sha256_digest(
                _required_str(snapshot, "sha256"),
                f"{suite_id}.corpus_snapshots.{source_id}.sha256",
            )
            if (
                _normalized_catalog_sha256_digest(
                    _required_str(source, "payloadSha256"),
                    f"{suite_id}.corpusManifest.payloadSha256",
                )
                != digest
            ):
                raise ValueError(f"benchmark catalog corpus digest mismatch: {suite_id}")
            if _required_str(snapshot, "url") != (
                f"derived://native-corpus/{suite_id}/{source_id}"
            ):
                raise ValueError(f"benchmark catalog corpus URL mismatch: {suite_id}")
            artifact = _required_mapping(snapshot, "immutable_artifact")
            if _required_str(artifact, "objectLockMode") != "COMPLIANCE":
                raise ValueError(f"benchmark catalog corpus must be COMPLIANCE WORM: {suite_id}")
            artifact_sha = _required_str(artifact, "artifactSha256")
            if "sha256:" + artifact_sha != digest:
                raise ValueError(f"benchmark catalog corpus artifact digest mismatch: {suite_id}")
            artifact_path = _required_str(artifact, "artifactPath")
            if not artifact_path.startswith("s3://"):
                raise ValueError(f"benchmark catalog corpus path must be s3://: {suite_id}")
            expected_evidence.append(
                {
                    "artifactPath": artifact_path,
                    "artifactSha256": artifact_sha,
                    "artifactVersionId": _required_str(artifact, "artifactVersionId"),
                    "corpusRole": corpus_role,
                    "objectLockMode": "COMPLIANCE",
                    "sourceId": source_id,
                }
            )
        corpus_evidence = native_manifest.get("corpusEvidence")
        if not isinstance(corpus_evidence, list) or corpus_evidence != expected_evidence:
            raise ValueError(f"benchmark catalog corpus evidence mismatch: {suite_id}")


def _validate_benchmark_catalog_suite_shapes(
    suites: Sequence[Mapping[str, Any]],
) -> None:
    required_fields = {
        "corpus_snapshots",
        "dataset_id",
        "dataset_license_id",
        "dataset_revision",
        "dataset_snapshots",
        "distribution_rule",
        "execution_status",
        "execution_substrate_artifacts",
        "legal_boundary",
        "license_snapshot",
        "native_adapter_manifest",
        "official_harness",
        "rights_status",
        "source_snapshot",
        "suite_id",
    }
    for suite in suites:
        expected = set(required_fields)
        if _required_str(suite, "execution_status") != "ready":
            expected.add("blocking_reason")
        if set(suite) != expected:
            raise ValueError(
                "benchmark catalog suite has an invalid v5 shape: "
                + _required_str(suite, "suite_id")
            )


def _validate_benchmark_catalog_execution_substrate_artifacts(
    suites: Sequence[Mapping[str, Any]],
) -> None:
    from dags.serp_benchmark_catalog import MANDATORY_EXECUTION_SUBSTRATE_ROLES

    for suite in suites:
        suite_id = _required_str(suite, "suite_id")
        execution_status = _required_str(suite, "execution_status")
        artifacts = _required_mapping(suite, "execution_substrate_artifacts")
        if execution_status in {"corpus-evidence-blocked", "execution-substrate-blocked"}:
            if artifacts:
                raise ValueError(
                    f"benchmark catalog blocked suite cannot expose partial substrate: {suite_id}"
                )
            continue
        expected_roles = MANDATORY_EXECUTION_SUBSTRATE_ROLES[suite_id]
        if tuple(artifacts) != expected_roles:
            raise ValueError(
                f"benchmark catalog execution substrate roles are incomplete: {suite_id}"
            )
        identities: list[tuple[str, str]] = []
        for role, value in artifacts.items():
            if not isinstance(value, Mapping) or set(value) != {
                "artifactPath",
                "artifactSha256",
                "artifactVersionId",
                "objectLockMode",
            }:
                raise ValueError(
                    f"benchmark catalog execution substrate handle is invalid: {suite_id}/{role}"
                )
            path = _required_str(value, "artifactPath")
            if not path.startswith("s3://"):
                raise ValueError(
                    f"benchmark catalog execution substrate path must use s3://: {suite_id}/{role}"
                )
            _normalized_catalog_sha256_digest(
                _required_str(value, "artifactSha256"),
                f"{suite_id}.execution_substrate_artifacts.{role}.artifactSha256",
            )
            if _required_str(value, "objectLockMode") != "COMPLIANCE":
                raise ValueError(
                    f"benchmark catalog execution substrate must be COMPLIANCE: {suite_id}/{role}"
                )
            identities.append((path, _required_str(value, "artifactVersionId")))
        if len(set(identities)) != len(identities):
            raise ValueError(
                f"benchmark catalog execution substrate handles must be unique: {suite_id}"
            )


def _catalog_blocking_reason_by_suite(
    suites: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """Derive operator-facing D6 blocks from the immutable catalog itself.

    A legal snapshot can be valid while a runner is still absent.  Keep that
    distinction explicit so scheduled D6 failures cannot be mistaken for a
    licensing decision or bypassed by a caller-provided reason.
    """

    reasons: dict[str, str] = {}
    for suite in suites:
        suite_id = _required_str(suite, "suite_id")
        execution_status = _required_str(suite, "execution_status")
        if execution_status not in _CATALOG_EXECUTION_STATUSES:
            raise ValueError(
                "catalog-evidence-invalid: unsupported execution_status for " + suite_id
            )
        if execution_status == "ready":
            continue
        reason = _required_str(suite, "blocking_reason")
        if not reason.startswith(
            (
                "execution-substrate-unavailable: ",
                "query-independent-corpus-unavailable: ",
                "rights-policy-blocked: ",
            )
        ):
            raise ValueError(
                "catalog-evidence-invalid: blocking_reason is unsupported for " + suite_id
            )
        reasons[suite_id] = reason
    return reasons


def _read_compliance_locked_s3_bytes(
    s3_client: Any,
    artifact_path: str,
    *,
    field_name: str,
    version_id: str | None = None,
    max_bytes: int | None = None,
) -> tuple[bytes, str, str]:
    artifact = _artifact_ref(field_name, artifact_path)
    if artifact.kind != "s3":
        raise ValueError(f"{field_name} must be an s3:// immutable artifact")
    bucket = _required_str_ref(artifact.bucket)
    key = _required_str_ref(artifact.key)
    head_kwargs: dict[str, str] = {"Bucket": bucket, "Key": key}
    if version_id is not None:
        head_kwargs["VersionId"] = version_id
    head = s3_client.head_object(**head_kwargs)
    if not isinstance(head, Mapping):
        raise ValueError(f"{field_name} HeadObject response is invalid")
    observed_version_id = head.get("VersionId")
    if not isinstance(observed_version_id, str) or not observed_version_id.strip():
        raise ValueError(f"{field_name} HeadObject is missing VersionId")
    if version_id is not None and observed_version_id != version_id:
        raise ValueError(f"{field_name} HeadObject VersionId does not match requested version")
    if head.get("ObjectLockMode") != "COMPLIANCE":
        raise ValueError(f"{field_name} must use COMPLIANCE object lock")
    retain_until = head.get("ObjectLockRetainUntilDate")
    if not isinstance(retain_until, datetime) or retain_until.tzinfo is None:
        raise ValueError(f"{field_name} must include a retention timestamp")
    if retain_until.astimezone(UTC) <= datetime.now(UTC):
        raise ValueError(f"{field_name} retention is expired")
    content_length = head.get("ContentLength")
    if max_bytes is not None:
        if isinstance(max_bytes, bool) or max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        if (
            isinstance(content_length, bool)
            or not isinstance(content_length, int)
            or content_length < 0
        ):
            raise ValueError(f"{field_name} HeadObject is missing a valid ContentLength")
        if content_length > max_bytes:
            raise ValueError(f"{field_name} exceeds the governed byte ceiling")
    response = s3_client.get_object(Bucket=bucket, Key=key, VersionId=observed_version_id)
    if not isinstance(response, Mapping):
        raise ValueError(f"{field_name} GetObject response is invalid")
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise ValueError(f"{field_name} GetObject response is missing Body")
    payload = body.read(max_bytes + 1) if max_bytes is not None else body.read()
    if not isinstance(payload, bytes) or not payload:
        raise ValueError(f"{field_name} object is empty")
    if max_bytes is not None:
        if len(payload) > max_bytes:
            raise ValueError(f"{field_name} exceeds the governed byte ceiling")
        if len(payload) != content_length:
            raise ValueError(f"{field_name} ContentLength does not match its body")
    return (
        payload,
        observed_version_id,
        retain_until.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    )


def _fetch_https_bytes(url: str) -> bytes:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("benchmark upstream evidence URLs must use https")
    huggingface_artifact = _huggingface_dataset_artifact(parsed)
    if huggingface_artifact is not None:
        return _fetch_huggingface_dataset_bytes(*huggingface_artifact)
    request = Request(
        url,
        headers={
            "Accept": "application/json, text/plain, text/markdown;q=0.9, */*;q=0.1",
            "User-Agent": "adapstory-serp-benchmark-catalog/1.0",
        },
    )
    try:
        with _open_public_docs_crawler_request(request, timeout=30) as response:
            payload = response.read()
    except (HTTPError, URLError, OSError) as exc:
        raise ValueError(f"benchmark upstream evidence fetch failed: {url}") from exc
    if not isinstance(payload, bytes) or not payload:
        raise ValueError(f"benchmark upstream evidence fetch returned no bytes: {url}")
    return payload


def _huggingface_dataset_artifact(parsed_url: ParseResult) -> tuple[str, str, str] | None:
    """Return a pinned Hub dataset reference only for immutable resolve URLs."""
    if parsed_url.hostname != "huggingface.co":
        return None
    match = re.fullmatch(
        r"/datasets/([^/]+)/([^/]+)/resolve/([0-9a-f]{40})/(.+)",
        parsed_url.path,
    )
    if match is None:
        return None
    namespace, repository, revision, filename = match.groups()
    return f"{unquote(namespace)}/{unquote(repository)}", revision, unquote(filename)


def _fetch_huggingface_dataset_bytes(repo_id: str, revision: str, filename: str) -> bytes:
    """Download a pinned Hub dataset file through the official Xet-aware client."""
    hub = importlib.import_module("huggingface_hub")
    download = getattr(hub, "hf_hub_download", None)
    if not callable(download):
        raise ValueError("huggingface_hub does not provide hf_hub_download")
    _configure_huggingface_proxy_transport(hub)
    token = os.environ.get("ADAPSTORY_SERP_HUGGINGFACE_TOKEN") or None
    try:
        local_path = download(
            filename=filename,
            repo_id=repo_id,
            repo_type="dataset",
            revision=revision,
            token=token,
        )
        payload = Path(local_path).read_bytes()
    except Exception as exc:
        raise ValueError(
            f"Hugging Face dataset download failed for {repo_id}@{revision}:{filename}"
        ) from exc
    if not payload:
        raise ValueError(f"Hugging Face dataset download returned no bytes: {repo_id}:{filename}")
    return payload


def _configure_huggingface_proxy_transport(hub: Any) -> None:
    """Route Hub metadata and Xet transfers through the approved source proxy."""

    proxy_url = _public_docs_source_proxy_url()
    if proxy_url is None:
        return
    httpx = importlib.import_module("httpx")
    client_class = getattr(httpx, "Client", None)
    timeout_class = getattr(httpx, "Timeout", None)
    set_client_factory = getattr(hub, "set_client_factory", None)
    close_session = getattr(hub, "close_session", None)
    if not (
        callable(client_class)
        and callable(timeout_class)
        and callable(set_client_factory)
        and callable(close_session)
    ):
        raise ValueError("huggingface_hub proxy transport configuration is unavailable")
    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    os.environ["NO_PROXY"] = _benchmark_source_no_proxy_value()
    set_client_factory(
        lambda: client_class(
            follow_redirects=True,
            proxy=proxy_url,
            timeout=timeout_class(
                _HUGGINGFACE_PROXY_READ_TIMEOUT_SECONDS,
                connect=_HUGGINGFACE_PROXY_CONNECT_TIMEOUT_SECONDS,
                read=_HUGGINGFACE_PROXY_READ_TIMEOUT_SECONDS,
                write=_HUGGINGFACE_PROXY_WRITE_TIMEOUT_SECONDS,
                pool=_HUGGINGFACE_PROXY_POOL_TIMEOUT_SECONDS,
            ),
            trust_env=False,
        )
    )
    close_session()


def _benchmark_source_no_proxy_value() -> str:
    existing = [
        value.strip() for value in os.environ.get("NO_PROXY", "").split(",") if value.strip()
    ]
    required_hosts = ("localhost", "127.0.0.1", ".svc", ".svc.cluster.local")
    return ",".join(dict.fromkeys((*existing, *required_hosts)))


def read_evidence_artifact(artifact_path: str, field_name: str) -> dict[str, Any]:
    """Read a JSON evidence object through the canonical local/S3 artifact transport."""
    return dict(_read_json_file(_artifact_path(field_name, artifact_path), field_name))


def validate_internal_service_base_url(value: str, field_name: str) -> str:
    """Accept only HTTPS, localhost HTTP, or Kubernetes Service DNS HTTP endpoints."""
    return _required_internal_or_https_base_url({field_name: value}, field_name).rstrip("/")


def post_bc21_json(
    base_url: str,
    path: str,
    *,
    body: Mapping[str, Any],
    headers: Mapping[str, str],
    error_label: str,
) -> dict[str, Any]:
    """Submit one authenticated BC-21 JSON mutation without exposing transport internals."""
    normalized_base = validate_internal_service_base_url(base_url, "bc21_base_url")
    if not path.startswith("/api/") or "?" in path or "#" in path:
        raise ValueError("BC-21 path must be an absolute API path without parameters")
    _reject_raw_secrets(body)
    return dict(
        _bc21_json_request(
            normalized_base + path,
            method="POST",
            body=body,
            headers=headers,
            error_label=error_label,
        )
        or {}
    )


def write_public_docs_seed_registry_from_snapshot(
    plan_handle: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan, snapshot = _load_public_docs_airflow_plan_snapshot_bundle(plan_handle)
    artifact_path = _required_artifact_paths(plan, ("public_docs_seed_registry",))[
        "public_docs_seed_registry"
    ]
    payload = {
        "contract_version": _EVAL_CONTRACT_VERSION,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "operation_id": _required_str(plan, "operation_id"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "seed_count": len(_required_object_list(plan, "seed_registry")),
        "seed_evidence": list(snapshot["seedEvidence"]),
        "seed_registry": _public_docs_compact_seed_registry(plan),
        "seed_registry_sha256": _required_str(plan, "seed_registry_sha256"),
        "source_type_counts": dict(_required_mapping(plan, "source_type_counts")),
        "status": "validated",
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    written = write_immutable_evidence_snapshot(
        artifact_path,
        artifact_type="public_docs_seed_registry",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )
    return _public_docs_task_artifact_handle(
        written,
        summary={
            "operationId": _required_str(plan, "operation_id"),
            "seedCount": len(_required_object_list(plan, "seed_registry")),
            "status": "validated",
        },
    )


def write_public_docs_seed_refresh_plan_from_snapshot(
    plan_handle: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan, snapshot = _load_public_docs_airflow_plan_snapshot_bundle(plan_handle)
    refresh_payload = _compact_public_docs_seed_refresh_payload(
        _public_docs_seed_refresh_payload(plan),
        plan=plan,
        seed_evidence=cast(Sequence[Mapping[str, Any]], snapshot["seedEvidence"]),
    )
    payload_size = len(_canonical_json(refresh_payload).encode("utf-8"))
    if payload_size > PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES:
        raise ValueError(
            "public docs refresh plan exceeds the governed byte ceiling: "
            f"bytes={payload_size} limit={PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES}"
        )
    artifact_path = _required_artifact_paths(plan, ("public_docs_seed_refresh_plan",))[
        "public_docs_seed_refresh_plan"
    ]
    written = write_immutable_evidence_snapshot(
        artifact_path,
        artifact_type="public_docs_seed_refresh_plan",
        operation_id=_required_str(plan, "operation_id"),
        payload=refresh_payload,
    )
    return _public_docs_task_artifact_handle(
        written,
        summary={
            "operationId": _required_str(plan, "operation_id"),
            "seedCount": int(refresh_payload["seed_count"]),
            "skippedSeedCount": int(refresh_payload["skipped_seed_count"]),
            "status": _required_str(refresh_payload, "status"),
        },
    )


def write_public_docs_seed_registry_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("plan dag_id does not match public docs seed-registry writer")
    artifact_paths = _required_artifact_paths(plan, ("public_docs_seed_registry",))
    seed_registry = _required_object_list(plan, "seed_registry")
    payload = {
        "contract_version": _EVAL_CONTRACT_VERSION,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "operation_id": _required_str(plan, "operation_id"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "seed_count": len(seed_registry),
        "seed_registry": [dict(seed) for seed in seed_registry],
        "seed_registry_sha256": _required_str(plan, "seed_registry_sha256"),
        "source_type_counts": dict(_required_mapping(plan, "source_type_counts")),
        "status": "validated",
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    artifact_path = artifact_paths["public_docs_seed_registry"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_seed_registry",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_public_docs_seed_refresh_plan_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("plan dag_id does not match public docs seed-refresh writer")
    artifact_paths = _required_artifact_paths(plan, ("public_docs_seed_refresh_plan",))
    payload = _public_docs_seed_refresh_payload(plan)
    artifact_path = artifact_paths["public_docs_seed_refresh_plan"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_seed_refresh_plan",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_public_docs_publish_activation_trigger_conf_from_snapshot(
    plan_handle: Mapping[str, Any] | str,
    refresh_plan_handle: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = load_public_docs_airflow_plan_snapshot(plan_handle)
    refresh_payload, refresh_evidence = _read_public_docs_task_artifact_payload(
        refresh_plan_handle,
        expected_artifact_type="public_docs_seed_refresh_plan",
        max_bytes=PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES,
    )
    if _required_str(refresh_payload, "operation_id") != _required_str(plan, "operation_id"):
        raise ValueError("public docs refresh plan operation_id does not match plan snapshot")
    return write_public_docs_publish_activation_trigger_conf_artifact(
        plan,
        refresh_plan_evidence=refresh_evidence,
    )


def write_public_docs_publish_activation_trigger_conf_artifact(
    plan_json: Mapping[str, Any] | str,
    *,
    refresh_plan_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("plan dag_id does not match public docs publish trigger-conf writer")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_bc21_pipeline_state_receipt",
            "public_docs_seed_refresh_plan",
            "public_docs_seed_refresh_result",
            "public_docs_publish_activation_trigger_conf",
        ),
    )
    seed_refresh_result_path = _artifact_path(
        "public_docs_seed_refresh_result_path",
        artifact_paths["public_docs_seed_refresh_result"],
    )
    seed_refresh_result = _read_json_file(
        seed_refresh_result_path,
        "public_docs_seed_refresh_result",
    )
    if _required_str(seed_refresh_result, "artifact_type") == "public_docs_seed_refresh_noop":
        return _write_public_docs_noop_publish_trigger_artifact(
            plan=plan,
            artifact_paths=artifact_paths,
            seed_refresh_result_path=seed_refresh_result_path,
        )
    seed_refresh_identity = _public_docs_seed_refresh_result_identity(seed_refresh_result_path)
    if seed_refresh_identity["tenant_id"] != _required_str(plan, "tenant_id"):
        raise ValueError("public_docs_seed_refresh_result identity must match tenant_id")
    if seed_refresh_identity["pack_id"] != _required_str(plan, "pack_id"):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_id")
    if seed_refresh_identity["pack_version_id"] != _required_str(plan, "pack_version_id"):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_version_id")
    bc21_receipt_path = _artifact_path(
        "public_docs_bc21_pipeline_state_receipt",
        artifact_paths["public_docs_bc21_pipeline_state_receipt"],
    )
    bc21_receipt = _read_json_file(
        bc21_receipt_path,
        "public_docs_bc21_pipeline_state_receipt",
    )
    if _required_str(bc21_receipt, "status") != "accepted":
        raise ValueError("public docs BC-21 pipeline-state receipt must be accepted")
    bc21_response = _required_mapping(bc21_receipt, "response")
    if _required_str(bc21_response, "tenantId") != _required_str(plan, "tenant_id"):
        raise ValueError("public docs BC-21 receipt tenantId must match plan")
    if _required_str(bc21_response, "resourceId") != _required_str(plan, "pack_id"):
        raise ValueError("public docs BC-21 receipt resourceId must match pack_id")
    if _required_str(bc21_receipt, "pack_version_id") != _required_str(plan, "pack_version_id"):
        raise ValueError("public docs BC-21 receipt packVersionId must match plan")
    batch_evidence = _required_mapping(seed_refresh_result, "batch_evidence")
    if _required_str(bc21_response, "runId") != _required_str(batch_evidence, "indexed_run_id"):
        raise ValueError("public docs BC-21 receipt runId must match indexed_run_id")
    receipt_evidence_bundle_id = _required_uuid(bc21_response, "evidenceBundleId")
    receipt_evidence_seal_hash = _required_sha256_prefixed(bc21_response, "evidenceSealHash")
    batch_evidence_sha256 = _required_str(seed_refresh_result, "batch_evidence_sha256")
    policy_inputs = _public_docs_pack_policy_inputs(plan, batch_evidence)
    material = "|".join(
        (
            "public-docs-d5",
            _required_str(plan, "tenant_id"),
            _required_str(plan, "pack_id"),
            _required_str(plan, "pack_version_id"),
            _required_str(bc21_response, "runId"),
            batch_evidence_sha256,
        )
    )

    trigger_conf: dict[str, Any] = {
        "activation_idempotency_key": str(uuid5(_PUBLIC_DOCS_NAMESPACE, material + "|activation")),
        "activation_reason_code": "public-docs-d20-indexed",
        "actor_id": _required_str(plan, "actor_id"),
        "approval_idempotency_key": str(
            uuid5(_PUBLIC_DOCS_NAMESPACE, material + "|autonomous-approval")
        ),
        "artifact_root_path": _required_str(plan, "artifact_root_path"),
        "benchmark_gate_export_sha256": "sha256:"
        + sha256((material + "|benchmark-gate").encode("utf-8")).hexdigest(),
        "evidence_bundle_id": str(receipt_evidence_bundle_id),
        "evidence_seal_hash": receipt_evidence_seal_hash,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "qdrant_collection": _required_str(plan, "qdrant_collection"),
        "opensearch_index": _required_str(plan, "opensearch_index"),
        "neo4j_database": _required_str(plan, "neo4j_database"),
        "public_docs_crawl_state_path": _public_docs_crawl_state_path(
            plan,
            _required_artifact_root_path(plan),
        ),
        "public_docs_seed_refresh_plan_path": artifact_paths["public_docs_seed_refresh_plan"],
        **policy_inputs,
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
        "registry_resource_id": _required_str(plan, "registry_resource_id"),
        "registry_resource_type": _required_str(plan, "registry_resource_type"),
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    if refresh_plan_evidence is not None:
        normalized_refresh_evidence = _validated_public_docs_exact_evidence_handle(
            refresh_plan_evidence,
            "public docs seed refresh plan evidence",
        )
        if normalized_refresh_evidence["s3Uri"] != artifact_paths["public_docs_seed_refresh_plan"]:
            raise ValueError("public docs seed refresh plan evidence URI does not match plan")
        trigger_conf["public_docs_seed_refresh_plan_evidence"] = normalized_refresh_evidence
    if bc21_base_url := plan.get("bc21_base_url"):
        trigger_conf["bc21_base_url"] = _required_bc21_base_url({"bc21_base_url": bc21_base_url})
    if previous_active_pack_version_id := plan.get("previous_active_pack_version_id"):
        trigger_conf["previous_active_pack_version_id"] = str(
            _required_uuid(
                {"previous_active_pack_version_id": previous_active_pack_version_id},
                "previous_active_pack_version_id",
            )
        )
    governance_required_fields: list[str] = []
    if "bc21_base_url" not in trigger_conf:
        governance_required_fields.append("bc21_base_url")
    payload = {
        "artifact_type": "public_docs_publish_activation_trigger_conf",
        "contract_version": _EVAL_CONTRACT_VERSION,
        "dag_id": "serp_web_seed_crawl_refresh",
        "d5_publish_target": "serp_publish_signed_pack",
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "governance_required_fields": governance_required_fields,
        "operation_id": _required_str(plan, "operation_id"),
        "source_seed_refresh_result_path": seed_refresh_result_path,
        "status": "ready_for_d5_publish_activation"
        if not governance_required_fields
        else "governance_inputs_required",
        "target_dag_id": "serp_publish_signed_pack",
        "target_dag_run_conf": trigger_conf,
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    artifact_path = artifact_paths["public_docs_publish_activation_trigger_conf"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_publish_activation_trigger_conf",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def submit_public_docs_bc21_pipeline_state_from_snapshot(
    plan_handle: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = load_public_docs_airflow_plan_snapshot(plan_handle)
    return submit_public_docs_bc21_pipeline_state_artifact(plan)


def submit_public_docs_bc21_pipeline_state_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("plan dag_id does not match public docs BC-21 pipeline-state submit")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_seed_refresh_result",
            "public_docs_bc21_pipeline_state_receipt",
        ),
    )
    seed_refresh_result_path = _artifact_path(
        "public_docs_seed_refresh_result_path",
        artifact_paths["public_docs_seed_refresh_result"],
    )
    refresh_result = _read_json_file(
        seed_refresh_result_path,
        "public_docs_seed_refresh_result",
    )
    if _required_str(refresh_result, "artifact_type") == "public_docs_seed_refresh_noop":
        return _write_public_docs_noop_bc21_receipt(
            plan=plan,
            artifact_paths=artifact_paths,
            seed_refresh_result_path=seed_refresh_result_path,
        )
    bc21_base_url = _required_bc21_base_url(plan)
    bc21_pipeline_state: Any = importlib.import_module(
        "adapstory_serp_pipeline.registry.bc21_pipeline_state"
    )
    batch_evidence = _required_mapping(refresh_result, "batch_evidence")
    coverage_proof = _required_mapping(refresh_result, "coverage_proof")
    if _required_str(coverage_proof, "coverage_status") != "indexed_pending_publish":
        raise ValueError(
            "public docs coverage proof must be fully indexed before BC-21 registration"
        )
    _assert_equal("tenant_id", _required_str(batch_evidence, "tenant_id"), coverage_proof)
    _assert_equal("pack_id", _required_str(batch_evidence, "pack_id"), coverage_proof)
    _assert_equal(
        "pack_version_id", _required_str(batch_evidence, "pack_version_id"), coverage_proof
    )
    status = _required_str(batch_evidence, "status")
    if status not in {
        "indexed",
        "indexed_with_optional_failures",
        "indexed_with_quarantined_failures",
    }:
        raise ValueError("public docs seed refresh must be publishable before BC-21 registration")
    _validate_publishable_public_docs_batch_counters(batch_evidence, status=status)
    catalog_source_id = _ensure_public_docs_catalog_source(plan, bc21_base_url=bc21_base_url)
    submission = bc21_pipeline_state.build_public_docs_batch_pipeline_state_submission(
        refresh_result,
        actor_id=_required_str(plan, "actor_id"),
        catalog_source_id=UUID(catalog_source_id),
        started_at=datetime.fromisoformat(
            _required_datetime_string(plan, "generated_at").replace("Z", "+00:00")
        ),
    )
    receipt = bc21_pipeline_state.submit_pipeline_state_submission(
        submission,
        bc21_base_url=bc21_base_url,
    )
    payload = {
        **receipt,
        "catalog_source_id": catalog_source_id,
        "operation_id": _required_str(plan, "operation_id"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
    }
    artifact_path = artifact_paths["public_docs_bc21_pipeline_state_receipt"]
    _write_json_artifact(artifact_path, payload)
    _emit_public_docs_operational_gauge(
        "refresh_success_timestamp",
        _datetime_value(
            _required_datetime_string(plan, "generated_at"),
            "generated_at",
        ).timestamp(),
    )
    _emit_public_docs_operational_gauge("quarantined_sources", 0)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_bc21_pipeline_state_receipt",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def _write_public_docs_noop_bc21_receipt(
    *,
    plan: Mapping[str, Any],
    artifact_paths: Mapping[str, str],
    seed_refresh_result_path: str,
) -> dict[str, Any]:
    active_pack_version_id = str(_required_uuid(plan, "previous_active_pack_version_id"))
    payload = {
        "active_pack_version_id": active_pack_version_id,
        "artifact_type": "public_docs_noop_pipeline_state_receipt",
        "contract_version": _EVAL_CONTRACT_VERSION,
        "operation_id": _required_str(plan, "operation_id"),
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
        "reason": "all_seed_refreshes_within_max_age",
        "status": "not_submitted_no_change",
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    artifact_path = artifact_paths["public_docs_bc21_pipeline_state_receipt"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_bc21_pipeline_state_receipt",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def _write_public_docs_noop_publish_trigger_artifact(
    *,
    plan: Mapping[str, Any],
    artifact_paths: Mapping[str, str],
    seed_refresh_result_path: str,
) -> dict[str, Any]:
    receipt_path = artifact_paths["public_docs_bc21_pipeline_state_receipt"]
    noop_receipt = _read_json_file(receipt_path, "public_docs_bc21_pipeline_state_receipt")
    if _required_str(noop_receipt, "status") != "not_submitted_no_change":
        raise ValueError("public docs no-op receipt must retain the active pack")
    active_pack_version_id = str(_required_uuid(plan, "previous_active_pack_version_id"))
    if _required_str(noop_receipt, "active_pack_version_id") != active_pack_version_id:
        raise ValueError("public docs no-op receipt active_pack_version_id must match state")
    payload = {
        "active_pack_version_id": active_pack_version_id,
        "artifact_type": "public_docs_publish_activation_trigger_conf",
        "contract_version": _EVAL_CONTRACT_VERSION,
        "dag_id": "serp_web_seed_crawl_refresh",
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "operation_id": _required_str(plan, "operation_id"),
        "source_seed_refresh_result_path": seed_refresh_result_path,
        "status": "no_change_active_pack_retained",
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    artifact_path = artifact_paths["public_docs_publish_activation_trigger_conf"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_publish_activation_trigger_conf",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_public_docs_search_serve_smoke_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match public docs search serve smoke")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_publish_activation_receipt",
            "public_docs_search_serve_smoke",
        ),
    )
    receipt_path = _artifact_path(
        "public_docs_publish_activation_receipt",
        artifact_paths["public_docs_publish_activation_receipt"],
    )
    activation_receipt = _read_json_file(
        receipt_path,
        "public_docs_publish_activation_receipt",
    )
    expected_pack_version_id = _required_str(plan, "pack_version_id")
    if _required_str(activation_receipt, "active_pack_version_id") != expected_pack_version_id:
        raise ValueError("publish activation receipt active_pack_version_id must match plan")
    request_payload = _public_docs_search_serve_smoke_request(plan)
    endpoint = _public_docs_search_serve_base_url(plan) + "/api/serp/search/v1/query"
    response_payload = _post_json(
        endpoint,
        request_payload,
        attempts=3,
        retry_statuses=(503,),
    )
    selected_pack_version_ids = response_payload.get("selected_pack_version_ids")
    if not isinstance(selected_pack_version_ids, list) or not selected_pack_version_ids:
        raise ValueError("search serve smoke response must include selected_pack_version_ids")
    if selected_pack_version_ids[0] != expected_pack_version_id:
        raise ValueError("search serve smoke selected pack version must match activated pack")
    if _required_positive_int(response_payload, "result_count") < 1:
        raise ValueError("search serve smoke must return at least one result")
    payload = {
        "activation_receipt_path": receipt_path,
        "artifact_type": "public_docs_search_serve_smoke",
        "contract_version": _EVAL_CONTRACT_VERSION,
        "dag_id": "serp_publish_signed_pack",
        "endpoint": endpoint,
        "expected_pack_version_id": expected_pack_version_id,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "operation_id": _required_str(plan, "operation_id"),
        "request": request_payload,
        "response": response_payload,
        "result_count": _required_positive_int(response_payload, "result_count"),
        "selected_pack_version_ids": selected_pack_version_ids,
        "status": "served_active_pack",
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    artifact_path = artifact_paths["public_docs_search_serve_smoke"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_search_serve_smoke",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_public_docs_retrieval_golden_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    """Run the governed public-docs retrieval acceptance set against the live MCP API."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match public docs retrieval golden runner")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_publish_activation_receipt",
            "public_docs_retrieval_golden",
        ),
    )
    receipt = _read_json_file(
        artifact_paths["public_docs_publish_activation_receipt"],
        "public_docs_publish_activation_receipt",
    )
    expected_pack_version_id = _required_str(plan, "pack_version_id")
    if _required_str(receipt, "active_pack_version_id") != expected_pack_version_id:
        raise ValueError("retrieval golden requires the candidate pack to be active")
    refresh_plan = _read_public_docs_refresh_plan_for_d5(plan)
    refresh_result = _read_json_file(
        _artifact_path(
            "public_docs_seed_refresh_result_path",
            _required_str(plan, "public_docs_seed_refresh_result_path"),
        ),
        "public_docs_seed_refresh_result",
    )
    cases = _public_docs_retrieval_golden_cases(refresh_plan, refresh_result=refresh_result)
    endpoint = _public_docs_search_serve_base_url(plan) + "/api/serp/search/v1/query"
    observed_cases: list[dict[str, Any]] = []
    latency_seconds: list[float] = []
    for case in cases:
        request = _public_docs_search_request_for_golden_case(plan, case)
        first_started_at = perf_counter()
        first_response = _post_json(endpoint, request, attempts=3, retry_statuses=(503,))
        latency_seconds.append(perf_counter() - first_started_at)
        second_started_at = perf_counter()
        second_response = _post_json(endpoint, request, attempts=3, retry_statuses=(503,))
        latency_seconds.append(perf_counter() - second_started_at)
        observed_cases.append(
            _validate_public_docs_retrieval_golden_case(
                case=case,
                expected_pack_version_id=expected_pack_version_id,
                generated_at=_required_datetime_string(plan, "generated_at"),
                first_response=first_response,
                second_response=second_response,
                first_latency_seconds=latency_seconds[-2],
                second_latency_seconds=latency_seconds[-1],
            )
        )
    p50 = _public_docs_latency_percentile(latency_seconds, percentile=0.50)
    p95 = _public_docs_latency_percentile(latency_seconds, percentile=0.95)
    if p95 > _PUBLIC_DOCS_RETRIEVAL_GOLDEN_P95_SLO_SECONDS:
        raise ValueError(
            "public docs retrieval golden p95 latency exceeds SLO: "
            f"p95={p95:.6f}s slo={_PUBLIC_DOCS_RETRIEVAL_GOLDEN_P95_SLO_SECONDS:.6f}s"
        )
    payload = {
        "artifact_type": "public_docs_retrieval_golden",
        "case_count": len(observed_cases),
        "cases": observed_cases,
        "contract_version": _EVAL_CONTRACT_VERSION,
        "endpoint": endpoint,
        "expected_pack_version_id": expected_pack_version_id,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "latency_seconds": {"p50": p50, "p95": p95},
        "operation_id": _required_str(plan, "operation_id"),
        "status": "passed",
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    artifact_path = artifact_paths["public_docs_retrieval_golden"]
    _write_json_artifact(artifact_path, payload)
    _emit_public_docs_operational_gauge("retrieval_golden_p95_seconds", p95)
    _emit_public_docs_operational_gauge("retrieval_golden_passed", 1)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_retrieval_golden",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_public_docs_post_activation_rollback_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    """Restore the directly preceding active pack after a D5 serve-validation failure.

    The rollback is intentionally constrained to the predecessor captured by D20;
    neither the scheduler nor an operator can select an arbitrary historical pack.
    The caller must fail the DAG after this durable artifact is written so a
    successful compensation never masks the original validation failure.
    """

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match public docs post-activation rollback")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_publish_activation_receipt",
            "public_docs_post_activation_rollback",
        ),
    )
    previous_pack_version_id = _optional_previous_active_pack_version_id(plan)
    failed_pack_version_id = _required_str(plan, "pack_version_id")
    activation_receipt_path = _artifact_path(
        "public_docs_publish_activation_receipt",
        artifact_paths["public_docs_publish_activation_receipt"],
    )
    activation_receipt = _read_json_file(
        activation_receipt_path,
        "public_docs_publish_activation_receipt",
    )
    if _required_str(activation_receipt, "active_pack_version_id") != failed_pack_version_id:
        raise ValueError("post-activation rollback requires the candidate pack to be active")
    _assert_equal("tenant_id", _required_str(plan, "tenant_id"), activation_receipt)
    _assert_equal("pack_id", _required_str(plan, "pack_id"), activation_receipt)
    if previous_pack_version_id is None:
        payload = {
            "activation_receipt_path": activation_receipt_path,
            "active_pack_version_id": failed_pack_version_id,
            "artifact_type": "public_docs_post_activation_rollback",
            "contract_version": _EVAL_CONTRACT_VERSION,
            "generated_at": _required_datetime_string(plan, "generated_at"),
            "operation_id": _required_str(plan, "operation_id"),
            "remediation": "publish a corrected successor pack before expanding audience scope",
            "rollback_attempted": False,
            "status": "first_activation_no_restore_target",
            "tenant_id": _required_str(plan, "tenant_id"),
        }
        artifact_path = artifact_paths["public_docs_post_activation_rollback"]
        _write_json_artifact(artifact_path, payload)
        _emit_public_docs_operational_gauge("post_activation_rollback", 0)
        _emit_public_docs_operational_gauge("first_activation_no_restore_target", 1)
        return _artifact_result(
            artifact_path,
            artifact_type="public_docs_post_activation_rollback",
            operation_id=_required_str(plan, "operation_id"),
            payload=payload,
        )

    rollback_reason_code = "public-docs-d5-post-activation-validation-failed"
    request_payload = {
        "failedPackVersionId": failed_pack_version_id,
        "restoredPackVersionId": previous_pack_version_id,
        "rollbackReasonCode": rollback_reason_code,
    }
    tenant_id = _required_str(plan, "tenant_id")
    pack_id = _required_str(plan, "pack_id")
    actor_id = _required_str(plan, "actor_id")
    idempotency_key = str(
        uuid5(
            _PUBLIC_DOCS_NAMESPACE,
            "\n".join(
                (
                    "public-docs-d5-post-activation-rollback/v1",
                    tenant_id,
                    pack_id,
                    _required_str(plan, "operation_id"),
                    failed_pack_version_id,
                    previous_pack_version_id,
                )
            ),
        )
    )
    fingerprint = "sha256:" + sha256(_canonical_json(request_payload).encode("utf-8")).hexdigest()
    endpoint = (
        _required_bc21_base_url(plan).rstrip("/")
        + f"/api/bc-21/serp/v1/packs/{pack_id}/publish-rollbacks"
    )
    response_payload = _bc21_json_request(
        endpoint,
        method="POST",
        body=request_payload,
        headers={
            "X-Adapstory-Actor-Id": actor_id,
            "X-Adapstory-Tenant-Id": tenant_id,
            "X-Fingerprint": fingerprint,
            "X-Idempotency-Key": idempotency_key,
        },
        error_label="public docs post-activation rollback",
    )
    if response_payload is None:
        raise ValueError("public docs post-activation rollback returned no response")
    if _required_str(response_payload, "tenantId") != tenant_id:
        raise ValueError("post-activation rollback tenantId does not match plan")
    if _required_str(response_payload, "packId") != pack_id:
        raise ValueError("post-activation rollback packId does not match plan")
    if _required_str(response_payload, "failedPackVersionId") != failed_pack_version_id:
        raise ValueError("post-activation rollback failedPackVersionId does not match plan")
    if _required_str(response_payload, "restoredPackVersionId") != previous_pack_version_id:
        raise ValueError("post-activation rollback restoredPackVersionId does not match plan")
    if _required_str(response_payload, "rollbackReasonCode") != rollback_reason_code:
        raise ValueError("post-activation rollback reason code does not match contract")
    rollback_run_id = str(_required_uuid(response_payload, "rollbackRunId"))

    payload = {
        "activation_receipt_path": activation_receipt_path,
        "artifact_type": "public_docs_post_activation_rollback",
        "bc21_endpoint": endpoint,
        "contract_version": _EVAL_CONTRACT_VERSION,
        "failed_pack_version_id": failed_pack_version_id,
        "fingerprint": fingerprint,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "idempotency_key": idempotency_key,
        "operation_id": _required_str(plan, "operation_id"),
        "request": request_payload,
        "response": response_payload,
        "response_sha256": sha256(_canonical_json(response_payload).encode("utf-8")).hexdigest(),
        "restored_pack_version_id": previous_pack_version_id,
        "rollback_reason_code": rollback_reason_code,
        "rollback_run_id": rollback_run_id,
        "status": "rolled_back_to_previous_active_pack",
        "tenant_id": tenant_id,
    }
    artifact_path = artifact_paths["public_docs_post_activation_rollback"]
    _write_json_artifact(artifact_path, payload)
    _emit_public_docs_operational_gauge("post_activation_rollback", 1)
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_post_activation_rollback",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_public_docs_coverage_proof_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match public docs coverage proof")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_publish_activation_receipt",
            "public_docs_coverage_proof",
        ),
    )
    refresh_result_path = _artifact_path(
        "public_docs_seed_refresh_result_path",
        _required_str(plan, "public_docs_seed_refresh_result_path"),
    )
    refresh_result = _read_json_file(refresh_result_path, "public_docs_seed_refresh_result")
    indexed_proof = _required_mapping(refresh_result, "coverage_proof")
    receipt_path = _artifact_path(
        "public_docs_publish_activation_receipt",
        artifact_paths["public_docs_publish_activation_receipt"],
    )
    publish_receipt = _read_json_file(receipt_path, "public_docs_publish_activation_receipt")
    coverage_module: Any = importlib.import_module("adapstory_serp_pipeline.orchestration.coverage")
    coverage_proof = coverage_module.finalize_public_docs_coverage_proof(
        indexed_proof,
        publish_receipt=publish_receipt,
    )
    if coverage_proof.get("coverage_status") != "complete":
        raise ValueError("public docs coverage proof must be complete before D5 succeeds")
    payload = {
        **coverage_proof,
        "artifact_type": "public_docs_coverage_proof",
        "d5_operation_id": _required_str(plan, "operation_id"),
        "publish_receipt_path": receipt_path,
        "public_docs_seed_refresh_result_path": refresh_result_path,
    }
    artifact_path = artifact_paths["public_docs_coverage_proof"]
    _write_json_artifact(artifact_path, payload)
    _emit_public_docs_operational_gauge("coverage_ratio", 1)
    _emit_public_docs_operational_gauge("index_consistency", 1)
    _emit_public_docs_operational_gauge("post_activation_rollback", 0)
    _emit_public_docs_operational_gauge(
        "active_pack_published_timestamp",
        _datetime_value(
            _required_datetime_string(plan, "generated_at"),
            "generated_at",
        ).timestamp(),
    )
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_coverage_proof",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_public_docs_crawl_state_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    """Commit crawler validators only after D5 has activated complete coverage."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match public docs crawl-state writer")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_coverage_proof",
            "public_docs_crawl_state_commit_receipt",
            "public_docs_publish_activation_receipt",
        ),
    )
    coverage_path = artifact_paths["public_docs_coverage_proof"]
    coverage_proof = _read_json_file(coverage_path, "public_docs_coverage_proof")
    if _required_str(coverage_proof, "coverage_status") != "complete":
        raise ValueError("public docs coverage proof must be complete before crawler state commit")
    _assert_equal("tenant_id", _required_str(plan, "tenant_id"), coverage_proof)
    _assert_equal("pack_id", _required_str(plan, "pack_id"), coverage_proof)
    _assert_equal("pack_version_id", _required_str(plan, "pack_version_id"), coverage_proof)
    activation_receipt_path = artifact_paths["public_docs_publish_activation_receipt"]
    activation_receipt = _read_json_file(
        activation_receipt_path,
        "public_docs_publish_activation_receipt",
    )
    if _required_str(activation_receipt, "active_pack_version_id") != _required_str(
        plan, "pack_version_id"
    ):
        raise ValueError("publish activation receipt active_pack_version_id must match plan")
    if _required_str(activation_receipt, "status") not in {"active", "activated", "published"}:
        raise ValueError("publish activation receipt must be active before crawler state commit")
    refresh_plan_path = _artifact_path(
        "public_docs_seed_refresh_plan_path",
        _required_str(plan, "public_docs_seed_refresh_plan_path"),
    )
    refresh_plan = _read_public_docs_refresh_plan_for_d5(plan)
    crawl_state_path = _public_docs_crawl_state_path(
        plan,
        _required_artifact_root_path(plan),
    )
    state_payload = _public_docs_crawl_state_payload(
        plan=plan,
        refresh_plan=refresh_plan,
        coverage_proof=coverage_proof,
        activation_receipt=activation_receipt,
    )
    _write_json_artifact(crawl_state_path, state_payload)
    commit_payload = {
        "activation_receipt_path": activation_receipt_path,
        "artifact_type": "public_docs_crawl_state_commit_receipt",
        "committed_crawl_state_path": crawl_state_path,
        "contract_version": _EVAL_CONTRACT_VERSION,
        "coverage_proof_path": coverage_path,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "operation_id": _required_str(plan, "operation_id"),
        "refresh_plan_path": refresh_plan_path,
        "state_sha256": sha256(_canonical_json(state_payload).encode("utf-8")).hexdigest(),
        "status": "committed",
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    receipt_path = artifact_paths["public_docs_crawl_state_commit_receipt"]
    _write_json_artifact(receipt_path, commit_payload)
    return _artifact_result(
        receipt_path,
        artifact_type="public_docs_crawl_state_commit_receipt",
        operation_id=_required_str(plan, "operation_id"),
        payload=commit_payload,
    )


def _public_docs_crawl_state_payload(
    *,
    plan: Mapping[str, Any],
    refresh_plan: Mapping[str, Any],
    coverage_proof: Mapping[str, Any],
    activation_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    registry = _public_docs_seed_registry_from_refresh_plan(refresh_plan)
    coverage_by_seed: dict[str, Mapping[str, Any]] = {}
    for report in _required_object_list(coverage_proof, "seeds"):
        seed_id = _required_str(report, "seed_id")
        if seed_id in coverage_by_seed:
            raise ValueError("coverage proof contains duplicate seed_id")
        if _required_str(report, "status") != "published":
            raise ValueError("coverage proof contains unpublished seed")
        if _required_str(report, "index_status") != "passed":
            raise ValueError("coverage proof contains incompletely indexed seed")
        coverage_by_seed[seed_id] = report
    registry_by_seed = {_required_seed_id(seed): seed for seed in registry}
    if len(registry_by_seed) != len(registry) or set(registry_by_seed) != set(coverage_by_seed):
        raise ValueError("coverage proof seed set must match refresh plan registry")
    generated_at = _required_datetime_string(plan, "generated_at")
    state_seeds: dict[str, dict[str, Any]] = {}
    for seed_id, seed in sorted(registry_by_seed.items()):
        report = coverage_by_seed[seed_id]
        freshness_state = dict(_required_mapping(seed, "freshness_state"))
        crawl_policy = _required_mapping(seed, "crawl_policy")
        crawl_evidence = crawl_policy.get("crawl_evidence")
        if crawl_evidence is not None:
            if not isinstance(crawl_evidence, Mapping):
                raise ValueError("crawl_policy.crawl_evidence must be an object")
            if crawl_evidence.get("status") != "completed":
                raise ValueError("crawler evidence must be completed before state commit")
            page_state = _required_mapping(crawl_evidence, "state")
            freshness_state["page_state"] = _validated_public_docs_page_state(page_state)
        freshness_state.update(
            {
                "last_attempt_at": generated_at,
                "last_pipeline_evidence_sha256": "sha256:"
                + sha256(_canonical_json(report).encode("utf-8")).hexdigest(),
                "last_source_uri_hash": "sha256:"
                + sha256(_required_str(seed, "source_uri").encode("utf-8")).hexdigest(),
                "last_success_at": generated_at,
                "status": "indexed",
            }
        )
        state_seeds[seed_id] = {
            "freshness_state": _public_docs_freshness_state({"freshness_state": freshness_state})
        }
    return {
        "active_pack_version_id": _required_str(activation_receipt, "active_pack_version_id"),
        "artifact_type": "public_docs_crawl_state",
        "contract_version": _EVAL_CONTRACT_VERSION,
        "pack_id": _required_str(plan, "pack_id"),
        "seeds": state_seeds,
        "status": "active",
        "tenant_id": _required_str(plan, "tenant_id"),
        "updated_at": generated_at,
    }


def write_online_eval_rollup_plan_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_online_eval_rollup":
        raise ValueError("plan dag_id does not match online eval rollup-plan writer")
    artifact_paths = _required_artifact_paths(plan, ("online_eval_rollup_plan",))
    payload = {
        "contract_version": _EVAL_CONTRACT_VERSION,
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "normalized_gate_floor": _optional_unit_interval(
            plan,
            "normalized_gate_floor",
            SERP_NORMALIZED_GATE_FLOOR,
        ),
        "reports": [dict(report) for report in _required_object_list(plan, "reports")],
        "rollup_id": "serp_online_eval_rollup",
    }
    artifact_path = artifact_paths["online_eval_rollup_plan"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="online_eval_rollup_plan",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def execute_gateway_cli_spec(cli_spec: Mapping[str, Any] | str) -> dict[str, Any]:
    spec = _json_object(cli_spec, "cli_spec")
    _reject_raw_secrets(spec)
    if _required_str(spec, "contract_version") != _GATEWAY_CLI_CONTRACT_VERSION:
        raise ValueError("gateway cli spec contract version is unsupported")
    if _required_str(spec, "status") != "ready_for_gateway_cli_runner":
        raise ValueError("gateway cli spec is not ready for execution")
    argv = _required_str_list(spec, "argv")
    if any(value in {";", "&&", "|"} for value in argv):
        raise ValueError("gateway cli spec argv must not contain shell operators")
    input_paths = _required_str_list(spec, "input_paths")
    stdout_path = _artifact_path("stdout_path", _required_str(spec, "stdout_path"))
    with TemporaryDirectory(prefix="airflow-artifacts-") as temp_dir:
        argv = _materialize_gateway_cli_argv(argv, input_paths, temp_dir=temp_dir)
        completed = subprocess.run(
            argv,
            capture_output=True,
            check=False,
            text=True,
        )
    if completed.returncode != 0:
        stderr_sha256 = sha256(completed.stderr.encode("utf-8")).hexdigest()
        raise ValueError(
            "gateway cli execution failed: "
            f"task_id={_required_str(spec, 'task_id')} "
            f"returncode={completed.returncode} stderr_sha256={stderr_sha256}"
        )
    payload = _json_object(completed.stdout, "gateway_cli_stdout")
    _reject_raw_secrets(payload)
    _write_json_artifact(stdout_path, payload)
    return _artifact_result(
        stdout_path,
        artifact_type=_required_str(spec, "task_id"),
        operation_id=_required_str(spec, "operation_id"),
        payload=payload,
    )


def execute_pipeline_cli_spec(cli_spec: Mapping[str, Any] | str) -> dict[str, Any]:
    spec = _json_object(cli_spec, "cli_spec")
    _reject_raw_secrets(spec)
    if _required_str(spec, "contract_version") != _PIPELINE_CLI_CONTRACT_VERSION:
        raise ValueError("pipeline cli spec contract version is unsupported")
    status = _required_str(spec, "status")
    if status == "no_due_sources":
        return _execute_pipeline_noop_spec(spec)
    if status == "retired_pack_cleanup_not_required":
        return _execute_retired_pack_cleanup_noop_spec(spec)
    if status != "ready_for_pipeline_cli_runner":
        raise ValueError("pipeline cli spec is not ready for execution")
    argv = _required_str_list(spec, "argv")
    if any(value in {";", "&&", "|"} for value in argv):
        raise ValueError("pipeline cli spec argv must not contain shell operators")
    input_paths = _required_str_list(spec, "input_paths")
    stdout_path = _artifact_path("stdout_path", _required_str(spec, "stdout_path"))
    pipeline_owns_evidence_output = _pipeline_owns_evidence_output(spec, argv)
    with TemporaryDirectory(prefix="airflow-pipeline-artifacts-") as temp_dir:
        argv = _materialize_pipeline_cli_argv(
            argv,
            input_paths,
            stdout_path=stdout_path,
            temp_dir=temp_dir,
            preserve_evidence_output=pipeline_owns_evidence_output,
        )
        completed = subprocess.run(
            argv,
            capture_output=True,
            check=False,
            text=True,
        )
    if completed.returncode != 0:
        stderr_sha256 = sha256(completed.stderr.encode("utf-8")).hexdigest()
        failure_artifact_path = _write_pipeline_cli_failure_receipt(
            spec,
            stdout_path=stdout_path,
            returncode=completed.returncode,
            stderr=completed.stderr,
        )
        raise ValueError(
            "pipeline cli execution failed: "
            f"task_id={_required_str(spec, 'task_id')} "
            f"returncode={completed.returncode} stderr_sha256={stderr_sha256} "
            f"failure_artifact_path={failure_artifact_path}"
        )
    payload = _json_object(completed.stdout, "pipeline_cli_stdout")
    _reject_raw_secrets(payload)
    _write_json_artifact(stdout_path, payload)
    _raise_for_failed_pipeline_payload(spec, payload)
    return _artifact_result(
        stdout_path,
        artifact_type=_required_str(spec, "task_id"),
        operation_id=_required_str(spec, "operation_id"),
        payload=payload,
    )


def _write_pipeline_cli_failure_receipt(
    spec: Mapping[str, Any],
    *,
    stdout_path: str,
    returncode: int,
    stderr: str,
) -> str:
    failure_artifact_path = _pipeline_cli_failure_artifact_path(stdout_path)
    payload = {
        "artifact_type": "pipeline_cli_failure",
        "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
        "operation_id": _required_str(spec, "operation_id"),
        "returncode": returncode,
        "stderr_excerpt": _sanitize_pipeline_cli_failure_excerpt(stderr),
        "stderr_sha256": sha256(stderr.encode("utf-8")).hexdigest(),
        "task_id": _required_str(spec, "task_id"),
    }
    _reject_raw_secrets(payload)
    _write_json_artifact(failure_artifact_path, payload)
    return failure_artifact_path


def _pipeline_cli_failure_artifact_path(stdout_path: str) -> str:
    artifact = _artifact_ref("stdout_path", stdout_path)
    if artifact.kind == "s3":
        key = PurePosixPath(_required_str_ref(artifact.key))
        return (
            f"s3://{_required_str_ref(artifact.bucket)}/{key.with_name(f'{key.stem}.failure.json')}"
        )
    local_path = Path(_required_str_ref(artifact.local_path))
    return str(local_path.with_name(f"{local_path.stem}.failure.json"))


def _sanitize_pipeline_cli_failure_excerpt(stderr: str) -> str:
    redacted = _PIPELINE_CLI_FAILURE_SECRET_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group('field')}{match.group('separator')}[REDACTED]",
        stderr,
    )
    redacted = _PIPELINE_CLI_FAILURE_BEARER_RE.sub("Bearer [REDACTED]", redacted)
    redacted = _PIPELINE_CLI_FAILURE_BASIC_RE.sub("Basic [REDACTED]", redacted)
    redacted = _PIPELINE_CLI_FAILURE_URL_CREDENTIALS_RE.sub(
        lambda match: f"{match.group('scheme')}[REDACTED]@",
        redacted,
    )
    redacted = _PIPELINE_CLI_FAILURE_SK_RE.sub("[REDACTED]", redacted)
    return redacted[:_PIPELINE_CLI_FAILURE_EXCERPT_MAX_CHARS]


def _raise_for_failed_pipeline_payload(
    spec: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> None:
    if _required_str(spec, "task_id") != "public_docs_seed_refresh_pipeline":
        return
    if payload.get("artifact_type") != "public_docs_seed_refresh_batch_evidence":
        return
    batch_evidence = _required_mapping(payload, "batch_evidence")
    status = _required_str(batch_evidence, "status")
    index_mode = payload.get("index_mode", spec.get("index_mode"))
    if index_mode != "live":
        raise ValueError(
            "public docs seed refresh requires live index_mode before BC-21 registration: "
            f"index_mode={index_mode}"
        )
    if payload.get("index_effect") != "live":
        raise ValueError(
            "public docs seed refresh requires live index_effect before BC-21 registration: "
            f"index_effect={payload.get('index_effect')}"
        )
    if status in {"indexed", "indexed_with_optional_failures"}:
        _validate_publishable_public_docs_batch_counters(batch_evidence, status=status)
        return
    if status == "indexed_with_quarantined_failures":
        _emit_public_docs_operational_gauge(
            "quarantined_sources",
            _required_non_negative_int(batch_evidence, "quarantined_count"),
        )
        raise ValueError(
            "public docs seed refresh has required source failures or quarantined sources: "
            f"required_failed_count={batch_evidence.get('required_failed_count')} "
            f"quarantined_count={batch_evidence.get('quarantined_count')}"
        )
    raise ValueError(
        "public docs seed refresh is not publishable: "
        f"status={status} indexed_count={batch_evidence.get('indexed_count')} "
        f"failed_count={batch_evidence.get('failed_count')}"
    )


def _validate_publishable_public_docs_batch_counters(
    batch_evidence: Mapping[str, Any],
    *,
    status: str,
) -> None:
    failed_count = _required_non_negative_int(batch_evidence, "failed_count")
    indexed_count = _required_non_negative_int(batch_evidence, "indexed_count")
    optional_failed_count = _required_non_negative_int(batch_evidence, "optional_failed_count")
    required_failed_count = _required_non_negative_int(batch_evidence, "required_failed_count")
    if indexed_count == 0:
        raise ValueError("public docs seed refresh has no indexed sources")
    if status == "indexed" and (
        failed_count != 0 or optional_failed_count != 0 or required_failed_count != 0
    ):
        raise ValueError("public docs indexed seed refresh includes failed sources")
    if status == "indexed_with_optional_failures" and (
        required_failed_count != 0 or failed_count != optional_failed_count
    ):
        raise ValueError("public docs optional failure counters are inconsistent")
    quarantined_count = _required_non_negative_int(batch_evidence, "quarantined_count")
    if status == "indexed_with_quarantined_failures" and (
        quarantined_count == 0 or failed_count != quarantined_count
    ):
        raise ValueError("public docs quarantined failure counters are inconsistent")


def _required_non_negative_int(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _execute_pipeline_noop_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    stdout_path = _artifact_path("stdout_path", _required_str(spec, "stdout_path"))
    payload = {
        "artifact_type": "public_docs_seed_refresh_noop",
        "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
        "dag_id": _required_str(spec, "dag_id"),
        "operation_id": _required_str(spec, "operation_id"),
        "plan_sha256": _required_str(spec, "plan_sha256"),
        "seed_count": 0,
        "seed_registry_sha256": _required_str(spec, "seed_registry_sha256"),
        "skipped_seed_count": int(spec.get("skipped_seed_count", 0)),
        "status": "no_due_sources",
        "tenant_id": _required_str(spec, "tenant_id"),
        "index_mode": _required_str(spec, "index_mode"),
    }
    _write_json_artifact(stdout_path, payload)
    return _artifact_result(
        stdout_path,
        artifact_type=_required_str(spec, "task_id"),
        operation_id=_required_str(spec, "operation_id"),
        payload=payload,
    )


def _execute_retired_pack_cleanup_noop_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    stdout_path = _artifact_path("stdout_path", _required_str(spec, "stdout_path"))
    payload = {
        "artifact_type": "public_docs_retired_pack_cleanup",
        "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
        "dag_id": _required_str(spec, "dag_id"),
        "operation_id": _required_str(spec, "operation_id"),
        "reason": "no_previous_active_pack_version",
        "status": "not_required",
        "tenant_id": _required_str(spec, "tenant_id"),
    }
    _write_json_artifact(stdout_path, payload)
    return _artifact_result(
        stdout_path,
        artifact_type=_required_str(spec, "task_id"),
        operation_id=_required_str(spec, "operation_id"),
        payload=payload,
    )


def write_paired_eval_request_artifact(
    plan_json: Mapping[str, Any] | str,
    catalog_snapshot: Mapping[str, Any],
    promotion_snapshot: Mapping[str, Any],
    lifecycle_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist the scoreless D19 request consumed by the paired evaluator."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_benchmark_improvement_wave":
        raise ValueError("plan dag_id does not match paired-eval request writer")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "benchmark_catalog",
            "benchmark_catalog_receipt",
            "benchmark_catalog_pack_activation",
            "paired_eval_request",
            "paired_eval_receipt",
        ),
    )
    promotion = _validated_d19_promotion_snapshot(plan, promotion_snapshot)
    lifecycle = _validated_d19_lifecycle_result(plan, promotion, lifecycle_result)
    catalog_evidence = _paired_eval_catalog_evidence(plan, catalog_snapshot)
    activation_path = artifact_paths["benchmark_catalog_pack_activation"]
    activation_payload = _benchmark_catalog_pack_activation_payload(
        plan,
        catalog_evidence=catalog_evidence,
        lifecycle=lifecycle,
    )
    activation_snapshot = write_immutable_evidence_snapshot(
        activation_path,
        artifact_type="benchmark_catalog_pack_activation",
        operation_id=_required_str(plan, "operation_id"),
        payload=activation_payload,
    )
    activation_evidence = _written_worm_evidence_reference(
        activation_snapshot,
        activation_path,
        "benchmark catalog pack activation",
    )
    payload = _paired_eval_request_payload(
        plan,
        catalog_evidence=catalog_evidence,
        activation_evidence=activation_evidence,
        promotion=promotion,
        lifecycle=lifecycle,
    )
    artifact_path = artifact_paths["paired_eval_request"]
    request_evidence = write_immutable_evidence_snapshot(
        artifact_path,
        artifact_type="serp_paired_eval_request",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )
    return {
        **_artifact_result(
            artifact_path,
            artifact_type="paired_eval_request",
            operation_id=_required_str(plan, "operation_id"),
            payload=payload,
        ),
        "evidenceOutputPath": _required_str(artifact_paths, "paired_eval_receipt"),
        "requestEvidence": request_evidence,
    }


def write_paired_evaluation_verification_evidence(
    plan_json: Mapping[str, Any] | str,
    evaluator_result: Mapping[str, Any] | str,
    airflow_run: Mapping[str, Any] | str,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Seal the identity-bound D19 v9 verification handoff consumed by D6."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != _D19_DAG_ID:
        raise ValueError("plan dag_id does not match paired-evaluation verification writer")
    artifact_paths = _required_artifact_paths(
        plan,
        ("paired_eval_receipt", "paired_evaluation_verification_evidence"),
    )
    result = _json_object(evaluator_result, "evaluator_result")
    if set(result) != {
        "receiptAttestationEvidence",
        "receiptEvidence",
        "receiptStatus",
        "receiptVerification",
    }:
        raise ValueError("paired evaluation result fields are unsupported")
    receipt_status = _required_str(result, "receiptStatus")
    if receipt_status not in {"accepted", "rejected"}:
        raise ValueError("paired evaluation receipt status is unsupported")
    receipt_evidence = _paired_evaluation_receipt_worm_evidence(
        result,
        expected_path=artifact_paths["paired_eval_receipt"],
    )
    receipt_attestation_evidence = _worm_evidence_reference(
        result,
        "receiptAttestationEvidence",
    )
    client = s3_client or _s3_client(
        receipt_evidence["s3Uri"],
        receipt_attestation_evidence["s3Uri"],
        artifact_paths["paired_evaluation_verification_evidence"],
    )
    receipt_bytes = _read_exact_worm_evidence_bytes(
        receipt_evidence,
        field_name="paired evaluation receipt",
        s3_client=client,
    )
    receipt = _canonical_json_object_bytes(receipt_bytes, "paired evaluation receipt")
    if _required_str(receipt, "contractVersion") != _PAIRED_EVALUATION_RECEIPT_CONTRACT_VERSION:
        raise ValueError("paired evaluation receipt contract must be v9")
    operation_id = _required_str(plan, "operation_id")
    if _required_str(receipt, "requestId") != operation_id:
        raise ValueError("paired evaluation receipt requestId does not match operationId")
    if _required_str(receipt, "status") != receipt_status:
        raise ValueError("paired evaluation receipt status does not match evaluator result")

    receipt_verification, final_request_ids = _paired_evaluation_verification_descriptor(
        _required_mapping(result, "receiptVerification"),
        field_name="receiptVerification",
        expected_purpose=_PAIRED_EVALUATION_FINAL_RECEIPT_PURPOSE,
        expected_subject=receipt_evidence,
        expected_attestation=receipt_attestation_evidence,
    )
    attestation_verifications = _required_mapping(receipt, "attestationVerifications")
    if set(attestation_verifications) != set(_PAIRED_EVALUATION_ATTESTATION_PURPOSES):
        raise ValueError("paired evaluation receipt attestation verification set is unsupported")
    request_ids = list(final_request_ids)
    for descriptor_name, expected_purpose in _PAIRED_EVALUATION_ATTESTATION_PURPOSES.items():
        _, descriptor_request_ids = _paired_evaluation_verification_descriptor(
            _required_mapping(attestation_verifications, descriptor_name),
            field_name=f"attestationVerifications.{descriptor_name}",
            expected_purpose=expected_purpose,
        )
        request_ids.extend(descriptor_request_ids)
    if len(set(request_ids)) != len(request_ids):
        raise ValueError("paired evaluation verification request IDs must be unique")

    _read_exact_worm_evidence_bytes(
        receipt_attestation_evidence,
        field_name="paired evaluation receipt attestation",
        s3_client=client,
    )
    normalized_airflow_run = _normalized_d19_airflow_run(airflow_run)
    receipt_pointer = {
        "receiptAttestationEvidence": receipt_attestation_evidence,
        "receiptEvidence": receipt_evidence,
        "receiptStatus": receipt_status,
        "receiptVerification": receipt_verification,
    }
    payload = {
        "airflowRun": normalized_airflow_run,
        "operationId": operation_id,
        "receiptPointer": receipt_pointer,
        "requestId": operation_id,
        "schema": _PAIRED_EVALUATION_VERIFICATION_EVIDENCE_SCHEMA,
    }
    verification_path = artifact_paths["paired_evaluation_verification_evidence"]
    written = write_immutable_evidence_snapshot(
        verification_path,
        artifact_type="paired_evaluation_verification_evidence",
        operation_id=operation_id,
        payload=payload,
        s3_client=client,
    )
    verification_evidence = _written_worm_evidence_reference(
        written,
        verification_path,
        "paired evaluation verification evidence",
    )
    persisted_bytes = _read_exact_worm_evidence_bytes(
        verification_evidence,
        field_name="paired evaluation verification evidence",
        s3_client=client,
    )
    persisted = _canonical_json_object_bytes(
        persisted_bytes,
        "paired evaluation verification evidence",
    )
    if dict(persisted) != payload:
        raise ValueError("paired evaluation verification evidence readback does not match")
    return {
        "airflowRun": normalized_airflow_run,
        "pairedEvaluationVerificationEvidence": verification_evidence,
        "receiptStatus": receipt_status,
        "requestId": operation_id,
    }


def _paired_evaluation_receipt_worm_evidence(
    result: Mapping[str, Any],
    *,
    expected_path: str,
) -> dict[str, str]:
    raw = _required_mapping(result, "receiptEvidence")
    expected_fields = {
        "artifactETag",
        "artifactPath",
        "artifactSha256",
        "artifactType",
        "artifactVersionId",
        "objectLockMode",
        "objectLockRetainUntil",
        "status",
    }
    if set(raw) != expected_fields:
        raise ValueError("paired evaluation receipt evidence fields are unsupported")
    if _required_str(raw, "artifactPath") != expected_path:
        raise ValueError("paired evaluation receipt path does not match the D19 plan")
    if _required_str(raw, "artifactType") != "serp_paired_eval_receipt":
        raise ValueError("paired evaluation receipt artifact type is unsupported")
    if _required_str(raw, "status") != "written":
        raise ValueError("paired evaluation receipt was not written")
    _required_str(raw, "artifactETag")
    return _worm_evidence_reference(
        {
            "receiptEvidence": {
                "objectLockMode": _required_str(raw, "objectLockMode"),
                "retainUntil": _required_datetime_string(raw, "objectLockRetainUntil"),
                "s3Uri": expected_path,
                "sha256": "sha256:" + _required_sha256_hex(raw, "artifactSha256"),
                "versionId": _required_str(raw, "artifactVersionId"),
            }
        },
        "receiptEvidence",
    )


def _read_exact_worm_evidence_bytes(
    evidence: Mapping[str, str],
    *,
    field_name: str,
    s3_client: Any,
) -> bytes:
    payload, version_id, retain_until = _read_compliance_locked_s3_bytes(
        s3_client,
        evidence["s3Uri"],
        field_name=field_name,
        version_id=evidence["versionId"],
        max_bytes=_D19_VERIFICATION_EVIDENCE_MAX_BYTES,
    )
    if version_id != evidence["versionId"]:
        raise ValueError(f"{field_name} VersionId does not match")
    if retain_until != evidence["retainUntil"]:
        raise ValueError(f"{field_name} retention does not match")
    if "sha256:" + sha256(payload).hexdigest() != evidence["sha256"]:
        raise ValueError(f"{field_name} SHA-256 does not match")
    return payload


def _paired_evaluation_verification_descriptor(
    descriptor: Mapping[str, Any],
    *,
    field_name: str,
    expected_purpose: str,
    expected_subject: Mapping[str, str] | None = None,
    expected_attestation: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], tuple[str, str]]:
    expected_fields = {
        "attestationEvidence",
        "consumerVerification",
        "purpose",
        "signer",
        "statementSha256",
        "subject",
        "transit",
    }
    if set(descriptor) != expected_fields:
        raise ValueError(f"{field_name} fields are unsupported")
    purpose = _required_str(descriptor, "purpose")
    if purpose != expected_purpose:
        raise ValueError(f"{field_name} purpose is unsupported")
    attestation = _worm_evidence_reference(descriptor, "attestationEvidence")
    subject = _worm_evidence_reference(descriptor, "subject")
    if expected_attestation is not None and attestation != dict(expected_attestation):
        raise ValueError("paired evaluation receipt verification attestation does not match")
    if expected_subject is not None and subject != dict(expected_subject):
        raise ValueError("paired evaluation receipt verification subject does not match")
    consumer = _required_mapping(descriptor, "consumerVerification")
    if set(consumer) != {"requestId", "valid"}:
        raise ValueError(f"{field_name} consumerVerification fields are unsupported")
    _required_true(consumer, "valid")
    consumer_request_id = str(_required_uuid(consumer, "requestId"))
    signer = dict(_required_mapping(descriptor, "signer"))
    if not signer:
        raise ValueError(f"{field_name} signer is required")
    _reject_raw_secrets(signer)
    transit = _required_mapping(descriptor, "transit")
    if set(transit) != {"key", "keyVersion", "signature", "verifyRequestId"}:
        raise ValueError(f"{field_name} transit fields are unsupported")
    if _required_str(transit, "key") != _PAIRED_EVALUATION_PURPOSE_TRANSIT_KEYS[purpose]:
        raise ValueError(f"{field_name} transit key is unsupported")
    transit_request_id = str(_required_uuid(transit, "verifyRequestId"))
    normalized = {
        "attestationEvidence": attestation,
        "consumerVerification": {"requestId": consumer_request_id, "valid": True},
        "purpose": purpose,
        "signer": signer,
        "statementSha256": _required_sha256_prefixed(descriptor, "statementSha256"),
        "subject": subject,
        "transit": {
            "key": _required_str(transit, "key"),
            "keyVersion": _required_positive_int(transit, "keyVersion"),
            "signature": _required_str(transit, "signature"),
            "verifyRequestId": transit_request_id,
        },
    }
    return normalized, (consumer_request_id, transit_request_id)


def _normalized_d19_airflow_run(airflow_run: Mapping[str, Any] | str) -> dict[str, str]:
    metadata = _json_object(airflow_run, "airflow_run")
    _reject_raw_secrets(metadata)
    if set(metadata) != {"dagId", "logicalDate", "runId", "runType"}:
        raise ValueError("D19 airflowRun fields are unsupported")
    if _required_str(metadata, "dagId") != _D19_DAG_ID:
        raise ValueError("D19 airflowRun dagId does not match")
    if _required_str(metadata, "runType") != "manual":
        raise ValueError("D19 airflowRun runType must be manual")
    return {
        "dagId": _D19_DAG_ID,
        "logicalDate": _required_datetime_string(metadata, "logicalDate"),
        "runId": _required_str(metadata, "runId"),
        "runType": "manual",
    }


def build_tenant_golden_runner_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_tenant_golden_set_regression",
        task_id="run_tenant_golden_set_cases",
        command="tenant-golden-report",
        input_path_keys=("airflow_plan", "golden_set"),
        output_path_key="tenant_golden_report",
        option_names=("--airflow-plan", "--golden-set"),
    )


def build_tenant_golden_registry_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_tenant_golden_set_regression",
        task_id="build_tenant_golden_registry_submissions",
        command="tenant-golden-registry-submissions",
        input_path_keys=("airflow_plan", "tenant_golden_report"),
        output_path_key="tenant_golden_registry_submissions",
        option_names=("--airflow-plan", "--tenant-golden-report"),
    )


def build_online_eval_rollup_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_online_eval_rollup",
        task_id="build_online_eval_rollup",
        command="online-eval-rollup",
        input_path_keys=("online_eval_rollup_plan",),
        output_path_key="online_eval_rollup",
        option_names=("--rollup-plan",),
    )


def build_online_eval_registry_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_online_eval_rollup",
        task_id="build_online_eval_registry_submissions",
        command="online-eval-registry-submissions",
        input_path_keys=("airflow_plan", "online_eval_rollup"),
        output_path_key="online_eval_registry_submissions",
        option_names=("--airflow-plan", "--online-eval-rollup"),
    )


def evaluate_tenant_golden_gate(report: Mapping[str, Any]) -> dict[str, Any]:
    payload = _payload(report)
    findings: list[dict[str, Any]] = []
    for metric in _required_object_list(payload, "metric_results"):
        status = _required_str(metric, "status")
        if status != "passed":
            findings.append(
                {
                    "metric": _required_str(metric, "metric"),
                    "metric_family": _required_str(metric, "metric_family"),
                    "normalized_score": _required_number(metric, "normalized_score"),
                    "status": status,
                }
            )
    report_status = _required_str(payload, "status")
    if report_status == "passed" and findings:
        raise ValueError("tenant golden report status conflicts with metric results")
    return {
        "blocking_findings": findings,
        "status": "blocked" if report_status != "passed" or findings else "passed",
    }


def external_runner_pending(plan_json: str) -> dict[str, str]:
    plan = _json_object(plan_json, "plan_json")
    return {
        "operation_id": _required_str(plan, "operation_id"),
        "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
        "status": "pending_external_runner",
    }


def registry_submission_pending(plan_json: str) -> dict[str, str]:
    plan = _json_object(plan_json, "plan_json")
    return {
        "operation_id": _required_str(plan, "operation_id"),
        "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
        "status": "pending_bc21_submission",
    }


def governance_notification_pending(plan_json: str) -> dict[str, str]:
    plan = _json_object(plan_json, "plan_json")
    return {
        "dag_id": _required_str(plan, "dag_id"),
        "operation_id": _required_str(plan, "operation_id"),
        "status": "pending_governance_notification",
    }


def governance_notification_from_public_docs_snapshot(
    plan_handle: Mapping[str, Any] | str,
) -> dict[str, str]:
    plan = load_public_docs_airflow_plan_snapshot(plan_handle)
    return governance_notification_pending(_canonical_json(plan))


def dispatch_public_docs_seed_refresh_handoff_from_snapshot(
    plan_handle: Mapping[str, Any] | str,
    refresh_plan_handle: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan, snapshot = _load_public_docs_airflow_plan_snapshot_bundle(plan_handle)
    refresh_payload, refresh_evidence = _read_public_docs_task_artifact_payload(
        refresh_plan_handle,
        expected_artifact_type="public_docs_seed_refresh_plan",
        max_bytes=PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES,
    )
    if _required_str(refresh_payload, "operation_id") != _required_str(plan, "operation_id"):
        raise ValueError("public docs refresh plan operation_id does not match plan snapshot")
    refresh_plan_path = _required_artifact_paths(plan, ("public_docs_seed_refresh_plan",))[
        "public_docs_seed_refresh_plan"
    ]
    if refresh_evidence["s3Uri"] != refresh_plan_path:
        raise ValueError("public docs refresh plan URI does not match the canonical plan path")
    spec = dispatch_public_docs_seed_refresh_handoff(_canonical_json(plan))
    expected_status = _required_str(refresh_payload, "status")
    expected_cli_status = (
        "no_due_sources" if expected_status == "no_due_sources" else "ready_for_pipeline_cli_runner"
    )
    if _required_str(spec, "status") != expected_cli_status:
        raise ValueError("public docs refresh plan status does not match exact WORM evidence")
    if int(spec["seed_count"]) != _required_non_negative_int(refresh_payload, "seed_count"):
        raise ValueError("public docs refresh plan seed count does not match exact WORM evidence")
    spec["plan_evidence"] = dict(snapshot["planEvidence"])
    spec["plan_sha256"] = _required_str(snapshot["planEvidence"], "sha256").removeprefix("sha256:")
    spec["refresh_plan_evidence"] = refresh_evidence
    return spec


def dispatch_public_docs_seed_refresh_handoff(plan_json: str) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    if _required_str(plan, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("plan dag_id does not match public docs seed-refresh dispatch")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_seed_refresh_plan",
            "public_docs_seed_refresh_result",
        ),
    )
    refresh_plan_path = artifact_paths["public_docs_seed_refresh_plan"]
    result_path = artifact_paths["public_docs_seed_refresh_result"]
    artifact_root_path = _artifact_parent_path(result_path)
    refresh_payload = _public_docs_seed_refresh_payload(plan)
    refresh_status = _required_str(refresh_payload, "status")
    argv = [
        GATEWAY_CLI_PYTHON,
        "-m",
        PIPELINE_CLI_MODULE,
        "--refresh-plan",
        refresh_plan_path,
        "--artifact-root",
        artifact_root_path,
        "--evidence-output",
        result_path,
        "--clock-at",
        _required_datetime_string(plan, "generated_at"),
        "--embedding-mode",
        _required_str(plan, "embedding_mode"),
        "--index-mode",
        _required_str(plan, "index_mode"),
        "--tenant-id",
        _required_str(plan, "tenant_id"),
        "--pack-id",
        _required_str(plan, "pack_id"),
        "--pack-version-id",
        _required_str(plan, "pack_version_id"),
        "--qdrant-collection",
        _required_str(plan, "qdrant_collection"),
        "--opensearch-index",
        _required_str(plan, "opensearch_index"),
        "--neo4j-database",
        _required_str(plan, "neo4j_database"),
    ]
    if refresh_status == "no_due_sources":
        argv = []
        cli_status = "no_due_sources"
    else:
        cli_status = "ready_for_pipeline_cli_runner"
    return {
        "argv": argv,
        "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
        "dag_id": "serp_web_seed_crawl_refresh",
        "d4_dispatch_target": "serp_scan_parse_index",
        "input_paths": [refresh_plan_path],
        "operation_id": _required_str(plan, "operation_id"),
        "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
        "seed_count": int(refresh_payload["seed_count"]),
        "seed_registry_sha256": _required_str(plan, "seed_registry_sha256"),
        "skipped_seed_count": int(refresh_payload["skipped_seed_count"]),
        "status": cli_status,
        "stdout_path": result_path,
        "task_id": "public_docs_seed_refresh_pipeline",
        "tenant_id": _required_str(plan, "tenant_id"),
        "index_mode": _required_str(plan, "index_mode"),
        "embedding_mode": _required_str(plan, "embedding_mode"),
        "qdrant_collection": _required_str(plan, "qdrant_collection"),
        "opensearch_index": _required_str(plan, "opensearch_index"),
        "neo4j_database": _required_str(plan, "neo4j_database"),
    }


def build_public_docs_publish_activation_cli_spec(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match public docs publish activation dispatch")
    if _required_str(plan, "status") != "ready_for_publish_activation_handoff":
        raise ValueError("publish activation plan is not ready for handoff")
    artifact_paths = _required_artifact_paths(
        plan,
        ("public_docs_publish_activation_request",),
    )
    seed_refresh_result_path = _artifact_path(
        "public_docs_seed_refresh_result_path",
        _required_str(plan, "public_docs_seed_refresh_result_path"),
    )
    output_path = artifact_paths["public_docs_publish_activation_request"]
    argv = [
        GATEWAY_CLI_PYTHON,
        "-m",
        PIPELINE_PUBLISH_ACTIVATION_CLI_MODULE,
        "--seed-refresh-result",
        seed_refresh_result_path,
        "--evidence-output",
        output_path,
        "--actor-id",
        _required_str(plan, "actor_id"),
        "--activation-idempotency-key",
        _required_str(plan, "activation_idempotency_key"),
        "--approval-idempotency-key",
        _required_str(plan, "approval_idempotency_key"),
        "--evidence-bundle-id",
        _required_str(plan, "evidence_bundle_id"),
        "--evidence-seal-hash",
        _required_sha256_prefixed(plan, "evidence_seal_hash"),
        "--activation-reason-code",
        _required_str(plan, "activation_reason_code"),
        "--benchmark-gate-export-sha256",
        _required_sha256_prefixed(plan, "benchmark_gate_export_sha256"),
        "--policy-version",
        _required_str(plan, "policy_version"),
        "--policy-source-type",
        _required_str(plan, "policy_source_type"),
        "--policy-data-class",
        _required_str(plan, "policy_data_class"),
        "--policy-license-obligation-state",
        _required_str(plan, "policy_license_obligation_state"),
        "--policy-trust-state",
        _required_str(plan, "policy_trust_state"),
        "--policy-freshness-state",
        _required_str(plan, "policy_freshness_state"),
    ]
    return {
        "actor_id": _required_str(plan, "actor_id"),
        "argv": argv,
        "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
        "d5_publish_target": "serp_publish_signed_pack",
        "dag_id": "serp_publish_signed_pack",
        "input_paths": [seed_refresh_result_path],
        "operation_id": _required_str(plan, "operation_id"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
        "status": "ready_for_pipeline_cli_runner",
        "stdout_path": output_path,
        "task_id": "public_docs_publish_activation_handoff",
        "tenant_id": _required_str(plan, "tenant_id"),
    }


def build_public_docs_publish_activation_submit_cli_spec(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match public docs publish activation submit")
    if _required_str(plan, "status") != "ready_for_publish_activation_handoff":
        raise ValueError("publish activation plan is not ready for BC-21 submit")
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "public_docs_publish_activation_request",
            "public_docs_publish_activation_receipt",
        ),
    )
    request_path = _artifact_path(
        "public_docs_publish_activation_request",
        artifact_paths["public_docs_publish_activation_request"],
    )
    _read_json_file(request_path, "public_docs_publish_activation_request")
    receipt_path = artifact_paths["public_docs_publish_activation_receipt"]
    argv = [
        GATEWAY_CLI_PYTHON,
        "-m",
        PIPELINE_PUBLISH_ACTIVATION_CLI_MODULE,
        "submit",
        "--publish-activation-request",
        request_path,
        "--activation-receipt-output",
        receipt_path,
        "--bc21-base-url",
        _required_bc21_base_url(plan),
    ]
    return {
        "actor_id": _required_str(plan, "actor_id"),
        "argv": argv,
        "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
        "d5_publish_target": "serp_publish_signed_pack",
        "dag_id": "serp_publish_signed_pack",
        "input_paths": [request_path],
        "operation_id": _required_str(plan, "operation_id"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
        "status": "ready_for_pipeline_cli_runner",
        "stdout_path": receipt_path,
        "task_id": "public_docs_publish_activation_submit",
        "tenant_id": _required_str(plan, "tenant_id"),
    }


def build_public_docs_retired_pack_cleanup_cli_spec(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    """Build the post-D5 physical cleanup handoff for an old active pack."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_publish_signed_pack":
        raise ValueError("plan dag_id does not match retired public docs pack cleanup")
    artifact_paths = _required_artifact_paths(plan, ("public_docs_retired_pack_cleanup",))
    output_path = artifact_paths["public_docs_retired_pack_cleanup"]
    previous_active_pack_version_id = _optional_previous_active_pack_version_id(plan)
    if previous_active_pack_version_id is None:
        return {
            "argv": [],
            "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
            "dag_id": "serp_publish_signed_pack",
            "input_paths": [],
            "operation_id": _required_str(plan, "operation_id"),
            "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
            "status": "retired_pack_cleanup_not_required",
            "stdout_path": output_path,
            "task_id": "public_docs_retired_pack_cleanup",
            "tenant_id": _required_str(plan, "tenant_id"),
        }
    argv = [
        GATEWAY_CLI_PYTHON,
        "-m",
        PIPELINE_RETIRED_PACK_CLEANUP_CLI_MODULE,
        "--artifact-root",
        _required_str(plan, "artifact_root_path"),
        "--evidence-output",
        output_path,
        "--clock-at",
        _required_datetime_string(plan, "generated_at"),
        "--index-mode",
        "live",
        "--tenant-id",
        _required_str(plan, "tenant_id"),
        "--pack-id",
        _required_str(plan, "pack_id"),
        "--active-pack-version-id",
        _required_str(plan, "pack_version_id"),
        "--retired-pack-version-id",
        previous_active_pack_version_id,
        "--qdrant-collection",
        _required_str(plan, "qdrant_collection"),
        "--opensearch-index",
        _required_str(plan, "opensearch_index"),
        "--neo4j-database",
        _required_str(plan, "neo4j_database"),
    ]
    return {
        "argv": argv,
        "contract_version": _PIPELINE_CLI_CONTRACT_VERSION,
        "dag_id": "serp_publish_signed_pack",
        "input_paths": [],
        "operation_id": _required_str(plan, "operation_id"),
        "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
        "status": "ready_for_pipeline_cli_runner",
        "stdout_path": output_path,
        "task_id": "public_docs_retired_pack_cleanup",
        "tenant_id": _required_str(plan, "tenant_id"),
    }


def _public_docs_seed_refresh_result_identity(seed_refresh_result_path: str) -> dict[str, str]:
    result = _read_json_file(seed_refresh_result_path, "public_docs_seed_refresh_result")
    if _required_str(result, "artifact_type") != "public_docs_seed_refresh_batch_evidence":
        raise ValueError("public_docs_seed_refresh_result artifact_type is unsupported")
    batch_evidence = _required_mapping(result, "batch_evidence")
    return {
        "pack_id": _required_str(batch_evidence, "pack_id"),
        "pack_version_id": _required_str(batch_evidence, "pack_version_id"),
        "tenant_id": _required_str(batch_evidence, "tenant_id"),
    }


def _public_docs_search_serve_smoke_request(plan: Mapping[str, Any]) -> dict[str, Any]:
    request_id = str(
        uuid5(
            _PUBLIC_DOCS_NAMESPACE,
            "public-docs-search-serve-smoke:" + _required_str(plan, "operation_id"),
        )
    )
    policy_bundle_sha256 = (
        "sha256:"
        + sha256(
            _canonical_json(
                {
                    "operation_id": _required_str(plan, "operation_id"),
                    "pack_version_id": _required_str(plan, "pack_version_id"),
                    "purpose": "public-docs-search-serve-smoke",
                }
            ).encode("utf-8")
        ).hexdigest()
    )
    search_serve_actor_id = str(_required_uuid(plan, "search_serve_actor_id"))
    return {
        "actor_id": search_serve_actor_id,
        "auth_context_version": "airflow-public-docs-smoke@2026.07.1",
        "auth_issuer": "airflow://serp-public-docs",
        "auth_method": "airflow-dag-task",
        "auth_session_id": _required_str(plan, "operation_id"),
        "auth_subject_id": _PUBLIC_DOCS_SEARCH_SERVE_SMOKE_SUBJECT_ID,
        "auth_subject_type": "service",
        "authorization_decision_id": "authz:" + request_id,
        "authorization_effect": "allow",
        "break_glass_lease_id": None,
        "contract_version": "2026.07.1",
        "effective_data_class": "PUBLIC",
        "entitlement_snapshot_id": "public-docs-search-serve-smoke@2026.07.1",
        "feature_flags": {"hybrid": True},
        "max_results": 3,
        "metadata": {
            "expected_pack_version_id": _required_str(plan, "pack_version_id"),
            "operation_id": _required_str(plan, "operation_id"),
            "surface": "airflow-public-docs-search-serve-smoke",
        },
        "mode": "search_then_retrieve",
        "pack_scope_hash": "sha256:"
        + sha256(_required_str(plan, "pack_version_id").encode("utf-8")).hexdigest(),
        "policy_bundle_sha256": policy_bundle_sha256,
        "policy_rollout_state": "active",
        "policy_rule_ids_applied": ["serp-public-docs-active-pack-smoke"],
        "policy_version": "serp-public-docs-smoke-policy@2026.07.1",
        "query": "public docs installation quick start",
        "request_id": request_id,
        "tenant_id": _required_str(plan, "tenant_id"),
        "tenant_lifecycle_state": "ACTIVE",
        "tenant_mode": "public",
        "tenant_scope": "public",
    }


def _public_docs_retrieval_golden_cases(
    refresh_plan: Mapping[str, Any],
    *,
    refresh_result: Mapping[str, Any],
) -> list[dict[str, Any]]:
    registry_seeds = _public_docs_seed_registry_from_refresh_plan(refresh_plan)
    components_by_seed_id = {
        _required_str(seed, "seed_id"): _required_str(
            _required_mapping(seed, "inventory_evidence"), "component"
        )
        for seed in registry_seeds
    }
    coverage_proof = _required_mapping(refresh_result, "coverage_proof")
    if _required_str(coverage_proof, "coverage_status") != "indexed_pending_publish":
        raise ValueError("public docs retrieval golden requires indexed pending-publish coverage")
    indexed_seeds = _required_object_list(coverage_proof, "seeds")
    required_cases: list[dict[str, Any]] = []
    optional_cases: list[dict[str, Any]] = []
    for seed in sorted(indexed_seeds, key=lambda item: _required_str(item, "seed_id")):
        if _required_str(seed, "status") != "indexed":
            continue
        if _required_str(seed, "index_status") != "passed":
            continue
        seed_id = _required_str(seed, "seed_id")
        component = components_by_seed_id.get(seed_id)
        if component is None:
            raise ValueError("public docs coverage proof seed_id is missing from seed registry")
        source_uri = _required_str(seed, "source_uri")
        required_cases.append(
            _public_docs_retrieval_golden_case(
                seed_id=seed_id,
                component=component,
                source_uri=source_uri,
                query=f"{component} official documentation overview",
            )
        )
        raw_required_frontier = seed.get("required_frontier", [])
        if not isinstance(raw_required_frontier, list) or not all(
            isinstance(frontier, Mapping) for frontier in raw_required_frontier
        ):
            raise ValueError("required_frontier must be a list of objects")
        for frontier in sorted(
            raw_required_frontier, key=lambda item: _required_str(item, "source_uri")
        ):
            if frontier.get("status") != "indexed" or frontier.get("index_status") != "passed":
                raise ValueError("public docs retrieval golden requires every curated frontier")
            frontier_url = _required_str(frontier, "source_uri")
            required_cases.append(
                _public_docs_retrieval_golden_case(
                    seed_id=seed_id,
                    component=component,
                    source_uri=frontier_url,
                    query=_public_docs_retrieval_golden_query(component, frontier_url),
                )
            )
        raw_frontier = seed.get("optional_frontier", [])
        if not isinstance(raw_frontier, list) or not all(
            isinstance(frontier, Mapping) for frontier in raw_frontier
        ):
            raise ValueError("optional_frontier must be a list of objects")
        indexed_frontier = [
            frontier for frontier in raw_frontier if frontier.get("status") == "indexed"
        ]
        for frontier in sorted(
            indexed_frontier, key=lambda item: _required_str(item, "source_uri")
        ):
            frontier_url = _required_str(frontier, "source_uri")
            optional_cases.append(
                _public_docs_retrieval_golden_case(
                    seed_id=seed_id,
                    component=component,
                    source_uri=frontier_url,
                    query=_public_docs_retrieval_golden_query(component, frontier_url),
                )
            )
    unique_required_cases: dict[str, dict[str, Any]] = {}
    for case in required_cases:
        case_id = _required_str(case, "case_id")
        if case_id in unique_required_cases:
            raise ValueError("public docs retrieval golden contains duplicate case_id")
        unique_required_cases[case_id] = case
    selected = list(unique_required_cases.values())
    unique_optional_cases: dict[str, dict[str, Any]] = {}
    for case in optional_cases:
        case_id = _required_str(case, "case_id")
        if case_id in unique_required_cases or case_id in unique_optional_cases:
            raise ValueError("public docs retrieval golden contains duplicate case_id")
        unique_optional_cases[case_id] = case
    if len(selected) < _PUBLIC_DOCS_RETRIEVAL_GOLDEN_MIN_CASES:
        optional_case_budget = _PUBLIC_DOCS_RETRIEVAL_GOLDEN_MIN_CASES - len(selected)
        selected.extend(list(unique_optional_cases.values())[:optional_case_budget])
    if len(selected) < _PUBLIC_DOCS_RETRIEVAL_GOLDEN_MIN_CASES:
        raise ValueError(
            "public docs retrieval golden requires at least "
            f"{_PUBLIC_DOCS_RETRIEVAL_GOLDEN_MIN_CASES} governed cases"
        )
    return selected


def _public_docs_retrieval_golden_query(component: str, source_uri: str) -> str:
    parsed = urlparse(source_uri)
    tokens: list[str] = []
    for raw_segment in parsed.path.split("/"):
        segment = raw_segment.strip().removesuffix(".html")
        if not segment or segment in {
            "current",
            "docs",
            "documentation",
            "en",
            "index",
            "latest",
            "stable",
        }:
            continue
        tokens.extend(token for token in re.split(r"[-_]+", segment) if token)
    suffix = " ".join(tokens[:12])
    if not suffix:
        return f"{component} official documentation overview"
    return f"{component} documentation {suffix}"


def _public_docs_retrieval_golden_case(
    *,
    seed_id: str,
    component: str,
    source_uri: str,
    query: str,
) -> dict[str, Any]:
    source_uri_hash = sha256(source_uri.encode("utf-8")).hexdigest()[:16]
    return {
        "case_id": f"public-docs-{seed_id}-{source_uri_hash}",
        "component": component,
        "expected": {
            "max_freshness_hours": _PUBLIC_DOCS_RETRIEVAL_GOLDEN_MAX_FRESHNESS_HOURS,
            "minimum_citations": 1,
            "source_uri_prefix": source_uri,
        },
        "query": query,
        "seed_id": seed_id,
    }


def _public_docs_search_request_for_golden_case(
    plan: Mapping[str, Any], case: Mapping[str, Any]
) -> dict[str, Any]:
    request = _public_docs_search_serve_smoke_request(plan)
    case_id = _required_str(case, "case_id")
    expected = _required_mapping(case, "expected")
    request["request_id"] = str(
        uuid5(
            _PUBLIC_DOCS_NAMESPACE,
            "public-docs-retrieval-golden:" + _required_str(plan, "operation_id") + ":" + case_id,
        )
    )
    request["query"] = _required_str(case, "query")
    request["metadata"] = {
        **_required_mapping(request, "metadata"),
        "golden_case_expected_source_uri": _required_str(expected, "source_uri_prefix"),
        "golden_case_id": case_id,
        "surface": "airflow-public-docs-retrieval-golden",
    }
    request["policy_rule_ids_applied"] = ["serp-public-docs-active-pack-retrieval-golden"]
    return request


def _public_docs_source_uri_matches_expected_docs_root(
    *, expected_source_uri: str, observed_source_uri: str
) -> bool:
    """Match the governed docs root while tolerating a presentation locale.

    Public documentation sites commonly redirect a canonical root such as
    ``/docs/`` to ``/ru/docs/`` or ``/en/docs/``.  A locale is presentation
    metadata, whereas hostname and every remaining root path segment encode the
    governed source identity (including a pinned breaking-change version).
    """

    expected = _public_docs_source_uri_identity(expected_source_uri)
    observed = _public_docs_source_uri_identity(observed_source_uri)
    if expected is None or observed is None:
        return False
    expected_scheme, expected_host, expected_path = expected
    observed_scheme, observed_host, observed_path = observed
    if (expected_scheme, expected_host) != (observed_scheme, observed_host):
        return False
    return observed_path[: len(expected_path)] == expected_path


def _public_docs_source_uri_identity(source_uri: str) -> tuple[str, str, tuple[str, ...]] | None:
    parsed = urlparse(source_uri)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower().rstrip(".")
    if scheme != "https" or not host:
        return None
    path = tuple(segment for segment in parsed.path.split("/") if segment)
    if path and _PUBLIC_DOCS_LOCALE_PATH_SEGMENT.fullmatch(path[0]):
        path = path[1:]
    return scheme, host, path


def _validate_public_docs_retrieval_golden_case(
    *,
    case: Mapping[str, Any],
    expected_pack_version_id: str,
    generated_at: str,
    first_response: Mapping[str, Any],
    second_response: Mapping[str, Any],
    first_latency_seconds: float,
    second_latency_seconds: float,
) -> dict[str, Any]:
    first_signature = _public_docs_retrieval_golden_response_signature(
        response=first_response,
        case=case,
        expected_pack_version_id=expected_pack_version_id,
        generated_at=generated_at,
    )
    second_signature = _public_docs_retrieval_golden_response_signature(
        response=second_response,
        case=case,
        expected_pack_version_id=expected_pack_version_id,
        generated_at=generated_at,
    )
    if first_signature != second_signature:
        raise ValueError(
            "public docs retrieval golden replay is non-deterministic: "
            f"case_id={_required_str(case, 'case_id')}"
        )
    return {
        "case_id": _required_str(case, "case_id"),
        "expected": dict(_required_mapping(case, "expected")),
        "latency_seconds": {"first": first_latency_seconds, "replay": second_latency_seconds},
        "observed": first_signature,
        "query": _required_str(case, "query"),
        "seed_id": _required_str(case, "seed_id"),
        "status": "passed",
    }


def _public_docs_retrieval_golden_response_signature(
    *,
    response: Mapping[str, Any],
    case: Mapping[str, Any],
    expected_pack_version_id: str,
    generated_at: str,
) -> dict[str, Any]:
    case_id = _required_str(case, "case_id")

    def fail(reason: str) -> NoReturn:
        raise ValueError(f"public docs retrieval golden case_id={case_id}: {reason}")

    selected_pack_version_ids = _required_str_list(response, "selected_pack_version_ids")
    if selected_pack_version_ids != [expected_pack_version_id]:
        fail("selected pack must match candidate pack")
    expected = _required_mapping(case, "expected")
    expected_source_uri_prefix = _required_str(expected, "source_uri_prefix")
    minimum_citations = _required_positive_int(expected, "minimum_citations")
    citations = _required_object_list(response, "citations")
    if len(citations) < minimum_citations:
        fail("response has insufficient citations")
    if not _public_docs_source_uri_matches_expected_docs_root(
        expected_source_uri=expected_source_uri_prefix,
        observed_source_uri=_required_str(citations[0], "source_uri"),
    ):
        fail(
            "top-ranked expected source is absent from citations: "
            f"expected_source_uri_prefix={expected_source_uri_prefix} "
            f"observed_source_uri={_required_str(citations[0], 'source_uri')}"
        )
    canonical_citations: list[dict[str, str]] = []
    citation_source_uris: dict[str, str] = {}
    for citation in citations:
        citation_pack_version_id = _required_str(citation, "pack_version_id")
        citation_source_uri = _required_str(citation, "source_uri")
        if citation_pack_version_id != expected_pack_version_id:
            fail("citation pack version does not match candidate pack")
        citation_chunk_id = _required_str(citation, "chunk_id")
        if citation_chunk_id in citation_source_uris:
            fail(f"response contains duplicate citation chunk_id={citation_chunk_id}")
        citation_source_uris[citation_chunk_id] = citation_source_uri
        canonical_citations.append(
            {
                "chunk_id": citation_chunk_id,
                "source_uri": citation_source_uri,
            }
        )
    result_count = _required_positive_int(response, "result_count")
    result_cards = _required_object_list(response, "result_cards")
    if len(result_cards) != result_count:
        fail("result cards must match result_count")
    first_card_provenance = _required_mapping(result_cards[0], "provenance")
    first_card_source_url = _required_str(first_card_provenance, "source_url")
    if not _public_docs_source_uri_matches_expected_docs_root(
        expected_source_uri=expected_source_uri_prefix,
        observed_source_uri=first_card_source_url,
    ):
        fail(
            "top-ranked expected source is absent from result cards: "
            f"expected_source_uri_prefix={expected_source_uri_prefix} "
            f"observed_source_url={first_card_source_url}"
        )
    generated_at_value = _datetime_value(generated_at, "generated_at")
    max_freshness_hours = _required_positive_int(expected, "max_freshness_hours")
    canonical_cards: list[dict[str, str]] = []
    for card in result_cards:
        card_chunk_id = _required_str(card, "chunk_id")
        provenance = _required_mapping(card, "provenance")
        source_url = _required_str(provenance, "source_url")
        cited_source_uri = citation_source_uris.get(card_chunk_id)
        if cited_source_uri is None:
            fail(f"result card lacks candidate-pack citation: chunk_id={card_chunk_id}")
        if cited_source_uri != source_url:
            fail(
                "result card source does not match candidate-pack citation: "
                f"chunk_id={card_chunk_id} citation_source_uri={cited_source_uri} "
                f"result_source_url={source_url}"
            )
        if _required_str(provenance, "freshness_state") != "fresh":
            fail(f"result is not fresh: chunk_id={card_chunk_id}")
        crawled_at = _datetime_value(_required_str(provenance, "crawl_time"), "crawl_time")
        freshness_hours = (generated_at_value - crawled_at).total_seconds() / 3600
        if freshness_hours < 0 or freshness_hours > max_freshness_hours:
            fail(
                "result exceeds freshness SLO: "
                f"chunk_id={card_chunk_id} freshness_hours={freshness_hours:.6f} "
                f"max_freshness_hours={max_freshness_hours}"
            )
        canonical_cards.append(
            {
                "chunk_id": card_chunk_id,
                "source_url": source_url,
            }
        )
    return {
        "citations": canonical_citations,
        "result_cards": canonical_cards,
        "result_chunk_ids": _required_str_list(response, "result_chunk_ids"),
        "result_count": result_count,
        "selected_pack_version_ids": selected_pack_version_ids,
    }


def _public_docs_latency_percentile(samples: Sequence[float], *, percentile: float) -> float:
    if not samples:
        raise ValueError("public docs retrieval golden requires latency samples")
    if not 0 < percentile <= 1:
        raise ValueError("latency percentile must be within (0, 1]")
    ordered = sorted(samples)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _emit_public_docs_operational_gauge(metric_name: str, value: float | int) -> None:
    """Best-effort StatsD emission; durable D20/D5 artifacts remain the source of truth."""

    if not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError("public docs operational metric value must be finite")
    try:
        from airflow.stats import Stats
    except ImportError:
        return
    Stats.gauge(f"serp_public_docs.{metric_name}", value)


def _public_docs_pack_policy_inputs(
    plan: Mapping[str, Any],
    batch_evidence: Mapping[str, Any],
) -> dict[str, str]:
    indexed_sources = [
        source
        for source in _required_object_list(batch_evidence, "source_results")
        if _required_str(source, "pipeline_status") == "indexed"
    ]
    if not indexed_sources:
        raise ValueError("public docs policy inputs require indexed source evidence")
    seed_registry = {
        _required_str(seed, "seed_id"): seed
        for seed in _public_docs_seed_registry_from_refresh_plan(plan)
    }
    indexed_seed_by_id: dict[str, Mapping[str, Any]] = {}
    missing_seed_ids: list[str] = []
    for source in indexed_sources:
        seed = _public_docs_registry_seed_for_indexed_source(source, seed_registry)
        if seed is None:
            missing_seed_ids.append(_required_str(source, "seed_id"))
            continue
        indexed_seed_by_id[_required_str(seed, "seed_id")] = seed
    if missing_seed_ids:
        raise ValueError("public docs policy inputs require seed registry coverage")
    indexed_seeds = [indexed_seed_by_id[seed_id] for seed_id in sorted(indexed_seed_by_id)]
    data_class = _most_restrictive_public_docs_data_class(
        _required_str(seed, "data_class") for seed in indexed_seeds
    )
    license_obligation_state = _most_restrictive_public_docs_license_state(
        _required_str(_required_mapping(seed, "license"), "obligation_state")
        for seed in indexed_seeds
    )
    # The publish policy describes this candidate, not the previous active
    # crawl-state snapshot. Reaching this point requires a D20 evidence bundle
    # with every required seed indexed, so its records are fresh at ingestion.
    freshness_state = "fresh"
    trust_state = (
        "trusted" if all(bool(seed.get("approved")) for seed in indexed_seeds) else "unreviewed"
    )
    return {
        "policy_data_class": data_class,
        "policy_freshness_state": freshness_state,
        "policy_license_obligation_state": license_obligation_state,
        "policy_source_type": _public_docs_pack_policy_source_type(indexed_seeds),
        "policy_trust_state": trust_state,
        "policy_version": "source-approval@2026.07.1",
    }


def _public_docs_registry_seed_for_indexed_source(
    source: Mapping[str, Any],
    seed_registry: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    seed_id = _required_str(source, "seed_id")
    seed = seed_registry.get(seed_id)
    if seed is not None:
        return seed
    metadata = source.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    frontier = metadata.get("frontier")
    if not isinstance(frontier, Mapping):
        return None
    parent_seed_id = frontier.get("parent_seed_id")
    if not isinstance(parent_seed_id, str) or not parent_seed_id:
        return None
    return seed_registry.get(parent_seed_id)


def _public_docs_pack_policy_source_type(seeds: Sequence[Mapping[str, Any]]) -> str:
    source_types = {_required_str(seed, "source_type") for seed in seeds}
    if not source_types:
        raise ValueError("public docs policy sourceType requires indexed sources")
    if len(source_types) == 1:
        return next(iter(source_types))
    return "markdown"


def _most_restrictive_public_docs_data_class(values: Iterable[str]) -> str:
    order = {
        "PUBLIC": 0,
        "INTERNAL_EXTERNAL_OK": 1,
    }
    return _max_by_policy_order("data_class", values, order)


def _most_restrictive_public_docs_license_state(values: Iterable[str]) -> str:
    mapped = []
    for value in values:
        if value in {"public_share_allowed", "review_required", "no_redistribution"}:
            mapped.append(value)
        elif value in {"reviewed-public-docs", "cite-and-cache", "cite-only"}:
            mapped.append("public_share_allowed")
        elif value == "internal-cache-only":
            mapped.append("review_required")
        else:
            raise ValueError("public docs license obligation_state is unsupported")
    order = {
        "public_share_allowed": 0,
        "review_required": 1,
        "no_redistribution": 2,
    }
    return _max_by_policy_order("license_obligation_state", mapped, order)


def _most_restrictive_public_docs_freshness_state(values: Iterable[str]) -> str:
    mapped = []
    for value in values:
        if value in {"fresh", "pending", "expired"}:
            mapped.append(value)
        elif value in {"never_indexed", "stale", "unknown"}:
            mapped.append("pending")
        else:
            raise ValueError("public docs freshness_state status is unsupported")
    order = {
        "fresh": 0,
        "pending": 1,
        "expired": 2,
    }
    return _max_by_policy_order("freshness_state", mapped, order)


def _max_by_policy_order(field_name: str, values: Iterable[str], order: Mapping[str, int]) -> str:
    selected: str | None = None
    selected_rank = -1
    for value in values:
        if value not in order:
            raise ValueError(f"public docs {field_name} is unsupported")
        rank = order[value]
        if rank > selected_rank:
            selected = value
            selected_rank = rank
    if selected is None:
        raise ValueError(f"public docs {field_name} requires indexed sources")
    return selected


def _public_docs_frontier_budget(
    payload: Mapping[str, Any],
    *,
    generated_at: str,
) -> dict[str, Any]:
    raw_budget = payload.get("frontier_budget", {})
    if raw_budget is None:
        raw_budget = {}
    if not isinstance(raw_budget, Mapping):
        raise ValueError("frontier_budget must be an object")
    max_optional_frontier_sources = _frontier_budget_non_negative_int(
        raw_budget,
        "max_optional_frontier_sources",
        env_name=_PUBLIC_DOCS_MAX_OPTIONAL_FRONTIER_SOURCES_ENV,
        default=_PUBLIC_DOCS_DEFAULT_MAX_OPTIONAL_FRONTIER_SOURCES,
        upper_bound=2000,
    )
    max_optional_frontier_per_seed = _frontier_budget_non_negative_int(
        raw_budget,
        "max_optional_frontier_per_seed",
        env_name=_PUBLIC_DOCS_MAX_OPTIONAL_FRONTIER_PER_SEED_ENV,
        default=_PUBLIC_DOCS_DEFAULT_MAX_OPTIONAL_FRONTIER_PER_SEED,
        upper_bound=500,
    )
    rotation_key_value = raw_budget.get("rotation_key", generated_at[:10])
    if not isinstance(rotation_key_value, str) or not rotation_key_value.strip():
        raise ValueError("frontier_budget.rotation_key must be a non-empty string")
    return {
        "max_optional_frontier_per_seed": max_optional_frontier_per_seed,
        "max_optional_frontier_sources": max_optional_frontier_sources,
        "rotation_key": rotation_key_value.strip(),
        "strategy": "seed-and-curated-required-rotating-optional-frontier-budget",
    }


def _public_docs_crawler_discovery_workers(
    payload: Mapping[str, Any],
    *,
    seed_count: int,
) -> int:
    value = payload.get("crawler_discovery_workers")
    if value is None:
        value = os.environ.get(
            _PUBLIC_DOCS_CRAWLER_DISCOVERY_WORKERS_ENV,
            _PUBLIC_DOCS_DEFAULT_CRAWLER_DISCOVERY_WORKERS,
        )
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ValueError("crawler_discovery_workers must be a positive integer")
    if isinstance(value, str):
        if not value.strip().isdigit():
            raise ValueError("crawler_discovery_workers must be a positive integer")
        value = int(value.strip())
    if value < 1 or value > _PUBLIC_DOCS_MAX_CRAWLER_DISCOVERY_WORKERS:
        raise ValueError(
            "crawler_discovery_workers must be between 1 and "
            f"{_PUBLIC_DOCS_MAX_CRAWLER_DISCOVERY_WORKERS}"
        )
    return min(value, max(seed_count, 1))


def _frontier_budget_non_negative_int(
    raw_budget: Mapping[str, Any],
    field_name: str,
    *,
    env_name: str,
    default: int,
    upper_bound: int,
) -> int:
    value = raw_budget.get(field_name)
    if value is None:
        value = os.environ.get(env_name, default)
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ValueError(f"frontier_budget.{field_name} must be a non-negative integer")
    if isinstance(value, str):
        if not value.strip().isdigit():
            raise ValueError(f"frontier_budget.{field_name} must be a non-negative integer")
        value = int(value.strip())
    if value < 0:
        raise ValueError(f"frontier_budget.{field_name} must be a non-negative integer")
    if value > upper_bound:
        raise ValueError(f"frontier_budget.{field_name} must be bounded to {upper_bound} or fewer")
    return value


def _public_docs_seed_refresh_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "airflow_plan",
            "public_docs_seed_registry",
            "public_docs_seed_refresh_plan",
        ),
    )
    generated_at = _required_datetime_string(plan, "generated_at")
    refresh_mode = _required_str(plan, "refresh_mode")
    refresh_reason = plan.get("refresh_reason")
    if refresh_reason is not None and not isinstance(refresh_reason, str):
        raise ValueError("refresh_reason must be a string")
    force_full_refresh = refresh_mode == "force_full"
    due_seeds, skipped_seed_refreshes = _public_docs_due_seed_selection(
        _required_object_list(plan, "seed_registry"),
        generated_at,
        force_full_refresh=force_full_refresh,
        refresh_reason=refresh_reason,
    )
    # A D5 activation selects exactly one pack version.  Therefore a delta may
    # not contain only the due subset: that would silently remove fresh sources
    # from the new active pack.  Any due seed rebuilds the entire governed pack;
    # a true no-op is the only cycle that dispatches no source requests.
    candidate_seeds = (
        [
            {
                **dict(seed),
                "refresh_selection": {
                    **_public_docs_seed_refresh_decision(
                        seed,
                        _datetime_value(generated_at, "generated_at"),
                        force_full_refresh=force_full_refresh,
                        refresh_reason=refresh_reason,
                    ),
                    "candidate_rebuild_mode": "full_pack",
                },
            }
            for seed in _required_object_list(plan, "seed_registry")
        ]
        if due_seeds
        else []
    )
    for seed in candidate_seeds:
        crawl_evidence = _required_mapping(seed, "crawl_policy").get("crawl_evidence")
        if crawl_evidence is None:
            continue
        if not isinstance(crawl_evidence, Mapping):
            raise ValueError("crawler evidence must be an object")
        if crawl_evidence.get("status") != "completed":
            raise ValueError(
                "crawler evidence must be completed before pipeline dispatch: "
                f"seed_id={_required_str(seed, 'seed_id')} status={crawl_evidence.get('status')}"
            )
    source_fetch_requests, skipped_frontier_fetches = _public_docs_budgeted_source_fetch_requests(
        plan,
        candidate_seeds,
    )
    optional_frontier_selected_count = sum(
        1
        for request in source_fetch_requests
        if request["source_metadata"]["frontier"]["frontier_role"] == "sitemap-frontier"
    )
    status = "ready_for_pipeline_dispatch" if source_fetch_requests else "no_due_sources"
    return {
        "artifact_paths": artifact_paths,
        "candidate_rebuild_mode": "full_pack" if source_fetch_requests else "no_change",
        "contract_version": _EVAL_CONTRACT_VERSION,
        "d4_dispatch_target": "serp_scan_parse_index",
        "dag_id": "serp_web_seed_crawl_refresh",
        "embedding_mode": _required_str(plan, "embedding_mode"),
        "frontier_budget": dict(_required_mapping(plan, "frontier_budget")),
        "generated_at": generated_at,
        "operation_id": _required_str(plan, "operation_id"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "index_mode": _required_str(plan, "index_mode"),
        "qdrant_collection": _required_str(plan, "qdrant_collection"),
        "refresh_mode": refresh_mode,
        **({"refresh_reason": refresh_reason} if refresh_reason is not None else {}),
        "opensearch_index": _required_str(plan, "opensearch_index"),
        "neo4j_database": _required_str(plan, "neo4j_database"),
        "optional_frontier_selected_count": optional_frontier_selected_count,
        "seed_count": len(source_fetch_requests),
        "seed_registry": [dict(seed) for seed in _required_object_list(plan, "seed_registry")],
        "seed_registry_sha256": _required_str(plan, "seed_registry_sha256"),
        "skipped_frontier_count": len(skipped_frontier_fetches),
        "skipped_frontier_fetches": skipped_frontier_fetches,
        "skipped_seed_count": 0 if source_fetch_requests else len(skipped_seed_refreshes),
        "skipped_seed_refreshes": [] if source_fetch_requests else skipped_seed_refreshes,
        "source_fetch_requests": source_fetch_requests,
        "status": status,
        "tenant_id": _required_str(plan, "tenant_id"),
    }


def _public_docs_budgeted_source_fetch_requests(
    plan: Mapping[str, Any],
    seeds: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    budget = _required_mapping(plan, "frontier_budget")
    max_optional_frontier_sources = _required_frontier_budget_limit(
        budget,
        "max_optional_frontier_sources",
    )
    max_optional_frontier_per_seed = _required_frontier_budget_limit(
        budget,
        "max_optional_frontier_per_seed",
    )
    rotation_key = _required_str(budget, "rotation_key")
    grouped_frontier: list[tuple[list[dict[str, Any]], list[dict[str, Any]]]] = []
    per_seed_selected_optional: list[dict[str, Any]] = []
    skipped_frontier_fetches: list[dict[str, Any]] = []
    for seed in seeds:
        expanded = _expanded_public_docs_seed_frontier(seed)
        mandatory = [
            expanded_seed
            for expanded_seed in expanded
            if _frontier_role(expanded_seed) in {"seed-root", "curated-frontier"}
        ]
        optional = [
            expanded_seed
            for expanded_seed in expanded
            if _frontier_role(expanded_seed) == "sitemap-frontier"
        ]
        selected_optional_ids = _rotating_frontier_selection(
            optional,
            max_optional_frontier_per_seed,
            rotation_key=f"{rotation_key}|{_required_str(seed, 'seed_id')}",
        )
        for optional_seed in optional:
            if _frontier_identity(optional_seed) in selected_optional_ids:
                per_seed_selected_optional.append(optional_seed)
            else:
                skipped_frontier_fetches.append(
                    _frontier_skip_record(
                        optional_seed,
                        skip_reason="per_seed_frontier_budget_exhausted",
                    )
                )
        grouped_frontier.append((mandatory, optional))

    global_selected_optional_ids = _rotating_frontier_selection(
        per_seed_selected_optional,
        max_optional_frontier_sources,
        rotation_key=f"{rotation_key}|global",
    )
    per_seed_selected_optional_ids = {
        _frontier_identity(seed) for seed in per_seed_selected_optional
    }
    source_fetch_requests: list[dict[str, Any]] = []
    for mandatory, optional in grouped_frontier:
        source_fetch_requests.extend(
            _public_docs_source_fetch_request(plan, mandatory_seed) for mandatory_seed in mandatory
        )
        for optional_seed in optional:
            optional_identity = _frontier_identity(optional_seed)
            if optional_identity in global_selected_optional_ids:
                source_fetch_requests.append(_public_docs_source_fetch_request(plan, optional_seed))
            elif optional_identity in per_seed_selected_optional_ids:
                skipped_frontier_fetches.append(
                    _frontier_skip_record(
                        optional_seed,
                        skip_reason="global_frontier_budget_exhausted",
                    )
                )
    return source_fetch_requests, skipped_frontier_fetches


def _required_frontier_budget_limit(budget: Mapping[str, Any], field_name: str) -> int:
    value = budget.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"frontier_budget.{field_name} must be a non-negative integer")
    return value


def _frontier_role(seed: Mapping[str, Any]) -> str:
    return _required_str(_required_mapping(seed, "frontier"), "frontier_role")


def _frontier_identity(seed: Mapping[str, Any]) -> tuple[str, str]:
    return (_required_str(seed, "seed_id"), _required_str(seed, "source_uri"))


def _rotating_frontier_selection(
    frontier: Sequence[Mapping[str, Any]],
    limit: int,
    *,
    rotation_key: str,
) -> set[tuple[str, str]]:
    if limit <= 0:
        return set()
    items = list(frontier)
    if limit >= len(items):
        return {_frontier_identity(seed) for seed in items}
    offset = int(sha256(rotation_key.encode("utf-8")).hexdigest()[:16], 16) % len(items)
    selected = [*items[offset:], *items[:offset]][:limit]
    return {_frontier_identity(seed) for seed in selected}


def _frontier_skip_record(
    seed: Mapping[str, Any],
    *,
    skip_reason: str,
) -> dict[str, Any]:
    frontier = _required_mapping(seed, "frontier")
    source_uri = _required_str(seed, "source_uri")
    return {
        "frontier_index": _required_positive_int(frontier, "frontier_index"),
        "frontier_role": _required_str(frontier, "frontier_role"),
        "parent_seed_id": _required_str(frontier, "parent_seed_id"),
        "seed_id": _required_str(seed, "seed_id"),
        "skip_reason": skip_reason,
        "source_id": _required_str(seed, "source_id"),
        "source_uri": source_uri,
        "source_uri_hash": f"sha256:{sha256(source_uri.encode('utf-8')).hexdigest()}",
    }


def _public_docs_source_fetch_requests(
    plan: Mapping[str, Any],
    seed: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        _public_docs_source_fetch_request(plan, expanded_seed)
        for expanded_seed in _expanded_public_docs_seed_frontier(seed)
    ]


def _public_docs_source_fetch_request(
    plan: Mapping[str, Any],
    seed: Mapping[str, Any],
) -> dict[str, Any]:
    source_uri = _required_str(seed, "source_uri")
    source_id = _required_str(seed, "source_id")
    seed_id = _required_str(seed, "seed_id")
    source_type = _required_str(seed, "source_type")
    operation_id = _required_str(plan, "operation_id")
    fetch_run_id = str(uuid5(_PUBLIC_DOCS_NAMESPACE, f"fetch|{operation_id}|{seed_id}"))
    parse_run_id = str(uuid5(_PUBLIC_DOCS_NAMESPACE, f"parse|{operation_id}|{seed_id}"))
    pipeline_run_id = str(uuid5(_PUBLIC_DOCS_NAMESPACE, f"pipeline|{operation_id}|{seed_id}"))
    idempotency_key = str(
        uuid5(
            _PUBLIC_DOCS_NAMESPACE,
            "|".join(
                (
                    "public-docs-seed-refresh",
                    _required_str(plan, "tenant_id"),
                    seed_id,
                    source_id,
                    source_uri,
                    _required_str(plan, "seed_registry_sha256"),
                )
            ),
        )
    )
    source_metadata = dict(_required_mapping(seed, "metadata"))
    source_metadata.update(
        {
            "crawl_policy": dict(_required_mapping(seed, "crawl_policy")),
            "inventory_evidence": dict(_required_mapping(seed, "inventory_evidence")),
            "license": dict(_required_mapping(seed, "license")),
            "frontier": dict(_required_mapping(seed, "frontier")),
            "refresh_policy": dict(_required_mapping(seed, "refresh_policy")),
            "refresh_selection": dict(_required_mapping(seed, "refresh_selection")),
        }
    )
    return {
        "connector_name": _required_str(seed, "connector_name"),
        "data_class": _required_str(seed, "data_class"),
        "fetch_run_id": fetch_run_id,
        "idempotency_key": idempotency_key,
        "official_docs_uri": _required_str(seed, "official_docs_uri"),
        "pipeline_run_spec": {
            "index_targets": ["qdrant", "opensearch", "neo4j"],
            "pack_id": _required_str(plan, "pack_id"),
            "pack_version_id": _required_str(plan, "pack_version_id"),
            "parse_run_id": parse_run_id,
            "pipeline_run_id": pipeline_run_id,
            "pipeline_stages": ["fetch", "parse", "chunk", "embed", "index"],
            "publish_state_after_index": "activation_pending",
            "source_id": source_id,
            "source_type": source_type,
            "tenant_id": _required_str(plan, "tenant_id"),
        },
        "seed_id": seed_id,
        "source_id": source_id,
        "source_metadata": source_metadata,
        "source_type": source_type,
        "source_uri": source_uri,
        "source_uri_hash": f"sha256:{sha256(source_uri.encode('utf-8')).hexdigest()}",
        "status": "ready_for_fetch",
    }


def _expanded_public_docs_seed_frontier(seed: Mapping[str, Any]) -> list[dict[str, Any]]:
    source_type = _required_str(seed, "source_type")
    source_uri = _required_str(seed, "source_uri")
    crawl_policy = _required_mapping(seed, "crawl_policy")
    frontier_urls = list(crawl_policy.get("frontier_urls", []))
    curated_frontier_urls = {
        _canonical_public_docs_url(url) for url in _curated_frontier_urls(crawl_policy)
    }
    if source_type != "website" or not frontier_urls:
        singleton = dict(seed)
        singleton["frontier"] = _frontier_metadata(
            parent_seed=seed,
            source_uri=source_uri,
            frontier_index=0,
            frontier_role="seed-root",
            frontier_url_count=1,
        )
        return [singleton]
    max_pages = _required_positive_int(crawl_policy, "max_pages")
    bounded_urls = [source_uri, *frontier_urls[: max_pages - 1]]
    frontier_url_count = len(bounded_urls)
    expanded = [
        _frontier_seed(
            seed,
            source_uri=url,
            frontier_index=index,
            frontier_role=(
                "seed-root"
                if index == 0
                else (
                    "curated-frontier"
                    if _canonical_public_docs_url(url) in curated_frontier_urls
                    else "sitemap-frontier"
                )
            ),
            frontier_url_count=frontier_url_count,
        )
        for index, url in enumerate(bounded_urls)
    ]
    return expanded


def _frontier_seed(
    seed: Mapping[str, Any],
    *,
    source_uri: str,
    frontier_index: int,
    frontier_role: str,
    frontier_url_count: int,
) -> dict[str, Any]:
    parent_seed_id = _required_str(seed, "seed_id")
    expanded = dict(seed)
    expanded["source_uri"] = source_uri
    expanded["official_docs_uri"] = source_uri
    if frontier_index > 0:
        expanded["seed_id"] = f"{parent_seed_id}--{sha256(source_uri.encode()).hexdigest()[:12]}"
        expanded["source_id"] = str(
            uuid5(
                _PUBLIC_DOCS_NAMESPACE, f"frontier|{_required_str(seed, 'source_id')}|{source_uri}"
            )
        )
    expanded["frontier"] = _frontier_metadata(
        parent_seed=seed,
        source_uri=source_uri,
        frontier_index=frontier_index,
        frontier_role=frontier_role,
        frontier_url_count=frontier_url_count,
    )
    return expanded


def _frontier_metadata(
    *,
    parent_seed: Mapping[str, Any],
    source_uri: str,
    frontier_index: int,
    frontier_role: str,
    frontier_url_count: int,
) -> dict[str, Any]:
    return {
        "discovery_mode": "governed-seed-frontier",
        "frontier_index": frontier_index,
        "frontier_role": frontier_role,
        "frontier_url_count": frontier_url_count,
        "parent_seed_id": _required_str(parent_seed, "seed_id"),
        "parent_source_id": _required_str(parent_seed, "source_id"),
        "source_uri_hash": f"sha256:{sha256(source_uri.encode('utf-8')).hexdigest()}",
    }


def _ensure_public_docs_catalog_source(
    plan: Mapping[str, Any],
    *,
    bc21_base_url: str,
) -> str:
    base_url = _required_bc21_base_url({"bc21_base_url": bc21_base_url}).rstrip("/")
    tenant_id = _required_str(plan, "tenant_id")
    actor_id = _required_str(plan, "actor_id")
    seed_registry_sha256 = _required_str(plan, "seed_registry_sha256")
    source_uri_hash = f"sha256:{seed_registry_sha256}"
    existing_source_id = _find_public_docs_catalog_source_id(
        base_url,
        tenant_id=tenant_id,
        source_uri_hash=source_uri_hash,
    )
    if existing_source_id:
        return existing_source_id

    body = {
        "accessScope": "public",
        "dataClass": "PUBLIC",
        "displayName": "SERP public docs seed registry",
        "ownerActorId": actor_id,
        "sourceType": "markdown",
        "sourceUriHash": source_uri_hash,
    }
    fingerprint = "sha256:" + sha256(_canonical_json(body).encode("utf-8")).hexdigest()
    idempotency_key = str(
        uuid5(
            _PUBLIC_DOCS_NAMESPACE,
            "|".join(("bc21-public-docs-catalog-source", tenant_id, source_uri_hash)),
        )
    )
    response_payload = _bc21_json_request(
        base_url + "/api/bc-21/serp/v1/sources",
        method="POST",
        body=body,
        headers={
            "X-Adapstory-Actor-Id": actor_id,
            "X-Adapstory-Tenant-Id": tenant_id,
            "X-Fingerprint": fingerprint,
            "X-Idempotency-Key": idempotency_key,
        },
        error_label="public docs catalog source registration",
        allow_conflict=True,
    )
    if response_payload is not None:
        return _required_str(response_payload, "resourceId")
    existing_source_id = _find_public_docs_catalog_source_id(
        base_url,
        tenant_id=tenant_id,
        source_uri_hash=source_uri_hash,
    )
    if existing_source_id:
        return existing_source_id
    raise ValueError(
        "public docs catalog source registration conflicted but source was not resolvable"
    )


def _find_public_docs_catalog_source_id(
    base_url: str,
    *,
    tenant_id: str,
    source_uri_hash: str,
) -> str | None:
    response_payload = _bc21_json_request(
        base_url + "/api/bc-21/serp/v1/sources",
        method="GET",
        body=None,
        headers={"X-Adapstory-Tenant-Id": tenant_id},
        error_label="public docs catalog source lookup",
    )
    if response_payload is None:
        return None
    for item in _required_object_list(response_payload, "items"):
        if _required_str(item, "sourceUriHash") == source_uri_hash:
            return _required_str(item, "sourceId")
    return None


def _bc21_json_request(
    url: str,
    *,
    method: str,
    body: Mapping[str, Any] | None,
    headers: Mapping[str, str],
    error_label: str,
    allow_conflict: bool = False,
) -> dict[str, Any] | None:
    if any(str(key).casefold() == "authorization" for key in headers):
        raise ValueError(
            "BC-21 authorization must use the projected Kubernetes ServiceAccount token"
        )
    body_bytes = None if body is None else _canonical_json(body).encode("utf-8")
    request = Request(
        url,
        data=body_bytes,
        headers={
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if body is not None else {}),
            **{str(key): str(value) for key, value in headers.items()},
            **_bc21_workload_authorization_headers(),
        },
        method=method,
    )
    try:
        with urlopen(request, timeout=5.0) as response:
            return dict(_json_object(response.read().decode("utf-8"), error_label))
    except HTTPError as exc:
        if allow_conflict and exc.code == 409:
            return None
        raise ValueError(
            f"{error_label} failed: status={exc.code}{_safe_bc21_problem_detail(exc)}"
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ValueError(f"{error_label} failed") from exc


def _bc21_workload_authorization_headers() -> dict[str, str]:
    raw_token_path = os.environ.get(_BC21_SERVICE_ACCOUNT_TOKEN_PATH_ENV, "").strip()
    token_path = Path(raw_token_path) if raw_token_path else _DEFAULT_SERVICE_ACCOUNT_TOKEN_PATH
    try:
        token = token_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ValueError("BC-21 projected Kubernetes ServiceAccount token is unavailable") from exc
    if not token:
        raise ValueError("BC-21 projected Kubernetes ServiceAccount token is empty")
    return {"Authorization": f"Bearer {token}"}


def _safe_bc21_problem_detail(exc: HTTPError) -> str:
    try:
        payload = json.loads(exc.read().decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, Mapping):
        return ""
    title = payload.get("title")
    detail = payload.get("detail")
    if not isinstance(title, str) or not isinstance(detail, str):
        return ""
    normalized_title = title.strip()
    normalized_detail = detail.strip()
    if not normalized_title or not normalized_detail:
        return ""
    return f" problem={normalized_title[:120]}: {normalized_detail[:240]}"


def _paired_eval_request_payload(
    plan: Mapping[str, Any],
    *,
    catalog_evidence: Mapping[str, Mapping[str, str]],
    activation_evidence: Mapping[str, str],
    promotion: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the canonical request without inline execution selections."""

    baseline = _required_mapping(promotion, "baselineRelease")
    candidate = _required_mapping(promotion, "candidateRelease")
    return {
        "schema": _PAIRED_EVALUATION_REQUEST_SCHEMA,
        "requestId": _required_str(plan, "operation_id"),
        "evaluationReleasePromotionEvidence": dict(
            _required_mapping(promotion, "promotionEvidence")
        ),
        "baselineReleaseEvidence": dict(_required_mapping(baseline, "evidence")),
        "candidateReleaseEvidence": dict(_required_mapping(candidate, "evidence")),
        "evaluationBindingId": _required_str(lifecycle, "evaluationBindingId"),
        "evaluationBindingEvidence": dict(
            _required_mapping(lifecycle, "evaluationBindingEvidence")
        ),
        "metricCompatibilityMatrixEvidence": dict(
            _required_mapping(promotion, "metricCompatibilityMatrixEvidence")
        ),
        "evaluationObjectiveEvidence": dict(
            _required_mapping(promotion, "evaluationObjectiveEvidence")
        ),
        "evaluationObjectiveAttestationEvidence": dict(
            _required_mapping(promotion, "evaluationObjectiveAttestationEvidence")
        ),
        "benchmarkCatalogEvidence": {
            "activation": dict(activation_evidence),
            "catalog": dict(_required_mapping(catalog_evidence, "catalog")),
            "receipt": dict(_required_mapping(catalog_evidence, "receipt")),
        },
    }


def _benchmark_catalog_pack_activation_payload(
    plan: Mapping[str, Any],
    *,
    catalog_evidence: Mapping[str, Mapping[str, str]],
    lifecycle: Mapping[str, Any],
) -> dict[str, Any]:
    suite_bindings = _required_object_list(lifecycle, "packMaterialBindings")
    if [_required_str(item, "suiteId") for item in suite_bindings] != list(
        MANDATORY_SERP_BENCHMARK_SUITES
    ):
        raise ValueError("benchmark catalog activation must cover the canonical nine")
    return {
        "activationStatus": "evaluation-only",
        "benchmarkCatalogEvidence": {
            "catalog": dict(_required_mapping(catalog_evidence, "catalog")),
            "receipt": dict(_required_mapping(catalog_evidence, "receipt")),
        },
        "bindingFingerprint": _required_sha256_prefixed(lifecycle, "bindingFingerprint"),
        "contractVersion": _BENCHMARK_CATALOG_PACK_ACTIVATION_SCHEMA,
        "evaluationBindingEvidence": dict(
            _required_mapping(lifecycle, "evaluationBindingEvidence")
        ),
        "evaluationBindingId": _required_str(lifecycle, "evaluationBindingId"),
        "operationId": _required_str(plan, "operation_id"),
        "productionActivationRequested": False,
        "suitePackBindings": suite_bindings,
        "tenantId": _required_str(plan, "tenant_id"),
    }


def _validated_d19_lifecycle_result(
    plan: Mapping[str, Any],
    promotion: Mapping[str, Any],
    lifecycle_result: Mapping[str, Any],
) -> dict[str, Any]:
    expected_fields = {
        "baselineReleaseDigest",
        "baselineReleaseEvidence",
        "bindingFingerprint",
        "candidateReleaseDigest",
        "candidateReleaseEvidence",
        "evaluationBindingEvidence",
        "evaluationBindingId",
        "evaluationReleasePromotionEvidence",
        "expiresAt",
        "indexedReceiptCount",
        "packMaterialBindings",
        "productionActivationRequested",
        "schema",
        "suiteExecutionBindings",
        "tenantId",
    }
    if set(lifecycle_result) != expected_fields:
        raise ValueError("D19 benchmark pack lifecycle result fields are unsupported")
    if _required_str(lifecycle_result, "schema") != "BC21AllNineBenchmarkPackLifecycleResult/v1":
        raise ValueError("D19 benchmark pack lifecycle result schema is unsupported")
    if _required_str(lifecycle_result, "tenantId") != _required_str(plan, "tenant_id"):
        raise ValueError("D19 benchmark pack lifecycle tenantId does not match plan")
    binding_id = str(_required_uuid(lifecycle_result, "evaluationBindingId"))
    binding_evidence = _worm_evidence_reference(lifecycle_result, "evaluationBindingEvidence")
    _require_worm_evidence_within_artifact_root(
        binding_evidence,
        _required_str(plan, "artifact_root_path"),
        "evaluationBindingEvidence",
    )
    promotion_evidence = _worm_evidence_reference(
        lifecycle_result, "evaluationReleasePromotionEvidence"
    )
    if promotion_evidence != _required_mapping(promotion, "promotionEvidence"):
        raise ValueError("D19 lifecycle promotion evidence does not match D17")
    baseline_evidence = _worm_evidence_reference(lifecycle_result, "baselineReleaseEvidence")
    candidate_evidence = _worm_evidence_reference(lifecycle_result, "candidateReleaseEvidence")
    baseline = _required_mapping(promotion, "baselineRelease")
    candidate = _required_mapping(promotion, "candidateRelease")
    if baseline_evidence != _required_mapping(baseline, "evidence"):
        raise ValueError("D19 lifecycle baseline release does not match D17")
    if candidate_evidence != _required_mapping(candidate, "evidence"):
        raise ValueError("D19 lifecycle candidate release does not match D17")
    baseline_digest = _required_sha256_prefixed(lifecycle_result, "baselineReleaseDigest")
    candidate_digest = _required_sha256_prefixed(lifecycle_result, "candidateReleaseDigest")
    if baseline_digest != _required_str(baseline, "releaseDigest"):
        raise ValueError("D19 lifecycle baseline digest does not match D17")
    if candidate_digest != _required_str(candidate, "releaseDigest"):
        raise ValueError("D19 lifecycle candidate digest does not match D17")
    binding_fingerprint = _required_sha256_prefixed(lifecycle_result, "bindingFingerprint")
    _required_datetime_string(lifecycle_result, "expiresAt")
    indexed_receipt_count = lifecycle_result.get("indexedReceiptCount")
    if indexed_receipt_count != 18:
        raise ValueError("D19 lifecycle must prove exactly 18 indexed receipts")
    if lifecycle_result.get("productionActivationRequested") is not False:
        raise ValueError("D19 lifecycle must not request production activation")
    pack_material_bindings = _required_object_list(lifecycle_result, "packMaterialBindings")
    suite_execution_bindings = _required_object_list(lifecycle_result, "suiteExecutionBindings")
    for field_name, bindings in (
        ("packMaterialBindings", pack_material_bindings),
        ("suiteExecutionBindings", suite_execution_bindings),
    ):
        observed_suites = [_required_str(item, "suiteId") for item in bindings]
        if observed_suites != list(MANDATORY_SERP_BENCHMARK_SUITES):
            raise ValueError(f"D19 lifecycle {field_name} must cover the canonical nine")
    return {
        **dict(lifecycle_result),
        "baselineReleaseDigest": baseline_digest,
        "baselineReleaseEvidence": baseline_evidence,
        "bindingFingerprint": binding_fingerprint,
        "candidateReleaseDigest": candidate_digest,
        "candidateReleaseEvidence": candidate_evidence,
        "evaluationBindingEvidence": binding_evidence,
        "evaluationBindingId": binding_id,
        "evaluationReleasePromotionEvidence": promotion_evidence,
    }


def _paired_eval_catalog_evidence(
    plan: Mapping[str, Any], catalog_snapshot: Mapping[str, Any]
) -> dict[str, dict[str, str]]:
    """Bind D19 to exactly the catalog materialized by its acquisition workload."""

    artifact_paths = _required_artifact_paths(
        plan,
        ("benchmark_catalog", "benchmark_catalog_receipt"),
    )
    catalog_path = _required_str(catalog_snapshot, "artifactPath")
    if catalog_path != artifact_paths["benchmark_catalog"]:
        raise ValueError("paired evaluator catalog artifact path does not match plan")
    receipt_path = _required_str(catalog_snapshot, "catalogReceiptPath")
    if receipt_path != artifact_paths["benchmark_catalog_receipt"]:
        raise ValueError("paired evaluator catalog receipt path does not match plan")
    if _required_str(catalog_snapshot, "objectLockMode") != "COMPLIANCE":
        raise ValueError("paired evaluator catalog must use COMPLIANCE object lock")
    suite_summary = normalize_benchmark_catalog_suite_summary(catalog_snapshot.get("suiteSummary"))
    blocking_suite_ids = _required_str_list_allow_empty(catalog_snapshot, "blockingSuiteIds")
    derived_blocking_suite_ids = [
        item["suiteId"] for item in suite_summary if item["executionStatus"] != "ready"
    ]
    if blocking_suite_ids != derived_blocking_suite_ids:
        raise ValueError("paired evaluator catalog blocking suites do not match summary")
    catalog_status = _required_str(catalog_snapshot, "catalogStatus")
    if catalog_status not in {"ready", "blocked"}:
        raise ValueError("paired evaluator catalog status is unsupported")
    if (catalog_status == "ready") != (not blocking_suite_ids):
        raise ValueError("paired evaluator catalog status does not match blocking suites")
    catalog_retain_until = _required_datetime_string(catalog_snapshot, "catalogRetainUntil")
    receipt_retain_until = _required_datetime_string(catalog_snapshot, "catalogReceiptRetainUntil")
    return {
        "catalog": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": catalog_retain_until,
            "s3Uri": catalog_path,
            "sha256": "sha256:" + _required_sha256_hex(catalog_snapshot, "artifactSha256"),
            "versionId": _required_str(catalog_snapshot, "artifactVersionId"),
        },
        "receipt": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": receipt_retain_until,
            "s3Uri": receipt_path,
            "sha256": "sha256:" + _required_sha256_hex(catalog_snapshot, "catalogReceiptSha256"),
            "versionId": _required_str(catalog_snapshot, "catalogReceiptVersionId"),
        },
    }


def _validated_d19_promotion_snapshot(
    plan: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> dict[str, Any]:
    evidence = _worm_evidence_reference(snapshot, "promotionEvidence")
    if evidence != _worm_evidence_reference(plan, "evaluation_release_promotion_evidence"):
        raise ValueError("D19 promotion snapshot evidence does not match plan")
    promotion = _required_mapping(snapshot, "promotion")
    if _required_str(promotion, "schema") != _EVALUATION_RELEASE_PROMOTION_SCHEMA:
        raise ValueError("D19 promotion schema is unsupported")
    baseline = _validated_promoted_release_reference(
        _required_mapping(promotion, "baselineRelease"), "baselineRelease"
    )
    candidate = _validated_promoted_release_reference(
        _required_mapping(promotion, "candidateRelease"), "candidateRelease"
    )
    candidate_authority = _validated_candidate_release_authority(
        _required_mapping(promotion, "candidateReleaseAuthority")
    )
    if candidate_authority["evidence"] != candidate["evidence"]:
        raise ValueError("D19 candidateReleaseAuthority evidence does not match candidateRelease")
    if candidate_authority["releaseDigest"] != candidate["releaseDigest"]:
        raise ValueError("D19 candidateReleaseAuthority digest does not match candidateRelease")
    for field_name, plan_field in (
        ("tenantId", "tenant_id"),
        ("registryResourceId", "registry_resource_id"),
        ("registryResourceType", "registry_resource_type"),
    ):
        if _required_str(promotion, field_name) != _required_str(plan, plan_field):
            raise ValueError(f"D19 promotion {field_name} does not match plan")
    return {
        "baselineRelease": baseline,
        "candidateRelease": candidate,
        "candidateReleaseAuthority": candidate_authority,
        "metricCompatibilityMatrixEvidence": _worm_evidence_reference(
            promotion, "metricCompatibilityMatrixEvidence"
        ),
        "evaluationObjectiveEvidence": _worm_evidence_reference(
            promotion, "evaluationObjectiveEvidence"
        ),
        "evaluationObjectiveAttestationEvidence": _worm_evidence_reference(
            promotion, "evaluationObjectiveAttestationEvidence"
        ),
        "promotionEvidence": evidence,
        "promotionId": _required_str(promotion, "promotionId"),
    }


def _required_metric_compatibility(
    payload: Mapping[str, Any],
    *,
    selected_suite_ids: Sequence[str],
    contract_version_field: str,
    matrix_uri_field: str,
    matrix_sha256_field: str,
    matrix_version_id_field: str,
    suite_id_field: str,
    metric_families_field: str,
) -> dict[str, Any]:
    if _required_str(payload, contract_version_field) != _METRIC_COMPATIBILITY_CONTRACT_VERSION:
        raise ValueError("unsupported metric_compatibility contract version")
    matrix_uri = _required_str(payload, matrix_uri_field)
    matrix_sha256 = _required_str(payload, matrix_sha256_field)
    matrix_version_id = _required_str(payload, matrix_version_id_field)
    if _artifact_ref(matrix_uri_field, matrix_uri).kind != "s3":
        raise ValueError("metric_compatibility matrix URI must be an immutable s3:// artifact")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", matrix_sha256):
        raise ValueError("metric_compatibility matrix SHA-256 is invalid")
    requirements_by_suite: dict[str, dict[str, Any]] = {}
    for raw_requirement in _required_object_list(payload, "requirements"):
        suite_id = _required_str(raw_requirement, suite_id_field)
        if suite_id in requirements_by_suite:
            raise ValueError(f"metric_compatibility has duplicate suite {suite_id!r}")
        metric_families = _required_str_list(raw_requirement, metric_families_field)
        unsupported_metric_families = sorted(
            set(metric_families).difference(_BENCHMARK_METRIC_FAMILIES)
        )
        if unsupported_metric_families:
            raise ValueError(
                "metric_compatibility has unsupported metric families: "
                + ", ".join(unsupported_metric_families)
            )
        canonical_metric_families = tuple(
            metric_family
            for metric_family in _BENCHMARK_METRIC_FAMILIES
            if metric_family in metric_families
        )
        if tuple(metric_families) != canonical_metric_families:
            raise ValueError("metric_compatibility metric families must be canonical and unique")
        requirements_by_suite[suite_id] = {
            "metric_families": list(canonical_metric_families),
            "suite_id": suite_id,
        }
    if tuple(requirements_by_suite) != tuple(selected_suite_ids):
        raise ValueError("metric_compatibility must include selected suites in canonical order")
    return {
        "contract_version": _METRIC_COMPATIBILITY_CONTRACT_VERSION,
        "matrix_sha256": matrix_sha256,
        "matrix_uri": matrix_uri,
        "matrix_version_id": matrix_version_id,
        "requirements": [requirements_by_suite[suite_id] for suite_id in selected_suite_ids],
    }


def _public_docs_seed_registry(
    payload: Mapping[str, Any],
    *,
    crawler_discovery_workers: int,
    sitemap_frontier_discoverer: PublicDocsSitemapFrontierDiscoverer | None = None,
) -> list[dict[str, Any]]:
    raw_seeds = _required_object_list(payload, "seed_registry")
    seed_builder = partial(
        _public_docs_seed,
        sitemap_frontier_discoverer=sitemap_frontier_discoverer,
    )
    if sitemap_frontier_discoverer is None or crawler_discovery_workers == 1 or len(raw_seeds) < 2:
        seeds = [seed_builder(seed) for seed in raw_seeds]
    else:
        with ThreadPoolExecutor(
            max_workers=crawler_discovery_workers,
            thread_name_prefix="public-docs-crawl",
        ) as executor:
            seeds = list(executor.map(seed_builder, raw_seeds))
    _require_unique_public_docs_seed_values(seeds)
    return sorted(seeds, key=lambda seed: _required_str(seed, "seed_id"))


def _default_public_docs_seed_registry() -> list[dict[str, Any]]:
    return [
        _default_public_docs_seed(
            str(source["seed_id"]),
            str(source.get("source_type", "website")),
            str(source["docs_url"]),
            catalog_docs_url=str(source.get("catalog_docs_url", source["docs_url"])),
            component=str(source["component"]),
            frontier_urls=tuple(str(value) for value in source.get("frontier_urls", ())),
            priority=str(source.get("priority", "P0")),
            releases_url=str(source["releases_url"]),
            repo_url=str(source["repo_url"]),
            suggested_ingest_modes=tuple(str(value) for value in source["suggested_ingest_modes"]),
            version=str(source.get("version", "catalog@2026-07-08")),
        )
        for source in p0_public_docs_sources()
    ]


def _default_public_docs_seed(
    seed_id: str,
    source_type: str,
    source_uri: str,
    *,
    catalog_docs_url: str,
    component: str,
    releases_url: str,
    repo_url: str,
    suggested_ingest_modes: Sequence[str],
    version: str,
    frontier_urls: Sequence[str] = (),
    priority: str = "P0",
) -> dict[str, Any]:
    parsed = urlparse(source_uri)
    allowed_domain = parsed.hostname or "opt.adapstory"
    evidence_payload = {
        "catalog_docs_url": catalog_docs_url,
        "component": component,
        "docs_url": source_uri,
        "releases_url": releases_url,
        "repo_url": repo_url,
        "source_type": source_type,
        "stack_inventory_path": _PUBLIC_DOCS_STACK_INVENTORY_PATH,
        "suggested_ingest_modes": list(suggested_ingest_modes),
        "version": version,
    }
    return {
        "approved": True,
        "connector_name": source_type,
        "crawl_policy": {
            "allowed_domains": [allowed_domain],
            "curated_frontier_urls": list(frontier_urls),
            "deny_patterns": ["/login", "/admin"],
            "frontier_urls": list(frontier_urls),
            "max_depth": 2,
            "max_pages": 25,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "AdapstorySERPDocsRefresh/2026.07",
        },
        "data_class": "PUBLIC",
        "inventory_evidence": {
            "component": component,
            "evidence_sha256": sha256(
                json.dumps(evidence_payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            "stack_inventory_path": _PUBLIC_DOCS_STACK_INVENTORY_PATH,
            "version": version,
        },
        "license": {
            "distribution_rule": "cite-and-cache",
            "obligation_state": "reviewed-public-docs",
        },
        "metadata": {
            "catalog_docs_url": catalog_docs_url,
            "nightly_source_catalog_path": PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH,
            "origin": _PUBLIC_DOCS_STACK_INVENTORY_PATH,
            "priority": priority,
            "purpose": "public-docs-seed-to-serve",
            "releases_url": releases_url,
            "repo_url": repo_url,
            "suggested_ingest_modes": list(suggested_ingest_modes),
        },
        "official_docs_uri": source_uri,
        "refresh_policy": {
            "cadence": "daily",
            "max_age_hours": 24,
        },
        "seed_id": seed_id,
        "source_id": str(uuid5(NAMESPACE_URL, f"adapstory-serp-public-docs:{seed_id}")),
        "source_type": source_type,
        "source_uri": source_uri,
    }


def _public_docs_seed(
    seed: Mapping[str, Any],
    *,
    sitemap_frontier_discoverer: PublicDocsSitemapFrontierDiscoverer | None = None,
) -> dict[str, Any]:
    _reject_raw_secrets(seed)
    seed_id = _required_seed_id(seed)
    source_id = str(_required_uuid(seed, "source_id"))
    source_type = _required_public_docs_source_type(seed)
    source_uri = _required_public_docs_source_uri(seed, source_type)
    official_docs_uri = _required_public_docs_official_docs_uri(seed)
    crawl_policy = _public_docs_crawl_policy(
        seed,
        source_uri,
        source_type,
        sitemap_frontier_discoverer=sitemap_frontier_discoverer,
    )
    refresh_policy = _public_docs_refresh_policy(seed)
    license_contract = _public_docs_license(seed)
    inventory_evidence = _public_docs_inventory_evidence(seed)
    metadata = seed.get("metadata", {})
    if not isinstance(metadata, Mapping):
        raise ValueError("metadata must be an object")
    return {
        "approved": _required_public_docs_approved(seed),
        "connector_name": _required_public_docs_connector_name(seed, source_type),
        "crawl_policy": crawl_policy,
        "data_class": _required_public_docs_data_class(seed),
        "freshness_state": _public_docs_freshness_state(seed),
        "inventory_evidence": inventory_evidence,
        "license": license_contract,
        "metadata": dict(metadata),
        "official_docs_uri": official_docs_uri,
        "refresh_policy": refresh_policy,
        "seed_id": seed_id,
        "source_id": source_id,
        "source_type": source_type,
        "source_uri": source_uri,
    }


def _required_seed_id(seed: Mapping[str, Any]) -> str:
    seed_id = _required_str(seed, "seed_id")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,80}", seed_id):
        raise ValueError("seed_id must be stable lowercase slug")
    return seed_id


def _required_public_docs_approved(seed: Mapping[str, Any]) -> bool:
    if seed.get("approved") is not True:
        raise ValueError("approved must be true")
    return True


def _required_public_docs_source_type(seed: Mapping[str, Any]) -> str:
    source_type = _required_str(seed, "source_type")
    if source_type not in _PUBLIC_DOCS_EXECUTABLE_SOURCE_TYPES:
        raise ValueError("source_type is not executable by current connectors")
    return source_type


def _required_public_docs_connector_name(
    seed: Mapping[str, Any],
    source_type: str,
) -> str:
    connector_name = _required_str(seed, "connector_name")
    if connector_name != source_type:
        raise ValueError("connector_name must match source_type")
    return connector_name


def _required_public_docs_data_class(seed: Mapping[str, Any]) -> str:
    data_class = _required_str(seed, "data_class")
    if data_class not in _PUBLIC_DOCS_DATA_CLASSES:
        raise ValueError("data_class is not allowed for public docs seed refresh")
    return data_class


def _required_public_docs_source_uri(seed: Mapping[str, Any], source_type: str) -> str:
    source_uri = _required_str(seed, "source_uri")
    if _contains_raw_secret(source_uri):
        raise ValueError("source_uri must not contain raw secret material")
    parsed = urlparse(source_uri)
    if source_type == "git":
        if parsed.scheme != "git+file":
            raise ValueError(
                "git public docs seeds must use git+file until remote git connector exists"
            )
        return source_uri
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("public docs source_uri must use https")
    return source_uri


def _required_public_docs_official_docs_uri(seed: Mapping[str, Any]) -> str:
    official_docs_uri = _required_str(seed, "official_docs_uri")
    parsed = urlparse(official_docs_uri)
    if parsed.scheme not in {"https", "git+file"}:
        raise ValueError("official_docs_uri must use an approved docs URI scheme")
    if parsed.scheme == "https" and not parsed.hostname:
        raise ValueError("official_docs_uri must include a host")
    return official_docs_uri


def _public_docs_crawl_policy(
    seed: Mapping[str, Any],
    source_uri: str,
    source_type: str,
    *,
    sitemap_frontier_discoverer: PublicDocsSitemapFrontierDiscoverer | None = None,
) -> dict[str, Any]:
    policy = _required_mapping(seed, "crawl_policy")
    if policy.get("respect_robots_txt") is not True:
        raise ValueError("respect_robots_txt must be true")
    max_depth = _required_positive_int(policy, "max_depth")
    max_pages = _required_positive_int(policy, "max_pages")
    if max_depth > 5:
        raise ValueError("max_depth must be bounded to five or fewer")
    if max_pages > 500:
        raise ValueError("max_pages must be bounded to 500 or fewer")
    sitemap_discovery = policy.get("sitemap_discovery")
    if not isinstance(sitemap_discovery, bool):
        raise ValueError("sitemap_discovery must be boolean")
    allowed_domains = _required_str_list(policy, "allowed_domains")
    deny_patterns = policy.get("deny_patterns", [])
    if not isinstance(deny_patterns, list) or not all(
        isinstance(value, str) and value.strip() for value in deny_patterns
    ):
        raise ValueError("deny_patterns must be a list of strings")
    if source_type != "git":
        hostname = urlparse(source_uri).hostname
        if hostname not in set(allowed_domains):
            raise ValueError("source_uri host must be in allowed_domains")
        if _public_docs_url_matches_deny_patterns(source_uri, deny_patterns):
            raise ValueError("source_uri must not match deny_patterns")
    curated_frontier_urls = _curated_frontier_urls(policy)
    freshness_state = seed.get("freshness_state")
    previous_state = (
        freshness_state.get("page_state", {}) if isinstance(freshness_state, Mapping) else {}
    )
    if not isinstance(previous_state, Mapping):
        raise ValueError("freshness_state.page_state must be an object")
    crawler_policy = {**policy, "previous_state": dict(previous_state)}
    frontier_urls, crawl_evidence = _public_docs_frontier_urls(
        crawler_policy,
        source_uri=source_uri,
        source_type=source_type,
        allowed_domains=allowed_domains,
        deny_patterns=list(deny_patterns),
        sitemap_discovery=sitemap_discovery,
        max_pages=max_pages,
        sitemap_frontier_discoverer=sitemap_frontier_discoverer,
    )
    return {
        "allowed_domains": allowed_domains,
        "curated_frontier_urls": curated_frontier_urls,
        "deny_patterns": list(deny_patterns),
        "frontier_urls": frontier_urls,
        "max_depth": max_depth,
        "max_pages": max_pages,
        "previous_state": dict(previous_state),
        "crawl_evidence": crawl_evidence,
        "respect_robots_txt": True,
        "sitemap_discovery": sitemap_discovery,
        "user_agent": _required_str(policy, "user_agent"),
    }


def _public_docs_frontier_urls(
    policy: Mapping[str, Any],
    *,
    source_uri: str,
    source_type: str,
    allowed_domains: Sequence[str],
    deny_patterns: Sequence[str],
    sitemap_discovery: bool,
    max_pages: int,
    sitemap_frontier_discoverer: PublicDocsSitemapFrontierDiscoverer | None = None,
) -> tuple[list[str], Mapping[str, Any] | None]:
    curated_urls = _curated_frontier_urls(policy)
    if source_type != "website":
        if curated_urls:
            raise ValueError("curated_frontier_urls are supported only for website seeds")
        return [], None
    remaining_slots = max_pages - len(curated_urls) - 1
    if remaining_slots < 0:
        raise ValueError("curated_frontier_urls must leave room for the seed source_uri")
    discovered_urls: Sequence[str] = ()
    crawl_evidence: Mapping[str, Any] | None = None
    if sitemap_discovery and sitemap_frontier_discoverer is not None:
        discovery_result = sitemap_frontier_discoverer(
            source_uri,
            policy,
            max_pages - 1 if remaining_slots > 0 else 0,
        )
        if isinstance(discovery_result, Mapping):
            discovered_urls = discovery_result.get("urls", ())
            raw_crawl_evidence = discovery_result.get("evidence")
            if raw_crawl_evidence is not None and not isinstance(raw_crawl_evidence, Mapping):
                raise ValueError("crawler evidence must be an object")
            crawl_evidence = raw_crawl_evidence
        else:
            discovered_urls = discovery_result
        if not all(isinstance(value, str) for value in discovered_urls):
            raise ValueError("sitemap frontier discoverer must return strings")
        discovered_urls = tuple(discovered_urls)[: max_pages - 1]
    candidate_urls = [*curated_urls, *discovered_urls]
    allowed_domain_set = set(allowed_domains)
    source_host = urlparse(source_uri).hostname
    normalized: list[str] = []
    seen = {_canonical_public_docs_url(source_uri)}
    for value in candidate_urls:
        url = value.strip()
        if not url:
            raise ValueError("curated and discovered frontier URLs must be non-empty")
        if _contains_raw_secret(url):
            raise ValueError(
                "curated and discovered frontier URLs must not contain raw secret material"
            )
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("curated and discovered frontier URLs must use https")
        if parsed.hostname not in allowed_domain_set:
            raise ValueError("curated and discovered frontier URLs host must be in allowed_domains")
        if source_host and parsed.hostname != source_host:
            raise ValueError("curated and discovered frontier URLs host must match source_uri host")
        if _public_docs_url_matches_deny_patterns(url, deny_patterns):
            raise ValueError("curated and discovered frontier URLs must not match deny_patterns")
        canonical_url = _canonical_public_docs_url(url)
        if canonical_url in seen:
            continue
        seen.add(canonical_url)
        normalized.append(url)
    return normalized[: max_pages - 1], crawl_evidence


def _curated_frontier_urls(policy: Mapping[str, Any]) -> list[str]:
    value = policy.get("curated_frontier_urls")
    if not isinstance(value, list) or not all(
        isinstance(url, str) and url.strip() for url in value
    ):
        raise ValueError("curated_frontier_urls must be a list of non-empty strings")
    return [url.strip() for url in value]


def _public_docs_url_matches_deny_patterns(
    url: str,
    deny_patterns: Sequence[str],
) -> bool:
    parsed = urlparse(url)
    path_and_query = parsed.path
    if parsed.query:
        path_and_query = f"{path_and_query}?{parsed.query}"
    return any(
        pattern and (pattern in path_and_query or pattern in url) for pattern in deny_patterns
    )


def _canonical_public_docs_url(value: str) -> str:
    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    port = f":{parsed.port}" if parsed.port else ""
    path = parsed.path or "/"
    if path == "":
        path = "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{scheme}://{hostname}{port}{path}{query}"


def discover_public_docs_crawler_frontier(
    source_uri: str,
    crawl_policy: Mapping[str, Any],
    max_urls: int,
) -> Mapping[str, Any]:
    """Discover changed/new same-domain pages through the governed crawler."""

    if max_urls < 0:
        raise ValueError("max_urls must be non-negative")
    previous_state = crawl_policy.get("previous_state", {})
    if not isinstance(previous_state, Mapping):
        raise ValueError("crawl_policy.previous_state must be an object")
    evidence = crawl_public_docs(
        seed_uri=source_uri,
        crawl_policy=crawl_policy,
        previous_state=previous_state,
        fetcher=_fetch_public_docs_crawler_response,
    )
    if evidence.get("status") != "completed":
        return {"evidence": evidence, "urls": []}
    state = evidence.get("state", {})
    if not isinstance(state, Mapping):
        raise ValueError("crawler state must be an object")
    canonical_seed_uri = _canonical_public_docs_url(source_uri)
    urls = [
        url
        for url, page_state in sorted(state.items())
        if (
            isinstance(url, str)
            and isinstance(page_state, Mapping)
            and page_state.get("status") == "active"
            and _canonical_public_docs_url(url) != canonical_seed_uri
        )
    ][:max_urls]
    return {"evidence": evidence, "urls": urls}


def _fetch_public_docs_crawler_response(
    url: str,
    headers: Mapping[str, str],
) -> CrawlResponse:
    request = Request(
        url,
        headers={"Accept": "text/html,application/xml,text/plain,*/*", **dict(headers)},
    )
    for attempt in range(1, _PUBLIC_DOCS_CRAWLER_FETCH_ATTEMPTS + 1):
        try:
            with _open_public_docs_crawler_request(
                request,
                timeout=_PUBLIC_DOCS_SITEMAP_FETCH_TIMEOUT_SECONDS,
            ) as response:
                return CrawlResponse(
                    status_code=int(response.status),
                    headers={str(key): str(value) for key, value in response.headers.items()},
                    body=response.read(1_000_001),
                )
        except HTTPError as exc:
            if (
                int(exc.code) in _PUBLIC_DOCS_CRAWLER_RETRY_STATUSES
                and attempt < _PUBLIC_DOCS_CRAWLER_FETCH_ATTEMPTS
            ):
                exc.close()
                sleep(0.5 * attempt)
                continue
            try:
                body = exc.read(1_000_001)
            except (OSError, TimeoutError):
                body = b""
            return CrawlResponse(
                status_code=int(exc.code),
                headers={str(key): str(value) for key, value in exc.headers.items()},
                body=body,
            )
        except (URLError, OSError, TimeoutError):
            if attempt < _PUBLIC_DOCS_CRAWLER_FETCH_ATTEMPTS:
                sleep(0.5 * attempt)
                continue
            return CrawlResponse(status_code=599, headers={}, body=b"")
    raise AssertionError("public docs crawler retry loop exhausted without a response")


def _open_public_docs_crawler_request(
    request: Request,
    *,
    timeout: int,
) -> Any:
    proxy_url = _public_docs_source_proxy_url()
    if proxy_url is None:
        return urlopen(request, timeout=timeout)
    opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
    return opener.open(request, timeout=timeout)


def _public_docs_source_proxy_url() -> str | None:
    raw_value = os.environ.get(_PUBLIC_DOCS_SOURCE_PROXY_URL_ENV, "").strip()
    if not raw_value:
        return None
    parsed = urlparse(raw_value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"{_PUBLIC_DOCS_SOURCE_PROXY_URL_ENV} must be an HTTP(S) proxy URL")
    if parsed.username or parsed.password:
        raise ValueError(f"{_PUBLIC_DOCS_SOURCE_PROXY_URL_ENV} must not contain proxy credentials")
    return raw_value


def _post_json(
    url: str,
    payload: Mapping[str, Any],
    *,
    attempts: int = 1,
    retry_statuses: Sequence[int] = (),
) -> dict[str, Any]:
    if attempts < 1:
        raise ValueError("POST attempts must be positive")
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "AdapstorySERPDocsRefresh/2026.07",
        },
        method="POST",
    )
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=_PUBLIC_DOCS_SITEMAP_FETCH_TIMEOUT_SECONDS) as response:
                response_payload = response.read(2_000_001)
            break
        except HTTPError as exc:
            if attempt == attempts or exc.code not in retry_statuses:
                raise
            sleep(0.5 * attempt)
        except (URLError, TimeoutError, OSError):
            if attempt == attempts:
                raise
            sleep(0.5 * attempt)
    if len(response_payload) > 2_000_000:
        raise ValueError("JSON response payload is too large")
    decoded = json.loads(response_payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("JSON response payload must be an object")
    return decoded


def _public_docs_refresh_policy(seed: Mapping[str, Any]) -> dict[str, Any]:
    policy = _required_mapping(seed, "refresh_policy")
    cadence = _required_str(policy, "cadence")
    if cadence not in {"daily", "nightly"}:
        raise ValueError("refresh_policy cadence must be daily or nightly")
    return {
        "cadence": cadence,
        "max_age_hours": _required_positive_int(policy, "max_age_hours"),
    }


def _public_docs_freshness_state(seed: Mapping[str, Any]) -> dict[str, Any]:
    freshness = seed.get("freshness_state")
    if freshness is None:
        return {"status": "never_indexed"}
    if not isinstance(freshness, Mapping):
        raise ValueError("freshness_state must be an object")
    _reject_raw_secrets(freshness)
    status = _required_str(freshness, "status")
    if status not in _PUBLIC_DOCS_FRESHNESS_STATUSES:
        raise ValueError("freshness_state status is unsupported")
    result: dict[str, Any] = {"status": status}
    last_success_at = freshness.get("last_success_at")
    if last_success_at is not None:
        result["last_success_at"] = _normalized_datetime_string(
            last_success_at,
            "freshness_state.last_success_at",
        )
    last_attempt_at = freshness.get("last_attempt_at")
    if last_attempt_at is not None:
        result["last_attempt_at"] = _normalized_datetime_string(
            last_attempt_at,
            "freshness_state.last_attempt_at",
        )
    for field_name in ("last_pipeline_evidence_sha256", "last_source_uri_hash"):
        value = freshness.get(field_name)
        if value is None:
            continue
        if not isinstance(value, str):
            raise ValueError(f"freshness_state {field_name} must be a string")
        normalized = value.removeprefix("sha256:")
        if not re.fullmatch(r"[a-f0-9]{64}", normalized):
            raise ValueError(f"freshness_state {field_name} must be sha256 hex")
        result[field_name] = f"sha256:{normalized}"
    page_state = freshness.get("page_state")
    if page_state is not None:
        if not isinstance(page_state, Mapping):
            raise ValueError("freshness_state.page_state must be an object")
        result["page_state"] = _validated_public_docs_page_state(page_state)
    return result


def _validated_public_docs_page_state(page_state: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for raw_url, raw_state in page_state.items():
        if (
            not isinstance(raw_url, str)
            or not raw_url.strip()
            or not isinstance(raw_state, Mapping)
        ):
            raise ValueError("freshness_state.page_state entries must map URLs to objects")
        parsed = urlparse(raw_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.fragment:
            raise ValueError("freshness_state.page_state URLs must be canonical https URLs")
        url = _canonical_public_docs_url(raw_url)
        if url != raw_url:
            raise ValueError("freshness_state.page_state URLs must be canonical")
        if url in normalized:
            raise ValueError("freshness_state.page_state contains duplicate canonical URL")
        _reject_raw_secrets(raw_state)
        status = _required_str(raw_state, "status")
        if status not in {"active", "tombstoned"}:
            raise ValueError("freshness_state.page_state status is unsupported")
        content_hash = raw_state.get("content_hash")
        if content_hash is not None and (
            not isinstance(content_hash, str) or not re.fullmatch(r"[a-f0-9]{64}", content_hash)
        ):
            raise ValueError("freshness_state.page_state content_hash must be sha256 hex")
        if status == "active" and content_hash is None:
            raise ValueError("active freshness_state page requires content_hash")
        state: dict[str, Any] = {"content_hash": content_hash, "status": status}
        for field_name in ("etag", "last_modified"):
            value = raw_state.get(field_name)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"freshness_state.page_state {field_name} must be a string")
            state[field_name] = value
        http_status = raw_state.get("http_status")
        if http_status is not None and (
            isinstance(http_status, bool)
            or not isinstance(http_status, int)
            or not 100 <= http_status <= 599
        ):
            raise ValueError("freshness_state.page_state http_status must be an HTTP status")
        state["http_status"] = http_status
        normalized[url] = state
    return normalized


def _public_docs_due_seed_selection(
    seeds: Sequence[Mapping[str, Any]],
    generated_at: str,
    *,
    force_full_refresh: bool = False,
    refresh_reason: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    generated_at_dt = _datetime_value(generated_at, "generated_at")
    due_seeds: list[dict[str, Any]] = []
    skipped_seed_refreshes: list[dict[str, Any]] = []
    for seed in seeds:
        decision = _public_docs_seed_refresh_decision(
            seed,
            generated_at_dt,
            force_full_refresh=force_full_refresh,
            refresh_reason=refresh_reason,
        )
        if decision["status"] == "due":
            selected_seed = dict(seed)
            selected_seed["refresh_selection"] = decision
            due_seeds.append(selected_seed)
        else:
            skipped_seed_refreshes.append(
                {
                    "freshness_state": dict(_required_mapping(seed, "freshness_state")),
                    "reason": _required_str(decision, "reason"),
                    "seed_id": _required_str(seed, "seed_id"),
                    "source_id": _required_str(seed, "source_id"),
                    "source_type": _required_str(seed, "source_type"),
                    "source_uri_hash": _public_docs_source_uri_hash(seed),
                    "status": "skipped",
                }
            )
    return due_seeds, skipped_seed_refreshes


def _public_docs_seed_refresh_decision(
    seed: Mapping[str, Any],
    generated_at: datetime,
    *,
    force_full_refresh: bool = False,
    refresh_reason: str | None = None,
) -> dict[str, str]:
    freshness_state = _required_mapping(seed, "freshness_state")
    last_success_at = freshness_state.get("last_success_at")
    refresh_policy = _required_mapping(seed, "refresh_policy")
    max_age_hours = _required_positive_int(refresh_policy, "max_age_hours")
    base = {
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "max_age_hours": str(max_age_hours),
    }
    if force_full_refresh:
        if refresh_reason is None:
            raise ValueError("force_full refresh requires refresh_reason")
        return {
            **base,
            "reason": "forced_revalidation",
            "refresh_reason": refresh_reason,
            "status": "due",
        }
    if not isinstance(last_success_at, str) or not last_success_at.strip():
        return {**base, "reason": "never_indexed", "status": "due"}
    last_success_dt = _datetime_value(last_success_at, "freshness_state.last_success_at")
    if last_success_dt > generated_at:
        raise ValueError("freshness_state.last_success_at must not be after generated_at")
    age_seconds = (generated_at - last_success_dt).total_seconds()
    if age_seconds >= max_age_hours * 3600:
        return {
            **base,
            "last_success_at": last_success_at,
            "reason": "max_age_exceeded",
            "status": "due",
        }
    return {
        **base,
        "last_success_at": last_success_at,
        "reason": "within_max_age",
        "status": "skipped",
    }


def _public_docs_refresh_mode(payload: Mapping[str, Any]) -> tuple[str, str | None]:
    value = payload.get("refresh_mode", "scheduled")
    if not isinstance(value, str) or value not in {"scheduled", "force_full"}:
        raise ValueError("refresh_mode must be scheduled or force_full")
    raw_reason = payload.get("refresh_reason")
    if value == "scheduled":
        if raw_reason is not None:
            raise ValueError("scheduled refresh must not include refresh_reason")
        return value, None
    if not isinstance(raw_reason, str) or not raw_reason.strip():
        raise ValueError("force_full refresh requires refresh_reason")
    if len(raw_reason.strip()) > 512:
        raise ValueError("refresh_reason must be at most 512 characters")
    return value, raw_reason.strip()


def _public_docs_source_uri_hash(seed: Mapping[str, Any]) -> str:
    return f"sha256:{sha256(_required_str(seed, 'source_uri').encode('utf-8')).hexdigest()}"


def _public_docs_license(seed: Mapping[str, Any]) -> dict[str, Any]:
    license_contract = _required_mapping(seed, "license")
    distribution_rule = _required_str(license_contract, "distribution_rule")
    if distribution_rule not in _PUBLIC_DOCS_DISTRIBUTION_RULES:
        raise ValueError("license distribution_rule is unsupported")
    return {
        "distribution_rule": distribution_rule,
        "obligation_state": _required_str(license_contract, "obligation_state"),
    }


def _public_docs_inventory_evidence(seed: Mapping[str, Any]) -> dict[str, Any]:
    evidence = _required_mapping(seed, "inventory_evidence")
    stack_inventory_path = _required_str(evidence, "stack_inventory_path")
    if stack_inventory_path != "tmp/stack-inventory-2026-07-02.md":
        raise ValueError("inventory_evidence must reference tmp stack inventory")
    evidence_sha256 = _required_str(evidence, "evidence_sha256")
    if not re.fullmatch(r"[a-f0-9]{64}", evidence_sha256):
        raise ValueError("inventory_evidence evidence_sha256 must be sha256 hex")
    return {
        "component": _required_str(evidence, "component"),
        "evidence_sha256": evidence_sha256,
        "stack_inventory_path": stack_inventory_path,
        "version": _required_str(evidence, "version"),
    }


def _require_unique_public_docs_seed_values(seeds: Sequence[Mapping[str, Any]]) -> None:
    for field_name in ("seed_id", "source_id", "source_uri"):
        values = [_required_str(seed, field_name) for seed in seeds]
        if len(values) != len(set(values)):
            raise ValueError(f"{field_name} values must be unique")


def _source_type_counts(seeds: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for seed in seeds:
        source_type = _required_str(seed, "source_type")
        counts[source_type] = counts.get(source_type, 0) + 1
    return dict(sorted(counts.items()))


def _public_docs_index_mode(payload: Mapping[str, Any]) -> str:
    value = payload.get("index_mode", os.environ.get(_PUBLIC_DOCS_INDEX_MODE_ENV, "live"))
    if not isinstance(value, str) or not value.strip():
        raise ValueError("index_mode is required")
    if value not in _PUBLIC_DOCS_INDEX_MODES:
        raise ValueError("index_mode is unsupported")
    return value


def _public_docs_embedding_mode(payload: Mapping[str, Any], index_mode: str) -> str:
    value = payload.get("embedding_mode", os.environ.get(_PUBLIC_DOCS_EMBEDDING_MODE_ENV))
    if value is None:
        value = "live-gateway" if index_mode == "live" else "deterministic-dev"
    if not isinstance(value, str) or not value.strip():
        raise ValueError("embedding_mode is required")
    if value not in _PUBLIC_DOCS_EMBEDDING_MODES:
        raise ValueError("embedding_mode is unsupported")
    if index_mode == "live" and value != "live-gateway":
        raise ValueError("live index mode requires live-gateway embedding mode")
    return value


def _public_docs_store_name(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    env_name: str,
    default: str,
) -> str:
    value = payload.get(field_name, os.environ.get(env_name, default))
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    if any(character.isspace() for character in value) or "/" in value or "\\" in value:
        raise ValueError(f"{field_name} must be a plain store name")
    _reject_raw_secrets({field_name: value})
    return value


def _optional_bc21_base_url(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("bc21_base_url", os.environ.get(_BC21_BASE_URL_ENV))
    if value is None:
        return None
    return _required_bc21_base_url({"bc21_base_url": value})


def _public_docs_search_serve_base_url(payload: Mapping[str, Any]) -> str:
    value = payload.get(
        "search_serve_base_url",
        os.environ.get(
            _PUBLIC_DOCS_SEARCH_SERVE_BASE_URL_ENV,
            _PUBLIC_DOCS_SEARCH_SERVE_DEFAULT_BASE_URL,
        ),
    )
    return _required_internal_or_https_base_url(
        {"search_serve_base_url": value},
        "search_serve_base_url",
    ).rstrip("/")


def _public_docs_search_serve_actor_id(payload: Mapping[str, Any]) -> str:
    value = payload.get(
        "search_serve_actor_id",
        os.environ.get(
            _PUBLIC_DOCS_SEARCH_SERVE_ACTOR_ID_ENV,
            _PUBLIC_DOCS_DEFAULT_SEARCH_SERVE_ACTOR_ID,
        ),
    )
    return str(_required_uuid({"search_serve_actor_id": value}, "search_serve_actor_id"))


def _is_plan_payload(value: Mapping[str, Any] | str) -> bool:
    payload = _json_object(value, "plan_json")
    return payload.get("dag_id") == "serp_benchmark_improvement_wave"


def _artifact_result(
    artifact_path: str,
    *,
    artifact_type: str,
    operation_id: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    payload_json = _canonical_json(payload)
    return {
        "artifactPath": artifact_path,
        "artifactSha256": sha256(payload_json.encode("utf-8")).hexdigest(),
        "artifactType": artifact_type,
        "contractVersion": _AIRFLOW_ARTIFACT_CONTRACT_VERSION,
        "operationId": operation_id,
        "payload": dict(payload),
        "status": "written",
    }


def _artifact_payload(
    artifact: Mapping[str, Any] | str,
    expected_type: str,
) -> Mapping[str, Any]:
    if isinstance(artifact, str):
        artifact = _json_object(artifact, "artifact")
    payload = _payload(artifact)
    if _required_str(payload, "artifactType") != expected_type:
        raise ValueError("artifact type does not match expected input")
    if _required_str(payload, "contractVersion") != _AIRFLOW_ARTIFACT_CONTRACT_VERSION:
        raise ValueError("artifact contract version does not match expected input")
    nested_payload = payload.get("payload")
    if not isinstance(nested_payload, Mapping):
        raise ValueError("artifact payload is required")
    _reject_raw_secrets(nested_payload)
    payload_json = _canonical_json(nested_payload)
    actual_sha256 = sha256(payload_json.encode("utf-8")).hexdigest()
    if _required_str(payload, "artifactSha256") != actual_sha256:
        raise ValueError("artifact payload sha256 does not match artifactSha256")
    return nested_payload


def _write_json_artifact(artifact_path: str, payload: Mapping[str, Any]) -> None:
    _write_artifact_text(artifact_path, _canonical_json(payload))


def _tasks(task_ids: Sequence[str]) -> list[dict[str, int | str]]:
    return [{"order": index + 1, "task_id": task_id} for index, task_id in enumerate(task_ids)]


def _gateway_cli_spec(
    plan_json: str,
    *,
    dag_id: str,
    task_id: str,
    command: str,
    input_path_keys: Sequence[str],
    output_path_key: str,
    option_names: Sequence[str],
    extra_options: Sequence[str] = (),
) -> dict[str, Any]:
    if len(extra_options) % 2 != 0:
        raise ValueError("gateway cli spec extra option mapping is invalid")
    if len(input_path_keys) != len(option_names):
        raise ValueError("gateway cli spec option mapping is invalid")
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != dag_id:
        raise ValueError("plan dag_id does not match gateway cli spec")
    artifact_paths = _required_artifact_paths(
        plan,
        (*input_path_keys, output_path_key),
    )
    argv = [
        GATEWAY_CLI_PYTHON,
        "-m",
        GATEWAY_CLI_MODULE,
        command,
    ]
    input_paths: list[str] = []
    for option_name, path_key in zip(option_names, input_path_keys, strict=True):
        path = artifact_paths[path_key]
        argv.extend([option_name, path])
        input_paths.append(path)
    argv.extend(extra_options)
    stdout_path = artifact_paths[output_path_key]
    return {
        "actor_id": _required_str(plan, "actor_id"),
        "argv": argv,
        "contract_version": _GATEWAY_CLI_CONTRACT_VERSION,
        "dag_id": dag_id,
        "input_paths": input_paths,
        "operation_id": _required_str(plan, "operation_id"),
        "plan_sha256": sha256(_canonical_json(plan).encode("utf-8")).hexdigest(),
        "status": "ready_for_gateway_cli_runner",
        "stdout_path": stdout_path,
        "task_id": task_id,
        "tenant_id": _required_str(plan, "tenant_id"),
    }


def _operation_id(prefix: str, *parts: object) -> str:
    material = "|".join(str(part) for part in parts)
    return f"{prefix}-{sha256(material.encode('utf-8')).hexdigest()[:32]}"


def _payload(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("dag run config must be an object")
    return value


def _json_object(value: Mapping[str, Any] | str, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a JSON object or mapping")
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if not isinstance(loaded, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    return loaded


def _read_json_file(path: str, field_name: str) -> Mapping[str, Any]:
    try:
        raw = _read_artifact_text(path, field_name)
    except Exception as exc:
        raise ValueError(f"{field_name} file is not readable: {path}") from exc
    return _json_object(raw, field_name)


def _read_optional_json_file(path: str, field_name: str) -> Mapping[str, Any] | None:
    try:
        raw = _read_artifact_text(path, field_name)
    except FileNotFoundError:
        return None
    except Exception as exc:
        if _is_s3_missing_object(exc):
            return None
        raise ValueError(f"{field_name} file is not readable: {path}") from exc
    return _json_object(raw, field_name)


def _is_s3_missing_object(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return False
    error = response.get("Error")
    if not isinstance(error, Mapping):
        return False
    code = error.get("Code")
    return code in {"404", "NoSuchKey"}


def _assert_plan_matches_suite_plan(
    airflow_plan: Mapping[str, Any],
    suite_plan: Mapping[str, Any],
) -> None:
    _assert_equal("tenant_id", _required_str(airflow_plan, "tenant_id"), suite_plan)
    _assert_equal(
        "registry_resource_type",
        _required_str(airflow_plan, "registry_resource_type"),
        suite_plan,
    )
    _assert_equal(
        "registry_resource_id",
        _required_str(airflow_plan, "registry_resource_id"),
        suite_plan,
    )
    _assert_equal(
        "generated_at",
        _required_str(airflow_plan, "generated_at"),
        suite_plan,
    )
    _assert_sequence_equal(
        "pack_version_ids",
        _required_str_list(airflow_plan, "pack_version_ids"),
        suite_plan,
    )
    _assert_sequence_equal(
        "selected_suite_ids",
        _required_str_list(airflow_plan, "selected_suite_ids"),
        suite_plan,
    )


def _assert_plan_matches_report(
    airflow_plan: Mapping[str, Any],
    report: Mapping[str, Any],
) -> None:
    _assert_equal("tenant_id", _required_str(airflow_plan, "tenant_id"), report)
    _assert_equal(
        "registry_resource_type",
        _required_str(airflow_plan, "registry_resource_type"),
        report,
    )
    _assert_equal(
        "registry_resource_id",
        _required_str(airflow_plan, "registry_resource_id"),
        report,
    )
    _assert_sequence_equal(
        "pack_version_ids",
        _required_str_list(airflow_plan, "pack_version_ids"),
        report,
    )
    _assert_sequence_equal(
        "selected_suite_ids",
        _required_str_list(airflow_plan, "selected_suite_ids"),
        report,
    )


def _assert_equal(
    field_name: str,
    expected_value: str,
    payload: Mapping[str, Any],
) -> None:
    if _required_str(payload, field_name) != expected_value:
        raise ValueError(f"{field_name} must match airflow plan")


def _assert_sequence_equal(
    field_name: str,
    expected_value: Sequence[str],
    payload: Mapping[str, Any],
) -> None:
    if _required_str_list(payload, field_name) != list(expected_value):
        raise ValueError(f"{field_name} must match airflow plan")


def _canonical_json(value: Mapping[str, Any]) -> str:
    return rfc8785.dumps(value).decode("utf-8")


def _canonical_json_object_bytes(payload: bytes, field_name: str) -> dict[str, Any]:
    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_nonfinite_json_constant,
        )
        if not isinstance(decoded, dict):
            raise ValueError("root value is not an object")
        if _canonical_json(decoded).encode("utf-8") != payload:
            raise ValueError("input bytes are not canonical")
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a canonical RFC 8785 JSON object") from exc
    return cast(dict[str, Any], decoded)


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object member: {key}")
        result[key] = value
    return result


def _reject_nonfinite_json_constant(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON number: {value}")


def _required_object_list(payload: Mapping[str, Any], field_name: str) -> list[Mapping[str, Any]]:
    value = payload.get(field_name)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list")
    objects: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError(f"{field_name} entries must be objects")
        objects.append(item)
    return objects


def _required_object_list_allow_empty(
    payload: Mapping[str, Any], field_name: str
) -> list[Mapping[str, Any]]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    objects: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError(f"{field_name} entries must be objects")
        objects.append(item)
    return objects


def _required_str_list(payload: Mapping[str, Any], field_name: str) -> list[str]:
    value = payload.get(field_name)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list")
    strings: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"{field_name} entries must be strings")
        _require_non_empty(field_name, item)
        strings.append(item)
    return strings


def _required_uuid_list(payload: Mapping[str, Any], field_name: str) -> list[UUID]:
    values = tuple(_required_str_list(payload, field_name))
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must not contain duplicates")
    return [_uuid_value(field_name, value) for value in values]


def _required_uuid(payload: Mapping[str, Any], field_name: str) -> UUID:
    return _uuid_value(field_name, _required_str(payload, field_name))


def _required_sha256_prefixed(payload: Mapping[str, Any], field_name: str) -> str:
    value = _required_str(payload, field_name)
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise ValueError(f"{field_name} must be sha256:<64 lowercase hex>")
    return value


def _required_sha256_hex(payload: Mapping[str, Any], field_name: str) -> str:
    value = _required_str(payload, field_name)
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"{field_name} must be 64 lowercase hex characters")
    return value


def _required_existing_local_artifact_path(payload: Mapping[str, Any], field_name: str) -> str:
    value = _artifact_path(field_name, _required_str(payload, field_name))
    if value.startswith("s3://"):
        raise ValueError(f"{field_name} must be a local artifact path for pipeline CLI handoff")
    path = Path(value)
    if not path.exists() or not path.is_file():
        raise ValueError(f"{field_name} must exist")
    return str(path)


def _uuid_value(field_name: str, value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a UUID") from exc


def _required_resource_type(payload: Mapping[str, Any], field_name: str) -> str:
    value = _required_str(payload, field_name)
    if value not in _RESOURCE_TYPES:
        raise ValueError(f"{field_name} is unsupported")
    return value


def _required_bc21_base_url(payload: Mapping[str, Any]) -> str:
    return _required_internal_or_https_base_url(payload, "bc21_base_url")


def _required_internal_or_https_base_url(payload: Mapping[str, Any], field_name: str) -> str:
    value = _required_str(payload, field_name)
    if "://" not in value or "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be an absolute single-line URL")
    if _contains_raw_secret(value):
        raise ValueError(f"{field_name} must not contain raw secret material")
    parsed = urlparse(value)
    if parsed.scheme == "https" and parsed.hostname:
        return value
    if parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}:
        return value
    if parsed.scheme == "http" and _is_kubernetes_service_host(parsed.hostname):
        return value
    raise ValueError(f"{field_name} must use https, localhost http, or Kubernetes service http")


def _is_kubernetes_service_host(hostname: str | None) -> bool:
    return hostname is not None and (
        hostname.endswith(".svc") or hostname.endswith(".svc.cluster.local")
    )


def _required_datetime_string(payload: Mapping[str, Any], field_name: str) -> str:
    value = _required_str(payload, field_name)
    return _normalized_datetime_string(value, field_name)


def _normalized_datetime_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a datetime string")
    parsed = _datetime_value(value, field_name)
    return parsed.isoformat().replace("+00:00", "Z")


def _datetime_value(value: str, field_name: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone")
    return parsed


def _required_str(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    _require_non_empty(field_name, value)
    return value


def _required_number(payload: Mapping[str, Any], field_name: str) -> float:
    value = payload.get(field_name)
    if not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"{field_name} must be numeric")
    return float(value)


def _optional_unit_interval(
    payload: Mapping[str, Any],
    field_name: str,
    default_value: float,
) -> float:
    value = payload.get(field_name, default_value)
    if not isinstance(value, int | float) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"{field_name} must be numeric")
    result = float(value)
    if result < 0.0 or result > 1.0:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return result


def _required_str_list_allow_empty(payload: Mapping[str, Any], field_name: str) -> list[str]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field_name} entries must be non-empty strings")
        result.append(item)
    return result


def _required_number_from_string(payload: Mapping[str, Any], field_name: str) -> float:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise ValueError(f"{field_name} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be finite")
    return number


def _required_positive_int(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _required_true(payload: Mapping[str, Any], field_name: str) -> None:
    if payload.get(field_name) is not True:
        raise ValueError(f"{field_name} must be true")


def _required_mapping(payload: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _required_artifact_root_path(payload: Mapping[str, Any]) -> str:
    value = payload.get("artifact_root_path")
    if not isinstance(value, str):
        value = os.environ.get(_ARTIFACT_ROOT_ENV)
    if not isinstance(value, str):
        raise ValueError("artifact_root_path is required")
    return _artifact_path("artifact_root_path", value)


def _artifact_paths(
    artifact_root_path: str,
    operation_id: str,
    filenames: Sequence[tuple[str, str]],
) -> dict[str, str]:
    root = _artifact_path("artifact_root_path", artifact_root_path).rstrip("/")
    operation_path = f"{root}/{operation_id}"
    return {key: _artifact_path(key, f"{operation_path}/{filename}") for key, filename in filenames}


def _public_docs_crawl_state_path(
    payload: Mapping[str, Any],
    artifact_root_path: str,
) -> str:
    root = _artifact_path("artifact_root_path", artifact_root_path).rstrip("/")
    expected_path = _artifact_path(
        "public_docs_crawl_state_path",
        f"{root}/public-docs-crawl-state/state.json",
    )
    configured_path = payload.get("public_docs_crawl_state_path")
    if configured_path is None:
        return expected_path
    if not isinstance(configured_path, str):
        raise ValueError("public_docs_crawl_state_path must be a string")
    actual_path = _artifact_path("public_docs_crawl_state_path", configured_path)
    if actual_path != expected_path:
        raise ValueError("public_docs_crawl_state_path must use the canonical artifact location")
    return actual_path


def _validate_public_docs_crawl_state_identity(
    state: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> None:
    if _required_str(state, "artifact_type") != "public_docs_crawl_state":
        raise ValueError("public_docs_crawl_state artifact_type is unsupported")
    if _required_str(state, "contract_version") != _EVAL_CONTRACT_VERSION:
        raise ValueError("public_docs_crawl_state contract_version is unsupported")
    if _required_str(state, "status") != "active":
        raise ValueError("public_docs_crawl_state must be active")
    if _required_str(state, "tenant_id") != _required_str(payload, "tenant_id"):
        raise ValueError("public_docs_crawl_state tenant_id must match plan")
    if _required_str(state, "pack_id") != _required_str(payload, "pack_id"):
        raise ValueError("public_docs_crawl_state pack_id must match plan")
    _required_uuid(state, "active_pack_version_id")
    _required_mapping(state, "seeds")


def _optional_public_docs_active_pack_version_id(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("active_pack_version_id")
    if value is None:
        return None
    return str(_required_uuid(payload, "active_pack_version_id"))


def _optional_public_docs_crawl_state_recovery(
    payload: Mapping[str, Any],
    *,
    active_pack_version_id: str | None,
) -> dict[str, str] | None:
    value = payload.get("public_docs_crawl_state_recovery")
    if value is None:
        return None
    if active_pack_version_id is None:
        raise ValueError("public_docs_crawl_state_recovery requires active_pack_version_id")
    recovery = _required_mapping(payload, "public_docs_crawl_state_recovery")
    if _required_str(recovery, "method") != "bc21_active_pack_resolution":
        raise ValueError("public_docs_crawl_state_recovery method is unsupported")
    recovered_pack_version_id = str(_required_uuid(recovery, "active_pack_version_id"))
    if recovered_pack_version_id != active_pack_version_id:
        raise ValueError(
            "public_docs_crawl_state_recovery active_pack_version_id must match active pack"
        )
    return {
        "active_pack_version_id": recovered_pack_version_id,
        "activation_run_id": str(_required_uuid(recovery, "activation_run_id")),
        "method": "bc21_active_pack_resolution",
    }


def _optional_previous_active_pack_version_id(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("previous_active_pack_version_id")
    if value is None:
        return None
    return str(_required_uuid(payload, "previous_active_pack_version_id"))


def _required_artifact_paths(
    payload: Mapping[str, Any],
    required_keys: Sequence[str],
) -> dict[str, str]:
    value = payload.get("artifact_paths")
    if not isinstance(value, Mapping):
        raise ValueError("artifact_paths is required")
    return {key: _artifact_path(key, _required_str(value, key)) for key in required_keys}


def _artifact_path(field_name: str, value: str) -> str:
    _require_non_empty(field_name, value)
    return _artifact_ref(field_name, value).location


def _require_non_empty(field_name: str, value: str | None) -> None:
    if value is None or not value.strip():
        raise ValueError(f"{field_name} is required")


def _reject_raw_secrets(value: Any) -> None:
    if _contains_raw_secret(value):
        raise ValueError("dag run config must not contain raw secret material")


def _contains_raw_secret(value: Any) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if _contains_raw_secret_mapping_key(key):
                return True
            if _contains_raw_secret(nested):
                return True
        return False
    if isinstance(value, list | tuple | set):
        return any(_contains_raw_secret(item) for item in value)
    if isinstance(value, str) and any(
        pattern.match(value.strip()) for pattern in _SECRET_VALUE_PATTERNS
    ):
        return True
    return False


def _contains_raw_secret_mapping_key(key: Any) -> bool:
    """Treat URL-indexed crawler evidence as data while still rejecting secret URLs."""

    raw_key = str(key)
    parsed = urlparse(raw_key)
    if parsed.scheme in {"http", "https"} and parsed.hostname:
        if parsed.username or parsed.password:
            return True
        return any(
            _is_raw_secret_field_name(parameter)
            for parameter, _ in parse_qsl(parsed.query, keep_blank_values=True)
        )
    return _is_raw_secret_field_name(raw_key)


def _is_raw_secret_field_name(value: Any) -> bool:
    normalized_key = str(value).lower().replace("-", "_")
    return normalized_key in _RAW_SECRET_KEYS or any(
        normalized_key.endswith(f"_{secret_key}") for secret_key in _RAW_SECRET_KEYS
    )


def _artifact_ref(field_name: str, value: str) -> _ArtifactRef:
    _require_non_empty(field_name, value)
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be a single-line absolute path or s3:// URI")
    if _contains_raw_secret(value):
        raise ValueError(f"{field_name} must not contain raw secret material")
    parsed = urlparse(value)
    if parsed.scheme == "s3":
        if not parsed.netloc:
            raise ValueError(f"{field_name} must include an S3 bucket")
        if parsed.params or parsed.query or parsed.fragment:
            raise ValueError(f"{field_name} must not include URL parameters")
        key = parsed.path.lstrip("/")
        if not key:
            raise ValueError(f"{field_name} must include an S3 object key")
        if ".." in PurePosixPath(f"/{key}").parts:
            raise ValueError(f"{field_name} must not contain parent traversal")
        return _ArtifactRef(
            location=f"s3://{parsed.netloc}/{key}",
            kind="s3",
            bucket=parsed.netloc,
            key=key,
        )
    if "://" in value or not value.startswith("/"):
        raise ValueError(f"{field_name} must be an absolute path or s3:// URI")
    if ".." in PurePosixPath(value).parts:
        raise ValueError(f"{field_name} must not contain parent traversal")
    return _ArtifactRef(location=value, kind="file", local_path=value)


def _read_artifact_text(path: str, field_name: str) -> str:
    artifact = _artifact_ref(field_name, path)
    if artifact.kind == "file":
        return Path(artifact.local_path or "").read_text(encoding="utf-8")
    response = _s3_client(path).get_object(
        Bucket=_required_str_ref(artifact.bucket), Key=_required_str_ref(artifact.key)
    )
    body = response.get("Body")
    if not hasattr(body, "read"):
        raise ValueError(f"{field_name} S3 response body is unreadable")
    raw = body.read()
    if not isinstance(raw, bytes):
        raise ValueError(f"{field_name} S3 response body is unreadable")
    return raw.decode("utf-8")


def _write_artifact_text(path: str, raw: str) -> None:
    artifact = _artifact_ref("artifact_path", path)
    if artifact.kind == "file":
        local_path = Path(artifact.local_path or "")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(raw, encoding="utf-8")
        return
    _s3_client(path).put_object(
        Bucket=_required_str_ref(artifact.bucket),
        Key=_required_str_ref(artifact.key),
        Body=raw.encode("utf-8"),
        ContentType="application/json",
    )


def _artifact_parent_path(path: str) -> str:
    artifact = _artifact_ref("artifact_path", path)
    if artifact.kind == "s3":
        parent = str(PurePosixPath(_required_str_ref(artifact.key)).parent).strip("/")
        if not parent or parent == ".":
            raise ValueError("artifact_path parent must include an S3 object prefix")
        return f"s3://{_required_str_ref(artifact.bucket)}/{parent}"
    return str(Path(_required_str_ref(artifact.local_path)).parent)


def _replace_cli_option_value(
    argv: Sequence[str],
    option_name: str,
    replacement_value: str,
) -> list[str]:
    result = list(argv)
    try:
        option_index = result.index(option_name)
    except ValueError:
        return result
    value_index = option_index + 1
    if value_index >= len(result):
        raise ValueError(f"{option_name} requires a value")
    result[value_index] = replacement_value
    return result


def _materialize_gateway_cli_argv(
    argv: Sequence[str],
    input_paths: Sequence[str],
    *,
    temp_dir: str,
) -> list[str]:
    materialized: dict[str, str] = {}
    for input_path in input_paths:
        artifact = _artifact_ref("input_path", input_path)
        if artifact.kind == "file":
            local_path = Path(artifact.local_path or "")
            if not local_path.is_file():
                raise ValueError(f"gateway cli input path is not readable: {input_path}")
            materialized[input_path] = str(local_path)
            continue
        target_path = (
            Path(temp_dir)
            / sha256(input_path.encode("utf-8")).hexdigest()
            / Path(artifact.key or "artifact.json").name
        )
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(_read_artifact_text(input_path, "input_path"), encoding="utf-8")
        materialized[input_path] = str(target_path)
    return [materialized.get(value, value) for value in argv]


def _materialize_pipeline_cli_argv(
    argv: Sequence[str],
    input_paths: Sequence[str],
    *,
    stdout_path: str,
    temp_dir: str,
    preserve_evidence_output: bool = False,
) -> list[str]:
    materialized = _materialize_gateway_cli_argv(argv, input_paths, temp_dir=temp_dir)
    stdout_artifact = _artifact_ref("stdout_path", stdout_path)
    if stdout_artifact.kind == "s3":
        local_stdout_path = (
            Path(temp_dir) / "stdout" / Path(_required_str_ref(stdout_artifact.key)).name
        )
        local_stdout_path.parent.mkdir(parents=True, exist_ok=True)
        output_options = ["--activation-receipt-output"]
        if not preserve_evidence_output:
            output_options.append("--evidence-output")
        for output_option in output_options:
            if output_option in materialized:
                materialized = _replace_cli_option_value(
                    materialized,
                    output_option,
                    str(local_stdout_path),
                )
    if "--artifact-root" in materialized:
        artifact_root = materialized[materialized.index("--artifact-root") + 1]
        if _artifact_ref("artifact_root", artifact_root).kind == "s3":
            local_artifact_root = Path(temp_dir) / "pipeline-artifacts"
            local_artifact_root.mkdir(parents=True, exist_ok=True)
            materialized = _replace_cli_option_value(
                materialized,
                "--artifact-root",
                str(local_artifact_root),
            )
    return materialized


def _pipeline_owns_evidence_output(spec: Mapping[str, Any], argv: Sequence[str]) -> bool:
    owner = spec.get("evidence_output_owner")
    if owner is None:
        return False
    if owner != "pipeline":
        raise ValueError("evidence_output_owner must be pipeline when declared")
    evidence_output_path = _artifact_path(
        "evidence_output_path", _required_str(spec, "evidence_output_path")
    )
    if "--evidence-output" not in argv:
        raise ValueError("pipeline-owned evidence output requires --evidence-output")
    supplied_path = argv[argv.index("--evidence-output") + 1]
    if supplied_path != evidence_output_path:
        raise ValueError("pipeline-owned evidence output must match evidence_output_path")
    if evidence_output_path == _artifact_path("stdout_path", _required_str(spec, "stdout_path")):
        raise ValueError("pipeline-owned evidence output must not share stdout_path")
    return True


def _s3_client(*artifact_paths: str) -> Any:
    from dags.serp_evidence_workload_identity import operation_prefix_s3_client

    return operation_prefix_s3_client(artifact_uris=artifact_paths)


def _s3_read_client(*artifact_paths: str) -> Any:
    from dags.serp_evidence_workload_identity import operation_prefix_read_s3_client

    return operation_prefix_read_s3_client(artifact_uris=artifact_paths)


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value or not value.strip():
        raise ValueError(f"{name} is required for s3:// artifact paths")
    return value


def _required_positive_int_env(name: str) -> int:
    raw_value = _required_env(name)
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _required_str_ref(value: str | None) -> str:
    if value is None or not value.strip():
        raise ValueError("artifact reference is incomplete")
    return value
