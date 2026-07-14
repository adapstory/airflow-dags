from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import re
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache, partial
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from time import perf_counter, sleep
from typing import Any, NoReturn
from urllib.error import HTTPError, URLError
from urllib.parse import ParseResult, parse_qsl, unquote, urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen
from uuid import NAMESPACE_URL, UUID, uuid5

from dags.public_docs_crawler import CrawlResponse, crawl_public_docs
from dags.serp_public_docs_seed_catalog import (
    PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH,
    STACK_INVENTORY_SOURCE_PATH,
    p0_public_docs_sources,
)

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
SERP_NORMALIZED_GATE_FLOOR = 0.75
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
_BENCHMARK_SUITE_CONTRACT_VERSION = "2026.07.3"
_BENCHMARK_EVALUATION_CONTRACT_CODE = "d6-evidence-2026.07.3"
_METRIC_COMPATIBILITY_CONTRACT_VERSION = "serp-suite-metric-compatibility/v1"
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
_PUBLIC_DOCS_DEFAULT_ACTOR_ID = "airflow-serp-public-docs-refresh"
_PUBLIC_DOCS_SEARCH_SERVE_SMOKE_ACTOR_ID = "00000000-0000-4000-a000-000000000202"
_PUBLIC_DOCS_DEFAULT_ARTIFACT_ROOT = "/var/opt/adapstory/serp-public-docs-refresh"
_PUBLIC_DOCS_STACK_INVENTORY_PATH = STACK_INVENTORY_SOURCE_PATH
_PUBLIC_DOCS_SITEMAP_FETCH_TIMEOUT_SECONDS = 8
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
_ARTIFACT_ROOT_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_ROOT"
_PUBLIC_DOCS_SEARCH_SERVE_BASE_URL_ENV = "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL"
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
    "pack_version_ids": "ADAPSTORY_SERP_D6_PACK_VERSION_IDS",
    "registry_resource_id": "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_ID",
    "registry_resource_type": "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_TYPE",
    "reranker_profile_version": "ADAPSTORY_SERP_D6_RERANKER_PROFILE_VERSION",
    "retrieval_profile_version": "ADAPSTORY_SERP_D6_RETRIEVAL_PROFILE_VERSION",
    "tenant_id": "ADAPSTORY_SERP_D6_TENANT_ID",
}
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
_ARTIFACT_S3_ACCESS_KEY_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY"
_ARTIFACT_S3_SECRET_KEY_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY"
_ARTIFACT_S3_PATH_STYLE_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE"
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog=f"{GATEWAY_CLI_PYTHON} -m {GATEWAY_CLI_MODULE}",
        description="Self-contained SERP Airflow D6 eval runner and BC-21 bridge.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    nightly_report = subparsers.add_parser("nightly-report")
    nightly_report.add_argument("--airflow-plan", required=True)
    nightly_report.add_argument("--suite-plan", required=True)
    nightly_report.set_defaults(handler=_cli_nightly_report)

    benchmark_export = subparsers.add_parser("nightly-benchmark-export")
    benchmark_export.add_argument("--airflow-plan", required=True)
    benchmark_export.add_argument("--nightly-report", required=True)
    benchmark_export.set_defaults(handler=_cli_nightly_benchmark_export)

    registry_submissions = subparsers.add_parser("nightly-registry-submissions")
    registry_submissions.add_argument("--airflow-plan", required=True)
    registry_submissions.add_argument("--nightly-report", required=True)
    registry_submissions.set_defaults(handler=_cli_nightly_registry_submissions)

    submit_registry_submissions = subparsers.add_parser("submit-nightly-registry-submissions")
    submit_registry_submissions.add_argument("--airflow-plan", required=True)
    submit_registry_submissions.add_argument("--nightly-registry-submissions", required=True)
    submit_registry_submissions.add_argument("--bc21-base-url", required=True)
    submit_registry_submissions.set_defaults(handler=_cli_submit_registry_submissions)

    args = parser.parse_args(argv)
    try:
        output = args.handler(args)
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(_canonical_json(output))
    return 0


def _cli_nightly_report(args: argparse.Namespace) -> Mapping[str, Any]:
    airflow_plan = _read_json_file(args.airflow_plan, "airflow_plan")
    suite_plan = _read_json_file(args.suite_plan, "suite_plan")
    _assert_plan_matches_suite_plan(airflow_plan, suite_plan)
    return _nightly_report_from_suite_plan_payload(suite_plan)


def _cli_nightly_benchmark_export(args: argparse.Namespace) -> Mapping[str, Any]:
    airflow_plan = _read_json_file(args.airflow_plan, "airflow_plan")
    report = _read_json_file(args.nightly_report, "nightly_report")
    _assert_plan_matches_report(airflow_plan, report)
    payload = _benchmark_export_payload(report)
    _validate_benchmark_export_payload(payload)
    return payload


def _cli_nightly_registry_submissions(args: argparse.Namespace) -> Mapping[str, Any]:
    airflow_plan = _read_json_file(args.airflow_plan, "airflow_plan")
    report = _read_json_file(args.nightly_report, "nightly_report")
    _assert_plan_matches_report(airflow_plan, report)
    return _live_registry_submissions_payload(
        report,
        actor_id=_required_str(airflow_plan, "actor_id"),
    )


def _cli_submit_registry_submissions(args: argparse.Namespace) -> Mapping[str, Any]:
    airflow_plan = _read_json_file(args.airflow_plan, "airflow_plan")
    submissions = _read_json_file(
        args.nightly_registry_submissions,
        "nightly_registry_submissions",
    )
    _assert_equal("tenant_id", _required_str(airflow_plan, "tenant_id"), submissions)
    return _submit_live_registry_submissions(
        submissions,
        bc21_base_url=args.bc21_base_url,
    )


def _required_nightly_benchmark_suite_inputs(
    payload: Mapping[str, Any], *, selected_suite_ids: Sequence[str]
) -> list[dict[str, Any]]:
    """Accept only adapter-produced suites with immutable execution provenance.

    D6 is a production benchmark gate. It must consume actual adapter output,
    never manufacture ranked chunks, aggregate observations, or reference
    scores. The immutable dataset and run-evidence locations make every score
    independently replayable from MinIO/BC-21 evidence.
    """

    suites = _required_object_list(payload, "benchmark_suite_inputs")
    observed_suite_ids = tuple(_required_str(suite, "suite_id") for suite in suites)
    if observed_suite_ids != tuple(selected_suite_ids):
        raise ValueError("benchmark_suite_inputs must match selected_suite_ids in canonical order")
    materialized: list[dict[str, Any]] = []
    metric_compatibility: dict[str, Any] | None = None
    for suite in suites:
        _reject_raw_secrets(suite)
        if _required_str(suite, "suite_contract_version") != _BENCHMARK_SUITE_CONTRACT_VERSION:
            raise ValueError("benchmark_suite_inputs has unsupported suite_contract_version")
        if not _required_object_list(suite, "cases"):
            raise ValueError("benchmark_suite_inputs suite cases must not be empty")
        if not _required_object_list(suite, "references"):
            raise ValueError("benchmark_suite_inputs suite references must not be empty")
        suite_metric_compatibility = _required_metric_compatibility(
            _required_mapping(suite, "metric_compatibility"),
            selected_suite_ids=selected_suite_ids,
            contract_version_field="contract_version",
            matrix_uri_field="matrix_uri",
            matrix_sha256_field="matrix_sha256",
            matrix_version_id_field="matrix_version_id",
            suite_id_field="suite_id",
            metric_families_field="metric_families",
        )
        if metric_compatibility is None:
            metric_compatibility = suite_metric_compatibility
        elif _canonical_json(metric_compatibility) != _canonical_json(suite_metric_compatibility):
            raise ValueError("benchmark_suite_inputs must use the same metric_compatibility matrix")
        _validate_nightly_suite_metric_records(
            suite,
            required_metric_families=_metric_families_for_suite(
                suite_metric_compatibility,
                _required_str(suite, "suite_id"),
            ),
        )
        _validate_nightly_benchmark_suite_provenance(
            _required_mapping(suite, "metadata"),
        )
        materialized.append(dict(suite))
    return materialized


def _validate_nightly_benchmark_suite_provenance(metadata: Mapping[str, Any]) -> None:
    for field_name in (
        "adapter_id",
        "adapter_version",
        "adapter_source_uri",
        "adapter_source_revision",
        "adapter_image_digest",
        "dataset_license_id",
        "dataset_distribution_rule",
        "dataset_rights_status",
        "dataset_manifest_uri",
        "dataset_manifest_sha256",
        "dataset_manifest_version_id",
        "execution_evidence_uri",
        "execution_evidence_sha256",
        "execution_evidence_version_id",
        "reference_source_uri",
    ):
        _required_str(metadata, field_name)
    for field_name in ("adapter_source_uri", "reference_source_uri"):
        parsed = urlparse(_required_str(metadata, field_name))
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError(f"benchmark suite {field_name} must be an https URL")
    if not re.fullmatch(r"[0-9a-f]{40}", _required_str(metadata, "adapter_source_revision")):
        raise ValueError("benchmark suite adapter_source_revision must be a 40-character SHA")
    for field_name in (
        "adapter_image_digest",
        "dataset_manifest_sha256",
        "execution_evidence_sha256",
    ):
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", _required_str(metadata, field_name)):
            raise ValueError(f"benchmark suite {field_name} must be a sha256 digest")
    for field_name in ("dataset_manifest_uri", "execution_evidence_uri"):
        if _artifact_ref(field_name, _required_str(metadata, field_name)).kind != "s3":
            raise ValueError(f"benchmark suite {field_name} must be an s3:// immutable artifact")
    _validate_dataset_rights(
        license_id=_required_str(metadata, "dataset_license_id"),
        distribution_rule=_required_str(metadata, "dataset_distribution_rule"),
        rights_status=_required_str(metadata, "dataset_rights_status"),
        error_prefix="benchmark suite",
    )


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


def _validate_nightly_suite_metric_records(
    suite: Mapping[str, Any],
    *,
    required_metric_families: Sequence[str],
) -> None:
    references = _required_object_list(suite, "references")
    reference_keys: set[tuple[str, str]] = set()
    reference_families: set[str] = set()
    for reference in references:
        metric_family = _required_str(reference, "metric_family")
        metric = _required_str(reference, "metric")
        key = (metric_family, metric)
        if key in reference_keys:
            raise ValueError(f"duplicate suite reference {metric_family}/{metric}")
        reference_keys.add(key)
        reference_families.add(metric_family)
    if reference_families != set(required_metric_families):
        raise ValueError(
            "references must exactly match metric_compatibility required metric families"
        )
    observations = _required_object_list_allow_empty(suite, "metric_observations")
    observation_keys: set[tuple[str, str]] = set()
    for observation in observations:
        metric_family = _required_str(observation, "metric_family")
        metric = _required_str(observation, "metric")
        key = (metric_family, metric)
        if key in observation_keys:
            raise ValueError(f"duplicate suite metric_observation {metric_family}/{metric}")
        observation_keys.add(key)
        _required_number(observation, "score")
    required_observation_keys = {key for key in reference_keys if key[0] != "retrieval"}
    if observation_keys != required_observation_keys:
        raise ValueError("metric_observations must exactly match non-retrieval metric references")


def default_nightly_regression_conf(
    *,
    generated_at: str,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Return the D6 schedule context from GitOps-owned runtime configuration.

    Suite selection is deliberately never configurable: a scheduled D6 run is
    the canonical mandatory-suite matrix or it does not run.  The remaining
    identity and retrieval envelope is supplied by the Airflow deployment,
    rather than a mutable ``DagRun.conf`` payload.
    """

    values = os.environ if environment is None else environment
    raw_pack_version_ids = _required_environment_value(
        values, _NIGHTLY_REGRESSION_RUNTIME_ENV["pack_version_ids"]
    )
    try:
        pack_version_ids = json.loads(raw_pack_version_ids)
    except json.JSONDecodeError as exc:
        raise ValueError("ADAPSTORY_SERP_D6_PACK_VERSION_IDS must be a JSON array") from exc
    if not isinstance(pack_version_ids, list) or not all(
        isinstance(value, str) and value.strip() for value in pack_version_ids
    ):
        raise ValueError("ADAPSTORY_SERP_D6_PACK_VERSION_IDS must be a non-empty JSON string array")
    return {
        "actor_id": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["actor_id"]
        ),
        "artifact_root_path": _required_environment_value(values, _ARTIFACT_ROOT_ENV),
        "bc21_base_url": _required_environment_value(values, _BC21_BASE_URL_ENV),
        "generated_at": _required_datetime_string({"generated_at": generated_at}, "generated_at"),
        "pack_version_ids": pack_version_ids,
        "registry_resource_id": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["registry_resource_id"]
        ),
        "registry_resource_type": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["registry_resource_type"]
        ),
        "reranker_profile_version": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["reranker_profile_version"]
        ),
        "retrieval_profile_version": _required_environment_value(
            values, _NIGHTLY_REGRESSION_RUNTIME_ENV["retrieval_profile_version"]
        ),
        "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
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


def build_nightly_regression_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    payload = _payload(conf)
    _reject_raw_secrets(payload)
    if "benchmark_suite_inputs" in payload:
        raise ValueError(
            "benchmark_suite_inputs must be produced by canonical live adapters, not dag_run.conf"
        )
    selected_suite_ids = tuple(_required_str_list(payload, "selected_suite_ids"))
    if selected_suite_ids != MANDATORY_SERP_BENCHMARK_SUITES:
        raise ValueError("selected_suite_ids must include every mandatory suite")
    tenant_id = _required_uuid(payload, "tenant_id")
    pack_version_ids = tuple(_required_uuid_list(payload, "pack_version_ids"))
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    artifact_root_path = _required_artifact_root_path(payload)
    operation_id = _operation_id(
        "serp-airflow-nightly-plan",
        tenant_id,
        generated_at,
        ",".join(str(value) for value in pack_version_ids),
        ",".join(selected_suite_ids),
        "serp-benchmark-catalog/v1",
    )
    plan_payload = {
        "actor_id": _required_str(payload, "actor_id"),
        "artifact_root_path": artifact_root_path,
        "artifact_paths": _artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("airflow_plan", "airflow-plan.json"),
                ("suite_plan", "suite-plan.json"),
                ("nightly_report", "nightly-report.json"),
                ("benchmark_gate_export", "benchmark-gate-export.json"),
                ("benchmark_catalog", "benchmark-catalog.json"),
                (
                    "benchmark_catalog_receipt",
                    "benchmark-catalog-materialization-receipt.json",
                ),
                (
                    "nightly_registry_submissions",
                    "nightly-registry-submissions.json",
                ),
                ("nightly_registry_receipts", "nightly-registry-receipts.json"),
            ),
        ),
        "bc21_base_url": _required_bc21_base_url(payload),
        "dag_id": "serp_nightly_regression_suite",
        "generated_at": generated_at,
        "normalized_gate_floor": SERP_NORMALIZED_GATE_FLOOR,
        "operation_id": operation_id,
        "pack_version_ids": [str(value) for value in pack_version_ids],
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "reranker_profile_version": _required_str(payload, "reranker_profile_version"),
        "retrieval_profile_version": _required_str(payload, "retrieval_profile_version"),
        "selected_suite_ids": list(selected_suite_ids),
        "tasks": _tasks(
            (
                "validate_nightly_regression_plan",
                "materialize_live_benchmark_catalog",
                "load_materialized_benchmark_catalog",
                "write_nightly_suite_plan",
                "run_mandatory_benchmark_suites",
                "build_c1_benchmark_gate_export",
                "build_bc21_benchmark_run_submissions",
                "submit_bc21_benchmark_run_submissions",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


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


def build_benchmark_improvement_wave_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    payload = _payload(conf)
    _reject_raw_secrets(payload)
    if "candidate_evaluation" in payload:
        raise ValueError(
            "candidate_evaluation is forbidden: D19 derives results from executor receipts"
        )
    selected_suite_ids = tuple(_required_str_list(payload, "selected_suite_ids"))
    if selected_suite_ids != MANDATORY_SERP_BENCHMARK_SUITES:
        raise ValueError("selected_suite_ids must include every mandatory suite")
    tenant_id = _required_uuid(payload, "tenant_id")
    generated_at = _required_datetime_string(payload, "generated_at")
    registry_resource_type = _required_resource_type(payload, "registry_resource_type")
    registry_resource_id = _required_uuid(payload, "registry_resource_id")
    improvement_spec_id = _required_str(payload, "improvement_spec_id")
    baseline_run_id = _required_str(payload, "baseline_run_id")
    candidate_id = _required_str(payload, "candidate_id")
    candidate_run_id = _required_str(payload, "candidate_run_id")
    max_benchmark_runs = _required_positive_int(payload, "max_benchmark_runs")
    rollback_policy_ref = _required_str(payload, "rollback_policy_ref")
    replay_context = _improvement_replay_context(
        payload,
        baseline_run_id,
        candidate_run_id,
    )
    model_governance = _improvement_model_governance(payload)
    artifact_root_path = _required_artifact_root_path(payload)
    if not artifact_root_path.startswith("s3://"):
        raise ValueError("benchmark improvement wave requires an s3:// artifact_root_path")
    operation_id = _operation_id(
        "serp-airflow-benchmark-improvement-wave",
        tenant_id,
        improvement_spec_id,
        baseline_run_id,
        candidate_id,
        generated_at,
        ",".join(selected_suite_ids),
    )
    plan_payload = {
        "actor_id": _required_str(payload, "actor_id"),
        "artifact_root_path": artifact_root_path,
        "artifact_paths": _artifact_paths(
            artifact_root_path,
            operation_id,
            (
                ("airflow_plan", "airflow-plan.json"),
                ("improvement_spec", "improvement-spec.json"),
                ("paired_eval_request", "paired-eval-request.json"),
                ("paired_eval_receipt", "paired-eval-receipt.json"),
            ),
        ),
        "baseline_run_id": baseline_run_id,
        "candidate_id": candidate_id,
        "candidate_run_id": candidate_run_id,
        "dag_id": "serp_benchmark_improvement_wave",
        "generated_at": generated_at,
        "improvement_spec_id": improvement_spec_id,
        "max_benchmark_runs": max_benchmark_runs,
        "model_governance": model_governance,
        "normalized_gate_floor": SERP_NORMALIZED_GATE_FLOOR,
        "operation_id": operation_id,
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
        "replay_context": replay_context,
        "rollback_policy_ref": rollback_policy_ref,
        "selected_suite_ids": list(selected_suite_ids),
        "tasks": _tasks(
            (
                "validate_benchmark_improvement_wave_plan",
                "write_improvement_spec",
                "write_paired_eval_request",
                "run_paired_benchmark_evaluation",
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


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
        seed_refresh_result_path,
        approval_idempotency_key,
        evidence_bundle_id,
        evidence_seal_hash,
        benchmark_gate_export_sha256,
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
        "public_docs_crawl_state_path": crawl_state_path,
        **(
            {"previous_active_pack_version_id": previous_active_pack_version_id}
            if previous_active_pack_version_id is not None
            else {}
        ),
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
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
        "actor_id": _required_str(payload, "actor_id"),
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
    client = s3_client or _s3_client()
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
    _verify_compliance_locked_evidence_version(
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
) -> None:
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


def materialize_live_benchmark_catalog_artifact(
    plan_json: Mapping[str, Any] | str,
    *,
    fetch_bytes: Callable[[str], bytes] | None = None,
    snapshot_writer: Callable[..., dict[str, Any]] | None = None,
    snapshot_bytes_writer: Callable[..., dict[str, Any]] | None = None,
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
    result["blockingSuiteIds"] = [
        _required_str(suite, "suite_id")
        for suite in _required_object_list(evidence, "suites")
        if _required_str(suite, "execution_status") != "ready"
    ]
    return result


def load_materialized_benchmark_catalog_snapshot(
    plan_json: Mapping[str, Any] | str,
    *,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Load a KPO-produced catalog receipt and bind the catalog's exact S3 version.

    The scheduler never trusts pod stdout as benchmark provenance.  It reads the
    receipt from the isolated executor's WORM path, then checks the receipt's
    catalog VersionId, SHA-256, status, and blocking suites against the exact
    catalog object before handing it to the D6 suite-plan writer.
    """

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") not in {
        "serp_nightly_regression_suite",
        "serp_mandatory_benchmark_dataset_evidence_snapshot",
    }:
        raise ValueError("plan dag_id does not match benchmark catalog receipt loader")
    artifact_paths = _required_artifact_paths(
        plan,
        ("benchmark_catalog", "benchmark_catalog_receipt"),
    )
    client = s3_client or _s3_client()
    receipt_bytes, receipt_version_id = _read_compliance_locked_s3_bytes(
        client,
        artifact_paths["benchmark_catalog_receipt"],
        field_name="benchmark_catalog_receipt",
    )
    try:
        receipt = _json_object(
            receipt_bytes.decode("utf-8"),
            "benchmark_catalog_materialization_receipt",
        )
    except UnicodeDecodeError as exc:
        raise ValueError("benchmark catalog materialization receipt is not UTF-8 JSON") from exc
    if _required_str(receipt, "contractVersion") != "serp-benchmark-catalog-materializer/v1":
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
    catalog_bytes, observed_catalog_version_id = _read_compliance_locked_s3_bytes(
        client,
        artifact_paths["benchmark_catalog"],
        field_name="benchmark_catalog",
        version_id=catalog_version_id,
    )
    if observed_catalog_version_id != catalog_version_id:
        raise ValueError("benchmark catalog receipt VersionId does not match catalog object")
    if sha256(catalog_bytes).hexdigest() != _required_str(catalog_snapshot, "artifactSha256"):
        raise ValueError("benchmark catalog receipt SHA-256 does not match catalog object")
    try:
        catalog = _json_object(catalog_bytes.decode("utf-8"), "benchmark_catalog")
    except UnicodeDecodeError as exc:
        raise ValueError("benchmark catalog object is not UTF-8 JSON") from exc
    if _required_str(catalog, "catalog_status") != _required_str(catalog_snapshot, "catalogStatus"):
        raise ValueError("benchmark catalog receipt status does not match catalog object")
    suites = _required_object_list(catalog, "suites")
    suite_ids = [_required_str(suite, "suite_id") for suite in suites]
    if suite_ids != list(MANDATORY_SERP_BENCHMARK_SUITES):
        raise ValueError(
            "benchmark catalog object must contain mandatory suites in canonical order"
        )
    actual_blocking_suite_ids = [
        _required_str(suite, "suite_id")
        for suite in suites
        if _required_str(suite, "execution_status") != "ready"
    ]
    if actual_blocking_suite_ids != _required_str_list(catalog_snapshot, "blockingSuiteIds"):
        raise ValueError("benchmark catalog receipt blocking suites do not match catalog object")
    return {
        **catalog_snapshot,
        "catalogReceiptPath": artifact_paths["benchmark_catalog_receipt"],
        "catalogReceiptVersionId": receipt_version_id,
    }


def _read_compliance_locked_s3_bytes(
    s3_client: Any,
    artifact_path: str,
    *,
    field_name: str,
    version_id: str | None = None,
) -> tuple[bytes, str]:
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
    response = s3_client.get_object(Bucket=bucket, Key=key, VersionId=observed_version_id)
    if not isinstance(response, Mapping):
        raise ValueError(f"{field_name} GetObject response is invalid")
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise ValueError(f"{field_name} GetObject response is missing Body")
    payload = body.read()
    if not isinstance(payload, bytes) or not payload:
        raise ValueError(f"{field_name} object is empty")
    return payload, observed_version_id


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


def write_public_docs_publish_activation_trigger_conf_artifact(
    plan_json: Mapping[str, Any] | str,
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

    trigger_conf = {
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
    refresh_plan = _read_json_file(
        _artifact_path(
            "public_docs_seed_refresh_plan_path",
            _required_str(plan, "public_docs_seed_refresh_plan_path"),
        ),
        "public_docs_seed_refresh_plan",
    )
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
    refresh_plan = _read_json_file(refresh_plan_path, "public_docs_seed_refresh_plan")
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
    registry = _required_object_list(refresh_plan, "seed_registry")
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


def write_nightly_suite_plan_artifact(
    plan_json: Mapping[str, Any] | str,
    benchmark_catalog_snapshot: Mapping[str, Any] | str | None = None,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_nightly_regression_suite":
        raise ValueError("plan dag_id does not match nightly suite-plan writer")
    if benchmark_catalog_snapshot is None:
        raise ValueError("nightly suite plan requires the live benchmark catalog snapshot")
    catalog_snapshot = _json_object(benchmark_catalog_snapshot, "benchmark_catalog_snapshot")
    artifact_paths = _required_artifact_paths(plan, ("suite_plan", "benchmark_catalog"))
    if _required_str(catalog_snapshot, "artifactPath") != artifact_paths["benchmark_catalog"]:
        raise ValueError("benchmark catalog snapshot must match the plan artifact path")
    if _required_str(catalog_snapshot, "objectLockMode") != "COMPLIANCE":
        raise ValueError("benchmark catalog snapshot must use COMPLIANCE object lock")
    if not _required_str(catalog_snapshot, "artifactVersionId"):
        raise ValueError("benchmark catalog snapshot must include an S3 object version")
    catalog_status = _required_str(catalog_snapshot, "catalogStatus")
    if catalog_status != "ready":
        blocking_suite_ids = _required_str_list(catalog_snapshot, "blockingSuiteIds")
        raise ValueError(
            "benchmark catalog blocks D6 until dataset licenses are attested: "
            + ", ".join(blocking_suite_ids)
        )
    payload = _nightly_suite_plan_payload(plan)
    artifact_path = artifact_paths["suite_plan"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="suite_plan",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


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


def write_nightly_benchmark_export_artifact(
    nightly_report_artifact: Mapping[str, Any] | str,
) -> dict[str, Any]:
    report = _artifact_payload(nightly_report_artifact, "nightly_report")
    artifact_paths = _required_artifact_paths(report, ("benchmark_gate_export",))
    payload = _benchmark_export_payload(report)
    _validate_benchmark_export_payload(payload)
    artifact_path = artifact_paths["benchmark_gate_export"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="benchmark_gate_export",
        operation_id=_required_str(report, "operation_id"),
        payload=payload,
    )


def write_nightly_registry_submissions_artifact(
    benchmark_export_artifact: Mapping[str, Any] | str,
) -> dict[str, Any]:
    export_payload = _artifact_payload(benchmark_export_artifact, "benchmark_gate_export")
    artifact_paths = _required_artifact_paths(
        export_payload,
        ("nightly_registry_submissions", "nightly_registry_receipts"),
    )
    submissions = {
        "artifact_paths": artifact_paths,
        "contractVersion": "serp-bc21-dry-run-submissions/v1",
        "dryRun": True,
        "generatedAt": _required_str(export_payload, "generatedAt"),
        "items": [
            {
                "benchmarkResultId": _required_str(item, "benchmarkResultId"),
                "evidenceBundleId": _required_str(item, "evidenceBundleId"),
                "gateStatus": _required_str(item, "gateStatus"),
                "normalizedScore": _required_str(item, "normalizedScore"),
                "registryResourceId": _required_str(item, "registryResourceId"),
                "registryResourceType": _required_str(item, "registryResourceType"),
                "sourceEvidenceBundleId": _required_str(item, "sourceEvidenceBundleId"),
                "suiteCode": _required_str(item, "suiteCode"),
            }
            for item in _required_object_list(export_payload, "items")
        ],
        "operationId": _required_str(export_payload, "operationId"),
        "status": "ready_for_dry_run_submission",
        "tenantId": _required_str(export_payload, "tenantId"),
    }
    artifact_path = artifact_paths["nightly_registry_submissions"]
    _write_json_artifact(artifact_path, submissions)
    return _artifact_result(
        artifact_path,
        artifact_type="nightly_registry_submissions",
        operation_id=_required_str(export_payload, "operationId"),
        payload=submissions,
    )


def write_nightly_registry_receipts_artifact(
    registry_submissions_artifact: Mapping[str, Any] | str,
) -> dict[str, Any]:
    submissions = _artifact_payload(registry_submissions_artifact, "nightly_registry_submissions")
    artifact_paths = _required_artifact_paths(submissions, ("nightly_registry_receipts",))
    receipts = {
        "contractVersion": "serp-bc21-dry-run-receipts/v1",
        "dryRun": True,
        "generatedAt": _required_str(submissions, "generatedAt"),
        "operationId": _required_str(submissions, "operationId"),
        "receipts": [
            {
                "accepted": True,
                "benchmarkResultId": _required_str(item, "benchmarkResultId"),
                "dryRun": True,
                "evidenceBundleId": _required_str(item, "evidenceBundleId"),
                "registryReceiptId": str(
                    uuid5(
                        _BENCHMARK_NAMESPACE,
                        "bc21-dry-run-receipt|"
                        f"{_required_str(submissions, 'operationId')}|"
                        f"{_required_str(item, 'suiteCode')}|"
                        f"{_required_str(item, 'benchmarkResultId')}",
                    )
                ),
                "sourceEvidenceBundleId": _required_str(item, "sourceEvidenceBundleId"),
                "statusCode": 202,
                "suiteCode": _required_str(item, "suiteCode"),
            }
            for item in _required_object_list(submissions, "items")
        ],
        "status": "dry_run_accepted",
        "tenantId": _required_str(submissions, "tenantId"),
    }
    artifact_path = artifact_paths["nightly_registry_receipts"]
    _write_json_artifact(artifact_path, receipts)
    return _artifact_result(
        artifact_path,
        artifact_type="nightly_registry_receipts",
        operation_id=_required_str(submissions, "operationId"),
        payload=receipts,
    )


def write_improvement_spec_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_benchmark_improvement_wave":
        raise ValueError("plan dag_id does not match improvement artifact writer")
    _required_artifact_paths(
        plan,
        (
            "airflow_plan",
            "improvement_spec",
            "paired_eval_request",
            "paired_eval_receipt",
        ),
    )
    artifact_paths = _required_mapping(plan, "artifact_paths")
    payload = _improvement_spec_payload(plan, artifact_paths)
    artifact_path = artifact_paths["improvement_spec"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="improvement_spec",
        operation_id=_required_str(plan, "operation_id"),
        payload=payload,
    )


def write_paired_eval_request_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    """Persist the scoreless D19 request consumed by the paired evaluator."""

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_benchmark_improvement_wave":
        raise ValueError("plan dag_id does not match paired-eval request writer")
    artifact_paths = _required_artifact_paths(plan, ("paired_eval_request",))
    payload = _paired_eval_request_payload(plan)
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
        "requestEvidence": request_evidence,
    }


def build_nightly_runner_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_nightly_regression_suite",
        task_id="run_mandatory_benchmark_suites",
        command="nightly-report",
        input_path_keys=("airflow_plan", "suite_plan"),
        output_path_key="nightly_report",
        option_names=("--airflow-plan", "--suite-plan"),
    )


def build_nightly_registry_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_nightly_regression_suite",
        task_id="build_bc21_benchmark_run_submissions",
        command="nightly-registry-submissions",
        input_path_keys=("airflow_plan", "nightly_report"),
        output_path_key="nightly_registry_submissions",
        option_names=("--airflow-plan", "--nightly-report"),
    )


def build_nightly_registry_submit_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_nightly_regression_suite",
        task_id="submit_bc21_benchmark_run_submissions",
        command="submit-nightly-registry-submissions",
        input_path_keys=("airflow_plan", "nightly_registry_submissions"),
        output_path_key="nightly_registry_receipts",
        option_names=("--airflow-plan", "--nightly-registry-submissions"),
        extra_options=(
            "--bc21-base-url",
            _required_bc21_base_url(_json_object(plan_json, "plan_json")),
        ),
    )


def build_nightly_benchmark_export_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_nightly_regression_suite",
        task_id="build_c1_benchmark_gate_export",
        command="nightly-benchmark-export",
        input_path_keys=("airflow_plan", "nightly_report"),
        output_path_key="benchmark_gate_export",
        option_names=("--airflow-plan", "--nightly-report"),
    )


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


def evaluate_nightly_regression_gate(report: Mapping[str, Any]) -> dict[str, Any]:
    payload = _payload(report)
    findings: list[dict[str, Any]] = []
    for suite in _required_object_list(payload, "suite_results"):
        suite_id = _required_str(suite, "suite_id")
        for metric in _required_object_list(suite, "metric_results"):
            normalized_score = _required_number(metric, "normalized_score")
            if normalized_score < SERP_NORMALIZED_GATE_FLOOR:
                findings.append(
                    {
                        "metric": _required_str(metric, "metric"),
                        "metric_family": _required_str(metric, "metric_family"),
                        "normalized_score": normalized_score,
                        "suite_id": suite_id,
                    }
                )
    return {
        "blocking_findings": findings,
        "normalized_gate_floor": SERP_NORMALIZED_GATE_FLOOR,
        "status": "blocked" if findings else "passed",
    }


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
    return {
        "actor_id": _PUBLIC_DOCS_SEARCH_SERVE_SMOKE_ACTOR_ID,
        "auth_context_version": "airflow-public-docs-smoke@2026.07.1",
        "auth_issuer": "airflow://serp-public-docs",
        "auth_method": "airflow-dag-task",
        "auth_session_id": _required_str(plan, "operation_id"),
        "auth_subject_id": _PUBLIC_DOCS_SEARCH_SERVE_SMOKE_ACTOR_ID,
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
    registry_seeds = _required_object_list(refresh_plan, "seed_registry")
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
        for seed in _required_object_list(plan, "seed_registry")
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


def _nightly_suite_plan_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    generated_at = _required_datetime_string(plan, "generated_at")
    selected_suite_ids = _required_str_list(plan, "selected_suite_ids")
    pack_version_ids = _required_str_list(plan, "pack_version_ids")
    tenant_id = _required_str(plan, "tenant_id")
    retrieval_profile_version = _required_str(plan, "retrieval_profile_version")
    reranker_profile_version = _required_str(plan, "reranker_profile_version")
    return {
        "artifact_paths": dict(_required_mapping(plan, "artifact_paths")),
        "contract_version": _EVAL_CONTRACT_VERSION,
        "generated_at": generated_at,
        "metadata": {
            "airflowOperationId": _required_str(plan, "operation_id"),
            "trigger": "airflow-nightly",
        },
        "pack_version_ids": pack_version_ids,
        "registry_resource_id": _required_str(plan, "registry_resource_id"),
        "registry_resource_type": _required_resource_type(plan, "registry_resource_type"),
        "reranker_profile_version": reranker_profile_version,
        "retrieval_profile_version": retrieval_profile_version,
        "schedule_id": _required_str(plan, "dag_id"),
        "selected_suite_ids": selected_suite_ids,
        "suites": _required_nightly_benchmark_suite_inputs(
            plan,
            selected_suite_ids=tuple(selected_suite_ids),
        ),
        "tenant_id": tenant_id,
    }


def _nightly_report_from_suite_plan_payload(
    suite_plan: Mapping[str, Any],
) -> dict[str, Any]:
    if _required_str(suite_plan, "contract_version") != _EVAL_CONTRACT_VERSION:
        raise ValueError("unsupported suite plan contract_version")
    selected_suite_ids = _required_str_list(suite_plan, "selected_suite_ids")
    suites_by_id = {
        _required_str(suite, "suite_id"): suite
        for suite in _required_object_list(suite_plan, "suites")
    }
    if tuple(suites_by_id) != tuple(selected_suite_ids):
        raise ValueError("suites must match selected_suite_ids")
    suite_results = [
        _suite_result_from_suite_plan(suites_by_id[suite_id]) for suite_id in selected_suite_ids
    ]
    status = "blocked" if any(suite["status"] == "blocked" for suite in suite_results) else "passed"
    operation_id = _operation_id(
        "serp-nightly-regression",
        _required_str(suite_plan, "schedule_id"),
        _required_str(suite_plan, "tenant_id"),
        _required_str(suite_plan, "generated_at"),
        ",".join(_required_str_list(suite_plan, "pack_version_ids")),
        ",".join(selected_suite_ids),
        ",".join(_required_str(suite, "operation_id") for suite in suite_results),
    )
    return {
        "artifact_paths": dict(_required_mapping(suite_plan, "artifact_paths")),
        "contract_version": _EVAL_CONTRACT_VERSION,
        "generated_at": _required_str(suite_plan, "generated_at"),
        "metadata": dict(_required_mapping(suite_plan, "metadata")),
        "operation_id": operation_id,
        "pack_version_ids": _required_str_list(suite_plan, "pack_version_ids"),
        "registry_resource_id": _required_str(suite_plan, "registry_resource_id"),
        "registry_resource_type": _required_resource_type(suite_plan, "registry_resource_type"),
        "reranker_profile_version": _required_str(suite_plan, "reranker_profile_version"),
        "retrieval_profile_version": _required_str(suite_plan, "retrieval_profile_version"),
        "schedule_id": _required_str(suite_plan, "schedule_id"),
        "selected_suite_ids": selected_suite_ids,
        "status": status,
        "suite_results": suite_results,
        "tenant_id": _required_str(suite_plan, "tenant_id"),
    }


def _suite_result_from_suite_plan(suite: Mapping[str, Any]) -> dict[str, Any]:
    if _required_str(suite, "suite_contract_version") != _BENCHMARK_SUITE_CONTRACT_VERSION:
        raise ValueError("unsupported suite_contract_version")
    metric_compatibility = _required_metric_compatibility(
        _required_mapping(suite, "metric_compatibility"),
        selected_suite_ids=MANDATORY_SERP_BENCHMARK_SUITES,
        contract_version_field="contract_version",
        matrix_uri_field="matrix_uri",
        matrix_sha256_field="matrix_sha256",
        matrix_version_id_field="matrix_version_id",
        suite_id_field="suite_id",
        metric_families_field="metric_families",
    )
    required_metric_families = _metric_families_for_suite(
        metric_compatibility,
        _required_str(suite, "suite_id"),
    )
    _validate_nightly_suite_metric_records(
        suite,
        required_metric_families=required_metric_families,
    )
    query_ids = [_required_str(case, "query_id") for case in _required_object_list(suite, "cases")]
    metric_results = [
        _metric_result_from_reference(suite, reference)
        for reference in _required_object_list(suite, "references")
    ]
    if {metric["metric_family"] for metric in metric_results} != set(required_metric_families):
        raise ValueError(
            "suite results must exactly match metric_compatibility required metric families"
        )
    status = (
        "blocked" if any(metric["status"] == "blocked" for metric in metric_results) else "passed"
    )
    operation_id = _operation_id(
        "retrieval-eval",
        _required_str(suite, "suite_id"),
        _required_str(suite, "suite_version"),
        _required_str(suite, "tenant_id"),
        ",".join(query_ids),
        ",".join(_required_str(metric, "metric") for metric in metric_results),
    )
    operation_sha256 = sha256(
        _canonical_json(
            {
                "metric_results": metric_results,
                "operation_id": operation_id,
                "query_ids": query_ids,
                "suite_id": _required_str(suite, "suite_id"),
                "suite_version": _required_str(suite, "suite_version"),
            }
        ).encode("utf-8")
    ).hexdigest()
    return {
        "metadata": {
            **dict(_required_mapping(suite, "metadata")),
            "metric_compatibility": metric_compatibility,
        },
        "metric_count": len(metric_results),
        "metric_results": metric_results,
        "operation_id": operation_id,
        "operation_sha256": operation_sha256,
        "query_ids": query_ids,
        "status": status,
        "suite_id": _required_str(suite, "suite_id"),
        "suite_version": _required_str(suite, "suite_version"),
    }


def _metric_result_from_reference(
    suite: Mapping[str, Any],
    reference: Mapping[str, Any],
) -> dict[str, Any]:
    metric = _required_str(reference, "metric")
    metric_family = _required_str(reference, "metric_family")
    reference_score = _required_number(reference, "reference_score")
    threshold = _required_number(reference, "threshold")
    score = (
        _retrieval_score(suite, metric)
        if metric_family == "retrieval"
        else _observed_metric_score(suite, metric_family, metric)
    )
    normalized_score = score / reference_score
    return {
        "metric": metric,
        "metric_family": metric_family,
        "normalized_score": normalized_score,
        "reference_id": _required_str(reference, "reference_id"),
        "reference_score": reference_score,
        "score": score,
        "status": "passed" if normalized_score >= threshold else "blocked",
        "threshold": threshold,
    }


def _retrieval_score(suite: Mapping[str, Any], metric: str) -> float:
    if metric != "MRR@10":
        raise ValueError("D6 suite-plan runner currently supports retrieval MRR@10")
    scores: list[float] = []
    for case in _required_object_list(suite, "cases"):
        relevant = set(_required_str_list(case, "relevant_chunk_ids"))
        ranked = _required_str_list(case, "ranked_chunk_ids")[:10]
        score = 0.0
        for index, chunk_id in enumerate(ranked, start=1):
            if chunk_id in relevant:
                score = 1.0 / index
                break
        scores.append(score)
    return sum(scores) / len(scores)


def _observed_metric_score(
    suite: Mapping[str, Any],
    metric_family: str,
    metric: str,
) -> float:
    observations = {
        (_required_str(item, "metric_family"), _required_str(item, "metric")): item
        for item in _required_object_list(suite, "metric_observations")
    }
    key = (metric_family, metric)
    if key not in observations:
        raise ValueError(f"missing metric_observation {metric_family}/{metric}")
    return _required_number(observations[key], "score")


def _live_registry_submissions_payload(
    report: Mapping[str, Any],
    *,
    actor_id: str,
) -> dict[str, Any]:
    _require_non_empty("actor_id", actor_id)
    if _required_str(report, "status") != "passed":
        raise ValueError("nightly report must pass before registry submission")
    submissions = [
        _live_registry_submission(report, suite, metric_family, actor_id)
        for suite in _required_object_list(report, "suite_results")
        for metric_family in _suite_result_metric_families(suite)
    ]
    return {
        "contract_version": _EVAL_CONTRACT_VERSION,
        "nightly_operation_id": _required_str(report, "operation_id"),
        "operation_id": _operation_id(
            "serp-nightly-registry-bridge",
            _required_str(report, "tenant_id"),
            _required_str(report, "operation_id"),
            ",".join(_required_str(item, "idempotencyKey") for item in submissions),
        ),
        "submissions": submissions,
        "tenant_id": _required_str(report, "tenant_id"),
    }


def _live_registry_submission(
    report: Mapping[str, Any],
    suite: Mapping[str, Any],
    metric_family: str,
    actor_id: str,
) -> dict[str, Any]:
    suite_code = _required_str(suite, "suite_id")
    metrics = [
        metric
        for metric in _required_object_list(suite, "metric_results")
        if _required_str(metric, "metric_family") == metric_family
    ]
    if not metrics:
        raise ValueError(f"missing mandatory metric_family {suite_code}/{metric_family}")
    body = {
        "actorId": actor_id,
        "cases": [
            {
                "caseId": (
                    f"{_required_str(suite, 'operation_id')}:"
                    f"{_required_str(metric, 'metric')}:"
                    f"{_required_str(metric, 'reference_id')}:"
                    f"{_required_str(suite, 'operation_sha256')}"
                ),
                "expectedScore": _required_number(metric, "reference_score"),
                "observedScore": _required_number(metric, "score"),
            }
            for metric in metrics
        ],
        "evaluationContractCode": _BENCHMARK_EVALUATION_CONTRACT_CODE,
        "metricFamily": metric_family,
        "provenance": _benchmark_run_provenance(suite),
        "referenceSourceType": "official_baseline",
        "resourceId": _required_str(report, "registry_resource_id"),
        "resourceType": _required_resource_type(report, "registry_resource_type"),
        "runnerVersion": "airflow-d6-serp-eval-runner@2026.07.3",
        "scoringAlgorithmVersion": f"airflow-d6-eval-contract@{_EVAL_CONTRACT_VERSION}",
        "suiteCode": suite_code,
        "suiteVersion": _required_str(suite, "suite_version"),
    }
    idempotency_key = uuid5(
        NAMESPACE_URL,
        "\n".join(
            (
                "serp-nightly-registry-idempotency-v1",
                _required_str(report, "tenant_id"),
                _required_str(report, "operation_id"),
                suite_code,
                metric_family,
                _required_str(suite, "operation_sha256"),
            )
        ),
    )
    return {
        "body": body,
        "endpointPath": "/api/bc-21/serp/v1/governance/benchmark-runs",
        "fingerprint": "sha256:" + sha256(_canonical_json(body).encode("utf-8")).hexdigest(),
        "idempotencyKey": str(idempotency_key),
        "metricFamily": metric_family,
        "suiteCode": suite_code,
        "tenantId": _required_str(report, "tenant_id"),
        "trustedActorId": actor_id,
    }


def _benchmark_run_provenance(suite: Mapping[str, Any]) -> dict[str, str]:
    metadata = _required_mapping(suite, "metadata")
    metric_compatibility = _required_mapping(metadata, "metric_compatibility")
    return {
        "adapterId": _required_str(metadata, "adapter_id"),
        "adapterVersion": _required_str(metadata, "adapter_version"),
        "adapterSourceUri": _required_str(metadata, "adapter_source_uri"),
        "adapterSourceRevision": _required_str(metadata, "adapter_source_revision"),
        "adapterImageDigest": _required_str(metadata, "adapter_image_digest"),
        "datasetLicenseId": _required_str(metadata, "dataset_license_id"),
        "datasetDistributionRule": _required_str(metadata, "dataset_distribution_rule"),
        "datasetRightsStatus": _required_str(metadata, "dataset_rights_status"),
        "datasetManifestUri": _required_str(metadata, "dataset_manifest_uri"),
        "datasetManifestSha256": _required_str(metadata, "dataset_manifest_sha256"),
        "datasetManifestVersionId": _required_str(metadata, "dataset_manifest_version_id"),
        "executionEvidenceUri": _required_str(metadata, "execution_evidence_uri"),
        "executionEvidenceSha256": _required_str(metadata, "execution_evidence_sha256"),
        "executionEvidenceVersionId": _required_str(metadata, "execution_evidence_version_id"),
        "metricCompatibilityUri": _required_str(metric_compatibility, "matrix_uri"),
        "metricCompatibilitySha256": _required_str(metric_compatibility, "matrix_sha256"),
        "metricCompatibilityVersionId": _required_str(metric_compatibility, "matrix_version_id"),
        "referenceSourceUri": _required_str(metadata, "reference_source_uri"),
    }


def _submit_live_registry_submissions(
    submissions: Mapping[str, Any],
    *,
    bc21_base_url: str,
) -> dict[str, Any]:
    base_url = _required_bc21_base_url({"bc21_base_url": bc21_base_url}).rstrip("/")
    tenant_id = _required_str(submissions, "tenant_id")
    receipts = [
        _submit_live_registry_submission(base_url, tenant_id, submission)
        for submission in _required_object_list(submissions, "submissions")
    ]
    return {
        "contract_version": _EVAL_CONTRACT_VERSION,
        "nightly_operation_id": _required_str(submissions, "nightly_operation_id"),
        "operation_id": _operation_id(
            "serp-nightly-registry-receipts",
            _required_str(submissions, "operation_id"),
            ",".join(_required_str(receipt, "benchmarkResultId") for receipt in receipts),
        ),
        "receipts": receipts,
        "status": "accepted",
        "tenant_id": tenant_id,
    }


def _submit_live_registry_submission(
    base_url: str,
    tenant_id: str,
    submission: Mapping[str, Any],
) -> dict[str, Any]:
    body = _required_mapping(submission, "body")
    endpoint_path = _required_str(submission, "endpointPath")
    request = Request(
        base_url + endpoint_path,
        data=_canonical_json(body).encode("utf-8"),
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Adapstory-Actor-Id": _required_str(submission, "trustedActorId"),
            "X-Adapstory-Tenant-Id": tenant_id,
            "X-Fingerprint": _required_str(submission, "fingerprint"),
            "X-Idempotency-Key": _required_str(submission, "idempotencyKey"),
            **_bc21_workload_authorization_headers(),
        },
    )
    try:
        with urlopen(request, timeout=5.0) as response:
            status_code = response.status
            response_payload = _json_object(
                response.read().decode("utf-8"), "benchmark_registry_response"
            )
    except HTTPError as exc:
        raise ValueError(
            "benchmark registry submission for "
            f"{_required_str(submission, 'suiteCode')}/"
            f"{_required_str(submission, 'metricFamily')} failed: status={exc.code}"
            f"{_safe_bc21_problem_detail(exc)}"
        ) from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ValueError(
            "benchmark registry submission failed for "
            f"{_required_str(submission, 'suiteCode')}/"
            f"{_required_str(submission, 'metricFamily')}"
        ) from exc
    if status_code < 200 or status_code >= 300:
        raise ValueError(f"benchmark registry submission failed: status={status_code}")
    return {
        "benchmarkResultId": _required_str(response_payload, "benchmarkResultId"),
        "endpointPath": endpoint_path,
        "gateStatus": _required_str(response_payload, "gateStatus"),
        "metricFamily": _required_str(submission, "metricFamily"),
        "responseBodySha256": sha256(_canonical_json(response_payload).encode("utf-8")).hexdigest(),
        "runId": _required_str(response_payload, "runId"),
        "statusCode": status_code,
        "suiteCode": _required_str(submission, "suiteCode"),
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


def _benchmark_export_payload(report: Mapping[str, Any]) -> dict[str, Any]:
    artifact_paths = _required_artifact_paths(
        report,
        (
            "benchmark_gate_export",
            "nightly_registry_submissions",
            "nightly_registry_receipts",
        ),
    )
    items: list[dict[str, Any]] = []
    for suite in _required_object_list(report, "suite_results"):
        suite_id = _required_str(suite, "suite_id")
        normalized_score = min(
            _required_number(metric, "normalized_score")
            for metric in _required_object_list(suite, "metric_results")
        )
        operation_id = _required_str(suite, "operation_id")
        benchmark_result_id = str(
            uuid5(
                _BENCHMARK_NAMESPACE,
                "benchmark-result|"
                f"{_required_str(report, 'operation_id')}|{suite_id}|"
                f"{operation_id}",
            )
        )
        evidence_bundle_id = str(
            uuid5(
                _BENCHMARK_NAMESPACE,
                "evidence-bundle|"
                f"{_required_str(report, 'operation_id')}|{suite_id}|"
                f"{_required_str(suite, 'operation_sha256')}",
            )
        )
        items.append(
            {
                "benchmarkResultId": benchmark_result_id,
                "evidenceBundleId": evidence_bundle_id,
                "gateStatus": _required_str(suite, "status"),
                "generatedAt": _required_str(report, "generated_at"),
                "normalizedScore": f"{normalized_score:.4f}",
                "operationSha256": _required_str(suite, "operation_sha256"),
                "registryResourceId": _required_str(report, "registry_resource_id"),
                "registryResourceType": _required_resource_type(report, "registry_resource_type"),
                "runId": operation_id,
                "sourceEvidenceBundleId": evidence_bundle_id,
                "suiteCode": suite_id,
                "suiteVersion": _required_str(suite, "suite_version"),
                "tenantId": _required_str(report, "tenant_id"),
            }
        )
    return {
        "artifact_paths": artifact_paths,
        "contractVersion": "serp-c1-benchmark-gate-export/v1",
        "generatedAt": _required_str(report, "generated_at"),
        "items": items,
        "normalizedGateFloor": f"{SERP_NORMALIZED_GATE_FLOOR:.4f}",
        "operationId": _required_str(report, "operation_id"),
        "packVersionIds": _required_str_list(report, "pack_version_ids"),
        "status": "passed",
        "tenantId": _required_str(report, "tenant_id"),
    }


def _validate_benchmark_export_payload(payload: Mapping[str, Any]) -> None:
    items = _required_object_list(payload, "items")
    suites = [_required_str(item, "suiteCode") for item in items]
    if suites != list(MANDATORY_SERP_BENCHMARK_SUITES):
        raise ValueError("benchmark export must include every mandatory suite")
    for item in items:
        if _required_str(item, "gateStatus") != "passed":
            raise ValueError("benchmark export includes a non-passing suite")
        if float(_required_str(item, "normalizedScore")) < SERP_NORMALIZED_GATE_FLOOR:
            raise ValueError("benchmark export normalized score is below gate floor")


def _improvement_replay_context(
    payload: Mapping[str, Any], baseline_run_id: str, candidate_run_id: str
) -> dict[str, Any]:
    if isinstance(payload.get("replay_context"), Mapping):
        replay_context = dict(_required_mapping(payload, "replay_context"))
        if _required_str(replay_context, "baselineRunId") != baseline_run_id:
            raise ValueError("replay_context baselineRunId does not match baseline")
        if _required_str(replay_context, "candidateRunId") != candidate_run_id:
            raise ValueError("replay_context candidateRunId does not match candidate")
        feature_flags = _required_str_list(replay_context, "featureFlags")
        if len(feature_flags) != len(set(feature_flags)):
            raise ValueError("featureFlags must not contain duplicates")
        for field_name in (
            "guardrailBundleVersion",
            "judgeModelId",
            "judgeModelVersion",
            "judgePromptTemplateVersion",
            "modelCatalogEntryId",
            "policyBundleVersion",
            "providerRouteId",
            "rerankerProfileVersion",
            "retrievalProfileVersion",
        ):
            _required_str(replay_context, field_name)
        return replay_context
    feature_flags = _required_str_list(payload, "feature_flags")
    if len(feature_flags) != len(set(feature_flags)):
        raise ValueError("feature_flags must not contain duplicates")
    return {
        "baselineRunId": baseline_run_id,
        "candidateRunId": candidate_run_id,
        "featureFlags": feature_flags,
        "guardrailBundleVersion": _required_str(payload, "guardrail_bundle_version"),
        "judgeModelId": _required_str(payload, "judge_model_id"),
        "judgeModelVersion": _required_str(payload, "judge_model_version"),
        "judgePromptTemplateVersion": _required_str(payload, "judge_prompt_template_version"),
        "modelCatalogEntryId": _required_str(payload, "model_catalog_entry_id"),
        "policyBundleVersion": _required_str(payload, "policy_bundle_version"),
        "providerRouteId": _required_str(payload, "provider_route_id"),
        "rerankerProfileVersion": _required_str(payload, "reranker_profile_version"),
        "retrievalProfileVersion": _required_str(payload, "retrieval_profile_version"),
    }


def _improvement_model_governance(payload: Mapping[str, Any]) -> dict[str, str]:
    if isinstance(payload.get("model_governance"), Mapping):
        governance = dict(_required_mapping(payload, "model_governance"))
        if _required_str(governance, "status") != "approved-for-eval-dry-run":
            raise ValueError("model_governance status is not approved")
        return {
            "guardrailBundleVersion": _required_str(governance, "guardrailBundleVersion"),
            "judgeModelId": _required_str(governance, "judgeModelId"),
            "judgeModelVersion": _required_str(governance, "judgeModelVersion"),
            "modelCatalogEntryId": _required_str(governance, "modelCatalogEntryId"),
            "policyBundleVersion": _required_str(governance, "policyBundleVersion"),
            "providerRouteId": _required_str(governance, "providerRouteId"),
            "status": "approved-for-eval-dry-run",
        }
    return {
        "guardrailBundleVersion": _required_str(payload, "guardrail_bundle_version"),
        "judgeModelId": _required_str(payload, "judge_model_id"),
        "judgeModelVersion": _required_str(payload, "judge_model_version"),
        "modelCatalogEntryId": _required_str(payload, "model_catalog_entry_id"),
        "policyBundleVersion": _required_str(payload, "policy_bundle_version"),
        "providerRouteId": _required_str(payload, "provider_route_id"),
        "status": "approved-for-eval-dry-run",
    }


def _paired_eval_request_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Build a canonical, scoreless request from the authoritative suite catalog."""

    from dags.serp_benchmark_catalog import MANDATORY_BENCHMARK_SUITE_CATALOG

    selected_suite_ids = _required_str_list(plan, "selected_suite_ids")
    catalog_suite_ids = [entry.suite_id for entry in MANDATORY_BENCHMARK_SUITE_CATALOG]
    if selected_suite_ids != catalog_suite_ids:
        raise ValueError("paired evaluator request must use the canonical suite catalog order")
    return {
        "baselineRunId": _required_str(plan, "baseline_run_id"),
        "candidateId": _required_str(plan, "candidate_id"),
        "candidateRunId": _required_str(plan, "candidate_run_id"),
        "improvementSpecId": _required_str(plan, "improvement_spec_id"),
        "metricDefinitionAuthority": "executor-pinned-metric-definition-profile",
        "requestId": _required_str(plan, "operation_id"),
        "selectedSuiteIds": selected_suite_ids,
        "suiteBindings": [
            {
                "adapterId": f"catalog:{entry.suite_id}@{entry.dataset_revision}",
                "executionStatus": entry.execution_status,
                "suiteId": entry.suite_id,
            }
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        ],
    }


def _improvement_spec_payload(
    plan: Mapping[str, Any], artifact_paths: Mapping[str, str]
) -> dict[str, Any]:
    generated_at = _required_datetime_string(plan, "generated_at")
    baseline_run_id = _required_str(plan, "baseline_run_id")
    selected_suite_ids = _required_str_list(plan, "selected_suite_ids")
    return {
        "acceptance": {
            "keepRule": {"type": "multi-metric"},
            "rejectRule": {"type": "fail-fast"},
        },
        "apiVersion": "serp.adapstory.ai/v1alpha1",
        "artifact_paths": dict(artifact_paths),
        "baseline": {
            "beatCondition": {
                "bootstrapConfidenceLevel": "0.95",
                "minimumMultiplier": "2.0",
                "pairedRunCount": 5,
                "rule": "all_required_primary_accuracy_metrics_meet_multiplier",
            },
            "normalizedGateFloor": SERP_NORMALIZED_GATE_FLOOR,
            "referenceRunId": baseline_run_id,
            "source": "validated-internal-baseline",
        },
        "benchmarks": {"requiredSuites": selected_suite_ids},
        "budgets": {
            "maxBenchmarkRuns": _required_positive_int(plan, "max_benchmark_runs"),
            "maxCostUsdEquivalent": 50,
            "wallClockBudgetMinutes": 180,
        },
        "candidate": {
            "id": _required_str(plan, "candidate_id"),
            "runId": _required_str(plan, "candidate_run_id"),
            "scoreAuthority": "executor-receipt-only",
        },
        "dryRun": False,
        "constraints": {
            "mustHold": [
                "Policy Compliance Rate == 1.0 on blocking cases",
                "Citation Accuracy >= 0.95",
                "Evidence Completeness >= 0.99",
            ],
        },
        "evidence": {
            "requiredArtifacts": [
                "candidate_diff_summary",
                "benchmark_report",
                "regression_report",
                "cost_report",
                "rollout_decision",
            ],
        },
        "generatedAt": generated_at,
        "kind": "ImprovementSpec",
        "metadata": {
            "id": _required_str(plan, "improvement_spec_id"),
            "owner": {"role": "Eval Engineer", "team": "serp-platform"},
            "status": "draft",
        },
        "modelGovernance": dict(_required_mapping(plan, "model_governance")),
        "objective": {
            "optimizationDirection": "maximize",
            "primaryMetricAuthority": "executor-pinned-metric-definition-profile",
            "type": "benchmark-ratchet",
        },
        "operationId": _required_str(plan, "operation_id"),
        "registryResourceId": _required_str(plan, "registry_resource_id"),
        "registryResourceType": _required_resource_type(plan, "registry_resource_type"),
        "replay": dict(_required_mapping(plan, "replay_context")),
        "rollback": {
            "automatic": True,
            "policyRef": _required_str(plan, "rollback_policy_ref"),
            "revertTo": {
                "referenceRunId": baseline_run_id,
                "type": "last-validated-baseline",
            },
            "triggerConditions": [
                "mandatory_suite_below_floor",
                "policy_compliance_regression",
            ],
        },
        "scope": {
            "allowedComponents": ["reranker-profile-public-docs"],
            "forbiddenChanges": [
                "api-breaking-change",
                "policy-bypass",
                "manual-runtime-hotfix",
                "new-legacy-compatibility-layer",
            ],
            "kind": "bounded",
        },
        "selectedSuiteIds": selected_suite_ids,
        "status": "awaiting-executor-derived-metrics",
        "tenantId": _required_str(plan, "tenant_id"),
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


def _metric_families_for_suite(metric_compatibility: Mapping[str, Any], suite_id: str) -> list[str]:
    for requirement in _required_object_list(metric_compatibility, "requirements"):
        if _required_str(requirement, "suite_id") == suite_id:
            return _required_str_list(requirement, "metric_families")
    raise ValueError(f"metric_compatibility does not include suite {suite_id!r}")


def _metric_compatibility_requirement_pairs(
    metric_compatibility: Mapping[str, Any],
) -> list[tuple[str, list[str]]]:
    return [
        (
            _required_str(requirement, "suite_id"),
            _required_str_list(requirement, "metric_families"),
        )
        for requirement in _required_object_list(metric_compatibility, "requirements")
    ]


def _suite_result_metric_families(suite_result: Mapping[str, Any]) -> list[str]:
    suite_id = _required_str(suite_result, "suite_id")
    metric_compatibility = _required_metric_compatibility(
        _required_mapping(_required_mapping(suite_result, "metadata"), "metric_compatibility"),
        selected_suite_ids=MANDATORY_SERP_BENCHMARK_SUITES,
        contract_version_field="contract_version",
        matrix_uri_field="matrix_uri",
        matrix_sha256_field="matrix_sha256",
        matrix_version_id_field="matrix_version_id",
        suite_id_field="suite_id",
        metric_families_field="metric_families",
    )
    return _metric_families_for_suite(metric_compatibility, suite_id)


def _mandatory_metric_families() -> tuple[str, str, str, str]:
    return _BENCHMARK_METRIC_FAMILIES


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
        return CrawlResponse(status_code=599, headers={}, body=b"")


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
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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
        f"{root}/public-docs-crawl-state.json",
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
    response = _s3_client().get_object(
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
    _s3_client().put_object(
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


@lru_cache(maxsize=1)
def _s3_client() -> Any:
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise ValueError("boto3 is required for s3:// artifact paths") from exc
    return boto3.client(
        "s3",
        endpoint_url=_required_env(_ARTIFACT_S3_ENDPOINT_ENV),
        aws_access_key_id=_required_env(_ARTIFACT_S3_ACCESS_KEY_ENV),
        aws_secret_access_key=_required_env(_ARTIFACT_S3_SECRET_KEY_ENV),
        region_name=os.environ.get(_ARTIFACT_S3_REGION_ENV, "us-east-1"),
        config=Config(
            s3={
                "addressing_style": (
                    "path"
                    if os.environ.get(_ARTIFACT_S3_PATH_STYLE_ENV, "true").lower() != "false"
                    else "virtual"
                )
            }
        ),
    )


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


if __name__ == "__main__":
    raise SystemExit(main())
