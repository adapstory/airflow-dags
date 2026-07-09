from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ElementTree
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from hashlib import sha256
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser
from uuid import NAMESPACE_URL, UUID, uuid5

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

_RESOURCE_TYPES = frozenset({"pack", "tenant", "workflow"})
_GATEWAY_CLI_CONTRACT_VERSION = "serp-airflow-gateway-cli-bridge/v1"
_PIPELINE_CLI_CONTRACT_VERSION = "serp-airflow-pipeline-cli-bridge/v1"
_AIRFLOW_ARTIFACT_CONTRACT_VERSION = "serp-airflow-artifact-writer/v1"
_EVAL_CONTRACT_VERSION = "2026.07.2"
_DRY_RUN_SUITE_VERSION = "dry-run@2026.07.2"
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
_PUBLIC_DOCS_DEFAULT_TENANT_ID = "00000000-0000-4000-a000-000000000001"
_PUBLIC_DOCS_DEFAULT_PACK_ID = "00000000-0000-4000-a000-000000000201"
_PUBLIC_DOCS_DEFAULT_PACK_VERSION_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
_PUBLIC_DOCS_DEFAULT_ACTOR_ID = "airflow-serp-public-docs-refresh"
_PUBLIC_DOCS_SEARCH_SERVE_SMOKE_ACTOR_ID = "00000000-0000-4000-a000-000000000202"
_PUBLIC_DOCS_DEFAULT_ARTIFACT_ROOT = "/var/opt/adapstory/serp-public-docs-refresh"
_PUBLIC_DOCS_STACK_INVENTORY_PATH = STACK_INVENTORY_SOURCE_PATH
_PUBLIC_DOCS_SITEMAP_FETCH_TIMEOUT_SECONDS = 8
_PUBLIC_DOCS_MAX_SITEMAP_INDEX_CHILDREN = 3
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
_BC21_BASE_URL_ENV = "ADAPSTORY_SERP_BC21_BASE_URL"
_PUBLIC_DOCS_DEFAULT_QDRANT_COLLECTION = "serp_vectors_dev"
_PUBLIC_DOCS_DEFAULT_OPENSEARCH_INDEX = "serp_lexical_dev"
_PUBLIC_DOCS_DEFAULT_NEO4J_DATABASE = "serp_graph_dev"
_ARTIFACT_S3_ENDPOINT_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT"
_ARTIFACT_S3_REGION_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION"
_ARTIFACT_S3_ACCESS_KEY_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY"
_ARTIFACT_S3_SECRET_KEY_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY"
_ARTIFACT_S3_PATH_STYLE_ENV = "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE"
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

PublicDocsSitemapFrontierDiscoverer = Callable[
    [str, Mapping[str, Any], int],
    Sequence[str],
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


def build_nightly_regression_plan(conf: Mapping[str, Any]) -> SerpDagPlan:
    payload = _payload(conf)
    _reject_raw_secrets(payload)
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
    max_benchmark_runs = _required_positive_int(payload, "max_benchmark_runs")
    rollback_policy_ref = _required_str(payload, "rollback_policy_ref")
    replay_context = _improvement_replay_context(payload, baseline_run_id, candidate_id)
    model_governance = _improvement_model_governance(payload)
    artifact_root_path = _required_artifact_root_path(payload)
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
                ("candidate_eval_report", "candidate-eval-report.json"),
                ("keep_discard_decision", "keep-discard-decision.json"),
                ("improvement_scoreboard", "improvement-scoreboard.json"),
            ),
        ),
        "baseline_run_id": baseline_run_id,
        "candidate_id": candidate_id,
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
                "run_targeted_benchmark_eval_harness",
                "decide_keep_or_discard_candidate",
                "publish_improvement_scoreboard",
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
    approval_run_id = _required_uuid(payload, "approval_run_id")
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
    seed_refresh_identity = _public_docs_seed_refresh_result_identity(seed_refresh_result_path)
    if seed_refresh_identity["tenant_id"] != str(tenant_id):
        raise ValueError("public_docs_seed_refresh_result identity must match tenant_id")
    if seed_refresh_identity["pack_id"] != str(pack_id):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_id")
    if seed_refresh_identity["pack_version_id"] != str(pack_version_id):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_version_id")
    activation_reason_code = _required_str(payload, "activation_reason_code")
    artifact_root_path = _required_artifact_root_path(payload)
    search_serve_base_url = _public_docs_search_serve_base_url(payload)
    operation_id = _operation_id(
        "serp-airflow-publish-signed-pack",
        tenant_id,
        registry_resource_type,
        registry_resource_id,
        pack_id,
        pack_version_id,
        generated_at,
        seed_refresh_result_path,
        approval_run_id,
        evidence_bundle_id,
        evidence_seal_hash,
        benchmark_gate_export_sha256,
    )
    plan_payload = {
        "activation_idempotency_key": str(activation_idempotency_key),
        "activation_reason_code": activation_reason_code,
        "actor_id": _required_str(payload, "actor_id"),
        "approval_run_id": str(approval_run_id),
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
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
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
    seeds = _public_docs_seed_registry(
        payload,
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
        index_mode,
        embedding_mode,
        bc21_base_url or "",
    )
    plan_payload = {
        "actor_id": _required_str(payload, "actor_id"),
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
        "dag_id": "serp_web_seed_crawl_refresh",
        "generated_at": generated_at,
        "embedding_mode": embedding_mode,
        "index_mode": index_mode,
        "neo4j_database": neo4j_database,
        "operation_id": operation_id,
        "opensearch_index": opensearch_index,
        "pack_id": str(pack_id),
        "pack_version_id": str(pack_version_id),
        "qdrant_collection": qdrant_collection,
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
                "notify_governance_eval_surfaces",
            )
        ),
        "tenant_id": str(tenant_id),
    }
    return SerpDagPlan(plan_payload)


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
            "public_docs_seed_refresh_result",
            "public_docs_publish_activation_trigger_conf",
        ),
    )
    seed_refresh_result_path = _artifact_path(
        "public_docs_seed_refresh_result_path",
        artifact_paths["public_docs_seed_refresh_result"],
    )
    seed_refresh_identity = _public_docs_seed_refresh_result_identity(seed_refresh_result_path)
    if seed_refresh_identity["tenant_id"] != _required_str(plan, "tenant_id"):
        raise ValueError("public_docs_seed_refresh_result identity must match tenant_id")
    if seed_refresh_identity["pack_id"] != _required_str(plan, "pack_id"):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_id")
    if seed_refresh_identity["pack_version_id"] != _required_str(plan, "pack_version_id"):
        raise ValueError("public_docs_seed_refresh_result identity must match pack_version_id")

    trigger_conf = {
        "activation_reason_code": "public-docs-d20-indexed",
        "actor_id": _required_str(plan, "actor_id"),
        "artifact_root_path": _required_str(plan, "artifact_root_path"),
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
        "registry_resource_id": _required_str(plan, "registry_resource_id"),
        "registry_resource_type": _required_str(plan, "registry_resource_type"),
        "tenant_id": _required_str(plan, "tenant_id"),
    }
    if bc21_base_url := plan.get("bc21_base_url"):
        trigger_conf["bc21_base_url"] = _required_bc21_base_url({"bc21_base_url": bc21_base_url})
    governance_required_fields = [
        "activation_idempotency_key",
        "approval_run_id",
        "benchmark_gate_export_sha256",
    ]
    if "bc21_base_url" not in trigger_conf:
        governance_required_fields.append("bc21_base_url")
    governance_required_fields.extend(
        [
            "evidence_bundle_id",
            "evidence_seal_hash",
        ]
    )
    payload = {
        "artifact_type": "public_docs_publish_activation_trigger_conf",
        "contract_version": _EVAL_CONTRACT_VERSION,
        "dag_id": "serp_web_seed_crawl_refresh",
        "d5_publish_target": "serp_publish_signed_pack",
        "generated_at": _required_datetime_string(plan, "generated_at"),
        "governance_required_fields": governance_required_fields,
        "operation_id": _required_str(plan, "operation_id"),
        "source_seed_refresh_result_path": seed_refresh_result_path,
        "status": "governance_inputs_required",
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
    bc21_pipeline_state: Any = importlib.import_module(
        "adapstory_serp_pipeline.registry.bc21_pipeline_state"
    )

    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_web_seed_crawl_refresh":
        raise ValueError("plan dag_id does not match public docs BC-21 pipeline-state submit")
    bc21_base_url = _required_bc21_base_url(plan)
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
    batch_evidence = _required_mapping(refresh_result, "batch_evidence")
    if _required_str(batch_evidence, "status") != "indexed":
        raise ValueError("public docs seed refresh must be indexed before BC-21 registration")
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
    return _artifact_result(
        artifact_path,
        artifact_type="public_docs_bc21_pipeline_state_receipt",
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
    if _required_str(activation_receipt, "packVersionId") != expected_pack_version_id:
        raise ValueError("publish activation receipt packVersionId must match plan")
    request_payload = _public_docs_search_serve_smoke_request(plan)
    endpoint = _public_docs_search_serve_base_url(plan) + "/api/serp/search/v1/query"
    response_payload = _post_json(endpoint, request_payload)
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


def write_nightly_suite_plan_artifact(
    plan_json: Mapping[str, Any] | str,
) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_nightly_regression_suite":
        raise ValueError("plan dag_id does not match nightly suite-plan writer")
    artifact_paths = _required_artifact_paths(plan, ("suite_plan",))
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
    if status != "ready_for_pipeline_cli_runner":
        raise ValueError("pipeline cli spec is not ready for execution")
    argv = _required_str_list(spec, "argv")
    if any(value in {";", "&&", "|"} for value in argv):
        raise ValueError("pipeline cli spec argv must not contain shell operators")
    input_paths = _required_str_list(spec, "input_paths")
    stdout_path = _artifact_path("stdout_path", _required_str(spec, "stdout_path"))
    with TemporaryDirectory(prefix="airflow-pipeline-artifacts-") as temp_dir:
        argv = _materialize_pipeline_cli_argv(
            argv,
            input_paths,
            stdout_path=stdout_path,
            temp_dir=temp_dir,
        )
        completed = subprocess.run(
            argv,
            capture_output=True,
            check=False,
            text=True,
        )
    if completed.returncode != 0:
        stderr_sha256 = sha256(completed.stderr.encode("utf-8")).hexdigest()
        raise ValueError(
            "pipeline cli execution failed: "
            f"task_id={_required_str(spec, 'task_id')} "
            f"returncode={completed.returncode} stderr_sha256={stderr_sha256}"
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
    if status == "indexed":
        return
    raise ValueError(
        "public docs seed refresh did not fully index: "
        f"status={status} indexed_count={batch_evidence.get('indexed_count')} "
        f"failed_count={batch_evidence.get('failed_count')}"
    )


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


def write_nightly_report_artifact(plan_json: str) -> dict[str, Any]:
    plan = _json_object(plan_json, "plan_json")
    _reject_raw_secrets(plan)
    if _required_str(plan, "dag_id") != "serp_nightly_regression_suite":
        raise ValueError("plan dag_id does not match nightly artifact writer")
    artifact_paths = _required_artifact_paths(plan, ("nightly_report",))
    payload = _nightly_report_payload(plan)
    artifact_path = artifact_paths["nightly_report"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="nightly_report",
        operation_id=_required_str(plan, "operation_id"),
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
            "candidate_eval_report",
            "keep_discard_decision",
            "improvement_scoreboard",
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


def write_improvement_candidate_eval_artifact(
    improvement_spec_artifact: Mapping[str, Any] | str,
) -> dict[str, Any]:
    spec_artifact = (
        write_improvement_spec_artifact(improvement_spec_artifact)
        if _is_plan_payload(improvement_spec_artifact)
        else improvement_spec_artifact
    )
    spec = _artifact_payload(spec_artifact, "improvement_spec")
    _required_artifact_paths(
        spec,
        ("candidate_eval_report", "keep_discard_decision", "improvement_scoreboard"),
    )
    artifact_paths = _required_mapping(spec, "artifact_paths")
    payload = _improvement_candidate_eval_payload(spec)
    _validate_improvement_candidate_payload(payload)
    artifact_path = artifact_paths["candidate_eval_report"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="candidate_eval_report",
        operation_id=_required_str(spec, "operationId"),
        payload=payload,
    )


def write_benchmark_improvement_decision_artifact(
    candidate_eval_artifact: Mapping[str, Any] | str,
) -> dict[str, Any]:
    candidate = _artifact_payload(candidate_eval_artifact, "candidate_eval_report")
    _required_artifact_paths(
        candidate,
        ("keep_discard_decision", "improvement_scoreboard"),
    )
    artifact_paths = _required_mapping(candidate, "artifact_paths")
    payload = _improvement_decision_payload(candidate)
    artifact_path = artifact_paths["keep_discard_decision"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="keep_discard_decision",
        operation_id=_required_str(candidate, "operationId"),
        payload=payload,
    )


def write_benchmark_improvement_scoreboard_artifact(
    decision_artifact: Mapping[str, Any] | str,
) -> dict[str, Any]:
    decision = _artifact_payload(decision_artifact, "keep_discard_decision")
    _required_artifact_paths(decision, ("improvement_scoreboard",))
    artifact_paths = _required_mapping(decision, "artifact_paths")
    payload = _improvement_scoreboard_payload(decision)
    artifact_path = artifact_paths["improvement_scoreboard"]
    _write_json_artifact(artifact_path, payload)
    return _artifact_result(
        artifact_path,
        artifact_type="improvement_scoreboard",
        operation_id=_required_str(decision, "operationId"),
        payload=payload,
    )


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


def build_improvement_candidate_eval_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_benchmark_improvement_wave",
        task_id="run_targeted_benchmark_eval_harness",
        command="benchmark-improvement-candidate-eval",
        input_path_keys=("airflow_plan", "improvement_spec"),
        output_path_key="candidate_eval_report",
        option_names=("--airflow-plan", "--improvement-spec"),
    )


def build_benchmark_improvement_decision_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_benchmark_improvement_wave",
        task_id="decide_keep_or_discard_candidate",
        command="benchmark-improvement-decision",
        input_path_keys=("airflow_plan", "improvement_spec", "candidate_eval_report"),
        output_path_key="keep_discard_decision",
        option_names=(
            "--airflow-plan",
            "--improvement-spec",
            "--candidate-eval-report",
        ),
    )


def build_benchmark_improvement_scoreboard_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_benchmark_improvement_wave",
        task_id="publish_improvement_scoreboard",
        command="benchmark-improvement-scoreboard",
        input_path_keys=(
            "airflow_plan",
            "candidate_eval_report",
            "keep_discard_decision",
        ),
        output_path_key="improvement_scoreboard",
        option_names=(
            "--airflow-plan",
            "--candidate-eval-report",
            "--keep-discard-decision",
        ),
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
        "--approval-run-id",
        _required_str(plan, "approval_run_id"),
        "--evidence-bundle-id",
        _required_str(plan, "evidence_bundle_id"),
        "--evidence-seal-hash",
        _required_sha256_prefixed(plan, "evidence_seal_hash"),
        "--activation-reason-code",
        _required_str(plan, "activation_reason_code"),
        "--benchmark-gate-export-sha256",
        _required_sha256_prefixed(plan, "benchmark_gate_export_sha256"),
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
        "auth_subject_type": "system",
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
    due_seeds, skipped_seed_refreshes = _public_docs_due_seed_selection(
        _required_object_list(plan, "seed_registry"),
        generated_at,
    )
    source_fetch_requests = [
        request for seed in due_seeds for request in _public_docs_source_fetch_requests(plan, seed)
    ]
    status = "ready_for_pipeline_dispatch" if source_fetch_requests else "no_due_sources"
    return {
        "artifact_paths": artifact_paths,
        "contract_version": _EVAL_CONTRACT_VERSION,
        "d4_dispatch_target": "serp_scan_parse_index",
        "dag_id": "serp_web_seed_crawl_refresh",
        "embedding_mode": _required_str(plan, "embedding_mode"),
        "generated_at": generated_at,
        "operation_id": _required_str(plan, "operation_id"),
        "pack_id": _required_str(plan, "pack_id"),
        "pack_version_id": _required_str(plan, "pack_version_id"),
        "index_mode": _required_str(plan, "index_mode"),
        "qdrant_collection": _required_str(plan, "qdrant_collection"),
        "opensearch_index": _required_str(plan, "opensearch_index"),
        "neo4j_database": _required_str(plan, "neo4j_database"),
        "seed_count": len(source_fetch_requests),
        "seed_registry_sha256": _required_str(plan, "seed_registry_sha256"),
        "skipped_seed_count": len(skipped_seed_refreshes),
        "skipped_seed_refreshes": skipped_seed_refreshes,
        "source_fetch_requests": source_fetch_requests,
        "status": status,
        "tenant_id": _required_str(plan, "tenant_id"),
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
            frontier_role="seed-root" if index == 0 else "sitemap-frontier",
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


def _nightly_report_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    artifact_paths = _required_artifact_paths(
        plan,
        (
            "airflow_plan",
            "suite_plan",
            "nightly_report",
            "benchmark_gate_export",
            "nightly_registry_submissions",
            "nightly_registry_receipts",
        ),
    )
    operation_id = _required_str(plan, "operation_id")
    generated_at = _required_datetime_string(plan, "generated_at")
    suites = _required_str_list(plan, "selected_suite_ids")
    suite_results = [_nightly_suite_result(plan, suite_id, generated_at) for suite_id in suites]
    return {
        "artifact_paths": artifact_paths,
        "contract_version": "serp-nightly-report-dry-run/v1",
        "dag_id": _required_str(plan, "dag_id"),
        "generated_at": generated_at,
        "normalized_gate_floor": SERP_NORMALIZED_GATE_FLOOR,
        "operation_id": operation_id,
        "pack_version_ids": list(_required_str_list(plan, "pack_version_ids")),
        "registry_resource_id": _required_str(plan, "registry_resource_id"),
        "registry_resource_type": _required_resource_type(plan, "registry_resource_type"),
        "reranker_profile_version": _required_str(plan, "reranker_profile_version"),
        "retrieval_profile_version": _required_str(plan, "retrieval_profile_version"),
        "selected_suite_ids": suites,
        "status": evaluate_nightly_regression_gate({"suite_results": suite_results})["status"],
        "suite_results": suite_results,
        "tenant_id": _required_str(plan, "tenant_id"),
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
        "suites": [
            _nightly_suite_plan_suite(
                suite_id,
                generated_at=generated_at,
                pack_version_ids=pack_version_ids,
                reranker_profile_version=reranker_profile_version,
                retrieval_profile_version=retrieval_profile_version,
                tenant_id=tenant_id,
            )
            for suite_id in selected_suite_ids
        ],
        "tenant_id": tenant_id,
    }


def _nightly_suite_plan_suite(
    suite_id: str,
    *,
    generated_at: str,
    pack_version_ids: Sequence[str],
    reranker_profile_version: str,
    retrieval_profile_version: str,
    tenant_id: str,
) -> dict[str, Any]:
    return {
        "cases": [
            {
                "query_id": f"{suite_id}:c1-live-query-001",
                "ranked_chunk_ids": [f"{suite_id}:chunk-a", f"{suite_id}:chunk-b"],
                "relevant_chunk_ids": [f"{suite_id}:chunk-a"],
            }
        ],
        "generated_at": generated_at,
        "metadata": {
            "suite_contract_version": _EVAL_CONTRACT_VERSION,
            "trigger": "airflow-nightly",
        },
        "metric_observations": [
            {
                "metric": "Faithfulness",
                "metric_family": "answer-quality",
                "score": 0.96,
            },
            {
                "metric": "Citation Accuracy",
                "metric_family": "citation",
                "score": 0.97,
            },
            {
                "metric": "Policy Compliance Rate",
                "metric_family": "policy",
                "score": 1.0,
            },
        ],
        "pack_version_ids": list(pack_version_ids),
        "references": [
            {
                "metric": "MRR@10",
                "metric_family": "retrieval",
                "reference_id": f"{suite_id}:mrr10-baseline",
                "reference_score": 1.0,
                "threshold": SERP_NORMALIZED_GATE_FLOOR,
            },
            {
                "metric": "Faithfulness",
                "metric_family": "answer-quality",
                "reference_id": f"{suite_id}:answer-quality-baseline",
                "reference_score": 1.0,
                "threshold": SERP_NORMALIZED_GATE_FLOOR,
            },
            {
                "metric": "Citation Accuracy",
                "metric_family": "citation",
                "reference_id": f"{suite_id}:citation-baseline",
                "reference_score": 1.0,
                "threshold": SERP_NORMALIZED_GATE_FLOOR,
            },
            {
                "metric": "Policy Compliance Rate",
                "metric_family": "policy",
                "reference_id": f"{suite_id}:policy-baseline",
                "reference_score": 1.0,
                "threshold": 1.0,
            },
        ],
        "reranker_profile_version": reranker_profile_version,
        "retrieval_profile_version": retrieval_profile_version,
        "suite_contract_version": _EVAL_CONTRACT_VERSION,
        "suite_id": suite_id,
        "suite_version": "golden@2026.07.2",
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
    if _required_str(suite, "suite_contract_version") != _EVAL_CONTRACT_VERSION:
        raise ValueError("unsupported suite_contract_version")
    query_ids = [_required_str(case, "query_id") for case in _required_object_list(suite, "cases")]
    metric_results = [
        _metric_result_from_reference(suite, reference)
        for reference in _required_object_list(suite, "references")
    ]
    if {metric["metric_family"] for metric in metric_results} != set(_mandatory_metric_families()):
        raise ValueError("suite references must include every mandatory metric family")
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
        "metadata": dict(_required_mapping(suite, "metadata")),
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
        for metric_family in _mandatory_metric_families()
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
        "metricFamily": metric_family,
        "referenceSourceType": "official_baseline",
        "resourceId": _required_str(report, "registry_resource_id"),
        "resourceType": _required_resource_type(report, "registry_resource_type"),
        "runnerVersion": "airflow-d6-serp-eval-runner@2026.07.2",
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
    body_bytes = _canonical_json(body).encode("utf-8")
    endpoint_path = _required_str(submission, "endpointPath")
    request = Request(
        base_url + endpoint_path,
        data=body_bytes,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Adapstory-Actor-Id": _required_str(submission, "trustedActorId"),
            "X-Adapstory-Tenant-Id": tenant_id,
            "X-Adapstory-Trusted-Actor-Id": _required_str(submission, "trustedActorId"),
            "X-Adapstory-Trusted-Tenant-Id": tenant_id,
            "X-Fingerprint": _required_str(submission, "fingerprint"),
            "X-Idempotency-Key": _required_str(submission, "idempotencyKey"),
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=5.0) as response:
            status_code = response.status
            response_body = response.read().decode("utf-8")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise ValueError(
            "benchmark registry submission failed for "
            f"{_required_str(submission, 'suiteCode')}/"
            f"{_required_str(submission, 'metricFamily')}"
        ) from exc
    response_payload = _json_object(response_body, "benchmark_registry_response")
    if status_code < 200 or status_code >= 300:
        raise ValueError(
            "benchmark registry submission failed: "
            f"status={status_code} response_sha256="
            f"{sha256(_canonical_json(response_payload).encode('utf-8')).hexdigest()}"
        )
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
            "X-Adapstory-Trusted-Actor-Id": actor_id,
            "X-Adapstory-Trusted-Tenant-Id": tenant_id,
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
    body_bytes = None if body is None else _canonical_json(body).encode("utf-8")
    request = Request(
        url,
        data=body_bytes,
        headers={
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if body is not None else {}),
            **{str(key): str(value) for key, value in headers.items()},
        },
        method=method,
    )
    try:
        with urlopen(request, timeout=5.0) as response:
            return dict(_json_object(response.read().decode("utf-8"), error_label))
    except HTTPError as exc:
        if allow_conflict and exc.code == 409:
            return None
        raise ValueError(f"{error_label} failed: status={exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ValueError(f"{error_label} failed") from exc


def _nightly_suite_result(
    plan: Mapping[str, Any], suite_id: str, generated_at: str
) -> dict[str, Any]:
    operation_id = _operation_id(
        "serp-airflow-nightly-suite-dry-run",
        _required_str(plan, "operation_id"),
        suite_id,
    )
    material = _canonical_json(
        {
            "generated_at": generated_at,
            "operation_id": operation_id,
            "suite_id": suite_id,
        }
    )
    suite_sha256 = sha256(material.encode("utf-8")).hexdigest()
    return {
        "generated_at": generated_at,
        "metric_results": [
            _dry_run_metric_result(suite_id, "Recall@10", "retrieval", suite_sha256),
            _dry_run_metric_result(suite_id, "nDCG@10", "retrieval", suite_sha256),
        ],
        "operation_id": operation_id,
        "operation_sha256": suite_sha256,
        "query_ids": [f"{suite_id}:dry-run-query-001"],
        "status": "passed",
        "suite_id": suite_id,
        "suite_version": _DRY_RUN_SUITE_VERSION,
    }


def _dry_run_metric_result(
    suite_id: str,
    metric: str,
    metric_family: str,
    suite_sha256: str,
) -> dict[str, Any]:
    return {
        "metric": metric,
        "metric_family": metric_family,
        "normalized_score": SERP_NORMALIZED_GATE_FLOOR,
        "reference_id": f"{suite_id}:{metric}:dry-run-floor",
        "reference_score": 1.0,
        "score": SERP_NORMALIZED_GATE_FLOOR,
        "status": "passed",
        "threshold": SERP_NORMALIZED_GATE_FLOOR,
        "trace_id": suite_sha256[:16],
    }


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
    payload: Mapping[str, Any], baseline_run_id: str, candidate_id: str
) -> dict[str, Any]:
    if isinstance(payload.get("replay_context"), Mapping):
        replay_context = dict(_required_mapping(payload, "replay_context"))
        if _required_str(replay_context, "baselineRunId") != baseline_run_id:
            raise ValueError("replay_context baselineRunId does not match baseline")
        if _required_str(replay_context, "candidateRunId") != f"{candidate_id}-dry-run":
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
        "candidateRunId": f"{candidate_id}-dry-run",
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


def _improvement_spec_payload(
    plan: Mapping[str, Any], artifact_paths: Mapping[str, str]
) -> dict[str, Any]:
    generated_at = _required_datetime_string(plan, "generated_at")
    baseline_run_id = _required_str(plan, "baseline_run_id")
    candidate_id = _required_str(plan, "candidate_id")
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
                "minimumLead": {"MRR@10": 0.01, "nDCG@10": 0.01},
                "rule": "primary_metrics_improve_without_blocking_regressions",
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
        "dryRun": True,
        "candidateEvaluation": {
            "baselineRunId": baseline_run_id,
            "candidateId": candidate_id,
            "candidateRunId": f"{candidate_id}-dry-run",
            "constraintResults": [
                {"name": "Policy Compliance Rate", "status": "passed"},
                {"name": "Citation Accuracy", "status": "passed"},
                {"name": "Evidence Completeness", "status": "passed"},
            ],
            "evidence": {
                "benchmarkReportId": f"benchmark-report-{candidate_id}",
                "candidateDiffSummaryId": f"diff-{candidate_id}",
                "costReportId": f"cost-report-{candidate_id}",
                "regressionReportId": f"regression-report-{candidate_id}",
            },
            "scope": {"changedComponents": ["reranker-profile-public-docs"]},
            "suiteResults": [
                _improvement_suite_result(suite_id, metric_family)
                for suite_id in selected_suite_ids
                for metric_family in _mandatory_metric_families()
            ],
        },
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
            "targetMetricFamily": {
                "primary": ["nDCG@10", "MRR@10"],
                "secondary": ["Recall@10", "Citation Accuracy"],
            },
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
        "status": "ready",
        "tenantId": _required_str(plan, "tenant_id"),
    }


def _improvement_candidate_eval_payload(spec: Mapping[str, Any]) -> dict[str, Any]:
    candidate = _required_mapping(spec, "candidateEvaluation")
    suite_results = _required_object_list(candidate, "suiteResults")
    _required_true(spec, "dryRun")
    return {
        "artifact_paths": _required_artifact_paths(
            spec,
            (
                "airflow_plan",
                "improvement_spec",
                "candidate_eval_report",
                "keep_discard_decision",
                "improvement_scoreboard",
            ),
        ),
        "baselineRunId": _required_str(candidate, "baselineRunId"),
        "candidateId": _required_str(candidate, "candidateId"),
        "candidateRunId": _required_str(candidate, "candidateRunId"),
        "candidateScore": _minimum_normalized_score(suite_results),
        "constraintResults": list(_required_object_list(candidate, "constraintResults")),
        "dryRun": True,
        "evidence": dict(_required_mapping(candidate, "evidence")),
        "generatedAt": _required_str(spec, "generatedAt"),
        "improvementSpecId": _required_str(_required_mapping(spec, "metadata"), "id"),
        "mandatoryMetricFamilyCount": len(_mandatory_metric_families()),
        "mandatorySuiteCount": len(MANDATORY_SERP_BENCHMARK_SUITES),
        "modelGovernance": dict(_required_mapping(spec, "modelGovernance")),
        "normalizedGateFloor": f"{SERP_NORMALIZED_GATE_FLOOR:.4f}",
        "operationId": _operation_id(
            "serp-airflow-improvement-candidate-eval",
            _required_str(spec, "operationId"),
            _required_str(candidate, "candidateId"),
        ),
        "replay": dict(_required_mapping(spec, "replay")),
        "rollbackPolicyRef": _required_str(_required_mapping(spec, "rollback"), "policyRef"),
        "scope": dict(_required_mapping(candidate, "scope")),
        "selectedSuiteIds": list(_required_str_list(spec, "selectedSuiteIds")),
        "status": "passed",
        "suiteResults": list(suite_results),
        "tenantId": _required_str(spec, "tenantId"),
    }


def _improvement_decision_payload(candidate: Mapping[str, Any]) -> dict[str, Any]:
    _validate_improvement_candidate_payload(candidate)
    if float(_required_str(candidate, "candidateScore")) < SERP_NORMALIZED_GATE_FLOOR:
        raise ValueError("improvement candidate score is below gate floor")
    if not _candidate_objective_improved(candidate):
        raise ValueError("improvement candidate does not beat baseline")
    return {
        "artifact_paths": _required_artifact_paths(
            candidate,
            (
                "airflow_plan",
                "improvement_spec",
                "candidate_eval_report",
                "keep_discard_decision",
                "improvement_scoreboard",
            ),
        ),
        "blockingFindings": [],
        "candidateId": _required_str(candidate, "candidateId"),
        "decision": "keep",
        "dryRun": True,
        "evidence": {
            "rolloutDecisionId": _operation_id(
                "serp-airflow-improvement-rollout-decision",
                _required_str(candidate, "operationId"),
            ),
            "scoreboardId": _operation_id(
                "serp-airflow-improvement-scoreboard",
                _required_str(candidate, "operationId"),
            ),
        },
        "improvementSpecId": _required_str(candidate, "improvementSpecId"),
        "latestCandidateScore": _required_str(candidate, "candidateScore"),
        "modelGovernance": dict(_required_mapping(candidate, "modelGovernance")),
        "objectiveImproved": True,
        "operationId": _operation_id(
            "serp-airflow-improvement-keep-discard",
            _required_str(candidate, "operationId"),
        ),
        "reason": "primary metrics improved and all blocking gates held",
        "replay": dict(_required_mapping(candidate, "replay")),
        "rollback": {
            "automatic": True,
            "policyRef": _required_str(candidate, "rollbackPolicyRef"),
            "revertTo": {
                "referenceRunId": _required_str(candidate, "baselineRunId"),
                "type": "last-validated-baseline",
            },
        },
        "status": "accepted",
        "tenantId": _required_str(candidate, "tenantId"),
    }


def _improvement_scoreboard_payload(decision: Mapping[str, Any]) -> dict[str, Any]:
    if _required_str(decision, "decision") != "keep":
        raise ValueError("improvement scoreboard only publishes accepted keep decisions")
    if _required_str(decision, "status") != "accepted":
        raise ValueError("improvement scoreboard requires an accepted decision")
    _required_true(decision, "dryRun")
    return {
        "artifact_paths": _required_artifact_paths(
            decision,
            (
                "airflow_plan",
                "improvement_spec",
                "candidate_eval_report",
                "keep_discard_decision",
                "improvement_scoreboard",
            ),
        ),
        "candidateId": _required_str(decision, "candidateId"),
        "dryRun": True,
        "improvementSpecId": _required_str(decision, "improvementSpecId"),
        "latestCandidateScore": _required_str(decision, "latestCandidateScore"),
        "latestDecision": _required_str(decision, "decision"),
        "modelGovernance": dict(_required_mapping(decision, "modelGovernance")),
        "operationId": _operation_id(
            "serp-airflow-improvement-scoreboard-publish",
            _required_str(decision, "operationId"),
        ),
        "publishedAt": _required_str(decision, "operationId"),
        "rolloutDecisionId": _required_str(
            _required_mapping(decision, "evidence"), "rolloutDecisionId"
        ),
        "replay": dict(_required_mapping(decision, "replay")),
        "status": "published",
        "tenantId": _required_str(decision, "tenantId"),
    }


def _improvement_suite_result(suite_code: str, metric_family: str) -> dict[str, Any]:
    return {
        "gateStatus": "passed",
        "metricFamily": metric_family,
        "metricResults": _improvement_metric_results(metric_family),
        "suiteCode": suite_code,
        "suiteVersion": _DRY_RUN_SUITE_VERSION,
    }


def _improvement_metric_results(metric_family: str) -> list[dict[str, str]]:
    if metric_family != "retrieval":
        return [
            {
                "baselineScore": "0.9600",
                "candidateScore": "0.9600",
                "metric": f"{metric_family}:golden",
                "metricFamily": metric_family,
                "normalizedScore": "0.9600",
            }
        ]
    return [
        {
            "baselineScore": "0.7800",
            "candidateScore": "0.8000",
            "metric": "nDCG@10",
            "metricFamily": metric_family,
            "normalizedScore": "0.8000",
        },
        {
            "baselineScore": "0.7700",
            "candidateScore": "0.7900",
            "metric": "MRR@10",
            "metricFamily": metric_family,
            "normalizedScore": "0.7900",
        },
    ]


def _validate_improvement_candidate_payload(candidate: Mapping[str, Any]) -> None:
    _required_true(candidate, "dryRun")
    replay = _required_mapping(candidate, "replay")
    if _required_str(candidate, "baselineRunId") != _required_str(replay, "baselineRunId"):
        raise ValueError("improvement candidate replay baseline mismatch")
    if _required_str(candidate, "candidateRunId") != _required_str(replay, "candidateRunId"):
        raise ValueError("improvement candidate replay candidate mismatch")
    _required_str_list(replay, "featureFlags")
    _required_str(replay, "guardrailBundleVersion")
    _required_str(replay, "judgeModelId")
    _required_str(replay, "judgeModelVersion")
    _required_str(replay, "judgePromptTemplateVersion")
    _required_str(replay, "modelCatalogEntryId")
    _required_str(replay, "policyBundleVersion")
    _required_str(replay, "providerRouteId")
    _required_str(replay, "rerankerProfileVersion")
    _required_str(replay, "retrievalProfileVersion")
    governance = _required_mapping(candidate, "modelGovernance")
    if _required_str(governance, "status") != "approved-for-eval-dry-run":
        raise ValueError("improvement candidate model governance is not approved")
    for field_name in (
        "guardrailBundleVersion",
        "judgeModelId",
        "judgeModelVersion",
        "modelCatalogEntryId",
        "policyBundleVersion",
        "providerRouteId",
    ):
        if _required_str(governance, field_name) != _required_str(replay, field_name):
            raise ValueError("improvement candidate governance replay mismatch")
    selected_suite_ids = tuple(_required_str_list(candidate, "selectedSuiteIds"))
    if selected_suite_ids != MANDATORY_SERP_BENCHMARK_SUITES:
        raise ValueError("improvement candidate must include every mandatory suite")
    by_cell: dict[tuple[str, str], Mapping[str, Any]] = {}
    for suite in _required_object_list(candidate, "suiteResults"):
        suite_code = _required_str(suite, "suiteCode")
        metric_family = _required_str(suite, "metricFamily")
        cell = (suite_code, metric_family)
        if cell in by_cell:
            raise ValueError(f"{suite_code}/{metric_family}: duplicate suite results")
        by_cell[cell] = suite
    for suite_code in selected_suite_ids:
        for metric_family in _mandatory_metric_families():
            suite_value = by_cell.get((suite_code, metric_family))
            if suite_value is None:
                raise ValueError(f"missing mandatory suite result {suite_code}/{metric_family}")
            suite_result: Mapping[str, Any] = suite_value
            if _required_str(suite_result, "gateStatus") != "passed":
                raise ValueError(f"{suite_code}/{metric_family}: gateStatus must be passed")
            for metric in _required_object_list(suite_result, "metricResults"):
                if _required_str(metric, "metricFamily") != metric_family:
                    raise ValueError(f"{suite_code}/{metric_family}: metricFamily mismatch")
                if (
                    _required_number_from_string(metric, "normalizedScore")
                    < SERP_NORMALIZED_GATE_FLOOR
                ):
                    raise ValueError("improvement candidate score is below gate floor")
    for constraint in _required_object_list(candidate, "constraintResults"):
        if _required_str(constraint, "status") != "passed":
            raise ValueError("improvement candidate constraint must pass")


def _candidate_objective_improved(candidate: Mapping[str, Any]) -> bool:
    for suite in _required_object_list(candidate, "suiteResults"):
        for metric in _required_object_list(suite, "metricResults"):
            metric_name = _required_str(metric, "metric")
            if metric_name not in {"nDCG@10", "MRR@10"}:
                continue
            baseline = _required_number_from_string(metric, "baselineScore")
            score = _required_number_from_string(metric, "candidateScore")
            if score - baseline >= 0.01:
                return True
    return False


def _minimum_normalized_score(suite_results: Sequence[Mapping[str, Any]]) -> str:
    score = min(
        _required_number_from_string(metric, "normalizedScore")
        for suite in suite_results
        for metric in _required_object_list(suite, "metricResults")
    )
    return f"{score:.4f}"


def _mandatory_metric_families() -> tuple[str, str, str, str]:
    return ("retrieval", "answer-quality", "citation", "policy")


def _public_docs_seed_registry(
    payload: Mapping[str, Any],
    *,
    sitemap_frontier_discoverer: PublicDocsSitemapFrontierDiscoverer | None = None,
) -> list[dict[str, Any]]:
    raw_seeds = _required_object_list(payload, "seed_registry")
    seeds = [
        _public_docs_seed(
            seed,
            sitemap_frontier_discoverer=sitemap_frontier_discoverer,
        )
        for seed in raw_seeds
    ]
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
    frontier_urls = _public_docs_frontier_urls(
        policy,
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
        "deny_patterns": list(deny_patterns),
        "frontier_urls": frontier_urls,
        "max_depth": max_depth,
        "max_pages": max_pages,
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
) -> list[str]:
    raw_urls = policy.get("frontier_urls", [])
    if raw_urls is None:
        raw_urls = []
    if not isinstance(raw_urls, list) or not all(isinstance(value, str) for value in raw_urls):
        raise ValueError("frontier_urls must be a list of strings")
    if source_type != "website":
        if raw_urls:
            raise ValueError("frontier_urls are supported only for website seeds")
        return []
    remaining_slots = max_pages - len(raw_urls) - 1
    if remaining_slots < 0:
        raise ValueError("frontier_urls must leave room for the seed source_uri")
    discovered_urls: Sequence[str] = ()
    if sitemap_discovery and sitemap_frontier_discoverer is not None and remaining_slots > 0:
        discovered_urls = sitemap_frontier_discoverer(
            source_uri,
            policy,
            remaining_slots,
        )
        if not all(isinstance(value, str) for value in discovered_urls):
            raise ValueError("sitemap frontier discoverer must return strings")
    candidate_urls = [*raw_urls, *discovered_urls]
    if len(candidate_urls) >= max_pages:
        raise ValueError("frontier_urls must leave room for the seed source_uri")
    allowed_domain_set = set(allowed_domains)
    source_host = urlparse(source_uri).hostname
    normalized: list[str] = []
    seen = {_canonical_public_docs_url(source_uri)}
    for value in candidate_urls:
        url = value.strip()
        if not url:
            raise ValueError("frontier_urls entries must be non-empty")
        if _contains_raw_secret(url):
            raise ValueError("frontier_urls must not contain raw secret material")
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("frontier_urls must use https")
        if parsed.hostname not in allowed_domain_set:
            raise ValueError("frontier_urls host must be in allowed_domains")
        if source_host and parsed.hostname != source_host:
            raise ValueError("frontier_urls host must match source_uri host")
        if _public_docs_url_matches_deny_patterns(url, deny_patterns):
            raise ValueError("frontier_urls must not match deny_patterns")
        canonical_url = _canonical_public_docs_url(url)
        if canonical_url in seen:
            continue
        seen.add(canonical_url)
        normalized.append(url)
    return normalized


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


def discover_public_docs_sitemap_frontier(
    source_uri: str,
    crawl_policy: Mapping[str, Any],
    max_urls: int,
) -> list[str]:
    if max_urls <= 0:
        return []
    parsed_source = urlparse(source_uri)
    if parsed_source.scheme not in {"http", "https"} or not parsed_source.hostname:
        return []
    allowed_domains = set(_required_str_list(crawl_policy, "allowed_domains"))
    deny_patterns = tuple(str(value) for value in crawl_policy.get("deny_patterns", ()))
    user_agent = _required_str(crawl_policy, "user_agent")
    robots_url = urljoin(source_uri, "/robots.txt")
    try:
        robots_payload = _fetch_public_docs_discovery_text(robots_url, user_agent)
    except (HTTPError, URLError, OSError, TimeoutError, ValueError):
        return []
    robot_parser = RobotFileParser(robots_url)
    robot_parser.parse(robots_payload.splitlines())
    sitemap_urls = tuple(robot_parser.site_maps() or ()) or (urljoin(source_uri, "/sitemap.xml"),)
    discovered: list[str] = []
    sitemap_queue = list(sitemap_urls)
    seen_sitemaps: set[str] = set()
    sitemap_index_children = 0
    while sitemap_queue and len(discovered) < max_urls:
        sitemap_url = sitemap_queue.pop(0).strip()
        sitemap_canonical = _canonical_public_docs_url(sitemap_url)
        if not sitemap_url or sitemap_canonical in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_canonical)
        if _contains_raw_secret(sitemap_url) or not robot_parser.can_fetch(user_agent, sitemap_url):
            continue
        try:
            sitemap_payload = _fetch_public_docs_discovery_text(sitemap_url, user_agent)
            sitemap_kind, locations = _public_docs_sitemap_locations(sitemap_payload)
        except (HTTPError, URLError, OSError, TimeoutError, ValueError, ElementTree.ParseError):
            continue
        if sitemap_kind == "sitemapindex":
            for location in locations:
                if sitemap_index_children >= _PUBLIC_DOCS_MAX_SITEMAP_INDEX_CHILDREN:
                    break
                sitemap_index_children += 1
                sitemap_queue.append(location)
            continue
        for location in locations:
            if len(discovered) >= max_urls:
                break
            if not _public_docs_discovered_url_is_allowed(
                location,
                allowed_domains=allowed_domains,
                deny_patterns=deny_patterns,
                source_host=parsed_source.hostname,
            ):
                continue
            if _contains_raw_secret(location) or not robot_parser.can_fetch(user_agent, location):
                continue
            discovered.append(location)
    return discovered


def _public_docs_discovered_url_is_allowed(
    url: str,
    *,
    allowed_domains: set[str],
    deny_patterns: Sequence[str],
    source_host: str,
) -> bool:
    parsed = urlparse(url)
    return (
        parsed.scheme == "https"
        and parsed.hostname == source_host
        and parsed.hostname in allowed_domains
        and not _contains_raw_secret(url)
        and not _public_docs_url_matches_deny_patterns(url, deny_patterns)
    )


def _fetch_public_docs_discovery_text(url: str, user_agent: str) -> str:
    request = Request(
        url,
        headers={
            "Accept": "application/xml,text/xml,text/plain,*/*",
            "User-Agent": user_agent,
        },
    )
    with urlopen(request, timeout=_PUBLIC_DOCS_SITEMAP_FETCH_TIMEOUT_SECONDS) as response:
        payload = cast(bytes, response.read(1_000_001))
    if len(payload) > 1_000_000:
        raise ValueError("public docs discovery payload is too large")
    return payload.decode("utf-8", errors="replace")


def _post_json(url: str, payload: Mapping[str, Any]) -> dict[str, Any]:
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
    with urlopen(request, timeout=_PUBLIC_DOCS_SITEMAP_FETCH_TIMEOUT_SECONDS) as response:
        response_payload = response.read(2_000_001)
    if len(response_payload) > 2_000_000:
        raise ValueError("JSON response payload is too large")
    decoded = json.loads(response_payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("JSON response payload must be an object")
    return decoded


def _public_docs_sitemap_locations(payload: str) -> tuple[str, list[str]]:
    root = ElementTree.fromstring(payload)
    root_name = _xml_local_name(root.tag)
    if root_name not in {"urlset", "sitemapindex"}:
        raise ValueError("public docs sitemap root is unsupported")
    locations: list[str] = []
    for element in root.iter():
        if _xml_local_name(element.tag) != "loc":
            continue
        value = (element.text or "").strip()
        if value:
            locations.append(value)
    return root_name, locations


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", maxsplit=1)[-1]


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
    return result


def _public_docs_due_seed_selection(
    seeds: Sequence[Mapping[str, Any]],
    generated_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    generated_at_dt = _datetime_value(generated_at, "generated_at")
    due_seeds: list[dict[str, Any]] = []
    skipped_seed_refreshes: list[dict[str, Any]] = []
    for seed in seeds:
        decision = _public_docs_seed_refresh_decision(seed, generated_at_dt)
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
) -> dict[str, str]:
    freshness_state = _required_mapping(seed, "freshness_state")
    last_success_at = freshness_state.get("last_success_at")
    refresh_policy = _required_mapping(seed, "refresh_policy")
    max_age_hours = _required_positive_int(refresh_policy, "max_age_hours")
    base = {
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "max_age_hours": str(max_age_hours),
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
            normalized_key = str(key).lower().replace("-", "_")
            if normalized_key in _RAW_SECRET_KEYS or any(
                normalized_key.endswith(f"_{secret_key}") for secret_key in _RAW_SECRET_KEYS
            ):
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
) -> list[str]:
    materialized = _materialize_gateway_cli_argv(argv, input_paths, temp_dir=temp_dir)
    stdout_artifact = _artifact_ref("stdout_path", stdout_path)
    if stdout_artifact.kind == "s3":
        local_stdout_path = (
            Path(temp_dir) / "stdout" / Path(_required_str_ref(stdout_artifact.key)).name
        )
        local_stdout_path.parent.mkdir(parents=True, exist_ok=True)
        for output_option in ("--evidence-output", "--activation-receipt-output"):
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


def _required_str_ref(value: str | None) -> str:
    if value is None or not value.strip():
        raise ValueError("artifact reference is incomplete")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
