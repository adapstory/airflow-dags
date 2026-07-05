from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse
from uuid import UUID, uuid5

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

_RESOURCE_TYPES = frozenset({"pack", "tenant", "workflow"})
_GATEWAY_CLI_CONTRACT_VERSION = "serp-airflow-gateway-cli-bridge/v1"
_AIRFLOW_ARTIFACT_CONTRACT_VERSION = "serp-airflow-artifact-writer/v1"
_DRY_RUN_SUITE_VERSION = "dry-run@2026.07.1"
_BENCHMARK_NAMESPACE = UUID("018f5e13-2d73-7a77-a052-8d1bcbf96599")
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


@dataclass(frozen=True, slots=True)
class SerpDagPlan:
    payload: dict[str, Any]

    def to_canonical_json(self) -> str:
        return _canonical_json(self.payload)

    def operation_sha256(self) -> str:
        return sha256(self.to_canonical_json().encode("utf-8")).hexdigest()


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
        "retrieval_profile_version": _required_str(
            payload, "retrieval_profile_version"
        ),
        "selected_suite_ids": list(selected_suite_ids),
        "tasks": _tasks(
            (
                "validate_nightly_regression_plan",
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
    changed_pack_version_ids = tuple(
        _required_uuid_list(payload, "changed_pack_version_ids")
    )
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
        "normalized_gate_floor": SERP_NORMALIZED_GATE_FLOOR,
        "operation_id": operation_id,
        "registry_resource_id": str(registry_resource_id),
        "registry_resource_type": registry_resource_type,
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


def write_airflow_plan_artifact(plan: SerpDagPlan) -> str:
    plan_json = plan.to_canonical_json()
    artifact_paths = _required_artifact_paths(
        plan.payload,
        ("airflow_plan",),
    )
    airflow_plan_path = Path(artifact_paths["airflow_plan"])
    airflow_plan_path.parent.mkdir(parents=True, exist_ok=True)
    airflow_plan_path.write_text(plan_json, encoding="utf-8")
    return plan_json


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
    export_payload = _artifact_payload(
        benchmark_export_artifact, "benchmark_gate_export"
    )
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
    submissions = _artifact_payload(
        registry_submissions_artifact, "nightly_registry_submissions"
    )
    artifact_paths = _required_artifact_paths(
        submissions, ("nightly_registry_receipts",)
    )
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
        extra_options=("--bc21-base-url", _required_bc21_base_url(_json_object(plan_json, "plan_json"))),
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
        option_names=("--airflow-plan", "--improvement-spec", "--candidate-eval-report"),
    )


def build_benchmark_improvement_scoreboard_cli_spec(plan_json: str) -> dict[str, Any]:
    return _gateway_cli_spec(
        plan_json,
        dag_id="serp_benchmark_improvement_wave",
        task_id="publish_improvement_scoreboard",
        command="benchmark-improvement-scoreboard",
        input_path_keys=("airflow_plan", "candidate_eval_report", "keep_discard_decision"),
        output_path_key="improvement_scoreboard",
        option_names=("--airflow-plan", "--candidate-eval-report", "--keep-discard-decision"),
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
    suite_results = [
        _nightly_suite_result(plan, suite_id, generated_at) for suite_id in suites
    ]
    return {
        "artifact_paths": artifact_paths,
        "contract_version": "serp-nightly-report-dry-run/v1",
        "dag_id": _required_str(plan, "dag_id"),
        "generated_at": generated_at,
        "normalized_gate_floor": SERP_NORMALIZED_GATE_FLOOR,
        "operation_id": operation_id,
        "pack_version_ids": list(_required_str_list(plan, "pack_version_ids")),
        "registry_resource_id": _required_str(plan, "registry_resource_id"),
        "registry_resource_type": _required_resource_type(
            plan, "registry_resource_type"
        ),
        "reranker_profile_version": _required_str(plan, "reranker_profile_version"),
        "retrieval_profile_version": _required_str(plan, "retrieval_profile_version"),
        "selected_suite_ids": suites,
        "status": evaluate_nightly_regression_gate(
            {"suite_results": suite_results}
        )["status"],
        "suite_results": suite_results,
        "tenant_id": _required_str(plan, "tenant_id"),
    }


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
                "registryResourceType": _required_resource_type(
                    report, "registry_resource_type"
                ),
                "runId": operation_id,
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
    nested_payload = payload.get("payload")
    if not isinstance(nested_payload, Mapping):
        raise ValueError("artifact payload is required")
    _reject_raw_secrets(nested_payload)
    return nested_payload


def _write_json_artifact(artifact_path: str, payload: Mapping[str, Any]) -> None:
    path = Path(_artifact_path("artifact_path", artifact_path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_canonical_json(payload), encoding="utf-8")


def _tasks(task_ids: Sequence[str]) -> list[dict[str, int | str]]:
    return [
        {"order": index + 1, "task_id": task_id}
        for index, task_id in enumerate(task_ids)
    ]


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


def _json_object(value: str, field_name: str) -> Mapping[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if not isinstance(loaded, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    return loaded


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _required_object_list(
    payload: Mapping[str, Any], field_name: str
) -> list[Mapping[str, Any]]:
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
    value = _required_str(payload, "bc21_base_url")
    if "://" not in value or "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError("bc21_base_url must be an absolute single-line URL")
    if _contains_raw_secret(value):
        raise ValueError("bc21_base_url must not contain raw secret material")
    parsed = urlparse(value)
    if parsed.scheme == "https" and parsed.hostname:
        return value
    if parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}:
        return value
    if parsed.scheme == "http" and _is_kubernetes_service_host(parsed.hostname):
        return value
    raise ValueError(
        "bc21_base_url must use https, localhost http, or Kubernetes service http"
    )


def _is_kubernetes_service_host(hostname: str | None) -> bool:
    return hostname is not None and (
        hostname.endswith(".svc") or hostname.endswith(".svc.cluster.local")
    )


def _required_datetime_string(payload: Mapping[str, Any], field_name: str) -> str:
    value = _required_str(payload, field_name)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def _required_str(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} is required")
    _require_non_empty(field_name, value)
    return value


def _required_number(payload: Mapping[str, Any], field_name: str) -> float:
    value = payload.get(field_name)
    if (
        not isinstance(value, int | float)
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        raise ValueError(f"{field_name} must be numeric")
    return float(value)


def _required_positive_int(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _required_artifact_root_path(payload: Mapping[str, Any]) -> str:
    return _artifact_path(
        "artifact_root_path", _required_str(payload, "artifact_root_path")
    )


def _artifact_paths(
    artifact_root_path: str,
    operation_id: str,
    filenames: Sequence[tuple[str, str]],
) -> dict[str, str]:
    root = _artifact_path("artifact_root_path", artifact_root_path).rstrip("/")
    operation_path = f"{root}/{operation_id}"
    return {
        key: _artifact_path(key, f"{operation_path}/{filename}")
        for key, filename in filenames
    }


def _required_artifact_paths(
    payload: Mapping[str, Any],
    required_keys: Sequence[str],
) -> dict[str, str]:
    value = payload.get("artifact_paths")
    if not isinstance(value, Mapping):
        raise ValueError("artifact_paths is required")
    return {
        key: _artifact_path(key, _required_str(value, key)) for key in required_keys
    }


def _artifact_path(field_name: str, value: str) -> str:
    _require_non_empty(field_name, value)
    if "://" in value or not value.startswith("/"):
        raise ValueError(f"{field_name} must be an absolute path")
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be a single-line absolute path")
    if ".." in PurePosixPath(value).parts:
        raise ValueError(f"{field_name} must not contain parent traversal")
    if _contains_raw_secret(value):
        raise ValueError(f"{field_name} must not contain raw secret material")
    return value


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
                normalized_key.endswith(f"_{secret_key}")
                for secret_key in _RAW_SECRET_KEYS
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
