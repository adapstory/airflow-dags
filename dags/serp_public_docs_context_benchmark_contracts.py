from __future__ import annotations

import json
import math
import os
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, uuid5

from dags.serp_eval_contracts import (
    build_evidence_artifact_paths,
    post_bc21_json,
    read_evidence_artifact,
    validate_internal_service_base_url,
    write_evidence_artifact,
)

CONTRACT_VERSION = "serp-public-docs-context-benchmark/v1"
DAG_ID = "serp_public_docs_context_benchmark"
SUITE_CODE = "PublicDocsGolden"
SUITE_ID = "public-docs-golden-v1"
PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES = (
    "retrieval",
    "answer-quality",
    "citation",
    "policy",
)
TENANT_ID = "00000000-0000-4000-a000-000000000001"
PUBLIC_CORPUS_ID = "00000000-0000-4000-a000-000000000201"
ACTOR_ID = "airflow-serp-public-docs-context-benchmark"
SCORING_ALGORITHM_VERSION = "arithmetic-mean-per-case-family/v1"
GITHUB_STATUS_CONTEXT = "serp/public-docs-context-benchmark"
GITHUB_REPOSITORY = "adapstory/adapstory-ai-lms"
_GITHUB_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_BENCHMARK_COMMAND_TIMEOUT_SECONDS = 600


def build_context_benchmark_plan(conf: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_conf(conf)
    generated_at = _normalized_datetime(conf.get("generated_at"))
    benchmark_root = Path(
        os.environ.get(
            "ADAPSTORY_SERP_CONTEXT_BENCHMARK_ROOT",
            "/opt/adapstory/adapstory-context-benchmark",
        )
    )
    cases_path = benchmark_root / "data" / "public-docs-golden-v1.jsonl"
    request_template_path = benchmark_root / "data" / "serp-request-template.example.json"
    for path in (cases_path, request_template_path):
        if not path.is_file():
            raise ValueError(f"vendored context benchmark asset is missing: {path.name}")
    benchmark_source_ref = _required_git_sha(
        os.environ.get("ADAPSTORY_SERP_CONTEXT_BENCHMARK_REF"),
        "ADAPSTORY_SERP_CONTEXT_BENCHMARK_REF",
    )
    search_base_url = validate_internal_service_base_url(
        _required_env("ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL"),
        "search_serve_base_url",
    )
    bc21_base_url = validate_internal_service_base_url(
        _required_env("ADAPSTORY_SERP_BC21_BASE_URL"),
        "bc21_base_url",
    )
    artifact_root = str(
        conf.get("artifact_root_path") or os.environ.get("ADAPSTORY_AIRFLOW_ARTIFACT_ROOT") or ""
    )
    if not artifact_root:
        raise ValueError("artifact_root_path is required")
    operation_id = (
        "serp-public-docs-context-benchmark-"
        + sha256(
            "|".join((generated_at, benchmark_source_ref, SUITE_ID)).encode("utf-8")
        ).hexdigest()[:32]
    )
    artifact_paths = build_evidence_artifact_paths(
        artifact_root,
        operation_id,
        (
            ("plan", "context-benchmark-plan.json"),
            ("report", "context-benchmark-report.json"),
            ("execution", "context-benchmark-execution.json"),
            ("bc21_receipts", "context-benchmark-bc21-receipts.json"),
            ("github_status", "context-benchmark-github-status.json"),
        ),
    )
    return {
        "actor_id": ACTOR_ID,
        "artifact_paths": artifact_paths,
        "bc21_base_url": bc21_base_url,
        "benchmark_root": str(benchmark_root),
        "benchmark_source_ref": benchmark_source_ref,
        "cases_path": str(cases_path),
        "contract_version": CONTRACT_VERSION,
        "dag_id": DAG_ID,
        "generated_at": generated_at,
        "github_repository": GITHUB_REPOSITORY,
        "operation_id": operation_id,
        "request_template_path": str(request_template_path),
        "resource_id": PUBLIC_CORPUS_ID,
        "resource_type": "public_corpus",
        "runner_version": f"adapstory-context-benchmark@{benchmark_source_ref}",
        "scoring_algorithm_version": SCORING_ALGORITHM_VERSION,
        "serp_url": search_base_url + "/api/serp/search/v1/query",
        "suite_code": SUITE_CODE,
        "suite_id": SUITE_ID,
        "suite_version": benchmark_source_ref,
        "tenant_id": TENANT_ID,
    }


def write_context_benchmark_plan(conf: Mapping[str, Any]) -> str:
    plan = build_context_benchmark_plan(conf)
    write_evidence_artifact(
        plan["artifact_paths"]["plan"],
        artifact_type="serp_public_docs_context_benchmark_plan",
        operation_id=plan["operation_id"],
        payload=plan,
    )
    return _canonical_json(plan)


def execute_context_benchmark(plan_json: Mapping[str, Any] | str) -> dict[str, Any]:
    plan = _plan(plan_json)
    report_path = plan["artifact_paths"]["report"]
    with TemporaryDirectory(prefix="serp-public-docs-context-benchmark-") as temp_dir:
        local_report_path = Path(temp_dir) / "report.json"
        command = [
            sys.executable,
            "-m",
            "adapstory_context_benchmark",
            "run",
            "--cases",
            plan["cases_path"],
            "--serp-url",
            plan["serp_url"],
            "--request-template",
            plan["request_template_path"],
            "--timeout-sec",
            "10",
            "--warmup-query",
            "K3s architecture documentation",
            "--warmup-timeout-sec",
            "20",
            "--suite-id",
            plan["suite_id"],
            "--suite-version",
            plan["suite_version"],
            "--output",
            str(local_report_path),
        ]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                check=False,
                text=True,
                timeout=_BENCHMARK_COMMAND_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return _write_execution_failure(plan, "process_error", str(exc))
        if completed.returncode != 0:
            return _write_execution_failure(plan, "nonzero_exit", completed.stderr)
        if not local_report_path.is_file():
            return _write_execution_failure(plan, "missing_report", completed.stdout)
        try:
            report = _json_object(local_report_path.read_text(encoding="utf-8"), "benchmark_report")
            gate_status = _validate_benchmark_report(plan, report)
        except (OSError, ValueError) as exc:
            return _write_execution_failure(plan, "invalid_report", str(exc))
    report_artifact = write_evidence_artifact(
        report_path,
        artifact_type="serp_public_docs_context_benchmark_report",
        operation_id=plan["operation_id"],
        payload=report,
    )
    execution = {
        "artifact_path": plan["artifact_paths"]["execution"],
        "contract_version": CONTRACT_VERSION,
        "gate_status": gate_status,
        "operation_id": plan["operation_id"],
        "report_artifact_path": report_path,
        "report_sha256": report_artifact["artifactSha256"],
        "status": "passed" if gate_status == "passed" else "blocked",
        "suite_code": SUITE_CODE,
        "tenant_id": TENANT_ID,
    }
    write_evidence_artifact(
        plan["artifact_paths"]["execution"],
        artifact_type="serp_public_docs_context_benchmark_execution",
        operation_id=plan["operation_id"],
        payload=execution,
    )
    return execution


def build_bc21_submission_payloads(
    plan_json: Mapping[str, Any] | str,
    report: Mapping[str, Any],
) -> list[dict[str, Any]]:
    plan = _plan(plan_json)
    _validate_benchmark_report(plan, report)
    candidates = _required_mapping(report, "candidates")
    serp = _required_mapping(candidates, "serp")
    case_metrics = _required_list(serp, "case_metrics")
    score_buckets: dict[str, dict[str, list[float]]] = {
        family: defaultdict(list) for family in PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES
    }
    for item in case_metrics:
        if not isinstance(item, Mapping):
            raise ValueError("case_metrics entries must be objects")
        metric = item
        family = _required_str(metric, "metric_family")
        if family not in score_buckets:
            continue
        case_id = _required_str(metric, "case_id")
        score = _unit_score(metric.get("score"), "case_metric.score")
        score_buckets[family][case_id].append(score)

    submissions: list[dict[str, Any]] = []
    expected_case_count = _required_case_count(serp)
    for family in PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES:
        cases = score_buckets[family]
        if len(cases) != expected_case_count:
            raise ValueError(f"{family} score-only case evidence is incomplete")
        scored_cases = [
            {
                "caseId": case_id,
                "expectedScore": 1.0,
                "observedScore": round(sum(scores) / len(scores), 6),
            }
            for case_id, scores in sorted(cases.items())
        ]
        body = {
            "actorId": plan["actor_id"],
            "cases": scored_cases,
            "metricFamily": family,
            "referenceSourceType": "official_baseline",
            "resourceId": plan["resource_id"],
            "resourceType": plan["resource_type"],
            "runnerVersion": plan["runner_version"],
            "scoringAlgorithmVersion": plan["scoring_algorithm_version"],
            "suiteCode": plan["suite_code"],
            "suiteVersion": plan["suite_version"],
        }
        fingerprint = "sha256:" + sha256(_canonical_json(body).encode("utf-8")).hexdigest()
        headers = {
            "X-Adapstory-Tenant-Id": plan["tenant_id"],
            "X-Adapstory-Trusted-Actor-Id": plan["actor_id"],
            "X-Adapstory-Trusted-Tenant-Id": plan["tenant_id"],
            "X-Fingerprint": fingerprint,
            "X-Idempotency-Key": str(
                uuid5(NAMESPACE_URL, f"{plan['operation_id']}|{family}|bc21-benchmark-run")
            ),
        }
        submissions.append({"body": body, "headers": headers})
    return submissions


def submit_context_benchmark_bc21_runs(
    plan_json: Mapping[str, Any] | str,
    execution: Mapping[str, Any],
) -> dict[str, Any]:
    plan = _plan(plan_json)
    if execution.get("status") == "execution_failed":
        return _write_bc21_receipts(plan, "not_submitted", [])
    try:
        report = read_evidence_artifact(execution["report_artifact_path"], "benchmark_report")
        submissions = build_bc21_submission_payloads(plan, report)
    except (KeyError, TypeError, ValueError) as exc:
        return _write_bc21_receipts(plan, "submission_preparation_failed", [], str(exc))

    receipts: list[dict[str, Any]] = []
    for submission in submissions:
        body = _required_mapping(submission, "body")
        try:
            response = post_bc21_json(
                plan["bc21_base_url"],
                "/api/bc-21/serp/v1/governance/benchmark-runs",
                body=body,
                headers=_string_mapping(_required_mapping(submission, "headers"), "headers"),
                error_label="public docs context benchmark BC-21 submission",
            )
        except ValueError as exc:
            return _write_bc21_receipts(plan, "submission_failed", receipts, str(exc))
        if response.get("suiteCode") != plan["suite_code"]:
            return _write_bc21_receipts(
                plan, "submission_failed", receipts, "unexpected suite response"
            )
        if response.get("metricFamily") != body["metricFamily"]:
            return _write_bc21_receipts(
                plan, "submission_failed", receipts, "unexpected metric response"
            )
        receipts.append(
            {
                "benchmark_result_id": response.get("benchmarkResultId"),
                "gate_status": response.get("gateStatus"),
                "metric_family": body["metricFamily"],
                "run_id": response.get("runId"),
            }
        )
    return _write_bc21_receipts(plan, "submitted", receipts)


def publish_context_benchmark_github_status(
    plan_json: Mapping[str, Any] | str,
    execution: Mapping[str, Any],
    bc21_receipts: Mapping[str, Any],
) -> dict[str, Any]:
    plan = _plan(plan_json)
    source_ref = plan["benchmark_source_ref"]
    repository = plan["github_repository"]
    if not _GITHUB_REPOSITORY_RE.fullmatch(repository):
        raise ValueError("GitHub repository is invalid")
    token = _required_env("ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN")
    success = execution.get("status") == "passed" and bc21_receipts.get("status") == "submitted"
    payload = {
        "context": GITHUB_STATUS_CONTEXT,
        "description": (
            "PublicDocsGolden passed; BC-21 evidence recorded"
            if success
            else "PublicDocsGolden failed; inspect in-cluster evidence"
        ),
        "state": "success" if success else "failure",
    }
    request = Request(
        f"https://api.github.com/repos/{repository}/statuses/{source_ref}",
        data=_canonical_json(payload).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=10.0) as response:
            response_payload = _json_object(
                response.read().decode("utf-8"), "github_commit_status_response"
            )
    except HTTPError as exc:
        raise ValueError(f"GitHub commit status publication failed: status={exc.code}") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise ValueError("GitHub commit status publication failed") from exc
    receipt = {
        "context": GITHUB_STATUS_CONTEXT,
        "contract_version": CONTRACT_VERSION,
        "operation_id": plan["operation_id"],
        "state": payload["state"],
        "status_id": response_payload.get("id"),
        "status_url": response_payload.get("url"),
    }
    write_evidence_artifact(
        plan["artifact_paths"]["github_status"],
        artifact_type="serp_public_docs_context_benchmark_github_status",
        operation_id=plan["operation_id"],
        payload=receipt,
    )
    return receipt


def enforce_context_benchmark_gate(
    execution: Mapping[str, Any],
    bc21_receipts: Mapping[str, Any],
    github_status: Mapping[str, Any],
) -> None:
    if execution.get("status") != "passed":
        raise ValueError("PublicDocsGolden benchmark gate is blocked or execution failed")
    if bc21_receipts.get("status") != "submitted":
        raise ValueError("PublicDocsGolden BC-21 evidence submission did not complete")
    if github_status.get("state") != "success":
        raise ValueError("PublicDocsGolden GitHub status publication did not succeed")


def _write_execution_failure(plan: Mapping[str, Any], reason: str, detail: str) -> dict[str, Any]:
    payload = {
        "artifact_path": plan["artifact_paths"]["execution"],
        "contract_version": CONTRACT_VERSION,
        "error_sha256": sha256(detail.encode("utf-8")).hexdigest(),
        "operation_id": plan["operation_id"],
        "reason": reason,
        "status": "execution_failed",
        "suite_code": SUITE_CODE,
        "tenant_id": TENANT_ID,
    }
    write_evidence_artifact(
        plan["artifact_paths"]["execution"],
        artifact_type="serp_public_docs_context_benchmark_execution",
        operation_id=plan["operation_id"],
        payload=payload,
    )
    return payload


def _write_bc21_receipts(
    plan: Mapping[str, Any],
    status: str,
    receipts: Sequence[Mapping[str, Any]],
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "operation_id": plan["operation_id"],
        "receipts": [dict(receipt) for receipt in receipts],
        "status": status,
        "suite_code": SUITE_CODE,
        "tenant_id": TENANT_ID,
    }
    if error is not None:
        payload["error_sha256"] = sha256(error.encode("utf-8")).hexdigest()
    write_evidence_artifact(
        plan["artifact_paths"]["bc21_receipts"],
        artifact_type="serp_public_docs_context_benchmark_bc21_receipts",
        operation_id=plan["operation_id"],
        payload=payload,
    )
    return payload


def _validate_benchmark_report(plan: Mapping[str, Any], report: Mapping[str, Any]) -> str:
    if _required_str(report, "suite_id") != plan["suite_id"]:
        raise ValueError("benchmark report suite_id does not match plan")
    if _required_str(report, "suite_version") != plan["suite_version"]:
        raise ValueError("benchmark report suite_version does not match plan")
    serp = _required_mapping(_required_mapping(report, "candidates"), "serp")
    if _required_case_count(serp) != 30:
        raise ValueError("public docs benchmark must include exactly 30 golden cases")
    gate_status = _required_str(serp, "status")
    if gate_status not in {"passed", "blocked"}:
        raise ValueError("SERP benchmark candidate status is invalid")
    _required_list(serp, "case_metrics")
    return gate_status


def _plan(value: Mapping[str, Any] | str) -> dict[str, Any]:
    plan = _json_object(value, "plan") if isinstance(value, str) else value
    required = (
        "actor_id",
        "bc21_base_url",
        "benchmark_source_ref",
        "cases_path",
        "github_repository",
        "operation_id",
        "request_template_path",
        "resource_id",
        "resource_type",
        "runner_version",
        "scoring_algorithm_version",
        "serp_url",
        "suite_code",
        "suite_id",
        "suite_version",
        "tenant_id",
    )
    result: dict[str, Any] = {key: _required_str(plan, key) for key in required}
    artifact_paths = _required_mapping(plan, "artifact_paths")
    result["artifact_paths"] = artifact_paths
    return result


def _reject_unknown_conf(conf: Mapping[str, Any]) -> None:
    allowed = {"artifact_root_path", "generated_at"}
    unknown = set(conf) - allowed
    if unknown:
        names = ", ".join(sorted(map(str, unknown)))
        raise ValueError(f"unsupported context benchmark config: {names}")


def _normalized_datetime(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("generated_at is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("generated_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("generated_at must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value


def _required_git_sha(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not _GIT_SHA_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a full lowercase Git SHA")
    return value


def _json_object(value: str, field_name: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return parsed


def _required_mapping(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    nested = value.get(field_name)
    if not isinstance(nested, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return nested


def _required_list(value: Mapping[str, Any], field_name: str) -> list[Any]:
    nested = value.get(field_name)
    if not isinstance(nested, list):
        raise ValueError(f"{field_name} must be a list")
    return nested


def _required_str(value: Mapping[str, Any], field_name: str) -> str:
    nested = value.get(field_name)
    if not isinstance(nested, str) or not nested.strip():
        raise ValueError(f"{field_name} is required")
    return nested


def _required_case_count(value: Mapping[str, Any]) -> int:
    case_count = value.get("case_count")
    if isinstance(case_count, bool) or not isinstance(case_count, int) or case_count <= 0:
        raise ValueError("case_count must be a positive integer")
    return case_count


def _unit_score(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{field_name} must be numeric")
    score = float(value)
    if not math.isfinite(score) or score < 0.0 or score > 1.0:
        raise ValueError(f"{field_name} must be between zero and one")
    return score


def _string_mapping(value: Mapping[str, Any], field_name: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, nested in value.items():
        if not isinstance(key, str) or not isinstance(nested, str):
            raise ValueError(f"{field_name} must contain string keys and values")
        result[key] = nested
    return result


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
