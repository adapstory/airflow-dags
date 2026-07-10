from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from urllib.request import Request

import pytest

import dags.serp_public_docs_context_benchmark_contracts as benchmark_contracts
from dags.serp_public_docs_context_benchmark_contracts import (
    PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES,
    build_bc21_submission_payloads,
    build_context_benchmark_plan,
    execute_context_benchmark,
    publish_context_benchmark_github_status,
    submit_context_benchmark_bc21_runs,
)


def _benchmark_root(tmp_path: Path) -> Path:
    benchmark_root = tmp_path / "context-benchmark"
    data = benchmark_root / "data"
    data.mkdir(parents=True)
    (data / "public-docs-golden-v1.jsonl").write_text("{}\n", encoding="utf-8")
    (data / "serp-request-template.example.json").write_text("{}\n", encoding="utf-8")
    return benchmark_root


def test_plan_binds_the_vendored_benchmark_to_internal_services_and_immutable_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_root = _benchmark_root(tmp_path)
    source_sha = "a" * 40
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_ROOT", str(benchmark_root))
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_REF", source_sha)
    monkeypatch.setenv(
        "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL",
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000",
    )
    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )

    plan = build_context_benchmark_plan(
        {
            "artifact_root_path": str(tmp_path / "evidence"),
            "generated_at": "2026-07-11T03:15:00Z",
        }
    )

    assert plan["serp_url"] == (
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
        "/api/serp/search/v1/query"
    )
    assert plan["bc21_base_url"] == (
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080"
    )
    assert plan["benchmark_source_ref"] == source_sha
    assert plan["suite_code"] == "PublicDocsGolden"
    assert plan["artifact_paths"]["report"].endswith("context-benchmark-report.json")
    assert "competitor" not in plan


def test_bc21_submissions_average_duplicate_metric_observations_per_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_root = _benchmark_root(tmp_path)
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_ROOT", str(benchmark_root))
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_REF", "b" * 40)
    monkeypatch.setenv(
        "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL",
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000",
    )
    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    plan = build_context_benchmark_plan(
        {
            "artifact_root_path": str(tmp_path / "evidence"),
            "generated_at": "2026-07-11T03:15:00Z",
        }
    )
    case_ids = ["k3s-1", *[f"case-{index}" for index in range(2, 31)]]
    case_metrics = [
        {"case_id": case_id, "metric_family": family, "score": 1.0}
        for case_id in case_ids
        for family in PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES
    ]
    case_metrics[0]["score"] = 0.5
    case_metrics.append({"case_id": "k3s-1", "metric_family": "retrieval", "score": 1.0})
    report = {
        "suite_id": "public-docs-golden-v1",
        "suite_version": plan["suite_version"],
        "candidates": {
            "serp": {"case_metrics": case_metrics, "case_count": 30, "status": "passed"}
        },
    }

    submissions = build_bc21_submission_payloads(plan, report)

    assert [item["body"]["metricFamily"] for item in submissions] == list(
        PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES
    )
    retrieval = submissions[0]
    assert retrieval["body"]["suiteCode"] == "PublicDocsGolden"
    assert retrieval["body"]["resourceType"] == "public_corpus"
    assert retrieval["body"]["referenceSourceType"] == "official_baseline"
    assert len(retrieval["body"]["cases"]) == 30
    assert next(
        item for item in retrieval["body"]["cases"] if item["caseId"] == "k3s-1"
    ) == {"caseId": "k3s-1", "expectedScore": 1.0, "observedScore": 0.75}
    assert retrieval["headers"]["X-Fingerprint"].startswith("sha256:")


def test_execution_persists_report_then_submits_the_four_bc21_metric_families(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_root = _benchmark_root(tmp_path)
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_ROOT", str(benchmark_root))
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_REF", "c" * 40)
    monkeypatch.setenv(
        "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL",
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000",
    )
    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    plan = build_context_benchmark_plan(
        {
            "artifact_root_path": str(tmp_path / "evidence"),
            "generated_at": "2026-07-11T03:15:00Z",
        }
    )
    case_metrics = [
        {"case_id": f"case-{index}", "metric_family": family, "score": 1.0}
        for index in range(1, 31)
        for family in PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES
    ]

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        output = Path(command[command.index("--output") + 1])
        output.write_text(
            json.dumps(
                {
                    "suite_id": "public-docs-golden-v1",
                    "suite_version": plan["suite_version"],
                    "candidates": {
                        "serp": {
                            "case_count": 30,
                            "case_metrics": case_metrics,
                            "status": "passed",
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    posted_metric_families: list[str] = []

    def fake_post_bc21_json(
        _base_url: str,
        _path: str,
        *,
        body: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, str]:
        metric_family = str(body["metricFamily"])
        posted_metric_families.append(metric_family)
        return {
            "benchmarkResultId": f"result-{metric_family}",
            "gateStatus": "passed",
            "metricFamily": metric_family,
            "runId": f"run-{metric_family}",
            "suiteCode": "PublicDocsGolden",
        }

    monkeypatch.setattr(
        "dags.serp_public_docs_context_benchmark_contracts.subprocess.run", fake_run
    )
    monkeypatch.setattr(benchmark_contracts, "post_bc21_json", fake_post_bc21_json)

    execution = execute_context_benchmark(plan)
    receipts = submit_context_benchmark_bc21_runs(plan, execution)

    assert execution["status"] == "passed"
    assert receipts["status"] == "submitted"
    assert posted_metric_families == list(PUBLIC_DOCS_BENCHMARK_METRIC_FAMILIES)


def test_github_status_is_a_final_external_projection_of_in_cluster_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_root = _benchmark_root(tmp_path)
    source_sha = "d" * 40
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_ROOT", str(benchmark_root))
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_REF", source_sha)
    monkeypatch.setenv(
        "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL",
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000",
    )
    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    monkeypatch.setenv("ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN", "test-token")
    plan = build_context_benchmark_plan(
        {
            "artifact_root_path": str(tmp_path / "evidence"),
            "generated_at": "2026-07-11T03:15:00Z",
        }
    )
    captured: dict[str, object] = {}

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"id": 12, "url": "https://api.github.com/statuses/12"}'

    def fake_urlopen(request: object, *, timeout: float) -> Response:
        captured["request"] = request
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(
        "dags.serp_public_docs_context_benchmark_contracts.urlopen", fake_urlopen
    )

    receipt = publish_context_benchmark_github_status(
        plan,
        {"status": "passed"},
        {"status": "submitted"},
    )

    request = cast(Request, captured["request"])
    assert request.full_url.endswith(f"/statuses/{source_sha}")
    assert json.loads(cast(bytes, request.data)) == {
        "context": "serp/public-docs-context-benchmark",
        "description": "PublicDocsGolden passed; BC-21 evidence recorded",
        "state": "success",
    }
    assert request.get_header("Authorization") == "Bearer test-token"
    assert receipt["state"] == "success"
