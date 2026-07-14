from __future__ import annotations

import ast
import importlib
import io
import json
import sys
import types
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from email.message import Message
from hashlib import sha256
from pathlib import Path
from threading import Barrier
from typing import Any, cast
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request
from uuid import UUID

import pytest

import dags.serp_eval_contracts as serp_eval_contracts_module
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    SERP_NORMALIZED_GATE_FLOOR,
    _fetch_public_docs_crawler_response,
    build_benchmark_improvement_wave_plan,
    build_mandatory_benchmark_dataset_evidence_plan,
    build_nightly_benchmark_export_cli_spec,
    build_nightly_registry_cli_spec,
    build_nightly_registry_submit_cli_spec,
    build_nightly_regression_plan,
    build_nightly_runner_cli_spec,
    build_online_eval_registry_cli_spec,
    build_online_eval_rollup_cli_spec,
    build_online_eval_rollup_plan,
    build_public_docs_publish_activation_cli_spec,
    build_public_docs_publish_activation_plan,
    build_public_docs_publish_activation_submit_cli_spec,
    build_public_docs_retired_pack_cleanup_cli_spec,
    build_public_docs_seed_refresh_plan,
    build_tenant_golden_registry_cli_spec,
    build_tenant_golden_regression_plan,
    build_tenant_golden_runner_cli_spec,
    default_nightly_regression_conf,
    default_public_docs_seed_refresh_conf,
    discover_public_docs_crawler_frontier,
    dispatch_public_docs_seed_refresh_handoff,
    evaluate_nightly_regression_gate,
    evaluate_tenant_golden_gate,
    execute_gateway_cli_spec,
    execute_pipeline_cli_spec,
    load_materialized_benchmark_catalog_snapshot,
    load_public_docs_crawl_state_conf,
    materialize_live_benchmark_catalog_artifact,
    submit_public_docs_bc21_pipeline_state_artifact,
    write_airflow_plan_artifact,
    write_improvement_spec_artifact,
    write_nightly_suite_plan_artifact,
    write_online_eval_rollup_plan_artifact,
    write_paired_eval_request_artifact,
    write_public_docs_coverage_proof_artifact,
    write_public_docs_crawl_state_artifact,
    write_public_docs_post_activation_rollback_artifact,
    write_public_docs_publish_activation_trigger_conf_artifact,
    write_public_docs_retrieval_golden_artifact,
    write_public_docs_search_serve_smoke_artifact,
    write_public_docs_seed_refresh_plan_artifact,
    write_public_docs_seed_registry_artifact,
)
from dags.serp_public_docs_seed_catalog import (
    P0_PUBLIC_DOCS_SOURCES,
    PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH,
    STACK_INVENTORY_SOURCE_PATH,
    p0_public_docs_sources,
)

TENANT_ID = "00000000-0000-4000-a000-000000000001"
PACK_ID = "00000000-0000-4000-a000-000000000201"
PACK_VERSION_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
REGISTRY_RESOURCE_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
REPO_ROOT = Path(__file__).resolve().parents[1]


def test_public_docs_crawler_preserves_http_status_when_error_body_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TimedOutErrorBody(io.BytesIO):
        def read(self, _size: int | None = -1) -> bytes:
            raise TimeoutError("error body timed out")

    def fake_urlopen(_request: object, *, timeout: int) -> object:
        assert timeout > 0
        response_headers = Message()
        response_headers["Content-Type"] = "text/plain"
        raise HTTPError(
            "https://docs.example.com/robots.txt",
            404,
            "Not Found",
            response_headers,
            TimedOutErrorBody(),
        )

    monkeypatch.setattr("dags.serp_eval_contracts.urlopen", fake_urlopen)

    response = _fetch_public_docs_crawler_response(
        "https://docs.example.com/robots.txt", {"User-Agent": "serp-test/1"}
    )

    assert response.status_code == 404
    assert response.body == b""


def test_bc21_request_uses_the_projected_service_account_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    token_path = tmp_path / "service-account.token"
    token_path.write_text("projected-workload-jwt\n", encoding="utf-8")
    observed: dict[str, object] = {}

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b"{}"

    def fake_urlopen(request: Request, *, timeout: float) -> Response:
        observed["authorization"] = request.get_header("Authorization")
        observed["timeout"] = timeout
        return Response()

    monkeypatch.setenv("ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH", str(token_path))
    monkeypatch.setattr(serp_eval_contracts_module, "urlopen", fake_urlopen)

    assert (
        serp_eval_contracts_module._bc21_json_request(
            "http://bc21.test/api/bc-21/serp/v1/runs/pipeline-state",
            method="POST",
            body={"status": "indexed"},
            headers={"X-Adapstory-Tenant-Id": TENANT_ID},
            error_label="test BC-21 submission",
        )
        == {}
    )
    assert observed == {"authorization": "Bearer projected-workload-jwt", "timeout": 5.0}


def test_bc21_request_exposes_only_safe_problem_detail_on_forbidden_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    token_path = tmp_path / "service-account.token"
    token_path.write_text("projected-workload-jwt\n", encoding="utf-8")

    def forbidden_urlopen(request: Request, *, timeout: float) -> object:
        assert request.get_header("Authorization") == "Bearer projected-workload-jwt"
        assert timeout == 5.0
        raise HTTPError(
            request.full_url,
            403,
            "Forbidden",
            hdrs=Message(),
            fp=io.BytesIO(
                b'{"status":403,"title":"Untrusted SERP request context",'
                b'"detail":"SERP request workload identity could not be verified."}'
            ),
        )

    monkeypatch.setenv("ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH", str(token_path))
    monkeypatch.setattr(serp_eval_contracts_module, "urlopen", forbidden_urlopen)

    with pytest.raises(
        ValueError,
        match=(
            "status=403 problem=Untrusted SERP request context: "
            "SERP request workload identity could not be verified."
        ),
    ):
        serp_eval_contracts_module._bc21_json_request(
            "http://bc21.test/api/bc-21/serp/v1/runs/pipeline-state",
            method="POST",
            body={"status": "indexed"},
            headers={"X-Adapstory-Tenant-Id": TENANT_ID},
            error_label="test BC-21 submission",
        )


def test_public_docs_crawler_uses_configured_source_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_proxy_urls: list[dict[str, str]] = []

    class Response:
        def __init__(self) -> None:
            self.status = 200
            self.headers = {"Content-Type": "text/html"}

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, _size: int) -> bytes:
            return b"<html>proxied</html>"

    class Opener:
        def open(self, _request: object, *, timeout: int) -> Response:
            assert timeout > 0
            return Response()

    class FakeProxyHandler:
        def __init__(self, proxy_urls: Mapping[str, str]) -> None:
            captured_proxy_urls.append(dict(proxy_urls))

    def fake_build_opener(_proxy_handler: object) -> Opener:
        return Opener()

    monkeypatch.setenv(
        "ADAPSTORY_SERP_SOURCE_PROXY_URL",
        "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
    )
    monkeypatch.setattr(
        "dags.serp_eval_contracts.urlopen",
        lambda *_args, **_kwargs: pytest.fail("crawler bypassed configured proxy"),
    )
    monkeypatch.setattr("dags.serp_eval_contracts.ProxyHandler", FakeProxyHandler)
    monkeypatch.setattr("dags.serp_eval_contracts.build_opener", fake_build_opener)

    response = _fetch_public_docs_crawler_response(
        "https://cert-manager.io/docs", {"User-Agent": "serp-test/1"}
    )

    assert response.status_code == 200
    assert response.body == b"<html>proxied</html>"
    assert captured_proxy_urls == [
        {
            "http": "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
            "https": "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
        }
    ]


def test_public_docs_crawler_discovery_scans_full_policy_before_bounding_ingestion_frontier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_policies: list[dict[str, object]] = []

    def fake_crawl_public_docs(**kwargs: object) -> dict[str, object]:
        observed_policies.append(dict(cast(Mapping[str, object], kwargs["crawl_policy"])))
        urls = [
            "https://docs.example.com/guide-a",
            "https://docs.example.com/guide-b",
            "https://docs.example.com/guide-c",
            "https://docs.example.com/guide-d",
            "https://docs.example.com/guide-e",
        ]
        return {
            "changed_urls": urls,
            "state": {url: {"status": "active"} for url in urls},
            "status": "completed",
        }

    monkeypatch.setattr("dags.serp_eval_contracts.crawl_public_docs", fake_crawl_public_docs)
    crawl_policy = {
        "allowed_domains": ["docs.example.com"],
        "max_depth": 2,
        "max_pages": 25,
        "respect_robots_txt": True,
        "user_agent": "serp-test/1",
    }

    result = discover_public_docs_crawler_frontier(
        "https://docs.example.com/", crawl_policy, max_urls=4
    )

    assert result["urls"] == [
        "https://docs.example.com/guide-a",
        "https://docs.example.com/guide-b",
        "https://docs.example.com/guide-c",
        "https://docs.example.com/guide-d",
    ]
    assert observed_policies == [crawl_policy]
    assert crawl_policy["max_pages"] == 25


def test_public_docs_seed_refresh_plan_runs_crawler_discovery_with_bounded_concurrency(
    tmp_path: Path,
) -> None:
    conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-10T12:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    conf["crawler_discovery_workers"] = 2
    barrier = Barrier(2, timeout=2)
    synchronized_sources = {
        "https://pve.proxmox.com/pve-docs/",
        "https://docs.k3s.io/",
    }

    def discover(source_uri: str, _policy: Mapping[str, Any], _max_urls: int) -> list[str]:
        if source_uri in synchronized_sources:
            barrier.wait()
        return []

    plan = build_public_docs_seed_refresh_plan(
        conf,
        sitemap_frontier_discoverer=discover,
    )

    assert plan.payload["crawler_discovery_workers"] == 2


def test_public_docs_seed_refresh_passes_committed_page_state_to_live_crawler(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    prior_page_state = {
        "https://docs.k3s.io/": {
            "content_hash": "a" * 64,
            "etag": '"k3s-v1"',
            "http_status": 200,
            "last_modified": "Wed, 08 Jul 2026 12:00:00 GMT",
            "status": "active",
        }
    }
    conf["seed_registry"][0]["freshness_state"] = {
        "page_state": prior_page_state,
        "status": "indexed",
    }
    observed_previous_states: list[Mapping[str, Any]] = []

    def discover(source_uri: str, policy: Mapping[str, Any], _max_urls: int) -> list[str]:
        if source_uri == "https://docs.k3s.io/":
            observed_previous_states.append(policy["previous_state"])
        return []

    build_public_docs_seed_refresh_plan(conf, sitemap_frontier_discoverer=discover)

    assert observed_previous_states == [prior_page_state]


def test_build_nightly_regression_plan_requires_all_mandatory_suites() -> None:
    plan = build_nightly_regression_plan(_nightly_conf())
    repeated = build_nightly_regression_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_nightly_regression_suite"
    assert plan.payload["normalized_gate_floor"] == SERP_NORMALIZED_GATE_FLOOR
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "nightly_registry_submissions": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/nightly-registry-submissions.json"
        ),
        "nightly_registry_receipts": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/nightly-registry-receipts.json"
        ),
        "nightly_report": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/nightly-report.json"
        ),
        "benchmark_gate_export": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-gate-export.json"
        ),
        "benchmark_catalog": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/benchmark-catalog.json"
        ),
        "benchmark_catalog_receipt": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-catalog-materialization-receipt.json"
        ),
        "suite_plan": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/suite-plan.json"
        ),
    }
    assert plan.payload["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_nightly_regression_plan",
        "materialize_live_benchmark_catalog",
        "load_materialized_benchmark_catalog",
        "write_nightly_suite_plan",
        "run_mandatory_benchmark_suites",
        "build_c1_benchmark_gate_export",
        "build_bc21_benchmark_run_submissions",
        "submit_bc21_benchmark_run_submissions",
        "notify_governance_eval_surfaces",
    ]

    missing_suite_conf = _nightly_conf()
    missing_suite_conf["selected_suite_ids"] = list(MANDATORY_SERP_BENCHMARK_SUITES[:-1])
    with pytest.raises(ValueError, match="selected_suite_ids must include every mandatory suite"):
        build_nightly_regression_plan(missing_suite_conf)

    url_artifact_root = _nightly_conf()
    url_artifact_root["artifact_root_path"] = "https://example.invalid/serp-evals"
    with pytest.raises(
        ValueError, match="artifact_root_path must be an absolute path or s3:// URI"
    ):
        build_nightly_regression_plan(url_artifact_root)

    unsafe_bc21_base_url = _nightly_conf()
    unsafe_bc21_base_url["bc21_base_url"] = "http://example.invalid"
    with pytest.raises(ValueError, match="bc21_base_url must use https"):
        build_nightly_regression_plan(unsafe_bc21_base_url)


def test_default_nightly_regression_conf_is_runtime_owned_and_canonical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_defaults = {
        "ADAPSTORY_AIRFLOW_ARTIFACT_ROOT": "s3://airflow-serp-evidence/serp-evals",
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://serp-context-platform.env-dev.svc.cluster.local",
        "ADAPSTORY_SERP_D6_ACTOR_ID": "airflow-serp-eval-runner",
        "ADAPSTORY_SERP_D6_PACK_VERSION_IDS": json.dumps([PACK_VERSION_ID]),
        "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_ID": REGISTRY_RESOURCE_ID,
        "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_TYPE": "workflow",
        "ADAPSTORY_SERP_D6_RERANKER_PROFILE_VERSION": "reranker@2026.07.1",
        "ADAPSTORY_SERP_D6_RETRIEVAL_PROFILE_VERSION": "hybrid@2026.07.1",
        "ADAPSTORY_SERP_D6_TENANT_ID": TENANT_ID,
    }
    for name, value in runtime_defaults.items():
        monkeypatch.setenv(name, value)

    conf = default_nightly_regression_conf(generated_at="2026-07-14T08:00:00Z")
    plan = build_nightly_regression_plan(conf)

    assert conf["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert plan.payload["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)

    monkeypatch.delenv("ADAPSTORY_SERP_D6_PACK_VERSION_IDS")
    with pytest.raises(ValueError, match="ADAPSTORY_SERP_D6_PACK_VERSION_IDS is required"):
        default_nightly_regression_conf(generated_at="2026-07-14T08:00:00Z")


def test_mandatory_benchmark_dataset_evidence_plan_is_isolated_from_scoring() -> None:
    plan = build_mandatory_benchmark_dataset_evidence_plan(
        {
            "artifact_root_path": "s3://airflow-serp-artifacts/serp-evals",
            "generated_at": "2026-07-13T19:30:00Z",
        }
    )

    assert plan.payload["dag_id"] == "serp_mandatory_benchmark_dataset_evidence_snapshot"
    assert plan.payload["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "s3://airflow-serp-artifacts/serp-evals/"
            f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "benchmark_catalog": (
            "s3://airflow-serp-artifacts/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-catalog.json"
        ),
        "benchmark_catalog_receipt": (
            "s3://airflow-serp-artifacts/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-catalog-materialization-receipt.json"
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_mandatory_benchmark_dataset_evidence_plan",
        "materialize_mandatory_benchmark_dataset_evidence",
    ]
    assert "benchmark_suite_inputs" not in plan.payload
    assert "metric" not in plan.to_canonical_json()

    with pytest.raises(
        ValueError,
        match="mandatory dataset evidence requires an s3:// artifact_root_path",
    ):
        build_mandatory_benchmark_dataset_evidence_plan(
            {
                "artifact_root_path": "/var/opt/adapstory/serp-evals",
                "generated_at": "2026-07-13T19:30:00Z",
            }
        )


def test_catalog_materializer_accepts_dedicated_dataset_evidence_plan() -> None:
    plan = build_mandatory_benchmark_dataset_evidence_plan(
        {
            "artifact_root_path": "s3://airflow-serp-artifacts/serp-evals",
            "generated_at": "2026-07-13T19:30:00Z",
        }
    )
    written: list[dict[str, object]] = []

    def snapshot_writer(**kwargs: object) -> dict[str, object]:
        written.append(kwargs)
        return {
            "artifactPath": kwargs["artifact_path"],
            "artifactSha256": "f" * 64,
            "artifactType": kwargs["artifact_type"],
            "artifactVersionId": "version-20260713",
            "objectLockMode": "COMPLIANCE",
            "operationId": kwargs["operation_id"],
            "status": "written",
        }

    def snapshot_bytes_writer(**kwargs: object) -> dict[str, object]:
        written.append(kwargs)
        payload = cast(bytes, kwargs["payload"])
        return {
            "artifactPath": kwargs["artifact_path"],
            "artifactSha256": sha256(payload).hexdigest(),
            "artifactType": kwargs["artifact_type"],
            "artifactVersionId": "version-20260713",
            "objectLockMode": "COMPLIANCE",
            "operationId": kwargs["operation_id"],
            "status": "written",
        }

    result = materialize_live_benchmark_catalog_artifact(
        plan.to_canonical_json(),
        fetch_bytes=lambda url: url.encode("utf-8"),
        snapshot_writer=snapshot_writer,
        snapshot_bytes_writer=snapshot_bytes_writer,
    )

    assert result["catalogStatus"] == "blocked"
    assert result["blockingSuiteIds"] == [
        suite_id for suite_id in MANDATORY_SERP_BENCHMARK_SUITES if suite_id != "BEIR"
    ]
    assert len(written) == (len(MANDATORY_SERP_BENCHMARK_SUITES) * 3) + 1


def test_nightly_regression_plan_rejects_caller_supplied_suite_inputs() -> None:
    conf = _nightly_conf()
    conf["benchmark_suite_inputs"] = [{"synthetic": "must-not-reach-d6"}]

    with pytest.raises(ValueError, match="must be produced by canonical live adapters"):
        build_nightly_regression_plan(conf)


def test_nightly_catalog_materialization_writes_all_live_legal_evidence_before_blocking() -> None:
    plan = build_nightly_regression_plan(
        {**_nightly_conf(), "artifact_root_path": "s3://airflow-serp-evidence/serp-evals"}
    )
    written: list[dict[str, object]] = []

    def snapshot_writer(**kwargs: object) -> dict[str, object]:
        written.append(kwargs)
        return {
            "artifactPath": kwargs["artifact_path"],
            "artifactSha256": "f" * 64,
            "artifactType": kwargs["artifact_type"],
            "artifactVersionId": "version-20260713",
            "objectLockMode": "COMPLIANCE",
            "operationId": kwargs["operation_id"],
            "status": "written",
        }

    def snapshot_bytes_writer(**kwargs: object) -> dict[str, object]:
        written.append(kwargs)
        payload = cast(bytes, kwargs["payload"])
        return {
            "artifactPath": kwargs["artifact_path"],
            "artifactSha256": sha256(payload).hexdigest(),
            "artifactType": kwargs["artifact_type"],
            "artifactVersionId": "version-20260713",
            "objectLockMode": "COMPLIANCE",
            "operationId": kwargs["operation_id"],
            "status": "written",
        }

    result = materialize_live_benchmark_catalog_artifact(
        plan.to_canonical_json(),
        fetch_bytes=lambda url: url.encode("utf-8"),
        snapshot_writer=snapshot_writer,
        snapshot_bytes_writer=snapshot_bytes_writer,
    )

    assert result["catalogStatus"] == "blocked"
    assert result["blockingSuiteIds"] == [
        suite_id for suite_id in MANDATORY_SERP_BENCHMARK_SUITES if suite_id != "BEIR"
    ]
    assert written[-1]["artifact_path"] == plan.payload["artifact_paths"]["benchmark_catalog"]
    assert len(written) == (len(MANDATORY_SERP_BENCHMARK_SUITES) * 3) + 1


def test_load_materialized_catalog_binds_receipt_and_catalog_s3_versions() -> None:
    plan = build_nightly_regression_plan(
        {**_nightly_conf(), "artifact_root_path": "s3://airflow-serp-evidence/serp-evals"}
    )
    catalog_path = plan.payload["artifact_paths"]["benchmark_catalog"]
    receipt_path = plan.payload["artifact_paths"]["benchmark_catalog_receipt"]
    blocking_suite_ids = [
        suite_id for suite_id in MANDATORY_SERP_BENCHMARK_SUITES if suite_id != "BEIR"
    ]
    catalog_payload = {
        "catalog_status": "blocked",
        "suites": [
            {
                "execution_status": "ready" if suite_id == "BEIR" else "adapter-unavailable",
                "suite_id": suite_id,
            }
            for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
    }
    catalog_bytes = json.dumps(catalog_payload, sort_keys=True).encode("utf-8")
    receipt_payload = {
        "catalogSnapshot": {
            "artifactPath": catalog_path,
            "artifactSha256": sha256(catalog_bytes).hexdigest(),
            "artifactVersionId": "catalog-v1",
            "blockingSuiteIds": blocking_suite_ids,
            "catalogStatus": "blocked",
            "objectLockMode": "COMPLIANCE",
        },
        "contractVersion": "serp-benchmark-catalog-materializer/v1",
        "dagId": "serp_nightly_regression_suite",
        "operationId": plan.payload["operation_id"],
    }

    class Body:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload

        def read(self) -> bytes:
            return self.payload

    class FakeS3Client:
        def head_object(
            self,
            *,
            Bucket: str,
            Key: str,
            VersionId: str | None = None,
        ) -> dict[str, object]:
            assert Bucket == "airflow-serp-evidence"
            if Key == receipt_path.removeprefix("s3://airflow-serp-evidence/"):
                assert VersionId is None
                return {
                    "ObjectLockMode": "COMPLIANCE",
                    "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=1),
                    "VersionId": "receipt-v1",
                }
            assert Key == catalog_path.removeprefix("s3://airflow-serp-evidence/")
            assert VersionId == "catalog-v1"
            return {
                "ObjectLockMode": "COMPLIANCE",
                "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=1),
                "VersionId": "catalog-v1",
            }

        def get_object(
            self,
            *,
            Bucket: str,
            Key: str,
            VersionId: str,
        ) -> dict[str, object]:
            assert Bucket == "airflow-serp-evidence"
            if VersionId == "receipt-v1":
                assert Key == receipt_path.removeprefix("s3://airflow-serp-evidence/")
                return {"Body": Body(json.dumps(receipt_payload, sort_keys=True).encode("utf-8"))}
            assert VersionId == "catalog-v1"
            assert Key == catalog_path.removeprefix("s3://airflow-serp-evidence/")
            return {"Body": Body(catalog_bytes)}

    snapshot = load_materialized_benchmark_catalog_snapshot(
        plan.to_canonical_json(),
        s3_client=FakeS3Client(),
    )

    assert snapshot["artifactVersionId"] == "catalog-v1"
    assert snapshot["catalogReceiptVersionId"] == "receipt-v1"
    assert snapshot["blockingSuiteIds"] == blocking_suite_ids


def test_build_nightly_regression_plan_accepts_s3_artifact_root_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _nightly_conf()
    conf.pop("artifact_root_path")
    monkeypatch.setenv(
        "ADAPSTORY_AIRFLOW_ARTIFACT_ROOT",
        "s3://airflow-serp-artifacts/serp-evals",
    )

    plan = build_nightly_regression_plan(conf)

    assert plan.payload["artifact_root_path"] == "s3://airflow-serp-artifacts/serp-evals"
    assert plan.payload["artifact_paths"]["airflow_plan"].startswith(
        "s3://airflow-serp-artifacts/serp-evals/"
    )


def test_write_airflow_plan_artifact_writes_s3_artifact_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _nightly_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_nightly_regression_plan(conf)
    airflow_plan_path = plan.payload["artifact_paths"]["airflow_plan"]
    bucket, key = airflow_plan_path.removeprefix("s3://").split("/", 1)
    put_calls: list[tuple[str, str, str, str]] = []

    class FakeS3Client:
        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    plan_json = write_airflow_plan_artifact(plan)

    assert json.loads(plan_json) == plan.payload
    assert put_calls == [
        (
            bucket,
            key,
            json.dumps(plan.payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            "application/json",
        )
    ]


def test_build_nightly_gateway_cli_specs_are_file_based_and_deterministic() -> None:
    plan = build_nightly_regression_plan(_nightly_conf())
    runner = build_nightly_runner_cli_spec(plan.to_canonical_json())
    benchmark_export = build_nightly_benchmark_export_cli_spec(plan.to_canonical_json())
    submissions = build_nightly_registry_cli_spec(plan.to_canonical_json())
    submit = build_nightly_registry_submit_cli_spec(plan.to_canonical_json())

    assert runner["status"] == "ready_for_gateway_cli_runner"
    assert runner["task_id"] == "run_mandatory_benchmark_suites"
    assert runner["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "nightly-report",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--suite-plan",
        plan.payload["artifact_paths"]["suite_plan"],
    ]
    assert runner["stdout_path"] == plan.payload["artifact_paths"]["nightly_report"]
    assert runner["input_paths"] == [
        plan.payload["artifact_paths"]["airflow_plan"],
        plan.payload["artifact_paths"]["suite_plan"],
    ]

    assert benchmark_export["status"] == "ready_for_gateway_cli_runner"
    assert benchmark_export["task_id"] == "build_c1_benchmark_gate_export"
    assert benchmark_export["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "nightly-benchmark-export",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--nightly-report",
        plan.payload["artifact_paths"]["nightly_report"],
    ]
    assert (
        benchmark_export["stdout_path"] == plan.payload["artifact_paths"]["benchmark_gate_export"]
    )
    assert benchmark_export["input_paths"] == [
        plan.payload["artifact_paths"]["airflow_plan"],
        plan.payload["artifact_paths"]["nightly_report"],
    ]

    assert submissions["status"] == "ready_for_gateway_cli_runner"
    assert submissions["task_id"] == "build_bc21_benchmark_run_submissions"
    assert submissions["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "nightly-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--nightly-report",
        plan.payload["artifact_paths"]["nightly_report"],
    ]
    assert (
        submissions["stdout_path"] == plan.payload["artifact_paths"]["nightly_registry_submissions"]
    )

    assert submit["status"] == "ready_for_gateway_cli_runner"
    assert submit["task_id"] == "submit_bc21_benchmark_run_submissions"
    assert submit["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "submit-nightly-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--nightly-registry-submissions",
        plan.payload["artifact_paths"]["nightly_registry_submissions"],
        "--bc21-base-url",
        plan.payload["bc21_base_url"],
    ]
    assert submit["stdout_path"] == plan.payload["artifact_paths"]["nightly_registry_receipts"]


def test_nightly_d6_airflow_path_blocks_before_gateway_runner_when_licenses_are_unattested(
    tmp_path: Path,
) -> None:
    conf = _nightly_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_nightly_regression_plan(conf)
    plan_json = write_airflow_plan_artifact(plan)

    catalog_snapshot = {
        "artifactPath": plan.payload["artifact_paths"]["benchmark_catalog"],
        "artifactVersionId": "version-20260713",
        "blockingSuiteIds": ["CodeRAG-Bench", "SWE-bench Verified", "rusBEIR"],
        "catalogStatus": "blocked",
        "objectLockMode": "COMPLIANCE",
    }

    with pytest.raises(ValueError, match="benchmark catalog blocks D6"):
        write_nightly_suite_plan_artifact(json.loads(plan_json), catalog_snapshot)


def test_nightly_provenance_requires_internal_only_boundary_for_unverified_rights() -> None:
    metadata = dict(cast(Mapping[str, Any], _nightly_benchmark_suite_input("BEIR")["metadata"]))
    metadata["dataset_rights_status"] = "rights-unverified"
    metadata["dataset_distribution_rule"] = "internal-only"

    with pytest.raises(ValueError, match="rights-unverified"):
        serp_eval_contracts_module._validate_nightly_benchmark_suite_provenance(metadata)


def test_nightly_metric_compatibility_requires_immutable_matrix_version() -> None:
    suites = [
        _nightly_benchmark_suite_input(suite_id) for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
    ]
    cast(dict[str, object], suites[0]["metric_compatibility"]).pop("matrix_version_id")

    with pytest.raises(ValueError, match="matrix_version_id is required"):
        serp_eval_contracts_module._required_nightly_benchmark_suite_inputs(
            {"benchmark_suite_inputs": suites},
            selected_suite_ids=MANDATORY_SERP_BENCHMARK_SUITES,
        )


def test_airflow_registry_submission_preserves_d6_executable_provenance() -> None:
    suite = serp_eval_contracts_module._suite_result_from_suite_plan(
        _nightly_benchmark_suite_input("BEIR")
    )
    submission = serp_eval_contracts_module._live_registry_submission(
        {
            "operation_id": "serp-nightly-fixture",
            "registry_resource_id": REGISTRY_RESOURCE_ID,
            "registry_resource_type": "workflow",
            "tenant_id": TENANT_ID,
        },
        suite,
        "retrieval",
        "airflow-serp-eval-runner",
    )

    assert submission["body"]["evaluationContractCode"] == "d6-evidence-2026.07.3"
    assert submission["body"]["provenance"]["adapterId"] == "fixture-beir"
    assert submission["body"]["provenance"]["metricCompatibilityVersionId"] == (
        "fixture-metric-compatibility-version"
    )


def test_build_online_eval_rollup_plan_materializes_d7_contract(tmp_path: Path) -> None:
    conf = _online_eval_rollup_conf()
    conf["artifact_root_path"] = str(tmp_path)

    plan = build_online_eval_rollup_plan(conf)
    repeated = build_online_eval_rollup_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_online_eval_rollup"
    assert plan.payload["normalized_gate_floor"] == SERP_NORMALIZED_GATE_FLOOR
    assert plan.payload["capacity_readiness_state"] == "ready_for_po_capacity_approval"
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "/".join((str(tmp_path), plan.payload["operation_id"], "airflow-plan.json"))
        ),
        "online_eval_registry_submissions": (
            "/".join(
                (
                    str(tmp_path),
                    plan.payload["operation_id"],
                    "online-eval-registry-submissions.json",
                )
            )
        ),
        "online_eval_rollup": (
            "/".join((str(tmp_path), plan.payload["operation_id"], "online-eval-rollup.json"))
        ),
        "online_eval_rollup_plan": (
            "/".join((str(tmp_path), plan.payload["operation_id"], "online-eval-rollup-plan.json"))
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_online_eval_rollup_plan",
        "write_online_eval_rollup_plan",
        "build_online_eval_rollup",
        "build_online_eval_registry_submissions",
        "notify_governance_eval_surfaces",
    ]

    rollup_plan_artifact = write_online_eval_rollup_plan_artifact(plan.to_canonical_json())
    rollup_plan_path = Path(plan.payload["artifact_paths"]["online_eval_rollup_plan"])
    assert rollup_plan_path.exists()
    rollup_plan = json.loads(rollup_plan_path.read_text(encoding="utf-8"))
    assert rollup_plan_artifact["artifactType"] == "online_eval_rollup_plan"
    assert rollup_plan["rollup_id"] == "serp_online_eval_rollup"
    assert rollup_plan["reports"] == conf["reports"]


def test_build_online_eval_rollup_gateway_cli_specs_are_file_based(tmp_path: Path) -> None:
    conf = _online_eval_rollup_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_online_eval_rollup_plan(conf)
    rollup = build_online_eval_rollup_cli_spec(plan.to_canonical_json())
    submissions = build_online_eval_registry_cli_spec(plan.to_canonical_json())

    assert rollup["status"] == "ready_for_gateway_cli_runner"
    assert rollup["task_id"] == "build_online_eval_rollup"
    assert rollup["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "online-eval-rollup",
        "--rollup-plan",
        plan.payload["artifact_paths"]["online_eval_rollup_plan"],
    ]
    assert rollup["input_paths"] == [plan.payload["artifact_paths"]["online_eval_rollup_plan"]]
    assert rollup["stdout_path"] == plan.payload["artifact_paths"]["online_eval_rollup"]

    assert submissions["status"] == "ready_for_gateway_cli_runner"
    assert submissions["task_id"] == "build_online_eval_registry_submissions"
    assert submissions["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "online-eval-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--online-eval-rollup",
        plan.payload["artifact_paths"]["online_eval_rollup"],
    ]
    assert submissions["input_paths"] == [
        plan.payload["artifact_paths"]["airflow_plan"],
        plan.payload["artifact_paths"]["online_eval_rollup"],
    ]
    assert (
        submissions["stdout_path"]
        == plan.payload["artifact_paths"]["online_eval_registry_submissions"]
    )


def test_execute_gateway_cli_spec_runs_without_shell_and_persists_stdout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "airflow-plan.json"
    output_path = tmp_path / "nightly-report.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = {"status": "accepted", "tenant_id": TENANT_ID}
    calls: list[object] = []

    def fake_run(
        argv: list[str],
        *,
        capture_output: bool,
        check: bool,
        text: bool,
    ) -> object:
        calls.append((argv, capture_output, check, text))

        class Result:
            returncode = 0
            stdout = json.dumps(payload)
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_gateway_cli_spec(
        {
            "argv": ["python", "-m", "safe.module", "run"],
            "contract_version": "serp-airflow-gateway-cli-bridge/v1",
            "dag_id": "serp_nightly_regression_suite",
            "input_paths": [str(input_path)],
            "operation_id": "op-1",
            "status": "ready_for_gateway_cli_runner",
            "stdout_path": str(output_path),
            "task_id": "run_mandatory_benchmark_suites",
            "tenant_id": TENANT_ID,
        }
    )

    assert calls == [(["python", "-m", "safe.module", "run"], True, False, True)]
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == payload
    assert result["payload"] == payload
    assert result["artifactPath"] == str(output_path)


def test_execute_pipeline_cli_spec_runs_without_shell_and_persists_stdout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload("indexed")
    calls: list[object] = []

    def fake_run(
        argv: list[str],
        *,
        capture_output: bool,
        check: bool,
        text: bool,
    ) -> object:
        calls.append((argv, capture_output, check, text))

        class Result:
            returncode = 0
            stdout = json.dumps(payload)
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_pipeline_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
            ],
            "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
            "dag_id": "serp_web_seed_crawl_refresh",
            "input_paths": [str(input_path)],
            "operation_id": "op-1",
            "status": "ready_for_pipeline_cli_runner",
            "stdout_path": str(output_path),
            "task_id": "public_docs_seed_refresh_pipeline",
            "tenant_id": TENANT_ID,
        }
    )

    assert calls == [
        (
            ["python", "-m", "adapstory_serp_pipeline.orchestration.seed_refresh_cli"],
            True,
            False,
            True,
        )
    ]
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == payload
    assert result["payload"] == payload
    assert result["artifactPath"] == str(output_path)


def test_execute_pipeline_cli_spec_persists_redacted_failure_receipt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")

    class Result:
        returncode = 2
        stdout = ""
        stderr = (
            "ValueError: live store request failed; "
            "authorization=super-secret-token; "
            "Bearer another-secret-token; "
            '{"api_key": "json-secret-token"}'
        )

    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    with pytest.raises(ValueError, match="failure_artifact_path=") as error:
        execute_pipeline_cli_spec(
            {
                "argv": [
                    "python",
                    "-m",
                    "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                ],
                "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
                "dag_id": "serp_web_seed_crawl_refresh",
                "input_paths": [str(input_path)],
                "operation_id": "op-1",
                "status": "ready_for_pipeline_cli_runner",
                "stdout_path": str(output_path),
                "task_id": "public_docs_seed_refresh_pipeline",
                "tenant_id": TENANT_ID,
            }
        )

    failure_path = tmp_path / "public-docs-seed-refresh-result.failure.json"
    receipt = json.loads(failure_path.read_text(encoding="utf-8"))

    assert f"failure_artifact_path={failure_path}" in str(error.value)
    assert receipt["artifact_type"] == "pipeline_cli_failure"
    assert receipt["returncode"] == 2
    assert receipt["stderr_sha256"] == sha256(Result.stderr.encode("utf-8")).hexdigest()
    assert "live store request failed" in receipt["stderr_excerpt"]
    assert "super-secret-token" not in receipt["stderr_excerpt"]
    assert "another-secret-token" not in receipt["stderr_excerpt"]
    assert "json-secret-token" not in receipt["stderr_excerpt"]
    assert "[REDACTED]" in receipt["stderr_excerpt"]


def test_execute_pipeline_cli_spec_persists_failure_receipt_to_s3(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = {
        (
            "airflow-serp-artifacts",
            "serp-evals/op/public-docs-seed-refresh-plan.json",
        ): b"{}",
    }
    put_calls: list[tuple[str, str, str]] = []

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            assert ContentType == "application/json"
            storage[(Bucket, Key)] = Body
            put_calls.append((Bucket, Key, Body.decode("utf-8")))

    class Result:
        returncode = 1
        stdout = ""
        stderr = "ValueError: OPENSEARCH_PASSWORD=do-not-persist"

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())
    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    with pytest.raises(ValueError, match="failure_artifact_path="):
        execute_pipeline_cli_spec(
            {
                "argv": [
                    "python",
                    "-m",
                    "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                    "--refresh-plan",
                    "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-plan.json",
                    "--evidence-output",
                    "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json",
                ],
                "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
                "dag_id": "serp_web_seed_crawl_refresh",
                "input_paths": [
                    "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-plan.json"
                ],
                "operation_id": "op-1",
                "status": "ready_for_pipeline_cli_runner",
                "stdout_path": "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json",
                "task_id": "public_docs_seed_refresh_pipeline",
                "tenant_id": TENANT_ID,
            }
        )

    assert len(put_calls) == 1
    bucket, key, raw_receipt = put_calls[0]
    assert (bucket, key) == (
        "airflow-serp-artifacts",
        "serp-evals/op/public-docs-seed-refresh-result.failure.json",
    )
    assert "do-not-persist" not in raw_receipt
    assert json.loads(raw_receipt)["stderr_excerpt"].endswith("[REDACTED]")


def test_execute_pipeline_cli_spec_rejects_evidence_only_public_docs_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload("indexed", index_mode="evidence-only")

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    with pytest.raises(ValueError, match="requires live index_mode"):
        execute_pipeline_cli_spec(
            {
                "argv": [
                    "python",
                    "-m",
                    "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                ],
                "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
                "dag_id": "serp_web_seed_crawl_refresh",
                "input_paths": [str(input_path)],
                "index_mode": "evidence-only",
                "operation_id": "op-1",
                "status": "ready_for_pipeline_cli_runner",
                "stdout_path": str(output_path),
                "task_id": "public_docs_seed_refresh_pipeline",
                "tenant_id": TENANT_ID,
            }
        )

    assert json.loads(output_path.read_text(encoding="utf-8")) == payload


def test_execute_pipeline_cli_spec_fails_d20_task_after_persisting_failed_batch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload("failed", indexed_count=0, failed_count=1)

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    with pytest.raises(ValueError, match="public docs seed refresh is not publishable"):
        execute_pipeline_cli_spec(
            {
                "argv": [
                    "python",
                    "-m",
                    "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                ],
                "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
                "dag_id": "serp_web_seed_crawl_refresh",
                "input_paths": [str(input_path)],
                "operation_id": "op-1",
                "status": "ready_for_pipeline_cli_runner",
                "stdout_path": str(output_path),
                "task_id": "public_docs_seed_refresh_pipeline",
                "tenant_id": TENANT_ID,
            }
        )

    assert json.loads(output_path.read_text(encoding="utf-8")) == payload


def test_execute_pipeline_cli_spec_accepts_optional_frontier_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload(
        "indexed_with_optional_failures",
        indexed_count=229,
        failed_count=16,
        optional_failed_count=16,
        required_failed_count=0,
    )

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    execute_pipeline_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
            ],
            "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
            "dag_id": "serp_web_seed_crawl_refresh",
            "input_paths": [str(input_path)],
            "operation_id": "op-1",
            "status": "ready_for_pipeline_cli_runner",
            "stdout_path": str(output_path),
            "task_id": "public_docs_seed_refresh_pipeline",
            "tenant_id": TENANT_ID,
        }
    )

    assert json.loads(output_path.read_text(encoding="utf-8")) == payload


def test_execute_pipeline_cli_spec_rejects_required_quarantined_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload(
        "indexed_with_quarantined_failures",
        indexed_count=34,
        failed_count=9,
        quarantined_count=9,
        required_failed_count=8,
    )

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    with pytest.raises(ValueError, match="required source failures"):
        execute_pipeline_cli_spec(
            {
                "argv": [
                    "python",
                    "-m",
                    "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                ],
                "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
                "dag_id": "serp_web_seed_crawl_refresh",
                "input_paths": [str(input_path)],
                "operation_id": "op-1",
                "status": "ready_for_pipeline_cli_runner",
                "stdout_path": str(output_path),
                "task_id": "public_docs_seed_refresh_pipeline",
                "tenant_id": TENANT_ID,
            }
        )

    assert json.loads(output_path.read_text(encoding="utf-8")) == payload


def test_execute_pipeline_cli_spec_materializes_s3_inputs_and_uploads_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = {
        (
            "airflow-serp-artifacts",
            "serp-evals/op/public-docs-seed-refresh-plan.json",
        ): b"{}",
    }
    payload = _pipeline_seed_refresh_payload("indexed")
    put_calls: list[tuple[str, str, str, str]] = []
    run_calls: list[list[str]] = []

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    def fake_run(
        argv: list[str],
        *,
        capture_output: bool,
        check: bool,
        text: bool,
    ) -> object:
        run_calls.append(argv)
        assert capture_output is True
        assert check is False
        assert text is True
        assert "s3://" not in " ".join(argv)
        refresh_plan = Path(argv[argv.index("--refresh-plan") + 1])
        artifact_root = Path(argv[argv.index("--artifact-root") + 1])
        evidence_output = Path(argv[argv.index("--evidence-output") + 1])
        assert refresh_plan.is_file()
        assert artifact_root.is_dir()
        assert evidence_output.is_absolute()

        class Result:
            returncode = 0
            stdout = json.dumps(payload)
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())
    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_pipeline_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                "--refresh-plan",
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-plan.json",
                "--artifact-root",
                "s3://airflow-serp-artifacts/serp-evals/op",
                "--evidence-output",
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json",
            ],
            "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
            "dag_id": "serp_web_seed_crawl_refresh",
            "input_paths": [
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-plan.json"
            ],
            "operation_id": "op-1",
            "status": "ready_for_pipeline_cli_runner",
            "stdout_path": (
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json"
            ),
            "task_id": "public_docs_seed_refresh_pipeline",
            "tenant_id": TENANT_ID,
        }
    )

    assert run_calls
    assert put_calls == [
        (
            "airflow-serp-artifacts",
            "serp-evals/op/public-docs-seed-refresh-result.json",
            json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            "application/json",
        )
    ]
    assert result["payload"] == payload
    assert result["artifactPath"] == (
        "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json"
    )


def test_execute_pipeline_cli_spec_preserves_pipeline_owned_immutable_evidence_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = "s3://airflow-serp-artifacts/serp-evals/op/paired-eval-request.json"
    receipt_path = "s3://airflow-serp-evidence/serp-evals/op/paired-eval-receipt.json"
    control_path = "s3://airflow-serp-artifacts/serp-evals/op/paired-eval-control.json"
    storage = {("airflow-serp-artifacts", "serp-evals/op/paired-eval-request.json"): b"{}"}
    put_calls: list[tuple[str, str, str]] = []

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            assert ContentType == "application/json"
            put_calls.append((Bucket, Key, Body.decode("utf-8")))

    def fake_run(argv: list[str], **_: object) -> object:
        assert Path(argv[argv.index("--paired-eval-request") + 1]).is_file()
        assert argv[argv.index("--evidence-output") + 1] == receipt_path

        class Result:
            returncode = 0
            stdout = json.dumps(
                {
                    "blockingSuiteIds": ["APIBench"],
                    "receiptEvidence": {
                        "artifactPath": receipt_path,
                        "artifactVersionId": "version-001",
                        "objectLockMode": "COMPLIANCE",
                    },
                    "receiptStatus": "blocked",
                }
            )
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())
    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_pipeline_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.paired_eval_receipt",
                "--paired-eval-request",
                request_path,
                "--evidence-output",
                receipt_path,
            ],
            "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
            "dag_id": "serp_benchmark_improvement_wave",
            "evidence_output_owner": "pipeline",
            "evidence_output_path": receipt_path,
            "input_paths": [request_path],
            "operation_id": "op-1",
            "status": "ready_for_pipeline_cli_runner",
            "stdout_path": control_path,
            "task_id": "run_paired_benchmark_evaluation",
            "tenant_id": TENANT_ID,
        }
    )

    assert result["payload"]["receiptStatus"] == "blocked"
    assert put_calls == [
        (
            "airflow-serp-artifacts",
            "serp-evals/op/paired-eval-control.json",
            json.dumps(result["payload"], ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        )
    ]


def test_dispatch_public_docs_seed_refresh_handoff_preserves_s3_artifact_root_uri() -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_public_docs_seed_refresh_plan(conf)

    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    artifact_root = cli_spec["argv"][cli_spec["argv"].index("--artifact-root") + 1]
    assert artifact_root.startswith("s3://airflow-serp-artifacts/serp-evals/")
    assert not artifact_root.startswith("s3:/airflow-serp-artifacts")


def test_execute_gateway_cli_spec_materializes_s3_inputs_and_uploads_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = {
        ("airflow-serp-artifacts", "serp-evals/input/airflow-plan.json"): b"{}",
    }
    payload = {"status": "accepted", "tenant_id": TENANT_ID}
    put_calls: list[tuple[str, str, str, str]] = []
    run_calls: list[tuple[list[str], bool, bool, bool]] = []

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            storage[(Bucket, Key)] = Body
            put_calls.append(("put_object", Bucket, Key, ContentType))

    def fake_run(
        argv: list[str],
        *,
        capture_output: bool,
        check: bool,
        text: bool,
    ) -> object:
        run_calls.append((argv, capture_output, check, text))
        assert argv[-1].startswith("/")
        assert not argv[-1].startswith("s3://")
        assert Path(argv[-1]).read_text(encoding="utf-8") == "{}"

        class Result:
            returncode = 0
            stdout = json.dumps(payload)
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())
    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_gateway_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "safe.module",
                "--airflow-plan",
                "s3://airflow-serp-artifacts/serp-evals/input/airflow-plan.json",
            ],
            "contract_version": "serp-airflow-gateway-cli-bridge/v1",
            "dag_id": "serp_nightly_regression_suite",
            "input_paths": ["s3://airflow-serp-artifacts/serp-evals/input/airflow-plan.json"],
            "operation_id": "op-1",
            "status": "ready_for_gateway_cli_runner",
            "stdout_path": "s3://airflow-serp-artifacts/serp-evals/output/nightly-report.json",
            "task_id": "run_mandatory_benchmark_suites",
            "tenant_id": TENANT_ID,
        }
    )

    assert len(run_calls) == 1
    argv, capture_output, check, text = run_calls[0]
    assert argv[:4] == ["python", "-m", "safe.module", "--airflow-plan"]
    assert argv[4].startswith("/")
    assert not argv[4].startswith("s3://")
    assert capture_output is True
    assert check is False
    assert text is True
    assert put_calls == [
        (
            "put_object",
            "airflow-serp-artifacts",
            "serp-evals/output/nightly-report.json",
            "application/json",
        )
    ]
    assert (
        json.loads(storage[("airflow-serp-artifacts", "serp-evals/output/nightly-report.json")])
        == payload
    )
    assert result["payload"] == payload
    assert (
        result["artifactPath"]
        == "s3://airflow-serp-artifacts/serp-evals/output/nightly-report.json"
    )


def test_evaluate_nightly_regression_gate_blocks_below_normalized_floor() -> None:
    gate = evaluate_nightly_regression_gate(
        {
            "suite_results": [
                {
                    "suite_id": "RAGBench",
                    "metric_results": [
                        {
                            "metric": "Recall@10",
                            "metric_family": "retrieval",
                            "normalized_score": 0.81,
                        }
                    ],
                },
                {
                    "suite_id": "APIBench",
                    "metric_results": [
                        {
                            "metric": "nDCG@10",
                            "metric_family": "retrieval",
                            "normalized_score": 0.74,
                        }
                    ],
                },
            ]
        }
    )

    assert gate["status"] == "blocked"
    assert gate["blocking_findings"] == [
        {
            "metric": "nDCG@10",
            "metric_family": "retrieval",
            "normalized_score": 0.74,
            "suite_id": "APIBench",
        }
    ]


def test_build_tenant_golden_regression_plan_preserves_workflow_provenance() -> None:
    plan = build_tenant_golden_regression_plan(_tenant_golden_conf())

    assert plan.payload["dag_id"] == "serp_tenant_golden_set_regression"
    assert plan.payload["tenant_id"] == TENANT_ID
    assert plan.payload["workflow_id"] == "workflow/private-course-authoring"
    assert plan.payload["golden_set_id"] == "tenant-public-course-authoring-golden"
    assert plan.payload["changed_pack_version_ids"] == [PACK_VERSION_ID]
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "golden_set": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/golden-set.json"
        ),
        "tenant_golden_registry_submissions": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/tenant-golden-registry-submissions.json"
        ),
        "tenant_golden_report": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/tenant-golden-report.json"
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_tenant_golden_regression_plan",
        "run_tenant_golden_set_cases",
        "build_tenant_golden_registry_submissions",
        "notify_governance_eval_surfaces",
    ]

    missing_workflow = _tenant_golden_conf()
    missing_workflow.pop("workflow_id")
    with pytest.raises(ValueError, match="workflow_id is required"):
        build_tenant_golden_regression_plan(missing_workflow)


def test_build_tenant_golden_gateway_cli_specs_are_file_based_and_deterministic() -> None:
    plan = build_tenant_golden_regression_plan(_tenant_golden_conf())
    runner = build_tenant_golden_runner_cli_spec(plan.to_canonical_json())
    submissions = build_tenant_golden_registry_cli_spec(plan.to_canonical_json())

    assert runner["status"] == "ready_for_gateway_cli_runner"
    assert runner["task_id"] == "run_tenant_golden_set_cases"
    assert runner["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "tenant-golden-report",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--golden-set",
        plan.payload["artifact_paths"]["golden_set"],
    ]
    assert runner["stdout_path"] == plan.payload["artifact_paths"]["tenant_golden_report"]

    assert submissions["status"] == "ready_for_gateway_cli_runner"
    assert submissions["task_id"] == "build_tenant_golden_registry_submissions"
    assert submissions["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "tenant-golden-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--tenant-golden-report",
        plan.payload["artifact_paths"]["tenant_golden_report"],
    ]
    assert (
        submissions["stdout_path"]
        == plan.payload["artifact_paths"]["tenant_golden_registry_submissions"]
    )


def test_evaluate_tenant_golden_gate_blocks_failed_metric_results() -> None:
    gate = evaluate_tenant_golden_gate(
        {
            "status": "blocked",
            "metric_results": [
                {
                    "metric": "Citation Accuracy",
                    "metric_family": "answer-quality",
                    "normalized_score": 1.0,
                    "status": "passed",
                },
                {
                    "metric": "Faithfulness",
                    "metric_family": "answer-quality",
                    "normalized_score": 0.82,
                    "status": "blocked",
                },
            ],
        }
    )

    assert gate["status"] == "blocked"
    assert gate["blocking_findings"] == [
        {
            "metric": "Faithfulness",
            "metric_family": "answer-quality",
            "normalized_score": 0.82,
            "status": "blocked",
        }
    ]


def test_build_benchmark_improvement_wave_plan_preserves_ratchet_contract() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    repeated = build_benchmark_improvement_wave_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_benchmark_improvement_wave"
    assert plan.payload["normalized_gate_floor"] == SERP_NORMALIZED_GATE_FLOOR
    assert plan.payload["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert plan.payload["improvement_spec_id"] == "improve-public-retrieval-reranker-v1"
    assert plan.payload["candidate_id"] == "candidate-reranker-v2"
    assert plan.payload["baseline_run_id"] == "evalrun_public_reranker_baseline_001"
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "improvement_spec": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/improvement-spec.json"
        ),
        "paired_eval_receipt": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-eval-receipt.json"
        ),
        "paired_eval_request": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-eval-request.json"
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_benchmark_improvement_wave_plan",
        "write_improvement_spec",
        "write_paired_eval_request",
        "run_paired_benchmark_evaluation",
        "notify_governance_eval_surfaces",
    ]

    missing_suite_conf = _improvement_wave_conf()
    missing_suite_conf["selected_suite_ids"] = list(MANDATORY_SERP_BENCHMARK_SUITES[:-1])
    with pytest.raises(ValueError, match="selected_suite_ids must include every mandatory suite"):
        build_benchmark_improvement_wave_plan(missing_suite_conf)

    unbounded_budget_conf = _improvement_wave_conf()
    unbounded_budget_conf["max_benchmark_runs"] = 0
    with pytest.raises(ValueError, match="max_benchmark_runs must be positive"):
        build_benchmark_improvement_wave_plan(unbounded_budget_conf)


def test_build_benchmark_improvement_wave_plan_rejects_caller_supplied_candidate_scores() -> None:
    conf = _improvement_wave_conf()
    conf["candidate_evaluation"] = {"candidateScore": "0.8"}

    with pytest.raises(ValueError, match="executor receipts"):
        build_benchmark_improvement_wave_plan(conf)


def test_paired_eval_request_derives_canonical_catalog_bindings_as_worm_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _improvement_wave_conf()
    plan = build_benchmark_improvement_wave_plan(conf)
    snapshots: list[dict[str, Any]] = []

    def snapshot_writer(
        artifact_path: str,
        *,
        artifact_type: str,
        operation_id: str,
        payload: Mapping[str, Any],
    ) -> dict[str, str]:
        snapshots.append(
            {
                "artifact_path": artifact_path,
                "artifact_type": artifact_type,
                "operation_id": operation_id,
                "payload": dict(payload),
            }
        )
        return {
            "artifactPath": artifact_path,
            "artifactSha256": "a" * 64,
            "artifactType": artifact_type,
            "artifactVersionId": "paired-request-version-001",
            "objectLockMode": "COMPLIANCE",
            "objectLockRetainUntil": "2027-07-14T00:00:00Z",
            "status": "written",
        }

    monkeypatch.setattr(
        serp_eval_contracts_module,
        "write_immutable_evidence_snapshot",
        snapshot_writer,
    )
    request_artifact = write_paired_eval_request_artifact(plan.to_canonical_json())
    request = request_artifact["payload"]

    assert request["selectedSuiteIds"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert request["metricDefinitionAuthority"] == "executor-pinned-metric-definition-profile"
    assert [item["suiteId"] for item in request["suiteBindings"]] == list(
        MANDATORY_SERP_BENCHMARK_SUITES
    )
    assert "Score" not in json.dumps(request)
    assert snapshots == [
        {
            "artifact_path": plan.payload["artifact_paths"]["paired_eval_request"],
            "artifact_type": "serp_paired_eval_request",
            "operation_id": plan.payload["operation_id"],
            "payload": request,
        }
    ]
    assert request_artifact["requestEvidence"]["artifactVersionId"] == "paired-request-version-001"


def test_build_paired_benchmark_plan_exposes_only_version_bound_request_and_receipt_paths() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())

    assert set(plan.payload["artifact_paths"]) == {
        "airflow_plan",
        "improvement_spec",
        "paired_eval_request",
        "paired_eval_receipt",
    }
    assert all(path.startswith("s3://") for path in plan.payload["artifact_paths"].values())


def test_write_benchmark_improvement_spec_never_persists_external_candidate_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _improvement_wave_conf()
    plan = build_benchmark_improvement_wave_plan(conf)
    persisted: list[tuple[str, Mapping[str, Any]]] = []

    monkeypatch.setattr(
        serp_eval_contracts_module,
        "_write_json_artifact",
        lambda path, payload: persisted.append((path, dict(payload))),
    )

    plan_json = write_airflow_plan_artifact(plan)
    spec_artifact = write_improvement_spec_artifact(json.loads(plan_json))

    assert persisted[-1][0] == plan.payload["artifact_paths"]["improvement_spec"]
    assert spec_artifact["payload"]["status"] == "awaiting-executor-derived-metrics"
    assert spec_artifact["payload"]["dryRun"] is False
    assert spec_artifact["payload"]["baseline"]["beatCondition"] == {
        "bootstrapConfidenceLevel": "0.95",
        "minimumMultiplier": "2.0",
        "pairedRunCount": 5,
        "rule": "all_required_primary_accuracy_metrics_meet_multiplier",
    }
    assert spec_artifact["payload"]["objective"] == {
        "optimizationDirection": "maximize",
        "primaryMetricAuthority": "executor-pinned-metric-definition-profile",
        "type": "benchmark-ratchet",
    }
    assert spec_artifact["payload"]["replay"] == {
        "baselineRunId": conf["baseline_run_id"],
        "candidateRunId": "candidate-reranker-v2-run-001",
        "featureFlags": conf["feature_flags"],
        "guardrailBundleVersion": conf["guardrail_bundle_version"],
        "judgeModelId": conf["judge_model_id"],
        "judgeModelVersion": conf["judge_model_version"],
        "judgePromptTemplateVersion": conf["judge_prompt_template_version"],
        "modelCatalogEntryId": conf["model_catalog_entry_id"],
        "policyBundleVersion": conf["policy_bundle_version"],
        "providerRouteId": conf["provider_route_id"],
        "rerankerProfileVersion": conf["reranker_profile_version"],
        "retrievalProfileVersion": conf["retrieval_profile_version"],
    }
    assert spec_artifact["payload"]["modelGovernance"] == {
        "guardrailBundleVersion": conf["guardrail_bundle_version"],
        "judgeModelId": conf["judge_model_id"],
        "judgeModelVersion": conf["judge_model_version"],
        "modelCatalogEntryId": conf["model_catalog_entry_id"],
        "policyBundleVersion": conf["policy_bundle_version"],
        "providerRouteId": conf["provider_route_id"],
        "status": "approved-for-eval-dry-run",
    }
    assert spec_artifact["payload"]["candidate"] == {
        "id": conf["candidate_id"],
        "runId": conf["candidate_run_id"],
        "scoreAuthority": "executor-receipt-only",
    }
    assert "candidateEvaluation" not in spec_artifact["payload"]


def test_build_benchmark_improvement_wave_plan_requires_replay_metadata() -> None:
    conf = _improvement_wave_conf()
    del conf["judge_model_version"]

    with pytest.raises(ValueError, match="judge_model_version is required"):
        build_benchmark_improvement_wave_plan(conf)


def test_build_benchmark_improvement_wave_plan_rejects_raw_secret_metadata() -> None:
    conf = _improvement_wave_conf()
    conf["judge_model_id"] = "sk-abcdefghijklmnop"

    with pytest.raises(ValueError, match="dag run config must not contain raw secret material"):
        build_benchmark_improvement_wave_plan(conf)


def test_build_public_docs_seed_refresh_plan_materializes_d20_contract(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)

    plan = build_public_docs_seed_refresh_plan(conf)
    repeated = build_public_docs_seed_refresh_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_web_seed_crawl_refresh"
    assert plan.payload["index_mode"] == "live"
    assert plan.payload["embedding_mode"] == "live-gateway"
    assert plan.payload["seed_count"] == 4
    assert plan.payload["status"] == "ready_for_public_docs_seed_refresh"
    assert plan.payload["source_type_counts"] == {
        "git": 1,
        "openapi": 1,
        "website": 2,
    }
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "airflow-plan.json")
        ),
        "public_docs_seed_refresh_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "public-docs-seed-refresh-plan.json")
        ),
        "public_docs_seed_refresh_result": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "public-docs-seed-refresh-result.json")
        ),
        "public_docs_publish_activation_trigger_conf": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-publish-activation-trigger-conf.json",
            )
        ),
        "public_docs_bc21_pipeline_state_receipt": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-bc21-pipeline-state-receipt.json",
            )
        ),
        "public_docs_seed_registry": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "public-docs-seed-registry.json")
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
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
    ]

    seed_registry_artifact = write_public_docs_seed_registry_artifact(plan.to_canonical_json())
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    assert Path(seed_registry_artifact["artifactPath"]).exists()
    assert Path(refresh_plan_artifact["artifactPath"]).exists()
    assert (
        seed_registry_artifact["payload"]["seed_registry_sha256"]
        == plan.payload["seed_registry_sha256"]
    )
    assert refresh_plan_artifact["payload"]["status"] == "ready_for_pipeline_dispatch"
    assert refresh_plan_artifact["payload"]["index_mode"] == "live"
    assert refresh_plan_artifact["payload"]["embedding_mode"] == "live-gateway"
    assert refresh_plan_artifact["payload"]["skipped_seed_count"] == 0
    assert refresh_plan_artifact["payload"]["seed_count"] == 9
    assert all(
        request["pipeline_run_spec"]["pipeline_stages"]
        == ["fetch", "parse", "chunk", "embed", "index"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    )
    assert [
        request["seed_id"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ] == [
        "k3s-docs",
        "k3s-docs--d15cca4a1ed9",
        "k3s-docs--906d7d24fe52",
    ]
    assert [
        request["source_uri"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
    ]
    assert {
        request["source_metadata"]["frontier"]["discovery_mode"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    } == {"governed-seed-frontier"}
    assert {
        request["source_type"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {"git", "openapi", "website"}
    assert all(
        request["source_uri_hash"].startswith("sha256:")
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    )
    assert all(
        "parse_run_id" in request["pipeline_run_spec"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    )
    assert {
        request["pipeline_run_spec"]["pack_id"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {PACK_ID}
    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())
    assert cli_spec["status"] == "ready_for_pipeline_cli_runner"
    assert cli_spec["task_id"] == "public_docs_seed_refresh_pipeline"
    assert (
        cli_spec["stdout_path"] == plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    assert "pending_pipeline_dispatch" not in json.dumps(cli_spec, sort_keys=True)
    assert cli_spec["seed_count"] == 9
    assert cli_spec["skipped_seed_count"] == 0
    assert "--index-mode" in cli_spec["argv"]
    assert cli_spec["argv"][cli_spec["argv"].index("--index-mode") + 1] == "live"
    assert "--embedding-mode" in cli_spec["argv"]
    assert cli_spec["argv"][cli_spec["argv"].index("--embedding-mode") + 1] == "live-gateway"
    assert cli_spec["argv"][cli_spec["argv"].index("--qdrant-collection") + 1] == "serp_vectors_dev"
    assert cli_spec["argv"][cli_spec["argv"].index("--opensearch-index") + 1] == "serp_lexical_dev"
    assert cli_spec["argv"][cli_spec["argv"].index("--neo4j-database") + 1] == "serp_graph_dev"
    assert cli_spec["argv"][:3] == [
        "python",
        "-m",
        "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
    ]


def test_public_docs_seed_registry_allows_url_keys_but_rejects_secret_fields(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    page_url = "https://neo4j.com/docs/operations-manual/current/authentication-authorization"

    plan = build_public_docs_seed_refresh_plan(conf)
    crawl_evidence = {
        "pages": {page_url: {"http_status": 200, "status": "new"}},
        "state": {},
        "status": "completed",
    }
    plan.payload["seed_registry"][0]["crawl_policy"]["crawl_evidence"] = crawl_evidence

    artifact = write_public_docs_seed_registry_artifact(plan.to_canonical_json())

    assert artifact["payload"]["status"] == "validated"

    plan.payload["seed_registry"][0]["crawl_policy"]["crawl_evidence"] = {
        "pages": {page_url: {"authorization": "Bearer test-token"}},
        "state": {},
        "status": "completed",
    }
    with pytest.raises(ValueError, match="dag run config must not contain raw secret material"):
        write_public_docs_seed_registry_artifact(plan.to_canonical_json())

    plan.payload["seed_registry"][0]["crawl_policy"]["crawl_evidence"] = {
        "pages": {f"{page_url}?token=test-token": {"http_status": 200}},
        "state": {},
        "status": "completed",
    }
    with pytest.raises(ValueError, match="dag run config must not contain raw secret material"):
        write_public_docs_seed_registry_artifact(plan.to_canonical_json())


def test_public_docs_crawl_state_conf_overlays_persisted_state(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    state_path = tmp_path / "public-docs-crawl-state.json"
    state_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "artifact_type": "public_docs_crawl_state",
                "contract_version": "2026.07.2",
                "pack_id": PACK_ID,
                "seeds": {
                    "k3s-docs": {
                        "freshness_state": {
                            "last_success_at": "2026-07-08T20:30:00Z",
                            "page_state": {
                                "https://docs.k3s.io/": {
                                    "content_hash": "a" * 64,
                                    "etag": '"k3s-v1"',
                                    "http_status": 200,
                                    "last_modified": "Wed, 08 Jul 2026 20:00:00 GMT",
                                    "status": "active",
                                }
                            },
                            "status": "indexed",
                        }
                    }
                },
                "status": "active",
                "tenant_id": TENANT_ID,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    hydrated = load_public_docs_crawl_state_conf(conf)
    plan = build_public_docs_seed_refresh_plan(hydrated)

    k3s_seed = next(seed for seed in plan.payload["seed_registry"] if seed["seed_id"] == "k3s-docs")
    assert plan.payload["public_docs_crawl_state_path"] == str(state_path)
    assert k3s_seed["freshness_state"]["status"] == "indexed"
    assert k3s_seed["freshness_state"]["page_state"]["https://docs.k3s.io/"]["etag"] == '"k3s-v1"'
    assert (
        k3s_seed["crawl_policy"]["previous_state"]["https://docs.k3s.io/"]["content_hash"]
        == "a" * 64
    )


def test_public_docs_crawl_state_conf_recovers_active_version_from_bc21_when_snapshot_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["bc21_base_url"] = "http://serp-context-platform.env-dev.svc.cluster.local"
    token_path = tmp_path / "service-account.token"
    token_path.write_text("test-workload-token\n", encoding="utf-8")
    monkeypatch.setenv("ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH", str(token_path))
    recovered_pack_version_id = "018f5e13-2d73-7a77-a052-8d1bcbf96542"

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "activationRunId": "018f5e13-2d73-7a77-a052-8d1bcbf96543",
                    "activationState": "active",
                    "activatedAt": "2026-07-09T21:00:00Z",
                    "approvalRunId": "018f5e13-2d73-7a77-a052-8d1bcbf96544",
                    "evidenceBundleId": "018f5e13-2d73-7a77-a052-8d1bcbf96545",
                    "evidenceSealHash": "sha256:" + "a" * 64,
                    "indexedRunId": "018f5e13-2d73-7a77-a052-8d1bcbf96546",
                    "packId": PACK_ID,
                    "packVersionId": recovered_pack_version_id,
                    "tenantId": TENANT_ID,
                    "versionState": "active",
                }
            ).encode("utf-8")

    def fake_urlopen(request: Any, *, timeout: float) -> Response:
        assert timeout == 5.0
        assert request.get_method() == "GET"
        assert request.full_url == (
            "http://serp-context-platform.env-dev.svc.cluster.local"
            f"/api/bc-21/serp/v1/packs/{PACK_ID}/active-version"
        )
        assert request.get_header("X-adapstory-tenant-id") == TENANT_ID
        assert request.get_header("Authorization") == "Bearer test-workload-token"
        return Response()

    monkeypatch.setattr("dags.serp_eval_contracts.urlopen", fake_urlopen)

    hydrated = load_public_docs_crawl_state_conf(conf)
    plan = build_public_docs_seed_refresh_plan(hydrated)

    assert hydrated["active_pack_version_id"] == recovered_pack_version_id
    assert hydrated["public_docs_crawl_state_recovery"] == {
        "active_pack_version_id": recovered_pack_version_id,
        "activation_run_id": "018f5e13-2d73-7a77-a052-8d1bcbf96543",
        "method": "bc21_active_pack_resolution",
    }
    assert plan.payload["previous_active_pack_version_id"] == recovered_pack_version_id
    assert (
        plan.payload["public_docs_crawl_state_recovery"]
        == (hydrated["public_docs_crawl_state_recovery"])
    )


def test_public_docs_crawl_state_conf_fails_closed_when_bc21_active_version_is_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["bc21_base_url"] = "http://serp-context-platform.env-dev.svc.cluster.local"
    token_path = tmp_path / "service-account.token"
    token_path.write_text("test-workload-token\n", encoding="utf-8")
    monkeypatch.setenv("ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH", str(token_path))

    def unavailable(_request: object, *, timeout: float) -> object:
        raise TimeoutError("registry unavailable")

    monkeypatch.setattr("dags.serp_eval_contracts.urlopen", unavailable)

    with pytest.raises(ValueError, match="public docs active pack resolution failed"):
        load_public_docs_crawl_state_conf(conf)


def test_public_docs_crawl_state_conf_allows_true_first_activation_when_bc21_has_no_active_pack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["bc21_base_url"] = "http://serp-context-platform.env-dev.svc.cluster.local"
    token_path = tmp_path / "service-account.token"
    token_path.write_text("test-workload-token\n", encoding="utf-8")
    monkeypatch.setenv("ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH", str(token_path))

    def no_active_pack(_request: object, *, timeout: float) -> object:
        raise HTTPError(
            "http://serp-context-platform.env-dev.svc.cluster.local/active-version",
            409,
            "Conflict",
            Message(),
            io.BytesIO(),
        )

    monkeypatch.setattr("dags.serp_eval_contracts.urlopen", no_active_pack)

    hydrated = load_public_docs_crawl_state_conf(conf)
    plan = build_public_docs_seed_refresh_plan(hydrated)

    assert "active_pack_version_id" not in hydrated
    assert "public_docs_crawl_state_recovery" not in hydrated
    assert "previous_active_pack_version_id" not in plan.payload


def test_public_docs_crawl_state_commits_only_after_complete_d5_coverage(
    tmp_path: Path,
) -> None:
    refresh_plan_path = tmp_path / "refresh-plan.json"
    coverage_path = tmp_path / "coverage.json"
    activation_receipt_path = tmp_path / "activation-receipt.json"
    state_path = tmp_path / "public-docs-crawl-state.json"
    receipt_path = tmp_path / "crawl-state-commit-receipt.json"
    refresh_plan = {
        "generated_at": "2026-07-09T21:00:00Z",
        "seed_registry": [
            {
                "crawl_policy": {
                    "crawl_evidence": {
                        "state": {
                            "https://docs.example.com/guide": {
                                "content_hash": "b" * 64,
                                "etag": '"docs-v2"',
                                "http_status": 200,
                                "last_modified": "Thu, 09 Jul 2026 20:00:00 GMT",
                                "status": "active",
                            }
                        },
                        "status": "completed",
                    },
                    "previous_state": {},
                },
                "freshness_state": {"status": "never_indexed"},
                "seed_id": "example-docs",
                "source_uri": "https://docs.example.com/guide",
            }
        ],
    }
    refresh_plan_path.write_text(json.dumps(refresh_plan), encoding="utf-8")
    coverage_path.write_text(
        json.dumps(
            {
                "coverage_status": "complete",
                "pack_id": PACK_ID,
                "pack_version_id": PACK_VERSION_ID,
                "seeds": [
                    {
                        "index_status": "passed",
                        "seed_id": "example-docs",
                        "status": "published",
                    }
                ],
                "tenant_id": TENANT_ID,
            }
        ),
        encoding="utf-8",
    )
    activation_receipt_path.write_text(
        json.dumps({"active_pack_version_id": PACK_VERSION_ID, "status": "active"}),
        encoding="utf-8",
    )
    plan = {
        "artifact_paths": {
            "public_docs_coverage_proof": str(coverage_path),
            "public_docs_crawl_state_commit_receipt": str(receipt_path),
            "public_docs_publish_activation_receipt": str(activation_receipt_path),
        },
        "artifact_root_path": str(tmp_path),
        "dag_id": "serp_publish_signed_pack",
        "generated_at": "2026-07-09T21:00:00Z",
        "operation_id": "d5-crawl-state-test",
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "public_docs_crawl_state_path": str(state_path),
        "public_docs_seed_refresh_plan_path": str(refresh_plan_path),
        "tenant_id": TENANT_ID,
    }

    artifact = write_public_docs_crawl_state_artifact(plan)

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert artifact["payload"]["status"] == "committed"
    assert state["active_pack_version_id"] == PACK_VERSION_ID
    assert state["seeds"]["example-docs"]["freshness_state"]["status"] == "indexed"
    assert (
        state["seeds"]["example-docs"]["freshness_state"]["page_state"][
            "https://docs.example.com/guide"
        ]["etag"]
        == '"docs-v2"'
    )

    coverage_path.write_text(
        json.dumps({"coverage_status": "incomplete", "seeds": []}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="coverage proof must be complete"):
        write_public_docs_crawl_state_artifact(plan)


def test_public_docs_seed_refresh_frontier_budget_keeps_roots_and_skips_optional(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["frontier_budget"] = {
        "max_optional_frontier_per_seed": 2,
        "max_optional_frontier_sources": 3,
        "rotation_key": "day-1",
    }
    conf["seed_registry"][0]["crawl_policy"]["frontier_urls"] = [
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
        "https://docs.k3s.io/advanced",
        "https://docs.k3s.io/cli",
    ]
    conf["seed_registry"][2]["crawl_policy"]["frontier_urls"] = [
        "https://www.postgresql.org/docs/16/tutorial.html",
        "https://www.postgresql.org/docs/16/sql.html",
        "https://www.postgresql.org/docs/16/index.html",
        "https://www.postgresql.org/docs/16/libpq.html",
    ]
    conf["seed_registry"][0]["crawl_policy"]["curated_frontier_urls"] = []
    conf["seed_registry"][2]["crawl_policy"]["curated_frontier_urls"] = []

    def discover(source_uri: str, policy: Mapping[str, Any], max_urls: int) -> list[str]:
        del policy, max_urls
        if source_uri == "https://docs.k3s.io/":
            return [
                "https://docs.k3s.io/quick-start",
                "https://docs.k3s.io/installation/requirements",
                "https://docs.k3s.io/advanced",
                "https://docs.k3s.io/cli",
            ]
        if source_uri == "https://www.postgresql.org/docs/16/":
            return [
                "https://www.postgresql.org/docs/16/tutorial.html",
                "https://www.postgresql.org/docs/16/sql.html",
                "https://www.postgresql.org/docs/16/index.html",
                "https://www.postgresql.org/docs/16/libpq.html",
            ]
        return []

    plan = build_public_docs_seed_refresh_plan(conf, sitemap_frontier_discoverer=discover)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    payload = refresh_plan_artifact["payload"]
    requests = payload["source_fetch_requests"]
    root_requests = [
        request
        for request in requests
        if request["source_metadata"]["frontier"]["frontier_role"] == "seed-root"
    ]
    optional_requests = [
        request
        for request in requests
        if request["source_metadata"]["frontier"]["frontier_role"] == "sitemap-frontier"
    ]
    assert payload["frontier_budget"] == {
        "max_optional_frontier_per_seed": 2,
        "max_optional_frontier_sources": 3,
        "rotation_key": "day-1",
        "strategy": "seed-and-curated-required-rotating-optional-frontier-budget",
    }
    assert len(root_requests) == 4
    assert payload["optional_frontier_selected_count"] == 3
    assert len(optional_requests) == 3
    assert payload["seed_count"] == 7
    assert payload["skipped_frontier_count"] == 5
    assert {skipped["skip_reason"] for skipped in payload["skipped_frontier_fetches"]} == {
        "global_frontier_budget_exhausted",
        "per_seed_frontier_budget_exhausted",
    }
    assert all(
        skipped["frontier_role"] == "sitemap-frontier"
        for skipped in payload["skipped_frontier_fetches"]
    )
    assert {request["seed_id"] for request in root_requests} == {
        "adapstory-gitops-docs",
        "k3s-docs",
        "kubernetes-openapi-docs",
        "postgresql-reference-docs",
    }


def test_public_docs_seed_refresh_never_budgets_curated_frontier_urls(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["frontier_budget"] = {
        "max_optional_frontier_per_seed": 0,
        "max_optional_frontier_sources": 0,
        "rotation_key": "day-1",
    }
    k3s_policy = conf["seed_registry"][0]["crawl_policy"]
    k3s_policy["curated_frontier_urls"] = [
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
    ]
    k3s_policy["frontier_urls"] = [
        *k3s_policy["curated_frontier_urls"],
        "https://docs.k3s.io/advanced",
    ]

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())["payload"]
    k3s_requests = [
        request
        for request in refresh_plan["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]

    assert [request["source_uri"] for request in k3s_requests] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
    ]
    assert [
        request["source_metadata"]["frontier"]["frontier_role"] for request in k3s_requests
    ] == ["seed-root", "curated-frontier", "curated-frontier"]
    assert refresh_plan["optional_frontier_selected_count"] == 0
    k3s_skipped = [
        skipped
        for skipped in refresh_plan["skipped_frontier_fetches"]
        if skipped["parent_seed_id"] == "k3s-docs"
    ]
    assert k3s_skipped == []


def test_d20_writes_public_docs_publish_activation_trigger_conf_artifact(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["index_mode"] = "evidence-only"
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    _write_public_docs_seed_refresh_result(seed_refresh_result_path)
    _write_public_docs_bc21_pipeline_state_receipt(
        Path(plan.payload["artifact_paths"]["public_docs_bc21_pipeline_state_receipt"])
    )

    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    assert Path(trigger_artifact["artifactPath"]).exists()
    assert trigger_artifact["artifactType"] == "public_docs_publish_activation_trigger_conf"
    assert (
        trigger_artifact["artifactPath"]
        == plan.payload["artifact_paths"]["public_docs_publish_activation_trigger_conf"]
    )
    payload = trigger_artifact["payload"]
    assert payload["status"] == "governance_inputs_required"
    assert payload["target_dag_id"] == "serp_publish_signed_pack"
    assert payload["d5_publish_target"] == "serp_publish_signed_pack"
    assert payload["source_seed_refresh_result_path"] == str(seed_refresh_result_path)
    assert payload["governance_required_fields"] == ["bc21_base_url"]
    target_conf = payload["target_dag_run_conf"]
    assert set(target_conf) == {
        "activation_idempotency_key",
        "activation_reason_code",
        "actor_id",
        "approval_idempotency_key",
        "artifact_root_path",
        "benchmark_gate_export_sha256",
        "evidence_bundle_id",
        "evidence_seal_hash",
        "generated_at",
        "pack_id",
        "pack_version_id",
        "qdrant_collection",
        "opensearch_index",
        "neo4j_database",
        "policy_data_class",
        "policy_freshness_state",
        "policy_license_obligation_state",
        "policy_source_type",
        "policy_trust_state",
        "policy_version",
        "public_docs_crawl_state_path",
        "public_docs_seed_refresh_plan_path",
        "public_docs_seed_refresh_result_path",
        "registry_resource_id",
        "registry_resource_type",
        "tenant_id",
    }
    assert target_conf["activation_reason_code"] == "public-docs-d20-indexed"
    assert target_conf["actor_id"] == "airflow-serp-public-docs-refresh"
    assert target_conf["artifact_root_path"] == str(tmp_path)
    assert target_conf["generated_at"] == "2026-07-08T21:00:00Z"
    assert target_conf["pack_id"] == PACK_ID
    assert target_conf["pack_version_id"] == PACK_VERSION_ID
    assert target_conf["public_docs_crawl_state_path"] == str(
        tmp_path / "public-docs-crawl-state.json"
    )
    assert (
        target_conf["public_docs_seed_refresh_plan_path"]
        == plan.payload["artifact_paths"]["public_docs_seed_refresh_plan"]
    )
    assert target_conf["public_docs_seed_refresh_result_path"] == str(seed_refresh_result_path)
    assert target_conf["registry_resource_id"] == REGISTRY_RESOURCE_ID
    assert target_conf["registry_resource_type"] == "pack"
    assert target_conf["tenant_id"] == TENANT_ID
    assert target_conf["policy_source_type"] == "website"
    assert target_conf["policy_data_class"] == "PUBLIC"
    assert target_conf["policy_license_obligation_state"] == "public_share_allowed"
    assert target_conf["policy_trust_state"] == "trusted"
    assert target_conf["policy_freshness_state"] == "fresh"
    assert target_conf["evidence_bundle_id"] == "018f5e13-2d73-7a77-a052-8d1bcbf96602"
    assert target_conf["evidence_seal_hash"] == "sha256:" + "b" * 64
    repeated_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        json.loads(plan.to_canonical_json())
    )
    assert trigger_artifact["artifactSha256"] == repeated_artifact["artifactSha256"]


def test_d20_trigger_conf_includes_bc21_base_url_from_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    _write_public_docs_seed_refresh_result(seed_refresh_result_path)
    _write_public_docs_bc21_pipeline_state_receipt(
        Path(plan.payload["artifact_paths"]["public_docs_bc21_pipeline_state_receipt"])
    )

    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    assert plan.payload["bc21_base_url"] == (
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080"
    )
    assert trigger_artifact["payload"]["status"] == "ready_for_d5_publish_activation"
    assert trigger_artifact["payload"]["governance_required_fields"] == []
    assert trigger_artifact["payload"]["target_dag_run_conf"]["bc21_base_url"] == (
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080"
    )


def test_d20_trigger_conf_derives_policy_from_frontier_parent_seed(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    batch_evidence = _public_docs_seed_refresh_batch_evidence(status="indexed")
    cast(list[dict[str, object]], batch_evidence["source_results"]).append(
        {
            "chunk_ids": ["chunk-k3s-frontier"],
            "embedding_ids": ["embedding-k3s-frontier"],
            "metadata": {
                "chunk_count": 1,
                "embedding_count": 1,
                "frontier": {
                    "frontier_role": "sitemap-frontier",
                    "parent_seed_id": "k3s-docs",
                    "source_uri_hash": "sha256:" + "1" * 64,
                },
            },
            "pipeline_evidence_sha256": "2" * 64,
            "pipeline_operation_id": "public-docs-frontier-refresh-test",
            "pipeline_run_id": "018f5e13-2d73-7a77-a052-8d1bcbf96543",
            "pipeline_status": "indexed",
            "post_index_state": "activation_pending",
            "seed_id": "k3s-docs--111111111111",
        }
    )
    seed_refresh_result_path.write_text(
        json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": batch_evidence,
                "batch_evidence_sha256": sha256(
                    json.dumps(
                        batch_evidence,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                ).hexdigest(),
                "coverage_proof": {
                    "coverage_status": "indexed_pending_publish",
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    _write_public_docs_bc21_pipeline_state_receipt(
        Path(plan.payload["artifact_paths"]["public_docs_bc21_pipeline_state_receipt"])
    )

    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    target_conf = trigger_artifact["payload"]["target_dag_run_conf"]
    assert target_conf["policy_source_type"] == "website"
    assert target_conf["policy_data_class"] == "PUBLIC"
    assert target_conf["policy_license_obligation_state"] == "public_share_allowed"
    assert target_conf["policy_trust_state"] == "trusted"
    assert target_conf["policy_freshness_state"] == "fresh"


def test_d20_writes_public_docs_publish_activation_trigger_conf_from_s3_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    trigger_conf_path = plan.payload["artifact_paths"][
        "public_docs_publish_activation_trigger_conf"
    ]
    receipt_path = plan.payload["artifact_paths"]["public_docs_bc21_pipeline_state_receipt"]
    result_bucket, result_key = seed_refresh_result_path.removeprefix("s3://").split("/", 1)
    receipt_bucket, receipt_key = receipt_path.removeprefix("s3://").split("/", 1)
    trigger_bucket, trigger_key = trigger_conf_path.removeprefix("s3://").split("/", 1)
    batch_evidence = _public_docs_seed_refresh_batch_evidence(status="indexed")
    storage = {
        (result_bucket, result_key): json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": batch_evidence,
                "batch_evidence_sha256": sha256(
                    json.dumps(
                        batch_evidence,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                ).hexdigest(),
                "coverage_proof": {
                    "coverage_status": "indexed_pending_publish",
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ).encode("utf-8"),
        (receipt_bucket, receipt_key): json.dumps(
            _public_docs_bc21_pipeline_state_receipt(),
            sort_keys=True,
        ).encode("utf-8"),
    }
    put_calls: list[tuple[str, str, str, str]] = []

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    assert trigger_artifact["artifactPath"] == trigger_conf_path
    assert trigger_artifact["payload"]["source_seed_refresh_result_path"] == (
        seed_refresh_result_path
    )
    assert put_calls
    assert put_calls[0][0] == trigger_bucket
    assert put_calls[0][1] == trigger_key
    assert put_calls[0][3] == "application/json"


def test_d20_bc21_pipeline_state_submit_accepts_quarantined_publishable_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["bc21_base_url"] = "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080"
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    batch_evidence = _public_docs_seed_refresh_batch_evidence(
        status="indexed_with_quarantined_failures"
    )
    seed_refresh_result_path.write_text(
        json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": batch_evidence,
                "batch_evidence_sha256": sha256(
                    json.dumps(
                        batch_evidence,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                ).hexdigest(),
                "coverage_proof": {
                    "coverage_status": "indexed_pending_publish",
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    submission_calls: list[dict[str, Any]] = []

    class FakeBC21PipelineState:
        @staticmethod
        def build_public_docs_batch_pipeline_state_submission(
            refresh_result: Mapping[str, Any],
            *,
            actor_id: str,
            catalog_source_id: UUID,
            started_at: object,
        ) -> dict[str, Any]:
            submission_calls.append(
                {
                    "actor_id": actor_id,
                    "catalog_source_id": str(catalog_source_id),
                    "status": refresh_result["batch_evidence"]["status"],
                    "started_at": started_at,
                }
            )
            return {"submission": "ok"}

        @staticmethod
        def submit_pipeline_state_submission(
            submission: Mapping[str, Any],
            *,
            bc21_base_url: str,
        ) -> dict[str, Any]:
            return {
                "bc21_base_url": bc21_base_url,
                "pipeline_state_submission": submission,
                "status": "accepted",
            }

    def fake_import_module(name: str) -> object:
        if name == "adapstory_serp_pipeline.registry.bc21_pipeline_state":
            return FakeBC21PipelineState
        return importlib.import_module(name)

    monkeypatch.setattr("dags.serp_eval_contracts.importlib.import_module", fake_import_module)
    monkeypatch.setattr(
        "dags.serp_eval_contracts._ensure_public_docs_catalog_source",
        lambda _plan, *, bc21_base_url: "018f5e13-2d73-7a77-a052-8d1bcbf96599",
    )

    receipt_artifact = submit_public_docs_bc21_pipeline_state_artifact(plan.to_canonical_json())

    assert submission_calls == [
        {
            "actor_id": "airflow-serp-public-docs-refresh",
            "catalog_source_id": "018f5e13-2d73-7a77-a052-8d1bcbf96599",
            "status": "indexed_with_quarantined_failures",
            "started_at": submission_calls[0]["started_at"],
        }
    ]
    assert receipt_artifact["payload"]["status"] == "accepted"
    assert Path(receipt_artifact["artifactPath"]).exists()


def test_d20_default_conf_rejects_unsafe_env_bc21_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_SERP_BC21_BASE_URL", "http://example.invalid")

    with pytest.raises(ValueError, match="bc21_base_url must use https"):
        default_public_docs_seed_refresh_conf(generated_at="2026-07-08T21:00:00Z")


def test_d20_trigger_conf_rejects_invalid_seed_refresh_result(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_seed_refresh_plan(conf)

    with pytest.raises(ValueError, match="public_docs_seed_refresh_result file is not readable"):
        write_public_docs_publish_activation_trigger_conf_artifact(plan.to_canonical_json())

    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    seed_refresh_result_path.write_text(
        json.dumps(
            {
                "artifact_type": "unsupported",
                "batch_evidence": {
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="artifact_type is unsupported"):
        write_public_docs_publish_activation_trigger_conf_artifact(plan.to_canonical_json())

    _write_public_docs_seed_refresh_result(
        seed_refresh_result_path,
        tenant_id="00000000-0000-4000-a000-000000000099",
    )
    with pytest.raises(ValueError, match="identity must match tenant_id"):
        write_public_docs_publish_activation_trigger_conf_artifact(plan.to_canonical_json())


def test_d5_still_requires_governance_inputs_from_d20_trigger_conf(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    _write_public_docs_seed_refresh_result(seed_refresh_result_path)
    _write_public_docs_bc21_pipeline_state_receipt(
        Path(plan.payload["artifact_paths"]["public_docs_bc21_pipeline_state_receipt"])
    )
    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    with pytest.raises(ValueError, match="bc21_base_url is required"):
        build_public_docs_publish_activation_plan(
            trigger_artifact["payload"]["target_dag_run_conf"]
        )


def test_public_docs_seed_refresh_uses_single_website_request_when_frontier_disabled(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["crawl_policy"]["sitemap_discovery"] = False
    conf["seed_registry"][0]["crawl_policy"]["curated_frontier_urls"] = []
    conf["seed_registry"][0]["crawl_policy"].pop("frontier_urls", None)

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert len(website_requests) == 1
    assert website_requests[0]["seed_id"] == "k3s-docs"
    assert website_requests[0]["source_uri"] == "https://docs.k3s.io/"


def test_public_docs_seed_refresh_allows_manual_frontier_without_sitemap_discovery(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["crawl_policy"]["sitemap_discovery"] = False

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert [request["source_uri"] for request in website_requests] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
    ]


def test_public_docs_seed_refresh_deduplicates_canonical_frontier_urls(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    k3s_policy = conf["seed_registry"][0]["crawl_policy"]
    k3s_policy["curated_frontier_urls"] = [
        "https://docs.k3s.io",
        "https://docs.k3s.io/#overview",
        "https://docs.k3s.io/quick-start#install",
        "https://docs.k3s.io/quick-start",
    ]
    k3s_policy["frontier_urls"] = k3s_policy["curated_frontier_urls"]

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert [request["source_uri"] for request in website_requests] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start#install",
    ]


def test_public_docs_seed_refresh_expands_sitemap_discovered_frontier_urls(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["crawl_policy"]["max_pages"] = 4

    def discover(source_uri: str, policy: Mapping[str, Any], max_urls: int) -> list[str]:
        if source_uri != "https://docs.k3s.io/":
            return []
        assert policy["sitemap_discovery"] is True
        assert max_urls == 3
        return ["https://docs.k3s.io/advanced"]

    plan = build_public_docs_seed_refresh_plan(conf, sitemap_frontier_discoverer=discover)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert [request["source_uri"] for request in website_requests] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
        "https://docs.k3s.io/advanced",
    ]
    assert website_requests[-1]["source_metadata"]["frontier"]["frontier_role"] == (
        "sitemap-frontier"
    )
    assert website_requests[-1]["source_metadata"]["frontier"]["frontier_url_count"] == 4


def test_public_docs_sitemap_discovery_respects_optional_frontier_budget(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["frontier_budget"] = {
        "max_optional_frontier_per_seed": 2,
        "max_optional_frontier_sources": 40,
        "rotation_key": "day-1",
    }
    conf["seed_registry"][0]["crawl_policy"]["frontier_urls"] = []
    conf["seed_registry"][0]["crawl_policy"]["curated_frontier_urls"] = []
    observed_max_urls: list[int] = []

    def discover(source_uri: str, policy: Mapping[str, Any], max_urls: int) -> list[str]:
        if source_uri != "https://docs.k3s.io/":
            return []
        assert policy["sitemap_discovery"] is True
        observed_max_urls.append(max_urls)
        return [
            "https://docs.k3s.io/quick-start",
            "https://docs.k3s.io/installation/requirements",
            "https://docs.k3s.io/advanced",
        ]

    plan = build_public_docs_seed_refresh_plan(conf, sitemap_frontier_discoverer=discover)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert observed_max_urls == [conf["seed_registry"][0]["crawl_policy"]["max_pages"] - 1]
    assert [request["source_uri"] for request in website_requests] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/advanced",
    ]
    assert (
        sum(
            1
            for request in website_requests
            if request["source_metadata"]["frontier"]["frontier_role"] == "sitemap-frontier"
        )
        == 2
    )


def test_public_docs_seed_refresh_crawls_for_evidence_without_remaining_frontier_capacity(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["crawl_policy"]["max_pages"] = 3
    called_sources: list[str] = []

    def discover(source_uri: str, policy: Mapping[str, Any], max_urls: int) -> list[str]:
        if source_uri != "https://docs.k3s.io/":
            return []
        called_sources.append(source_uri)
        assert policy["max_pages"] == 3
        assert max_urls == 0
        return ["https://docs.k3s.io/advanced"]

    plan = build_public_docs_seed_refresh_plan(conf, sitemap_frontier_discoverer=discover)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    assert called_sources == ["https://docs.k3s.io/"]
    assert [
        request["source_uri"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
    ]


def test_public_docs_seed_refresh_refuses_to_dispatch_a_quarantined_crawl(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)

    def discover(source_uri: str, _policy: Mapping[str, Any], _max_urls: int) -> Mapping[str, Any]:
        if source_uri != "https://docs.k3s.io/":
            return {"evidence": None, "urls": []}
        return {
            "evidence": {
                "failure": {"code": "HTTP_429", "message": "rate limited"},
                "status": "quarantined",
            },
            "urls": [],
        }

    plan = build_public_docs_seed_refresh_plan(conf, sitemap_frontier_discoverer=discover)

    with pytest.raises(ValueError, match="crawler evidence must be completed"):
        write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())


def test_public_docs_seed_refresh_rebuilds_every_seed_when_any_seed_is_due(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["freshness_state"] = {
        "last_success_at": "2026-07-08T12:00:00Z",
        "status": "indexed",
    }
    conf["seed_registry"][1]["freshness_state"] = {
        "last_success_at": "2026-07-07T20:00:00Z",
        "status": "indexed",
    }

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())
    payload = refresh_plan_artifact["payload"]

    assert payload["status"] == "ready_for_pipeline_dispatch"
    assert payload["candidate_rebuild_mode"] == "full_pack"
    assert payload["seed_count"] == 9
    assert payload["skipped_seed_count"] == 0
    assert payload["skipped_seed_refreshes"] == []
    assert {
        request["seed_id"]
        for request in payload["source_fetch_requests"]
        if "--" not in request["seed_id"]
    } == {
        "adapstory-gitops-docs",
        "k3s-docs",
        "kubernetes-openapi-docs",
        "postgresql-reference-docs",
    }
    assert {
        request["source_metadata"]["refresh_selection"]["reason"]
        for request in payload["source_fetch_requests"]
    } == {"max_age_exceeded", "never_indexed", "within_max_age"}

    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    assert cli_spec["status"] == "ready_for_pipeline_cli_runner"
    assert cli_spec["seed_count"] == 9
    assert cli_spec["skipped_seed_count"] == 0


def test_public_docs_seed_refresh_dispatches_live_index_mode(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["index_mode"] = "live"
    conf["qdrant_collection"] = "serp_vectors_prod"
    conf["opensearch_index"] = "serp_lexical_prod"
    conf["neo4j_database"] = "neo4j"

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())
    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    assert plan.payload["index_mode"] == "live"
    assert plan.payload["embedding_mode"] == "live-gateway"
    assert plan.payload["qdrant_collection"] == "serp_vectors_prod"
    assert plan.payload["opensearch_index"] == "serp_lexical_prod"
    assert plan.payload["neo4j_database"] == "neo4j"
    assert refresh_plan_artifact["payload"]["index_mode"] == "live"
    assert refresh_plan_artifact["payload"]["embedding_mode"] == "live-gateway"
    assert cli_spec["argv"][cli_spec["argv"].index("--index-mode") + 1] == "live"
    assert cli_spec["argv"][cli_spec["argv"].index("--embedding-mode") + 1] == "live-gateway"
    assert (
        cli_spec["argv"][cli_spec["argv"].index("--qdrant-collection") + 1] == "serp_vectors_prod"
    )
    assert cli_spec["argv"][cli_spec["argv"].index("--opensearch-index") + 1] == "serp_lexical_prod"
    assert cli_spec["argv"][cli_spec["argv"].index("--neo4j-database") + 1] == "neo4j"


def test_public_docs_seed_refresh_noops_when_no_seed_is_due(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    for seed in conf["seed_registry"]:
        seed["freshness_state"] = {
            "last_success_at": "2026-07-08T20:30:00Z",
            "status": "indexed",
        }

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())
    payload = refresh_plan_artifact["payload"]

    assert payload["status"] == "no_due_sources"
    assert payload["seed_count"] == 0
    assert payload["source_fetch_requests"] == []
    assert payload["skipped_seed_count"] == 4

    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    assert cli_spec["status"] == "no_due_sources"
    assert cli_spec["argv"] == []
    assert cli_spec["seed_count"] == 0
    assert cli_spec["skipped_seed_count"] == 4

    result = execute_pipeline_cli_spec(cli_spec)

    assert Path(result["artifactPath"]).exists()
    assert result["payload"]["status"] == "no_due_sources"
    assert result["payload"]["skipped_seed_count"] == 4


def test_force_full_revalidation_requires_reason_and_rebuilds_fresh_public_docs(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    for seed in conf["seed_registry"]:
        seed["freshness_state"] = {
            "last_success_at": "2026-07-08T20:30:00Z",
            "status": "indexed",
        }
    conf["refresh_mode"] = "scheduled"
    conf["refresh_reason"] = "unexpected scheduled override"

    with pytest.raises(ValueError, match="scheduled refresh must not include refresh_reason"):
        build_public_docs_seed_refresh_plan(conf)

    conf["refresh_mode"] = "force_full"
    conf.pop("refresh_reason")

    with pytest.raises(ValueError, match="force_full refresh requires refresh_reason"):
        build_public_docs_seed_refresh_plan(conf)

    conf["refresh_reason"] = "post-rollback D5 contract revalidation"
    plan = build_public_docs_seed_refresh_plan(conf)
    artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    assert plan.payload["refresh_mode"] == "force_full"
    assert plan.payload["refresh_reason"] == "post-rollback D5 contract revalidation"
    assert artifact["payload"]["status"] == "ready_for_pipeline_dispatch"
    assert artifact["payload"]["candidate_rebuild_mode"] == "full_pack"
    assert artifact["payload"]["refresh_mode"] == "force_full"
    assert artifact["payload"]["refresh_reason"] == "post-rollback D5 contract revalidation"
    assert {
        request["source_metadata"]["refresh_selection"]["reason"]
        for request in artifact["payload"]["source_fetch_requests"]
    } == {"forced_revalidation"}


def test_public_docs_noop_retains_active_pack_without_bc21_or_d5_publish(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    state_path = tmp_path / "public-docs-crawl-state.json"
    state_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "artifact_type": "public_docs_crawl_state",
                "contract_version": "2026.07.2",
                "pack_id": PACK_ID,
                "seeds": {
                    seed["seed_id"]: {
                        "freshness_state": {
                            "last_success_at": "2026-07-08T20:30:00Z",
                            "status": "indexed",
                        }
                    }
                    for seed in conf["seed_registry"]
                },
                "status": "active",
                "tenant_id": TENANT_ID,
            }
        ),
        encoding="utf-8",
    )
    plan = build_public_docs_seed_refresh_plan(load_public_docs_crawl_state_conf(conf))

    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())
    execute_pipeline_cli_spec(cli_spec)
    bc21_receipt = submit_public_docs_bc21_pipeline_state_artifact(plan.to_canonical_json())
    trigger = write_public_docs_publish_activation_trigger_conf_artifact(plan.to_canonical_json())

    assert plan.payload["previous_active_pack_version_id"] == PACK_VERSION_ID
    assert bc21_receipt["payload"]["status"] == "not_submitted_no_change"
    assert trigger["payload"]["status"] == "no_change_active_pack_retained"
    assert "target_dag_run_conf" not in trigger["payload"]


def test_public_docs_publish_activation_plan_dispatches_d5_handoff(tmp_path: Path) -> None:
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)

    plan = build_public_docs_publish_activation_plan(conf)
    repeated = build_public_docs_publish_activation_plan(json.loads(plan.to_canonical_json()))
    cli_spec = build_public_docs_publish_activation_cli_spec(plan.to_canonical_json())

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_publish_signed_pack"
    assert plan.payload["status"] == "ready_for_publish_activation_handoff"
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "airflow-plan.json")
        ),
        "public_docs_publish_activation_request": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-publish-activation-request.json",
            )
        ),
        "public_docs_publish_activation_receipt": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-publish-activation-receipt.json",
            )
        ),
        "public_docs_search_serve_smoke": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-search-serve-smoke.json",
            )
        ),
        "public_docs_retrieval_golden": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-retrieval-golden.json",
            )
        ),
        "public_docs_post_activation_rollback": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-post-activation-rollback.json",
            )
        ),
        "public_docs_coverage_proof": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-coverage-proof.json",
            )
        ),
        "public_docs_crawl_state_commit_receipt": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-crawl-state-commit-receipt.json",
            )
        ),
        "public_docs_retired_pack_cleanup": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-retired-pack-cleanup.json",
            )
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
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
    ]
    assert cli_spec["status"] == "ready_for_pipeline_cli_runner"
    assert cli_spec["task_id"] == "public_docs_publish_activation_handoff"
    assert cli_spec["d5_publish_target"] == "serp_publish_signed_pack"
    assert cli_spec["input_paths"] == [str(seed_refresh_result)]
    assert (
        cli_spec["stdout_path"]
        == plan.payload["artifact_paths"]["public_docs_publish_activation_request"]
    )
    assert cli_spec["argv"][:3] == [
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.publish_activation_cli",
    ]
    assert cli_spec["argv"][cli_spec["argv"].index("--seed-refresh-result") + 1] == str(
        seed_refresh_result
    )
    assert cli_spec["argv"][cli_spec["argv"].index("--benchmark-gate-export-sha256") + 1] == (
        "sha256:" + "c" * 64
    )
    request_artifact_path = Path(
        plan.payload["artifact_paths"]["public_docs_publish_activation_request"]
    )
    request_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    request_artifact_path.write_text(
        '{"artifact_type":"public_docs_publish_activation_submission"}',
        encoding="utf-8",
    )
    submit_spec = build_public_docs_publish_activation_submit_cli_spec(plan.to_canonical_json())
    assert submit_spec["status"] == "ready_for_pipeline_cli_runner"
    assert submit_spec["task_id"] == "public_docs_publish_activation_submit"
    assert submit_spec["input_paths"] == [
        plan.payload["artifact_paths"]["public_docs_publish_activation_request"]
    ]
    assert (
        submit_spec["stdout_path"]
        == plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"]
    )
    assert submit_spec["argv"][:4] == [
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.publish_activation_cli",
        "submit",
    ]
    assert submit_spec["argv"][submit_spec["argv"].index("--bc21-base-url") + 1] == (
        "http://serp-context-platform.env-dev.svc.cluster.local"
    )


def test_post_activation_failure_rolls_back_to_direct_predecessor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    previous_pack_version_id = "018f5e13-2d73-7a77-a052-8d1bcbf96542"
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)
    conf["previous_active_pack_version_id"] = previous_pack_version_id
    plan = build_public_docs_publish_activation_plan(conf)
    receipt_path = Path(plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "artifact_type": "public_docs_publish_activation_receipt",
                "pack_id": PACK_ID,
                "status": "activated",
                "tenant_id": TENANT_ID,
            }
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, object]] = []

    def fake_bc21_json_request(
        url: str,
        *,
        method: str,
        body: Mapping[str, object] | None,
        headers: Mapping[str, str],
        **_kwargs: object,
    ) -> dict[str, object]:
        calls.append(
            {"body": dict(body or {}), "headers": dict(headers), "method": method, "url": url}
        )
        return {
            "failedPackVersionId": PACK_VERSION_ID,
            "packId": PACK_ID,
            "rollbackReasonCode": "public-docs-d5-post-activation-validation-failed",
            "rollbackRunId": "018f5e13-2d73-7a77-a052-8d1bcbf96609",
            "rolledBackAt": "2026-07-08T22:03:00Z",
            "rolledBackBy": "airflow-serp-public-docs-refresh",
            "restoredPackVersionId": previous_pack_version_id,
            "tenantId": TENANT_ID,
        }

    monkeypatch.setattr("dags.serp_eval_contracts._bc21_json_request", fake_bc21_json_request)

    artifact = write_public_docs_post_activation_rollback_artifact(plan.to_canonical_json())

    assert calls == [
        {
            "body": {
                "failedPackVersionId": PACK_VERSION_ID,
                "restoredPackVersionId": previous_pack_version_id,
                "rollbackReasonCode": "public-docs-d5-post-activation-validation-failed",
            },
            "headers": {
                "X-Adapstory-Actor-Id": "airflow-serp-public-docs-refresh",
                "X-Adapstory-Tenant-Id": TENANT_ID,
                "X-Fingerprint": artifact["payload"]["fingerprint"],
                "X-Idempotency-Key": artifact["payload"]["idempotency_key"],
            },
            "method": "POST",
            "url": (
                "http://serp-context-platform.env-dev.svc.cluster.local"
                f"/api/bc-21/serp/v1/packs/{PACK_ID}/publish-rollbacks"
            ),
        }
    ]
    assert artifact["artifactType"] == "public_docs_post_activation_rollback"
    assert artifact["payload"]["status"] == "rolled_back_to_previous_active_pack"
    assert artifact["payload"]["restored_pack_version_id"] == previous_pack_version_id
    assert Path(artifact["artifactPath"]).exists()


def test_first_activation_failure_records_unrecoverable_compensation_without_retries(
    tmp_path: Path,
) -> None:
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_publish_activation_plan(conf)
    receipt_path = Path(plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "artifact_type": "public_docs_publish_activation_receipt",
                "pack_id": PACK_ID,
                "status": "activated",
                "tenant_id": TENANT_ID,
            }
        ),
        encoding="utf-8",
    )

    artifact = write_public_docs_post_activation_rollback_artifact(plan.to_canonical_json())

    assert artifact["payload"]["status"] == "first_activation_no_restore_target"
    assert artifact["payload"]["rollback_attempted"] is False
    assert artifact["payload"]["active_pack_version_id"] == PACK_VERSION_ID
    assert Path(artifact["artifactPath"]).exists()


def test_public_docs_retired_pack_cleanup_runs_only_for_a_previous_active_pack(
    tmp_path: Path,
) -> None:
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)
    plan_without_retired_pack = build_public_docs_publish_activation_plan(conf)

    no_cleanup = build_public_docs_retired_pack_cleanup_cli_spec(
        plan_without_retired_pack.to_canonical_json()
    )
    assert no_cleanup["status"] == "retired_pack_cleanup_not_required"
    assert no_cleanup["argv"] == []
    assert execute_pipeline_cli_spec(no_cleanup)["payload"]["status"] == "not_required"

    conf["previous_active_pack_version_id"] = "018f5e13-2d73-7a77-a052-8d1bcbf96542"
    plan = build_public_docs_publish_activation_plan(conf)
    cleanup = build_public_docs_retired_pack_cleanup_cli_spec(plan.to_canonical_json())

    assert cleanup["status"] == "ready_for_pipeline_cli_runner"
    assert cleanup["argv"][cleanup["argv"].index("--index-mode") + 1] == "live"
    assert cleanup["argv"][cleanup["argv"].index("--active-pack-version-id") + 1] == (
        PACK_VERSION_ID
    )
    assert cleanup["argv"][cleanup["argv"].index("--retired-pack-version-id") + 1] == (
        "018f5e13-2d73-7a77-a052-8d1bcbf96542"
    )


def test_public_docs_coverage_proof_artifact_finalizes_d20_after_d5(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_src = REPO_ROOT.parent / "adapstory-serp-pipeline" / "src"
    monkeypatch.syspath_prepend(str(pipeline_src))
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    indexed_proof = {
        "artifact_type": "public_docs_coverage_proof",
        "coverage_status": "indexed_pending_publish",
        "coverage_proof_version": "2026.07.1",
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "publish": {"status": "pending", "reason": "D5_PUBLISH_RECEIPT_MISSING"},
        "summary": {
            "expected_seed_count": 1,
            "failed_seed_count": 0,
            "fully_indexed_seed_count": 1,
            "fully_published_seed_count": 0,
            "missing_seed_count": 0,
            "partial_seed_count": 0,
        },
        "seeds": [
            {
                "counts": {
                    "chunks": 1,
                    "documents": 1,
                    "embeddings": 1,
                    "neo4j": 1,
                    "opensearch": 1,
                    "qdrant": 1,
                    "sections": 1,
                },
                "failure": {"code": None, "message": None},
                "index_status": "passed",
                "optional_frontier": [],
                "required_frontier": [],
                "seed_id": "k3s-docs",
                "source_id": "source-k3s",
                "source_type": "website",
                "source_uri": "https://docs.k3s.io/",
                "stages": {
                    stage: {"reason": "indexed", "status": "passed"}
                    for stage in ("fetch", "parse", "chunk", "embed", "index")
                },
                "status": "indexed",
                "targets": {
                    target: {"count": 1, "operation_id": f"{target}-op", "status": "passed"}
                    for target in ("qdrant", "opensearch", "neo4j")
                },
            }
        ],
        "tenant_id": TENANT_ID,
    }
    seed_refresh_result.write_text(
        json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": {
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
                "coverage_proof": indexed_proof,
            }
        ),
        encoding="utf-8",
    )
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_publish_activation_plan(conf)
    receipt_path = Path(plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "pack_id": PACK_ID,
                "status": "activated",
            }
        ),
        encoding="utf-8",
    )

    artifact = write_public_docs_coverage_proof_artifact(plan.to_canonical_json())

    assert artifact["payload"]["coverage_status"] == "complete"
    assert artifact["payload"]["seeds"][0]["status"] == "published"
    assert Path(artifact["artifactPath"]).exists()


def test_public_docs_publish_activation_writes_search_serve_smoke_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)
    conf["search_serve_base_url"] = (
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
    )
    plan = build_public_docs_publish_activation_plan(conf)
    receipt_path = Path(plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "artifact_type": "public_docs_publish_activation_receipt",
                "pack_id": PACK_ID,
                "status": "activated",
                "tenant_id": TENANT_ID,
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            return json.dumps(
                {
                    "api_version": "serp.search.v1",
                    "mode": "search_then_retrieve",
                    "result_count": 1,
                    "result_cards": [{"chunk_id": "chunk-k3s"}],
                    "selected_pack_version_ids": [PACK_VERSION_ID],
                }
            ).encode("utf-8")

    def fake_urlopen(request: Any, timeout: int) -> FakeResponse:
        captured["attempts"] = captured.get("attempts", 0) + 1
        captured["url"] = request.full_url
        captured["body"] = json.loads(request.data.decode("utf-8"))
        captured["timeout"] = timeout
        if captured["attempts"] == 1:
            raise HTTPError(
                request.full_url,
                503,
                "Service Unavailable",
                cast(Any, {}),
                io.BytesIO(b'{"detail":"transient"}'),
            )
        return FakeResponse()

    monkeypatch.setattr("dags.serp_eval_contracts.urlopen", fake_urlopen)

    artifact = write_public_docs_search_serve_smoke_artifact(plan.to_canonical_json())

    assert captured["url"].endswith("/api/serp/search/v1/query")
    assert captured["attempts"] == 2
    assert "selected_pack_version_ids" not in captured["body"]
    assert captured["body"]["auth_subject_type"] == "service"
    assert captured["body"]["tenant_scope"] == "public"
    assert captured["body"]["metadata"]["expected_pack_version_id"] == PACK_VERSION_ID
    assert artifact["artifactType"] == "public_docs_search_serve_smoke"
    assert artifact["payload"]["status"] == "served_active_pack"
    assert artifact["payload"]["selected_pack_version_ids"] == [PACK_VERSION_ID]
    assert Path(artifact["artifactPath"]).exists()


def test_public_docs_search_serve_smoke_retries_transient_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)
    conf["search_serve_base_url"] = (
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
    )
    plan = build_public_docs_publish_activation_plan(conf)
    receipt_path = Path(plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "artifact_type": "public_docs_publish_activation_receipt",
                "pack_id": PACK_ID,
                "status": "activated",
                "tenant_id": TENANT_ID,
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    class FakeResponse:
        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            return json.dumps(
                {
                    "api_version": "serp.search.v1",
                    "mode": "search_then_retrieve",
                    "result_count": 1,
                    "result_cards": [{"chunk_id": "chunk-k3s"}],
                    "selected_pack_version_ids": [PACK_VERSION_ID],
                }
            ).encode("utf-8")

    def fake_urlopen(request: Any, timeout: int) -> FakeResponse:
        captured["attempts"] = captured.get("attempts", 0) + 1
        if captured["attempts"] == 1:
            raise TimeoutError("timed out")
        return FakeResponse()

    monkeypatch.setattr("dags.serp_eval_contracts.urlopen", fake_urlopen)

    artifact = write_public_docs_search_serve_smoke_artifact(plan.to_canonical_json())

    assert captured["attempts"] == 2
    assert artifact["artifactType"] == "public_docs_search_serve_smoke"
    assert artifact["payload"]["status"] == "served_active_pack"


def test_public_docs_retrieval_golden_runs_thirty_live_contract_cases_with_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T22:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    refresh_plan = build_public_docs_seed_refresh_plan(refresh_conf)
    write_public_docs_seed_refresh_plan_artifact(refresh_plan.to_canonical_json())
    seed_refresh_result = Path(
        refresh_plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)
    conf["public_docs_seed_refresh_plan_path"] = refresh_plan.payload["artifact_paths"][
        "public_docs_seed_refresh_plan"
    ]
    plan = build_public_docs_publish_activation_plan(conf)
    receipt_path = Path(plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"])
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(
        json.dumps(
            {
                "active_pack_version_id": PACK_VERSION_ID,
                "artifact_type": "public_docs_publish_activation_receipt",
                "pack_id": PACK_ID,
                "status": "activated",
                "tenant_id": TENANT_ID,
            }
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, Any]] = []

    def fake_post_json(_url: str, request: Mapping[str, Any], **_kwargs: object) -> dict[str, Any]:
        calls.append(dict(request))
        source_uri = str(request["metadata"]["golden_case_expected_source_uri"])
        return {
            "citations": [
                {
                    "chunk_id": "chunk-" + sha256(source_uri.encode()).hexdigest()[:12],
                    "evidence_ref": "retrieval:golden",
                    "pack_version_id": PACK_VERSION_ID,
                    "source_uri": source_uri,
                }
            ],
            "result_cards": [
                {
                    "chunk_id": "chunk-" + sha256(source_uri.encode()).hexdigest()[:12],
                    "provenance": {
                        "crawl_time": "2026-07-08T22:00:00Z",
                        "freshness_state": "fresh",
                        "source_url": source_uri,
                    },
                }
            ],
            "result_chunk_ids": ["chunk-" + sha256(source_uri.encode()).hexdigest()[:12]],
            "result_count": 1,
            "selected_pack_version_ids": [PACK_VERSION_ID],
        }

    monkeypatch.setattr("dags.serp_eval_contracts._post_json", fake_post_json)

    artifact = write_public_docs_retrieval_golden_artifact(plan.to_canonical_json())

    assert artifact["artifactType"] == "public_docs_retrieval_golden"
    assert artifact["payload"]["status"] == "passed"
    assert artifact["payload"]["case_count"] == 30
    assert len(calls) == 60
    assert all("source_uri_filter" not in call["metadata"] for call in calls)
    assert artifact["payload"]["latency_seconds"]["p95"] <= 2.0


def test_public_docs_retrieval_golden_accepts_ranked_multi_source_results_from_active_pack() -> (
    None
):
    expected_source = "https://airflow.apache.org/docs/"
    secondary_source = "https://kubernetes.io/docs/"
    case = {
        "case_id": "public-docs-airflow-ranked-multi-source",
        "expected": {
            "max_freshness_hours": 24,
            "minimum_citations": 1,
            "source_uri_prefix": expected_source,
        },
        "query": "Apache Airflow official documentation overview",
        "seed_id": "apache-airflow-docs",
    }
    response = {
        "citations": [
            {
                "chunk_id": "chunk-expected",
                "evidence_ref": "retrieval:golden",
                "pack_version_id": PACK_VERSION_ID,
                "source_uri": expected_source,
            },
            {
                "chunk_id": "chunk-secondary",
                "evidence_ref": "retrieval:golden",
                "pack_version_id": PACK_VERSION_ID,
                "source_uri": secondary_source,
            },
        ],
        "result_cards": [
            {
                "chunk_id": "chunk-expected",
                "provenance": {
                    "crawl_time": "2026-07-08T22:00:00Z",
                    "freshness_state": "fresh",
                    "source_url": expected_source,
                },
            },
            {
                "chunk_id": "chunk-secondary",
                "provenance": {
                    "crawl_time": "2026-07-08T22:00:00Z",
                    "freshness_state": "fresh",
                    "source_url": secondary_source,
                },
            },
        ],
        "result_chunk_ids": ["chunk-expected", "chunk-secondary"],
        "result_count": 2,
        "selected_pack_version_ids": [PACK_VERSION_ID],
    }

    observed = serp_eval_contracts_module._public_docs_retrieval_golden_response_signature(
        response=response,
        case=case,
        expected_pack_version_id=PACK_VERSION_ID,
        generated_at="2026-07-08T22:00:00Z",
    )

    assert [citation["source_uri"] for citation in observed["citations"]] == [
        expected_source,
        secondary_source,
    ]


def test_public_docs_retrieval_golden_accepts_localized_url_for_same_docs_root() -> None:
    expected_source = "https://helm.sh/docs/"
    localized_source = "https://helm.sh/ru/docs/"
    case = {
        "case_id": "public-docs-helm-localized-docs-root",
        "expected": {
            "max_freshness_hours": 24,
            "minimum_citations": 1,
            "source_uri_prefix": expected_source,
        },
        "query": "Helm official documentation overview",
        "seed_id": "helm-docs",
    }
    response = {
        "citations": [
            {
                "chunk_id": "chunk-helm-ru",
                "evidence_ref": "retrieval:golden",
                "pack_version_id": PACK_VERSION_ID,
                "source_uri": localized_source,
            }
        ],
        "result_cards": [
            {
                "chunk_id": "chunk-helm-ru",
                "provenance": {
                    "crawl_time": "2026-07-08T22:00:00Z",
                    "freshness_state": "fresh",
                    "source_url": localized_source,
                },
            }
        ],
        "result_chunk_ids": ["chunk-helm-ru"],
        "result_count": 1,
        "selected_pack_version_ids": [PACK_VERSION_ID],
    }

    observed = serp_eval_contracts_module._public_docs_retrieval_golden_response_signature(
        response=response,
        case=case,
        expected_pack_version_id=PACK_VERSION_ID,
        generated_at="2026-07-08T22:00:00Z",
    )

    assert observed["citations"][0]["source_uri"] == localized_source


def test_public_docs_retrieval_golden_source_identity_preserves_pinned_version() -> None:
    assert serp_eval_contracts_module._public_docs_source_uri_matches_expected_docs_root(
        expected_source_uri="https://docs.example.com/docs/v3/",
        observed_source_uri="https://docs.example.com/ru/docs/v3/install/",
    )
    assert not serp_eval_contracts_module._public_docs_source_uri_matches_expected_docs_root(
        expected_source_uri="https://docs.example.com/docs/v3/",
        observed_source_uri="https://docs.example.com/ru/docs/v4/install/",
    )


def test_public_docs_retrieval_golden_rejects_missing_expected_top_ranked_source() -> None:
    expected_source = "https://airflow.apache.org/docs/"
    case = {
        "case_id": "public-docs-airflow-missing-top-result",
        "expected": {
            "max_freshness_hours": 24,
            "minimum_citations": 1,
            "source_uri_prefix": expected_source,
        },
        "query": "Apache Airflow official documentation overview",
        "seed_id": "apache-airflow-docs",
    }
    response = {
        "citations": [
            {
                "chunk_id": "chunk-wrong",
                "evidence_ref": "retrieval:golden",
                "pack_version_id": PACK_VERSION_ID,
                "source_uri": "https://kubernetes.io/docs/",
            }
        ],
        "result_cards": [
            {
                "chunk_id": "chunk-wrong",
                "provenance": {
                    "crawl_time": "2026-07-08T22:00:00Z",
                    "freshness_state": "fresh",
                    "source_url": "https://kubernetes.io/docs/",
                },
            }
        ],
        "result_chunk_ids": ["chunk-wrong"],
        "result_count": 1,
        "selected_pack_version_ids": [PACK_VERSION_ID],
    }

    with pytest.raises(
        ValueError,
        match="case_id=public-docs-airflow-missing-top-result.*top-ranked expected source",
    ):
        serp_eval_contracts_module._public_docs_retrieval_golden_response_signature(
            response=response,
            case=case,
            expected_pack_version_id=PACK_VERSION_ID,
            generated_at="2026-07-08T22:00:00Z",
        )


def test_public_docs_retrieval_golden_cases_use_indexed_coverage_not_planned_frontier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_plan = build_public_docs_seed_refresh_plan(
        default_public_docs_seed_refresh_conf(generated_at="2026-07-08T22:00:00Z")
    ).payload
    monkeypatch.setattr(
        serp_eval_contracts_module,
        "_PUBLIC_DOCS_RETRIEVAL_GOLDEN_MIN_CASES",
        3,
    )
    refresh_result = {
        "artifact_type": "public_docs_seed_refresh_batch_evidence",
        "coverage_proof": {
            "coverage_status": "indexed_pending_publish",
            "seeds": [
                {
                    "index_status": "passed",
                    "optional_frontier": [
                        {
                            "source_uri": "https://docs.k3s.io/quick-start",
                            "status": "indexed",
                        },
                        {
                            "source_uri": "https://docs.k3s.io/installation/requirements",
                            "status": "failed",
                        },
                    ],
                    "required_frontier": [
                        {
                            "index_status": "passed",
                            "source_uri": "https://docs.k3s.io/installation/requirements",
                            "status": "indexed",
                        }
                    ],
                    "seed_id": "k3s-docs",
                    "source_uri": "https://docs.k3s.io/",
                    "status": "indexed",
                }
            ],
        },
    }

    cases = serp_eval_contracts_module._public_docs_retrieval_golden_cases(
        refresh_plan,
        refresh_result=refresh_result,
    )

    assert [case["expected"]["source_uri_prefix"] for case in cases] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/installation/requirements",
        "https://docs.k3s.io/quick-start",
    ]
    assert cases[2]["query"] == "K3s documentation quick start"


def test_public_docs_retrieval_golden_cases_keep_every_indexed_root_when_budget_is_smaller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        serp_eval_contracts_module,
        "_PUBLIC_DOCS_RETRIEVAL_GOLDEN_MIN_CASES",
        3,
    )
    refresh_plan = {
        "seed_registry": [
            {
                "inventory_evidence": {"component": "Alpha"},
                "seed_id": "alpha-docs",
            },
            {
                "inventory_evidence": {"component": "Bravo"},
                "seed_id": "bravo-docs",
            },
            {
                "inventory_evidence": {"component": "Charlie"},
                "seed_id": "charlie-docs",
            },
            {
                "inventory_evidence": {"component": "Delta"},
                "seed_id": "delta-docs",
            },
        ]
    }
    refresh_result = {
        "artifact_type": "public_docs_seed_refresh_batch_evidence",
        "coverage_proof": {
            "coverage_status": "indexed_pending_publish",
            "seeds": [
                {
                    "index_status": "passed",
                    "seed_id": "alpha-docs",
                    "source_uri": "https://docs.example.com/alpha/",
                    "status": "indexed",
                },
                {
                    "index_status": "passed",
                    "seed_id": "bravo-docs",
                    "source_uri": "https://docs.example.com/bravo/",
                    "status": "indexed",
                },
                {
                    "index_status": "passed",
                    "seed_id": "charlie-docs",
                    "source_uri": "https://docs.example.com/charlie/",
                    "status": "indexed",
                },
                {
                    "index_status": "passed",
                    "seed_id": "delta-docs",
                    "source_uri": "https://docs.example.com/delta/",
                    "status": "indexed",
                },
            ],
        },
    }

    cases = serp_eval_contracts_module._public_docs_retrieval_golden_cases(
        refresh_plan,
        refresh_result=refresh_result,
    )

    assert [case["seed_id"] for case in cases] == [
        "alpha-docs",
        "bravo-docs",
        "charlie-docs",
        "delta-docs",
    ]


def test_public_docs_publish_activation_plan_accepts_s3_d20_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_refresh_result_path = (
        "s3://airflow-serp-artifacts/serp-evals/d20/public-docs-seed-refresh-result.json"
    )
    request_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-request.json"
    )
    receipt_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-receipt.json"
    )
    result_bucket, result_key = seed_refresh_result_path.removeprefix("s3://").split("/", 1)
    request_bucket, request_key = request_path.removeprefix("s3://").split("/", 1)
    storage = {
        (result_bucket, result_key): json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": {
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ).encode("utf-8"),
        (request_bucket, request_key): json.dumps(
            {
                "artifact_type": "public_docs_publish_activation_submission",
                "contract_version": "2026.07.1",
                "status": "ready_for_bc21_publish_activation",
                "submission": {"endpointPath": "/api/bc-21/serp/v1/packs/x"},
                "submission_sha256": "a" * 64,
            },
            sort_keys=True,
        ).encode("utf-8"),
    }

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    conf = _public_docs_publish_activation_conf(seed_refresh_result_path)
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_public_docs_publish_activation_plan(conf)
    plan.payload["artifact_paths"]["public_docs_publish_activation_request"] = request_path
    plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"] = receipt_path

    cli_spec = build_public_docs_publish_activation_cli_spec(plan.to_canonical_json())
    submit_spec = build_public_docs_publish_activation_submit_cli_spec(plan.to_canonical_json())

    assert plan.payload["public_docs_seed_refresh_result_path"] == seed_refresh_result_path
    assert cli_spec["input_paths"] == [seed_refresh_result_path]
    assert cli_spec["argv"][cli_spec["argv"].index("--seed-refresh-result") + 1] == (
        seed_refresh_result_path
    )
    assert submit_spec["input_paths"] == [request_path]
    assert submit_spec["argv"][submit_spec["argv"].index("--publish-activation-request") + 1] == (
        request_path
    )
    assert submit_spec["argv"][submit_spec["argv"].index("--activation-receipt-output") + 1] == (
        receipt_path
    )


def test_pipeline_cli_executor_materializes_s3_activation_receipt_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-request.json"
    )
    receipt_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-receipt.json"
    )
    request_bucket, request_key = request_path.removeprefix("s3://").split("/", 1)
    bucket, key = receipt_path.removeprefix("s3://").split("/", 1)
    storage = {
        (
            request_bucket,
            request_key,
        ): b'{"artifact_type":"public_docs_publish_activation_submission"}'
    }
    put_calls: list[tuple[str, str, str, str]] = []

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    cli_spec = {
        "argv": [
            "python",
            "-c",
            (
                "import json, pathlib, sys; "
                "request_arg = sys.argv.index('--publish-activation-request') + 1; "
                "request = pathlib.Path(sys.argv[request_arg]); "
                "assert request.exists(); "
                "receipt_arg = sys.argv.index('--activation-receipt-output') + 1; "
                "p = pathlib.Path(sys.argv[receipt_arg]); "
                "assert not str(p).startswith('s3://'); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                "payload = {'artifact_type': 'public_docs_publish_activation_receipt', "
                "'status': 'active'}; "
                "p.write_text(json.dumps(payload), encoding='utf-8'); "
                "print(json.dumps(payload))"
            ),
            "--publish-activation-request",
            request_path,
            "--activation-receipt-output",
            receipt_path,
        ],
        "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
        "dag_id": "serp_publish_signed_pack",
        "input_paths": [request_path],
        "operation_id": "serp-airflow-publish-signed-pack-test",
        "status": "ready_for_pipeline_cli_runner",
        "stdout_path": receipt_path,
        "task_id": "public_docs_publish_activation_submit",
    }

    result = execute_pipeline_cli_spec(cli_spec)

    assert result["artifactPath"] == receipt_path
    assert result["payload"]["status"] == "active"
    assert put_calls
    assert put_calls[0][0] == bucket
    assert put_calls[0][1] == key
    assert put_calls[0][3] == "application/json"


def test_public_docs_publish_activation_plan_requires_governed_inputs(tmp_path: Path) -> None:
    missing_result = _public_docs_publish_activation_conf(str(tmp_path / "missing.json"))
    missing_result["artifact_root_path"] = str(tmp_path)
    with pytest.raises(ValueError, match="public_docs_seed_refresh_result file is not readable"):
        build_public_docs_publish_activation_plan(missing_result)

    bad_seal = _public_docs_publish_activation_conf(str(tmp_path / "result.json"))
    bad_seal_path = str(bad_seal["public_docs_seed_refresh_result_path"])
    Path(bad_seal_path).write_text("{}", encoding="utf-8")
    bad_seal["evidence_seal_hash"] = "b" * 64
    with pytest.raises(ValueError, match="evidence_seal_hash"):
        build_public_docs_publish_activation_plan(bad_seal)

    identity_drift_path = tmp_path / "identity-drift-result.json"
    _write_public_docs_seed_refresh_result(
        identity_drift_path,
        pack_id="00000000-0000-4000-a000-000000000299",
    )
    identity_drift = _public_docs_publish_activation_conf(str(identity_drift_path))
    identity_drift["artifact_root_path"] = str(tmp_path)
    with pytest.raises(ValueError, match="identity must match pack_id"):
        build_public_docs_publish_activation_plan(identity_drift)


def test_default_public_docs_seed_refresh_conf_materializes_autonomous_d20_plan(
    tmp_path: Path,
) -> None:
    conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    assert conf["seed_registry"]
    assert plan.payload["dag_id"] == "serp_web_seed_crawl_refresh"
    assert plan.payload["status"] == "ready_for_public_docs_seed_refresh"
    assert plan.payload["seed_count"] == len(P0_PUBLIC_DOCS_SOURCES)
    assert plan.payload["source_type_counts"] == {
        "openapi": 1,
        "website": len(P0_PUBLIC_DOCS_SOURCES) - 1,
    }
    assert {
        seed["inventory_evidence"]["stack_inventory_path"] for seed in plan.payload["seed_registry"]
    } == {STACK_INVENTORY_SOURCE_PATH}
    assert {seed["metadata"]["origin"] for seed in plan.payload["seed_registry"]} == {
        STACK_INVENTORY_SOURCE_PATH
    }
    assert {
        seed["metadata"]["nightly_source_catalog_path"] for seed in plan.payload["seed_registry"]
    } == {PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH}
    assert {seed["metadata"]["priority"] for seed in plan.payload["seed_registry"]} == {"P0"}
    assert {seed["seed_id"]: seed["source_uri"] for seed in plan.payload["seed_registry"]} == {
        str(source["seed_id"]): str(source["docs_url"]) for source in P0_PUBLIC_DOCS_SOURCES
    }
    assert {
        seed["seed_id"]: seed["metadata"]["catalog_docs_url"]
        for seed in plan.payload["seed_registry"]
    } == {
        str(source["seed_id"]): str(source.get("catalog_docs_url", source["docs_url"]))
        for source in P0_PUBLIC_DOCS_SOURCES
    }
    assert {
        seed["seed_id"]: seed["metadata"]["repo_url"] for seed in plan.payload["seed_registry"]
    } == {str(source["seed_id"]): str(source["repo_url"]) for source in P0_PUBLIC_DOCS_SOURCES}
    assert {
        seed["seed_id"]: seed["metadata"]["releases_url"] for seed in plan.payload["seed_registry"]
    } == {str(source["seed_id"]): str(source["releases_url"]) for source in P0_PUBLIC_DOCS_SOURCES}
    assert {
        seed["seed_id"]: tuple(seed["metadata"]["suggested_ingest_modes"])
        for seed in plan.payload["seed_registry"]
    } == {
        str(source["seed_id"]): tuple(source["suggested_ingest_modes"])
        for source in P0_PUBLIC_DOCS_SOURCES
    }
    sources_by_seed_id = {str(source["seed_id"]): source for source in P0_PUBLIC_DOCS_SOURCES}
    assert {
        request["seed_id"]: request["source_metadata"]["repo_url"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {
        request["seed_id"]: str(
            sources_by_seed_id[request["seed_id"].split("--", maxsplit=1)[0]]["repo_url"]
        )
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    }
    assert {
        request["seed_id"]: request["source_metadata"]["releases_url"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {
        request["seed_id"]: str(
            sources_by_seed_id[request["seed_id"].split("--", maxsplit=1)[0]]["releases_url"]
        )
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    }
    assert {
        request["seed_id"]: tuple(request["source_metadata"]["suggested_ingest_modes"])
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {
        request["seed_id"]: tuple(
            sources_by_seed_id[request["seed_id"].split("--", maxsplit=1)[0]][
                "suggested_ingest_modes"
            ]
        )
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    }


def test_default_public_docs_seed_refresh_conf_uses_run_scoped_pack_version(
    tmp_path: Path,
) -> None:
    first = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    first_retry = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    second = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-09T00:00:00Z",
        artifact_root_path=str(tmp_path),
    )

    assert first["pack_version_id"] == first_retry["pack_version_id"]
    assert first["pack_version_id"] == first["registry_resource_id"]
    assert first["pack_version_id"] != PACK_VERSION_ID
    assert second["pack_version_id"] != first["pack_version_id"]
    UUID(str(first["pack_version_id"]))
    UUID(str(second["pack_version_id"]))


def test_default_public_docs_seed_refresh_conf_changes_identity_on_catalog_metadata_drift(
    tmp_path: Path,
) -> None:
    baseline = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    drifted = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    drifted["seed_registry"][0]["metadata"]["repo_url"] = "https://git.proxmox.com/drift"
    drifted["seed_registry"][0]["inventory_evidence"]["evidence_sha256"] = "b" * 64

    baseline_plan = build_public_docs_seed_refresh_plan(baseline)
    drifted_plan = build_public_docs_seed_refresh_plan(drifted)

    assert (
        baseline_plan.payload["seed_registry_sha256"]
        != drifted_plan.payload["seed_registry_sha256"]
    )
    assert baseline_plan.payload["operation_id"] != drifted_plan.payload["operation_id"]


def test_default_public_docs_seed_refresh_conf_does_not_read_tmp_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    assert not (tmp_path / PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH).exists()
    assert not (tmp_path / STACK_INVENTORY_SOURCE_PATH).exists()

    conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    plan = build_public_docs_seed_refresh_plan(conf)

    assert plan.payload["seed_count"] == len(P0_PUBLIC_DOCS_SOURCES)
    assert {
        seed["metadata"]["nightly_source_catalog_path"] for seed in plan.payload["seed_registry"]
    } == {PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH}
    assert {
        seed["inventory_evidence"]["stack_inventory_path"] for seed in plan.payload["seed_registry"]
    } == {STACK_INVENTORY_SOURCE_PATH}


def test_p0_public_docs_seed_catalog_shape_is_runtime_safe() -> None:
    allowed_source_types = {"git", "openapi", "pdf", "website"}
    seen_seed_ids: set[str] = set()

    for source in p0_public_docs_sources():
        seed_id = str(source["seed_id"])
        docs_url = str(source["docs_url"])
        source_type = str(source.get("source_type", "website"))
        docs_origin = urlparse(docs_url)

        assert seed_id not in seen_seed_ids
        assert seed_id
        assert str(source["component"])
        assert str(source.get("priority", "P0")) == "P0"
        assert source_type in allowed_source_types
        assert docs_origin.scheme in {"git+file", "https"}

        if source_type == "git":
            assert docs_url.startswith("git+file://")
        if source_type == "pdf":
            assert docs_url.endswith(".pdf")
        if source_type in {"openapi", "website"}:
            assert docs_origin.scheme == "https"

        for frontier_url in source.get("frontier_urls", ()):
            frontier_origin = urlparse(str(frontier_url))
            assert frontier_origin.scheme == docs_origin.scheme
            assert frontier_origin.netloc == docs_origin.netloc

        seen_seed_ids.add(seed_id)

    assert seen_seed_ids == {str(source["seed_id"]) for source in P0_PUBLIC_DOCS_SOURCES}


def test_p0_public_docs_sources_match_nightly_markdown_catalog() -> None:
    catalog_path = REPO_ROOT.parent / PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH
    stack_inventory_path = REPO_ROOT.parent / STACK_INVENTORY_SOURCE_PATH

    assert catalog_path.exists()
    assert stack_inventory_path.exists()

    catalog_text = catalog_path.read_text(encoding="utf-8")
    assert STACK_INVENTORY_SOURCE_PATH in catalog_text

    catalog_rows = _p0_nightly_catalog_rows(catalog_text)
    executable_sources = {str(source["component"]): source for source in P0_PUBLIC_DOCS_SOURCES}

    assert set(executable_sources) == set(catalog_rows)
    for component, source in executable_sources.items():
        row = catalog_rows[component]
        docs_url = str(source["docs_url"])
        catalog_docs_url = str(source.get("catalog_docs_url", docs_url))
        source_type = str(source.get("source_type", "website"))
        suggested_modes = {
            mode.strip() for mode in row["suggested_ingest_modes"].split(",") if mode.strip()
        }

        assert row["docs_url"] == catalog_docs_url
        assert row["repo_url"] == str(source["repo_url"])
        assert row["releases_url"] == str(source["releases_url"])
        assert suggested_modes == set(source["suggested_ingest_modes"])
        assert source_type in suggested_modes
        assert str(source.get("priority", "P0")) == row["priority"]


def test_p0_public_docs_website_seeds_use_canonical_public_docs_roots() -> None:
    sources_by_seed_id = {str(source["seed_id"]): source for source in P0_PUBLIC_DOCS_SOURCES}

    for seed_id in (
        "kustomize-docs",
        "qdrant-docs",
        "neo4j-docs",
        "redis-docs",
        "minio-docs",
        "jenkins-docs",
        "harbor-docs",
        "cert-manager-docs",
        "cilium-docs",
        "kyverno-docs",
        "openebs-docs",
        "traefik-proxy-docs",
    ):
        source = sources_by_seed_id[seed_id]
        assert not str(source["docs_url"]).startswith("https://raw.githubusercontent.com/")
        assert source["docs_url"] == source.get("catalog_docs_url", source["docs_url"])


def test_apache_kafka_seed_uses_version_pinned_canonical_docs_root() -> None:
    kafka = next(
        source for source in P0_PUBLIC_DOCS_SOURCES if source["seed_id"] == "apache-kafka-docs"
    )

    assert kafka["docs_url"] == "https://kafka.apache.org/43/"
    assert kafka.get("catalog_docs_url", kafka["docs_url"]) == "https://kafka.apache.org/43/"
    assert kafka["version"] == "4.3"


def _p0_nightly_catalog_rows(catalog_text: str) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for line in catalog_text.splitlines():
        if not line.startswith("| P0 |"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 7:
            raise AssertionError(f"unexpected P0 catalog row shape: {line}")
        priority, technology, docs_url, repo_url, releases_url, suggested_ingest_modes, notes = (
            cells
        )
        rows[technology] = {
            "docs_url": docs_url,
            "notes": notes,
            "priority": priority,
            "releases_url": releases_url,
            "repo_url": repo_url,
            "suggested_ingest_modes": suggested_ingest_modes,
        }
    return rows


def test_build_public_docs_seed_refresh_plan_rejects_unsafe_seed_registry() -> None:
    disallowed_source_type = _public_docs_seed_refresh_conf()
    disallowed_source_type["seed_registry"][0]["source_type"] = "confluence"
    with pytest.raises(ValueError, match="source_type is not executable by current connectors"):
        build_public_docs_seed_refresh_plan(disallowed_source_type)

    planned_markdown = _public_docs_seed_refresh_conf()
    planned_markdown["seed_registry"][0]["source_type"] = "markdown"
    with pytest.raises(ValueError, match="source_type is not executable by current connectors"):
        build_public_docs_seed_refresh_plan(planned_markdown)

    unsupported_index_mode = _public_docs_seed_refresh_conf()
    unsupported_index_mode["index_mode"] = "shadow-live"
    with pytest.raises(ValueError, match="index_mode is unsupported"):
        build_public_docs_seed_refresh_plan(unsupported_index_mode)

    unsupported_embedding_mode = _public_docs_seed_refresh_conf()
    unsupported_embedding_mode["embedding_mode"] = "direct-provider"
    with pytest.raises(ValueError, match="embedding_mode is unsupported"):
        build_public_docs_seed_refresh_plan(unsupported_embedding_mode)

    live_with_dev_embedding = _public_docs_seed_refresh_conf()
    live_with_dev_embedding["index_mode"] = "live"
    live_with_dev_embedding["embedding_mode"] = "deterministic-dev"
    with pytest.raises(ValueError, match="live index mode requires live-gateway"):
        build_public_docs_seed_refresh_plan(live_with_dev_embedding)

    unsafe_store_name = _public_docs_seed_refresh_conf()
    unsafe_store_name["qdrant_collection"] = "serp vectors prod"
    with pytest.raises(ValueError, match="qdrant_collection must be a plain store name"):
        build_public_docs_seed_refresh_plan(unsafe_store_name)

    remote_git = _public_docs_seed_refresh_conf()
    remote_git["seed_registry"][3]["source_uri"] = "git+https://github.com/adapstory/docs.git"
    remote_git["seed_registry"][3]["official_docs_uri"] = (
        "git+https://github.com/adapstory/docs.git"
    )
    with pytest.raises(ValueError, match="git public docs seeds must use git\\+file"):
        build_public_docs_seed_refresh_plan(remote_git)

    missing_robot_policy = _public_docs_seed_refresh_conf()
    missing_robot_policy["seed_registry"][1]["crawl_policy"]["respect_robots_txt"] = False
    with pytest.raises(ValueError, match="respect_robots_txt must be true"):
        build_public_docs_seed_refresh_plan(missing_robot_policy)

    cross_domain_frontier = _public_docs_seed_refresh_conf()
    cross_domain_frontier["seed_registry"][0]["crawl_policy"]["curated_frontier_urls"] = [
        "https://evil.example.com/k3s"
    ]
    with pytest.raises(
        ValueError,
        match="curated and discovered frontier URLs host must be in allowed_domains",
    ):
        build_public_docs_seed_refresh_plan(cross_domain_frontier)

    denied_seed_uri = _public_docs_seed_refresh_conf()
    denied_seed_uri["seed_registry"][0]["source_uri"] = "https://docs.k3s.io/login"
    denied_seed_uri["seed_registry"][0]["official_docs_uri"] = "https://docs.k3s.io/login"
    with pytest.raises(ValueError, match="source_uri must not match deny_patterns"):
        build_public_docs_seed_refresh_plan(denied_seed_uri)

    secret_in_metadata = _public_docs_seed_refresh_conf()
    secret_in_metadata["seed_registry"][2]["metadata"] = {"api_key": "sk-abcdefghijklmnop"}
    with pytest.raises(ValueError, match="dag run config must not contain raw secret material"):
        build_public_docs_seed_refresh_plan(secret_in_metadata)


@pytest.mark.parametrize(
    ("dag_file", "dag_id", "task_ids"),
    [
        (
            "serp_nightly_regression_suite.py",
            "serp_nightly_regression_suite",
            [
                "validate_nightly_regression_plan",
                "materialize_live_benchmark_catalog",
                "load_materialized_benchmark_catalog",
                "write_nightly_suite_plan",
                "run_mandatory_benchmark_suites",
                "build_c1_benchmark_gate_export",
                "build_bc21_benchmark_run_submissions",
                "submit_bc21_benchmark_run_submissions",
                "notify_governance_eval_surfaces",
            ],
        ),
        (
            "serp_mandatory_benchmark_dataset_evidence_snapshot.py",
            "serp_mandatory_benchmark_dataset_evidence_snapshot",
            [
                "validate_mandatory_benchmark_dataset_evidence_plan",
                "materialize_mandatory_benchmark_dataset_evidence",
            ],
        ),
        (
            "serp_tenant_golden_set_regression.py",
            "serp_tenant_golden_set_regression",
            [
                "validate_tenant_golden_regression_plan",
                "run_tenant_golden_set_cases",
                "build_tenant_golden_registry_submissions",
                "notify_governance_eval_surfaces",
            ],
        ),
        (
            "serp_online_eval_rollup.py",
            "serp_online_eval_rollup",
            [
                "validate_online_eval_rollup_plan",
                "write_online_eval_rollup_plan",
                "build_online_eval_rollup",
                "build_online_eval_registry_submissions",
                "notify_governance_eval_surfaces",
            ],
        ),
        (
            "serp_benchmark_improvement_wave.py",
            "serp_benchmark_improvement_wave",
            [
                "validate_benchmark_improvement_wave_plan",
                "write_improvement_spec",
                "write_paired_eval_request",
                "run_paired_benchmark_evaluation",
                "notify_governance_eval_surfaces",
            ],
        ),
        (
            "serp_publish_signed_pack.py",
            "serp_publish_signed_pack",
            [
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
            ],
        ),
        (
            "serp_web_seed_crawl_refresh.py",
            "serp_web_seed_crawl_refresh",
            [
                "validate_public_docs_seed_registry",
                "write_public_docs_seed_registry",
                "build_public_docs_seed_refresh_plan",
                "dispatch_pipeline_seed_refresh_handoff",
                "submit_public_docs_bc21_pipeline_state",
                "write_public_docs_publish_activation_trigger_conf",
                "prepare_public_docs_d5_dispatch",
                "notify_governance_eval_surfaces",
            ],
        ),
    ],
)
def test_serp_dag_files_declare_expected_airflow_contracts(
    dag_file: str,
    dag_id: str,
    task_ids: list[str],
) -> None:
    source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert _call_string_args(tree, "DAG")[0] == dag_id
    if dag_id == "serp_benchmark_improvement_wave":
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            "validate_benchmark_improvement_wave_plan",
            "write_improvement_spec",
            "write_paired_eval_request",
            "notify_governance_eval_surfaces",
        ]
        assert _keyword_values(tree, "KubernetesPodOperator", "task_id") == [
            "run_paired_benchmark_evaluation"
        ]
    elif dag_id == "serp_nightly_regression_suite":
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            task_id for task_id in task_ids if task_id != "materialize_live_benchmark_catalog"
        ]
        assert _keyword_values(tree, "KubernetesPodOperator", "task_id") == [
            "materialize_live_benchmark_catalog"
        ]
    elif dag_id == "serp_mandatory_benchmark_dataset_evidence_snapshot":
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            "validate_mandatory_benchmark_dataset_evidence_plan"
        ]
        assert _keyword_values(tree, "KubernetesPodOperator", "task_id") == [
            "materialize_mandatory_benchmark_dataset_evidence"
        ]
    else:
        assert _keyword_values(tree, "PythonOperator", "task_id") == task_ids
    assert "external_runner_pending" not in source
    assert "registry_submission_pending" not in source
    assert "host.docker.internal" not in source
    assert "localhost" not in source
    assert "http://" not in source
    assert "https://" not in source


def test_serp_dag_files_import_helpers_from_packaged_dags_namespace() -> None:
    for dag_file in (
        "serp_nightly_regression_suite.py",
        "serp_mandatory_benchmark_dataset_evidence_snapshot.py",
        "serp_online_eval_rollup.py",
        "serp_tenant_golden_set_regression.py",
        "serp_benchmark_improvement_wave.py",
        "serp_publish_signed_pack.py",
        "serp_web_seed_crawl_refresh.py",
    ):
        source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")

        assert "from dags.serp_eval_contracts import" in source
        assert "from serp_eval_contracts import" not in source


def test_serp_nightly_dag_uses_live_gateway_cli_for_d6_path() -> None:
    source = (REPO_ROOT / "dags" / "serp_nightly_regression_suite.py").read_text(encoding="utf-8")

    assert "write_nightly_suite_plan_artifact" in source
    assert "load_materialized_benchmark_catalog_snapshot" in source
    assert "materialize_catalog = KubernetesPodOperator(" in source
    assert "BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT" in source
    assert "BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS" in source
    assert "benchmark_catalog_acquisition_env_vars" in source
    assert "materialize_live_benchmark_catalog_artifact" not in source
    assert "execute_gateway_cli_spec" in source
    assert "build_nightly_runner_cli_spec" in source
    assert "build_nightly_benchmark_export_cli_spec" in source
    assert "build_nightly_registry_submit_cli_spec" in source
    assert "write_nightly_report_artifact" not in source
    assert "write_nightly_registry_receipts_artifact" not in source


def test_serp_nightly_dag_schedules_only_after_all_adapters_are_ready() -> None:
    source = (REPO_ROOT / "dags" / "serp_nightly_regression_suite.py").read_text(encoding="utf-8")

    assert "mandatory_benchmark_adapters_ready" in source
    assert "default_nightly_regression_conf" in source
    assert "nightly_regression_runtime_ready" in source
    assert "mandatory_benchmark_adapters_ready() and nightly_regression_runtime_ready()" in source


def test_serp_public_docs_dag_runs_default_seed_registry_pipeline_path() -> None:
    source = (REPO_ROOT / "dags" / "serp_web_seed_crawl_refresh.py").read_text(encoding="utf-8")

    assert "default_public_docs_seed_refresh_conf" in source
    assert "_public_docs_seed_refresh_conf_with_defaults" in source
    assert "datetime.now(UTC)" in source
    assert "KubernetesPodOperator" in source
    assert "run_public_docs_seed_refresh_pipeline" in source
    assert "airflow-serp-evidence-store" in source
    assert "airflow-artifact-store" not in source
    assert "PUBLIC_DOCS_ACQUISITION_WORKLOAD_LABELS" in source
    assert "airflow-serp-public-docs-acquisition" in source


def test_serp_public_docs_pipeline_task_survives_scheduler_rollout() -> None:
    source = (REPO_ROOT / "dags" / "serp_web_seed_crawl_refresh.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _matches_call(node, "KubernetesPodOperator"):
            continue
        task_id = next(
            (
                keyword.value.value
                for keyword in node.keywords
                if keyword.arg == "task_id"
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ),
            None,
        )
        if task_id == "run_public_docs_seed_refresh_pipeline":
            keyword_values = {
                keyword.arg: keyword.value.value
                for keyword in node.keywords
                if keyword.arg is not None and isinstance(keyword.value, ast.Constant)
            }
            assert keyword_values["retries"] == 1
            assert keyword_values["reattach_on_restart"] is True
            assert keyword_values["on_kill_action"] == "keep_pod"
            assert keyword_values["on_finish_action"] == "delete_pod"
            assert keyword_values["random_name_suffix"] is True
            assert "SERP_PIPELINE_RUNNER_RESOURCES" in source
            assert "pipeline_runner_env_vars" in source
            assert "current_airflow_runtime_image" in source
            assert "ADAPSTORY_SERP_EMBEDDING_DIMENSION" in source
            labels = next(keyword.value for keyword in node.keywords if keyword.arg == "labels")
            assert isinstance(labels, ast.Name)
            assert labels.id == "PUBLIC_DOCS_ACQUISITION_WORKLOAD_LABELS"
            labels_definition = next(
                assignment.value
                for assignment in tree.body
                if isinstance(assignment, ast.Assign)
                and any(
                    isinstance(target, ast.Name)
                    and target.id == "PUBLIC_DOCS_ACQUISITION_WORKLOAD_LABELS"
                    for target in assignment.targets
                )
            )
            assert isinstance(labels_definition, ast.Dict)
            assert {
                key.value: value.value
                for key, value in zip(
                    labels_definition.keys,
                    labels_definition.values,
                    strict=True,
                )
                if isinstance(key, ast.Constant)
                and isinstance(key.value, str)
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            } == {
                "adapstory.com/serp-evidence-workload": "true",
                "adapstory.com/serp-network-profile": "public-docs-acquisition",
                "component": "worker",
                "release": "airflow",
                "tier": "airflow",
            }
            return

    raise AssertionError("public docs pipeline task was not found")


def test_serp_public_docs_dag_serializes_manual_and_nightly_runs() -> None:
    source = (REPO_ROOT / "dags" / "serp_web_seed_crawl_refresh.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    dag_call = next(
        node for node in ast.walk(tree) if isinstance(node, ast.Call) and _matches_call(node, "DAG")
    )
    max_active_runs = next(
        (
            keyword.value.value
            for keyword in dag_call.keywords
            if keyword.arg == "max_active_runs"
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, int)
        ),
        None,
    )

    assert max_active_runs == 1


@pytest.mark.parametrize(
    "dag_file",
    (
        "serp_web_seed_crawl_refresh.py",
        "serp_publish_signed_pack.py",
        "serp_mandatory_benchmark_dataset_evidence_snapshot.py",
    ),
)
def test_public_docs_dags_are_unpaused_when_airflow_creates_them(dag_file: str) -> None:
    source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")
    tree = ast.parse(source)
    dag_call = next(
        node for node in ast.walk(tree) if isinstance(node, ast.Call) and _matches_call(node, "DAG")
    )
    is_paused_upon_creation = next(
        (
            keyword.value.value
            for keyword in dag_call.keywords
            if keyword.arg == "is_paused_upon_creation"
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, bool)
        ),
        None,
    )

    assert is_paused_upon_creation is False


def test_serp_public_docs_dag_dispatches_d5_natively_and_waits_for_completion() -> None:
    source = (REPO_ROOT / "dags" / "serp_web_seed_crawl_refresh.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    trigger_call = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _matches_call(node, "TriggerDagRunOperator")
    )
    values = {keyword.arg: keyword.value for keyword in trigger_call.keywords}

    expected_values = {
        "task_id": "trigger_public_docs_d5_publish_activation",
        "trigger_dag_id": "serp_publish_signed_pack",
        "wait_for_completion": True,
        "skip_when_already_exists": True,
        "fail_when_dag_is_paused": True,
    }
    for name, expected in expected_values.items():
        value = values[name]
        assert isinstance(value, ast.Constant)
        assert value.value == expected


def test_d5_post_activation_failure_uses_direct_parent_rollback_compensation() -> None:
    source = (REPO_ROOT / "dags" / "serp_publish_signed_pack.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    rollback_call = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and _matches_call(node, "PythonOperator")
        and any(
            keyword.arg == "task_id"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "rollback_public_docs_post_activation_failure"
            for keyword in node.keywords
        )
    )
    values = {keyword.arg: keyword.value for keyword in rollback_call.keywords}

    assert isinstance(values["python_callable"], ast.Name)
    assert values["python_callable"].id == "rollback_public_docs_post_activation_failure"
    assert isinstance(values["trigger_rule"], ast.Attribute)
    assert isinstance(values["trigger_rule"].value, ast.Name)
    assert values["trigger_rule"].value.id == "TriggerRule"
    assert values["trigger_rule"].attr == "ONE_FAILED"
    assert "verify_search_serve >> rollback_post_activation_failure" in source
    assert "run_retrieval_golden >> rollback_post_activation_failure" in source
    assert "raise AirflowException(" in source


def test_d5_rollback_compensation_preserves_validation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_publish_signed_pack")
    module = importlib.reload(module)
    monkeypatch.setattr(
        module,
        "write_public_docs_post_activation_rollback_artifact",
        lambda _plan: {"artifactPath": "/tmp/public-docs-post-activation-rollback.json"},
    )

    with pytest.raises(module.AirflowException, match="automatic rollback completed"):
        module.rollback_public_docs_post_activation_failure("{}")


def test_d5_first_activation_compensation_records_incident_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_publish_signed_pack")
    module = importlib.reload(module)
    monkeypatch.setattr(
        module,
        "write_public_docs_post_activation_rollback_artifact",
        lambda _plan: {
            "artifactPath": "/tmp/public-docs-post-activation-rollback.json",
            "payload": {"status": "first_activation_no_restore_target"},
        },
    )

    assert module.rollback_public_docs_post_activation_failure("{}") is None


def test_prepare_public_docs_d5_dispatch_returns_only_validated_ready_conf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_web_seed_crawl_refresh")
    module = importlib.reload(module)
    expected_conf = {"pack_version_id": "018f5e13-2d73-7a77-a052-8d1bcbf96541"}

    class TaskInstance:
        def xcom_pull(self, *, task_ids: str) -> dict[str, object]:
            assert task_ids == "write_public_docs_publish_activation_trigger_conf"
            return {
                "payload": {
                    "status": "ready_for_d5_publish_activation",
                    "target_dag_id": "serp_publish_signed_pack",
                    "target_dag_run_conf": expected_conf,
                }
            }

    assert module.prepare_public_docs_d5_dispatch(ti=TaskInstance()) == expected_conf


def test_prepare_public_docs_d5_dispatch_skips_noop_without_triggering_d5(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_web_seed_crawl_refresh")
    module = importlib.reload(module)

    class TaskInstance:
        def xcom_pull(self, *, task_ids: str) -> dict[str, object]:
            assert task_ids == "write_public_docs_publish_activation_trigger_conf"
            return {"payload": {"status": "no_change_active_pack_retained"}}

    with pytest.raises(module.AirflowSkipException, match="no-op"):
        module.prepare_public_docs_d5_dispatch(ti=TaskInstance())


def test_serp_public_docs_dag_overlays_partial_run_conf_on_default_seed_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_web_seed_crawl_refresh")
    module = importlib.reload(module)
    monkeypatch.setattr(module, "discover_public_docs_crawler_frontier", lambda *_args: [])

    class DagRun:
        def __init__(self) -> None:
            self.conf = {
                "artifact_root_path": str(tmp_path),
                "generated_at": "2026-07-08T21:30:00Z",
            }

    plan_json = module.validate_public_docs_seed_registry(dag_run=DagRun())
    plan = json.loads(plan_json)

    assert plan["generated_at"] == "2026-07-08T21:30:00Z"
    assert plan["seed_count"] == len(P0_PUBLIC_DOCS_SOURCES)
    assert {seed["seed_id"] for seed in plan["seed_registry"]} == {
        str(source["seed_id"]) for source in P0_PUBLIC_DOCS_SOURCES
    }
    assert all(path.startswith(str(tmp_path)) for path in plan["artifact_paths"].values())


def test_public_docs_pipeline_runner_env_contract_survives_native_template_rendering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_web_seed_crawl_refresh")
    module = importlib.reload(module)
    numeric_values = {
        "ADAPSTORY_SERP_EMBEDDING_BATCH_SIZE": "16",
        "ADAPSTORY_SERP_EMBEDDING_DIMENSION": "768",
        "ADAPSTORY_SERP_EMBEDDING_MAX_ATTEMPTS": "3",
        "ADAPSTORY_SERP_EMBEDDING_RETRY_DELAY_SECONDS": "0.5",
        "ADAPSTORY_SERP_QDRANT_UPSERT_BATCH_SIZE": "64",
    }
    for name, value in numeric_values.items():
        monkeypatch.setenv(name, value)

    env_vars = module.pipeline_runner_env_vars("dispatch_pipeline_seed_refresh_handoff")
    values = {
        env_var.kwargs["name"]: env_var.kwargs["value"]
        for env_var in env_vars
        if "value" in env_var.kwargs
    }

    for name, value in numeric_values.items():
        assert values[name] == repr(value)
        assert isinstance(ast.literal_eval(values[name]), str)
        assert ast.literal_eval(values[name]) == value
    expected_cli_spec_template = (
        "{{ ti.xcom_pull(task_ids='dispatch_pipeline_seed_refresh_handoff') | tojson | urlencode }}"
    )
    assert values["ADAPSTORY_SERP_PIPELINE_CLI_SPEC_URLENCODED"] == expected_cli_spec_template
    assert "ADAPSTORY_SERP_PIPELINE_CLI_SPEC_JSON" not in values


def _install_airflow_import_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeAirflowSkipException(Exception):
        pass

    class FakeAirflowException(Exception):
        pass

    class FakeTriggerRule:
        ONE_FAILED = "one_failed"

    class FakeDAG:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

    class FakePythonOperator:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def __rshift__(self, other: object) -> object:
            return other

    class FakeTriggerDagRunOperator(FakePythonOperator):
        pass

    class FakeKubernetesPodOperator(FakePythonOperator):
        pass

    class FakeConf:
        @staticmethod
        def get(section: str, key: str) -> str:
            values = {
                ("kubernetes_executor", "namespace"): "airflow",
                ("kubernetes_executor", "worker_container_repository"): "harbor/airflow",
                ("kubernetes_executor", "worker_container_tag"): "test",
            }
            return values[(section, key)]

    class FakeKubernetesModel:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            self.kwargs = kwargs

    for name in (
        "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
        "ADAPSTORY_SERP_EMBEDDING_BATCH_SIZE",
        "ADAPSTORY_SERP_EMBEDDING_DIMENSION",
        "ADAPSTORY_SERP_EMBEDDING_MAX_ATTEMPTS",
        "ADAPSTORY_SERP_EMBEDDING_MODEL_ID",
        "ADAPSTORY_SERP_EMBEDDING_MODEL_VERSION",
        "ADAPSTORY_SERP_EMBEDDING_PROFILE_VERSION",
        "ADAPSTORY_SERP_EMBEDDING_PROVIDER_MODEL",
        "ADAPSTORY_SERP_EMBEDDING_RETRY_DELAY_SECONDS",
        "ADAPSTORY_SERP_EMBEDDING_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_EMBEDDING_URL",
        "ADAPSTORY_SERP_NEO4J_HTTP_URL",
        "ADAPSTORY_SERP_NEO4J_MUTATION_BATCH_SIZE",
        "ADAPSTORY_SERP_NEO4J_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_NEO4J_USERNAME",
        "ADAPSTORY_SERP_OPENSEARCH_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_OPENSEARCH_URL",
        "ADAPSTORY_SERP_QDRANT_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_QDRANT_UPSERT_BATCH_SIZE",
        "ADAPSTORY_SERP_QDRANT_URL",
        "ADAPSTORY_SERP_SEARCH_SERVE_BASE_URL",
        "ADAPSTORY_SERP_PUBLIC_DOCS_RETRY_DELAY_SECONDS",
        "ADAPSTORY_SERP_SOURCE_CURL_FALLBACK_ENABLED",
        "ADAPSTORY_SERP_SOURCE_FETCH_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_SOURCE_PROXY_URL",
    ):
        monkeypatch.setenv(name, "test")

    modules = {
        "airflow": types.ModuleType("airflow"),
        "airflow.configuration": types.ModuleType("airflow.configuration"),
        "airflow.exceptions": types.ModuleType("airflow.exceptions"),
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.cncf": types.ModuleType("airflow.providers.cncf"),
        "airflow.providers.cncf.kubernetes": types.ModuleType("airflow.providers.cncf.kubernetes"),
        "airflow.providers.cncf.kubernetes.operators": types.ModuleType(
            "airflow.providers.cncf.kubernetes.operators"
        ),
        "airflow.providers.cncf.kubernetes.operators.pod": types.ModuleType(
            "airflow.providers.cncf.kubernetes.operators.pod"
        ),
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": types.ModuleType(
            "airflow.providers.standard.operators"
        ),
        "airflow.providers.standard.operators.python": types.ModuleType(
            "airflow.providers.standard.operators.python"
        ),
        "airflow.providers.standard.operators.trigger_dagrun": types.ModuleType(
            "airflow.providers.standard.operators.trigger_dagrun"
        ),
        "airflow.sdk": types.ModuleType("airflow.sdk"),
        "airflow.utils": types.ModuleType("airflow.utils"),
        "airflow.utils.trigger_rule": types.ModuleType("airflow.utils.trigger_rule"),
        "kubernetes": types.ModuleType("kubernetes"),
        "kubernetes.client": types.ModuleType("kubernetes.client"),
        "kubernetes.client.models": types.ModuleType("kubernetes.client.models"),
    }
    cast(Any, modules["airflow.configuration"]).conf = FakeConf()
    cast(
        Any, modules["airflow.providers.standard.operators.python"]
    ).PythonOperator = FakePythonOperator
    cast(Any, modules["airflow.exceptions"]).AirflowSkipException = FakeAirflowSkipException
    cast(Any, modules["airflow.exceptions"]).AirflowException = FakeAirflowException
    cast(
        Any, modules["airflow.providers.standard.operators.trigger_dagrun"]
    ).TriggerDagRunOperator = FakeTriggerDagRunOperator
    cast(
        Any, modules["airflow.providers.cncf.kubernetes.operators.pod"]
    ).KubernetesPodOperator = FakeKubernetesPodOperator
    cast(Any, modules["airflow.sdk"]).DAG = FakeDAG
    cast(Any, modules["airflow.utils.trigger_rule"]).TriggerRule = FakeTriggerRule
    models = cast(Any, modules["kubernetes.client.models"])
    models.V1Capabilities = FakeKubernetesModel
    models.V1EnvVar = FakeKubernetesModel
    models.V1EnvVarSource = FakeKubernetesModel
    models.V1ResourceRequirements = FakeKubernetesModel
    models.V1SecretKeySelector = FakeKubernetesModel
    models.V1SecurityContext = FakeKubernetesModel
    cast(Any, modules["kubernetes.client"]).models = models
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_serp_improvement_dag_uses_pipeline_executor_for_d19_path() -> None:
    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")

    assert "write_improvement_spec_artifact" in source
    assert "write_improvement_candidate_eval_artifact" not in source
    assert "write_benchmark_improvement_decision_artifact" not in source
    assert "write_benchmark_improvement_scoreboard_artifact" not in source
    assert "write_paired_eval_request_artifact" in source
    assert "build_paired_eval_executor_cli_spec" not in source
    assert "execute_pipeline_cli_spec" not in source
    assert "execute_gateway_cli_spec" not in source


def test_serp_improvement_dag_runs_paired_evaluation_in_an_isolated_evaluator_pod() -> None:
    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")

    assert (
        "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator"
        in source
    )
    assert "run_paired_evaluation = KubernetesPodOperator(" in source
    assert "service_account_name=D19_EVALUATOR_WORKLOAD_SERVICE_ACCOUNT" in source
    assert "labels=D19_EVALUATOR_WORKLOAD_LABELS" in source
    assert "automount_service_account_token=True" in source
    assert "def run_paired_benchmark_evaluation" not in source
    assert "execute_pipeline_cli_spec" not in source


def test_airflowignore_excludes_non_dag_test_modules() -> None:
    airflowignore = REPO_ROOT / ".airflowignore"
    tests_airflowignore = REPO_ROOT / "tests" / ".airflowignore"

    assert airflowignore.exists()
    ignored_patterns = set(airflowignore.read_text(encoding="utf-8").splitlines())
    assert "tests/*" in ignored_patterns
    assert "tests/**" in ignored_patterns
    assert "**/tests/**" in ignored_patterns
    assert "tests/test_*.py" in ignored_patterns
    assert "**/test_*.py" in ignored_patterns
    assert ".*test_.*" in ignored_patterns
    assert "tests/.*" in ignored_patterns
    assert tests_airflowignore.exists()
    assert "*" in tests_airflowignore.read_text(encoding="utf-8").splitlines()


def _call_string_args(tree: ast.AST, function_name: str) -> list[str]:
    values: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _matches_call(node, function_name):
            continue
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                values.append(arg.value)
    return values


def _keyword_values(tree: ast.AST, function_name: str, keyword_name: str) -> list[str]:
    values: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _matches_call(node, function_name):
            continue
        for keyword in node.keywords:
            if (
                keyword.arg == keyword_name
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ):
                values.append(keyword.value.value)
    return values


def _matches_call(node: ast.Call, function_name: str) -> bool:
    return (isinstance(node.func, ast.Name) and node.func.id == function_name) or (
        isinstance(node.func, ast.Attribute) and node.func.attr == function_name
    )


def _refresh_artifact_sha256(artifact: dict[str, object]) -> None:
    payload_json = json.dumps(
        artifact["payload"], ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )
    artifact["artifactSha256"] = sha256(payload_json.encode("utf-8")).hexdigest()


def _pipeline_seed_refresh_payload(
    status: str,
    *,
    indexed_count: int = 1,
    failed_count: int = 0,
    index_mode: str = "live",
    optional_failed_count: int = 0,
    quarantined_count: int = 0,
    required_failed_count: int = 0,
) -> dict[str, Any]:
    return {
        "artifact_type": "public_docs_seed_refresh_batch_evidence",
        "batch_evidence": {
            "failed_count": failed_count,
            "indexed_count": indexed_count,
            "optional_failed_count": optional_failed_count,
            "quarantined_count": quarantined_count,
            "required_failed_count": required_failed_count,
            "status": status,
        },
        "index_effect": "live" if index_mode == "live" else "dry-run",
        "index_mode": index_mode,
    }


def _nightly_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "bc21_base_url": "http://serp-context-platform.env-dev.svc.cluster.local",
        "generated_at": "2026-07-05T21:00:00Z",
        "pack_version_ids": [PACK_VERSION_ID],
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "reranker_profile_version": "reranker@2026.07.1",
        "retrieval_profile_version": "hybrid@2026.07.1",
        "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
        "tenant_id": TENANT_ID,
    }


def _nightly_benchmark_suite_input(
    suite_id: str,
    *,
    metric_families: tuple[str, ...] = ("retrieval", "answer-quality", "citation", "policy"),
) -> dict[str, object]:
    references_by_family = {
        "retrieval": {
            "metric": "MRR@10",
            "metric_family": "retrieval",
            "reference_id": f"{suite_id}:mrr10-fixture",
            "reference_score": 1.0,
            "threshold": SERP_NORMALIZED_GATE_FLOOR,
        },
        "answer-quality": {
            "metric": "Faithfulness",
            "metric_family": "answer-quality",
            "reference_id": f"{suite_id}:answer-quality-fixture",
            "reference_score": 1.0,
            "threshold": SERP_NORMALIZED_GATE_FLOOR,
        },
        "citation": {
            "metric": "Citation Accuracy",
            "metric_family": "citation",
            "reference_id": f"{suite_id}:citation-fixture",
            "reference_score": 1.0,
            "threshold": SERP_NORMALIZED_GATE_FLOOR,
        },
        "policy": {
            "metric": "Policy Compliance Rate",
            "metric_family": "policy",
            "reference_id": f"{suite_id}:policy-fixture",
            "reference_score": 1.0,
            "threshold": 1.0,
        },
    }
    observations_by_family = {
        "answer-quality": {
            "metric": "Faithfulness",
            "metric_family": "answer-quality",
            "score": 0.96,
        },
        "citation": {"metric": "Citation Accuracy", "metric_family": "citation", "score": 0.97},
        "policy": {"metric": "Policy Compliance Rate", "metric_family": "policy", "score": 1.0},
    }
    return {
        "cases": [
            {
                "query_id": f"{suite_id}:fixture-query-001",
                "ranked_chunk_ids": [f"{suite_id}:chunk-a", f"{suite_id}:chunk-b"],
                "relevant_chunk_ids": [f"{suite_id}:chunk-a"],
            }
        ],
        "generated_at": "2026-07-05T21:00:00Z",
        "metadata": {
            "adapter_id": f"fixture-{suite_id.casefold().replace(' ', '-')}",
            "adapter_version": "fixture@2026.07.1",
            "adapter_source_revision": "a" * 40,
            "adapter_source_uri": "https://example.com/adapter",
            "adapter_image_digest": "sha256:" + "b" * 64,
            "dataset_license_id": "Apache-2.0",
            "dataset_distribution_rule": "snippets-only",
            "dataset_rights_status": "attested",
            "dataset_manifest_sha256": "sha256:" + "c" * 64,
            "dataset_manifest_version_id": "fixture-dataset-version",
            "dataset_manifest_uri": (
                "s3://airflow-serp-artifacts/benchmark-fixtures/"
                f"{suite_id.casefold().replace(' ', '-')}/dataset-manifest.json"
            ),
            "execution_evidence_sha256": "sha256:" + "d" * 64,
            "execution_evidence_version_id": "fixture-execution-version",
            "execution_evidence_uri": (
                "s3://airflow-serp-artifacts/benchmark-fixtures/"
                f"{suite_id.casefold().replace(' ', '-')}/execution-evidence.json"
            ),
            "reference_source_uri": "https://example.com/reference",
            "suite_contract_version": "2026.07.3",
        },
        "metric_compatibility": _nightly_metric_compatibility(
            beir_metric_families=metric_families if suite_id == "BEIR" else None
        ),
        "metric_observations": [
            observations_by_family[metric_family]
            for metric_family in metric_families
            if metric_family != "retrieval"
        ],
        "pack_version_ids": [PACK_VERSION_ID],
        "references": [references_by_family[metric_family] for metric_family in metric_families],
        "reranker_profile_version": "reranker@2026.07.1",
        "retrieval_profile_version": "hybrid@2026.07.1",
        "suite_contract_version": "2026.07.3",
        "suite_id": suite_id,
        "suite_version": "fixture@2026.07.1",
        "tenant_id": TENANT_ID,
    }


def _nightly_metric_compatibility(
    *, beir_metric_families: tuple[str, ...] | None = None
) -> dict[str, object]:
    return {
        "contract_version": "serp-suite-metric-compatibility/v1",
        "matrix_sha256": "sha256:" + "e" * 64,
        "matrix_uri": "s3://airflow-serp-artifacts/benchmark-fixtures/metric-compatibility.json",
        "matrix_version_id": "fixture-metric-compatibility-version",
        "requirements": [
            {
                "metric_families": (
                    list(beir_metric_families)
                    if suite_id == "BEIR" and beir_metric_families is not None
                    else ["retrieval", "answer-quality", "citation", "policy"]
                ),
                "suite_id": suite_id,
            }
            for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
    }


def _tenant_golden_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "changed_pack_version_ids": [PACK_VERSION_ID],
        "generated_at": "2026-07-05T21:00:00Z",
        "golden_set_id": "tenant-public-course-authoring-golden",
        "golden_set_version": "golden@2026.07.1",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
        "workflow_id": "workflow/private-course-authoring",
    }


def _online_eval_rollup_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "generated_at": "2026-07-08T10:00:00Z",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "pack",
        "reports": [_online_eval_report()],
        "tenant_id": TENANT_ID,
    }


def _online_eval_report() -> dict[str, object]:
    return {
        "contract_version": "2026.07.2",
        "evidence": {
            "metadata": {
                "evidence_bundle_operation_id": "evidence-bundle-online-sample-001",
                "evidence_bundle_sha256": "a" * 64,
                "online_eval_sample_operation_id": "online-eval-sample-001",
                "retrieval_operation_id": "retrieval-op-001",
                "run_mode": "online-eval-sample",
            },
            "tenant_id": TENANT_ID,
        },
        "metric_results": [
            {
                "metric": "MRR@10",
                "metric_family": "retrieval",
                "normalized_score": 0.97,
                "status": "passed",
            },
            {
                "metric": "Faithfulness",
                "metric_family": "answer-quality",
                "normalized_score": 0.96,
                "status": "passed",
            },
            {
                "metric": "Citation Accuracy",
                "metric_family": "citation",
                "normalized_score": 0.98,
                "status": "passed",
            },
            {
                "metric": "Policy Compliance Rate",
                "metric_family": "policy",
                "normalized_score": 1.0,
                "status": "passed",
            },
        ],
        "operation_id": "retrieval-report-001",
        "status": "passed",
        "suite_id": "online-request-sample",
        "suite_version": "online@2026.07.1",
    }


def _improvement_wave_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "baseline_run_id": "evalrun_public_reranker_baseline_001",
        "candidate_id": "candidate-reranker-v2",
        "candidate_run_id": "candidate-reranker-v2-run-001",
        "feature_flags": ["serp.reranker.v2", "serp.d19.dry_run"],
        "generated_at": "2026-07-05T21:00:00Z",
        "guardrail_bundle_version": "guardrails@2026.07.1",
        "improvement_spec_id": "improve-public-retrieval-reranker-v1",
        "judge_model_id": "judge-serp-rubric",
        "judge_model_version": "judge@2026.07.1",
        "judge_prompt_template_version": "judge-template@2026.07.1",
        "max_benchmark_runs": 12,
        "model_catalog_entry_id": "model-catalog://serp/judge-serp-rubric@2026.07.1",
        "policy_bundle_version": "policy@2026.07.1",
        "provider_route_id": "llm-gateway://eval/judge-serp-rubric",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "reranker_profile_version": "reranker@2026.07.1",
        "retrieval_profile_version": "hybrid@2026.07.1",
        "rollback_policy_ref": "policy://rollback/last-validated-baseline@v1",
        "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
        "tenant_id": TENANT_ID,
    }


def _public_docs_seed_refresh_conf() -> dict[str, Any]:
    return {
        "actor_id": "airflow-serp-public-docs-refresh",
        "artifact_root_path": "/var/opt/adapstory/serp-public-docs-refresh",
        "generated_at": "2026-07-08T21:00:00Z",
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "pack",
        "seed_registry": [
            _public_docs_seed(
                "k3s-docs",
                "website",
                "https://docs.k3s.io/",
                component="K3s",
                version="v1.34.3+k3s1",
            ),
            _public_docs_seed(
                "kubernetes-openapi-docs",
                "openapi",
                "https://raw.githubusercontent.com/kubernetes/kubernetes/master/api/openapi-spec/swagger.json",
                component="Kubernetes OpenAPI",
                version="v1.34.3",
            ),
            _public_docs_seed(
                "postgresql-reference-docs",
                "website",
                "https://www.postgresql.org/docs/16/",
                component="PostgreSQL",
                version="16.1.0",
            ),
            _public_docs_seed(
                "adapstory-gitops-docs",
                "git",
                "git+file:///opt/adapstory/Adapstory-GitOps.git",
                component="Adapstory GitOps",
                version="main",
            ),
        ],
        "tenant_id": TENANT_ID,
    }


def _public_docs_publish_activation_conf(seed_refresh_result_path: str) -> dict[str, object]:
    return {
        "activation_idempotency_key": "018f5e13-2d73-7a77-a052-" + "8d1bcbf96603",
        "activation_reason_code": "public-docs-d20-indexed",
        "actor_id": "airflow-serp-public-docs-refresh",
        "approval_idempotency_key": "018f5e13-2d73-7a77-a052-" + "8d1bcbf96601",
        "artifact_root_path": "/var/opt/adapstory/serp-public-docs-publish",
        "benchmark_gate_export_sha256": "sha256:" + "c" * 64,
        "bc21_base_url": "http://serp-context-platform.env-dev.svc.cluster.local",
        "evidence_bundle_id": "018f5e13-2d73-7a77-a052-8d1bcbf96602",
        "evidence_seal_hash": "sha256:" + "b" * 64,
        "generated_at": "2026-07-08T22:00:00Z",
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "policy_data_class": "PUBLIC",
        "policy_freshness_state": "fresh",
        "policy_license_obligation_state": "public_share_allowed",
        "policy_source_type": "website",
        "policy_trust_state": "trusted",
        "policy_version": "source-approval@2026.07.1",
        "public_docs_seed_refresh_plan_path": _seed_refresh_plan_path(seed_refresh_result_path),
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "pack",
        "tenant_id": TENANT_ID,
    }


def _seed_refresh_plan_path(seed_refresh_result_path: str) -> str:
    filename = "public-docs-seed-refresh-plan.json"
    if seed_refresh_result_path.startswith("s3://"):
        return seed_refresh_result_path.rsplit("/", 1)[0] + "/" + filename
    return str(Path(seed_refresh_result_path).with_name(filename))


def _write_public_docs_seed_refresh_result(
    path: Path,
    *,
    tenant_id: str = TENANT_ID,
    pack_id: str = PACK_ID,
    pack_version_id: str = PACK_VERSION_ID,
) -> None:
    batch_evidence = _public_docs_seed_refresh_batch_evidence(status="indexed")
    batch_evidence["tenant_id"] = tenant_id
    batch_evidence["pack_id"] = pack_id
    batch_evidence["pack_version_id"] = pack_version_id
    path.write_text(
        json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": batch_evidence,
                "batch_evidence_sha256": sha256(
                    json.dumps(
                        batch_evidence,
                        ensure_ascii=True,
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8")
                ).hexdigest(),
                "coverage_proof": _public_docs_indexed_pending_publish_coverage_proof(
                    tenant_id=tenant_id,
                    pack_id=pack_id,
                    pack_version_id=pack_version_id,
                ),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _write_public_docs_bc21_pipeline_state_receipt(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_public_docs_bc21_pipeline_state_receipt(), sort_keys=True),
        encoding="utf-8",
    )


def _public_docs_bc21_pipeline_state_receipt() -> dict[str, object]:
    return {
        "artifact_type": "bc21_pipeline_state_receipt",
        "contract_version": "2026.07.1",
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "response": {
            "evidenceBundleId": "018f5e13-2d73-7a77-a052-8d1bcbf96602",
            "evidenceSealHash": "sha256:" + "b" * 64,
            "packVersionId": PACK_VERSION_ID,
            "resourceId": PACK_ID,
            "runId": "018f5e13-2d73-7a77-a052-8d1bcbf96542",
            "tenantId": TENANT_ID,
        },
        "status": "accepted",
    }


def _public_docs_seed_refresh_batch_evidence(*, status: str) -> dict[str, object]:
    quarantined_failure = status == "indexed_with_quarantined_failures"
    source_results: list[dict[str, object]] = [
        {
            "chunk_ids": ["chunk-k3s-docs"],
            "embedding_ids": ["embedding-k3s-docs"],
            "metadata": {"chunk_count": 1, "embedding_count": 1},
            "pipeline_evidence_sha256": "1" * 64,
            "pipeline_operation_id": "public-docs-seed-refresh-test",
            "pipeline_run_id": "018f5e13-2d73-7a77-a052-8d1bcbf96540",
            "pipeline_status": "indexed",
            "post_index_state": "activation_pending",
            "seed_id": "k3s-docs",
        }
    ]
    if quarantined_failure:
        source_results.append(
            {
                "chunk_ids": [],
                "embedding_ids": [],
                "failure_code": "TimeoutError",
                "failure_message": "timed out",
                "metadata": {},
                "pipeline_status": "quarantined",
                "seed_id": "redis-docs",
            }
        )
    return {
        "batch_version": "2026.07.1",
        "completed_at": "2026-07-08T21:00:00Z",
        "failed_count": 1 if quarantined_failure else 0,
        "indexed_count": 1,
        "indexed_run_id": "018f5e13-2d73-7a77-a052-8d1bcbf96542",
        "operation_id": "public-docs-seed-refresh-test",
        "optional_failed_count": 0,
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "quarantined_count": 1 if quarantined_failure else 0,
        "required_failed_count": 1 if quarantined_failure else 0,
        "seed_registry_sha256": "a" * 64,
        "source_results": source_results,
        "status": status,
        "tenant_id": TENANT_ID,
    }


def _public_docs_indexed_pending_publish_coverage_proof(
    *,
    tenant_id: str = TENANT_ID,
    pack_id: str = PACK_ID,
    pack_version_id: str = PACK_VERSION_ID,
) -> dict[str, object]:
    seeds: list[dict[str, object]] = []
    for seed in default_public_docs_seed_refresh_conf(generated_at="2026-07-08T22:00:00Z")[
        "seed_registry"
    ]:
        frontier_urls = cast(list[str], seed["crawl_policy"]["frontier_urls"])
        seeds.append(
            {
                "counts": {
                    "chunks": 1,
                    "documents": 1,
                    "embeddings": 1,
                    "neo4j": 1,
                    "opensearch": 1,
                    "qdrant": 1,
                    "sections": 1,
                },
                "failure": {"code": None, "message": None},
                "index_status": "passed",
                "optional_frontier": [
                    {
                        "failure_code": None,
                        "seed_id": f"{seed['seed_id']}--{sha256(url.encode()).hexdigest()[:12]}",
                        "source_uri": url,
                        "status": "indexed",
                    }
                    for url in frontier_urls[:2]
                ],
                "seed_id": seed["seed_id"],
                "source_id": seed["source_id"],
                "source_type": seed["source_type"],
                "source_uri": seed["source_uri"],
                "status": "indexed",
            }
        )
    return {
        "artifact_type": "public_docs_coverage_proof",
        "coverage_proof_version": "2026.07.1",
        "coverage_status": "indexed_pending_publish",
        "pack_id": pack_id,
        "pack_version_id": pack_version_id,
        "seeds": seeds,
        "tenant_id": tenant_id,
    }


def _public_docs_seed(
    seed_id: str,
    source_type: str,
    source_uri: str,
    *,
    component: str,
    version: str,
) -> dict[str, Any]:
    frontier_urls = (
        [
            "https://docs.k3s.io/quick-start",
            "https://docs.k3s.io/installation/requirements",
        ]
        if seed_id == "k3s-docs"
        else []
    )
    if seed_id == "postgresql-reference-docs":
        frontier_urls = [
            "https://www.postgresql.org/docs/16/tutorial.html",
            "https://www.postgresql.org/docs/16/sql.html",
            "https://www.postgresql.org/docs/16/index.html",
        ]
    parsed = urlparse(source_uri)
    allowed_domain = parsed.hostname or "opt.adapstory"
    return {
        "approved": True,
        "connector_name": source_type,
        "crawl_policy": {
            "allowed_domains": [allowed_domain],
            "curated_frontier_urls": list(frontier_urls),
            "deny_patterns": ["/login", "/admin"],
            "frontier_urls": frontier_urls,
            "max_depth": 2,
            "max_pages": 50,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "AdapstorySERPDocsRefresh/2026.07",
        },
        "data_class": "PUBLIC",
        "inventory_evidence": {
            "component": component,
            "evidence_sha256": sha256(f"{component}:{version}".encode()).hexdigest(),
            "stack_inventory_path": "tmp/stack-inventory-2026-07-02.md",
            "version": version,
        },
        "license": {
            "distribution_rule": "cite-and-cache",
            "obligation_state": "reviewed-public-docs",
        },
        "metadata": {
            "origin": "tmp/stack-inventory-2026-07-02.md",
            "purpose": "public-docs-seed-to-serve",
        },
        "official_docs_uri": source_uri,
        "refresh_policy": {
            "cadence": "daily",
            "max_age_hours": 24,
        },
        "seed_id": seed_id,
        "source_id": str(
            __import__("uuid").uuid5(
                __import__("uuid").NAMESPACE_URL,
                f"adapstory-serp-public-docs:{seed_id}",
            )
        ),
        "source_type": source_type,
        "source_uri": source_uri,
    }
