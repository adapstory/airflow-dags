from __future__ import annotations

import ast
import importlib
import io
import json
import math
import sys
import types
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from email.message import Message
from hashlib import sha256
from pathlib import Path
from threading import Barrier
from typing import Any, ClassVar, cast
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request
from uuid import UUID

import pytest
from adapstory_serp_pipeline.benchmark.native_suite_scoring import suite_metric_profile
from adapstory_serp_pipeline.registry.evaluation_release_contract import (
    BENCHMARK_EXECUTION_SUBSTRATE_ROLE_CONTRACTS,
)

import dags.serp_eval_contracts as serp_eval_contracts_module
from dags.serp_benchmark_catalog import (
    EXTERNAL_EXECUTION_SUBSTRATE_ROLES,
    MANDATORY_BENCHMARK_SUITE_CATALOG,
    MANDATORY_EXECUTION_SUBSTRATE_ROLES,
)
from dags.serp_ds1000_contract import DS1000_LIBRARY_VERSIONS
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    SERP_NORMALIZED_GATE_FLOOR,
    _fetch_public_docs_crawler_response,
    build_benchmark_improvement_wave_plan,
    build_mandatory_benchmark_dataset_evidence_plan,
    build_nightly_regression_plan,
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
    evaluate_tenant_golden_gate,
    execute_gateway_cli_spec,
    execute_pipeline_cli_spec,
    load_materialized_benchmark_catalog_snapshot,
    load_public_docs_crawl_state_conf,
    materialize_live_benchmark_catalog_artifact,
    submit_public_docs_bc21_pipeline_state_artifact,
    write_airflow_plan_artifact,
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
    write_scheduled_d6_regression_receipt,
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


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    return (
        "sha256:"
        + sha256(serp_eval_contracts_module._canonical_json(value).encode("utf-8")).hexdigest()
    )


def _complete_v4_wheelhouse_manifest(
    manifest: dict[str, Any],
    *,
    base_image_provenance_sha256: str = "sha256:" + "b" * 64,
) -> dict[str, Any]:
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, list)
    if not any(str(artifact["fileName"]).startswith("kiwisolver-1.4.5-") for artifact in artifacts):
        artifacts.append(
            {
                "fileName": "kiwisolver-1.4.5-cp310-cp310-manylinux2010_x86_64.whl",
                "sha256": "sha256:" + "f" * 64,
                "sizeBytes": 172045,
            }
        )
        artifacts.sort(key=lambda artifact: str(artifact["fileName"]))
    cache_identity = {
        "abi": "cp310",
        "baseImageProvenanceSha256": base_image_provenance_sha256,
        "cachePolicy": "ds1000-wheelhouse-cache/v1",
        "implementation": "cp",
        "platform": "linux/amd64",
        "pythonVersion": "3.10",
        "pytorchCpuIndexUrl": "https://download.pytorch.org/whl/cpu",
        "requirementsInputSha256": "sha256:" + "1" * 64,
        "requirementsLockSha256": "sha256:" + "2" * 64,
        "resolverConstraintsSha256": "sha256:" + "3" * 64,
    }
    manifest["cacheIdentity"] = cache_identity
    manifest["cacheKey"] = _canonical_sha256(cache_identity).removeprefix("sha256:")
    manifest["schema"] = "Ds1000WheelhouseManifest/v4"
    return manifest


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


def test_public_docs_crawler_retries_transient_http_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[str] = []
    delays: list[float] = []

    class Response:
        def __init__(self) -> None:
            self.status = 200
            self.headers = {"Content-Type": "text/html"}

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, _size: int) -> bytes:
            return b"<html>recovered</html>"

    def open_request(request: Request, *, timeout: int) -> Response:
        assert timeout > 0
        attempts.append(request.full_url)
        if len(attempts) == 1:
            raise HTTPError(
                request.full_url,
                503,
                "Service Unavailable",
                hdrs=Message(),
                fp=io.BytesIO(b"temporary"),
            )
        return Response()

    monkeypatch.setattr(
        serp_eval_contracts_module,
        "_open_public_docs_crawler_request",
        open_request,
    )
    monkeypatch.setattr(serp_eval_contracts_module, "sleep", delays.append)

    response = _fetch_public_docs_crawler_response(
        "https://doc.traefik.io/traefik/",
        {"User-Agent": "serp-test/1"},
    )

    assert response.status_code == 200
    assert response.body == b"<html>recovered</html>"
    assert attempts == [
        "https://doc.traefik.io/traefik/",
        "https://doc.traefik.io/traefik/",
    ]
    assert delays == [0.5]


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


def test_governed_airflow_json_uses_rfc8785_bytes() -> None:
    payload = {
        "literals": [None, True, False],
        "numbers": [333333333.33333329, 1e30, 4.50, 2e-3, 1e-27],
        "string": '€$\u000f\nA\'B"\\"/',
    }

    assert serp_eval_contracts_module._canonical_json(payload) == (
        '{"literals":[null,true,false],'
        '"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],'
        '"string":"€$\\u000f\\nA\'B\\"\\\\\\"/"}'
    )


@pytest.mark.parametrize("unsupported", (float("nan"), float("inf"), 2**53))
def test_governed_airflow_json_rejects_cross_language_ambiguous_numbers(
    unsupported: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        serp_eval_contracts_module._canonical_json({"value": unsupported})


@pytest.mark.parametrize(
    "payload",
    (
        b'{"duplicate":1,"duplicate":2}',
        b'{ "spaced":true}',
        b'{"unicode":"\\u00e9"}',
    ),
)
def test_governed_airflow_json_parser_rejects_ambiguous_bytes(payload: bytes) -> None:
    with pytest.raises(ValueError, match="canonical RFC 8785"):
        serp_eval_contracts_module._canonical_json_object_bytes(payload, "artifact")


def test_build_nightly_regression_plan_is_reference_only_d19_orchestrator() -> None:
    plan = build_nightly_regression_plan(_scheduled_d6_conf())
    repeated = build_nightly_regression_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_nightly_regression_suite"
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "d19_run_history_observation": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/d19-run-history-observation.json"
        ),
        "d19_run_history_observation_attestation": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/d19-run-history-observation.attestation.json"
        ),
        "scheduled_regression_receipt": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/scheduled-d6-regression-receipt.json"
        ),
    }
    assert plan.payload["d19_trigger_conf"] == {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "evaluation_release_promotion_evidence": _d19_worm_evidence(
            "model-releases/d17-promotion", "c"
        ),
        "generated_at": "2026-07-17T00:00:00Z",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
    }
    assert "prior_paired_evaluation_verification_evidence" not in plan.payload
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_nightly_regression_plan",
        "produce_d19_run_history_observation",
        "trigger_benchmark_improvement_wave",
        "load_triggered_d19_verification",
        "observe_triggered_d19_run",
        "write_scheduled_d6_regression_receipt",
        "release_d19_history_fence",
        "finalize_scheduled_d6_regression",
    ]
    serialized = plan.to_canonical_json()
    for legacy_field in (
        "benchmark_suite_inputs",
        "nightly_report",
        "normalized_gate_floor",
        "prior_paired_evaluation_verification_evidence",
        "selected_suite_ids",
        "suite_plan",
    ):
        assert legacy_field not in serialized

    url_artifact_root = _scheduled_d6_conf()
    url_artifact_root["artifact_root_path"] = "https://example.invalid/serp-evals"
    with pytest.raises(ValueError, match="scheduled D6 requires an s3:// artifact_root_path"):
        build_nightly_regression_plan(url_artifact_root)

    caller_history = _scheduled_d6_conf()
    caller_history["d19_run_history_observation_evidence"] = _d19_worm_evidence(
        "history/caller-supplied", "e"
    )
    with pytest.raises(ValueError, match="history observation is runtime-produced"):
        build_nightly_regression_plan(caller_history)

    caller_prior_handles = _scheduled_d6_conf()
    caller_prior_handles["prior_paired_evaluation_verification_evidence"] = [
        _d19_worm_evidence(f"verification/prior-{index}", str(index)) for index in range(1, 4)
    ]
    with pytest.raises(ValueError, match="legacy D6 scorer fields are unsupported"):
        build_nightly_regression_plan(caller_prior_handles)


def test_default_nightly_regression_conf_is_runtime_owned_and_canonical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_defaults = {
        "ADAPSTORY_AIRFLOW_ARTIFACT_ROOT": "s3://airflow-serp-evidence/serp-evals",
        "ADAPSTORY_SERP_D6_ACTOR_ID": "airflow-serp-eval-runner",
        "ADAPSTORY_SERP_D6_EVALUATION_RELEASE_PROMOTION_EVIDENCE": json.dumps(
            _d19_worm_evidence("model-releases/d17-promotion", "c")
        ),
        "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_ID": REGISTRY_RESOURCE_ID,
        "ADAPSTORY_SERP_D6_REGISTRY_RESOURCE_TYPE": "workflow",
        "ADAPSTORY_SERP_D6_TENANT_ID": TENANT_ID,
    }
    for name, value in runtime_defaults.items():
        monkeypatch.setenv(name, value)

    conf = default_nightly_regression_conf(generated_at="2026-07-14T08:00:00Z")
    plan = build_nightly_regression_plan(conf)

    assert conf == _scheduled_d6_conf() | {"generated_at": "2026-07-14T08:00:00Z"}
    assert (
        plan.payload["d19_trigger_conf"]["evaluation_release_promotion_evidence"]
        == conf["evaluation_release_promotion_evidence"]
    )

    monkeypatch.setenv(
        "ADAPSTORY_SERP_D6_PRIOR_PAIRED_EVALUATION_VERIFICATION_EVIDENCE",
        json.dumps(
            [_d19_worm_evidence(f"verification/prior-{index}", str(index)) for index in range(1, 4)]
        ),
    )
    with pytest.raises(
        ValueError,
        match="legacy D6 prior verification env pointers are unsupported",
    ):
        default_nightly_regression_conf(generated_at="2026-07-14T08:00:00Z")


def test_d6_history_observation_is_runtime_fresh_fenced_worm_and_transit_attested() -> None:
    plan = build_nightly_regression_plan(_scheduled_d6_conf())
    parent_run = _scheduled_d6_airflow_run()
    history = _scheduled_d6_history_client_result(parent_run)
    fence_client = _D6FenceClient(_scheduled_d6_fence(parent_run))
    writes: list[dict[str, Any]] = []

    def snapshot_writer(**kwargs: Any) -> dict[str, Any]:
        writes.append(kwargs)
        payload = cast(Mapping[str, Any], kwargs["payload"])
        return _d6_write_receipt(
            str(kwargs["artifact_path"]),
            serp_eval_contracts_module._canonical_json(payload).encode("utf-8"),
            artifact_type=str(kwargs["artifact_type"]),
        )

    def attestation_sealer(
        write_receipt: Mapping[str, Any],
        *,
        purpose: str,
    ) -> tuple[dict[str, str], dict[str, Any]]:
        assert purpose == "serp-d19-run-history-observation"
        subject = _d6_worm_from_write_receipt(write_receipt)
        attestation = _d6_history_attestation_evidence(plan)
        return attestation, _d6_history_attestation_verification(
            subject=subject,
            attestation=attestation,
        )

    result = serp_eval_contracts_module.produce_d19_run_history_observation(
        plan.to_canonical_json(),
        parent_run,
        history_client=_StaticD6HistoryClient(history),
        fence_client=fence_client,
        clock=lambda: datetime(2026, 7, 17, 0, 0, 10, tzinfo=UTC),
        snapshot_writer=snapshot_writer,
        attestation_sealer=attestation_sealer,
    )

    assert result["d19TriggerConf"] == {
        **plan.payload["d19_trigger_conf"],
        "scheduled_d6_fence": _scheduled_d6_fence(parent_run),
    }
    assert result["fence"] == _scheduled_d6_fence(parent_run)
    assert set(result) == {
        "d19RunHistoryObservationAttestationEvidence",
        "d19RunHistoryObservationEvidence",
        "d19RunHistoryObservationVerification",
        "d19TriggerConf",
        "fence",
    }
    assert len(writes) == 1
    payload = cast(Mapping[str, Any], writes[0]["payload"])
    assert payload == {
        **history,
        "fence": _scheduled_d6_fence(parent_run),
        "generatedAt": "2026-07-17T00:00:10Z",
        "parentAirflowRun": parent_run,
        "producer": {
            "namespace": "airflow",
            "serviceAccount": "airflow-serp-d19-history-observer",
        },
        "schema": "D19RunHistoryObservation/v2",
    }
    assert (
        writes[0]["artifact_path"] == plan.payload["artifact_paths"]["d19_run_history_observation"]
    )
    assert writes[0]["artifact_type"] == "d19_run_history_observation"
    assert fence_client.required == [_scheduled_d6_fence(parent_run)]
    assert fence_client.released == []


def test_d6_history_consumer_accepts_a_truthful_bounded_tail_snapshot() -> None:
    """The D6 consumer needs only the exact newest accepted manual run streak."""

    parent_run = _scheduled_d6_airflow_run()
    history = _scheduled_d6_history_client_result(parent_run)
    history["pagination"] = {
        "complete": False,
        "observedEntries": 3,
        "pageCount": 2,
        "pageLimit": 2,
        "strategy": "bounded-tail",
        "tailStartOffset": 7,
        "totalEntries": 10,
    }

    normalized = serp_eval_contracts_module._normalized_d19_history_client_result(
        history,
        parent_airflow_run=parent_run,
        artifact_root_path="s3://airflow-serp-evidence/serp-evals",
    )

    assert normalized["runs"] == _scheduled_d6_prior_runs()
    assert normalized["acceptedRunVerifications"] == history["acceptedRunVerifications"]
    assert normalized["pagination"] == history["pagination"]


@pytest.mark.parametrize(
    ("mutation", "match"),
    (
        ("complete", "bounded-tail pagination must be incomplete"),
        ("observed_entries", "bounded tail must observe exactly three runs"),
        ("tail_start", "bounded tail must start at the exact newest streak offset"),
    ),
)
def test_d6_history_consumer_rejects_misleading_bounded_tail_metadata(
    mutation: str,
    match: str,
) -> None:
    parent_run = _scheduled_d6_airflow_run()
    history = _scheduled_d6_history_client_result(parent_run)
    history["pagination"] = {
        "complete": False,
        "observedEntries": 3,
        "pageCount": 2,
        "pageLimit": 2,
        "strategy": "bounded-tail",
        "tailStartOffset": 7,
        "totalEntries": 10,
    }
    if mutation == "complete":
        history["pagination"]["complete"] = True
    elif mutation == "observed_entries":
        history["pagination"]["observedEntries"] = 2
    elif mutation == "tail_start":
        history["pagination"]["tailStartOffset"] = 6

    with pytest.raises(ValueError, match=match):
        serp_eval_contracts_module._normalized_d19_history_client_result(
            history,
            parent_airflow_run=parent_run,
            artifact_root_path="s3://airflow-serp-evidence/serp-evals",
        )


@pytest.mark.parametrize("race", ("inserted_terminal_run", "terminal_state_transition"))
def test_d6_history_observation_rejects_changes_between_fenced_history_reads(
    race: str,
) -> None:
    plan = build_nightly_regression_plan(_scheduled_d6_conf())
    parent_run = _scheduled_d6_airflow_run()
    prior_runs = _scheduled_d6_prior_runs()
    prior_pointers = [
        _scheduled_d6_prior_pointer(index, run) for index, run in enumerate(prior_runs, start=1)
    ]
    older_run = {
        "dagId": "serp_benchmark_improvement_wave",
        "logicalDate": "2026-07-01T00:00:00Z",
        "runId": "manual__terminal-race",
        "runType": "manual",
        "state": "failed",
    }
    if race == "inserted_terminal_run":
        first = _scheduled_d6_history_client_result(
            parent_run,
            runs=prior_runs,
            accepted_verifications=prior_pointers,
        )
        second_runs = [older_run, *prior_runs]
    else:
        first = _scheduled_d6_history_client_result(
            parent_run,
            runs=[older_run, *prior_runs],
            accepted_verifications=prior_pointers,
        )
        transitioned = {**older_run, "state": "success"}
        second_runs = [transitioned, *prior_runs]
    second = _scheduled_d6_history_client_result(
        parent_run,
        runs=second_runs,
        accepted_verifications=prior_pointers,
    )
    history_client = _SequenceD6HistoryClient(first, second)
    fence = _scheduled_d6_fence(parent_run)
    fence_client = _D6FenceClient(fence)

    def unexpected_write(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("an unstable history snapshot must not be persisted")

    with pytest.raises(ValueError, match="changed while the D19 fence was active"):
        serp_eval_contracts_module.produce_d19_run_history_observation(
            plan.to_canonical_json(),
            parent_run,
            history_client=history_client,
            fence_client=fence_client,
            clock=lambda: datetime(2026, 7, 17, 0, 0, 10, tzinfo=UTC),
            snapshot_writer=unexpected_write,
        )

    assert history_client.collect_count == 2
    assert fence_client.required == [fence]
    assert fence_client.released == [fence]


@pytest.mark.parametrize(
    ("mutation", "match"),
    (
        ("stale", "within five minutes of parent start"),
        ("wrong_window", "logicalDateLt must match parent"),
        ("truncated", "complete pagination"),
        ("active_race", "active D19 runs"),
        ("unsupported_server_version", "supported 3.x"),
        ("expired_fence", "fence must remain active"),
        ("missing_attestation", "Transit attestation"),
    ),
)
def test_d6_history_observation_fails_closed_and_releases_fence(
    mutation: str,
    match: str,
) -> None:
    plan = build_nightly_regression_plan(_scheduled_d6_conf())
    parent_run = _scheduled_d6_airflow_run()
    history = _scheduled_d6_history_client_result(parent_run)
    fence = _scheduled_d6_fence(parent_run)
    clock_at = datetime(2026, 7, 17, 0, 0, 10, tzinfo=UTC)
    if mutation == "stale":
        clock_at = datetime(2026, 7, 17, 0, 6, tzinfo=UTC)
    elif mutation == "wrong_window":
        history["query"]["logicalDateLt"] = "2026-07-17T00:00:01Z"
    elif mutation == "truncated":
        history["pagination"]["complete"] = False
    elif mutation == "active_race":
        history["activeRunQuery"]["totalEntries"] = 1
    elif mutation == "unsupported_server_version":
        history["api"]["airflowVersion"] = "4.0.0"
    elif mutation == "expired_fence":
        fence["acquiredAt"] = "2026-07-17T00:00:04Z"
        fence["expiresAt"] = "2026-07-17T00:00:09Z"
        fence["leaseDurationSeconds"] = 5
    fence_client = _D6FenceClient(fence)

    def clock() -> datetime:
        return clock_at

    def snapshot_writer(**kwargs: Any) -> dict[str, Any]:
        payload = cast(Mapping[str, Any], kwargs["payload"])
        return _d6_write_receipt(
            str(kwargs["artifact_path"]),
            serp_eval_contracts_module._canonical_json(payload).encode("utf-8"),
            artifact_type=str(kwargs["artifact_type"]),
        )

    def attestation_sealer(
        write_receipt: Mapping[str, Any],
        *,
        purpose: str,
    ) -> tuple[dict[str, str], dict[str, Any]]:
        if mutation == "missing_attestation":
            return {}, {}
        subject = _d6_worm_from_write_receipt(write_receipt)
        attestation = _d6_history_attestation_evidence(plan)
        return attestation, _d6_history_attestation_verification(
            subject=subject,
            attestation=attestation,
        )

    with pytest.raises(ValueError, match=match):
        serp_eval_contracts_module.produce_d19_run_history_observation(
            plan.to_canonical_json(),
            parent_run,
            history_client=_StaticD6HistoryClient(history),
            fence_client=fence_client,
            clock=clock,
            snapshot_writer=snapshot_writer,
            attestation_sealer=attestation_sealer,
        )

    assert fence_client.released == [fence]


def test_scheduled_d6_receipt_proves_three_prior_accepts_and_one_unique_child() -> None:
    fixture = _scheduled_d6_receipt_fixture()
    written_payloads: list[dict[str, Any]] = []

    def writer(**kwargs: Any) -> dict[str, Any]:
        payload = dict(cast(Mapping[str, Any], kwargs["payload"]))
        written_payloads.append(payload)
        receipt = _d6_write_receipt(
            str(kwargs["artifact_path"]),
            serp_eval_contracts_module._canonical_json(payload).encode("utf-8"),
            artifact_type=str(kwargs["artifact_type"]),
        )
        fixture["objects"][(receipt["artifactPath"], receipt["artifactVersionId"])] = payload
        return receipt

    result = write_scheduled_d6_regression_receipt(
        fixture["plan"].to_canonical_json(),
        fixture["history_result"],
        fixture["triggered_verification"],
        fixture["current_observation"],
        evidence_reader=fixture["reader"],
        snapshot_writer=writer,
        clock=lambda: datetime(2026, 7, 17, 4, 0, tzinfo=UTC),
    )

    assert result["status"] == "accepted"
    assert result["operationId"] == fixture["plan"].payload["operation_id"]
    assert (
        result["scheduledD6RegressionEvidence"]["s3Uri"]
        == fixture["plan"].payload["artifact_paths"]["scheduled_regression_receipt"]
    )
    assert len(written_payloads) == 1
    payload = written_payloads[0]
    assert payload["schema"] == "ScheduledD6RegressionReceipt/v2"
    assert payload["status"] == "accepted"
    assert payload["acceptedStreakLength"] == 4
    current_receipt = fixture["objects"][fixture["current_receipt_key"]]
    assert payload["authority"] == {
        field_name: current_receipt[field_name]
        for field_name in (
            "baselineReleaseEvidence",
            "candidateReleaseEvidence",
            "evaluationBindingEvidence",
            "evaluationBindingId",
            "evaluationObjectiveAttestationEvidence",
            "evaluationObjectiveEvidence",
            "evaluationReleasePromotionEvidence",
            "metricCompatibilityMatrixEvidence",
        )
    }
    assert len(payload["priorAcceptedEvaluations"]) == 3
    assert payload["triggeredEvaluation"]["airflowRun"] == fixture["child_run"]
    assert payload["currentRunObservation"] == fixture["current_observation"]
    assert (
        payload["triggeredEvaluation"]["observedNormalizedScoreCellsEvidence"]
        == (fixture["triggered_verification"]["observedNormalizedScoreCellsEvidence"])
    )


def test_scheduled_d6_receipt_rejects_score_cells_not_matching_the_signed_receipt() -> None:
    fixture = _scheduled_d6_receipt_fixture()
    score_handle = fixture["triggered_verification"]["observedNormalizedScoreCellsEvidence"]
    score_cells = fixture["objects"][(score_handle["s3Uri"], score_handle["versionId"])]
    score_cells["cells"][0]["candidate"].update({"meanScore": 0.864, "normalizedMean": 0.96})
    score_cells["benchmarkScore"]["supportingAggregates"]["meanCandidateNormalizedMean"] = (
        0.96 + 8 * 0.95
    ) / 9

    with pytest.raises(
        ValueError,
        match="observed normalized score-cell evidence does not match its signed receipt",
    ):
        write_scheduled_d6_regression_receipt(
            fixture["plan"].to_canonical_json(),
            fixture["history_result"],
            fixture["triggered_verification"],
            fixture["current_observation"],
            evidence_reader=fixture["reader"],
            clock=lambda: datetime(2026, 7, 17, 4, 0, tzinfo=UTC),
        )


def test_scheduled_d6_self_advances_the_exact_streak_on_the_next_daily_run() -> None:
    first = _scheduled_d6_receipt_fixture()
    first_history = first["objects"][first["history_observation_key"]]
    shifted_runs = [
        *[dict(run) for run in first_history["runs"][1:]],
        {**first["child_run"], "state": "success"},
    ]
    shifted_pointers = [
        *[dict(pointer) for pointer in first_history["acceptedRunVerifications"][1:]],
        dict(first["triggered_verification"]),
    ]
    second = _scheduled_d6_receipt_fixture(
        generated_at="2026-07-18T00:00:00Z",
        prior_runs=shifted_runs,
        prior_pointers=shifted_pointers,
        seed_objects=first["objects"],
        current_request_index=5,
    )
    written_payloads: list[dict[str, Any]] = []

    def writer(**kwargs: Any) -> dict[str, Any]:
        payload = dict(cast(Mapping[str, Any], kwargs["payload"]))
        written_payloads.append(payload)
        receipt = _d6_write_receipt(
            str(kwargs["artifact_path"]),
            serp_eval_contracts_module._canonical_json(payload).encode("utf-8"),
            artifact_type=str(kwargs["artifact_type"]),
        )
        second["objects"][(receipt["artifactPath"], receipt["artifactVersionId"])] = payload
        return receipt

    result = write_scheduled_d6_regression_receipt(
        second["plan"].to_canonical_json(),
        second["history_result"],
        second["triggered_verification"],
        second["current_observation"],
        evidence_reader=second["reader"],
        snapshot_writer=writer,
        clock=lambda: datetime(2026, 7, 18, 4, 0, tzinfo=UTC),
    )

    assert result["status"] == "accepted"
    assert "prior_paired_evaluation_verification_evidence" not in second["plan"].payload
    assert [
        entry["airflowRun"]["runId"] for entry in written_payloads[0]["priorAcceptedEvaluations"]
    ] == [run["runId"] for run in shifted_runs]
    assert (
        written_payloads[0]["priorAcceptedEvaluations"][-1]["verificationEvidence"]
        == first["triggered_verification"]["pairedEvaluationVerificationEvidence"]
    )


@pytest.mark.parametrize(
    ("mutation", "match"),
    (
        ("duplicate_request", "requestId values must be unique"),
        ("intervening_failure", "last three historical D19 runs"),
        ("promotion_drift", "evaluation authority must remain identical"),
        ("baseline_release_drift", "evaluation authority must remain identical"),
        ("evaluation_binding_evidence_drift", "evaluation authority must remain identical"),
        ("evaluation_binding_id_drift", "evaluation authority must remain identical"),
        ("metric_matrix_drift", "evaluation authority must remain identical"),
        ("duplicate_child", "sameLogicalDateRunCount must equal one"),
    ),
)
def test_scheduled_d6_receipt_rejects_fabricated_streaks(
    mutation: str,
    match: str,
) -> None:
    fixture = _scheduled_d6_receipt_fixture()
    if mutation == "duplicate_request":
        current = fixture["objects"][fixture["current_verification_key"]]
        current["requestId"] = _d6_request_id(3)
        current["operationId"] = _d6_request_id(3)
        current_receipt = fixture["objects"][fixture["current_receipt_key"]]
        current_receipt["requestId"] = _d6_request_id(3)
        current_receipt["pairedEvaluation"]["operationId"] = _d6_request_id(3)
        fixture["triggered_verification"]["requestId"] = _d6_request_id(3)
    elif mutation == "intervening_failure":
        history = fixture["objects"][fixture["history_observation_key"]]
        history["runs"].append(
            {
                "dagId": "serp_benchmark_improvement_wave",
                "logicalDate": "2026-07-16T12:00:00Z",
                "runId": "manual__intervening-failure",
                "runType": "manual",
                "state": "failed",
            }
        )
        history["pagination"].update({"observedEntries": 4, "pageCount": 1, "totalEntries": 4})
    elif mutation == "promotion_drift":
        current_receipt = fixture["objects"][fixture["current_receipt_key"]]
        current_receipt["evaluationReleasePromotionEvidence"] = _d19_worm_evidence(
            "model-releases/foreign-promotion", "9"
        )
    elif mutation == "baseline_release_drift":
        current_receipt = fixture["objects"][fixture["current_receipt_key"]]
        current_receipt["baselineReleaseEvidence"] = _d19_worm_evidence(
            "model-releases/foreign-baseline", "9"
        )
    elif mutation == "evaluation_binding_evidence_drift":
        current_receipt = fixture["objects"][fixture["current_receipt_key"]]
        current_receipt["evaluationBindingEvidence"] = _d19_worm_evidence(
            "evaluation-bindings/foreign", "9"
        )
    elif mutation == "evaluation_binding_id_drift":
        current_receipt = fixture["objects"][fixture["current_receipt_key"]]
        current_receipt["evaluationBindingId"] = "018f5e13-2d73-7a77-a052-8d1bcbf96799"
    elif mutation == "metric_matrix_drift":
        current_receipt = fixture["objects"][fixture["current_receipt_key"]]
        current_receipt["metricCompatibilityMatrixEvidence"] = _d19_worm_evidence(
            "metric-matrices/foreign", "9"
        )
    elif mutation == "duplicate_child":
        fixture["current_observation"]["sameLogicalDateRunCount"] = 2

    with pytest.raises(ValueError, match=match):
        write_scheduled_d6_regression_receipt(
            fixture["plan"].to_canonical_json(),
            fixture["history_result"],
            fixture["triggered_verification"],
            fixture["current_observation"],
            evidence_reader=fixture["reader"],
            snapshot_writer=lambda **_: {},
            clock=lambda: datetime(2026, 7, 17, 4, 0, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    "field_name",
    (
        "baselineReleaseEvidence",
        "candidateReleaseEvidence",
        "evaluationObjectiveAttestationEvidence",
        "evaluationObjectiveEvidence",
        "metricCompatibilityMatrixEvidence",
    ),
)
def test_scheduled_d6_receipt_binds_every_release_authority_to_d17_promotion(
    field_name: str,
) -> None:
    fixture = _scheduled_d6_receipt_fixture()
    foreign = _d19_worm_evidence(f"foreign/{field_name}", "9")
    receipt_keys = [
        *[(handle["s3Uri"], handle["versionId"]) for handle in fixture["prior_receipt_handles"]],
        fixture["current_receipt_key"],
    ]
    for key in receipt_keys:
        fixture["objects"][key][field_name] = foreign

    with pytest.raises(ValueError, match="does not match the D17 promotion"):
        write_scheduled_d6_regression_receipt(
            fixture["plan"].to_canonical_json(),
            fixture["history_result"],
            fixture["triggered_verification"],
            fixture["current_observation"],
            evidence_reader=fixture["reader"],
            snapshot_writer=lambda **_: {},
            clock=lambda: datetime(2026, 7, 17, 4, 0, tzinfo=UTC),
        )


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
        native_adapter_materializer=_native_adapter_materializer,
        native_corpus_materializer=_native_corpus_materializer,
        execution_substrate_materializer=_execution_substrate_materializer,
    )

    assert result["catalogStatus"] == "ready"
    assert result["blockingSuiteIds"] == []
    assert result["suiteSummary"] == [
        {
            "distributionRule": entry.distribution_rule,
            "executionStatus": "ready",
            "rightsStatus": entry.rights_status,
            "suiteId": entry.suite_id,
        }
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    ]
    assert len(written) == (
        (len(MANDATORY_SERP_BENCHMARK_SUITES) * 6)
        + 4
        + sum(len(roles) for roles in MANDATORY_EXECUTION_SUBSTRATE_ROLES.values())
    )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("schema", "GitOpsCheckoutProvenance/v0"),
        ("commit", "A" * 40),
        ("tree", "c" * 39),
        ("origin", "https://github.com/example/Adapstory-GitOps.git"),
        ("pipelinePath", "Jenkinsfile"),
        ("buildUrl", "https://jenkins.adapstory.com/job/serp-benchmark-sandbox-supply/2/"),
    ),
)
def test_benchmark_source_set_checkout_provenance_rejects_noncanonical_fields(
    field_name: str,
    invalid_value: str,
) -> None:
    checkout_provenance = {
        "buildUrl": "https://jenkins.adapstory.com/job/serp-benchmark-sandbox-supply/1/",
        "commit": "b" * 40,
        "origin": "https://github.com/adapstory/Adapstory-GitOps.git",
        "pipelinePath": "infra/ci/jenkins/pipelines/serp-benchmark-sandbox-supply.jenkinsfile",
        "schema": "GitOpsCheckoutProvenance/v1",
        "tree": "c" * 40,
    }
    checkout_provenance[field_name] = invalid_value

    with pytest.raises(ValueError, match="checkout provenance"):
        serp_eval_contracts_module._normalized_gitops_checkout_provenance(
            checkout_provenance,
            field_name="benchmark execution substrate source set",
            operation_id="ci-benchmark-substrates-1",
        )


def test_ds1000_wheelhouse_manifest_rejects_missing_or_altered_cpu_torch_root() -> None:
    def wheel_name(name: str, version: str) -> str:
        if name == "torch":
            return "torch-2.2.0+cpu-cp310-cp310-manylinux_2_17_x86_64.whl"
        return f"{name.replace('-', '_')}-{version}-py3-none-any.whl"

    manifest: dict[str, Any] = {
        "artifacts": [
            {
                "fileName": file_name,
                "sha256": "sha256:" + f"{index:064x}",
                "sizeBytes": index + 1,
            }
            for index, file_name in enumerate(
                sorted(wheel_name(name, version) for name, version in DS1000_LIBRARY_VERSIONS)
            )
        ],
        "directRequirements": [
            {"name": name, "version": version} for name, version in DS1000_LIBRARY_VERSIONS
        ],
        "platform": "linux/amd64",
        "pythonVersion": "3.10",
        "pytorchVariant": "cpuonly",
        "schema": "Ds1000WheelhouseManifest/v4",
    }
    _complete_v4_wheelhouse_manifest(manifest)

    assert (
        serp_eval_contracts_module._normalized_ds1000_wheelhouse_manifest(
            manifest,
            field_name="DS-1000 wheelhouse manifest",
        )["directRequirements"]
        == manifest["directRequirements"]
    )

    missing_torch = deepcopy(manifest)
    missing_torch["directRequirements"] = [
        requirement
        for requirement in manifest["directRequirements"]
        if requirement["name"] != "torch"
    ]
    with pytest.raises(ValueError, match="direct requirements are unsupported"):
        serp_eval_contracts_module._normalized_ds1000_wheelhouse_manifest(
            missing_torch,
            field_name="DS-1000 wheelhouse manifest",
        )

    altered_torch = deepcopy(manifest)
    altered_torch["directRequirements"] = [
        {
            **requirement,
            **({"version": "2.2.0+cu121"} if requirement["name"] == "torch" else {}),
        }
        for requirement in manifest["directRequirements"]
    ]
    with pytest.raises(ValueError, match="direct requirements are unsupported"):
        serp_eval_contracts_module._normalized_ds1000_wheelhouse_manifest(
            altered_torch,
            field_name="DS-1000 wheelhouse manifest",
        )


def test_execution_substrate_source_set_loads_only_exact_worm_role_versions() -> None:
    operation_id = "ci-benchmark-substrates-1"
    role_file_names = {
        (suite_id, role): file_name
        for suite_id, role, file_name in BENCHMARK_EXECUTION_SUBSTRATE_ROLE_CONTRACTS
    }
    assert set(role_file_names) == {
        (suite_id, role)
        for suite_id, roles in EXTERNAL_EXECUTION_SUBSTRATE_ROLES.items()
        for role in roles
    }
    role_payloads = {
        (suite_id, role): f"sealed:{suite_id}:{role}".encode()
        for suite_id, roles in EXTERNAL_EXECUTION_SUBSTRATE_ROLES.items()
        for role in roles
    }
    objects: dict[tuple[str, str], bytes] = {}
    suite_entries: list[dict[str, Any]] = []
    for suite_id, roles in EXTERNAL_EXECUTION_SUBSTRATE_ROLES.items():
        role_entries: list[dict[str, Any]] = []
        for role in roles:
            payload = role_payloads[(suite_id, role)]
            key = f"serp-evals/{operation_id}/roles/" f"{role_file_names[(suite_id, role)]}"
            version_id = f"version-{suite_id.casefold().replace(' ', '-')}-{role}"
            objects[(key, version_id)] = payload
            role_entries.append(
                {
                    "evidence": {
                        "objectLockMode": "COMPLIANCE",
                        "retainUntil": "2027-07-15T00:00:00Z",
                        "s3Uri": f"s3://airflow-serp-evidence/{key}",
                        "sha256": "sha256:" + sha256(payload).hexdigest(),
                        "versionId": version_id,
                    },
                    "role": role,
                }
            )
        suite_entries.append({"roles": role_entries, "suiteId": suite_id})

    source_root = f"s3://airflow-serp-evidence/serp-evals/{operation_id}"
    base_image_provenance: dict[str, Any] = {
        "imageReference": (
            "harbor.adapstory.com/dockerhub-cache/library/python@sha256:" + "c" * 64
        ),
        "platform": "linux/amd64",
        "schema": "Ds1000BaseImageProvenance/v1",
        "sourceReference": "harbor.adapstory.com/dockerhub-cache/library/python:3.10-slim-bookworm",
    }

    def sbom_handle(name: str) -> dict[str, str]:
        return {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-07-15T00:00:00Z",
            "s3Uri": f"{source_root}/sboms/{name}.json",
            "sha256": "sha256:" + "e" * 64,
            "versionId": f"sbom-{name}-v1",
        }

    def swe_sbom_handle(instance_id: str) -> dict[str, str]:
        return {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-07-15T00:00:00Z",
            "s3Uri": (
                f"{source_root}/sboms/swe/" f"{sha256(instance_id.encode()).hexdigest()}.cdx.json"
            ),
            "sha256": "sha256:" + "e" * 64,
            "versionId": f"sbom-swe-{instance_id}-v1",
        }

    wheelhouse_manifest: dict[str, Any] = {
        "artifacts": [
            {
                "fileName": file_name,
                "sha256": "sha256:" + f"{index:064x}",
                "sizeBytes": index + 1,
            }
            for index, file_name in enumerate(
                (
                    "datasets-2.19.1-py3-none-any.whl",
                    "gensim-4.3.2-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "matplotlib-3.8.4-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "numpy-1.26.4-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "pandas-1.5.3-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "scikit_learn-1.4.0-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "scipy-1.12.0-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "seaborn-0.13.2-py3-none-any.whl",
                    "statsmodels-0.14.1-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "tensorflow_cpu-2.16.1-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "torch-2.2.0+cpu-cp310-cp310-manylinux_2_17_x86_64.whl",
                    "tqdm-4.66.4-py3-none-any.whl",
                    "xgboost-2.0.3-cp310-cp310-manylinux_2_17_x86_64.whl",
                )
            )
        ],
        "directRequirements": [
            {"name": name, "version": version} for name, version in DS1000_LIBRARY_VERSIONS
        ],
        "platform": "linux/amd64",
        "pythonVersion": "3.10",
        "pytorchVariant": "cpuonly",
        "schema": "Ds1000WheelhouseManifest/v4",
    }
    _complete_v4_wheelhouse_manifest(
        wheelhouse_manifest,
        base_image_provenance_sha256=_canonical_sha256(base_image_provenance),
    )
    wheelhouse_resolution: dict[str, Any] = {
        "cacheEntryEvidence": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-07-15T00:00:00Z",
            "s3Uri": (
                "s3://airflow-serp-evidence/serp-evals/ds1000-wheelhouse-cache/v1/"
                f"{wheelhouse_manifest['cacheKey']}/entry.json"
            ),
            "sha256": "sha256:" + "9" * 64,
            "versionId": "ds1000-wheelhouse-cache-entry-version",
        },
        "cacheIdentity": deepcopy(wheelhouse_manifest["cacheIdentity"]),
        "cacheKey": wheelhouse_manifest["cacheKey"],
        "manifestSha256": _canonical_sha256(wheelhouse_manifest),
        "operationId": operation_id,
        "schema": "Ds1000WheelhouseResolution/v1",
    }
    wheelhouse_resolution_bytes = json.dumps(
        wheelhouse_resolution, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    wheelhouse_resolution_key = f"serp-evals/{operation_id}/wheelhouses/ds1000/resolution.json"
    wheelhouse_resolution_version = "ds1000-wheelhouse-resolution-version"
    objects[(wheelhouse_resolution_key, wheelhouse_resolution_version)] = (
        wheelhouse_resolution_bytes
    )
    wheelhouse_resolution_evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/{wheelhouse_resolution_key}",
        "sha256": "sha256:" + sha256(wheelhouse_resolution_bytes).hexdigest(),
        "versionId": wheelhouse_resolution_version,
    }
    base_image_bytes = json.dumps(
        base_image_provenance, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    base_image_key = f"serp-evals/{operation_id}/base-images/ds1000/provenance.json"
    base_image_version = "ds1000-base-image-provenance-version"
    objects[(base_image_key, base_image_version)] = base_image_bytes
    base_image_evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/{base_image_key}",
        "sha256": "sha256:" + sha256(base_image_bytes).hexdigest(),
        "versionId": base_image_version,
    }
    dataset_provenance: dict[str, Any] = {
        "datasetPath": "data/ds1000.jsonl.gz",
        "ds1000Revision": "b39aab71da6d23ef8d3cac59a7c5f834516ab334",
        "fieldNames": ["code_context", "metadata", "prompt", "reference_code"],
        "rowCount": 1000,
        "schema": "Ds1000SimplifiedDatasetProvenance/v1",
        "sha256": "sha256:" + "a" * 64,
    }
    dataset_bytes = json.dumps(
        dataset_provenance, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    dataset_key = f"serp-evals/{operation_id}/datasets/ds1000/simplified-provenance.json"
    dataset_version = "ds1000-dataset-provenance-version"
    objects[(dataset_key, dataset_version)] = dataset_bytes
    dataset_evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/{dataset_key}",
        "sha256": "sha256:" + sha256(dataset_bytes).hexdigest(),
        "versionId": dataset_version,
    }
    supply_attestations: dict[str, Any] = {
        "ds1000": {
            "baseImageProvenanceEvidence": base_image_evidence,
            "baseImageProvenanceSha256": base_image_evidence["sha256"],
            "datasetProvenanceEvidence": dataset_evidence,
            "datasetProvenanceSha256": dataset_evidence["sha256"],
            "imageReference": (
                "harbor.adapstory.com/benchmark-sandboxes/ds1000@sha256:" + "d" * 64
            ),
            "sbomEvidence": sbom_handle("ds1000/image.cdx"),
            "signatureStatus": "signed-and-verified",
            "wheelhouseManifestSha256": _canonical_sha256(wheelhouse_manifest),
            "wheelhouseResolutionEvidence": wheelhouse_resolution_evidence,
            "wheelhouseResolutionSha256": wheelhouse_resolution_evidence["sha256"],
        },
        "schema": "BenchmarkSubstrateSupplyAttestations/v4",
        "sweBench": {
            "datasetRevision": "91aa3ed51b709be6457e12d00300a6a596d4c6a3",
            "images": [
                {
                    "imageReference": (
                        "harbor.adapstory.com/benchmark-sandboxes/swe-bench/"
                        f"owner-repo-{index:03d}@sha256:{index:064x}"
                    ),
                    "instanceId": f"owner__repo-{index:03d}",
                    "sbomEvidence": swe_sbom_handle(f"owner__repo-{index:03d}"),
                    "signatureStatus": "signed-and-verified",
                }
                for index in range(500)
            ],
        },
    }
    supply_bytes = json.dumps(
        supply_attestations, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    supply_key = f"serp-evals/{operation_id}/supply-attestations.json"
    supply_version = "supply-attestations-version"
    objects[(supply_key, supply_version)] = supply_bytes
    supply_evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/{supply_key}",
        "sha256": "sha256:" + sha256(supply_bytes).hexdigest(),
        "versionId": supply_version,
    }
    ds1000_inventory: dict[str, Any] = {
        "baseImage": base_image_provenance,
        "datasetProvenance": dataset_provenance,
        "dockerSocketMounted": False,
        "ds1000Revision": "b39aab71da6d23ef8d3cac59a7c5f834516ab334",
        "imageDigest": "sha256:" + "d" * 64,
        "imagePurpose": "ds1000-simplified-official-execution",
        "imageReference": "harbor.adapstory.com/benchmark-sandboxes/ds1000@sha256:" + "d" * 64,
        "libraries": wheelhouse_manifest["directRequirements"],
        "networkMode": "disabled",
        "officialDatasetPath": "data/ds1000.jsonl.gz",
        "pythonVersion": "3.10",
        "pytorchVariant": "cpuonly",
        "readOnlyRootFilesystem": True,
        "schema": "Ds1000SandboxImageInventory/v2",
        "suiteId": "DS-1000",
    }
    ds1000_role_payload = json.dumps(
        ds1000_inventory, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    ds1000_role = ("DS-1000", "execution-sandbox")
    role_payloads[ds1000_role] = ds1000_role_payload
    ds1000_role_key = f"serp-evals/{operation_id}/roles/{role_file_names[ds1000_role]}"
    ds1000_role_version = f"version-{ds1000_role[0].casefold()}-{ds1000_role[1]}"
    objects[(ds1000_role_key, ds1000_role_version)] = ds1000_role_payload
    for suite in suite_entries:
        if suite["suiteId"] != "DS-1000":
            continue
        role = suite["roles"][0]
        role["evidence"]["sha256"] = "sha256:" + sha256(ds1000_role_payload).hexdigest()
        break
    source_set: dict[str, Any] = {
        "checkoutProvenance": {
            "buildUrl": "https://jenkins.adapstory.com/job/serp-benchmark-sandbox-supply/1/",
            "commit": "b" * 40,
            "origin": "https://github.com/adapstory/Adapstory-GitOps.git",
            "pipelinePath": (
                "infra/ci/jenkins/pipelines/serp-benchmark-sandbox-supply.jenkinsfile"
            ),
            "schema": "GitOpsCheckoutProvenance/v1",
            "tree": "c" * 40,
        },
        "ds1000BaseImageProvenanceEvidence": base_image_evidence,
        "ds1000DatasetProvenanceEvidence": dataset_evidence,
        "ds1000WheelhouseResolutionEvidence": wheelhouse_resolution_evidence,
        "schema": "BenchmarkExecutionSubstrateSourceSet/v7",
        "suites": suite_entries,
        "supplyAttestationsEvidence": supply_evidence,
    }
    source_set_bytes = json.dumps(
        source_set, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    source_key = f"serp-evals/{operation_id}/source-set.json"
    source_version = "source-set-version"
    objects[(source_key, source_version)] = source_set_bytes
    source_evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/{source_key}",
        "sha256": "sha256:" + sha256(source_set_bytes).hexdigest(),
        "versionId": source_version,
    }
    verified_source_set: dict[str, Any] = {
        "checkoutProvenance": source_set["checkoutProvenance"],
        "operationId": operation_id,
        "retainUntil": source_evidence["retainUntil"],
        "sourceSet": source_set,
        "sourceSetEvidence": source_evidence,
        "ds1000BaseImageProvenance": base_image_provenance,
        "ds1000BaseImageProvenanceEvidence": base_image_evidence,
        "ds1000DatasetProvenance": dataset_provenance,
        "ds1000DatasetProvenanceEvidence": dataset_evidence,
        "ds1000WheelhouseManifest": wheelhouse_manifest,
        "ds1000WheelhouseResolution": wheelhouse_resolution,
        "ds1000WheelhouseResolutionEvidence": wheelhouse_resolution_evidence,
        "supplyAttestations": supply_attestations,
        "supplyAttestationsEvidence": supply_evidence,
    }

    class Body:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return self._payload

    class FakeS3Client:
        def head_object(self, *, Bucket: str, Key: str, VersionId: str) -> dict[str, object]:
            assert Bucket == "airflow-serp-evidence"
            assert (Key, VersionId) in objects
            return {
                "ObjectLockMode": "COMPLIANCE",
                "ObjectLockRetainUntilDate": datetime(2027, 7, 15, tzinfo=UTC),
                "VersionId": VersionId,
            }

        def get_object(self, *, Bucket: str, Key: str, VersionId: str) -> dict[str, object]:
            assert Bucket == "airflow-serp-evidence"
            return {"Body": Body(objects[(Key, VersionId)])}

    loaded = serp_eval_contracts_module._load_execution_substrate_source_set(
        verified_source_set,
        s3_client=FakeS3Client(),
    )

    assert loaded == {
        suite_id: {role: role_payloads[(suite_id, role)] for role in roles}
        for suite_id, roles in EXTERNAL_EXECUTION_SUBSTRATE_ROLES.items()
    }

    noncanonical_role_path = dict(verified_source_set)
    noncanonical_role_source_set = {
        **source_set,
        "suites": [
            {
                **suite,
                "roles": [
                    {
                        **role,
                        "evidence": {
                            **role["evidence"],
                            "s3Uri": (
                                "s3://airflow-serp-evidence/serp-evals/"
                                f"{operation_id}/roles/noncanonical.json"
                            ),
                        },
                    }
                    for role in suite["roles"]
                ],
            }
            if suite["suiteId"] == "ARES"
            else suite
            for suite in source_set["suites"]
        ],
    }
    noncanonical_role_path["sourceSet"] = noncanonical_role_source_set
    with pytest.raises(ValueError, match="canonical source-set role object"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            noncanonical_role_path,
            s3_client=FakeS3Client(),
        )

    legacy_supply_attestations = {
        **supply_attestations,
        "ds1000": {
            key: value
            for key, value in supply_attestations["ds1000"].items()
            if key not in {"wheelhouseResolutionEvidence", "wheelhouseResolutionSha256"}
        },
        "schema": "BenchmarkSubstrateSupplyAttestations/v2",
    }
    legacy_supply_bytes = json.dumps(
        legacy_supply_attestations,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    legacy_supply_key = f"serp-evals/{operation_id}/legacy-supply-attestations.json"
    legacy_supply_version = "legacy-supply-attestations-version"
    objects[(legacy_supply_key, legacy_supply_version)] = legacy_supply_bytes
    with pytest.raises(ValueError, match="shape is invalid"):
        serp_eval_contracts_module._load_benchmark_supply_attestations(
            {
                "objectLockMode": "COMPLIANCE",
                "retainUntil": "2027-07-15T00:00:00Z",
                "s3Uri": f"s3://airflow-serp-evidence/{legacy_supply_key}",
                "sha256": "sha256:" + sha256(legacy_supply_bytes).hexdigest(),
                "versionId": legacy_supply_version,
            },
            base_image_evidence=base_image_evidence,
            dataset_evidence=dataset_evidence,
            wheelhouse_manifest=wheelhouse_manifest,
            wheelhouse_resolution=wheelhouse_resolution,
            wheelhouse_resolution_evidence=wheelhouse_resolution_evidence,
            s3_client=FakeS3Client(),
        )

    mismatched_operation = dict(verified_source_set)
    mismatched_operation["operationId"] = "ci-benchmark-substrates-2"
    with pytest.raises(ValueError, match="operationId"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            mismatched_operation,
            s3_client=FakeS3Client(),
        )

    mismatched_retain_until = dict(verified_source_set)
    mismatched_retain_until["retainUntil"] = "2027-07-16T00:00:00Z"
    with pytest.raises(ValueError, match="retainUntil"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            mismatched_retain_until,
            s3_client=FakeS3Client(),
        )

    with pytest.raises(ValueError, match="verified"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            source_evidence,
            s3_client=FakeS3Client(),
        )

    legacy_source_set = dict(verified_source_set)
    legacy_source_set["sourceSet"] = {
        **source_set,
        "schema": "BenchmarkExecutionSubstrateSourceSet/v2",
    }
    with pytest.raises(ValueError, match="source set"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            legacy_source_set,
            s3_client=FakeS3Client(),
        )

    v5_source_set = dict(verified_source_set)
    v5_source_set["sourceSet"] = {
        **source_set,
        "schema": "BenchmarkExecutionSubstrateSourceSet/v5",
    }
    with pytest.raises(ValueError, match="source set"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            v5_source_set,
            s3_client=FakeS3Client(),
        )

    source_set_without_checkout_provenance = dict(verified_source_set)
    source_set_without_checkout_provenance["sourceSet"] = dict(source_set)
    source_set_without_checkout_provenance["sourceSet"].pop("checkoutProvenance")
    with pytest.raises(ValueError, match="source set"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            source_set_without_checkout_provenance,
            s3_client=FakeS3Client(),
        )

    malformed_checkout_provenance = dict(verified_source_set)
    malformed_checkout_provenance["sourceSet"] = {
        **source_set,
        "checkoutProvenance": {
            **source_set["checkoutProvenance"],
            "tree": "not-a-lowercase-git-tree",
        },
    }
    with pytest.raises(ValueError, match="checkout provenance"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            malformed_checkout_provenance,
            s3_client=FakeS3Client(),
        )

    extra_checkout_provenance = dict(verified_source_set)
    extra_checkout_provenance["sourceSet"] = {
        **source_set,
        "checkoutProvenance": {
            **source_set["checkoutProvenance"],
            "unexpected": "unsupported",
        },
    }
    with pytest.raises(ValueError, match="checkout provenance"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            extra_checkout_provenance,
            s3_client=FakeS3Client(),
        )

    mismatched_checkout_build = dict(verified_source_set)
    mismatched_checkout_build["sourceSet"] = {
        **source_set,
        "checkoutProvenance": {
            **source_set["checkoutProvenance"],
            "buildUrl": ("https://jenkins.adapstory.com/job/serp-benchmark-sandbox-supply/2/"),
        },
    }
    with pytest.raises(ValueError, match="checkout provenance build URL"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            mismatched_checkout_build,
            s3_client=FakeS3Client(),
        )

    missing_verified_checkout_provenance = dict(verified_source_set)
    missing_verified_checkout_provenance.pop("checkoutProvenance")
    with pytest.raises(ValueError, match="must define exactly"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            missing_verified_checkout_provenance,
            s3_client=FakeS3Client(),
        )

    mismatched_verified_checkout_provenance = {
        **verified_source_set,
        "checkoutProvenance": {
            **source_set["checkoutProvenance"],
            "commit": "d" * 40,
        },
    }
    with pytest.raises(ValueError, match="checkout provenance does not match sourceSet"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            mismatched_verified_checkout_provenance,
            s3_client=FakeS3Client(),
        )

    source_set_without_wheelhouse = dict(verified_source_set)
    source_set_without_wheelhouse["sourceSet"] = dict(source_set)
    source_set_without_wheelhouse["sourceSet"].pop("ds1000WheelhouseResolutionEvidence")
    with pytest.raises(ValueError, match="source set"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            source_set_without_wheelhouse,
            s3_client=FakeS3Client(),
        )

    missing_wheelhouse = dict(verified_source_set)
    missing_wheelhouse.pop("ds1000WheelhouseResolutionEvidence")
    with pytest.raises(ValueError, match="verified"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            missing_wheelhouse,
            s3_client=FakeS3Client(),
        )

    mismatched_wheelhouse_evidence = dict(verified_source_set)
    mismatched_wheelhouse_evidence["ds1000WheelhouseResolutionEvidence"] = {
        **wheelhouse_resolution_evidence,
        "versionId": "different-wheelhouse-resolution-version",
    }
    with pytest.raises(ValueError, match="wheelhouse resolution evidence"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            mismatched_wheelhouse_evidence,
            s3_client=FakeS3Client(),
        )

    mismatched_wheelhouse_manifest = dict(verified_source_set)
    mismatched_wheelhouse_manifest["ds1000WheelhouseManifest"] = {
        **wheelhouse_manifest,
        "artifacts": [
            {
                **artifact,
                **({"sha256": "sha256:" + "f" * 64} if index == 0 else {}),
            }
            for index, artifact in enumerate(wheelhouse_manifest["artifacts"])
        ],
    }
    with pytest.raises(ValueError, match="canonical manifest identity"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            mismatched_wheelhouse_manifest,
            s3_client=FakeS3Client(),
        )

    class SupplyRetentionMismatchS3Client(FakeS3Client):
        def head_object(self, *, Bucket: str, Key: str, VersionId: str) -> dict[str, object]:
            result = super().head_object(Bucket=Bucket, Key=Key, VersionId=VersionId)
            if Key == supply_key:
                result["ObjectLockRetainUntilDate"] = datetime(2027, 7, 16, tzinfo=UTC)
            return result

    with pytest.raises(ValueError, match="supply attestations retainUntil is mismatched"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            verified_source_set,
            s3_client=SupplyRetentionMismatchS3Client(),
        )

    mismatched_inventory = deepcopy(ds1000_inventory)
    mismatched_inventory["imageDigest"] = "sha256:" + "f" * 64
    mismatched_inventory["imageReference"] = (
        "harbor.adapstory.com/benchmark-sandboxes/ds1000@sha256:" + "f" * 64
    )
    mismatched_inventory_payload = json.dumps(
        mismatched_inventory, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    mismatched_role_version = "ds1000-mismatched-inventory-version"
    objects[(ds1000_role_key, mismatched_role_version)] = mismatched_inventory_payload
    mismatched_source_set = deepcopy(source_set)
    mismatched_role = next(
        suite["roles"][0]
        for suite in mismatched_source_set["suites"]
        if suite["suiteId"] == "DS-1000"
    )
    mismatched_role["evidence"] = {
        **mismatched_role["evidence"],
        "sha256": "sha256:" + sha256(mismatched_inventory_payload).hexdigest(),
        "versionId": mismatched_role_version,
    }
    mismatched_source_bytes = json.dumps(
        mismatched_source_set, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    ).encode()
    mismatched_source_version = "source-set-mismatched-inventory-version"
    objects[(source_key, mismatched_source_version)] = mismatched_source_bytes
    mismatched_source_evidence = {
        **source_evidence,
        "sha256": "sha256:" + sha256(mismatched_source_bytes).hexdigest(),
        "versionId": mismatched_source_version,
    }
    mismatched_inventory_source_set = deepcopy(verified_source_set)
    mismatched_inventory_source_set["sourceSet"] = mismatched_source_set
    mismatched_inventory_source_set["sourceSetEvidence"] = mismatched_source_evidence
    with pytest.raises(ValueError, match="inventory image differs from supply attestations"):
        serp_eval_contracts_module._load_execution_substrate_source_set(
            mismatched_inventory_source_set,
            s3_client=FakeS3Client(),
        )


def test_nightly_regression_plan_rejects_caller_supplied_suite_inputs() -> None:
    conf = _scheduled_d6_conf()
    conf["benchmark_suite_inputs"] = [{"synthetic": "must-not-reach-d6"}]

    with pytest.raises(ValueError, match="legacy D6 scorer fields are unsupported"):
        build_nightly_regression_plan(conf)


def test_load_materialized_catalog_binds_receipt_and_catalog_s3_versions() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    catalog_path = plan.payload["artifact_paths"]["benchmark_catalog"]
    receipt_path = plan.payload["artifact_paths"]["benchmark_catalog_receipt"]
    corpus_role_by_suite = {
        "APIBench": "api-documentation",
        "ARES": "context-corpus",
        "BEIR": "beir-corpus",
        "CodeRAG-Bench": "documentation-corpus",
        "RAGBench": "source-context",
        "RepoQA": "repository-code",
        "SWE-bench Verified": "base-commit-repository",
        "cwd-benchmark-data": "reference-graph",
        "rusBEIR": "beir-corpus",
    }
    catalog_payload = {
        "catalog_status": "ready",
        "contract_version": "serp-benchmark-catalog/v5",
        "observed_at": "2026-07-13T00:00:00Z",
        "suites": [
            {
                "corpus_snapshots": {
                    "corpus": {
                        "corpus_role": corpus_role_by_suite[entry.suite_id],
                        "immutable_artifact": {
                            "artifactPath": (
                                f"s3://airflow-serp-evidence/catalog/{entry.suite_id}/corpus"
                            ),
                            "artifactSha256": "e" * 64,
                            "artifactVersionId": f"{entry.suite_id}-corpus-v1",
                            "objectLockMode": "COMPLIANCE",
                        },
                        "sha256": "sha256:" + "e" * 64,
                        "url": f"derived://native-corpus/{entry.suite_id}/corpus",
                    }
                },
                "dataset_id": entry.dataset_id,
                "dataset_license_id": entry.dataset_license_id,
                "dataset_revision": entry.dataset_revision,
                "dataset_snapshots": {},
                "distribution_rule": entry.distribution_rule,
                "execution_status": "ready",
                "execution_substrate_artifacts": {
                    role: {
                        "artifactPath": (
                            "s3://airflow-serp-evidence/catalog/"
                            f"{entry.suite_id}/execution-substrate-{role}"
                        ),
                        "artifactSha256": sha256(f"{entry.suite_id}:{role}".encode()).hexdigest(),
                        "artifactVersionId": f"{entry.suite_id}-{role}-v1",
                        "objectLockMode": "COMPLIANCE",
                    }
                    for role in MANDATORY_EXECUTION_SUBSTRATE_ROLES[entry.suite_id]
                },
                "legal_boundary": entry.legal_boundary,
                "license_snapshot": {},
                "native_adapter_manifest": {
                    "corpusEvidence": [
                        {
                            "artifactPath": (
                                f"s3://airflow-serp-evidence/catalog/{entry.suite_id}/corpus"
                            ),
                            "artifactSha256": "e" * 64,
                            "artifactVersionId": f"{entry.suite_id}-corpus-v1",
                            "corpusRole": corpus_role_by_suite[entry.suite_id],
                            "objectLockMode": "COMPLIANCE",
                            "sourceId": "corpus",
                        }
                    ],
                    "corpusManifest": {
                        "schema": "NativeBenchmarkCorpusManifest/v1",
                        "sources": [
                            {
                                "corpusRole": corpus_role_by_suite[entry.suite_id],
                                "documentCount": 1,
                                "payloadSha256": "sha256:" + "e" * 64,
                                "sourceId": "corpus",
                            }
                        ],
                        "status": "materialized",
                        "suiteId": entry.suite_id,
                    },
                },
                "official_harness": {
                    "entrypoint": entry.harness_entrypoint,
                    "license_id": entry.harness_license_id,
                    "license_snapshot": {"sha256": "sha256:" + "b" * 64},
                    "license_status": entry.harness_license_status,
                    "revision": entry.harness_revision,
                    "source_archive_snapshot": {"sha256": "sha256:" + "c" * 64},
                },
                "rights_status": entry.rights_status,
                "source_snapshot": {},
                "suite_id": entry.suite_id,
            }
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        ],
    }
    catalog_bytes = serp_eval_contracts_module._canonical_json(catalog_payload).encode("utf-8")
    receipt_payload = {
        "catalogSnapshot": {
            "artifactPath": catalog_path,
            "artifactSha256": sha256(catalog_bytes).hexdigest(),
            "artifactVersionId": "catalog-v1",
            "blockingSuiteIds": [],
            "catalogStatus": "ready",
            "objectLockMode": "COMPLIANCE",
            "officialHarnessLineage": [
                {
                    "entrypoint": entry.harness_entrypoint,
                    "harnessLicenseId": entry.harness_license_id,
                    "harnessLicenseSha256": "sha256:" + "b" * 64,
                    "harnessLicenseStatus": entry.harness_license_status,
                    "harnessSourceArchiveSha256": "sha256:" + "c" * 64,
                    "revision": entry.harness_revision,
                    "suiteId": entry.suite_id,
                }
                for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
            ],
            "suiteSummary": [
                {
                    "distributionRule": entry.distribution_rule,
                    "executionStatus": "ready",
                    "rightsStatus": entry.rights_status,
                    "suiteId": entry.suite_id,
                }
                for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
            ],
        },
        "contractVersion": "serp-benchmark-catalog-materializer/v5",
        "dagId": plan.payload["dag_id"],
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
                return {
                    "Body": Body(
                        serp_eval_contracts_module._canonical_json(receipt_payload).encode("utf-8")
                    )
                }
            assert VersionId == "catalog-v1"
            assert Key == catalog_path.removeprefix("s3://airflow-serp-evidence/")
            return {"Body": Body(catalog_bytes)}

    snapshot = load_materialized_benchmark_catalog_snapshot(
        plan.to_canonical_json(),
        s3_client=FakeS3Client(),
    )

    assert snapshot["artifactVersionId"] == "catalog-v1"
    assert snapshot["catalogReceiptVersionId"] == "receipt-v1"
    assert snapshot["blockingSuiteIds"] == []
    assert snapshot["blockingReasonBySuite"] == {}

    receipt_payload["catalogSnapshot"]["suiteSummary"][0]["distributionRule"] = "internal-only"
    with pytest.raises(ValueError, match="suite summary does not match catalog object"):
        load_materialized_benchmark_catalog_snapshot(
            plan.to_canonical_json(),
            s3_client=FakeS3Client(),
        )


def test_write_airflow_plan_artifact_writes_s3_artifact_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _scheduled_d6_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-evidence/serp-evals"
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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )

    plan_json = write_airflow_plan_artifact(plan)

    assert json.loads(plan_json) == plan.payload
    assert put_calls == [
        (
            bucket,
            key,
            plan.to_canonical_json(),
            "application/json",
        )
    ]


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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )
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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )
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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )
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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )
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
    assert plan.payload["evaluation_release_promotion_evidence"] == _d19_worm_evidence(
        "model-releases/d17-promotion", "c"
    )
    assert "evaluation_binding_id" not in plan.payload
    assert "evaluation_binding_evidence" not in plan.payload
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "benchmark_catalog": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-catalog.json"
        ),
        "benchmark_catalog_receipt": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-catalog-materialization-receipt.json"
        ),
        "benchmark_catalog_pack_activation": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-catalog-pack-activation.json"
        ),
        "benchmark_pack_build_result": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-pack-build-result.json"
        ),
        "benchmark_pack_lifecycle_result": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-pack-lifecycle-result.json"
        ),
        "official_serp_mcp_measurement": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/official-serp-mcp-measurement.json"
        ),
        "paired_eval_receipt": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-eval-receipt.json"
        ),
        "paired_evaluation_score_cells": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-evaluation-observed-normalized-score-cells.json"
        ),
        "paired_evaluation_verification_evidence": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-evaluation-verification-evidence.json"
        ),
        "paired_evaluation_assembly_plan": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-evaluation-assembly-plan.json"
        ),
        "paired_execution_manifest": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-execution-manifest.json"
        ),
        "paired_eval_request": (
            "s3://airflow-serp-evidence/serp-evals/"
            f"{plan.payload['operation_id']}/paired-eval-request.json"
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "verify_runtime_terminal_activation_admission",
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
        *[
            task_id
            for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
            for repetition in range(1, 6)
            for side in ("baseline", "candidate")
            for task_id in (
                (
                    "prepare_code_sandbox_"
                    f"{suite_id.casefold().replace(' ', '_').replace('-', '_')}_"
                    f"{side}_{repetition}",
                    "fanout_code_sandbox_"
                    f"{suite_id.casefold().replace(' ', '_').replace('-', '_')}_"
                    f"{side}_{repetition}",
                    "execute_code_sandbox_"
                    f"{suite_id.casefold().replace(' ', '_').replace('-', '_')}_"
                    f"{side}_{repetition}",
                    "result_set_plan_code_sandbox_"
                    f"{suite_id.casefold().replace(' ', '_').replace('-', '_')}_"
                    f"{side}_{repetition}",
                    f"seal_code_sandbox_{suite_id.casefold().replace(' ', '_').replace('-', '_')}_"
                    f"{side}_{repetition}",
                )
                if suite_id in {"CodeRAG-Bench", "SWE-bench Verified"}
                else (
                    "run_official_harness_"
                    f"{suite_id.casefold().replace(' ', '_').replace('-', '_')}_"
                    f"{side}_{repetition}",
                )
            )
        ],
        "write_paired_evaluation_assembly_plan",
        "assemble_paired_execution_manifest",
        "run_paired_benchmark_evaluation",
        "persist_paired_evaluation_verification_evidence",
        "write_official_serp_mcp_measurement",
        "publish_official_serp_mcp_measurement",
    ]


def test_build_benchmark_improvement_wave_plan_rejects_caller_supplied_candidate_scores() -> None:
    conf = _improvement_wave_conf()
    conf["candidate_evaluation"] = {"candidateScore": "0.8"}

    with pytest.raises(ValueError, match="inline D19 field is forbidden"):
        build_benchmark_improvement_wave_plan(conf)


def test_d19_persists_observed_normalized_score_cells_and_identity_bound_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    evaluator_result, objects = _d19_evaluator_result(plan)
    s3_client = _D19VerificationS3(objects)
    airflow_run = _d19_airflow_run()

    persisted = serp_eval_contracts_module.write_paired_evaluation_verification_evidence(
        plan.to_canonical_json(),
        evaluator_result,
        airflow_run,
        s3_client=s3_client,
    )

    verification_evidence = persisted["pairedEvaluationVerificationEvidence"]
    assert set(verification_evidence) == {
        "objectLockMode",
        "retainUntil",
        "s3Uri",
        "sha256",
        "versionId",
    }
    assert (
        verification_evidence["s3Uri"]
        == plan.payload["artifact_paths"]["paired_evaluation_verification_evidence"]
    )
    assert verification_evidence["versionId"] == "verification-version-001"
    assert persisted["requestId"] == plan.payload["operation_id"]
    assert persisted["receiptStatus"] == "accepted"
    score_cells_evidence = persisted["observedNormalizedScoreCellsEvidence"]
    assert set(score_cells_evidence) == {
        "objectLockMode",
        "retainUntil",
        "s3Uri",
        "sha256",
        "versionId",
    }
    assert (
        score_cells_evidence["s3Uri"]
        == plan.payload["artifact_paths"]["paired_evaluation_score_cells"]
    )

    verification_payload = json.loads(
        s3_client.objects[(verification_evidence["s3Uri"], verification_evidence["versionId"])]
    )
    expected_pointer = {
        **evaluator_result,
        "receiptEvidence": _d19_receipt_subject(evaluator_result),
    }
    assert verification_payload == {
        "airflowRun": airflow_run,
        "operationId": plan.payload["operation_id"],
        "receiptPointer": expected_pointer,
        "requestId": plan.payload["operation_id"],
        "schema": "PairedEvaluationVerificationEvidence/v2",
        "observedNormalizedScoreCellsEvidence": score_cells_evidence,
    }
    assert verification_payload["receiptPointer"]["receiptEvidence"] == (
        _d19_receipt_subject(evaluator_result)
    )
    score_cells_payload = json.loads(
        s3_client.objects[(score_cells_evidence["s3Uri"], score_cells_evidence["versionId"])]
    )
    receipt_payload = json.loads(
        s3_client.objects[
            (
                evaluator_result["receiptEvidence"]["artifactPath"],
                evaluator_result["receiptEvidence"]["artifactVersionId"],
            )
        ]
    )
    assert score_cells_payload["schema"] == "D19ObservedNormalizedScoreCells/v2"
    assert score_cells_payload["operationId"] == plan.payload["operation_id"]
    assert score_cells_payload["receiptEvidence"] == _d19_receipt_subject(evaluator_result)
    assert (
        score_cells_payload["receiptAttestationEvidence"]
        == evaluator_result["receiptAttestationEvidence"]
    )
    assert (
        score_cells_payload["benchmarkScore"]
        == receipt_payload["pairedEvaluation"]["benchmarkScore"]
    )
    assert score_cells_payload["benchmarkScore"]["allNineCandidateNormalizedLcb95"] == 0.925
    assert [cell["suiteId"] for cell in score_cells_payload["cells"]] == list(
        MANDATORY_SERP_BENCHMARK_SUITES
    )
    assert score_cells_payload["cells"][0]["baseline"] == {
        "meanScore": 0.81,
        "normalizedMean": 0.9,
    }
    assert score_cells_payload["cells"][0]["candidate"] == {
        "meanScore": 0.855,
        "normalizedLcb95": 0.925,
        "normalizedMean": 0.95,
    }


def test_d19_writes_one_deterministic_official_serp_mcp_measurement_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    evaluator_result, objects = _d19_evaluator_result(plan)
    s3_client = _D19VerificationS3(objects)
    verification = serp_eval_contracts_module.write_paired_evaluation_verification_evidence(
        plan.to_canonical_json(),
        evaluator_result,
        _d19_airflow_run(),
        s3_client=s3_client,
    )
    s3_client.put_object_calls.clear()

    first = serp_eval_contracts_module.write_official_serp_mcp_measurement(
        plan.to_canonical_json(),
        verification,
        _d19_airflow_run(),
        s3_client=s3_client,
    )
    second = serp_eval_contracts_module.write_official_serp_mcp_measurement(
        plan.to_canonical_json(),
        verification,
        _d19_airflow_run(),
        s3_client=s3_client,
    )

    assert second == first
    assert first["operationId"] == plan.payload["operation_id"]
    assert first["measurementStatus"] == "measured"
    measurement_evidence = first["measurementEvidence"]
    assert (
        measurement_evidence["s3Uri"]
        == plan.payload["artifact_paths"]["official_serp_mcp_measurement"]
    )
    assert s3_client.put_object_calls == [measurement_evidence["s3Uri"]]
    measurement = json.loads(
        s3_client.objects[(measurement_evidence["s3Uri"], measurement_evidence["versionId"])]
    )
    assert measurement == {
        "airflowRun": _d19_airflow_run(),
        "allNineBaselineRetentionLcb95ToMean": pytest.approx(0.925 / 0.9),
        "allNineCandidateNormalizedLcb95": 0.925,
        "cellCount": 9,
        "cells": measurement["cells"],
        "generatedAt": plan.payload["generated_at"],
        "measurementStatus": "measured",
        "observedNormalizedScoreCellsEvidence": verification[
            "observedNormalizedScoreCellsEvidence"
        ],
        "operationId": plan.payload["operation_id"],
        "pairedEvaluationReceiptAttestationEvidence": evaluator_result[
            "receiptAttestationEvidence"
        ],
        "pairedEvaluationReceiptEvidence": _d19_receipt_subject(evaluator_result),
        "pairedEvaluationVerificationEvidence": verification[
            "pairedEvaluationVerificationEvidence"
        ],
        "rejectionReasons": [],
        "schema": "OfficialSerpMcpMeasurement/v1",
        "signedReceiptStatus": "accepted",
        "tenantId": TENANT_ID,
        "threshold": 0.9,
        "thresholdOutcome": "met",
    }
    assert [cell["suiteId"] for cell in measurement["cells"]] == list(
        MANDATORY_SERP_BENCHMARK_SUITES
    )


def test_d19_writes_rejected_official_measurement_with_its_real_score(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    evaluator_result, objects = _d19_rejected_evaluator_result(plan)
    s3_client = _D19VerificationS3(objects)
    verification = serp_eval_contracts_module.write_paired_evaluation_verification_evidence(
        plan.to_canonical_json(),
        evaluator_result,
        _d19_airflow_run(),
        s3_client=s3_client,
    )

    result = serp_eval_contracts_module.write_official_serp_mcp_measurement(
        plan.to_canonical_json(),
        verification,
        _d19_airflow_run(),
        s3_client=s3_client,
    )

    measurement_evidence = result["measurementEvidence"]
    measurement = json.loads(
        s3_client.objects[(measurement_evidence["s3Uri"], measurement_evidence["versionId"])]
    )
    assert result["measurementStatus"] == "rejected"
    assert measurement["measurementStatus"] == "rejected"
    assert measurement["signedReceiptStatus"] == "rejected"
    assert measurement["allNineCandidateNormalizedLcb95"] == 0.8
    assert measurement["allNineBaselineRetentionLcb95ToMean"] == pytest.approx(0.8 / 0.9)
    assert measurement["thresholdOutcome"] == "not_met"
    assert measurement["rejectionReasons"] == [
        "candidate-normalized-mean-not-met:APIBench:answer-quality:observed-metric-1",
        "candidate-normalized-lcb95-not-met:APIBench:answer-quality:observed-metric-1",
        "baseline-retention-lcb95-to-mean-not-met:APIBench:answer-quality:observed-metric-1",
    ]


def test_d19_official_measurement_keeps_baseline_retention_outcome_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    evaluator_result, objects = _d19_rejected_evaluator_result(
        plan,
        candidate_normalized_lcb95=0.89,
    )
    s3_client = _D19VerificationS3(objects)
    verification = serp_eval_contracts_module.write_paired_evaluation_verification_evidence(
        plan.to_canonical_json(),
        evaluator_result,
        _d19_airflow_run(),
        s3_client=s3_client,
    )

    result = serp_eval_contracts_module.write_official_serp_mcp_measurement(
        plan.to_canonical_json(),
        verification,
        _d19_airflow_run(),
        s3_client=s3_client,
    )
    evidence = result["measurementEvidence"]
    measurement = json.loads(s3_client.objects[(evidence["s3Uri"], evidence["versionId"])])

    assert measurement["measurementStatus"] == "rejected"
    assert measurement["allNineCandidateNormalizedLcb95"] == 0.89
    assert measurement["allNineBaselineRetentionLcb95ToMean"] == pytest.approx(0.89 / 0.9)
    assert measurement["thresholdOutcome"] == "met"


@pytest.mark.parametrize(
    ("tamper", "match"),
    (
        # These mutations model a byte-level alteration of a persisted WORM
        # projection.  The official-measurement writer must reject at the
        # immutable evidence boundary, before it could ever consume a changed
        # score or pointer.  The score-cell validator has dedicated invariant
        # tests below for well-formed but semantically invalid producer output.
        ("null-retention", "SHA-256"),
        ("non-finite-score", "SHA-256"),
        ("eight-cells", "SHA-256"),
        ("receipt-pointer-mismatch", "SHA-256"),
        ("tampered-score-cells", "SHA-256"),
        ("invalid-receipt-status", "receipt status is unsupported"),
    ),
)
def test_d19_official_measurement_fails_closed_for_noncanonical_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
    match: str,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    evaluator_result, objects = _d19_evaluator_result(plan)
    s3_client = _D19VerificationS3(objects)
    verification = serp_eval_contracts_module.write_paired_evaluation_verification_evidence(
        plan.to_canonical_json(),
        evaluator_result,
        _d19_airflow_run(),
        s3_client=s3_client,
    )
    score_evidence = verification["observedNormalizedScoreCellsEvidence"]
    score_key = (score_evidence["s3Uri"], score_evidence["versionId"])

    if tamper == "invalid-receipt-status":
        verification["receiptStatus"] = "unknown"
    else:
        score_payload = json.loads(s3_client.objects[score_key])
        if tamper == "null-retention":
            score_payload["benchmarkScore"]["allNineBaselineRetentionLcb95ToMean"] = None
        elif tamper == "non-finite-score":
            score_payload["benchmarkScore"]["allNineCandidateNormalizedLcb95"] = float("nan")
        elif tamper == "eight-cells":
            score_payload["cells"].pop()
        elif tamper == "receipt-pointer-mismatch":
            score_payload["receiptEvidence"] = _d19_worm_evidence("receipts/foreign", "f")
        elif tamper == "tampered-score-cells":
            score_payload["summary"]["cellCount"] = 8
        else:
            raise AssertionError(f"unsupported tamper case: {tamper}")
        if tamper == "non-finite-score":
            s3_client.objects[score_key] = json.dumps(
                score_payload,
                allow_nan=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        else:
            s3_client.objects[score_key] = serp_eval_contracts_module._canonical_json(
                score_payload
            ).encode("utf-8")

    with pytest.raises(ValueError, match=match):
        serp_eval_contracts_module.write_official_serp_mcp_measurement(
            plan.to_canonical_json(),
            verification,
            _d19_airflow_run(),
            s3_client=s3_client,
        )


def test_d19_official_measurement_publish_is_idempotent_and_sends_only_the_pointer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    measurement_evidence = {
        **_d19_worm_evidence("official-measurement", "e"),
        "s3Uri": plan.payload["artifact_paths"]["official_serp_mcp_measurement"],
    }
    artifact = {
        "measurementEvidence": measurement_evidence,
        "measurementStatus": "measured",
        "operationId": plan.payload["operation_id"],
    }
    calls: list[dict[str, Any]] = []

    def request(
        url: str,
        *,
        method: str,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        error_label: str,
        allow_conflict: bool = False,
    ) -> dict[str, Any] | None:
        calls.append(
            {
                "allowConflict": allow_conflict,
                "body": dict(body or {}),
                "errorLabel": error_label,
                "headers": dict(headers),
                "method": method,
                "url": url,
            }
        )
        return {"status": "accepted"}

    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://bc21.test.svc.cluster.local",
    )
    monkeypatch.setattr(serp_eval_contracts_module, "_bc21_json_request", request)

    first = serp_eval_contracts_module.publish_official_serp_mcp_measurement(
        plan.to_canonical_json(), artifact
    )
    second = serp_eval_contracts_module.publish_official_serp_mcp_measurement(
        plan.to_canonical_json(), artifact
    )

    assert second == first == {"status": "accepted"}
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert calls[0]["url"] == (
        "http://bc21.test.svc.cluster.local/api/bc-21/serp/v1/governance/official-measurements"
    )
    assert calls[0]["method"] == "POST"
    assert calls[0]["errorLabel"] == "official SERP/MCP measurement publication"
    assert calls[0]["allowConflict"] is True
    assert calls[0]["body"] == {
        "actorId": "airflow-serp-eval-runner",
        "measurementEvidence": measurement_evidence,
    }
    assert calls[0]["headers"]["X-Adapstory-Actor-Id"] == "airflow-serp-eval-runner"
    assert calls[0]["headers"]["X-Adapstory-Tenant-Id"] == TENANT_ID
    assert (
        calls[0]["headers"]["X-Fingerprint"]
        == "sha256:"
        + sha256(
            serp_eval_contracts_module._canonical_json(calls[0]["body"]).encode("utf-8")
        ).hexdigest()
    )
    assert calls[0]["headers"]["X-Idempotency-Key"]


@pytest.mark.parametrize("conflict_response", [None, {"status": "conflict"}])
def test_d19_official_measurement_publish_fails_closed_for_a_conflict(
    monkeypatch: pytest.MonkeyPatch,
    conflict_response: dict[str, str] | None,
) -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    artifact = {
        "measurementEvidence": {
            **_d19_worm_evidence("official-measurement-conflict", "f"),
            "s3Uri": plan.payload["artifact_paths"]["official_serp_mcp_measurement"],
        },
        "measurementStatus": "measured",
        "operationId": plan.payload["operation_id"],
    }

    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://bc21.test.svc.cluster.local",
    )
    monkeypatch.setattr(
        serp_eval_contracts_module,
        "_bc21_json_request",
        lambda _url, **_: conflict_response,
    )

    with pytest.raises(ValueError, match="conflict"):
        serp_eval_contracts_module.publish_official_serp_mcp_measurement(
            plan.to_canonical_json(), artifact
        )


def test_d19_materializes_observed_score_cells_for_a_rejected_evaluation() -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-rejected"
    paired = _d19_observed_paired_evaluation(operation_id)
    first_cell = paired["metricCells"][0]
    first_cell["candidateNormalizedLcb95"] = 0.8
    first_cell["candidateNormalizedMean"] = 0.8
    first_cell["meanCandidateScore"] = 0.72
    paired["benchmarkScore"] = _d19_paired_benchmark_score(paired["metricCells"])
    paired.update(
        {
            "rejectionReasons": [
                "candidate-normalized-mean-not-met:APIBench:answer-quality:observed-metric-1",
                "candidate-normalized-lcb95-not-met:APIBench:answer-quality:observed-metric-1",
                "baseline-retention-lcb95-to-mean-not-met:APIBench:answer-quality:observed-metric-1",
            ],
            "status": "rejected",
        }
    )
    receipt_evidence = _d19_worm_evidence("receipts/rejected", "7")
    attestation_evidence = _d19_worm_evidence("receipts/rejected.attestation", "8")

    payload = serp_eval_contracts_module._observed_normalized_score_cells_payload(
        {"pairedEvaluation": paired},
        operation_id=operation_id,
        receipt_evidence=receipt_evidence,
        receipt_attestation_evidence=attestation_evidence,
        receipt_status="rejected",
    )

    assert payload["receiptStatus"] == "rejected"
    assert payload["schema"] == "D19ObservedNormalizedScoreCells/v2"
    assert payload["summary"]["status"] == "rejected"
    assert payload["summary"]["rejectionReasons"] == [
        "candidate-normalized-mean-not-met:APIBench:answer-quality:observed-metric-1",
        "candidate-normalized-lcb95-not-met:APIBench:answer-quality:observed-metric-1",
        "baseline-retention-lcb95-to-mean-not-met:APIBench:answer-quality:observed-metric-1",
    ]
    assert len(payload["cells"]) == len(MANDATORY_SERP_BENCHMARK_SUITES)


def test_d19_materializes_insufficient_baseline_as_a_rejected_score() -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-insufficient-baseline"
    paired = _d19_observed_paired_evaluation(operation_id)
    first_cell = paired["metricCells"][0]
    first_cell["baselineNormalizedMean"] = 0.0
    first_cell["meanBaselineScore"] = 0.0
    paired["benchmarkScore"] = _d19_paired_benchmark_score(paired["metricCells"])
    paired.update(
        {
            "rejectionReasons": [
                "insufficient-baseline-normalized-mean:APIBench:answer-quality:observed-metric-1"
            ],
            "status": "rejected",
        }
    )

    payload = serp_eval_contracts_module._observed_normalized_score_cells_payload(
        {"pairedEvaluation": paired},
        operation_id=operation_id,
        receipt_evidence=_d19_worm_evidence("receipts/insufficient-baseline", "7"),
        receipt_attestation_evidence=_d19_worm_evidence(
            "receipts/insufficient-baseline.attestation", "8"
        ),
        receipt_status="rejected",
    )

    benchmark_score = payload["benchmarkScore"]
    assert benchmark_score["allNineCandidateNormalizedLcb95"] == 0.925
    assert benchmark_score["allNineBaselineRetentionLcb95ToMean"] is None
    assert benchmark_score["worstBaselineRetentionCell"] is None
    aggregates = benchmark_score["supportingAggregates"]
    assert aggregates["baselineRetentionMeasuredCellCount"] == 8
    assert aggregates["insufficientBaselineCellCount"] == 1
    assert aggregates["meanBaselineNormalizedMean"] == pytest.approx(0.8)
    assert aggregates["meanCandidateNormalizedLcb95"] == pytest.approx(0.925)
    assert aggregates["meanCandidateNormalizedMean"] == pytest.approx(0.95)
    assert aggregates["meanReferenceNormalizedPairedDeltaLcb95"] == pytest.approx(0.025)
    assert aggregates["minimumReferenceNormalizedPairedDeltaLcb95"] == pytest.approx(0.025)


def test_d19_rejects_accepted_score_with_insufficient_baseline() -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-insufficient-baseline-accepted"
    paired = _d19_observed_paired_evaluation(operation_id)
    first_cell = paired["metricCells"][0]
    first_cell["baselineNormalizedMean"] = 0.0
    first_cell["meanBaselineScore"] = 0.0
    paired["benchmarkScore"] = _d19_paired_benchmark_score(paired["metricCells"])

    with pytest.raises(ValueError, match="does not satisfy quality gates"):
        serp_eval_contracts_module._observed_normalized_score_cells_payload(
            {"pairedEvaluation": paired},
            operation_id=operation_id,
            receipt_evidence=_d19_worm_evidence("receipts/insufficient-baseline-accepted", "7"),
            receipt_attestation_evidence=_d19_worm_evidence(
                "receipts/insufficient-baseline-accepted.attestation", "8"
            ),
            receipt_status="accepted",
        )


def test_d19_rejects_accepted_score_below_candidate_floor_despite_healthy_retention() -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-candidate-floor"
    paired = _d19_observed_paired_evaluation(operation_id)
    first_cell = paired["metricCells"][0]
    first_cell["candidateNormalizedLcb95"] = 0.89
    paired["benchmarkScore"] = _d19_paired_benchmark_score(paired["metricCells"])

    with pytest.raises(ValueError, match="does not satisfy quality gates"):
        serp_eval_contracts_module._observed_normalized_score_cells_payload(
            {"pairedEvaluation": paired},
            operation_id=operation_id,
            receipt_evidence=_d19_worm_evidence("receipts/candidate-floor-accepted", "7"),
            receipt_attestation_evidence=_d19_worm_evidence(
                "receipts/candidate-floor-accepted.attestation", "8"
            ),
            receipt_status="accepted",
        )

    paired.update(
        {
            "rejectionReasons": [
                "candidate-normalized-lcb95-not-met:APIBench:answer-quality:observed-metric-1"
            ],
            "status": "rejected",
        }
    )
    payload = serp_eval_contracts_module._observed_normalized_score_cells_payload(
        {"pairedEvaluation": paired},
        operation_id=operation_id,
        receipt_evidence=_d19_worm_evidence("receipts/candidate-floor-rejected", "7"),
        receipt_attestation_evidence=_d19_worm_evidence(
            "receipts/candidate-floor-rejected.attestation", "8"
        ),
        receipt_status="rejected",
    )

    assert payload["benchmarkScore"]["allNineCandidateNormalizedLcb95"] == pytest.approx(0.89)
    assert payload["benchmarkScore"]["allNineBaselineRetentionLcb95ToMean"] == pytest.approx(
        0.89 / 0.9
    )
    assert payload["summary"]["rejectionReasons"] == paired["rejectionReasons"]


def test_d19_score_cells_v2_rejects_legacy_v1_artifact() -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-legacy-score-cells"
    receipt_evidence = _d19_worm_evidence("receipts/legacy-score-cells", "7")
    attestation_evidence = _d19_worm_evidence("receipts/legacy-score-cells.attestation", "8")
    payload = serp_eval_contracts_module._observed_normalized_score_cells_payload(
        {"pairedEvaluation": _d19_observed_paired_evaluation(operation_id)},
        operation_id=operation_id,
        receipt_evidence=receipt_evidence,
        receipt_attestation_evidence=attestation_evidence,
        receipt_status="accepted",
    )
    payload["schema"] = "D19ObservedNormalizedScoreCells/v1"

    with pytest.raises(
        ValueError,
        match="observed normalized score-cell evidence schema is unsupported",
    ):
        serp_eval_contracts_module._normalized_observed_normalized_score_cells_evidence(
            payload,
            expected_operation_id=operation_id,
            expected_receipt_evidence=receipt_evidence,
            expected_receipt_attestation_evidence=attestation_evidence,
            expected_receipt_status="accepted",
        )


def test_d19_score_cells_v2_requires_receipt_benchmark_score_without_fallback() -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-score-required"
    paired = _d19_observed_paired_evaluation(operation_id)
    paired.pop("benchmarkScore")

    with pytest.raises(ValueError, match="benchmarkScore must be an object"):
        serp_eval_contracts_module._observed_normalized_score_cells_payload(
            {"pairedEvaluation": paired},
            operation_id=operation_id,
            receipt_evidence=_d19_worm_evidence("receipts/score-required", "7"),
            receipt_attestation_evidence=_d19_worm_evidence(
                "receipts/score-required.attestation", "8"
            ),
            receipt_status="accepted",
        )


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    (
        ("schema", "PairedBenchmarkScore/v1", "benchmark score schema is unsupported"),
        (
            "canonicalScoreFormula",
            "mean(candidateNormalizedLcb95 across canonical metric cells)",
            "benchmark score canonical formula is unsupported",
        ),
        (
            "allNineCandidateNormalizedLcb95",
            0.924,
            "benchmark score canonical scalar does not match the limiting cell",
        ),
    ),
)
def test_d19_score_cells_v2_rejects_noncanonical_receipt_benchmark_score(
    field_name: str,
    value: object,
    message: str,
) -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-score-invariant"
    paired = _d19_observed_paired_evaluation(operation_id)
    paired["benchmarkScore"][field_name] = value

    with pytest.raises(ValueError, match=message):
        serp_eval_contracts_module._observed_normalized_score_cells_payload(
            {"pairedEvaluation": paired},
            operation_id=operation_id,
            receipt_evidence=_d19_worm_evidence("receipts/score-invariant", "7"),
            receipt_attestation_evidence=_d19_worm_evidence(
                "receipts/score-invariant.attestation", "8"
            ),
            receipt_status="accepted",
        )


def test_d19_score_cells_v2_rejects_unknown_receipt_benchmark_score_field() -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-score-fields"
    paired = _d19_observed_paired_evaluation(operation_id)
    paired["benchmarkScore"]["notCanonical"] = 1

    with pytest.raises(ValueError, match="benchmark score fields are unsupported"):
        serp_eval_contracts_module._observed_normalized_score_cells_payload(
            {"pairedEvaluation": paired},
            operation_id=operation_id,
            receipt_evidence=_d19_worm_evidence("receipts/score-fields", "7"),
            receipt_attestation_evidence=_d19_worm_evidence(
                "receipts/score-fields.attestation", "8"
            ),
            receipt_status="accepted",
        )


@pytest.mark.parametrize(
    ("tamper", "message"),
    (
        ("paired-delta-formula", "benchmark score paired delta formula is unsupported"),
        ("retention-formula", "benchmark score baseline retention formula is unsupported"),
        ("worst-cell", "benchmark score worst cell does not match the limiting cell"),
        (
            "paired-delta-scorecard",
            "benchmark score paired delta scorecard does not match its cells",
        ),
        (
            "retention-scorecard",
            "benchmark score baseline retention scorecard does not match its cells",
        ),
        (
            "retention-worst",
            "benchmark score worst baseline retention cell does not match the limiting cell",
        ),
        ("supporting-aggregate", "benchmark score supporting aggregates do not match its cells"),
    ),
)
def test_d19_score_cells_v2_rejects_tampered_benchmark_scorecard_invariants(
    tamper: str,
    message: str,
) -> None:
    operation_id = "serp-airflow-benchmark-improvement-wave-scorecard-invariant"
    paired = _d19_observed_paired_evaluation(operation_id)
    benchmark_score = paired["benchmarkScore"]
    if tamper == "paired-delta-formula":
        benchmark_score["referenceNormalizedPairedDeltaFormula"] = "mean delta"
    elif tamper == "retention-formula":
        benchmark_score["baselineRetentionFormula"] = "candidate / baseline"
    elif tamper == "worst-cell":
        benchmark_score["worstCell"]["metricCellId"] = "wrong:metric:cell"
    elif tamper == "paired-delta-scorecard":
        benchmark_score["referenceNormalizedPairedDeltaLcb95ByCell"][0][
            "referenceNormalizedPairedDeltaLcb95"
        ] = 0.024
    elif tamper == "retention-scorecard":
        benchmark_score["baselineRetentionByCell"][0]["baselineRetentionLcb95ToMean"] = 1.0
    elif tamper == "retention-worst":
        benchmark_score["worstBaselineRetentionCell"]["metricCellId"] = "wrong:metric:cell"
    else:
        benchmark_score["supportingAggregates"]["meanCandidateNormalizedMean"] = 0.94

    with pytest.raises(ValueError, match=message):
        serp_eval_contracts_module._observed_normalized_score_cells_payload(
            {"pairedEvaluation": paired},
            operation_id=operation_id,
            receipt_evidence=_d19_worm_evidence("receipts/scorecard-invariant", "7"),
            receipt_attestation_evidence=_d19_worm_evidence(
                "receipts/scorecard-invariant.attestation", "8"
            ),
            receipt_status="accepted",
        )


@pytest.mark.parametrize("tamper", ("receipt-digest", "verification-subject"))
def test_d19_verification_persistence_rejects_tampered_receipt_identity(
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    evaluator_result, objects = _d19_evaluator_result(plan)
    if tamper == "receipt-digest":
        evaluator_result["receiptEvidence"]["artifactSha256"] = "0" * 64
    else:
        evaluator_result["receiptVerification"]["subject"]["sha256"] = "sha256:" + "0" * 64

    with pytest.raises(ValueError, match="receipt.*(SHA-256|subject)"):
        serp_eval_contracts_module.write_paired_evaluation_verification_evidence(
            plan.to_canonical_json(),
            evaluator_result,
            _d19_airflow_run(),
            s3_client=_D19VerificationS3(objects),
        )


def test_d19_lifecycle_rejects_missing_mcp_runtime_binding_evidence_for_each_side() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    promotion_snapshot = _d19_promotion_snapshot(plan)
    promotion = serp_eval_contracts_module._validated_d19_promotion_snapshot(
        json.loads(plan.to_canonical_json()), promotion_snapshot
    )
    lifecycle_result = _d19_lifecycle_result(promotion_snapshot)

    with pytest.raises(ValueError, match="mcpRuntimeBindingEvidence"):
        serp_eval_contracts_module._validated_d19_lifecycle_result(
            json.loads(plan.to_canonical_json()), promotion, lifecycle_result
        )


@pytest.mark.parametrize(
    ("tamper", "message"),
    (
        ("receipt", "MCP runtime binding receipt evidence is mismatched"),
        ("pack", "MCP runtime binding packId is mismatched"),
        ("snapshot", "hermetic MCP snapshot ID is mismatched"),
    ),
)
def test_d19_lifecycle_rejects_mcp_runtime_binding_identity_drift(
    tamper: str,
    message: str,
) -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    promotion_snapshot = _d19_promotion_snapshot(plan)
    promotion = serp_eval_contracts_module._validated_d19_promotion_snapshot(
        json.loads(plan.to_canonical_json()), promotion_snapshot
    )
    lifecycle_result = _d19_lifecycle_result(promotion_snapshot)
    objects = _attach_d19_mcp_runtime_binding_evidence(lifecycle_result)
    material_pair = lifecycle_result["packMaterialBindings"][0]
    assert isinstance(material_pair, dict)
    material = material_pair["baseline"]
    assert isinstance(material, dict)
    runtime_evidence = material["mcpRuntimeBindingEvidence"]
    assert isinstance(runtime_evidence, dict)
    raw = objects[(runtime_evidence["s3Uri"], runtime_evidence["versionId"])]
    binding = json.loads(raw)
    assert isinstance(binding, dict)
    if tamper == "receipt":
        binding["packBuildReceiptEvidence"] = _d19_worm_evidence("foreign-receipt", "9")
        binding["packBuildReceiptSha256"] = "sha256:" + "9" * 64
    elif tamper == "pack":
        candidate = material_pair["candidate"]
        assert isinstance(candidate, dict)
        binding["packId"] = candidate["packId"]
    else:
        binding["packSnapshotId"] = "pack-snapshot:v2:tampered"
    material["mcpRuntimeBindingEvidence"] = _d19_worm_payload(
        f"mcp-runtime-bindings/tampered-{tamper}", binding, objects
    )

    with pytest.raises(ValueError, match=message):
        serp_eval_contracts_module._validated_d19_lifecycle_result(
            json.loads(plan.to_canonical_json()),
            promotion,
            lifecycle_result,
            s3_client=_D19VerificationS3(objects),
        )


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
            "retainUntil": "2027-07-14T00:00:00Z",
            "status": "written",
        }

    monkeypatch.setattr(
        serp_eval_contracts_module,
        "write_immutable_evidence_snapshot",
        snapshot_writer,
    )
    promotion_snapshot = _d19_promotion_snapshot(plan)
    lifecycle_result = _d19_lifecycle_result(promotion_snapshot)
    mcp_objects = _attach_d19_mcp_runtime_binding_evidence(lifecycle_result)
    request_artifact = write_paired_eval_request_artifact(
        plan.to_canonical_json(),
        json.dumps(_d19_catalog_snapshot(plan)),
        json.dumps(promotion_snapshot),
        json.dumps(lifecycle_result),
        s3_client=_D19VerificationS3(mcp_objects),
    )
    request = request_artifact["payload"]

    assert request["schema"] == "PairedEvaluationRequest/v6"
    assert request["evaluationBindingId"] == lifecycle_result["evaluationBindingId"]
    assert request["evaluationBindingEvidence"] == lifecycle_result["evaluationBindingEvidence"]
    assert (
        request["metricCompatibilityMatrixEvidence"]
        == promotion_snapshot["promotion"]["metricCompatibilityMatrixEvidence"]
    )
    assert (
        request["evaluationObjectiveEvidence"]
        == promotion_snapshot["promotion"]["evaluationObjectiveEvidence"]
    )
    assert (
        request["evaluationObjectiveAttestationEvidence"]
        == promotion_snapshot["promotion"]["evaluationObjectiveAttestationEvidence"]
    )
    assert request["benchmarkCatalogEvidence"] == {
        "catalog": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-07-14T00:00:00Z",
            "s3Uri": plan.payload["artifact_paths"]["benchmark_catalog"],
            "sha256": "sha256:" + "a" * 64,
            "versionId": "catalog-version-001",
        },
        "activation": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-07-14T00:00:00Z",
            "s3Uri": plan.payload["artifact_paths"]["benchmark_catalog_pack_activation"],
            "sha256": "sha256:" + "a" * 64,
            "versionId": "paired-request-version-001",
        },
        "receipt": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2027-07-14T00:00:00Z",
            "s3Uri": plan.payload["artifact_paths"]["benchmark_catalog_receipt"],
            "sha256": "sha256:" + "b" * 64,
            "versionId": "catalog-receipt-version-001",
        },
    }
    activation = snapshots[0]["payload"]
    assert activation == {
        "activationStatus": "evaluation-only",
        "benchmarkCatalogEvidence": {
            "catalog": request["benchmarkCatalogEvidence"]["catalog"],
            "receipt": request["benchmarkCatalogEvidence"]["receipt"],
        },
        "bindingFingerprint": lifecycle_result["bindingFingerprint"],
        "contractVersion": "BenchmarkCatalogPackActivation/v1",
        "evaluationBindingEvidence": lifecycle_result["evaluationBindingEvidence"],
        "evaluationBindingId": lifecycle_result["evaluationBindingId"],
        "operationId": plan.payload["operation_id"],
        "productionActivationRequested": False,
        "suitePackBindings": lifecycle_result["packMaterialBindings"],
        "tenantId": plan.payload["tenant_id"],
    }
    assert "Score" not in json.dumps(request)
    assert [snapshot["artifact_path"] for snapshot in snapshots] == [
        plan.payload["artifact_paths"]["benchmark_catalog_pack_activation"],
        plan.payload["artifact_paths"]["paired_eval_request"],
    ]
    assert [snapshot["artifact_type"] for snapshot in snapshots] == [
        "benchmark_catalog_pack_activation",
        "serp_paired_eval_request",
    ]
    assert snapshots[1]["payload"] == request
    assert request_artifact["requestEvidence"]["artifactVersionId"] == "paired-request-version-001"


def test_d19_rejects_v3_promotion_without_compatibility_fallback() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    promotion_snapshot = _d19_promotion_snapshot(plan)
    promotion_snapshot["promotion"]["schema"] = "EvaluationReleasePromotionReceipt/v3"

    with pytest.raises(ValueError, match="promotion schema is unsupported"):
        serp_eval_contracts_module._validated_d19_promotion_snapshot(
            plan.payload, promotion_snapshot
        )


def test_build_paired_benchmark_plan_exposes_only_server_owned_evaluation_paths() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())

    assert set(plan.payload["artifact_paths"]) == {
        "airflow_plan",
        "benchmark_catalog",
        "benchmark_catalog_pack_activation",
        "benchmark_catalog_receipt",
        "benchmark_pack_build_result",
        "benchmark_pack_lifecycle_result",
        "official_serp_mcp_measurement",
        "paired_evaluation_assembly_plan",
        "paired_evaluation_score_cells",
        "paired_evaluation_verification_evidence",
        "paired_execution_manifest",
        "paired_eval_request",
        "paired_eval_receipt",
    }
    assert all(path.startswith("s3://") for path in plan.payload["artifact_paths"].values())


def test_build_benchmark_improvement_wave_plan_requires_d17_promotion_metadata() -> None:
    conf = _improvement_wave_conf()
    del conf["evaluation_release_promotion_evidence"]

    with pytest.raises(ValueError, match="evaluation_release_promotion_evidence"):
        build_benchmark_improvement_wave_plan(conf)


def test_build_benchmark_improvement_wave_plan_rejects_raw_secret_metadata() -> None:
    conf = _improvement_wave_conf()
    conf["evaluation_release_promotion_evidence"] = {
        **_d19_worm_evidence("model-releases/d17-promotion", "c"),
        "versionId": "sk-abcdefghijklmnop",
    }

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
    state_path = tmp_path / "public-docs-crawl-state" / "state.json"
    state_path.parent.mkdir()
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


def test_public_docs_crawl_state_uses_a_dedicated_s3_security_prefix() -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-evidence/serp-evals"

    plan = build_public_docs_seed_refresh_plan(conf)

    assert plan.payload["public_docs_crawl_state_path"] == (
        "s3://airflow-serp-evidence/serp-evals/public-docs-crawl-state/state.json"
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
    state_path = tmp_path / "public-docs-crawl-state" / "state.json"
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
    assert target_conf["actor_id"] == "airflow-serp-public-docs-acquisition"
    assert target_conf["artifact_root_path"] == str(tmp_path)
    assert target_conf["generated_at"] == "2026-07-08T21:00:00Z"
    assert target_conf["pack_id"] == PACK_ID
    assert target_conf["pack_version_id"] == PACK_VERSION_ID
    assert target_conf["public_docs_crawl_state_path"] == str(
        tmp_path / "public-docs-crawl-state" / "state.json"
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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )

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

    monkeypatch.setattr(
        serp_eval_contracts_module,
        "importlib",
        types.SimpleNamespace(import_module=fake_import_module),
    )
    monkeypatch.setattr(
        serp_eval_contracts_module,
        "_ensure_public_docs_catalog_source",
        lambda _plan, *, bc21_base_url: "018f5e13-2d73-7a77-a052-8d1bcbf96599",
    )

    receipt_artifact = submit_public_docs_bc21_pipeline_state_artifact(plan.to_canonical_json())

    assert submission_calls == [
        {
            "actor_id": "airflow-serp-public-docs-acquisition",
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


def test_d20_rejects_actor_that_does_not_match_projected_workload_identity() -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["actor_id"] = "airflow-serp-public-docs-refresh"

    with pytest.raises(
        ValueError,
        match="actor_id must match the public-docs acquisition workload identity",
    ):
        build_public_docs_seed_refresh_plan(conf)


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
    state_path = tmp_path / "public-docs-crawl-state" / "state.json"
    state_path.parent.mkdir()
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
            "rolledBackBy": "airflow-serp-public-docs-acquisition",
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
                "X-Adapstory-Actor-Id": "airflow-serp-public-docs-acquisition",
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
    conf["search_serve_actor_id"] = "00000000-0000-4000-a000-000000000203"
    plan = build_public_docs_publish_activation_plan(conf)
    assert plan.payload["search_serve_actor_id"] == conf["search_serve_actor_id"]
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
    assert captured["body"]["actor_id"] == conf["search_serve_actor_id"]
    assert captured["body"]["auth_subject_id"] == "00000000-0000-4000-a000-000000000202"
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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )

    conf = _public_docs_publish_activation_conf(seed_refresh_result_path)
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    conf["public_docs_seed_refresh_plan_evidence"] = {
        "s3Uri": conf["public_docs_seed_refresh_plan_path"],
        "sha256": "sha256:" + "9" * 64,
        "versionId": "d20-refresh-plan-version-1",
    }
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

    monkeypatch.setattr(
        "dags.serp_eval_contracts._s3_client", lambda *_artifact_paths: FakeS3Client()
    )

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

    assert conf["actor_id"] == "airflow-serp-public-docs-acquisition"
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
                "produce_d19_run_history_observation",
                "trigger_benchmark_improvement_wave",
                "load_triggered_d19_verification",
                "observe_triggered_d19_run",
                "write_scheduled_d6_regression_receipt",
                "release_d19_history_fence",
                "finalize_scheduled_d6_regression",
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
            ],
        ),
        (
            "serp_model_catalog_promotion.py",
            "serp_model_catalog_promotion",
            [
                "validate_model_catalog_promotion_plan",
                "verify_runtime_terminal_activation_admission",
                "write_model_catalog_promotion_receipt",
                "build_d17_event_d6_trigger_conf",
                "trigger_model_promotion_regression_suite",
            ],
        ),
        (
            "serp_model_promotion_regression_suite.py",
            "serp_model_promotion_regression_suite",
            [
                "validate_d17_event_d6_plan",
                "trigger_benchmark_improvement_wave",
            ],
        ),
        (
            "serp_benchmark_improvement_wave.py",
            "serp_benchmark_improvement_wave",
            [
                "validate_benchmark_improvement_wave_plan",
                "materialize_live_benchmark_catalog",
                "load_materialized_benchmark_catalog",
                "load_model_catalog_promotion",
                "write_paired_eval_request",
                "run_paired_benchmark_evaluation",
                "write_official_serp_mcp_measurement",
                "publish_official_serp_mcp_measurement",
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
        assert "render_template_as_native_obj=True" in source
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            "validate_d19_fence_admission",
            "verify_runtime_terminal_activation_admission",
            "validate_benchmark_improvement_wave_plan",
            "load_materialized_benchmark_catalog",
            "load_model_catalog_promotion",
            "load_exact_nine_evaluation_binding",
            "write_paired_eval_request",
            "write_paired_evaluation_assembly_plan",
            "persist_paired_evaluation_verification_evidence",
            "write_official_serp_mcp_measurement",
            "publish_official_serp_mcp_measurement",
        ]
        assert _keyword_values(tree, "KubernetesPodOperator", "task_id") == [
            "materialize_live_benchmark_catalog",
            "build_exact_nine_benchmark_packs",
            "register_exact_nine_evaluation_binding",
            "materialize_official_harness_work_items",
            "assemble_paired_execution_manifest",
            "run_paired_benchmark_evaluation",
        ]
    elif dag_id == "serp_nightly_regression_suite":
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            task_id for task_id in task_ids if task_id != "trigger_benchmark_improvement_wave"
        ]
        assert _keyword_values(tree, "TriggerDagRunOperator", "task_id") == [
            "trigger_benchmark_improvement_wave"
        ]
    elif dag_id == "serp_model_catalog_promotion":
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            task_id for task_id in task_ids if task_id != "trigger_model_promotion_regression_suite"
        ]
        assert _keyword_values(tree, "TriggerDagRunOperator", "task_id") == [
            "trigger_model_promotion_regression_suite"
        ]
        assert 'trigger_dag_id="serp_model_promotion_regression_suite"' in source
        assert "write_receipt\n    >> build_event_d6_conf" in source
        assert "build_event_d6_conf\n    >> trigger_event_d6\n)" in source
        assert "fail_when_dag_is_paused=True" in source
    elif dag_id == "serp_model_promotion_regression_suite":
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            task_id for task_id in task_ids if task_id != "trigger_benchmark_improvement_wave"
        ]
        assert _keyword_values(tree, "TriggerDagRunOperator", "task_id") == [
            "trigger_benchmark_improvement_wave"
        ]
        assert 'trigger_dag_id="serp_benchmark_improvement_wave"' in source
        assert "logical_date" in source
        assert "fail_when_dag_is_paused=True" in source
        assert "default_nightly_regression_conf" not in source
        assert "scheduled_d6_fence" not in source
    elif dag_id == "serp_mandatory_benchmark_dataset_evidence_snapshot":
        assert _keyword_values(tree, "PythonOperator", "task_id") == [
            "validate_mandatory_benchmark_dataset_evidence_plan"
        ]
        assert _keyword_values(tree, "KubernetesPodOperator", "task_id") == [
            "materialize_mandatory_benchmark_dataset_evidence"
        ]
        assert "BENCHMARK_CATALOG_ACQUISITION_RESOURCES" in source
        assert "security_context=benchmark_catalog_acquisition_pod_security_context()" in source
        assert (
            "container_security_context=benchmark_catalog_acquisition_container_security_context()"
            in source
        )
        assert "BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS" in source
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
        "serp_model_catalog_promotion.py",
        "serp_model_promotion_regression_suite.py",
        "serp_benchmark_improvement_wave.py",
        "serp_publish_signed_pack.py",
        "serp_web_seed_crawl_refresh.py",
    ):
        source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")

        assert "from dags.serp_eval_contracts import" in source
        assert "from serp_eval_contracts import" not in source


def test_no_airflow_dag_keeps_the_retired_pending_governance_marker() -> None:
    retired_markers = {
        "governance_notification_pending",
        "governance_notification_from_public_docs_snapshot",
    }

    for dag_path in sorted((REPO_ROOT / "dags").glob("*.py")):
        source = dag_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported = {
            alias.asname or alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }
        defined = {
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef)
        }

        assert retired_markers.isdisjoint(imported), dag_path.name
        assert retired_markers.isdisjoint(defined), dag_path.name
        for marker in retired_markers:
            assert marker not in source, dag_path.name


def test_every_kubernetes_pod_operator_uses_the_dedicated_controller_executor_identity() -> None:
    for dag_file in (
        "serp_benchmark_improvement_wave.py",
        "serp_beir_scifact_live_benchmark.py",
        "serp_mandatory_benchmark_dataset_evidence_snapshot.py",
        "serp_web_seed_crawl_refresh.py",
    ):
        tree = ast.parse((REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8"))
        pod_operator_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "KubernetesPodOperator"
        ]

        assert pod_operator_calls, dag_file
        for call in pod_operator_calls:
            executor_config = next(
                (keyword.value for keyword in call.keywords if keyword.arg == "executor_config"),
                None,
            )
            assert isinstance(executor_config, ast.Call), dag_file
            assert isinstance(executor_config.func, ast.Name), dag_file
            assert executor_config.func.id == "kubernetes_pod_launcher_executor_config", dag_file


def test_serp_nightly_dag_uses_fenced_native_d19_without_gateway_scorer() -> None:
    source = (REPO_ROOT / "dags" / "serp_nightly_regression_suite.py").read_text(encoding="utf-8")

    assert "produce_d19_run_history_observation" in source
    assert "write_scheduled_d6_regression_receipt" in source
    assert "D19_HISTORY_OBSERVER_EXECUTOR_CONFIG" in source
    assert "airflow-serp-d19-history-observer" in source
    assert "serp-d19-history-observer-attestor-role" in source
    assert "TriggerDagRunOperator" in source
    for retired_surface in (
        "write_nightly_suite_plan_artifact",
        "execute_gateway_cli_spec",
        "build_nightly_runner_cli_spec",
        "build_nightly_benchmark_export_cli_spec",
        "build_nightly_registry_submit_cli_spec",
        "KubernetesPodOperator",
    ):
        assert retired_surface not in source


def test_d19_history_observer_separates_airflow_api_trust_from_credentials() -> None:
    source = (REPO_ROOT / "dags" / "serp_nightly_regression_suite.py").read_text(encoding="utf-8")

    assert 'secret_name="airflow-serp-d19-history-observer-credentials"' in source
    assert 'name="airflow-api-internal-ca"' in source
    assert 'k8s.V1KeyToPath(key="ca.crt", path="ca.crt")' in source
    assert "D19_HISTORY_API_TRUST_VOLUME" in source
    assert "_HISTORY_TRUST_ROOT" in source
    assert "_HISTORY_CREDENTIALS_ROOT" in source
    assert 'secret_name="airflow-serp-d19-history-observer-api"' not in source
    assert "_HISTORY_SECRET_ROOT" not in source


def test_every_catalog_acquisition_dag_projects_only_a_short_lived_minio_identity() -> None:
    for dag_file in (
        "serp_benchmark_improvement_wave.py",
        "serp_mandatory_benchmark_dataset_evidence_snapshot.py",
    ):
        source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")

        assert "benchmark_catalog_acquisition_web_identity_volumes" in source
        assert "benchmark_catalog_acquisition_web_identity_volume_mounts" in source
        assert "volumes=benchmark_catalog_acquisition_web_identity_volumes()" in source
        assert "volume_mounts=benchmark_catalog_acquisition_web_identity_volume_mounts()" in source
        assert "airflow-serp-evidence-store" not in source


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
    assert 'cmds=["python", "-m", "dags.serp_public_docs_seed_refresh_remote_runner"]' in source
    assert "airflow-serp-evidence-store" not in source
    assert "minio_web_identity_env_vars" in source
    assert "minio_web_identity_volumes" in source
    assert "minio_web_identity_volume_mounts" in source
    assert "airflow-artifact-store" not in source
    assert "PUBLIC_DOCS_ACQUISITION_WORKLOAD_LABELS" in source
    assert "airflow-serp-public-docs-acquisition" in source
    assert "bc21_authorized_minio_executor_config" in source
    assert (
        "PUBLIC_DOCS_ACQUISITION_EXECUTOR_CONFIG = bc21_authorized_minio_executor_config(" in source
    )


def test_serp_public_docs_dag_retries_transient_executor_api_outages() -> None:
    source = (REPO_ROOT / "dags" / "serp_web_seed_crawl_refresh.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    default_args = next(
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "default_args" for target in node.targets
        )
    )
    assert isinstance(default_args, ast.Dict)
    values = {
        key.value: value
        for key, value in zip(default_args.keys, default_args.values, strict=True)
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }

    retries = values["retries"]
    assert isinstance(retries, ast.Constant)
    assert retries.value == 2

    retry_delay = values["retry_delay"]
    assert isinstance(retry_delay, ast.Call)
    assert isinstance(retry_delay.func, ast.Name)
    assert retry_delay.func.id == "timedelta"
    assert len(retry_delay.keywords) == 1
    assert retry_delay.keywords[0].arg == "minutes"
    assert isinstance(retry_delay.keywords[0].value, ast.Constant)
    assert retry_delay.keywords[0].value.value == 2


def test_serp_public_docs_dag_uses_airflow_3_sdk_exceptions() -> None:
    source = (REPO_ROOT / "dags" / "serp_web_seed_crawl_refresh.py").read_text(encoding="utf-8")

    assert "from airflow.sdk.exceptions import AirflowSkipException" in source
    assert "from airflow.exceptions import AirflowSkipException" not in source


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
            assert "volumes=PUBLIC_DOCS_ACQUISITION_RUNTIME_VOLUMES" in source
            assert "volume_mounts=PUBLIC_DOCS_ACQUISITION_RUNTIME_VOLUME_MOUNTS" in source
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
        "serp_model_catalog_promotion.py",
        "serp_model_promotion_regression_suite.py",
        "serp_benchmark_improvement_wave.py",
    ),
)
def test_serp_operational_dags_are_unpaused_when_airflow_creates_them(dag_file: str) -> None:
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


def test_event_d6_accepts_the_earliest_d17_contract_epoch() -> None:
    """A D17 release generated at its supported epoch must create task instances."""

    source = (REPO_ROOT / "dags" / "serp_model_promotion_regression_suite.py").read_text(
        encoding="utf-8"
    )

    assert '"start_date": datetime(2026, 7, 15, tzinfo=UTC)' in source


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
        "skip_when_already_exists": False,
        "fail_when_dag_is_paused": True,
    }
    for name, expected in expected_values.items():
        value = values[name]
        assert isinstance(value, ast.Constant)
        assert value.value == expected


def test_scheduled_d6_triggers_one_unique_serialized_d19_child_without_legacy_scorer() -> None:
    source = (REPO_ROOT / "dags" / "serp_nightly_regression_suite.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    trigger_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _matches_call(node, "TriggerDagRunOperator")
    ]

    assert len(trigger_calls) == 1
    values = {keyword.arg: keyword.value for keyword in trigger_calls[0].keywords}
    expected_constants = {
        "task_id": "trigger_benchmark_improvement_wave",
        "trigger_dag_id": "serp_benchmark_improvement_wave",
        "reset_dag_run": False,
        "wait_for_completion": True,
        "skip_when_already_exists": False,
        "deferrable": True,
    }
    for name, expected in expected_constants.items():
        value = values[name]
        assert isinstance(value, ast.Constant)
        assert value.value == expected
    assert isinstance(values["trigger_run_id"], ast.Constant)
    assert values["trigger_run_id"].value == "d6__{{ run_id }}"
    assert isinstance(values["logical_date"], ast.Constant)
    assert values["logical_date"].value == "{{ logical_date }}"

    for legacy_surface in (
        "write_nightly_suite_plan_artifact",
        "build_nightly_runner_cli_spec",
        "run_mandatory_benchmark_suites",
        "materialize_live_benchmark_catalog",
    ):
        assert legacy_surface not in source


def test_observe_triggered_d19_run_uses_task_instance_public_metadata_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    monkeypatch.delitem(sys.modules, "dags.serp_nightly_regression_suite", raising=False)
    module = importlib.import_module("dags.serp_nightly_regression_suite")

    class DagRunWithoutTaskInstanceMethods:
        pass

    class SuccessfulDagRunState:
        value = "success"

    class TaskInstance:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []

        def get_dagrun_state(self, dag_id: str, run_id: str) -> SuccessfulDagRunState:
            self.calls.append(("get_dagrun_state", (dag_id, run_id)))
            return SuccessfulDagRunState()

        def get_dr_count(
            self,
            *,
            dag_id: str,
            logical_dates: list[datetime],
            states: list[str] | None = None,
        ) -> int:
            self.calls.append(("get_dr_count", (dag_id, logical_dates, states)))
            return 1

    task_instance = TaskInstance()
    result = module.observe_triggered_d19_run(
        "d6__scheduled__2026-07-17T00:00:00+00:00",
        "2026-07-17T00:00:00Z",
        dag_run=DagRunWithoutTaskInstanceMethods(),
        ti=task_instance,
    )

    assert result["state"] == "success"
    assert result["sameLogicalDateRunCount"] == 1
    assert result["sameLogicalDateSuccessCount"] == 1
    assert [call[0] for call in task_instance.calls] == [
        "get_dagrun_state",
        "get_dr_count",
        "get_dr_count",
    ]


def test_scheduled_d6_receipt_failure_propagates_after_fence_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    monkeypatch.delitem(sys.modules, "dags.serp_nightly_regression_suite", raising=False)
    module = importlib.import_module("dags.serp_nightly_regression_suite")
    plan = build_nightly_regression_plan(_scheduled_d6_conf())
    fence = _scheduled_d6_fence(_scheduled_d6_airflow_run())

    class FenceClient:
        def __init__(self) -> None:
            self.released: list[dict[str, Any]] = []

        def release(self, value: Mapping[str, Any]) -> None:
            self.released.append(dict(value))

    fence_client = FenceClient()
    released = module.release_d19_history_fence(
        {"fence": fence},
        fence_client=fence_client,
    )

    assert released == {"status": "released"}
    assert fence_client.released == [fence]
    assert module.release_fence.kwargs["trigger_rule"] == "all_done"
    assert module.finalize_regression.kwargs["trigger_rule"] == "all_done"
    assert (
        module.finalize_regression.kwargs["python_callable"]
        is serp_eval_contracts_module.finalize_scheduled_d6_regression
    )
    source = (REPO_ROOT / "dags" / "serp_nightly_regression_suite.py").read_text(encoding="utf-8")
    assert "[write_receipt, release_fence] >> finalize_regression" in source
    receipt_evidence = {
        **_d19_worm_evidence("scheduled-d6-terminal", "d"),
        "s3Uri": plan.payload["artifact_paths"]["scheduled_regression_receipt"],
    }
    accepted_receipt = {
        "operationId": plan.payload["operation_id"],
        "scheduledD6RegressionEvidence": receipt_evidence,
        "status": "accepted",
    }
    assert serp_eval_contracts_module.finalize_scheduled_d6_regression(
        plan.to_canonical_json(),
        accepted_receipt,
        released,
    ) == {
        "operationId": plan.payload["operation_id"],
        "scheduledD6RegressionEvidence": receipt_evidence,
        "status": "finalized",
    }
    with pytest.raises(ValueError, match="scheduled D6 receipt result must be accepted"):
        serp_eval_contracts_module.finalize_scheduled_d6_regression(
            plan.to_canonical_json(),
            {**accepted_receipt, "status": "rejected"},
            released,
        )
    with pytest.raises(ValueError, match="scheduled D6 fence release result must be released"):
        serp_eval_contracts_module.finalize_scheduled_d6_regression(
            plan.to_canonical_json(),
            accepted_receipt,
            {"status": "failed"},
        )
    with pytest.raises(ValueError, match="scheduled D6 receipt result is required"):
        serp_eval_contracts_module.finalize_scheduled_d6_regression(
            plan.to_canonical_json(),
            None,
            released,
        )


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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_web_seed_crawl_refresh")
    module = importlib.reload(module)
    monkeypatch.setattr(module, "discover_public_docs_crawler_frontier", lambda *_args: [])
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "30")
    storage: dict[tuple[str, str, str], bytes] = {}

    class MissingObjectError(Exception):
        response: ClassVar[dict[str, dict[str, str]]] = {"Error": {"Code": "NoSuchKey"}}

    class FakeS3Client:
        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> dict[str, str]:
            assert ContentType == "application/json"
            version_id = f"version-{len(storage) + 1}"
            storage[(Bucket, Key, version_id)] = Body
            return {"ETag": sha256(Body).hexdigest(), "VersionId": version_id}

        def head_object(self, *, Bucket: str, Key: str, VersionId: str) -> dict[str, object]:
            assert (Bucket, Key, VersionId) in storage
            return {
                "ContentLength": len(storage[(Bucket, Key, VersionId)]),
                "ObjectLockMode": "COMPLIANCE",
                "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=365),
                "VersionId": VersionId,
            }

        def get_object(
            self, *, Bucket: str, Key: str, VersionId: str | None = None
        ) -> dict[str, object]:
            if VersionId is None:
                raise MissingObjectError()
            return {"Body": io.BytesIO(storage[(Bucket, Key, VersionId)])}

    client = FakeS3Client()
    monkeypatch.setattr(serp_eval_contracts_module, "_s3_client", lambda *_paths: client)

    class DagRun:
        def __init__(self) -> None:
            self.conf = {
                "artifact_root_path": "s3://airflow-serp-evidence/serp-public-docs",
                "generated_at": "2026-07-08T21:30:00Z",
            }

    plan_handle = module.validate_public_docs_seed_registry(dag_run=DagRun())
    plan = serp_eval_contracts_module.load_public_docs_airflow_plan_snapshot(
        plan_handle,
        s3_client=client,
    )

    assert set(plan_handle) == {"planEvidence", "schema", "summary"}
    assert plan["generated_at"] == "2026-07-08T21:30:00Z"
    assert plan["seed_count"] == len(P0_PUBLIC_DOCS_SOURCES)
    assert {seed["seed_id"] for seed in plan["seed_registry"]} == {
        str(source["seed_id"]) for source in P0_PUBLIC_DOCS_SOURCES
    }
    assert all(
        path.startswith("s3://airflow-serp-evidence/serp-public-docs")
        for path in plan["artifact_paths"].values()
    )


def test_public_docs_pipeline_runner_env_contract_survives_native_template_rendering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_web_seed_crawl_refresh")
    module = importlib.reload(module)
    numeric_values = {
        "ADAPSTORY_SERP_EMBEDDING_BATCH_SIZE": "16",
        "ADAPSTORY_SERP_EMBEDDING_DIMENSION": "768",
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
        assert values[name] == f'"{value}"'
    assert values["ADAPSTORY_BC10_GATEWAY_URL"] == "test"
    assert values["ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_ROUTE_ID"] == "test"
    assert values["ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_MODEL_VERSION_ID"] == "test"
    assert values["ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_PROMPT_TEMPLATE_VERSION"] == "test"
    assert values["ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_BUDGET_POLICY_ID"] == "test"
    assert values["ADAPSTORY_BC10_TOKEN_PATH"] == ("/var/run/secrets/adapstory/bc10-workload/token")
    assert "ADAPSTORY_SERP_EMBEDDING_URL" not in values
    assert "ADAPSTORY_SERP_EMBEDDING_PROVIDER_MODEL" not in values
    assert "ADAPSTORY_SERP_EMBEDDING_MODEL_ID" not in values
    assert "ADAPSTORY_SERP_EMBEDDING_MODEL_VERSION" not in values
    assert "ADAPSTORY_SERP_EMBEDDING_MAX_ATTEMPTS" not in values
    assert "ADAPSTORY_SERP_EMBEDDING_RETRY_DELAY_SECONDS" not in values
    assert "ADAPSTORY_SERP_EMBEDDING_TIMEOUT_SECONDS" not in values
    assert values["ADAPSTORY_SERP_SEARCH_SERVE_ACTOR_ID"] == "test"
    volume_names = {item.kwargs["name"] for item in module.PUBLIC_DOCS_ACQUISITION_RUNTIME_VOLUMES}
    mount_paths = {
        item.kwargs["mount_path"] for item in module.PUBLIC_DOCS_ACQUISITION_RUNTIME_VOLUME_MOUNTS
    }
    assert "bc10-workload-token" in volume_names
    assert "/var/run/secrets/adapstory/bc10-workload" in mount_paths
    expected_cli_spec_template = (
        "{{ ti.xcom_pull(task_ids='dispatch_pipeline_seed_refresh_handoff') | tojson | urlencode }}"
    )
    assert values["ADAPSTORY_SERP_PIPELINE_CLI_SPEC_URLENCODED"] == expected_cli_spec_template
    assert "ADAPSTORY_SERP_PIPELINE_CLI_SPEC_JSON" not in values


@pytest.mark.parametrize("value", ("900", "0.5", "-7", "1e-3"))
def test_native_template_safe_env_value_preserves_every_numeric_literal_as_a_string(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_evidence_workload_identity")
    module = importlib.reload(module)

    assert module.native_template_safe_env_value(value) == f'"{value}"'


def test_bc10_workload_identity_projects_exact_bounded_audience(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_evidence_workload_identity")
    module = importlib.reload(module)

    env = module.bc10_workload_env_vars()[0].kwargs
    assert env == {
        "name": "ADAPSTORY_BC10_TOKEN_PATH",
        "value": "/var/run/secrets/adapstory/bc10-workload/token",
    }

    volume = module.bc10_workload_volumes()[0].kwargs
    assert volume["name"] == "bc10-workload-token"
    projection = volume["projected"].kwargs["sources"][0].kwargs["service_account_token"].kwargs
    assert projection == {
        "audience": "adapstory-bc10-model-gateway",
        "expiration_seconds": 900,
        "path": "token",
    }

    mount = module.bc10_workload_volume_mounts()[0].kwargs
    assert mount == {
        "mount_path": "/var/run/secrets/adapstory/bc10-workload",
        "name": "bc10-workload-token",
        "read_only": True,
    }


def test_history_observer_uses_its_dedicated_transit_attestor_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_evidence_workload_identity")
    module = importlib.reload(module)

    values = {
        env.kwargs["name"]: env.kwargs["value"]
        for env in module.vault_transit_env_vars(
            auth_role="serp-d19-history-observer-attestor-role"
        )
    }

    assert values["ADAPSTORY_VAULT_KUBERNETES_AUTH_ROLE"] == (
        "serp-d19-history-observer-attestor-role"
    )


def _install_airflow_import_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    # The workload-identity helper can be imported earlier by a real-kubernetes
    # contract test. Remove that cached module for this test scope so the DAG
    # under test resolves every Kubernetes model from the stubs below.
    monkeypatch.delitem(sys.modules, "dags.serp_evidence_workload_identity", raising=False)

    class FakeAirflowSkipException(Exception):
        pass

    class FakeAirflowException(Exception):
        pass

    class FakeTriggerRule:
        ALL_DONE = "all_done"
        ONE_FAILED = "one_failed"

    class FakeDAG:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

    class FakeXComArg:
        def __init__(self, operator: object) -> None:
            self.operator = operator

    class FakePartialOperator:
        def __init__(
            self, operator_class: type[FakePythonOperator], kwargs: dict[str, object]
        ) -> None:
            self.operator_class = operator_class
            self.kwargs = kwargs

        def expand_kwargs(self, mapped_kwargs: object) -> FakePythonOperator:
            return self.operator_class(**self.kwargs, mapped_kwargs=mapped_kwargs)

    class FakePythonOperator:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.kwargs = _kwargs

        @classmethod
        def partial(cls, **kwargs: object) -> FakePartialOperator:
            return FakePartialOperator(cls, kwargs)

        @property
        def output(self) -> FakeXComArg:
            return FakeXComArg(self)

        def __rshift__(self, other: object) -> object:
            return other

        def __rrshift__(self, _other: object) -> FakePythonOperator:
            return self

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
        "ADAPSTORY_BC10_GATEWAY_URL",
        "ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_BUDGET_POLICY_ID",
        "ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_MODEL_VERSION_ID",
        "ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_PROMPT_TEMPLATE_VERSION",
        "ADAPSTORY_BC10_PUBLIC_DOCS_EMBEDDING_ROUTE_ID",
        "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
        "ADAPSTORY_SERP_EMBEDDING_BATCH_SIZE",
        "ADAPSTORY_SERP_EMBEDDING_DIMENSION",
        "ADAPSTORY_SERP_EMBEDDING_PROFILE_VERSION",
        "ADAPSTORY_SERP_NEO4J_HTTP_URL",
        "ADAPSTORY_SERP_NEO4J_MUTATION_BATCH_SIZE",
        "ADAPSTORY_SERP_NEO4J_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_NEO4J_USERNAME",
        "ADAPSTORY_SERP_OPENSEARCH_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_OPENSEARCH_URL",
        "ADAPSTORY_SERP_QDRANT_TIMEOUT_SECONDS",
        "ADAPSTORY_SERP_QDRANT_UPSERT_BATCH_SIZE",
        "ADAPSTORY_SERP_QDRANT_URL",
        "ADAPSTORY_SERP_SEARCH_SERVE_ACTOR_ID",
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
        "airflow.sdk.exceptions": types.ModuleType("airflow.sdk.exceptions"),
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
    cast(Any, modules["airflow.sdk.exceptions"]).AirflowSkipException = FakeAirflowSkipException
    cast(Any, modules["airflow.exceptions"]).AirflowException = FakeAirflowException
    cast(
        Any, modules["airflow.providers.standard.operators.trigger_dagrun"]
    ).TriggerDagRunOperator = FakeTriggerDagRunOperator
    cast(
        Any, modules["airflow.providers.cncf.kubernetes.operators.pod"]
    ).KubernetesPodOperator = FakeKubernetesPodOperator
    cast(Any, modules["airflow.sdk"]).DAG = FakeDAG
    cast(Any, modules["airflow.sdk"]).literal = lambda value: value
    cast(Any, modules["airflow.utils.trigger_rule"]).TriggerRule = FakeTriggerRule
    models = cast(Any, modules["kubernetes.client.models"])
    models.V1Capabilities = FakeKubernetesModel
    models.V1ConfigMapKeySelector = FakeKubernetesModel
    models.V1ConfigMapProjection = FakeKubernetesModel
    models.V1ConfigMapVolumeSource = FakeKubernetesModel
    models.V1EnvVar = FakeKubernetesModel
    models.V1EnvVarSource = FakeKubernetesModel
    models.V1Container = FakeKubernetesModel
    models.V1EmptyDirVolumeSource = FakeKubernetesModel
    models.V1ObjectMeta = FakeKubernetesModel
    models.V1ObjectFieldSelector = FakeKubernetesModel
    models.V1Pod = FakeKubernetesModel
    models.V1PodSpec = FakeKubernetesModel
    models.V1PodSecurityContext = FakeKubernetesModel
    models.V1ResourceRequirements = FakeKubernetesModel
    models.V1SeccompProfile = FakeKubernetesModel
    models.V1SecretKeySelector = FakeKubernetesModel
    models.V1SecretVolumeSource = FakeKubernetesModel
    models.V1SecurityContext = FakeKubernetesModel
    models.V1ServiceAccountTokenProjection = FakeKubernetesModel
    models.V1KeyToPath = FakeKubernetesModel
    models.V1Volume = FakeKubernetesModel
    models.V1VolumeMount = FakeKubernetesModel
    models.V1VolumeProjection = FakeKubernetesModel
    models.V1ProjectedVolumeSource = FakeKubernetesModel
    cast(Any, modules["kubernetes.client"]).models = models
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_serp_improvement_dag_uses_pipeline_executor_for_d19_path() -> None:
    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")

    assert "write_improvement_spec_artifact" not in source
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
    assert "service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT" in source
    assert "labels=D19_AGGREGATOR_WORKLOAD_LABELS" in source
    tree = ast.parse(source)
    evaluator = next(
        call
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and _matches_call(call, "KubernetesPodOperator")
        and any(
            keyword.arg == "task_id"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "run_paired_benchmark_evaluation"
            for keyword in call.keywords
        )
    )
    evaluator_automount = next(
        keyword.value
        for keyword in evaluator.keywords
        if keyword.arg == "automount_service_account_token"
    )
    assert isinstance(evaluator_automount, ast.Constant)
    assert evaluator_automount.value is False
    assert "volumes=D19_AGGREGATOR_VOLUMES" in source
    assert "volume_mounts=D19_AGGREGATOR_VOLUME_MOUNTS" in source
    assert "security_context=hardened_runtime_pod_security_context()" in source
    assert "container_security_context=hardened_runtime_container_security_context()" in source
    assert "bc21_workload_env_vars()" in source
    assert "bc21_workload_volumes()" in source
    assert "bc21_workload_volume_mounts()" in source
    assert "hardened_runtime_volumes()" in source
    assert "hardened_runtime_volume_mounts()" in source
    assert "airflow-serp-evidence-store" not in source
    assert '"--paired-eval-request-sha256"' in source
    assert "['requestEvidence']['artifactSha256']" in source
    assert "def run_paired_benchmark_evaluation" not in source
    assert "execute_pipeline_cli_spec" not in source


def test_serp_improvement_dag_passes_exact_s3_values_to_the_evaluator_pod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    monkeypatch.setenv(
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
        "http://minio.env-prod.svc.cluster.local:9000",
    )
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION", "us-west-1")
    monkeypatch.setenv(
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST",
        "sha256:" + "d" * 64,
    )
    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://serp-context-platform.serp.svc.cluster.local:8080/api/bc-21/serp/v1",
    )
    monkeypatch.setenv(
        "ADAPSTORY_SERP_MCP_GATEWAY_BASE_URL",
        "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000",
    )
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    values = {
        env_var.kwargs["name"]: env_var.kwargs["value"]
        for env_var in module.d19_aggregator_env_vars()
        if "value" in env_var.kwargs
    }

    assert module.D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT == ("airflow-serp-benchmark-aggregator")
    assert (
        module.D19_AGGREGATOR_WORKLOAD_LABELS["adapstory.com/serp-network-profile"]
        == "benchmark-aggregator"
    )

    assert values == {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": ("http://minio.env-prod.svc.cluster.local:9000"),
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-west-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS": '"900"',
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE": (
            "/var/run/secrets/adapstory/minio-web-identity/token"
        ),
    }


def test_d19_serializes_runs_and_caps_expensive_parallelism() -> None:
    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    dag_call = next(
        node for node in ast.walk(tree) if isinstance(node, ast.Call) and _matches_call(node, "DAG")
    )
    integer_keywords = {
        keyword.arg: keyword.value.value
        for keyword in dag_call.keywords
        if keyword.arg in {"max_active_runs", "max_active_tasks"}
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, int)
    }

    assert integer_keywords == {"max_active_runs": 1, "max_active_tasks": 2}


def test_d19_builds_and_registers_server_owned_exact_nine_before_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
        "ADAPSTORY_SERP_MCP_GATEWAY_BASE_URL": (
            "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
        ),
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    builder = module.build_exact_nine_benchmark_packs.kwargs
    registrar = module.register_exact_nine_evaluation_binding.kwargs
    expected_module = [
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.bc21_benchmark_pack_lifecycle_cli",
    ]
    for task in (builder, registrar):
        assert task["cmds"] == expected_module
        assert task["service_account_name"] == "airflow-serp-benchmark-builder"
        assert task["automount_service_account_token"] is False
        assert task["do_xcom_push"] is True
        assert task["labels"]["adapstory.com/serp-network-profile"] == "benchmark-builder"
        assert task["security_context"].kwargs["run_as_non_root"] is True
        assert task["container_security_context"].kwargs["read_only_root_filesystem"] is True
    assert builder["arguments"][0] == "build-exact-nine"
    assert registrar["arguments"][0] == "register-binding"
    assert "--lifecycle-input" in registrar["arguments"]
    assert "--result-output" in builder["arguments"]
    assert "--result-output" in registrar["arguments"]

    builder_env = {
        item.kwargs["name"]: item.kwargs.get("value") for item in module.d19_builder_env_vars()
    }
    assert builder_env["ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST"] == "sha256:" + "d" * 64
    assert builder_env["ADAPSTORY_SERP_BC21_BASE_URL"] == (
        "http://context-platform:8080/api/bc-21/serp/v1"
    )
    assert builder_env["ADAPSTORY_BC10_GATEWAY_URL"] == "test"
    assert builder_env["ADAPSTORY_BC10_TOKEN_PATH"] == (
        "/var/run/secrets/adapstory/bc10-workload/token"
    )
    assert "ADAPSTORY_OLLAMA_BASE_URL" not in builder_env
    assert "ADAPSTORY_SERP_MCP_GATEWAY_BASE_URL" not in builder_env
    assert not any(
        "QDRANT" in name or "OPENSEARCH" in name or "NEO4J" in name for name in builder_env
    )
    builder_volume_names = {item.kwargs["name"] for item in module.D19_BUILDER_VOLUMES}
    builder_mount_paths = {item.kwargs["mount_path"] for item in module.D19_BUILDER_VOLUME_MOUNTS}
    assert "bc10-workload-token" in builder_volume_names
    assert "/var/run/secrets/adapstory/bc10-workload" in builder_mount_paths

    aggregator_env = {
        item.kwargs["name"]: item.kwargs.get("value") for item in module.d19_aggregator_env_vars()
    }
    assert "ADAPSTORY_SERP_BC21_BASE_URL" not in aggregator_env
    assert "ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH" not in aggregator_env
    for task in (
        module.materialize_official_harness_work_items,
        module.assemble_paired_execution_manifest,
        module.run_paired_evaluation,
    ):
        assert task.kwargs["service_account_name"] == "airflow-serp-benchmark-aggregator"
        assert task.kwargs["labels"]["adapstory.com/serp-network-profile"] == (
            "benchmark-aggregator"
        )
    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")
    assert "load_catalog >> build_exact_nine_benchmark_packs" in source
    assert "load_promotion >> build_exact_nine_benchmark_packs" in source
    assert "build_exact_nine_benchmark_packs >> register_exact_nine_evaluation_binding" in source
    assert "register_exact_nine_evaluation_binding >> load_exact_nine_evaluation_binding" in source
    assert '"--lifecycle-result"' in source
    assert '"--lifecycle-result-version-id"' in source
    assert '"--lifecycle-result-sha256"' in source


def test_d19_runs_exact_ninety_server_owned_official_harness_work_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_MCP_GATEWAY_BASE_URL": (
            "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
        ),
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    tasks = module.D19_OFFICIAL_HARNESS_RUN_TASKS
    expected = [
        (suite_id, side, repetition)
        for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        for repetition in range(1, 6)
        for side in ("baseline", "candidate")
    ]
    assert list(tasks) == expected
    assert len(tasks) == 90
    assert module.D19_MODEL_RUNNER_WORKLOAD_SERVICE_ACCOUNT == (
        "airflow-serp-benchmark-model-runner"
    )
    assert (
        module.D19_MODEL_RUNNER_WORKLOAD_LABELS["adapstory.com/serp-network-profile"]
        == "benchmark-model-runner"
    )
    expected_limits = {
        "APIBench": {"cpu": "2000m", "memory": "4Gi"},
        "ARES": {"cpu": "4000m", "memory": "8Gi"},
        "BEIR": {"cpu": "4000m", "memory": "8Gi"},
        "CodeRAG-Bench": {"cpu": "8000m", "memory": "16Gi"},
        "RAGBench": {"cpu": "4000m", "memory": "8Gi"},
        "RepoQA": {"cpu": "8000m", "memory": "16Gi"},
        "SWE-bench Verified": {"cpu": "8000m", "memory": "16Gi"},
        "cwd-benchmark-data": {"cpu": "4000m", "memory": "8Gi"},
        "rusBEIR": {"cpu": "4000m", "memory": "8Gi"},
    }
    for (suite_id, _side, _repetition), task in module.D19_STANDARD_HARNESS_RUN_TASKS.items():
        assert task.kwargs["service_account_name"] == ("airflow-serp-benchmark-model-runner")
        assert task.kwargs["automount_service_account_token"] is False
        assert task.kwargs["do_xcom_push"] is True
        assert task.kwargs["cmds"] == [
            "python",
            "-m",
            "adapstory_serp_pipeline.orchestration.official_harness_execution",
        ]
        assert task.kwargs["arguments"][0] == "run-suite"
        assert task.kwargs["security_context"].kwargs["run_as_non_root"] is True
        assert task.kwargs["container_security_context"].kwargs["read_only_root_filesystem"] is True
        assert task.kwargs["container_resources"].kwargs["limits"] == expected_limits[suite_id]


def test_d19_routes_code_suites_through_credential_isolated_sandbox_chains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    assert module.D19_CODE_SANDBOX_SUITES == frozenset({"CodeRAG-Bench", "SWE-bench Verified"})
    assert len(module.D19_STANDARD_HARNESS_RUN_TASKS) == 70
    assert len(module.D19_CODE_SANDBOX_PREPARE_TASKS) == 20
    assert len(module.D19_CODE_SANDBOX_FANOUT_TASKS) == 20
    assert len(module.D19_CODE_SANDBOX_TASKS) == 20
    assert len(module.D19_CODE_SANDBOX_RESULT_SET_PLAN_TASKS) == 20
    assert len(module.D19_CODE_SANDBOX_SEAL_TASKS) == 20
    assert len(module.D19_OFFICIAL_HARNESS_RUN_TASKS) == 90
    for identity, sandbox_task in module.D19_CODE_SANDBOX_TASKS.items():
        suite_id, _side, _repetition = identity
        assert suite_id in module.D19_CODE_SANDBOX_SUITES
        kwargs = sandbox_task.kwargs
        assert kwargs["service_account_name"] == "airflow-serp-benchmark-code-sandbox"
        assert kwargs["automount_service_account_token"] is False
        assert kwargs["labels"]["adapstory.com/serp-network-profile"] == ("benchmark-code-sandbox")
        assert kwargs["do_xcom_push"] is True
        output_mount = next(
            mount
            for mount in kwargs["volume_mounts"]
            if mount.kwargs["mount_path"] == "/sandbox/output"
        )
        assert output_mount.kwargs["read_only"] is True
        assert kwargs["cmds"] == [
            "python",
            "-m",
            "adapstory_serp_pipeline.orchestration.official_harness_execution",
        ]
        assert kwargs["image"] == "harbor/airflow@sha256:" + "d" * 64
        publisher_env = {env.kwargs["name"]: env for env in kwargs["env_vars"]}
        assert set(("POD_NAME", "POD_NAMESPACE", "POD_UID")) <= set(publisher_env)
        assert (
            publisher_env["POD_NAME"].kwargs["value_from"].kwargs["field_ref"].kwargs["field_path"]
            == "metadata.name"
        )
        assert (
            publisher_env["POD_NAMESPACE"]
            .kwargs["value_from"]
            .kwargs["field_ref"]
            .kwargs["field_path"]
            == "metadata.namespace"
        )
        assert (
            publisher_env["POD_UID"].kwargs["value_from"].kwargs["field_ref"].kwargs["field_path"]
            == "metadata.uid"
        )
        pod_status_mount = next(
            mount
            for mount in kwargs["volume_mounts"]
            if mount.kwargs["mount_path"] == "/var/run/secrets/kubernetes.io/serviceaccount"
        )
        assert pod_status_mount.kwargs["name"] == "d19-code-sandbox-pod-status-token"
        assert pod_status_mount.kwargs["read_only"] is True
        pod_status_volume = next(
            volume
            for volume in kwargs["volumes"]
            if volume.kwargs["name"] == "d19-code-sandbox-pod-status-token"
        )
        projected_sources = pod_status_volume.kwargs["projected"].kwargs["sources"]
        assert len(projected_sources) == 2
        assert (
            projected_sources[0].kwargs["service_account_token"].kwargs["expiration_seconds"] == 900
        )
        assert projected_sources[1].kwargs["config_map"].kwargs["name"] == ("kube-root-ca.crt")
        workspace = next(
            volume
            for volume in kwargs["volumes"]
            if volume.kwargs["name"] == "d19-code-sandbox-workspace"
        )
        assert workspace.kwargs["empty_dir"].kwargs["size_limit"] == "32Gi"
        assert kwargs["mapped_kwargs"].operator is module.D19_CODE_SANDBOX_FANOUT_TASKS[identity]
        assert not any(
            "OLLAMA" in env.kwargs.get("name", "")
            or "BC10" in env.kwargs.get("name", "")
            or "BC21" in env.kwargs.get("name", "")
            or "MCP" in env.kwargs.get("name", "")
            for env in kwargs["env_vars"]
        )
        assert "arguments" not in kwargs
        assert "init_containers" not in kwargs
        assert "full_pod_spec" not in kwargs
        fanout = module.D19_CODE_SANDBOX_FANOUT_TASKS[identity]
        assert fanout.kwargs["python_callable"] is (
            module.build_code_sandbox_mapped_operator_kwargs
        )
        result_set_plan = module.D19_CODE_SANDBOX_RESULT_SET_PLAN_TASKS[identity]
        assert result_set_plan.kwargs["python_callable"] is (
            module.write_code_sandbox_result_set_assembly_plan
        )
        seal_arguments = module.D19_CODE_SANDBOX_SEAL_TASKS[identity].kwargs["arguments"]
        assert "--sandbox-result-set-plan" in seal_arguments
        assert "--sandbox-result" not in seal_arguments
        prepare = module.D19_CODE_SANDBOX_PREPARE_TASKS[identity].kwargs
        assert prepare["service_account_name"] == "airflow-serp-benchmark-model-runner"
        assert prepare["labels"]["adapstory.com/serp-network-profile"] == ("benchmark-model-runner")
        prepare_env = {item.kwargs["name"] for item in prepare["env_vars"]}
        assert {"ADAPSTORY_BC10_GATEWAY_URL", "ADAPSTORY_BC10_TOKEN_PATH"} <= prepare_env
        assert "ADAPSTORY_OLLAMA_BASE_URL" not in prepare_env
        assert (
            module.D19_CODE_SANDBOX_SEAL_TASKS[identity].kwargs["service_account_name"]
            == "airflow-serp-benchmark-aggregator"
        )

    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")
    assert "KubernetesPodOperator.partial(" in source
    assert ").expand_kwargs(fanout_task.output)" in source
    assert "prepare_task >> fanout_task >> sandbox_task >> result_set_plan_task" in source
    assert "result_set_plan_task >> seal_task >> write_assembly_plan" in source


def test_d19_maps_ds1000_from_one_sealed_suite_specific_sandbox_work_item(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)
    evidence = {
        "artifactPath": "s3://airflow-serp-evidence/serp-evals/op/sandbox-work-item.json",
        "artifactSha256": "sha256:" + "a" * 64,
        "artifactVersionId": "sandbox-work-item-version",
        "objectLockMode": "COMPLIANCE",
    }
    image_digest = "sha256:" + "b" * 64
    prepared = {
        "schema": "SandboxWorkItemSet/v1",
        "suiteId": "CodeRAG-Bench",
        "side": "candidate",
        "repetition": 1,
        "workItemSetEvidence": {
            **evidence,
            "artifactPath": "s3://airflow-serp-evidence/serp-evals/op/sandbox-work-items.json",
        },
        "workItems": [
            {
                "caseIdSha256": "sha256:" + "c" * 64,
                "executorArgs": ["/sandbox/input/ds1000_executor.py"],
                "executorCommand": "/opt/ds1000-venv/bin/python",
                "sandboxImageDigest": image_digest,
                "sandboxImageReference": "harbor.adapstory.com/serp/ds1000@" + image_digest,
                "sandboxWorkItemEvidence": evidence,
            }
        ],
    }

    mapped = module.build_code_sandbox_mapped_operator_kwargs(
        prepared,
        expected_suite_id="CodeRAG-Bench",
        expected_side="candidate",
        expected_repetition=1,
        trusted_runtime_image="harbor.adapstory.com/adapstory/airflow@sha256:" + "d" * 64,
    )

    assert len(mapped) == 1
    assert mapped[0]["arguments"][0] == "publish-code-sandbox-result"
    assert "/sandbox/output/raw-result.json" in mapped[0]["arguments"]
    pod = mapped[0]["pod_template_dict"]
    assert pod["spec"]["initContainers"][0]["image"].endswith("@sha256:" + "d" * 64)
    assert [container["name"] for container in pod["spec"]["containers"]] == ["base"]
    assert [container["name"] for container in pod["spec"]["initContainers"]] == [
        "stage-code-sandbox",
        "sandbox-executor",
    ]
    executor = pod["spec"]["initContainers"][1]
    stage = pod["spec"]["initContainers"][0]
    assert executor["image"] == "harbor.adapstory.com/serp/ds1000@" + image_digest
    assert executor["env"] == []
    assert executor["envFrom"] == []
    assert executor["command"] == ["/opt/ds1000-venv/bin/python"]
    assert executor["args"] == ["/sandbox/input/ds1000_executor.py"]
    assert executor["resources"]["requests"]["ephemeral-storage"] == "8Gi"
    assert executor["resources"]["limits"]["ephemeral-storage"] == "36Gi"
    assert stage["resources"]["requests"]["ephemeral-storage"] == "1Gi"
    assert stage["resources"]["limits"]["ephemeral-storage"] == "6Gi"
    assert "adapstory_serp_pipeline" not in json.dumps(executor, sort_keys=True)
    assert "/airflow/xcom" not in {mount["mountPath"] for mount in executor["volumeMounts"]}
    assert "/var/run/docker.sock" not in {mount["mountPath"] for mount in executor["volumeMounts"]}
    assert "/var/run/secrets/kubernetes.io/serviceaccount" not in {
        mount["mountPath"] for mount in executor["volumeMounts"]
    }
    assert "/var/run/secrets/kubernetes.io/serviceaccount" not in {
        mount["mountPath"] for mount in stage["volumeMounts"]
    }


def test_d19_maps_swe_bench_by_exact_instance_image_repo_and_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    def evidence(name: str, digest: str) -> dict[str, str]:
        return {
            "artifactPath": f"s3://airflow-serp-evidence/serp-evals/op/{name}.json",
            "artifactSha256": "sha256:" + digest * 64,
            "artifactVersionId": f"{name}-version",
            "objectLockMode": "COMPLIANCE",
        }

    first_digest = "sha256:" + "1" * 64
    second_digest = "sha256:" + "2" * 64
    prepared = {
        "schema": "SandboxWorkItemSet/v1",
        "suiteId": "SWE-bench Verified",
        "side": "baseline",
        "repetition": 5,
        "workItemSetEvidence": evidence("work-item-set", "a"),
        "workItems": [
            {
                "baseCommit": "3" * 40,
                "caseIdSha256": "sha256:" + "3" * 64,
                "executorArgs": ["/sandbox/input/swe_executor.sh"],
                "executorCommand": "/bin/bash",
                "repository": "django/django",
                "sandboxImageDigest": first_digest,
                "sandboxImageReference": "harbor.adapstory.com/serp/swe-django@" + first_digest,
                "sandboxWorkItemEvidence": evidence("django", "b"),
            },
            {
                "baseCommit": "4" * 40,
                "caseIdSha256": "sha256:" + "4" * 64,
                "executorArgs": ["/sandbox/input/swe_executor.sh"],
                "executorCommand": "/bin/bash",
                "repository": "pytest-dev/pytest",
                "sandboxImageDigest": second_digest,
                "sandboxImageReference": "harbor.adapstory.com/serp/swe-pytest@" + second_digest,
                "sandboxWorkItemEvidence": evidence("pytest", "c"),
            },
        ],
    }

    mapped = module.build_code_sandbox_mapped_operator_kwargs(
        prepared,
        expected_suite_id="SWE-bench Verified",
        expected_side="baseline",
        expected_repetition=5,
        trusted_runtime_image="harbor.adapstory.com/adapstory/airflow@sha256:" + "d" * 64,
    )

    executors = [item["pod_template_dict"]["spec"]["initContainers"][1] for item in mapped]
    assert [executor["image"] for executor in executors] == [
        "harbor.adapstory.com/serp/swe-django@" + first_digest,
        "harbor.adapstory.com/serp/swe-pytest@" + second_digest,
    ]
    assert all(executor["command"] == ["/bin/bash"] for executor in executors)
    assert all(executor["args"] == ["/sandbox/input/swe_executor.sh"] for executor in executors)
    assert all("adapstory_serp_pipeline" not in json.dumps(executor) for executor in executors)
    assert all(
        executor["securityContext"]["readOnlyRootFilesystem"] is True for executor in executors
    )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda payload: payload.update(workItems=[]), "inventory is required"),
        (
            lambda payload: payload["workItems"][0].update(
                sandboxImageReference="harbor.adapstory.com/serp/ds1000:latest"
            ),
            "immutable digest",
        ),
        (
            lambda payload: payload["workItems"][0]["sandboxWorkItemEvidence"].update(
                objectLockMode="GOVERNANCE"
            ),
            "COMPLIANCE",
        ),
        (
            lambda payload: payload["workItems"][0].update(executorCommand="/bin/sh"),
            "executor command",
        ),
        (
            lambda payload: payload["workItems"][0].update(executorArgs=["-c", "id"]),
            "executor arguments",
        ),
    ],
)
def test_d19_sandbox_fanout_fails_closed_without_exact_inventory(
    monkeypatch: pytest.MonkeyPatch,
    mutation: Any,
    message: str,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)
    image_digest = "sha256:" + "b" * 64
    prepared = {
        "schema": "SandboxWorkItemSet/v1",
        "suiteId": "CodeRAG-Bench",
        "side": "candidate",
        "repetition": 1,
        "workItemSetEvidence": {
            "artifactPath": "s3://airflow-serp-evidence/serp-evals/op/set.json",
            "artifactSha256": "sha256:" + "a" * 64,
            "artifactVersionId": "set-version",
            "objectLockMode": "COMPLIANCE",
        },
        "workItems": [
            {
                "caseIdSha256": "sha256:" + "c" * 64,
                "executorArgs": ["/sandbox/input/ds1000_executor.py"],
                "executorCommand": "/opt/ds1000-venv/bin/python",
                "sandboxImageDigest": image_digest,
                "sandboxImageReference": "harbor.adapstory.com/serp/ds1000@" + image_digest,
                "sandboxWorkItemEvidence": {
                    "artifactPath": "s3://airflow-serp-evidence/serp-evals/op/item.json",
                    "artifactSha256": "sha256:" + "d" * 64,
                    "artifactVersionId": "item-version",
                    "objectLockMode": "COMPLIANCE",
                },
            }
        ],
    }
    mutation(prepared)

    with pytest.raises(ValueError, match=message):
        module.build_code_sandbox_mapped_operator_kwargs(
            prepared,
            expected_suite_id="CodeRAG-Bench",
            expected_side="candidate",
            expected_repetition=1,
            trusted_runtime_image=("harbor.adapstory.com/adapstory/airflow@sha256:" + "d" * 64),
        )


def test_d19_aggregates_every_mapped_swe_result_into_one_worm_seal_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    def evidence(name: str, digest: str) -> dict[str, str]:
        return {
            "artifactPath": f"s3://airflow-serp-evidence/serp-evals/op/{name}.json",
            "artifactSha256": "sha256:" + digest * 64,
            "artifactVersionId": f"{name}-version",
            "objectLockMode": "COMPLIANCE",
        }

    prepared = {
        "schema": "SandboxWorkItemSet/v1",
        "suiteId": "SWE-bench Verified",
        "side": "candidate",
        "repetition": 2,
        "workItemSetEvidence": evidence("work-item-set", "a"),
        "workItems": [
            {
                "baseCommit": "1" * 40,
                "caseIdSha256": "sha256:" + "1" * 64,
                "executorArgs": ["/sandbox/input/swe_executor.sh"],
                "executorCommand": "/bin/bash",
                "repository": "django/django",
                "sandboxImageDigest": "sha256:" + "3" * 64,
                "sandboxImageReference": (
                    "harbor.adapstory.com/serp/swe-django@sha256:" + "3" * 64
                ),
                "sandboxWorkItemEvidence": evidence("work-item-django", "b"),
            },
            {
                "baseCommit": "2" * 40,
                "caseIdSha256": "sha256:" + "2" * 64,
                "executorArgs": ["/sandbox/input/swe_executor.sh"],
                "executorCommand": "/bin/bash",
                "repository": "pytest-dev/pytest",
                "sandboxImageDigest": "sha256:" + "4" * 64,
                "sandboxImageReference": (
                    "harbor.adapstory.com/serp/swe-pytest@sha256:" + "4" * 64
                ),
                "sandboxWorkItemEvidence": evidence("work-item-pytest", "c"),
            },
        ],
    }
    results = [
        {
            "caseIdSha256": "sha256:" + "1" * 64,
            "sandboxResultEvidence": evidence("result-django", "d"),
        },
        {
            "caseIdSha256": "sha256:" + "2" * 64,
            "sandboxResultEvidence": evidence("result-pytest", "e"),
        },
    ]
    captured: list[dict[str, Any]] = []

    def snapshot_writer(
        artifact_path: str,
        *,
        artifact_type: str,
        operation_id: str,
        payload: Mapping[str, Any],
    ) -> dict[str, str]:
        captured.append(
            {
                "artifactPath": artifact_path,
                "artifactType": artifact_type,
                "operationId": operation_id,
                "payload": dict(payload),
            }
        )
        return evidence("result-set-plan", "f")

    monkeypatch.setattr(module, "write_immutable_evidence_snapshot", snapshot_writer)
    plan = {
        "dag_id": "serp_benchmark_improvement_wave",
        "operation_id": "op",
        "artifact_paths": {
            "paired_evaluation_assembly_plan": (
                "s3://airflow-serp-evidence/serp-evals/op/paired-assembly.json"
            )
        },
    }

    assembled = module.write_code_sandbox_result_set_assembly_plan(
        plan,
        prepared,
        results,
        expected_suite_id="SWE-bench Verified",
        expected_side="candidate",
        expected_repetition=2,
    )

    assert assembled == {
        "resultCount": 2,
        "resultSetPlanEvidence": evidence("result-set-plan", "f"),
        "repetition": 2,
        "side": "candidate",
        "suiteId": "SWE-bench Verified",
    }
    assert captured[0]["artifactType"] == "sandbox_result_set_assembly_plan"
    payload = captured[0]["payload"]
    assert payload["schema"] == "SandboxResultSetAssemblyPlan/v1"
    assert [item["caseIdSha256"] for item in payload["results"]] == [
        "sha256:" + "1" * 64,
        "sha256:" + "2" * 64,
    ]
    assert [item["sandboxResultEvidence"] for item in payload["results"]] == [
        evidence("result-django", "d"),
        evidence("result-pytest", "e"),
    ]


def test_d19_model_runner_has_only_minio_and_bc10_runtime_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_MCP_GATEWAY_BASE_URL": (
            "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
        ),
        "ADAPSTORY_BC10_GATEWAY_URL": ("http://serp-ai-orchestration.serp.svc.cluster.local:8080"),
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    values = {
        item.kwargs["name"]: item.kwargs.get("value") for item in module.d19_model_runner_env_vars()
    }
    assert values["ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST"] == "sha256:" + "d" * 64
    assert values["ADAPSTORY_BC10_GATEWAY_URL"] == (
        "http://serp-ai-orchestration.serp.svc.cluster.local:8080"
    )
    assert values["ADAPSTORY_BC10_TOKEN_PATH"] == ("/var/run/secrets/adapstory/bc10-workload/token")
    assert "ADAPSTORY_OLLAMA_BASE_URL" not in values
    assert "ADAPSTORY_SERP_MCP_GATEWAY_BASE_URL" not in values
    assert "ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH" not in values
    assert "ADAPSTORY_SERP_BC21_BASE_URL" not in values
    assert not any("QDRANT" in name or "OPENSEARCH" in name or "NEO4J" in name for name in values)

    volume_names = {item.kwargs["name"] for item in module.D19_MODEL_RUNNER_VOLUMES}
    mount_paths = {item.kwargs["mount_path"] for item in module.D19_MODEL_RUNNER_VOLUME_MOUNTS}
    assert "bc10-workload-token" in volume_names
    assert "/var/run/secrets/adapstory/bc10-workload" in mount_paths


def test_airflow_model_callers_have_no_direct_provider_runtime_contract() -> None:
    for filename in (
        "serp_benchmark_improvement_wave.py",
        "serp_web_seed_crawl_refresh.py",
    ):
        source = (REPO_ROOT / "dags" / filename).read_text(encoding="utf-8")
        assert "ADAPSTORY_OLLAMA_BASE_URL" not in source
        assert "ADAPSTORY_SERP_EMBEDDING_URL" not in source


def test_d19_assembly_plan_seals_exact_canonical_ninety_without_scores(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    for name, value in {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT": "http://minio:9000",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION": "us-east-1",
        "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST": "sha256:" + "d" * 64,
        "ADAPSTORY_SERP_BC21_BASE_URL": "http://context-platform:8080/api/bc-21/serp/v1",
        "ADAPSTORY_SERP_MCP_GATEWAY_BASE_URL": (
            "http://prod-serp-mcp-gateway-svc.env-prod.svc.cluster.local:8000"
        ),
    }.items():
        monkeypatch.setenv(name, value)
    module = importlib.import_module("dags.serp_benchmark_improvement_wave")
    module = importlib.reload(module)

    def evidence(path: str, digest_character: str) -> dict[str, str]:
        return {
            "artifactPath": f"s3://airflow-serp-evidence/{path}.json",
            "artifactSha256": "sha256:" + digest_character * 64,
            "artifactVersionId": f"{path}-version",
            "objectLockMode": "COMPLIANCE",
        }

    captured: list[dict[str, Any]] = []

    def snapshot_writer(
        artifact_path: str,
        *,
        artifact_type: str,
        operation_id: str,
        payload: Mapping[str, Any],
    ) -> dict[str, str]:
        captured.append(
            {
                "artifactPath": artifact_path,
                "artifactType": artifact_type,
                "operationId": operation_id,
                "payload": dict(payload),
            }
        )
        return {
            **evidence("operation/paired-evaluation-assembly-plan", "d"),
            "artifactPath": artifact_path,
        }

    monkeypatch.setattr(module, "write_immutable_evidence_snapshot", snapshot_writer)
    work_items: list[dict[str, Any]] = []
    run_results: list[dict[str, Any]] = []
    for index, (suite_id, side, repetition) in enumerate(module.D19_OFFICIAL_HARNESS_WORK_ITEMS):
        identity = {"suiteId": suite_id, "side": side, "repetition": repetition}
        work_items.append(
            {
                **identity,
                "workItemEvidence": evidence(f"operation/work-items/{index}", "a"),
            }
        )
        run_results.append(
            {
                **identity,
                "receiptEvidence": evidence(f"operation/receipts/{index}", "b"),
            }
        )
    plan: dict[str, Any] = {
        "dag_id": "serp_benchmark_improvement_wave",
        "operation_id": "operation",
        "artifact_paths": {
            "paired_evaluation_assembly_plan": (
                "s3://airflow-serp-evidence/operation/paired-evaluation-assembly-plan.json"
            ),
            "paired_execution_manifest": (
                "s3://airflow-serp-evidence/operation/paired-execution-manifest.json"
            ),
        },
    }

    result = module.write_paired_evaluation_assembly_plan(
        json.dumps(plan),
        {"requestEvidence": evidence("operation/paired-eval-request", "c")},
        {"workItems": work_items},
        run_results,
    )

    assert result["runCount"] == 90
    assert result["manifestOutput"] == plan["artifact_paths"]["paired_execution_manifest"]
    assert captured[0]["artifactType"] == "paired_evaluation_assembly_plan"
    payload = captured[0]["payload"]
    assert payload["schema"] == "PairedEvaluationAssemblyPlan/v1"
    assert len(payload["runs"]) == 90
    assert list(payload["runs"][0]) == ["workItemEvidence", "receiptEvidence"]
    assert "score" not in json.dumps(payload).casefold()


def test_d19_assembly_manifest_is_server_owned_and_feeds_only_the_v2_aggregator() -> None:
    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")

    assert "materialize_official_harness_work_items = KubernetesPodOperator(" in source
    assert "assemble_paired_execution_manifest = KubernetesPodOperator(" in source
    assert '"materialize-work-items"' in source
    assert '"assemble-manifest"' in source
    assert '"--execution-manifest"' in source
    assert '"--execution-manifest-version-id"' in source
    assert '"--execution-manifest-sha256"' in source
    assert "write_paired_evaluation_assembly_plan" in source
    assert "candidate_evaluation" not in source


def test_web_identity_env_values_are_plain_strings_serializable_by_airflow() -> None:
    source = (REPO_ROOT / "dags" / "serp_evidence_workload_identity.py").read_text(encoding="utf-8")

    assert "from airflow.sdk import literal" not in source
    assert "value=native_template_safe_env_value(value.strip())" in source
    assert (
        "value=native_template_safe_env_value(str(MINIO_WEB_IDENTITY_EXPIRATION_SECONDS))" in source
    )


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


def _scheduled_d6_conf(
    *,
    generated_at: str = "2026-07-17T00:00:00Z",
) -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "s3://airflow-serp-evidence/serp-evals",
        "evaluation_release_promotion_evidence": _d19_worm_evidence(
            "model-releases/d17-promotion", "c"
        ),
        "generated_at": generated_at,
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
    }


def _scheduled_d6_airflow_run(
    *,
    logical_date: str = "2026-07-17T00:00:00Z",
) -> dict[str, str]:
    logical_at = datetime.fromisoformat(logical_date.replace("Z", "+00:00"))
    return {
        "dagId": "serp_nightly_regression_suite",
        "logicalDate": logical_at.isoformat().replace("+00:00", "Z"),
        "runId": f"scheduled__{logical_at.isoformat()}",
        "runType": "scheduled",
        "startDate": (logical_at + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
    }


def _scheduled_d6_history_client_result(
    parent_run: Mapping[str, str],
    *,
    runs: list[dict[str, str]] | None = None,
    accepted_verifications: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    history_runs = _scheduled_d6_prior_runs() if runs is None else runs
    pointers = (
        [_scheduled_d6_prior_pointer(index, run) for index, run in enumerate(history_runs, start=1)]
        if accepted_verifications is None
        else accepted_verifications
    )
    return {
        "activeRunQuery": {
            "dagId": "serp_benchmark_improvement_wave",
            "states": ["queued", "running"],
            "totalEntries": 0,
        },
        "api": {
            "apiVersion": "v2",
            "airflowVersion": "3.3.0",
            "serverAuthority": "airflow-api-server.airflow.svc.cluster.local:8080",
        },
        "acceptedRunVerifications": pointers,
        "pagination": {
            "complete": True,
            "observedEntries": len(history_runs),
            "pageCount": 1 if history_runs else 0,
            "pageLimit": 100,
            "strategy": "complete",
            "tailStartOffset": 0,
            "totalEntries": len(history_runs),
        },
        "query": {
            "apiPath": "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns",
            "dagId": "serp_benchmark_improvement_wave",
            "logicalDateLt": parent_run["logicalDate"],
            "orderBy": ["logical_date", "run_id"],
        },
        "runs": history_runs,
        "verificationPointerQuery": {
            "apiPathTemplate": (
                "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns/{dagRunId}/"
                "taskInstances/persist_paired_evaluation_verification_evidence/"
                "xcomEntries/return_value"
            ),
            "deserialize": True,
            "mapIndex": -1,
            "stringify": False,
            "taskId": "persist_paired_evaluation_verification_evidence",
            "xcomKey": "return_value",
        },
    }


def _scheduled_d6_prior_runs() -> list[dict[str, str]]:
    return [
        {
            "dagId": "serp_benchmark_improvement_wave",
            "logicalDate": f"2026-07-{13 + index:02d}T00:00:00Z",
            "runId": f"manual__prior-{index}",
            "runType": "manual",
            "state": "success",
        }
        for index in range(1, 4)
    ]


def _scheduled_d6_prior_pointer(
    index: int,
    run: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "airflowRun": {key: run[key] for key in ("dagId", "logicalDate", "runId", "runType")},
        "observedNormalizedScoreCellsEvidence": _d19_worm_evidence(
            f"score-cells/prior-{index}", format((index + 9) % 16, "x")
        ),
        "pairedEvaluationVerificationEvidence": _d19_worm_evidence(
            f"verification/prior-{index}", str(index)
        ),
        "receiptStatus": "accepted",
        "requestId": _d6_request_id(index),
    }


def _d6_request_id(index: int) -> str:
    return f"00000000-0000-4000-a000-{index:012d}"


def _scheduled_d6_fence(parent_run: Mapping[str, str]) -> dict[str, Any]:
    parent_start = datetime.fromisoformat(parent_run["startDate"].replace("Z", "+00:00"))
    acquired_at = parent_start + timedelta(seconds=4)
    return {
        "acquiredAt": acquired_at.isoformat().replace("+00:00", "Z"),
        "expiresAt": (acquired_at + timedelta(hours=12)).isoformat().replace("+00:00", "Z"),
        "holderIdentity": f"d6:{parent_run['runId']}",
        "leaseDurationSeconds": 43_200,
        "leaseName": "serp-d19-history-fence",
        "namespace": "airflow",
        "parentDagId": parent_run["dagId"],
        "parentRunId": parent_run["runId"],
        "resourceVersion": "812345",
        "schema": "D19HistoryFence/v1",
    }


class _StaticD6HistoryClient:
    def __init__(self, result: Mapping[str, Any]) -> None:
        self.result = result

    def collect(self, *, parent_logical_date: str) -> dict[str, Any]:
        assert parent_logical_date == _scheduled_d6_airflow_run()["logicalDate"]
        return cast(dict[str, Any], json.loads(json.dumps(self.result)))


class _SequenceD6HistoryClient:
    def __init__(self, *results: Mapping[str, Any]) -> None:
        self.results = list(results)
        self.collect_count = 0

    def collect(self, *, parent_logical_date: str) -> dict[str, Any]:
        assert parent_logical_date == _scheduled_d6_airflow_run()["logicalDate"]
        result = self.results[self.collect_count]
        self.collect_count += 1
        return cast(dict[str, Any], json.loads(json.dumps(result)))


class _D6FenceClient:
    def __init__(self, fence: Mapping[str, Any]) -> None:
        self.fence = dict(fence)
        self.required: list[dict[str, Any]] = []
        self.released: list[dict[str, Any]] = []

    def acquire(self, *, parent_airflow_run: Mapping[str, str]) -> dict[str, Any]:
        assert parent_airflow_run == _scheduled_d6_airflow_run()
        return dict(self.fence)

    def require_active(self, fence: Mapping[str, Any]) -> dict[str, Any]:
        self.required.append(dict(fence))
        return dict(self.fence)

    def release(self, fence: Mapping[str, Any]) -> None:
        self.released.append(dict(fence))


def _d6_write_receipt(
    path: str,
    payload: bytes,
    *,
    artifact_type: str,
) -> dict[str, Any]:
    return {
        "artifactETag": sha256(payload).hexdigest(),
        "artifactPath": path,
        "artifactSha256": sha256(payload).hexdigest(),
        "artifactType": artifact_type,
        "artifactVersionId": f"version-{artifact_type}",
        "objectLockMode": "COMPLIANCE",
        "objectLockRetainUntil": "2033-07-17T00:00:00Z",
        "retainUntil": "2033-07-17T00:00:00Z",
        "status": "written",
    }


def _d6_worm_from_write_receipt(receipt: Mapping[str, Any]) -> dict[str, str]:
    return {
        "objectLockMode": str(receipt["objectLockMode"]),
        "retainUntil": str(receipt["objectLockRetainUntil"]),
        "s3Uri": str(receipt["artifactPath"]),
        "sha256": "sha256:" + str(receipt["artifactSha256"]),
        "versionId": str(receipt["artifactVersionId"]),
    }


def _d6_history_attestation_verification(
    *,
    subject: Mapping[str, str],
    attestation: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "attestationEvidence": dict(attestation),
        "consumerVerification": {
            "requestId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "valid": True,
        },
        "purpose": "serp-d19-run-history-observation",
        "signer": {
            "authMethod": "kubernetes",
            "authMount": "auth/kubernetes",
            "authRole": "serp-d19-history-observer-attestor-role",
            "entityId": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            "loginRequestId": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            "serviceAccountName": "airflow-serp-d19-history-observer",
            "serviceAccountNamespace": "airflow",
            "serviceAccountUid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            "tokenAudience": "vault",
            "tokenPolicy": "serp-d19-history-observer-attestor",
        },
        "statementSha256": "sha256:" + "a" * 64,
        "subject": dict(subject),
        "transit": {
            "key": "serp-d19-history-observation",
            "keyVersion": 1,
            "signature": "vault:v1:d19-history-observation",
            "verifyRequestId": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        },
    }


def _d6_history_attestation_evidence(plan: Any) -> dict[str, str]:
    return {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2033-07-17T00:00:00Z",
        "s3Uri": plan.payload["artifact_paths"]["d19_run_history_observation_attestation"],
        "sha256": "sha256:" + "f" * 64,
        "versionId": "version-d19-history-observation-attestation",
    }


def _scheduled_d6_receipt_fixture(
    *,
    generated_at: str = "2026-07-17T00:00:00Z",
    prior_runs: list[dict[str, str]] | None = None,
    prior_pointers: list[dict[str, Any]] | None = None,
    seed_objects: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
    current_request_index: int = 4,
) -> dict[str, Any]:
    plan = build_nightly_regression_plan(_scheduled_d6_conf(generated_at=generated_at))
    parent_run = _scheduled_d6_airflow_run(logical_date=generated_at)
    fence = _scheduled_d6_fence(parent_run)
    prior_run_values = _scheduled_d6_prior_runs() if prior_runs is None else prior_runs
    history = _scheduled_d6_history_client_result(
        parent_run,
        runs=prior_run_values,
        accepted_verifications=prior_pointers,
    )
    parent_start = datetime.fromisoformat(parent_run["startDate"].replace("Z", "+00:00"))
    history_observation = {
        **history,
        "fence": fence,
        "generatedAt": (parent_start + timedelta(seconds=9)).isoformat().replace("+00:00", "Z"),
        "parentAirflowRun": parent_run,
        "producer": {
            "namespace": "airflow",
            "serviceAccount": "airflow-serp-d19-history-observer",
        },
        "schema": "D19RunHistoryObservation/v2",
    }
    history_handle = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2033-07-17T00:00:00Z",
        "s3Uri": plan.payload["artifact_paths"]["d19_run_history_observation"],
        "sha256": "sha256:" + "a" * 64,
        "versionId": "version-d19-history-observation",
    }
    history_attestation_handle = _d6_history_attestation_evidence(plan)
    history_verification = _d6_history_attestation_verification(
        subject=history_handle,
        attestation=history_attestation_handle,
    )
    history_attestation = {
        "domain": "serp.adapstory.ai/evaluation-governance/v1",
        "purpose": "serp-d19-run-history-observation",
        "schema": "ArtifactSignatureAttestationReceipt/v2",
        "signatureProvider": "vault-transit",
        "signer": history_verification["signer"],
        "statementSha256": history_verification["statementSha256"],
        "subject": history_handle,
        "transit": {
            "hashAlgorithm": "sha2-256",
            "key": "serp-d19-history-observation",
            "keyType": "ecdsa-p256",
            "keyVersion": 1,
            "mount": "transit",
            "prehashed": False,
            "signRequestId": "ffffffff-ffff-4fff-8fff-ffffffffffff",
            "signature": "vault:v1:d19-history-observation",
            "signatureMarshalingAlgorithm": "asn1",
            "verificationValid": True,
            "verifyRequestId": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
        },
    }
    promotion = plan.payload["evaluation_release_promotion_evidence"]
    candidate = _d19_worm_evidence("model-releases/candidate", "d")
    objective = _d19_worm_evidence("evaluation-objective", "e")
    objects: dict[tuple[str, str], dict[str, Any]] = {
        **(
            {
                key: cast(dict[str, Any], json.loads(json.dumps(value)))
                for key, value in seed_objects.items()
            }
            if seed_objects is not None
            else {}
        ),
        (history_handle["s3Uri"], history_handle["versionId"]): history_observation,
        (
            history_attestation_handle["s3Uri"],
            history_attestation_handle["versionId"],
        ): history_attestation,
        (promotion["s3Uri"], promotion["versionId"]): _d6_d17_promotion_receipt(
            plan,
            candidate=candidate,
            objective=objective,
        ),
    }
    prior_receipt_handles: list[dict[str, str]] = []
    for index, (verification_pointer, airflow_run) in enumerate(
        zip(history["acceptedRunVerifications"], prior_run_values, strict=True),
        start=1,
    ):
        verification_handle = verification_pointer["pairedEvaluationVerificationEvidence"]
        score_cells_handle = verification_pointer["observedNormalizedScoreCellsEvidence"]
        verification_key = (verification_handle["s3Uri"], verification_handle["versionId"])
        if verification_key in objects:
            prior_receipt_handles.append(
                dict(objects[verification_key]["receiptPointer"]["receiptEvidence"])
            )
            continue
        receipt_handle = _d19_worm_evidence(f"receipts/prior-{index}", str(index + 3))
        attestation_handle = _d19_worm_evidence(
            f"receipts/prior-{index}.attestation", str(index + 6)
        )
        request_id = verification_pointer["requestId"]
        objects[verification_key] = _d6_paired_verification_evidence(
            airflow_run={
                key: airflow_run[key] for key in ("dagId", "logicalDate", "runId", "runType")
            },
            request_id=request_id,
            receipt_handle=receipt_handle,
            receipt_attestation_handle=attestation_handle,
            score_cells_handle=score_cells_handle,
            request_uuid_suffix=int(str(request_id)[-12:]),
        )
        objects[(receipt_handle["s3Uri"], receipt_handle["versionId"])] = _d6_v9_receipt(
            request_id=request_id,
            promotion=promotion,
            candidate=candidate,
            objective=objective,
        )
        objects[(attestation_handle["s3Uri"], attestation_handle["versionId"])] = {
            "schema": "ArtifactSignatureAttestationReceipt/v2"
        }
        objects[(score_cells_handle["s3Uri"], score_cells_handle["versionId"])] = (
            serp_eval_contracts_module._observed_normalized_score_cells_payload(
                objects[(receipt_handle["s3Uri"], receipt_handle["versionId"])],
                operation_id=request_id,
                receipt_evidence=receipt_handle,
                receipt_attestation_evidence=attestation_handle,
                receipt_status="accepted",
            )
        )
        prior_receipt_handles.append(receipt_handle)

    child_run = {
        "dagId": "serp_benchmark_improvement_wave",
        "logicalDate": parent_run["logicalDate"],
        "runId": f"d6__{parent_run['runId']}",
        "runType": "manual",
    }
    current_verification_handle = _d19_worm_evidence(
        f"verification/current-{current_request_index}",
        format(current_request_index % 16, "x"),
    )
    current_receipt_handle = _d19_worm_evidence(
        f"receipts/current-{current_request_index}",
        format((current_request_index + 4) % 16, "x"),
    )
    current_attestation_handle = _d19_worm_evidence(
        f"receipts/current-{current_request_index}.attestation",
        format((current_request_index + 8) % 16, "x"),
    )
    current_score_cells_handle = _d19_worm_evidence(
        f"score-cells/current-{current_request_index}",
        format((current_request_index + 12) % 16, "x"),
    )
    current_request_id = _d6_request_id(current_request_index)
    objects[(current_verification_handle["s3Uri"], current_verification_handle["versionId"])] = (
        _d6_paired_verification_evidence(
            airflow_run=child_run,
            request_id=current_request_id,
            receipt_handle=current_receipt_handle,
            receipt_attestation_handle=current_attestation_handle,
            score_cells_handle=current_score_cells_handle,
            request_uuid_suffix=current_request_index,
        )
    )
    objects[(current_receipt_handle["s3Uri"], current_receipt_handle["versionId"])] = (
        _d6_v9_receipt(
            request_id=current_request_id,
            promotion=promotion,
            candidate=candidate,
            objective=objective,
        )
    )
    objects[(current_attestation_handle["s3Uri"], current_attestation_handle["versionId"])] = {
        "schema": "ArtifactSignatureAttestationReceipt/v2"
    }
    objects[(current_score_cells_handle["s3Uri"], current_score_cells_handle["versionId"])] = (
        serp_eval_contracts_module._observed_normalized_score_cells_payload(
            objects[(current_receipt_handle["s3Uri"], current_receipt_handle["versionId"])],
            operation_id=current_request_id,
            receipt_evidence=current_receipt_handle,
            receipt_attestation_evidence=current_attestation_handle,
            receipt_status="accepted",
        )
    )

    def reader(evidence: Mapping[str, str], _field_name: str) -> dict[str, Any]:
        key = (evidence["s3Uri"], evidence["versionId"])
        if key not in objects:
            raise ValueError("fixture WORM object is missing")
        return cast(dict[str, Any], json.loads(json.dumps(objects[key])))

    return {
        "child_run": child_run,
        "current_observation": {
            "dagId": "serp_benchmark_improvement_wave",
            "logicalDate": parent_run["logicalDate"],
            "observedAt": (
                datetime.fromisoformat(parent_run["logicalDate"].replace("Z", "+00:00"))
                + timedelta(hours=4)
                - timedelta(seconds=1)
            )
            .isoformat()
            .replace("+00:00", "Z"),
            "runId": child_run["runId"],
            "sameLogicalDateRunCount": 1,
            "sameLogicalDateSuccessCount": 1,
            "schema": "D19CurrentRunObservation/v1",
            "state": "success",
        },
        "current_receipt_key": (
            current_receipt_handle["s3Uri"],
            current_receipt_handle["versionId"],
        ),
        "current_verification_key": (
            current_verification_handle["s3Uri"],
            current_verification_handle["versionId"],
        ),
        "history_observation_key": (history_handle["s3Uri"], history_handle["versionId"]),
        "history_result": {
            "d19RunHistoryObservationAttestationEvidence": history_attestation_handle,
            "d19RunHistoryObservationEvidence": history_handle,
            "d19RunHistoryObservationVerification": history_verification,
            "d19TriggerConf": {**plan.payload["d19_trigger_conf"], "scheduled_d6_fence": fence},
            "fence": fence,
        },
        "objects": objects,
        "plan": plan,
        "prior_receipt_handles": prior_receipt_handles,
        "reader": reader,
        "triggered_verification": {
            "airflowRun": child_run,
            "observedNormalizedScoreCellsEvidence": current_score_cells_handle,
            "pairedEvaluationVerificationEvidence": current_verification_handle,
            "receiptStatus": "accepted",
            "requestId": current_request_id,
        },
    }


def _d6_paired_verification_evidence(
    *,
    airflow_run: Mapping[str, str],
    request_id: str,
    receipt_handle: Mapping[str, str],
    receipt_attestation_handle: Mapping[str, str],
    score_cells_handle: Mapping[str, str],
    request_uuid_suffix: int,
) -> dict[str, Any]:
    request_uuid = f"00000000-0000-4000-8000-{request_uuid_suffix:012d}"
    transit_uuid = f"10000000-0000-4000-8000-{request_uuid_suffix:012d}"
    return {
        "airflowRun": dict(airflow_run),
        "observedNormalizedScoreCellsEvidence": dict(score_cells_handle),
        "operationId": request_id,
        "receiptPointer": {
            "receiptAttestationEvidence": dict(receipt_attestation_handle),
            "receiptEvidence": dict(receipt_handle),
            "receiptStatus": "accepted",
            "receiptVerification": {
                "attestationEvidence": dict(receipt_attestation_handle),
                "consumerVerification": {"requestId": request_uuid, "valid": True},
                "purpose": "serp-paired-evaluation-final-receipt",
                "signer": {"serviceAccountName": "airflow-serp-benchmark-aggregator"},
                "statementSha256": "sha256:" + "1" * 64,
                "subject": dict(receipt_handle),
                "transit": {
                    "key": "serp-evaluation-runtime",
                    "keyVersion": 1,
                    "signature": "vault:v1:paired-receipt",
                    "verifyRequestId": transit_uuid,
                },
            },
        },
        "requestId": request_id,
        "schema": "PairedEvaluationVerificationEvidence/v2",
    }


def _d6_v9_receipt(
    *,
    request_id: str,
    promotion: Mapping[str, str],
    candidate: Mapping[str, str],
    objective: Mapping[str, str],
) -> dict[str, Any]:
    return {
        "attestationVerifications": {},
        "baselineReleaseEvidence": _d19_worm_evidence("model-releases/baseline", "2"),
        "candidateReleaseEvidence": dict(candidate),
        "contractVersion": "serp-paired-eval-receipt/v9",
        "evaluationBindingEvidence": _d19_worm_evidence("evaluation-binding", "3"),
        "evaluationBindingId": "018f5e13-2d73-7a77-a052-8d1bcbf96701",
        "evaluationObjectiveAttestationEvidence": _d19_worm_evidence(
            "evaluation-objective.attestation", "4"
        ),
        "evaluationObjectiveEvidence": dict(objective),
        "evaluationReleasePromotionEvidence": dict(promotion),
        "metricCompatibilityMatrixEvidence": _d19_worm_evidence("metric-matrix", "5"),
        "pairedEvaluation": _d19_observed_paired_evaluation(request_id),
        "requestEvidence": _d19_worm_evidence(f"requests/{request_id}", "6"),
        "requestId": request_id,
        "status": "accepted",
    }


def _d6_d17_promotion_receipt(
    plan: Any,
    *,
    candidate: Mapping[str, str],
    objective: Mapping[str, str],
) -> dict[str, Any]:
    baseline = _d19_worm_evidence("model-releases/baseline", "2")
    candidate_digest = "sha256:" + "d" * 64
    return {
        "baselineRelease": {
            "evidence": baseline,
            "releaseDigest": "sha256:" + "b" * 64,
        },
        "candidateRelease": {
            "evidence": dict(candidate),
            "releaseDigest": candidate_digest,
        },
        "candidateReleaseAuthority": {
            "canaryState": "passed",
            "evidence": dict(candidate),
            "modelId": "serp-all-nine-candidate-router@2026.07.3",
            "provider": "adapstory-model-gateway",
            "purpose": "serp-benchmark-candidate",
            "releaseDigest": candidate_digest,
            "releaseId": "serp-all-nine-candidate@2026.07.3",
        },
        "dagId": "serp_model_catalog_promotion",
        "evaluationObjectiveAttestationEvidence": _d19_worm_evidence(
            "evaluation-objective.attestation", "4"
        ),
        "evaluationObjectiveEvidence": dict(objective),
        "generatedAt": "2026-07-12T00:00:00Z",
        "metricCompatibilityMatrixEvidence": _d19_worm_evidence("metric-matrix", "5"),
        "operationId": "serp-model-promotion-test",
        "promotionId": "serp-model-promotion-2026-07-12",
        "registryResourceId": plan.payload["registry_resource_id"],
        "registryResourceType": plan.payload["registry_resource_type"],
        "schema": "EvaluationReleasePromotionReceipt/v8",
        "evaluationReleaseContractVersion": "serp-ci-evaluation-release-evidence/v8",
        "status": "approved-for-evaluation",
        "tenantId": plan.payload["tenant_id"],
    }


def _nightly_worm_evidence(label: str) -> dict[str, str]:
    return {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2033-07-05T21:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/serp-evals/governed/{label}.json",
        "sha256": "sha256:" + sha256(label.encode("utf-8")).hexdigest(),
        "versionId": f"version-{label}",
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
        "evaluation_release_promotion_evidence": _d19_worm_evidence(
            "model-releases/d17-promotion", "c"
        ),
        "generated_at": "2026-07-05T21:00:00Z",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
    }


def _d19_worm_evidence(name: str, digest: str) -> dict[str, str]:
    return {
        "s3Uri": f"s3://airflow-serp-evidence/serp-evals/{name}.json",
        "sha256": "sha256:" + digest * 64,
        "versionId": f"{name.replace('/', '-')}-version-001",
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
    }


def _d19_evaluation_objective_v6() -> dict[str, Any]:
    reference_set_evidence = _d19_worm_evidence("evaluation-reference-set", "6")
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
                "referenceEvidence": _d19_worm_evidence(
                    f"evaluation-reference/{suite_id.casefold().replace(' ', '-')}",
                    format(len(cells), "x"),
                ),
                "referenceScore": 0.9,
                "suiteId": suite_id,
            }
        )
    objective: dict[str, Any] = {
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
        "referenceSetAttestationEvidence": _d19_worm_evidence(
            "evaluation-reference-set-attestation", "7"
        ),
        "requiredConsecutiveAcceptedEvaluations": 3,
        "schema": "EvaluationObjective/v6",
    }
    objective["version"] = (
        "serp-all-nine-quality-"
        + sha256(serp_eval_contracts_module._canonical_json(objective).encode("utf-8")).hexdigest()
        + ".v6"
    )
    return objective


def _d19_airflow_run() -> dict[str, str]:
    return {
        "dagId": "serp_benchmark_improvement_wave",
        "logicalDate": "2026-07-16T21:00:00Z",
        "runId": "manual__serp-all-nine-d19-wave-01-20260716T210000Z",
        "runType": "manual",
    }


def _d19_evaluator_result(
    plan: Any,
) -> tuple[dict[str, Any], dict[tuple[str, str], bytes]]:
    receipt_payload = {
        "attestationVerifications": {
            "evaluationObjective": _d19_verification_descriptor(
                "evaluation-objective",
                purpose="serp-evaluation-objective",
                consumer_request_id="11111111-1111-4111-8111-111111111111",
                transit_request_id="22222222-2222-4222-8222-222222222222",
            ),
            "evaluationReferenceSet": _d19_verification_descriptor(
                "evaluation-reference-set",
                purpose="serp-evaluation-reference-set",
                consumer_request_id="33333333-3333-4333-8333-333333333333",
                transit_request_id="44444444-4444-4444-8444-444444444444",
            ),
            "executionManifest": _d19_verification_descriptor(
                "execution-manifest",
                purpose="serp-evaluation-execution-manifest",
                consumer_request_id="55555555-5555-4555-8555-555555555555",
                transit_request_id="66666666-6666-4666-8666-666666666666",
            ),
        },
        "contractVersion": "serp-paired-eval-receipt/v9",
        "pairedEvaluation": _d19_observed_paired_evaluation(plan.payload["operation_id"]),
        "requestId": plan.payload["operation_id"],
        "status": "accepted",
    }
    receipt_bytes = serp_eval_contracts_module._canonical_json(receipt_payload).encode("utf-8")
    receipt_path = plan.payload["artifact_paths"]["paired_eval_receipt"]
    receipt_version = "paired-receipt-version-001"
    retain_until = "2033-07-16T21:00:00Z"
    receipt_evidence = {
        "artifactETag": sha256(receipt_bytes).hexdigest(),
        "artifactPath": receipt_path,
        "artifactSha256": sha256(receipt_bytes).hexdigest(),
        "artifactType": "serp_paired_eval_receipt",
        "artifactVersionId": receipt_version,
        "objectLockMode": "COMPLIANCE",
        "objectLockRetainUntil": retain_until,
        "status": "written",
    }
    receipt_subject = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": retain_until,
        "s3Uri": receipt_path,
        "sha256": "sha256:" + sha256(receipt_bytes).hexdigest(),
        "versionId": receipt_version,
    }
    attestation_payload = {"statement": "runtime-signed-final-receipt"}
    attestation_bytes = serp_eval_contracts_module._canonical_json(attestation_payload).encode(
        "utf-8"
    )
    attestation_path = receipt_path.removesuffix(".json") + ".attestation.json"
    attestation_evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": retain_until,
        "s3Uri": attestation_path,
        "sha256": "sha256:" + sha256(attestation_bytes).hexdigest(),
        "versionId": "paired-receipt-attestation-version-001",
    }
    verification = {
        "attestationEvidence": dict(attestation_evidence),
        "consumerVerification": {
            "requestId": "88888888-8888-4888-8888-888888888888",
            "valid": True,
        },
        "purpose": "serp-paired-evaluation-final-receipt",
        "signer": {
            "namespace": "env-prod",
            "serviceAccount": "airflow-serp-benchmark-aggregator",
        },
        "statementSha256": "sha256:" + "a" * 64,
        "subject": dict(receipt_subject),
        "transit": {
            "key": "serp-evaluation-runtime",
            "keyVersion": 1,
            "signature": "vault:v1:test-signature",
            "verifyRequestId": "99999999-9999-4999-8999-999999999999",
        },
    }
    return (
        {
            "receiptAttestationEvidence": attestation_evidence,
            "receiptEvidence": receipt_evidence,
            "receiptStatus": "accepted",
            "receiptVerification": verification,
        },
        {
            (receipt_path, receipt_version): receipt_bytes,
            (attestation_path, attestation_evidence["versionId"]): attestation_bytes,
        },
    )


def _d19_rejected_evaluator_result(
    plan: Any,
    *,
    candidate_normalized_lcb95: float = 0.8,
) -> tuple[dict[str, Any], dict[tuple[str, str], bytes]]:
    """Build a fully bound rejected D19 receipt, without inventing a score."""

    evaluator_result, objects = _d19_evaluator_result(plan)
    result = json.loads(json.dumps(evaluator_result))
    receipt_evidence = cast(dict[str, Any], result["receiptEvidence"])
    receipt_path = str(receipt_evidence["artifactPath"])
    receipt_version = str(receipt_evidence["artifactVersionId"])
    receipt = json.loads(objects[(receipt_path, receipt_version)])
    paired = receipt["pairedEvaluation"]
    first_cell = paired["metricCells"][0]
    first_cell["candidateNormalizedLcb95"] = candidate_normalized_lcb95
    first_cell["candidateNormalizedMean"] = candidate_normalized_lcb95
    first_cell["meanCandidateScore"] = candidate_normalized_lcb95 * 0.9
    paired["benchmarkScore"] = _d19_paired_benchmark_score(paired["metricCells"])
    paired.update(
        {
            "rejectionReasons": [
                "candidate-normalized-mean-not-met:APIBench:answer-quality:observed-metric-1",
                "candidate-normalized-lcb95-not-met:APIBench:answer-quality:observed-metric-1",
                *(
                    [
                        "baseline-retention-lcb95-to-mean-not-met:APIBench:answer-quality:observed-metric-1"
                    ]
                    if candidate_normalized_lcb95 / 0.9 < 0.9
                    else []
                ),
            ],
            "status": "rejected",
        }
    )
    receipt["status"] = "rejected"
    receipt_bytes = serp_eval_contracts_module._canonical_json(receipt).encode("utf-8")
    digest = sha256(receipt_bytes).hexdigest()
    objects[(receipt_path, receipt_version)] = receipt_bytes
    receipt_evidence["artifactETag"] = digest
    receipt_evidence["artifactSha256"] = digest
    result["receiptStatus"] = "rejected"
    receipt_subject = cast(dict[str, Any], result["receiptVerification"])["subject"]
    receipt_subject["sha256"] = "sha256:" + digest
    return result, objects


def _d19_observed_paired_evaluation(operation_id: str) -> dict[str, Any]:
    cells: list[dict[str, Any]] = []
    for index, suite_id in enumerate(MANDATORY_SERP_BENCHMARK_SUITES, start=1):
        cells.append(
            {
                "aggregation": "mean",
                "baselineNormalizedMean": 0.9,
                "candidateNormalizedLcb95": 0.925,
                "candidateNormalizedMean": 0.95,
                "maximumScore": 1.0,
                "meanBaselineScore": 0.81,
                "meanCandidateScore": 0.855,
                "metricFamily": "answer-quality",
                "metricId": f"observed-metric-{index}",
                "pairedCaseReceiptEvidence": [
                    {
                        "baseline": _d19_worm_evidence(
                            f"case-receipts/{suite_id}/baseline/{repetition}",
                            format((index + repetition) % 16, "x"),
                        ),
                        "candidate": _d19_worm_evidence(
                            f"case-receipts/{suite_id}/candidate/{repetition}",
                            format((index + repetition + 8) % 16, "x"),
                        ),
                        "repetition": repetition,
                    }
                    for repetition in range(1, 6)
                ],
                "pairedNormalizedDeltaLcb95": 0.025,
                "referenceAuthority": "official",
                "referenceEvidence": _d19_worm_evidence(
                    f"metric-references/{suite_id}", format(index % 16, "x")
                ),
                "referenceScore": 0.9,
                "suiteId": suite_id,
            }
        )
    return {
        "benchmarkScore": _d19_paired_benchmark_score(cells),
        "contractVersion": "serp-paired-evaluation/v6",
        "metricCells": cells,
        "operationId": operation_id,
        "status": "accepted",
    }


def _d19_paired_benchmark_score(cells: list[dict[str, Any]]) -> dict[str, Any]:
    score_cells = [
        {
            "baselineNormalizedMean": cell["baselineNormalizedMean"],
            "candidateNormalizedLcb95": cell["candidateNormalizedLcb95"],
            "candidateNormalizedMean": cell["candidateNormalizedMean"],
            "metricCellId": (f"{cell['suiteId']}:{cell['metricFamily']}:{cell['metricId']}"),
            "pairedNormalizedDeltaLcb95": cell["pairedNormalizedDeltaLcb95"],
        }
        for cell in cells
    ]
    worst_cell = min(score_cells, key=lambda cell: cell["candidateNormalizedLcb95"])
    retention_by_cell = [
        {
            "baselineNormalizedMean": cell["baselineNormalizedMean"],
            "baselineRetentionLcb95ToMean": (
                cell["candidateNormalizedLcb95"] / cell["baselineNormalizedMean"]
                if cell["baselineNormalizedMean"] > 0.0
                else None
            ),
            "candidateNormalizedLcb95": cell["candidateNormalizedLcb95"],
            "metricCellId": cell["metricCellId"],
        }
        for cell in score_cells
    ]
    measured_retention_rows = [
        row for row in retention_by_cell if row["baselineRetentionLcb95ToMean"] is not None
    ]
    worst_retention_cell = (
        min(
            measured_retention_rows,
            key=lambda row: cast(float, row["baselineRetentionLcb95ToMean"]),
        )
        if len(measured_retention_rows) == len(retention_by_cell)
        else None
    )
    return {
        "allNineBaselineRetentionLcb95ToMean": (
            worst_retention_cell["baselineRetentionLcb95ToMean"]
            if worst_retention_cell is not None
            else None
        ),
        "allNineCandidateNormalizedLcb95": worst_cell["candidateNormalizedLcb95"],
        "baselineRetentionByCell": retention_by_cell,
        "baselineRetentionFormula": (
            "min(candidateNormalizedLcb95 / baselineNormalizedMean across canonical metric cells)"
        ),
        "canonicalScoreFormula": "min(candidateNormalizedLcb95 across canonical metric cells)",
        "referenceNormalizedPairedDeltaFormula": (
            "LCB95 of paired (candidateScore - baselineScore) / referenceScore"
        ),
        "referenceNormalizedPairedDeltaLcb95ByCell": [
            {
                "metricCellId": cell["metricCellId"],
                "referenceNormalizedPairedDeltaLcb95": cell["pairedNormalizedDeltaLcb95"],
            }
            for cell in score_cells
        ],
        "schema": "PairedBenchmarkScore/v2",
        "supportingAggregates": {
            "baselineRetentionMeasuredCellCount": len(measured_retention_rows),
            "insufficientBaselineCellCount": len(score_cells) - len(measured_retention_rows),
            "meanBaselineNormalizedMean": math.fsum(
                cell["baselineNormalizedMean"] for cell in score_cells
            )
            / len(score_cells),
            "meanCandidateNormalizedLcb95": math.fsum(
                cell["candidateNormalizedLcb95"] for cell in score_cells
            )
            / len(score_cells),
            "meanCandidateNormalizedMean": math.fsum(
                cell["candidateNormalizedMean"] for cell in score_cells
            )
            / len(score_cells),
            "meanReferenceNormalizedPairedDeltaLcb95": math.fsum(
                cell["pairedNormalizedDeltaLcb95"] for cell in score_cells
            )
            / len(score_cells),
            "minimumReferenceNormalizedPairedDeltaLcb95": min(
                cell["pairedNormalizedDeltaLcb95"] for cell in score_cells
            ),
        },
        "worstBaselineRetentionCell": (
            dict(worst_retention_cell) if worst_retention_cell is not None else None
        ),
        "worstCell": {
            "candidateNormalizedLcb95": worst_cell["candidateNormalizedLcb95"],
            "metricCellId": worst_cell["metricCellId"],
        },
    }


def _d19_verification_descriptor(
    label: str,
    *,
    purpose: str,
    consumer_request_id: str,
    transit_request_id: str,
) -> dict[str, object]:
    subject = _nightly_worm_evidence(f"{label}-subject")
    attestation = _nightly_worm_evidence(f"{label}-attestation")
    return {
        "attestationEvidence": attestation,
        "consumerVerification": {"requestId": consumer_request_id, "valid": True},
        "purpose": purpose,
        "signer": {
            "namespace": "airflow" if "execution" in purpose else "jenkins",
            "serviceAccount": (
                "airflow-serp-benchmark-aggregator"
                if "execution" in purpose
                else "serp-evaluation-attestor"
            ),
        },
        "statementSha256": "sha256:" + sha256(label.encode("utf-8")).hexdigest(),
        "subject": subject,
        "transit": {
            "key": (
                "serp-evaluation-runtime" if "execution" in purpose else "serp-evaluation-authority"
            ),
            "keyVersion": 1,
            "signature": f"vault:v1:{label}",
            "verifyRequestId": transit_request_id,
        },
    }


def _d19_receipt_subject(evaluator_result: Mapping[str, Any]) -> dict[str, str]:
    receipt = cast(Mapping[str, Any], evaluator_result["receiptEvidence"])
    return {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": str(receipt["objectLockRetainUntil"]),
        "s3Uri": str(receipt["artifactPath"]),
        "sha256": "sha256:" + str(receipt["artifactSha256"]),
        "versionId": str(receipt["artifactVersionId"]),
    }


class _D19VerificationS3:
    def __init__(self, objects: Mapping[tuple[str, str], bytes]) -> None:
        self.objects = dict(objects)
        self.put_object_calls: list[str] = []

    def head_object(
        self, *, Bucket: str, Key: str, VersionId: str | None = None
    ) -> dict[str, object]:
        uri = f"s3://{Bucket}/{Key}"
        if VersionId is None:
            versions = [version_id for object_uri, version_id in self.objects if object_uri == uri]
            if not versions:
                raise FileNotFoundError(uri)
            VersionId = versions[-1]
        assert (uri, VersionId) in self.objects
        return {
            "ContentLength": len(self.objects[(uri, VersionId)]),
            "ObjectLockMode": "COMPLIANCE",
            "ObjectLockRetainUntilDate": datetime(2033, 7, 16, 21, tzinfo=UTC),
            "VersionId": VersionId,
        }

    def get_object(self, *, Bucket: str, Key: str, VersionId: str) -> dict[str, object]:
        uri = f"s3://{Bucket}/{Key}"
        return {"Body": io.BytesIO(self.objects[(uri, VersionId)])}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> dict[str, str]:
        assert ContentType == "application/json"
        uri = f"s3://{Bucket}/{Key}"
        version_id = "verification-version-001"
        self.objects[(uri, version_id)] = Body
        self.put_object_calls.append(uri)
        return {"ETag": sha256(Body).hexdigest(), "VersionId": version_id}


def _native_adapter_materializer(
    suite_id: str,
    payloads: Mapping[str, bytes],
    snapshots: Mapping[str, Mapping[str, object]],
) -> Mapping[str, object]:
    assert payloads
    assert set(payloads) == set(snapshots)
    return {
        "adapterId": f"native/{suite_id.casefold()}@v1",
        "caseCount": 1,
        "caseManifestSha256": "sha256:" + ("a" * 64),
        "status": "materialized",
        "suiteId": suite_id,
    }


def _native_corpus_materializer(
    suite_id: str,
    payloads: Mapping[str, bytes],
    snapshots: Mapping[str, Mapping[str, object]],
) -> Mapping[str, object]:
    assert set(payloads) == set(snapshots)
    role = {
        "APIBench": "api-documentation",
        "ARES": "context-corpus",
        "BEIR": "beir-corpus",
        "CodeRAG-Bench": "documentation-corpus",
        "RAGBench": "source-context",
        "RepoQA": "repository-code",
        "SWE-bench Verified": "base-commit-repository",
        "cwd-benchmark-data": "reference-graph",
        "rusBEIR": "beir-corpus",
    }[suite_id]
    corpus = b'{"documentId":"doc-1","text":"query-independent corpus"}\n'
    return {
        "manifest": {
            "datasetSha256BySource": {
                source_id: "sha256:" + sha256(payload).hexdigest()
                for source_id, payload in payloads.items()
            },
            "schema": "NativeBenchmarkCorpusManifest/v1",
            "sources": [
                {
                    "corpusRole": role,
                    "documentCount": 1,
                    "payloadSha256": "sha256:" + sha256(corpus).hexdigest(),
                    "sourceId": "corpus",
                }
            ],
            "status": "materialized",
            "suiteId": suite_id,
        },
        "payloads": {"corpus": corpus},
    }


def _execution_substrate_materializer(
    suite_id: str,
    _dataset_payloads: Mapping[str, bytes],
    _dataset_snapshots: Mapping[str, Mapping[str, object]],
    _corpus_payloads: Mapping[str, bytes],
    _corpus_snapshots: Mapping[str, Mapping[str, object]],
    _official_harness_payloads: Mapping[str, bytes],
) -> Mapping[str, bytes]:
    payloads = {
        role: f"pinned-execution-substrate:{suite_id}:{role}".encode()
        for role in MANDATORY_EXECUTION_SUBSTRATE_ROLES[suite_id]
    }
    revision = next(
        entry.harness_revision
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        if entry.suite_id == suite_id
    )
    if suite_id == "CodeRAG-Bench":
        payloads["execution-sandbox"] = json.dumps(
            {
                "baseImage": {
                    "imageReference": (
                        "harbor.adapstory.com/dockerhub-cache/library/python@sha256:" + "2" * 64
                    ),
                    "platform": "linux/amd64",
                    "schema": "Ds1000BaseImageProvenance/v1",
                    "sourceReference": (
                        "harbor.adapstory.com/dockerhub-cache/library/python:" "3.10-slim-bookworm"
                    ),
                },
                "datasetProvenance": {
                    "datasetPath": "data/ds1000.jsonl.gz",
                    "ds1000Revision": "b39aab71da6d23ef8d3cac59a7c5f834516ab334",
                    "fieldNames": [
                        "code_context",
                        "metadata",
                        "prompt",
                        "reference_code",
                    ],
                    "rowCount": 1000,
                    "schema": "Ds1000SimplifiedDatasetProvenance/v1",
                    "sha256": "sha256:" + "3" * 64,
                },
                "ds1000Revision": "b39aab71da6d23ef8d3cac59a7c5f834516ab334",
                "dockerSocketMounted": False,
                "imageDigest": "sha256:" + "1" * 64,
                "imageReference": ("harbor.adapstory.com/serp/ds1000@sha256:" + "1" * 64),
                "imagePurpose": "ds1000-simplified-official-execution",
                "libraries": [
                    {"name": name, "version": version} for name, version in DS1000_LIBRARY_VERSIONS
                ],
                "networkMode": "disabled",
                "officialDatasetPath": "data/ds1000.jsonl.gz",
                "pythonVersion": "3.10",
                "pytorchVariant": "cpuonly",
                "readOnlyRootFilesystem": True,
                "schema": "Ds1000SandboxImageInventory/v2",
                "suiteId": "DS-1000",
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    if suite_id == "SWE-bench Verified":
        payloads["sandbox-image-set"] = json.dumps(
            {
                "dockerSocketMounted": False,
                "executionMode": "prebuilt-per-instance-image",
                "instances": [
                    {
                        "baseCommit": "2" * 40,
                        "imageDigest": "sha256:" + "2" * 64,
                        "imageReference": (
                            "harbor.adapstory.com/serp/swe-django@sha256:" + "2" * 64
                        ),
                        "instanceId": "django__django-12345",
                        "repository": "django/django",
                    }
                ],
                "networkMode": "disabled",
                "officialHarnessRevision": revision,
                "schema": "SweBenchSandboxImageInventory/v1",
                "suiteId": suite_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    return payloads


def _d19_catalog_snapshot(plan: Any) -> dict[str, object]:
    return {
        "artifactPath": plan.payload["artifact_paths"]["benchmark_catalog"],
        "artifactSha256": "a" * 64,
        "artifactVersionId": "catalog-version-001",
        "blockingSuiteIds": [],
        "catalogReceiptPath": plan.payload["artifact_paths"]["benchmark_catalog_receipt"],
        "catalogReceiptSha256": "b" * 64,
        "catalogReceiptVersionId": "catalog-receipt-version-001",
        "catalogReceiptRetainUntil": "2027-07-14T00:00:00Z",
        "catalogRetainUntil": "2027-07-14T00:00:00Z",
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


def _d19_promotion_snapshot(plan: Any) -> dict[str, Any]:
    return {
        "promotionEvidence": plan.payload["evaluation_release_promotion_evidence"],
        "evaluationObjective": _d19_evaluation_objective_v6(),
        "promotion": {
            "schema": "EvaluationReleasePromotionReceipt/v8",
            "evaluationReleaseContractVersion": "serp-ci-evaluation-release-evidence/v8",
            "baselineRelease": {
                "evidence": _d19_worm_evidence("model-releases/baseline", "d"),
                "releaseDigest": "sha256:" + "1" * 64,
            },
            "candidateRelease": {
                "evidence": _d19_worm_evidence("model-releases/candidate", "e"),
                "releaseDigest": "sha256:" + "2" * 64,
            },
            "candidateReleaseAuthority": {
                "canaryState": "passed",
                "evidence": _d19_worm_evidence("model-releases/candidate", "e"),
                "modelId": "serp-all-nine-candidate-router@2026.07.3",
                "provider": "adapstory-model-gateway",
                "purpose": "serp-benchmark-candidate",
                "releaseDigest": "sha256:" + "2" * 64,
                "releaseId": "serp-candidate-release-2026.07.1",
            },
            "metricCompatibilityMatrixEvidence": _d19_worm_evidence("metric-matrix", "3"),
            "evaluationObjectiveEvidence": _d19_worm_evidence("evaluation-objective", "4"),
            "evaluationObjectiveAttestationEvidence": _d19_worm_evidence(
                "evaluation-objective-attestation", "5"
            ),
            "promotionId": "public-docs-reranker-eval-001",
            "registryResourceId": REGISTRY_RESOURCE_ID,
            "registryResourceType": "workflow",
            "tenantId": TENANT_ID,
        },
    }


def _d19_lifecycle_result(promotion_snapshot: Mapping[str, Any]) -> dict[str, Any]:
    promotion = promotion_snapshot["promotion"]
    return {
        "schema": "BC21AllNineBenchmarkPackLifecycleResult/v1",
        "tenantId": TENANT_ID,
        "evaluationBindingId": "018f5e13-2d73-7a77-a052-8d1bcbf96701",
        "evaluationBindingEvidence": _d19_worm_evidence("evaluation-binding", "b"),
        "bindingFingerprint": "sha256:" + "f" * 64,
        "expiresAt": "2026-07-15T23:00:00Z",
        "evaluationReleasePromotionEvidence": promotion_snapshot["promotionEvidence"],
        "baselineReleaseEvidence": promotion["baselineRelease"]["evidence"],
        "candidateReleaseEvidence": promotion["candidateRelease"]["evidence"],
        "baselineReleaseDigest": promotion["baselineRelease"]["releaseDigest"],
        "candidateReleaseDigest": promotion["candidateRelease"]["releaseDigest"],
        "packMaterialBindings": [
            _d19_pack_material_binding(suite_id, index)
            for index, suite_id in enumerate(MANDATORY_SERP_BENCHMARK_SUITES)
        ],
        "suiteExecutionBindings": [
            {"suiteId": suite_id} for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        ],
        "indexedReceiptCount": 18,
        "productionActivationRequested": False,
    }


def _d19_pack_material_binding(suite_id: str, index: int) -> dict[str, object]:
    return {
        "suiteId": suite_id,
        "baseline": _d19_pack_material_side(suite_id, "baseline", index),
        "candidate": _d19_pack_material_side(suite_id, "candidate", index),
    }


def _d19_pack_material_side(suite_id: str, side: str, index: int) -> dict[str, object]:
    side_digit = "1" if side == "baseline" else "2"
    digest_digit = "a" if side == "baseline" else "b"
    profile_digit = "c" if side == "baseline" else "d"
    return {
        "executionSubstrateSha256": "sha256:" + digest_digit * 64,
        "metricProfileSha256": "sha256:" + profile_digit * 64,
        "officialHarnessIdentitySha256": "sha256:" + "e" * 64,
        "packBuildReceiptEvidence": _d19_worm_evidence(
            f"pack-receipts/{suite_id.casefold().replace(' ', '-')}/{side}",
            digest_digit,
        ),
        "packBuildReceiptSha256": "sha256:" + digest_digit * 64,
        "packId": f"00000000-0000-4000-a000-{side_digit}{index:011d}",
        "packProfileEvidence": _d19_worm_evidence(
            f"pack-profiles/{suite_id.casefold().replace(' ', '-')}/{side}",
            profile_digit,
        ),
        "packProfileSha256": "sha256:" + profile_digit * 64,
        "packVersionId": f"00000000-0000-4000-a000-{side_digit}{index + 100:011d}",
        "releaseManifestSha256": "sha256:" + "f" * 64,
        "side": side,
        "suiteId": suite_id,
    }


def _attach_d19_mcp_runtime_binding_evidence(
    lifecycle_result: Mapping[str, Any],
) -> dict[tuple[str, str], bytes]:
    """Attach valid immutable receipt/snapshot/binding artifacts to a test lifecycle."""

    objects: dict[tuple[str, str], bytes] = {}
    bindings = lifecycle_result["packMaterialBindings"]
    assert isinstance(bindings, list)
    for pair in bindings:
        assert isinstance(pair, dict)
        suite_id = pair["suiteId"]
        assert isinstance(suite_id, str)
        suite_slug = suite_id.casefold().replace(" ", "-")
        for side in ("baseline", "candidate"):
            material = pair[side]
            assert isinstance(material, dict)
            receipt_payload = {
                "packId": material["packId"],
                "packVersionId": material["packVersionId"],
                "profileSha256": material["packProfileSha256"],
                "schema": "BenchmarkPackBuildReceipt/v1",
                "suiteId": suite_id,
            }
            receipt_evidence = _d19_worm_payload(
                f"pack-receipts/{suite_slug}/{side}", receipt_payload, objects
            )
            material["packBuildReceiptEvidence"] = receipt_evidence
            material["packBuildReceiptSha256"] = receipt_evidence["sha256"]
            snapshot_id = f"pack-snapshot:v2:{suite_slug}:{side}"
            snapshot_evidence = _d19_worm_payload(
                f"hermetic-snapshots/{suite_slug}/{side}",
                {
                    "contract_version": "SerpMcpHermeticPackSnapshot/v2",
                    "pack_build_receipt_sha256": receipt_evidence["sha256"],
                    "pack_id": material["packId"],
                    "pack_snapshot_id": snapshot_id,
                    "pack_version_id": material["packVersionId"],
                    "tenant_id": TENANT_ID,
                },
                objects,
            )
            binding_evidence = _d19_worm_payload(
                f"mcp-runtime-bindings/{suite_slug}/{side}",
                {
                    "contractVersion": "BenchmarkPackMcpRuntimeBinding/v1",
                    "mcpRuntimeContractVersion": "SerpMcpHermeticBenchmarkRuntime/v1",
                    "packBuildReceiptEvidence": receipt_evidence,
                    "packBuildReceiptSha256": receipt_evidence["sha256"],
                    "packId": material["packId"],
                    "packSnapshotId": snapshot_id,
                    "packSnapshotSha256": snapshot_evidence["sha256"],
                    "packVersionId": material["packVersionId"],
                    "snapshotContractVersion": "SerpMcpHermeticPackSnapshot/v2",
                    "snapshotEvidence": snapshot_evidence,
                },
                objects,
            )
            material["mcpRuntimeBindingEvidence"] = binding_evidence
    return objects


def _d19_worm_payload(
    name: str,
    payload: Mapping[str, object],
    objects: dict[tuple[str, str], bytes],
) -> dict[str, str]:
    raw = serp_eval_contracts_module._canonical_json(payload).encode("utf-8")
    evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-15T00:00:00Z",
        "s3Uri": f"s3://airflow-serp-evidence/serp-evals/{name}.json",
        "sha256": "sha256:" + sha256(raw).hexdigest(),
        "versionId": f"{name.replace('/', '-')}-version-001",
    }
    objects[(evidence["s3Uri"], evidence["versionId"])] = raw
    return evidence


def _public_docs_seed_refresh_conf() -> dict[str, Any]:
    return {
        "actor_id": "airflow-serp-public-docs-acquisition",
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
        "actor_id": "airflow-serp-public-docs-acquisition",
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
