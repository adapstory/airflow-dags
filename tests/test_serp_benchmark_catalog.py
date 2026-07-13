from __future__ import annotations

from hashlib import sha256
from typing import Any, cast

import pytest

from dags.serp_benchmark_catalog import (
    BENCHMARK_CATALOG_CONTRACT_VERSION,
    MANDATORY_BENCHMARK_SUITE_CATALOG,
    build_live_benchmark_catalog_evidence,
)
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    _fetch_https_bytes,
    write_immutable_evidence_bytes_snapshot,
    write_immutable_evidence_snapshot,
)


def test_benchmark_catalog_fetch_uses_configured_source_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proxy_urls: list[dict[str, str]] = []

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return b"licensed-dataset-card"

    class Opener:
        def open(self, _request: object, *, timeout: int) -> Response:
            assert timeout == 30
            return Response()

    class FakeProxyHandler:
        def __init__(self, urls: dict[str, str]) -> None:
            proxy_urls.append(urls)

    monkeypatch.setenv(
        "ADAPSTORY_SERP_SOURCE_PROXY_URL",
        "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
    )
    monkeypatch.setattr("dags.serp_eval_contracts.ProxyHandler", FakeProxyHandler)
    monkeypatch.setattr("dags.serp_eval_contracts.build_opener", lambda _handler: Opener())

    assert _fetch_https_bytes("https://example.test/dataset") == b"licensed-dataset-card"
    assert proxy_urls == [
        {
            "http": "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
            "https": "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
        }
    ]


def test_catalog_covers_every_mandatory_suite_with_explicit_licensing_boundary() -> None:
    assert tuple(entry.suite_id for entry in MANDATORY_BENCHMARK_SUITE_CATALOG) == (
        MANDATORY_SERP_BENCHMARK_SUITES
    )
    assert all(
        entry.dataset_source_url.startswith("https://")
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        entry.license_evidence_url.startswith("https://")
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        entry.adapter_source_url.startswith("https://")
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(entry.distribution_rule for entry in MANDATORY_BENCHMARK_SUITE_CATALOG)


def test_catalog_pins_each_upstream_dataset_to_an_immutable_revision() -> None:
    assert all(
        len(entry.dataset_revision) == 40
        and all(character in "0123456789abcdef" for character in entry.dataset_revision)
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        "/main/" not in entry.dataset_source_url for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        "/main/" not in entry.license_evidence_url for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )


def test_live_catalog_evidence_is_content_addressed_and_fails_closed_for_unattested_suite() -> None:
    payload_by_url = {
        entry.dataset_source_url: f"dataset-source:{entry.suite_id}".encode()
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    }
    payload_by_url.update(
        {
            entry.license_evidence_url: (
                f"license:{entry.suite_id}:{entry.dataset_license_id}"
            ).encode()
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        }
    )
    payload_by_url.update(
        {
            entry.adapter_source_url: f"adapter:{entry.suite_id}".encode()
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        }
    )

    evidence = build_live_benchmark_catalog_evidence(
        observed_at="2026-07-13T00:00:00Z",
        fetch_bytes=payload_by_url.__getitem__,
    )
    suites = cast(list[dict[str, Any]], evidence["suites"])

    assert evidence["contract_version"] == BENCHMARK_CATALOG_CONTRACT_VERSION
    assert evidence["catalog_status"] == "blocked"
    assert [item["suite_id"] for item in suites] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert all(item["source_snapshot"]["sha256"].startswith("sha256:") for item in suites)
    assert all(item["license_snapshot"]["sha256"].startswith("sha256:") for item in suites)
    assert {
        item["suite_id"] for item in suites if item["execution_status"] == "review-required"
    } == {"CodeRAG-Bench", "SWE-bench Verified", "rusBEIR"}


def test_live_catalog_retains_immutable_source_and_license_snapshots() -> None:
    payload_by_url = {
        entry.dataset_source_url: f"dataset-source:{entry.suite_id}".encode()
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    }
    payload_by_url.update(
        {
            entry.license_evidence_url: f"license:{entry.suite_id}".encode()
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        }
    )
    calls: list[tuple[str, str, str, bytes]] = []

    def snapshot_bytes(
        suite_id: str,
        evidence_type: str,
        url: str,
        payload: bytes,
    ) -> dict[str, str]:
        calls.append((suite_id, evidence_type, url, payload))
        return {
            "artifactPath": f"s3://airflow-serp-evidence/catalog/{suite_id}/{evidence_type}",
            "artifactSha256": sha256(payload).hexdigest(),
            "artifactVersionId": f"version-{suite_id}-{evidence_type}",
            "objectLockMode": "COMPLIANCE",
        }

    evidence = build_live_benchmark_catalog_evidence(
        observed_at="2026-07-13T00:00:00Z",
        fetch_bytes=payload_by_url.__getitem__,
        snapshot_bytes=snapshot_bytes,
    )
    suites = cast(list[dict[str, Any]], evidence["suites"])

    assert len(calls) == len(MANDATORY_SERP_BENCHMARK_SUITES) * 2
    beir = next(item for item in suites if item["suite_id"] == "BEIR")
    assert beir["source_snapshot"]["immutable_artifact"]["objectLockMode"] == "COMPLIANCE"
    assert beir["license_snapshot"]["immutable_artifact"]["artifactVersionId"] == (
        "version-BEIR-license"
    )


def test_immutable_evidence_snapshot_requires_versioned_compliance_locked_s3_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class S3Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def put_object(self, **kwargs: object) -> dict[str, str]:
            self.calls.append(kwargs)
            return {"ETag": '"deadbeef"', "VersionId": "version-20260713"}

    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    client = S3Client()

    result = write_immutable_evidence_snapshot(
        "s3://airflow-serp-evidence/serp-evals/run/catalog.json",
        artifact_type="benchmark_catalog",
        operation_id="run",
        payload={"catalog_status": "blocked"},
        s3_client=client,
    )

    assert result["artifactVersionId"] == "version-20260713"
    assert result["objectLockMode"] == "COMPLIANCE"
    assert result["artifactETag"] == "deadbeef"
    assert client.calls[0]["Bucket"] == "airflow-serp-evidence"
    assert client.calls[0]["Key"] == "serp-evals/run/catalog.json"
    assert client.calls[0]["ObjectLockMode"] == "COMPLIANCE"


def test_immutable_binary_snapshot_binds_raw_dataset_bytes_to_s3_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class S3Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def put_object(self, **kwargs: object) -> dict[str, str]:
            self.calls.append(kwargs)
            return {"ETag": '"scifact-archive"', "VersionId": "version-scifact"}

    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "365")
    client = S3Client()

    result = write_immutable_evidence_bytes_snapshot(
        "s3://airflow-serp-evidence/serp-evals/run/datasets/scifact.zip",
        artifact_type="beir_scifact_archive",
        operation_id="run",
        payload=b"PK\\x03\\x04scifact",
        content_type="application/zip",
        s3_client=client,
    )

    assert result["artifactVersionId"] == "version-scifact"
    assert result["artifactSha256"] == (
        "4b724b1a7d2ebf3ed72a63ea0abec451734920b428729b6f7f7f4b7f75ea0962"
    )
    assert client.calls[0]["Body"] == b"PK\\x03\\x04scifact"
    assert client.calls[0]["ContentType"] == "application/zip"
    assert client.calls[0]["ObjectLockMode"] == "COMPLIANCE"
