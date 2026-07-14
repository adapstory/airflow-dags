from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import dags.serp_eval_contracts as serp_eval_contracts_module
from dags.serp_benchmark_catalog import (
    BENCHMARK_CATALOG_CONTRACT_VERSION,
    MANDATORY_BENCHMARK_SUITE_CATALOG,
    build_live_benchmark_catalog_evidence,
    mandatory_benchmark_adapters_ready,
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


def test_huggingface_dataset_artifact_uses_pinned_xet_aware_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    downloaded = tmp_path / "dataset.parquet"
    downloaded.write_bytes(b"xet-backed-dataset")
    calls: list[dict[str, object]] = []
    transport_events: list[str] = []
    client_kwargs: list[dict[str, object]] = []
    timeout_calls: list[dict[str, float]] = []

    class Httpx:
        class Timeout:
            def __init__(
                self,
                default: float,
                *,
                connect: float,
                read: float,
                write: float,
                pool: float,
            ) -> None:
                timeout_calls.append(
                    {
                        "default": default,
                        "connect": connect,
                        "read": read,
                        "write": write,
                        "pool": pool,
                    }
                )

        class Client:
            def __init__(self, **kwargs: object) -> None:
                client_kwargs.append(kwargs)

    class HuggingFaceHub:
        @staticmethod
        def set_client_factory(factory: object) -> None:
            assert callable(factory)
            transport_events.append("set-client-factory")
            factory()

        @staticmethod
        def close_session() -> None:
            transport_events.append("close-session")

        @staticmethod
        def hf_hub_download(**kwargs: object) -> str:
            calls.append(kwargs)
            return str(downloaded)

    def fake_import_module(name: str) -> object:
        if name == "huggingface_hub":
            return HuggingFaceHub
        assert name == "httpx"
        return Httpx

    monkeypatch.setenv("ADAPSTORY_SERP_HUGGINGFACE_TOKEN", "test-token")
    monkeypatch.setenv(
        "ADAPSTORY_SERP_SOURCE_PROXY_URL",
        "http://forward-proxy.forward-proxy.svc.cluster.local:3128",
    )
    monkeypatch.setattr(
        serp_eval_contracts_module,
        "importlib",
        SimpleNamespace(import_module=fake_import_module),
    )
    monkeypatch.setattr(
        serp_eval_contracts_module,
        "_open_public_docs_crawler_request",
        lambda *_args, **_kwargs: pytest.fail("Xet dataset download must not use raw urllib"),
    )

    assert (
        _fetch_https_bytes(
            "https://huggingface.co/datasets/galileo-ai/ragbench/resolve/"
            "97808f3e5fd16ede40bbff6c2949af8139b2eb7b/covidqa/test-00000-of-00001.parquet"
        )
        == b"xet-backed-dataset"
    )
    assert calls == [
        {
            "filename": "covidqa/test-00000-of-00001.parquet",
            "repo_id": "galileo-ai/ragbench",
            "repo_type": "dataset",
            "revision": "97808f3e5fd16ede40bbff6c2949af8139b2eb7b",
            "token": "test-token",
        }
    ]
    assert transport_events == ["set-client-factory", "close-session"]
    assert timeout_calls == [
        {
            "default": 120.0,
            "connect": 30.0,
            "read": 120.0,
            "write": 30.0,
            "pool": 30.0,
        }
    ]
    assert len(client_kwargs) == 1
    assert client_kwargs[0]["follow_redirects"] is True
    assert client_kwargs[0]["proxy"] == "http://forward-proxy.forward-proxy.svc.cluster.local:3128"
    assert client_kwargs[0]["timeout"] is not None
    assert client_kwargs[0]["trust_env"] is False
    assert os.environ["HTTP_PROXY"] == "http://forward-proxy.forward-proxy.svc.cluster.local:3128"
    assert os.environ["HTTPS_PROXY"] == "http://forward-proxy.forward-proxy.svc.cluster.local:3128"
    assert ".svc.cluster.local" in os.environ["NO_PROXY"]


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
        entry.dataset_artifact_url.startswith("https://")
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        url.startswith("https://")
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        for _, url in entry.supplemental_dataset_artifacts
    )
    assert all(
        entry.adapter_source_url.startswith("https://")
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(entry.distribution_rule for entry in MANDATORY_BENCHMARK_SUITE_CATALOG)


def test_catalog_exposes_fail_closed_d6_schedule_readiness() -> None:
    """D6 is schedulable only because every suite has a native adapter in the image."""

    assert mandatory_benchmark_adapters_ready() is True


def test_catalog_pins_each_upstream_dataset_to_an_immutable_revision() -> None:
    assert all(
        all(
            len(revision) == 40 and all(character in "0123456789abcdef" for character in revision)
            for revision in entry.dataset_revision.split("+")
        )
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        "/main/" not in entry.dataset_source_url for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        "/main/" not in entry.license_evidence_url for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        "/main/" not in entry.dataset_artifact_url for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
    )
    assert all(
        "/main/" not in url
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        for _, url in entry.supplemental_dataset_artifacts
    )


def test_live_catalog_allows_rights_unverified_internal_runs() -> None:
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
            entry.dataset_artifact_url: f"dataset-bytes:{entry.suite_id}".encode()
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        }
    )
    payload_by_url.update(_supplemental_dataset_payloads())
    payload_by_url.update(
        {
            entry.adapter_source_url: f"adapter:{entry.suite_id}".encode()
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        }
    )

    evidence = build_live_benchmark_catalog_evidence(
        observed_at="2026-07-13T00:00:00Z",
        fetch_bytes=payload_by_url.__getitem__,
        snapshot_bytes=_snapshot_bytes,
        native_adapter_materializer=_native_adapter_materializer,
    )
    suites = cast(list[dict[str, Any]], evidence["suites"])

    assert evidence["contract_version"] == BENCHMARK_CATALOG_CONTRACT_VERSION
    assert evidence["catalog_status"] == "ready"
    assert [item["suite_id"] for item in suites] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert all(item["source_snapshot"]["sha256"].startswith("sha256:") for item in suites)
    assert all(item["license_snapshot"]["sha256"].startswith("sha256:") for item in suites)
    assert all(
        all(
            snapshot["sha256"].startswith("sha256:")
            for snapshot in item["dataset_snapshots"].values()
        )
        for item in suites
    )
    assert {
        item["suite_id"] for item in suites if item["rights_status"] == "rights-unverified"
    } == {"CodeRAG-Bench", "SWE-bench Verified", "rusBEIR"}
    assert {item["suite_id"] for item in suites if item["execution_status"] == "ready"} == set(
        MANDATORY_SERP_BENCHMARK_SUITES
    )
    assert {
        item["distribution_rule"] for item in suites if item["rights_status"] == "rights-unverified"
    } == {"internal-only-no-redistribution"}


def test_catalog_refuses_to_mark_a_snapshot_ready_without_native_adapter_evidence() -> None:
    with pytest.raises(ValueError, match="native adapter materializer is required"):
        build_live_benchmark_catalog_evidence(
            observed_at="2026-07-13T00:00:00Z",
            fetch_bytes=lambda _url: b"upstream-bytes",
            snapshot_bytes=_snapshot_bytes,
        )


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
    payload_by_url.update(
        {
            entry.dataset_artifact_url: f"dataset-bytes:{entry.suite_id}".encode()
            for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        }
    )
    payload_by_url.update(_supplemental_dataset_payloads())
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
        native_adapter_materializer=_native_adapter_materializer,
    )
    suites = cast(list[dict[str, Any]], evidence["suites"])

    assert len(calls) == (len(MANDATORY_SERP_BENCHMARK_SUITES) * 3) + 2
    beir = next(item for item in suites if item["suite_id"] == "BEIR")
    assert (
        beir["dataset_snapshots"]["dataset"]["immutable_artifact"]["objectLockMode"] == "COMPLIANCE"
    )
    assert beir["source_snapshot"]["immutable_artifact"]["objectLockMode"] == "COMPLIANCE"
    assert beir["license_snapshot"]["immutable_artifact"]["artifactVersionId"] == (
        "version-BEIR-license"
    )


def _snapshot_bytes(
    suite_id: str,
    evidence_type: str,
    _url: str,
    payload: bytes,
) -> dict[str, str]:
    return {
        "artifactPath": f"s3://airflow-serp-evidence/catalog/{suite_id}/{evidence_type}",
        "artifactSha256": sha256(payload).hexdigest(),
        "artifactVersionId": f"version-{suite_id}-{evidence_type}",
        "objectLockMode": "COMPLIANCE",
    }


def _native_adapter_materializer(
    suite_id: str,
    payloads: Mapping[str, bytes],
    snapshots: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    assert payloads
    assert set(payloads) == set(snapshots)
    return {
        "adapterId": f"native/{suite_id.casefold()}@v1",
        "caseCount": 1,
        "caseManifestSha256": "sha256:" + ("a" * 64),
        "status": "materialized",
        "suiteId": suite_id,
    }


def _supplemental_dataset_payloads() -> dict[str, bytes]:
    return {
        url: f"dataset-bytes:{entry.suite_id}:{source_id}".encode()
        for entry in MANDATORY_BENCHMARK_SUITE_CATALOG
        for source_id, url in entry.supplemental_dataset_artifacts
    }


def test_immutable_evidence_snapshot_requires_versioned_compliance_locked_s3_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class S3Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.head_calls: list[dict[str, object]] = []

        def put_object(self, **kwargs: object) -> dict[str, str]:
            self.calls.append(kwargs)
            return {"ETag": '"deadbeef"', "VersionId": "version-20260713"}

        def head_object(self, **kwargs: object) -> dict[str, object]:
            self.head_calls.append(kwargs)
            return {
                "ObjectLockMode": "COMPLIANCE",
                "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=366),
                "VersionId": "version-20260713",
            }

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
    assert "ObjectLockMode" not in client.calls[0]
    assert client.head_calls == [
        {
            "Bucket": "airflow-serp-evidence",
            "Key": "serp-evals/run/catalog.json",
            "VersionId": "version-20260713",
        }
    ]


def test_immutable_binary_snapshot_binds_raw_dataset_bytes_to_s3_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class S3Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.head_calls: list[dict[str, object]] = []

        def put_object(self, **kwargs: object) -> dict[str, str]:
            self.calls.append(kwargs)
            return {"ETag": '"scifact-archive"', "VersionId": "version-scifact"}

        def head_object(self, **kwargs: object) -> dict[str, object]:
            self.head_calls.append(kwargs)
            return {
                "ObjectLockMode": "COMPLIANCE",
                "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=366),
                "VersionId": "version-scifact",
            }

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
    assert "ObjectLockMode" not in client.calls[0]
    assert client.head_calls[0]["VersionId"] == "version-scifact"
