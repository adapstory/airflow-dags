from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest

import dags.serp_eval_contracts as contracts
from dags.serp_eval_contracts import (
    build_public_docs_seed_refresh_plan,
    default_public_docs_seed_refresh_conf,
    dispatch_public_docs_seed_refresh_handoff_from_snapshot,
    load_public_docs_airflow_plan_snapshot,
    write_public_docs_airflow_plan_snapshot,
    write_public_docs_seed_refresh_plan_from_snapshot,
)


class _VersionedComplianceS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str, str], bytes] = {}
        self.get_calls: list[tuple[str, str, str]] = []
        self.head_calls: list[tuple[str, str, str]] = []
        self.put_calls: list[tuple[str, str, str]] = []

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
    ) -> dict[str, str]:
        assert ContentType == "application/json"
        version_id = f"version-{len(self.put_calls) + 1}"
        self.objects[(Bucket, Key, version_id)] = Body
        self.put_calls.append((Bucket, Key, version_id))
        return {"ETag": sha256(Body).hexdigest(), "VersionId": version_id}

    def head_object(self, *, Bucket: str, Key: str, VersionId: str) -> dict[str, object]:
        assert (Bucket, Key, VersionId) in self.objects
        self.head_calls.append((Bucket, Key, VersionId))
        return {
            "ContentLength": len(self.objects[(Bucket, Key, VersionId)]),
            "ObjectLockMode": "COMPLIANCE",
            "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=400),
            "VersionId": VersionId,
        }

    def get_object(self, *, Bucket: str, Key: str, VersionId: str) -> dict[str, object]:
        self.get_calls.append((Bucket, Key, VersionId))
        return {"Body": io.BytesIO(self.objects[(Bucket, Key, VersionId)])}


def test_exact_public_docs_evidence_rejects_head_size_before_body_download() -> None:
    evidence = {
        "s3Uri": "s3://airflow-serp-evidence/serp-public-docs/op/refresh-plan.json",
        "sha256": "sha256:" + sha256(b"oversized").hexdigest(),
        "versionId": "version-oversized",
    }

    class OversizedS3:
        def head_object(self, **_kwargs: str) -> dict[str, object]:
            return {
                "ContentLength": 9,
                "ObjectLockMode": "COMPLIANCE",
                "ObjectLockRetainUntilDate": datetime.now(UTC) + timedelta(days=400),
                "VersionId": evidence["versionId"],
            }

        def get_object(self, **_kwargs: str) -> dict[str, object]:
            raise AssertionError("oversized evidence body must not be downloaded")

    with pytest.raises(ValueError, match="exceeds the governed byte ceiling"):
        contracts._read_public_docs_exact_evidence_bytes(
            evidence,
            field_name="public docs test evidence",
            s3_client=OversizedS3(),
            max_bytes=8,
        )


def test_public_docs_plan_xcom_contains_only_exact_worm_handle_and_bounded_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "30")
    client = _VersionedComplianceS3()
    plan = _one_seed_plan()

    handle = write_public_docs_airflow_plan_snapshot(plan, s3_client=client)

    assert set(handle) == {"planEvidence", "schema", "summary"}
    assert handle["schema"] == "PublicDocsAirflowPlanHandle/v1"
    assert set(handle["planEvidence"]) == {"s3Uri", "sha256", "versionId"}
    assert set(handle["summary"]) == {
        "generatedAt",
        "operationId",
        "seedCount",
        "sourceTypeCounts",
    }
    encoded_handle = json.dumps(handle, sort_keys=True)
    assert len(encoded_handle.encode("utf-8")) <= contracts.PUBLIC_DOCS_MAX_XCOM_BYTES
    assert "seed_registry" not in encoded_handle
    assert "crawl_evidence" not in encoded_handle
    assert "https://docs.k3s.io/" not in encoded_handle
    assert len(client.put_calls) == 2

    hydrated = load_public_docs_airflow_plan_snapshot(handle, s3_client=client)

    assert hydrated["seed_registry"] == plan.payload["seed_registry"]
    assert hydrated["seed_registry_sha256"] == plan.payload["seed_registry_sha256"]
    assert client.get_calls
    assert all(version_id for _, _, version_id in client.get_calls)


def test_public_docs_plan_snapshot_rejects_plan_or_seed_digest_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "30")
    client = _VersionedComplianceS3()
    handle = write_public_docs_airflow_plan_snapshot(_one_seed_plan(), s3_client=client)
    corrupt_plan_handle = json.loads(json.dumps(handle))
    corrupt_plan_handle["planEvidence"]["sha256"] = "sha256:" + "0" * 64

    with pytest.raises(ValueError, match="plan evidence digest"):
        load_public_docs_airflow_plan_snapshot(corrupt_plan_handle, s3_client=client)

    plan_key = client.put_calls[-1]
    plan_snapshot = json.loads(client.objects[plan_key])
    seed_evidence = plan_snapshot["seedEvidence"][0]
    seed_ref = seed_evidence["evidence"]
    seed_path = seed_ref["s3Uri"].removeprefix("s3://")
    seed_bucket, seed_key = seed_path.split("/", 1)
    client.objects[(seed_bucket, seed_key, seed_ref["versionId"])] += b" "

    with pytest.raises(ValueError, match="seed evidence digest"):
        load_public_docs_airflow_plan_snapshot(handle, s3_client=client)


def test_public_docs_plan_snapshot_enforces_seed_cardinality_and_byte_ceilings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "30")
    client = _VersionedComplianceS3()
    conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-16T12:00:00Z",
        artifact_root_path="s3://airflow-serp-evidence/serp-public-docs",
    )
    conf["seed_registry"] = conf["seed_registry"][:2]
    plan = build_public_docs_seed_refresh_plan(conf)
    monkeypatch.setattr(contracts, "PUBLIC_DOCS_MAX_SEED_COUNT", 1)

    with pytest.raises(ValueError, match="seed count exceeds"):
        write_public_docs_airflow_plan_snapshot(plan, s3_client=client)

    one_seed = _one_seed_plan()
    monkeypatch.setattr(contracts, "PUBLIC_DOCS_MAX_SEED_COUNT", 128)
    monkeypatch.setattr(contracts, "PUBLIC_DOCS_MAX_SEED_EVIDENCE_BYTES", 256)

    with pytest.raises(ValueError, match="seed evidence exceeds"):
        write_public_docs_airflow_plan_snapshot(one_seed, s3_client=client)


def test_public_docs_plan_snapshot_rejects_crawler_collections_beyond_max_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "30")
    plan = _one_seed_plan()
    seed = plan.payload["seed_registry"][0]
    max_pages = seed["crawl_policy"]["max_pages"]
    seed["crawl_policy"]["crawl_evidence"] = {
        "blocked_urls": [f"https://docs.k3s.io/blocked-{index}" for index in range(max_pages + 1)],
        "changed_urls": [],
        "deleted_urls": [],
        "failed_urls": [],
        "pages": {},
        "state": {},
        "status": "completed",
        "summary": {
            "blocked": max_pages + 1,
            "changed": 0,
            "deleted": 0,
            "failed": 0,
            "unchanged": 0,
        },
        "unchanged_urls": [],
    }

    with pytest.raises(ValueError, match="blocked_urls exceeds max_pages"):
        write_public_docs_airflow_plan_snapshot(plan, s3_client=_VersionedComplianceS3())


def test_compact_refresh_plan_replays_exact_seed_versions_without_inline_crawl_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS", "30")
    client = _VersionedComplianceS3()
    monkeypatch.setattr(contracts, "_s3_client", lambda *_paths: client)
    plan_handle = write_public_docs_airflow_plan_snapshot(_one_seed_plan(), s3_client=client)

    refresh_handle = write_public_docs_seed_refresh_plan_from_snapshot(plan_handle)
    refresh_evidence = refresh_handle["evidence"]
    refresh_path = refresh_evidence["s3Uri"].removeprefix("s3://")
    refresh_bucket, refresh_key = refresh_path.split("/", 1)
    refresh_payload = json.loads(
        client.objects[(refresh_bucket, refresh_key, refresh_evidence["versionId"])]
    )

    assert set(refresh_handle) == {"artifactType", "evidence", "schema", "summary"}
    assert refresh_handle["artifactType"] == "public_docs_seed_refresh_plan"
    assert "seed_evidence" in refresh_payload
    assert "crawl_evidence" not in json.dumps(refresh_payload["seed_registry"], sort_keys=True)
    assert "previous_state" not in json.dumps(refresh_payload["seed_registry"], sort_keys=True)
    assert all(
        "crawl_evidence_reference" in request["source_metadata"]
        for request in refresh_payload["source_fetch_requests"]
    )
    hydrated_registry = contracts._public_docs_seed_registry_from_refresh_plan(refresh_payload)
    assert (
        hydrated_registry
        == load_public_docs_airflow_plan_snapshot(plan_handle, s3_client=client)["seed_registry"]
    )

    cli_spec = dispatch_public_docs_seed_refresh_handoff_from_snapshot(
        plan_handle,
        refresh_handle,
    )
    assert cli_spec["plan_evidence"] == plan_handle["planEvidence"]
    assert cli_spec["refresh_plan_evidence"] == refresh_handle["evidence"]
    d5_refresh_plan = contracts._read_public_docs_refresh_plan_for_d5(
        {
            "public_docs_seed_refresh_plan_evidence": refresh_handle["evidence"],
            "public_docs_seed_refresh_plan_path": refresh_handle["evidence"]["s3Uri"],
        }
    )
    assert d5_refresh_plan == refresh_payload

    corrupt_d5_evidence = json.loads(json.dumps(refresh_handle["evidence"]))
    corrupt_d5_evidence["sha256"] = "sha256:" + "f" * 64
    with pytest.raises(ValueError, match="digest"):
        contracts._read_public_docs_refresh_plan_for_d5(
            {
                "public_docs_seed_refresh_plan_evidence": corrupt_d5_evidence,
                "public_docs_seed_refresh_plan_path": corrupt_d5_evidence["s3Uri"],
            }
        )
    assert all(version_id for _, _, version_id in client.get_calls)


def test_d20_dag_uses_snapshot_specific_callables_instead_of_full_plan_xcom() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_web_seed_crawl_refresh.py"
    ).read_text(encoding="utf-8")

    assert "write_public_docs_airflow_plan_snapshot" in source
    assert "write_public_docs_seed_registry_from_snapshot" in source
    assert "write_public_docs_seed_refresh_plan_from_snapshot" in source
    assert "dispatch_public_docs_seed_refresh_handoff_from_snapshot" in source
    assert "submit_public_docs_bc21_pipeline_state_from_snapshot" in source
    assert "write_public_docs_publish_activation_trigger_conf_from_snapshot" in source
    assert "governance_notification_from_public_docs_snapshot" not in source
    validate_function = source.split("def validate_public_docs_seed_registry", 1)[1].split(
        "\n\ndef ", 1
    )[0]
    assert "write_airflow_plan_artifact" not in validate_function


def _one_seed_plan() -> contracts.SerpDagPlan:
    conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-16T12:00:00Z",
        artifact_root_path="s3://airflow-serp-evidence/serp-public-docs",
    )
    conf["seed_registry"] = conf["seed_registry"][:1]
    return build_public_docs_seed_refresh_plan(conf)
