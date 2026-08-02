from __future__ import annotations

import json
from pathlib import Path

import pytest


def _verified_source_set() -> dict[str, object]:
    operation_id = "ci-benchmark-substrates-79"
    evidence = {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": "2027-07-24T12:00:00Z",
        "s3Uri": (f"s3://airflow-serp-evidence/serp-evals/{operation_id}/source-set.json"),
        "sha256": "sha256:" + "a" * 64,
        "versionId": "source-set-v1",
    }
    return {
        "checkoutProvenance": {},
        "ds1000BaseImageProvenance": {},
        "ds1000BaseImageProvenanceEvidence": {},
        "ds1000DatasetProvenance": {},
        "ds1000DatasetProvenanceEvidence": {},
        "ds1000WheelhouseManifest": {},
        "ds1000WheelhouseResolution": {},
        "ds1000WheelhouseResolutionEvidence": {},
        "operationId": operation_id,
        "retainUntil": evidence["retainUntil"],
        "sourceSet": {
            "schema": "BenchmarkExecutionSubstrateSourceSet/v7",
        },
        "sourceSetEvidence": evidence,
        "supplyAttestations": {},
        "supplyAttestationsEvidence": {},
    }


def test_absent_source_set_is_a_recoverable_not_ready_state(tmp_path: Path) -> None:
    from dags.serp_benchmark_runtime_prerequisite import (
        source_set_prerequisite_state,
    )

    assert source_set_prerequisite_state({}) is None
    empty_contract = tmp_path / "source-set.json"
    empty_contract.write_text("  ", encoding="utf-8")
    assert (
        source_set_prerequisite_state(
            {"ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE_FILE": str(empty_contract)}
        )
        is None
    )


def test_present_source_set_is_parsed_and_bound_to_its_worm_identity(tmp_path: Path) -> None:
    from dags.serp_benchmark_runtime_prerequisite import (
        source_set_prerequisite_state,
    )

    source_set = _verified_source_set()
    contract = tmp_path / "source-set.json"
    contract.write_text(json.dumps(source_set), encoding="utf-8")
    assert source_set_prerequisite_state(
        {"ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE_FILE": str(contract)}
    ) == {
        "operationId": "ci-benchmark-substrates-79",
        "retainUntil": "2027-07-24T12:00:00Z",
        "sourceSetEvidence": source_set["sourceSetEvidence"],
    }


def test_present_but_malformed_source_set_fails_closed(tmp_path: Path) -> None:
    from dags.serp_benchmark_runtime_prerequisite import (
        source_set_prerequisite_state,
    )

    contract = tmp_path / "source-set.json"
    contract.write_text("{", encoding="utf-8")
    with pytest.raises(ValueError, match="valid JSON"):
        source_set_prerequisite_state(
            {"ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE_FILE": str(contract)}
        )

    mismatched = _verified_source_set()
    mismatched_evidence = mismatched["sourceSetEvidence"]
    assert isinstance(mismatched_evidence, dict)
    mismatched["sourceSetEvidence"] = {
        **mismatched_evidence,
        "s3Uri": (
            "s3://airflow-serp-evidence/serp-evals/ci-benchmark-substrates-80/source-set.json"
        ),
    }
    contract.write_text(json.dumps(mismatched), encoding="utf-8")
    with pytest.raises(ValueError, match="operationId"):
        source_set_prerequisite_state(
            {"ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE_FILE": str(contract)}
        )
