"""Classify the benchmark substrate runtime prerequisite without side effects."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

BENCHMARK_SUBSTRATE_SOURCE_SET_ENV_NAME = "ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE"
_VERIFIED_SOURCE_SET_FIELDS = frozenset(
    {
        "checkoutProvenance",
        "ds1000BaseImageProvenance",
        "ds1000BaseImageProvenanceEvidence",
        "ds1000DatasetProvenance",
        "ds1000DatasetProvenanceEvidence",
        "ds1000WheelhouseManifest",
        "ds1000WheelhouseResolution",
        "ds1000WheelhouseResolutionEvidence",
        "operationId",
        "retainUntil",
        "sourceSet",
        "sourceSetEvidence",
        "supplyAttestations",
        "supplyAttestationsEvidence",
    }
)
_WORM_HANDLE_FIELDS = frozenset({"objectLockMode", "retainUntil", "s3Uri", "sha256", "versionId"})
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SOURCE_SET_URI = re.compile(
    r"s3://airflow-serp-evidence/serp-evals/"
    r"(ci-benchmark-substrates-[1-9][0-9]*)/source-set\.json\Z"
)


def _required_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _required_string(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value


def source_set_prerequisite_state(
    environment: Mapping[str, str],
) -> dict[str, Any] | None:
    """Return the verified identity needed by a ready task, or ``None``.

    Absence is an expected, recoverable deployment state while the supply and
    runtime-promotion chain is still running.  Once the key exists, malformed
    or conflicting identity remains a fail-closed integrity error.
    """

    raw = environment.get(BENCHMARK_SUBSTRATE_SOURCE_SET_ENV_NAME, "").strip()
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError(
            "benchmark substrate source set prerequisite must be valid JSON"
        ) from error
    source_set = _required_mapping(value, label="benchmark substrate source set")
    if set(source_set) != _VERIFIED_SOURCE_SET_FIELDS:
        raise ValueError("benchmark substrate source set prerequisite has an unsupported field set")

    operation_id = _required_string(
        source_set.get("operationId"), label="benchmark substrate operationId"
    )
    retain_until = _required_string(
        source_set.get("retainUntil"), label="benchmark substrate retainUntil"
    )
    source_payload = _required_mapping(
        source_set.get("sourceSet"), label="benchmark substrate sourceSet"
    )
    if source_payload.get("schema") != "BenchmarkExecutionSubstrateSourceSet/v7":
        raise ValueError("benchmark substrate sourceSet schema is unsupported")

    evidence = _required_mapping(
        source_set.get("sourceSetEvidence"),
        label="benchmark substrate sourceSetEvidence",
    )
    if set(evidence) != _WORM_HANDLE_FIELDS:
        raise ValueError("benchmark substrate sourceSetEvidence must be an exact WORM handle")
    if evidence.get("objectLockMode") != "COMPLIANCE":
        raise ValueError("benchmark substrate sourceSetEvidence must be COMPLIANCE locked")
    if evidence.get("retainUntil") != retain_until:
        raise ValueError("benchmark substrate retainUntil does not match its WORM handle")
    if not _DIGEST.fullmatch(str(evidence.get("sha256", ""))):
        raise ValueError("benchmark substrate sourceSetEvidence sha256 is invalid")
    source_set_uri = _required_string(
        evidence.get("s3Uri"),
        label="benchmark substrate sourceSetEvidence s3Uri",
    )
    source_set_match = _SOURCE_SET_URI.fullmatch(source_set_uri)
    if source_set_match is None or source_set_match.group(1) != operation_id:
        raise ValueError("benchmark substrate operationId does not match sourceSetEvidence")
    _required_string(
        evidence.get("versionId"),
        label="benchmark substrate sourceSetEvidence versionId",
    )

    return {
        "operationId": operation_id,
        "retainUntil": retain_until,
        "sourceSetEvidence": dict(evidence),
    }
