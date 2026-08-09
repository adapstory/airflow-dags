"""Isolated executor for immutable mandatory-benchmark catalog acquisition."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote

from dags.serp_eval_contracts import (
    materialize_live_benchmark_catalog_artifact,
    normalize_benchmark_catalog_official_harness_lineage,
    normalize_benchmark_catalog_suite_summary,
    write_immutable_evidence_snapshot,
)

BENCHMARK_CATALOG_MATERIALIZER_CONTRACT_VERSION = "serp-benchmark-catalog-materializer/v5"
CATALOG_MATERIALIZATION_HEARTBEAT_SCHEMA = "CatalogMaterializationHeartbeat/v1"
CATALOG_MATERIALIZATION_HEARTBEAT_PREFIX = "SERP_CATALOG_MATERIALIZATION_PROGRESS "
_CATALOG_PHASES = (
    "catalog-start",
    "source-fetch",
    "native-adapter",
    "native-corpus",
    "execution-substrate",
    "catalog-snapshot",
    "receipt-snapshot",
    "complete",
)

CatalogMaterializer = Callable[..., dict[str, Any]]
ReceiptWriter = Callable[..., dict[str, Any]]
HeartbeatWriter = Callable[[Mapping[str, object]], None]


def parse_catalog_materialization_heartbeat(
    value: Mapping[str, object],
) -> dict[str, object]:
    """Validate the exact cross-runtime catalog progress contract."""

    expected = {"schema", "operationId", "phase", "pass", "byteCursor", "lastProgressAt"}
    if set(value) != expected or value.get("schema") != CATALOG_MATERIALIZATION_HEARTBEAT_SCHEMA:
        raise ValueError("catalog materialization heartbeat has an invalid shape/schema")
    operation_id = value.get("operationId")
    phase = value.get("phase")
    pass_number = value.get("pass")
    byte_cursor = value.get("byteCursor")
    observed_at = value.get("lastProgressAt")
    if not isinstance(operation_id, str) or not operation_id:
        raise ValueError("catalog materialization heartbeat operationId is required")
    if phase not in _CATALOG_PHASES:
        raise ValueError("catalog materialization heartbeat phase is unsupported")
    if isinstance(pass_number, bool) or not isinstance(pass_number, int) or pass_number < 0:
        raise ValueError("catalog materialization heartbeat pass is invalid")
    if isinstance(byte_cursor, bool) or not isinstance(byte_cursor, int) or byte_cursor < 0:
        raise ValueError("catalog materialization heartbeat byteCursor is invalid")
    if not isinstance(observed_at, str) or not observed_at.endswith("Z"):
        raise ValueError("catalog materialization heartbeat lastProgressAt must be RFC3339 UTC")
    try:
        parsed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            "catalog materialization heartbeat lastProgressAt must be RFC3339 UTC"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("catalog materialization heartbeat lastProgressAt must be timezone-aware")
    return dict(value)


def _emit_catalog_materialization_heartbeat(heartbeat: Mapping[str, object]) -> None:
    normalized = parse_catalog_materialization_heartbeat(heartbeat)
    print(
        CATALOG_MATERIALIZATION_HEARTBEAT_PREFIX
        + json.dumps(normalized, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
        file=sys.stderr,
        flush=True,
    )


@dataclass(slots=True)
class _CatalogProgressTracker:
    operation_id: str
    writer: HeartbeatWriter
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    last_pass: int = field(init=False, default=0)
    last_phase_index: int = field(init=False, default=0)
    byte_cursor: int = field(init=False, default=0)

    def advance(self, *, phase: str, pass_number: int, byte_cursor: int) -> None:
        if phase not in _CATALOG_PHASES:
            raise ValueError("catalog materialization heartbeat phase is unsupported")
        phase_index = _CATALOG_PHASES.index(phase)
        if pass_number < self.last_pass or byte_cursor < self.byte_cursor:
            raise ValueError("catalog materialization heartbeat regressed")
        if pass_number == self.last_pass and phase_index < self.last_phase_index:
            raise ValueError("catalog materialization heartbeat phase regressed")
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("catalog materialization heartbeat clock must be timezone-aware")
        heartbeat = parse_catalog_materialization_heartbeat(
            {
                "schema": CATALOG_MATERIALIZATION_HEARTBEAT_SCHEMA,
                "operationId": self.operation_id,
                "phase": phase,
                "pass": pass_number,
                "byteCursor": byte_cursor,
                "lastProgressAt": now.astimezone(UTC)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
            }
        )
        self.writer(heartbeat)
        self.last_pass = pass_number
        self.last_phase_index = phase_index
        self.byte_cursor = byte_cursor


def materialize_benchmark_catalog_receipt(
    plan: Mapping[str, Any] | str,
    *,
    catalog_materializer: CatalogMaterializer = materialize_live_benchmark_catalog_artifact,
    receipt_writer: ReceiptWriter = write_immutable_evidence_snapshot,
    heartbeat_writer: HeartbeatWriter = _emit_catalog_materialization_heartbeat,
) -> dict[str, Any]:
    """Fetch external catalog inputs and seal the resulting snapshot in a WORM receipt."""

    payload = _plan_payload(plan)
    artifact_paths = _required_mapping(payload, "artifact_paths")
    catalog_path = _required_str(artifact_paths, "benchmark_catalog")
    receipt_path = _required_str(artifact_paths, "benchmark_catalog_receipt")
    tracker = _CatalogProgressTracker(
        operation_id=_required_str(payload, "operation_id"), writer=heartbeat_writer
    )
    tracker.advance(phase="catalog-start", pass_number=0, byte_cursor=0)
    catalog_snapshot = catalog_materializer(payload, progress_heartbeat=tracker.advance)
    final_pass = tracker.last_pass + 1
    tracker.advance(
        phase="catalog-snapshot", pass_number=final_pass, byte_cursor=tracker.byte_cursor
    )
    if _required_str(catalog_snapshot, "artifactPath") != catalog_path:
        raise ValueError("benchmark catalog snapshot must match the plan artifact path")
    suite_summary = normalize_benchmark_catalog_suite_summary(catalog_snapshot.get("suiteSummary"))
    catalog_receipt = {
        "catalogSnapshot": {
            "artifactPath": catalog_path,
            "artifactSha256": _required_str(catalog_snapshot, "artifactSha256"),
            "artifactVersionId": _required_str(catalog_snapshot, "artifactVersionId"),
            "blockingSuiteIds": _required_str_list(catalog_snapshot, "blockingSuiteIds"),
            "catalogStatus": _required_str(catalog_snapshot, "catalogStatus"),
            "objectLockMode": _required_str(catalog_snapshot, "objectLockMode"),
            "officialHarnessLineage": normalize_benchmark_catalog_official_harness_lineage(
                catalog_snapshot.get("officialHarnessLineage")
            ),
            "suiteSummary": suite_summary,
        },
        "contractVersion": BENCHMARK_CATALOG_MATERIALIZER_CONTRACT_VERSION,
        "dagId": _required_str(payload, "dag_id"),
        "operationId": _required_str(payload, "operation_id"),
    }
    receipt = receipt_writer(
        artifact_path=receipt_path,
        artifact_type="benchmark_catalog_materialization_receipt",
        operation_id=_required_str(payload, "operation_id"),
        payload=catalog_receipt,
    )
    tracker.advance(
        phase="receipt-snapshot", pass_number=final_pass, byte_cursor=tracker.byte_cursor
    )
    if not isinstance(receipt, Mapping):
        raise ValueError(
            "benchmark catalog materialization receipt writer returned an invalid result"
        )
    result = dict(receipt)
    result["catalogSnapshot"] = catalog_receipt["catalogSnapshot"]
    tracker.advance(phase="complete", pass_number=final_pass, byte_cursor=tracker.byte_cursor)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    plan = _plan_payload(unquote(args.plan_json_urlencoded))
    receipt = materialize_benchmark_catalog_receipt(plan)
    catalog_snapshot = _required_mapping(receipt, "catalogSnapshot")
    print(
        json.dumps(
            {
                "artifactPath": _required_str(receipt, "artifactPath"),
                "artifactVersionId": _required_str(receipt, "artifactVersionId"),
                "catalogSnapshot": {
                    "artifactPath": _required_str(catalog_snapshot, "artifactPath"),
                    "artifactVersionId": _required_str(catalog_snapshot, "artifactVersionId"),
                    "blockingSuiteIds": _required_str_list(catalog_snapshot, "blockingSuiteIds"),
                    "catalogStatus": _required_str(catalog_snapshot, "catalogStatus"),
                    "objectLockMode": _required_str(catalog_snapshot, "objectLockMode"),
                    "suiteSummary": normalize_benchmark_catalog_suite_summary(
                        catalog_snapshot.get("suiteSummary")
                    ),
                },
                "objectLockMode": _required_str(receipt, "objectLockMode"),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Acquire mandatory benchmark catalog evidence in an isolated workload."
    )
    parser.add_argument("--plan-json-urlencoded", required=True)
    return parser


def _plan_payload(plan: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(plan, str):
        try:
            parsed = json.loads(plan)
        except json.JSONDecodeError as exc:
            raise ValueError("benchmark catalog materializer plan must be JSON") from exc
    else:
        parsed = plan
    if not isinstance(parsed, Mapping):
        raise ValueError("benchmark catalog materializer plan must be an object")
    return dict(parsed)


def _required_mapping(payload: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    value = payload.get(field_name)
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} is required")
    return value


def _required_str(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _required_str_list(payload: Mapping[str, Any], field_name: str) -> list[str]:
    value = payload.get(field_name)
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError(f"{field_name} must be a string list")
    return [item.strip() for item in value]


if __name__ == "__main__":
    raise SystemExit(main())
