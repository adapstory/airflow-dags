"""Isolated executor for immutable mandatory-benchmark catalog acquisition."""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import unquote

from dags.serp_eval_contracts import (
    materialize_live_benchmark_catalog_artifact,
    normalize_benchmark_catalog_official_harness_lineage,
    normalize_benchmark_catalog_suite_summary,
    write_immutable_evidence_snapshot,
)

BENCHMARK_CATALOG_MATERIALIZER_CONTRACT_VERSION = "serp-benchmark-catalog-materializer/v4"

CatalogMaterializer = Callable[[Mapping[str, Any] | str], dict[str, Any]]
ReceiptWriter = Callable[..., dict[str, Any]]


def materialize_benchmark_catalog_receipt(
    plan: Mapping[str, Any] | str,
    *,
    catalog_materializer: CatalogMaterializer = materialize_live_benchmark_catalog_artifact,
    receipt_writer: ReceiptWriter = write_immutable_evidence_snapshot,
) -> dict[str, Any]:
    """Fetch external catalog inputs and seal the resulting snapshot in a WORM receipt."""

    payload = _plan_payload(plan)
    artifact_paths = _required_mapping(payload, "artifact_paths")
    catalog_path = _required_str(artifact_paths, "benchmark_catalog")
    receipt_path = _required_str(artifact_paths, "benchmark_catalog_receipt")
    catalog_snapshot = catalog_materializer(payload)
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
    if not isinstance(receipt, Mapping):
        raise ValueError(
            "benchmark catalog materialization receipt writer returned an invalid result"
        )
    result = dict(receipt)
    result["catalogSnapshot"] = catalog_receipt["catalogSnapshot"]
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
