"""Fail-closed BC-10 ledger capacity admission for D19."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

_REQUIRED_METRICS = (
    "bc10_ledger_capacity_admitted",
    "bc10_ledger_record_headroom",
    "bc10_ledger_storage_headroom_bytes",
    "bc10_ledger_cold_wave_required_records",
    "bc10_ledger_cold_wave_required_bytes",
    "bc10_ledger_legacy_payload_records",
)

_CAPACITY_RETRY_DELAYS_SECONDS = (0.25, 1.0)


def validate_bc10_capacity_metrics(metrics_text: str) -> dict[str, object]:
    """Validate one unlabeled, internally consistent Prometheus snapshot."""
    values: dict[str, int] = {}
    for raw_line in metrics_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or " " not in line:
            continue
        name, raw_value = line.rsplit(None, 1)
        if name not in _REQUIRED_METRICS:
            continue
        if name in values:
            raise ValueError(f"BC-10 capacity metric is duplicated: {name}")
        try:
            numeric = float(raw_value)
        except ValueError as exc:
            raise ValueError(f"BC-10 capacity metric is invalid: {name}") from exc
        if numeric < 0 or not numeric.is_integer():
            raise ValueError(f"BC-10 capacity metric is invalid: {name}")
        values[name] = int(numeric)
    missing = [name for name in _REQUIRED_METRICS if name not in values]
    if missing:
        raise ValueError("BC-10 capacity telemetry is incomplete: " + ", ".join(missing))
    if values["bc10_ledger_legacy_payload_records"] != 0:
        raise ValueError("BC-10 legacy replay payload compaction is incomplete")
    if values["bc10_ledger_record_headroom"] < values["bc10_ledger_cold_wave_required_records"]:
        raise ValueError("BC-10 record headroom cannot admit one cold all-nine wave")
    if (
        values["bc10_ledger_storage_headroom_bytes"]
        < values["bc10_ledger_cold_wave_required_bytes"]
    ):
        raise ValueError("BC-10 byte headroom cannot admit one cold all-nine wave")
    if values["bc10_ledger_capacity_admitted"] != 1:
        raise ValueError("BC-10 governed capacity model rejected D19")
    return {
        "schema": "Bc10LedgerCapacityAdmission/v1",
        "admitted": True,
        "recordHeadroom": values["bc10_ledger_record_headroom"],
        "storageHeadroomBytes": values["bc10_ledger_storage_headroom_bytes"],
        "coldWaveRequiredRecords": values["bc10_ledger_cold_wave_required_records"],
        "coldWaveRequiredBytes": values["bc10_ledger_cold_wave_required_bytes"],
        "legacyPayloadRecords": 0,
        "observedAt": datetime.now(UTC).isoformat(),
    }


def validate_bc10_capacity_admission(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate the fresh gateway-owned admission contract used by D19."""
    if payload.get("schema") != "Bc10LedgerCapacityAdmission/v1":
        raise ValueError("BC-10 capacity admission schema is unsupported")
    blockers = payload.get("blockers")
    if not isinstance(blockers, list) or any(
        not isinstance(blocker, str) or not blocker for blocker in blockers
    ):
        raise ValueError("BC-10 capacity admission blockers are malformed")
    integer_fields = (
        "activeRecords",
        "relationBytes",
        "recordHeadroom",
        "storageHeadroomBytes",
    )
    for field in integer_fields:
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("BC-10 capacity admission counters are malformed")
    if payload.get("admitted") is not True or blockers:
        raise ValueError("BC-10 governed capacity model rejected D19")
    if not isinstance(payload.get("observedAt"), str) or not payload["observedAt"]:
        raise ValueError("BC-10 capacity admission observation time is missing")
    return dict(payload)


def verify_bc10_ledger_capacity_admission() -> dict[str, object]:
    """Fetch BC-10 telemetry and stop D19 before any expensive work."""
    gateway_url = os.environ.get("ADAPSTORY_BC10_GATEWAY_URL", "").strip().rstrip("/")
    parsed = urlsplit(gateway_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.fragment:
        raise ValueError("ADAPSTORY_BC10_GATEWAY_URL is unavailable or invalid")
    request = Request(
        f"{gateway_url}/api/v1/capacity",
        headers={"Accept": "application/json"},
    )
    body: object | None = None
    for attempt in range(len(_CAPACITY_RETRY_DELAYS_SECONDS) + 1):
        try:
            with urlopen(request, timeout=10) as response:
                if response.status != 200:
                    raise RuntimeError(
                        "BC-10 capacity telemetry request was not successful"
                    )
                try:
                    body = json.loads(response.read())
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        "BC-10 capacity admission response is invalid JSON"
                    ) from exc
            break
        except HTTPError:
            # An HTTP response proves the request reached BC-10; its rejection
            # is authoritative and must not be hidden by transport retries.
            raise
        except (TimeoutError, URLError, OSError) as exc:
            if attempt >= len(_CAPACITY_RETRY_DELAYS_SECONDS):
                raise RuntimeError(
                    "BC-10 capacity telemetry transport remained unavailable"
                ) from exc
            # GET is side-effect free. Reuse the exact Request object so a
            # transient pre-response failure cannot create a new identity.
            time.sleep(_CAPACITY_RETRY_DELAYS_SECONDS[attempt])
    if not isinstance(body, dict):
        raise ValueError("BC-10 capacity admission response must be an object")
    return validate_bc10_capacity_admission(body)
