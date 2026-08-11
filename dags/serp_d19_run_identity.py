"""Canonical fail-closed identity binding for D19 Airflow runs."""

from __future__ import annotations

import re

_OPERATOR_TRIGGERED_RUN_ID_PREFIXES = ("event_d6_d19__", "d6__")
_MANUAL_RUN_ID_PREFIX = "manual__"
_AUTHENTICATED_BOOTSTRAP_RUN_ID = re.compile(
    r"d19_(?:canary__[0-9a-f]{32}__01|governance__[0-9a-f]{32}__0[1-3])"
    r"__attempt_(?!000000)[0-9]{6}\Z"
)


def normalize_d19_run_type(*, run_id: str, run_type: str) -> str:
    """Bind the observable Airflow run type to the only supported run-id families."""

    # Context: Airflow 3.3 records every TriggerDagRunOperator child as
    # operator_triggered, including both D17 event-D6 and scheduled-D6 D19 runs.
    # Decision: bind the two deterministic native child prefixes exclusively to
    # operator_triggered, and bind manual__ plus authenticated bootstrap attempt
    # identities exclusively to manual.
    # Reason: accepting either type independently would permit provenance spoofing,
    # while requiring manual makes the canonical native trigger graph impossible.
    # Revisit when: D19 gains another authenticated trigger mechanism with its own
    # deterministic run-id family and an explicit migration of all producers.
    if run_id.startswith(_OPERATOR_TRIGGERED_RUN_ID_PREFIXES):
        expected_run_type = "operator_triggered"
    elif run_id.startswith(_MANUAL_RUN_ID_PREFIX) or _AUTHENTICATED_BOOTSTRAP_RUN_ID.fullmatch(
        run_id
    ):
        expected_run_type = "manual"
    else:
        raise ValueError("D19 airflowRun runId and runType provenance is unsupported")
    if run_type != expected_run_type:
        raise ValueError("D19 airflowRun runId and runType provenance is unsupported")
    return expected_run_type
