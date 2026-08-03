"""Canonical fail-closed identity binding for D19 Airflow runs."""

from __future__ import annotations

_OPERATOR_TRIGGERED_RUN_ID_PREFIXES = ("event_d6_d19__", "d6__")
_MANUAL_RUN_ID_PREFIX = "manual__"


def normalize_d19_run_type(*, run_id: str, run_type: str) -> str:
    """Bind the observable Airflow run type to the only supported run-id families."""

    # Context: Airflow 3.3 records every TriggerDagRunOperator child as
    # operator_triggered, including both D17 event-D6 and scheduled-D6 D19 runs.
    # Decision: bind the two deterministic native child prefixes exclusively to
    # operator_triggered, and bind Airflow/manual callers exclusively to manual__.
    # Reason: accepting either type independently would permit provenance spoofing,
    # while requiring manual makes the canonical native trigger graph impossible.
    # Revisit when: D19 gains another authenticated trigger mechanism with its own
    # deterministic run-id family and an explicit migration of all producers.
    if run_id.startswith(_OPERATOR_TRIGGERED_RUN_ID_PREFIXES):
        expected_run_type = "operator_triggered"
    elif run_id.startswith(_MANUAL_RUN_ID_PREFIX):
        expected_run_type = "manual"
    else:
        raise ValueError("D19 airflowRun runId and runType provenance is unsupported")
    if run_type != expected_run_type:
        raise ValueError("D19 airflowRun runId and runType provenance is unsupported")
    return expected_run_type
