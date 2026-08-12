from __future__ import annotations

from pathlib import Path


def test_bc10_capacity_admission_precedes_expensive_d19_work() -> None:
    source = (Path(__file__).parents[1] / "dags" / "serp_benchmark_improvement_wave.py").read_text(
        encoding="utf-8"
    )

    assert "validate_plan\n    >> verify_bc10_ledger_capacity\n    >> materialize_catalog" in source
    assert source.index('task_id="verify_bc10_ledger_capacity_admission"') < source.index(
        'task_id="materialize_live_benchmark_catalog"'
    )
