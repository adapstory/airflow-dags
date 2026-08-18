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


def test_d19_supervisors_stay_home_while_controller_owned_work_runs_remote() -> None:
    source = (Path(__file__).parents[1] / "dags" / "serp_benchmark_improvement_wave.py").read_text(
        encoding="utf-8"
    )

    assert "def d19_remote_kubernetes_pod_launcher_executor_config()" not in source
    assert "executor_config=d19_remote_kubernetes_pod_launcher_executor_config()," not in source
    assert source.count("executor_config=kubernetes_pod_launcher_executor_config(),") == 13

    classifier = source.split("classify_pack_cas =", maxsplit=1)[1].split(
        "pack_cas_readiness_gate =", maxsplit=1
    )[0]
    assert "BoundedKubernetesJobOperator(" in classifier
    assert "node_selector=D19_REMOTE_COMPUTE_NODE_SELECTOR," in classifier
    assert "tolerations=D19_REMOTE_COMPUTE_TOLERATIONS," in classifier

    standard_runner = source.split("runner =", maxsplit=1)[1].split(
        "D19_STANDARD_HARNESS_RUN_TASKS[identity]", maxsplit=1
    )[0]
    assert "BoundedKubernetesJobOperator(" in standard_runner
    assert "node_selector=D19_REMOTE_COMPUTE_NODE_SELECTOR," in standard_runner
    assert "tolerations=D19_REMOTE_COMPUTE_TOLERATIONS," in standard_runner
