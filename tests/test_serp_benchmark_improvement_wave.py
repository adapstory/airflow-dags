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


def test_pack_builders_have_bounded_checkpoint_aware_execution_recovery() -> None:
    source = (Path(__file__).parents[1] / "dags" / "serp_benchmark_improvement_wave.py").read_text(
        encoding="utf-8"
    )
    builder = source.split(
        "D19_PACK_SIDE_BUILD_TASKS[(suite_id, side)] = BoundedKubernetesJobOperator(",
        maxsplit=1,
    )[1].split("_D19_PACK_SIDE_RESULT_URIS_JSON", maxsplit=1)[0]
    assert "backoff_limit=1," in builder
    assert "retries=2," in builder
    assert "retry_delay=timedelta(minutes=1)," in builder
    assert "retry_exponential_backoff=True," in builder
    assert "max_retry_delay=timedelta(minutes=2)," in builder


def test_pack_builder_job_policy_retries_only_typed_infrastructure_failures() -> None:
    source = (Path(__file__).parents[1] / "dags" / "serp_benchmark_improvement_wave.py").read_text(
        encoding="utf-8"
    )
    builder = source.split(
        "D19_PACK_SIDE_BUILD_TASKS[(suite_id, side)] = BoundedKubernetesJobOperator(",
        maxsplit=1,
    )[1].split("_D19_PACK_SIDE_RESULT_URIS_JSON", maxsplit=1)[0]

    assert "pod_failure_policy=D19_PACK_BUILDER_POD_FAILURE_POLICY," in builder
    assert 'action="Count"' in source
    assert 'type="DisruptionTarget"' in source
    assert 'operator="In"' in source
    assert "values=[137, 143]" in source
    assert 'action="FailJob"' in source
    assert 'operator="NotIn"' in source


def test_swe_pack_sides_prefer_parallel_placement_across_both_compute_nodes() -> None:
    source = (Path(__file__).parents[1] / "dags" / "serp_benchmark_improvement_wave.py").read_text(
        encoding="utf-8"
    )
    builder = source.split(
        "D19_PACK_SIDE_BUILD_TASKS[(suite_id, side)] = BoundedKubernetesJobOperator(",
        maxsplit=1,
    )[1].split("_D19_PACK_SIDE_RESULT_URIS_JSON", maxsplit=1)[0]
    normalized_builder = " ".join(builder.split())

    assert '"adapstory.com/serp-pack-suite": artifact_slug' in builder
    assert (
        'None if suite_id == "SWE-bench Verified" else D19_REMOTE_COMPUTE_NODE_SELECTOR'
        in normalized_builder
    )
    assert (
        'D19_SWE_PACK_BUILDER_AFFINITY if suite_id == "SWE-bench Verified" else None'
        in normalized_builder
    )
    assert 'topology_key="kubernetes.io/hostname"' in source
    assert 'values=["swe-bench-verified"]' in source
