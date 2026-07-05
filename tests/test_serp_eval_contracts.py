from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    SERP_NORMALIZED_GATE_FLOOR,
    build_benchmark_improvement_decision_cli_spec,
    build_benchmark_improvement_scoreboard_cli_spec,
    build_benchmark_improvement_wave_plan,
    build_improvement_candidate_eval_cli_spec,
    build_nightly_benchmark_export_cli_spec,
    build_nightly_registry_cli_spec,
    build_nightly_registry_submit_cli_spec,
    build_nightly_regression_plan,
    build_nightly_runner_cli_spec,
    build_tenant_golden_registry_cli_spec,
    build_tenant_golden_regression_plan,
    build_tenant_golden_runner_cli_spec,
    evaluate_nightly_regression_gate,
    evaluate_tenant_golden_gate,
    write_airflow_plan_artifact,
    write_nightly_benchmark_export_artifact,
    write_nightly_registry_receipts_artifact,
    write_nightly_registry_submissions_artifact,
    write_nightly_report_artifact,
)

TENANT_ID = "00000000-0000-4000-a000-000000000001"
PACK_VERSION_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
REGISTRY_RESOURCE_ID = "018f5e13-2d73-7a77-a052-8d1bcbf96541"
REPO_ROOT = Path(__file__).resolve().parents[1]


def test_build_nightly_regression_plan_requires_all_mandatory_suites() -> None:
    plan = build_nightly_regression_plan(_nightly_conf())
    repeated = build_nightly_regression_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_nightly_regression_suite"
    assert plan.payload["normalized_gate_floor"] == SERP_NORMALIZED_GATE_FLOOR
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "nightly_registry_submissions": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/nightly-registry-submissions.json"
        ),
        "nightly_registry_receipts": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/nightly-registry-receipts.json"
        ),
        "nightly_report": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/nightly-report.json"
        ),
        "benchmark_gate_export": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-gate-export.json"
        ),
        "suite_plan": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/suite-plan.json"
        ),
    }
    assert plan.payload["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_nightly_regression_plan",
        "run_mandatory_benchmark_suites",
        "build_c1_benchmark_gate_export",
        "build_bc21_benchmark_run_submissions",
        "submit_bc21_benchmark_run_submissions",
        "notify_governance_eval_surfaces",
    ]

    missing_suite_conf = _nightly_conf()
    missing_suite_conf["selected_suite_ids"] = list(
        MANDATORY_SERP_BENCHMARK_SUITES[:-1]
    )
    with pytest.raises(
        ValueError, match="selected_suite_ids must include every mandatory suite"
    ):
        build_nightly_regression_plan(missing_suite_conf)

    url_artifact_root = _nightly_conf()
    url_artifact_root["artifact_root_path"] = "https://example.invalid/serp-evals"
    with pytest.raises(ValueError, match="artifact_root_path must be an absolute path"):
        build_nightly_regression_plan(url_artifact_root)

    unsafe_bc21_base_url = _nightly_conf()
    unsafe_bc21_base_url["bc21_base_url"] = "http://example.invalid"
    with pytest.raises(ValueError, match="bc21_base_url must use https"):
        build_nightly_regression_plan(unsafe_bc21_base_url)


def test_build_nightly_gateway_cli_specs_are_file_based_and_deterministic() -> None:
    plan = build_nightly_regression_plan(_nightly_conf())
    runner = build_nightly_runner_cli_spec(plan.to_canonical_json())
    benchmark_export = build_nightly_benchmark_export_cli_spec(plan.to_canonical_json())
    submissions = build_nightly_registry_cli_spec(plan.to_canonical_json())
    submit = build_nightly_registry_submit_cli_spec(plan.to_canonical_json())

    assert runner["status"] == "ready_for_gateway_cli_runner"
    assert runner["task_id"] == "run_mandatory_benchmark_suites"
    assert runner["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "nightly-report",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--suite-plan",
        plan.payload["artifact_paths"]["suite_plan"],
    ]
    assert runner["stdout_path"] == plan.payload["artifact_paths"]["nightly_report"]
    assert runner["input_paths"] == [
        plan.payload["artifact_paths"]["airflow_plan"],
        plan.payload["artifact_paths"]["suite_plan"],
    ]

    assert benchmark_export["status"] == "ready_for_gateway_cli_runner"
    assert benchmark_export["task_id"] == "build_c1_benchmark_gate_export"
    assert benchmark_export["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "nightly-benchmark-export",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--nightly-report",
        plan.payload["artifact_paths"]["nightly_report"],
    ]
    assert (
        benchmark_export["stdout_path"]
        == plan.payload["artifact_paths"]["benchmark_gate_export"]
    )
    assert benchmark_export["input_paths"] == [
        plan.payload["artifact_paths"]["airflow_plan"],
        plan.payload["artifact_paths"]["nightly_report"],
    ]

    assert submissions["status"] == "ready_for_gateway_cli_runner"
    assert submissions["task_id"] == "build_bc21_benchmark_run_submissions"
    assert submissions["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "nightly-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--nightly-report",
        plan.payload["artifact_paths"]["nightly_report"],
    ]
    assert (
        submissions["stdout_path"]
        == plan.payload["artifact_paths"]["nightly_registry_submissions"]
    )

    assert submit["status"] == "ready_for_gateway_cli_runner"
    assert submit["task_id"] == "submit_bc21_benchmark_run_submissions"
    assert submit["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "submit-nightly-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--nightly-registry-submissions",
        plan.payload["artifact_paths"]["nightly_registry_submissions"],
        "--bc21-base-url",
        plan.payload["bc21_base_url"],
    ]
    assert submit["stdout_path"] == plan.payload["artifact_paths"]["nightly_registry_receipts"]


def test_nightly_d6_airflow_path_writes_gate_export_and_dry_run_receipts(
    tmp_path: Path,
) -> None:
    conf = _nightly_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_nightly_regression_plan(conf)
    plan_json = write_airflow_plan_artifact(plan)

    report_artifact = write_nightly_report_artifact(json.loads(plan_json))
    export_artifact = write_nightly_benchmark_export_artifact(report_artifact)
    submissions_artifact = write_nightly_registry_submissions_artifact(export_artifact)
    receipts_artifact = write_nightly_registry_receipts_artifact(submissions_artifact)

    report_path = Path(str(report_artifact["artifactPath"]))
    export_path = Path(str(export_artifact["artifactPath"]))
    receipts_path = Path(str(receipts_artifact["artifactPath"]))
    assert report_path.exists()
    assert export_path.exists()
    assert receipts_path.exists()

    export_payload = export_artifact["payload"]
    suite_codes = [item["suiteCode"] for item in export_payload["items"]]
    assert suite_codes == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert all(item["gateStatus"] == "passed" for item in export_payload["items"])
    assert all(
        float(item["normalizedScore"]) >= SERP_NORMALIZED_GATE_FLOOR
        for item in export_payload["items"]
    )
    assert all(item["benchmarkResultId"] for item in export_payload["items"])
    assert all(item["evidenceBundleId"] for item in export_payload["items"])

    stored_export = json.loads(export_path.read_text(encoding="utf-8"))
    assert stored_export == export_payload

    receipts_payload = receipts_artifact["payload"]
    assert receipts_payload["status"] == "dry_run_accepted"
    assert receipts_payload["dryRun"] is True
    assert len(receipts_payload["receipts"]) == len(MANDATORY_SERP_BENCHMARK_SUITES)


def test_evaluate_nightly_regression_gate_blocks_below_normalized_floor() -> None:
    gate = evaluate_nightly_regression_gate(
        {
            "suite_results": [
                {
                    "suite_id": "RAGBench",
                    "metric_results": [
                        {
                            "metric": "Recall@10",
                            "metric_family": "retrieval",
                            "normalized_score": 0.81,
                        }
                    ],
                },
                {
                    "suite_id": "APIBench",
                    "metric_results": [
                        {
                            "metric": "nDCG@10",
                            "metric_family": "retrieval",
                            "normalized_score": 0.74,
                        }
                    ],
                },
            ]
        }
    )

    assert gate["status"] == "blocked"
    assert gate["blocking_findings"] == [
        {
            "metric": "nDCG@10",
            "metric_family": "retrieval",
            "normalized_score": 0.74,
            "suite_id": "APIBench",
        }
    ]


def test_build_tenant_golden_regression_plan_preserves_workflow_provenance() -> None:
    plan = build_tenant_golden_regression_plan(_tenant_golden_conf())

    assert plan.payload["dag_id"] == "serp_tenant_golden_set_regression"
    assert plan.payload["tenant_id"] == TENANT_ID
    assert plan.payload["workflow_id"] == "workflow/private-course-authoring"
    assert plan.payload["golden_set_id"] == "tenant-public-course-authoring-golden"
    assert plan.payload["changed_pack_version_ids"] == [PACK_VERSION_ID]
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "golden_set": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/golden-set.json"
        ),
        "tenant_golden_registry_submissions": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/tenant-golden-registry-submissions.json"
        ),
        "tenant_golden_report": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/tenant-golden-report.json"
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_tenant_golden_regression_plan",
        "run_tenant_golden_set_cases",
        "build_tenant_golden_registry_submissions",
        "notify_governance_eval_surfaces",
    ]

    missing_workflow = _tenant_golden_conf()
    missing_workflow.pop("workflow_id")
    with pytest.raises(ValueError, match="workflow_id is required"):
        build_tenant_golden_regression_plan(missing_workflow)


def test_build_tenant_golden_gateway_cli_specs_are_file_based_and_deterministic() -> (
    None
):
    plan = build_tenant_golden_regression_plan(_tenant_golden_conf())
    runner = build_tenant_golden_runner_cli_spec(plan.to_canonical_json())
    submissions = build_tenant_golden_registry_cli_spec(plan.to_canonical_json())

    assert runner["status"] == "ready_for_gateway_cli_runner"
    assert runner["task_id"] == "run_tenant_golden_set_cases"
    assert runner["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "tenant-golden-report",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--golden-set",
        plan.payload["artifact_paths"]["golden_set"],
    ]
    assert (
        runner["stdout_path"] == plan.payload["artifact_paths"]["tenant_golden_report"]
    )

    assert submissions["status"] == "ready_for_gateway_cli_runner"
    assert submissions["task_id"] == "build_tenant_golden_registry_submissions"
    assert submissions["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "tenant-golden-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--tenant-golden-report",
        plan.payload["artifact_paths"]["tenant_golden_report"],
    ]
    assert (
        submissions["stdout_path"]
        == plan.payload["artifact_paths"]["tenant_golden_registry_submissions"]
    )


def test_evaluate_tenant_golden_gate_blocks_failed_metric_results() -> None:
    gate = evaluate_tenant_golden_gate(
        {
            "status": "blocked",
            "metric_results": [
                {
                    "metric": "Citation Accuracy",
                    "metric_family": "answer-quality",
                    "normalized_score": 1.0,
                    "status": "passed",
                },
                {
                    "metric": "Faithfulness",
                    "metric_family": "answer-quality",
                    "normalized_score": 0.82,
                    "status": "blocked",
                },
            ],
        }
    )

    assert gate["status"] == "blocked"
    assert gate["blocking_findings"] == [
        {
            "metric": "Faithfulness",
            "metric_family": "answer-quality",
            "normalized_score": 0.82,
            "status": "blocked",
        }
    ]


def test_build_benchmark_improvement_wave_plan_preserves_ratchet_contract() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    repeated = build_benchmark_improvement_wave_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_benchmark_improvement_wave"
    assert plan.payload["normalized_gate_floor"] == SERP_NORMALIZED_GATE_FLOOR
    assert plan.payload["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert plan.payload["improvement_spec_id"] == "improve-public-retrieval-reranker-v1"
    assert plan.payload["candidate_id"] == "candidate-reranker-v2"
    assert plan.payload["baseline_run_id"] == "evalrun_public_reranker_baseline_001"
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "candidate_eval_report": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/candidate-eval-report.json"
        ),
        "improvement_scoreboard": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/improvement-scoreboard.json"
        ),
        "improvement_spec": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/improvement-spec.json"
        ),
        "keep_discard_decision": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/keep-discard-decision.json"
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_benchmark_improvement_wave_plan",
        "run_targeted_benchmark_eval_harness",
        "decide_keep_or_discard_candidate",
        "publish_improvement_scoreboard",
        "notify_governance_eval_surfaces",
    ]

    missing_suite_conf = _improvement_wave_conf()
    missing_suite_conf["selected_suite_ids"] = list(MANDATORY_SERP_BENCHMARK_SUITES[:-1])
    with pytest.raises(
        ValueError, match="selected_suite_ids must include every mandatory suite"
    ):
        build_benchmark_improvement_wave_plan(missing_suite_conf)

    unbounded_budget_conf = _improvement_wave_conf()
    unbounded_budget_conf["max_benchmark_runs"] = 0
    with pytest.raises(ValueError, match="max_benchmark_runs must be positive"):
        build_benchmark_improvement_wave_plan(unbounded_budget_conf)


def test_build_benchmark_improvement_gateway_cli_specs_are_file_based_and_deterministic() -> None:
    plan = build_benchmark_improvement_wave_plan(_improvement_wave_conf())
    candidate_eval = build_improvement_candidate_eval_cli_spec(plan.to_canonical_json())
    decision = build_benchmark_improvement_decision_cli_spec(plan.to_canonical_json())
    scoreboard = build_benchmark_improvement_scoreboard_cli_spec(plan.to_canonical_json())

    assert candidate_eval["status"] == "ready_for_gateway_cli_runner"
    assert candidate_eval["task_id"] == "run_targeted_benchmark_eval_harness"
    assert candidate_eval["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "benchmark-improvement-candidate-eval",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--improvement-spec",
        plan.payload["artifact_paths"]["improvement_spec"],
    ]
    assert candidate_eval["stdout_path"] == plan.payload["artifact_paths"]["candidate_eval_report"]

    assert decision["status"] == "ready_for_gateway_cli_runner"
    assert decision["task_id"] == "decide_keep_or_discard_candidate"
    assert decision["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "benchmark-improvement-decision",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--improvement-spec",
        plan.payload["artifact_paths"]["improvement_spec"],
        "--candidate-eval-report",
        plan.payload["artifact_paths"]["candidate_eval_report"],
    ]
    assert decision["stdout_path"] == plan.payload["artifact_paths"]["keep_discard_decision"]

    assert scoreboard["status"] == "ready_for_gateway_cli_runner"
    assert scoreboard["task_id"] == "publish_improvement_scoreboard"
    assert scoreboard["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "benchmark-improvement-scoreboard",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--candidate-eval-report",
        plan.payload["artifact_paths"]["candidate_eval_report"],
        "--keep-discard-decision",
        plan.payload["artifact_paths"]["keep_discard_decision"],
    ]
    assert scoreboard["stdout_path"] == plan.payload["artifact_paths"]["improvement_scoreboard"]


@pytest.mark.parametrize(
    ("dag_file", "dag_id", "task_ids"),
    [
        (
            "serp_nightly_regression_suite.py",
            "serp_nightly_regression_suite",
            [
                "validate_nightly_regression_plan",
                "run_mandatory_benchmark_suites",
                "build_c1_benchmark_gate_export",
                "build_bc21_benchmark_run_submissions",
                "submit_bc21_benchmark_run_submissions",
                "notify_governance_eval_surfaces",
            ],
        ),
        (
            "serp_tenant_golden_set_regression.py",
            "serp_tenant_golden_set_regression",
            [
                "validate_tenant_golden_regression_plan",
                "run_tenant_golden_set_cases",
                "build_tenant_golden_registry_submissions",
                "notify_governance_eval_surfaces",
            ],
        ),
        (
            "serp_benchmark_improvement_wave.py",
            "serp_benchmark_improvement_wave",
            [
                "validate_benchmark_improvement_wave_plan",
                "run_targeted_benchmark_eval_harness",
                "decide_keep_or_discard_candidate",
                "publish_improvement_scoreboard",
                "notify_governance_eval_surfaces",
            ],
        ),
    ],
)
def test_serp_dag_files_declare_expected_airflow_contracts(
    dag_file: str,
    dag_id: str,
    task_ids: list[str],
) -> None:
    source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert _call_string_args(tree, "DAG")[0] == dag_id
    assert _keyword_values(tree, "PythonOperator", "task_id") == task_ids
    assert "external_runner_pending" not in source
    assert "registry_submission_pending" not in source
    assert "host.docker.internal" not in source
    assert "localhost" not in source
    assert "http://" not in source
    assert "https://" not in source


def test_serp_dag_files_import_helpers_from_packaged_dags_namespace() -> None:
    for dag_file in (
        "serp_nightly_regression_suite.py",
        "serp_tenant_golden_set_regression.py",
        "serp_benchmark_improvement_wave.py",
    ):
        source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")

        assert "from dags.serp_eval_contracts import" in source
        assert "from serp_eval_contracts import" not in source


def test_serp_nightly_dag_uses_artifact_writers_for_d6_path() -> None:
    source = (
        REPO_ROOT / "dags" / "serp_nightly_regression_suite.py"
    ).read_text(encoding="utf-8")

    assert "write_nightly_report_artifact" in source
    assert "write_nightly_benchmark_export_artifact" in source
    assert "write_nightly_registry_submissions_artifact" in source
    assert "write_nightly_registry_receipts_artifact" in source
    assert "build_nightly_benchmark_export_cli_spec" not in source


def test_airflowignore_excludes_non_dag_test_modules() -> None:
    airflowignore = REPO_ROOT / ".airflowignore"
    tests_airflowignore = REPO_ROOT / "tests" / ".airflowignore"

    assert airflowignore.exists()
    ignored_patterns = set(airflowignore.read_text(encoding="utf-8").splitlines())
    assert "tests/*" in ignored_patterns
    assert "tests/**" in ignored_patterns
    assert "**/tests/**" in ignored_patterns
    assert "tests/test_*.py" in ignored_patterns
    assert "**/test_*.py" in ignored_patterns
    assert ".*test_.*" in ignored_patterns
    assert "tests/.*" in ignored_patterns
    assert tests_airflowignore.exists()
    assert "*" in tests_airflowignore.read_text(encoding="utf-8").splitlines()


def _call_string_args(tree: ast.AST, function_name: str) -> list[str]:
    values: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _matches_call(node, function_name):
            continue
        for arg in node.args:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                values.append(arg.value)
    return values


def _keyword_values(tree: ast.AST, function_name: str, keyword_name: str) -> list[str]:
    values: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _matches_call(node, function_name):
            continue
        for keyword in node.keywords:
            if (
                keyword.arg == keyword_name
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ):
                values.append(keyword.value.value)
    return values


def _matches_call(node: ast.Call, function_name: str) -> bool:
    return (
        isinstance(node.func, ast.Name)
        and node.func.id == function_name
        or isinstance(node.func, ast.Attribute)
        and node.func.attr == function_name
    )


def _nightly_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "bc21_base_url": "http://serp-context-platform.env-dev.svc.cluster.local",
        "generated_at": "2026-07-05T21:00:00Z",
        "pack_version_ids": [PACK_VERSION_ID],
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "reranker_profile_version": "reranker@2026.07.1",
        "retrieval_profile_version": "hybrid@2026.07.1",
        "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
        "tenant_id": TENANT_ID,
    }


def _tenant_golden_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "changed_pack_version_ids": [PACK_VERSION_ID],
        "generated_at": "2026-07-05T21:00:00Z",
        "golden_set_id": "tenant-public-course-authoring-golden",
        "golden_set_version": "golden@2026.07.1",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "tenant_id": TENANT_ID,
        "workflow_id": "workflow/private-course-authoring",
    }


def _improvement_wave_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "baseline_run_id": "evalrun_public_reranker_baseline_001",
        "candidate_id": "candidate-reranker-v2",
        "generated_at": "2026-07-05T21:00:00Z",
        "improvement_spec_id": "improve-public-retrieval-reranker-v1",
        "max_benchmark_runs": 12,
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "rollback_policy_ref": "policy://rollback/last-validated-baseline@v1",
        "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
        "tenant_id": TENANT_ID,
    }
