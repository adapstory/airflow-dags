from __future__ import annotations

import ast
import io
import json
from hashlib import sha256
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
    build_online_eval_registry_cli_spec,
    build_online_eval_rollup_cli_spec,
    build_online_eval_rollup_plan,
    build_public_docs_seed_refresh_plan,
    build_tenant_golden_registry_cli_spec,
    build_tenant_golden_regression_plan,
    build_tenant_golden_runner_cli_spec,
    evaluate_nightly_regression_gate,
    evaluate_tenant_golden_gate,
    execute_gateway_cli_spec,
    write_airflow_plan_artifact,
    write_benchmark_improvement_decision_artifact,
    write_benchmark_improvement_scoreboard_artifact,
    write_improvement_candidate_eval_artifact,
    write_improvement_spec_artifact,
    write_nightly_benchmark_export_artifact,
    write_nightly_registry_receipts_artifact,
    write_nightly_registry_submissions_artifact,
    write_nightly_report_artifact,
    write_nightly_suite_plan_artifact,
    write_online_eval_rollup_plan_artifact,
    write_public_docs_seed_refresh_plan_artifact,
    write_public_docs_seed_registry_artifact,
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
            "/var/opt/adapstory/serp-evals/" f"{plan.payload['operation_id']}/airflow-plan.json"
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
            "/var/opt/adapstory/serp-evals/" f"{plan.payload['operation_id']}/nightly-report.json"
        ),
        "benchmark_gate_export": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-gate-export.json"
        ),
        "suite_plan": (
            "/var/opt/adapstory/serp-evals/" f"{plan.payload['operation_id']}/suite-plan.json"
        ),
    }
    assert plan.payload["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_nightly_regression_plan",
        "write_nightly_suite_plan",
        "run_mandatory_benchmark_suites",
        "build_c1_benchmark_gate_export",
        "build_bc21_benchmark_run_submissions",
        "submit_bc21_benchmark_run_submissions",
        "notify_governance_eval_surfaces",
    ]

    missing_suite_conf = _nightly_conf()
    missing_suite_conf["selected_suite_ids"] = list(MANDATORY_SERP_BENCHMARK_SUITES[:-1])
    with pytest.raises(ValueError, match="selected_suite_ids must include every mandatory suite"):
        build_nightly_regression_plan(missing_suite_conf)

    url_artifact_root = _nightly_conf()
    url_artifact_root["artifact_root_path"] = "https://example.invalid/serp-evals"
    with pytest.raises(
        ValueError, match="artifact_root_path must be an absolute path or s3:// URI"
    ):
        build_nightly_regression_plan(url_artifact_root)

    unsafe_bc21_base_url = _nightly_conf()
    unsafe_bc21_base_url["bc21_base_url"] = "http://example.invalid"
    with pytest.raises(ValueError, match="bc21_base_url must use https"):
        build_nightly_regression_plan(unsafe_bc21_base_url)


def test_build_nightly_regression_plan_accepts_s3_artifact_root_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _nightly_conf()
    conf.pop("artifact_root_path")
    monkeypatch.setenv(
        "ADAPSTORY_AIRFLOW_ARTIFACT_ROOT",
        "s3://airflow-serp-artifacts/serp-evals",
    )

    plan = build_nightly_regression_plan(conf)

    assert plan.payload["artifact_root_path"] == "s3://airflow-serp-artifacts/serp-evals"
    assert plan.payload["artifact_paths"]["airflow_plan"].startswith(
        "s3://airflow-serp-artifacts/serp-evals/"
    )


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
        benchmark_export["stdout_path"] == plan.payload["artifact_paths"]["benchmark_gate_export"]
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
        submissions["stdout_path"] == plan.payload["artifact_paths"]["nightly_registry_submissions"]
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


def test_nightly_d6_airflow_path_writes_suite_plan_for_gateway_runner(
    tmp_path: Path,
) -> None:
    conf = _nightly_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_nightly_regression_plan(conf)
    plan_json = write_airflow_plan_artifact(plan)

    suite_plan_artifact = write_nightly_suite_plan_artifact(json.loads(plan_json))

    suite_plan_path = Path(str(suite_plan_artifact["artifactPath"]))
    assert suite_plan_path.exists()
    suite_plan = suite_plan_artifact["payload"]
    assert suite_plan["contract_version"] == "2026.07.2"
    assert suite_plan["schedule_id"] == "serp_nightly_regression_suite"
    assert suite_plan["selected_suite_ids"] == list(MANDATORY_SERP_BENCHMARK_SUITES)
    assert [suite["suite_id"] for suite in suite_plan["suites"]] == list(
        MANDATORY_SERP_BENCHMARK_SUITES
    )
    assert all(len(suite["references"]) == 4 for suite in suite_plan["suites"])
    assert all(len(suite["metric_observations"]) == 3 for suite in suite_plan["suites"])


def test_build_online_eval_rollup_plan_materializes_d7_contract(tmp_path: Path) -> None:
    conf = _online_eval_rollup_conf()
    conf["artifact_root_path"] = str(tmp_path)

    plan = build_online_eval_rollup_plan(conf)
    repeated = build_online_eval_rollup_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_online_eval_rollup"
    assert plan.payload["normalized_gate_floor"] == SERP_NORMALIZED_GATE_FLOOR
    assert plan.payload["capacity_readiness_state"] == "ready_for_po_capacity_approval"
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": (
            "/".join((str(tmp_path), plan.payload["operation_id"], "airflow-plan.json"))
        ),
        "online_eval_registry_submissions": (
            "/".join(
                (
                    str(tmp_path),
                    plan.payload["operation_id"],
                    "online-eval-registry-submissions.json",
                )
            )
        ),
        "online_eval_rollup": (
            "/".join((str(tmp_path), plan.payload["operation_id"], "online-eval-rollup.json"))
        ),
        "online_eval_rollup_plan": (
            "/".join((str(tmp_path), plan.payload["operation_id"], "online-eval-rollup-plan.json"))
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_online_eval_rollup_plan",
        "write_online_eval_rollup_plan",
        "build_online_eval_rollup",
        "build_online_eval_registry_submissions",
        "notify_governance_eval_surfaces",
    ]

    rollup_plan_artifact = write_online_eval_rollup_plan_artifact(plan.to_canonical_json())
    rollup_plan_path = Path(plan.payload["artifact_paths"]["online_eval_rollup_plan"])
    assert rollup_plan_path.exists()
    rollup_plan = json.loads(rollup_plan_path.read_text(encoding="utf-8"))
    assert rollup_plan_artifact["artifactType"] == "online_eval_rollup_plan"
    assert rollup_plan["rollup_id"] == "serp_online_eval_rollup"
    assert rollup_plan["reports"] == conf["reports"]


def test_build_online_eval_rollup_gateway_cli_specs_are_file_based(tmp_path: Path) -> None:
    conf = _online_eval_rollup_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_online_eval_rollup_plan(conf)
    rollup = build_online_eval_rollup_cli_spec(plan.to_canonical_json())
    submissions = build_online_eval_registry_cli_spec(plan.to_canonical_json())

    assert rollup["status"] == "ready_for_gateway_cli_runner"
    assert rollup["task_id"] == "build_online_eval_rollup"
    assert rollup["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "online-eval-rollup",
        "--rollup-plan",
        plan.payload["artifact_paths"]["online_eval_rollup_plan"],
    ]
    assert rollup["input_paths"] == [plan.payload["artifact_paths"]["online_eval_rollup_plan"]]
    assert rollup["stdout_path"] == plan.payload["artifact_paths"]["online_eval_rollup"]

    assert submissions["status"] == "ready_for_gateway_cli_runner"
    assert submissions["task_id"] == "build_online_eval_registry_submissions"
    assert submissions["argv"] == [
        "python",
        "-m",
        "adapstory_serp_mcp_gateway.airflow_eval_cli",
        "online-eval-registry-submissions",
        "--airflow-plan",
        plan.payload["artifact_paths"]["airflow_plan"],
        "--online-eval-rollup",
        plan.payload["artifact_paths"]["online_eval_rollup"],
    ]
    assert submissions["input_paths"] == [
        plan.payload["artifact_paths"]["airflow_plan"],
        plan.payload["artifact_paths"]["online_eval_rollup"],
    ]
    assert (
        submissions["stdout_path"]
        == plan.payload["artifact_paths"]["online_eval_registry_submissions"]
    )


def test_execute_gateway_cli_spec_runs_without_shell_and_persists_stdout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "airflow-plan.json"
    output_path = tmp_path / "nightly-report.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = {"status": "accepted", "tenant_id": TENANT_ID}
    calls: list[object] = []

    def fake_run(
        argv: list[str],
        *,
        capture_output: bool,
        check: bool,
        text: bool,
    ) -> object:
        calls.append((argv, capture_output, check, text))

        class Result:
            returncode = 0
            stdout = json.dumps(payload)
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_gateway_cli_spec(
        {
            "argv": ["python", "-m", "safe.module", "run"],
            "contract_version": "serp-airflow-gateway-cli-bridge/v1",
            "dag_id": "serp_nightly_regression_suite",
            "input_paths": [str(input_path)],
            "operation_id": "op-1",
            "status": "ready_for_gateway_cli_runner",
            "stdout_path": str(output_path),
            "task_id": "run_mandatory_benchmark_suites",
            "tenant_id": TENANT_ID,
        }
    )

    assert calls == [(["python", "-m", "safe.module", "run"], True, False, True)]
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == payload
    assert result["payload"] == payload
    assert result["artifactPath"] == str(output_path)


def test_execute_gateway_cli_spec_materializes_s3_inputs_and_uploads_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = {
        ("airflow-serp-artifacts", "serp-evals/input/airflow-plan.json"): b"{}",
    }
    payload = {"status": "accepted", "tenant_id": TENANT_ID}
    put_calls: list[tuple[str, str, str, str]] = []
    run_calls: list[tuple[list[str], bool, bool, bool]] = []

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            storage[(Bucket, Key)] = Body
            put_calls.append(("put_object", Bucket, Key, ContentType))

    def fake_run(
        argv: list[str],
        *,
        capture_output: bool,
        check: bool,
        text: bool,
    ) -> object:
        run_calls.append((argv, capture_output, check, text))
        assert argv[-1].startswith("/")
        assert not argv[-1].startswith("s3://")
        assert Path(argv[-1]).read_text(encoding="utf-8") == "{}"

        class Result:
            returncode = 0
            stdout = json.dumps(payload)
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())
    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_gateway_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "safe.module",
                "--airflow-plan",
                "s3://airflow-serp-artifacts/serp-evals/input/airflow-plan.json",
            ],
            "contract_version": "serp-airflow-gateway-cli-bridge/v1",
            "dag_id": "serp_nightly_regression_suite",
            "input_paths": ["s3://airflow-serp-artifacts/serp-evals/input/airflow-plan.json"],
            "operation_id": "op-1",
            "status": "ready_for_gateway_cli_runner",
            "stdout_path": "s3://airflow-serp-artifacts/serp-evals/output/nightly-report.json",
            "task_id": "run_mandatory_benchmark_suites",
            "tenant_id": TENANT_ID,
        }
    )

    assert len(run_calls) == 1
    argv, capture_output, check, text = run_calls[0]
    assert argv[:4] == ["python", "-m", "safe.module", "--airflow-plan"]
    assert argv[4].startswith("/")
    assert not argv[4].startswith("s3://")
    assert capture_output is True
    assert check is False
    assert text is True
    assert put_calls == [
        (
            "put_object",
            "airflow-serp-artifacts",
            "serp-evals/output/nightly-report.json",
            "application/json",
        )
    ]
    assert (
        json.loads(storage[("airflow-serp-artifacts", "serp-evals/output/nightly-report.json")])
        == payload
    )
    assert result["payload"] == payload
    assert (
        result["artifactPath"]
        == "s3://airflow-serp-artifacts/serp-evals/output/nightly-report.json"
    )


def test_explicit_nightly_dry_run_fallback_still_writes_marked_receipts(
    tmp_path: Path,
) -> None:
    conf = _nightly_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan_json = write_airflow_plan_artifact(build_nightly_regression_plan(conf))
    report_artifact = write_nightly_report_artifact(json.loads(plan_json))
    export_artifact = write_nightly_benchmark_export_artifact(report_artifact)
    submissions_artifact = write_nightly_registry_submissions_artifact(export_artifact)
    receipts_artifact = write_nightly_registry_receipts_artifact(submissions_artifact)

    receipts_payload = receipts_artifact["payload"]
    assert receipts_payload["status"] == "dry_run_accepted"
    assert receipts_payload["dryRun"] is True


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
            "/var/opt/adapstory/serp-evals/" f"{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "golden_set": (
            "/var/opt/adapstory/serp-evals/" f"{plan.payload['operation_id']}/golden-set.json"
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


def test_build_tenant_golden_gateway_cli_specs_are_file_based_and_deterministic() -> None:
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
    assert runner["stdout_path"] == plan.payload["artifact_paths"]["tenant_golden_report"]

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
            "/var/opt/adapstory/serp-evals/" f"{plan.payload['operation_id']}/airflow-plan.json"
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
            "/var/opt/adapstory/serp-evals/" f"{plan.payload['operation_id']}/improvement-spec.json"
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
    with pytest.raises(ValueError, match="selected_suite_ids must include every mandatory suite"):
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


def test_write_benchmark_improvement_wave_artifacts_persist_keep_decision(
    tmp_path: Path,
) -> None:
    conf = _improvement_wave_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_benchmark_improvement_wave_plan(conf)

    plan_json = write_airflow_plan_artifact(plan)
    spec_artifact = write_improvement_spec_artifact(json.loads(plan_json))
    candidate_artifact = write_improvement_candidate_eval_artifact(spec_artifact)
    decision_artifact = write_benchmark_improvement_decision_artifact(candidate_artifact)
    scoreboard_artifact = write_benchmark_improvement_scoreboard_artifact(decision_artifact)

    spec_path = Path(str(spec_artifact["artifactPath"]))
    candidate_path = Path(str(candidate_artifact["artifactPath"]))
    decision_path = Path(str(decision_artifact["artifactPath"]))
    scoreboard_path = Path(str(scoreboard_artifact["artifactPath"]))

    assert spec_path.exists()
    assert candidate_path.exists()
    assert decision_path.exists()
    assert scoreboard_path.exists()
    assert spec_artifact["payload"]["status"] == "ready"
    assert spec_artifact["payload"]["dryRun"] is True
    assert spec_artifact["payload"]["replay"] == {
        "baselineRunId": conf["baseline_run_id"],
        "candidateRunId": f"{conf['candidate_id']}-dry-run",
        "featureFlags": conf["feature_flags"],
        "guardrailBundleVersion": conf["guardrail_bundle_version"],
        "judgeModelId": conf["judge_model_id"],
        "judgeModelVersion": conf["judge_model_version"],
        "judgePromptTemplateVersion": conf["judge_prompt_template_version"],
        "modelCatalogEntryId": conf["model_catalog_entry_id"],
        "policyBundleVersion": conf["policy_bundle_version"],
        "providerRouteId": conf["provider_route_id"],
        "rerankerProfileVersion": conf["reranker_profile_version"],
        "retrievalProfileVersion": conf["retrieval_profile_version"],
    }
    assert spec_artifact["payload"]["modelGovernance"] == {
        "guardrailBundleVersion": conf["guardrail_bundle_version"],
        "judgeModelId": conf["judge_model_id"],
        "judgeModelVersion": conf["judge_model_version"],
        "modelCatalogEntryId": conf["model_catalog_entry_id"],
        "policyBundleVersion": conf["policy_bundle_version"],
        "providerRouteId": conf["provider_route_id"],
        "status": "approved-for-eval-dry-run",
    }
    assert candidate_artifact["payload"]["dryRun"] is True
    assert candidate_artifact["payload"]["replay"] == spec_artifact["payload"]["replay"]
    assert (
        candidate_artifact["payload"]["modelGovernance"]
        == spec_artifact["payload"]["modelGovernance"]
    )
    assert candidate_artifact["payload"]["status"] == "passed"
    assert candidate_artifact["payload"]["mandatorySuiteCount"] == len(
        MANDATORY_SERP_BENCHMARK_SUITES
    )
    assert candidate_artifact["payload"]["normalizedGateFloor"] == "0.7500"
    assert decision_artifact["payload"]["decision"] == "keep"
    assert decision_artifact["payload"]["dryRun"] is True
    assert decision_artifact["payload"]["replay"] == spec_artifact["payload"]["replay"]
    assert (
        decision_artifact["payload"]["modelGovernance"]
        == spec_artifact["payload"]["modelGovernance"]
    )
    assert decision_artifact["payload"]["status"] == "accepted"
    assert scoreboard_artifact["payload"]["status"] == "published"
    assert scoreboard_artifact["payload"]["dryRun"] is True
    assert scoreboard_artifact["payload"]["replay"] == spec_artifact["payload"]["replay"]
    assert (
        scoreboard_artifact["payload"]["modelGovernance"]
        == spec_artifact["payload"]["modelGovernance"]
    )
    assert scoreboard_artifact["payload"]["latestDecision"] == "keep"
    assert scoreboard_artifact["payload"]["artifact_paths"] == plan.payload["artifact_paths"]


def test_write_benchmark_improvement_wave_decision_fails_below_floor(
    tmp_path: Path,
) -> None:
    conf = _improvement_wave_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan_json = write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))
    spec_artifact = write_improvement_spec_artifact(json.loads(plan_json))
    candidate_artifact = write_improvement_candidate_eval_artifact(spec_artifact)
    candidate_artifact["payload"]["candidateScore"] = "0.7400"
    candidate_artifact["payload"]["suiteResults"][0]["normalizedScore"] = "0.7400"
    _refresh_artifact_sha256(candidate_artifact)

    with pytest.raises(ValueError, match="improvement candidate score is below gate floor"):
        write_benchmark_improvement_decision_artifact(candidate_artifact)


def test_write_benchmark_improvement_wave_rejects_tampered_artifact_payload(
    tmp_path: Path,
) -> None:
    conf = _improvement_wave_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan_json = write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))
    spec_artifact = write_improvement_spec_artifact(json.loads(plan_json))
    candidate_artifact = write_improvement_candidate_eval_artifact(spec_artifact)
    candidate_artifact["payload"]["candidateScore"] = "0.9900"

    with pytest.raises(ValueError, match="artifact payload sha256 does not match artifactSha256"):
        write_benchmark_improvement_decision_artifact(candidate_artifact)


def test_build_benchmark_improvement_wave_plan_requires_replay_metadata() -> None:
    conf = _improvement_wave_conf()
    del conf["judge_model_version"]

    with pytest.raises(ValueError, match="judge_model_version is required"):
        build_benchmark_improvement_wave_plan(conf)


def test_build_benchmark_improvement_wave_plan_rejects_raw_secret_metadata() -> None:
    conf = _improvement_wave_conf()
    conf["judge_model_id"] = "sk-abcdefghijklmnop"

    with pytest.raises(ValueError, match="dag run config must not contain raw secret material"):
        build_benchmark_improvement_wave_plan(conf)


def test_build_public_docs_seed_refresh_plan_materializes_d20_contract(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)

    plan = build_public_docs_seed_refresh_plan(conf)
    repeated = build_public_docs_seed_refresh_plan(json.loads(plan.to_canonical_json()))

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_web_seed_crawl_refresh"
    assert plan.payload["seed_count"] == 4
    assert plan.payload["status"] == "ready_for_public_docs_seed_refresh"
    assert plan.payload["source_type_counts"] == {
        "git": 1,
        "openapi": 1,
        "pdf": 1,
        "website": 1,
    }
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "airflow-plan.json")
        ),
        "public_docs_seed_refresh_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "public-docs-seed-refresh-plan.json")
        ),
        "public_docs_seed_registry": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "public-docs-seed-registry.json")
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_public_docs_seed_registry",
        "write_public_docs_seed_registry",
        "build_public_docs_seed_refresh_plan",
        "dispatch_pipeline_seed_refresh_handoff",
        "notify_governance_eval_surfaces",
    ]

    seed_registry_artifact = write_public_docs_seed_registry_artifact(plan.to_canonical_json())
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    assert Path(seed_registry_artifact["artifactPath"]).exists()
    assert Path(refresh_plan_artifact["artifactPath"]).exists()
    assert (
        seed_registry_artifact["payload"]["seed_registry_sha256"]
        == plan.payload["seed_registry_sha256"]
    )
    assert refresh_plan_artifact["payload"]["status"] == "ready_for_pipeline_dispatch"
    assert [
        request["pipeline_run_spec"]["pipeline_stages"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    ] == [
        ["fetch", "parse", "chunk", "embed", "index"],
        ["fetch", "parse", "chunk", "embed", "index"],
        ["fetch", "parse", "chunk", "embed", "index"],
        ["fetch", "parse", "chunk", "embed", "index"],
    ]
    assert {
        request["source_type"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {"git", "openapi", "pdf", "website"}


def test_build_public_docs_seed_refresh_plan_rejects_unsafe_seed_registry() -> None:
    disallowed_source_type = _public_docs_seed_refresh_conf()
    disallowed_source_type["seed_registry"][0]["source_type"] = "confluence"
    with pytest.raises(ValueError, match="source_type is not executable by current connectors"):
        build_public_docs_seed_refresh_plan(disallowed_source_type)

    missing_robot_policy = _public_docs_seed_refresh_conf()
    missing_robot_policy["seed_registry"][1]["crawl_policy"]["respect_robots_txt"] = False
    with pytest.raises(ValueError, match="respect_robots_txt must be true"):
        build_public_docs_seed_refresh_plan(missing_robot_policy)

    secret_in_metadata = _public_docs_seed_refresh_conf()
    secret_in_metadata["seed_registry"][2]["metadata"] = {"api_key": "sk-abcdefghijklmnop"}
    with pytest.raises(ValueError, match="dag run config must not contain raw secret material"):
        build_public_docs_seed_refresh_plan(secret_in_metadata)


@pytest.mark.parametrize(
    ("dag_file", "dag_id", "task_ids"),
    [
        (
            "serp_nightly_regression_suite.py",
            "serp_nightly_regression_suite",
            [
                "validate_nightly_regression_plan",
                "write_nightly_suite_plan",
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
            "serp_online_eval_rollup.py",
            "serp_online_eval_rollup",
            [
                "validate_online_eval_rollup_plan",
                "write_online_eval_rollup_plan",
                "build_online_eval_rollup",
                "build_online_eval_registry_submissions",
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
        (
            "serp_web_seed_crawl_refresh.py",
            "serp_web_seed_crawl_refresh",
            [
                "validate_public_docs_seed_registry",
                "write_public_docs_seed_registry",
                "build_public_docs_seed_refresh_plan",
                "dispatch_pipeline_seed_refresh_handoff",
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
        "serp_online_eval_rollup.py",
        "serp_tenant_golden_set_regression.py",
        "serp_benchmark_improvement_wave.py",
        "serp_web_seed_crawl_refresh.py",
    ):
        source = (REPO_ROOT / "dags" / dag_file).read_text(encoding="utf-8")

        assert "from dags.serp_eval_contracts import" in source
        assert "from serp_eval_contracts import" not in source


def test_serp_nightly_dag_uses_live_gateway_cli_for_d6_path() -> None:
    source = (REPO_ROOT / "dags" / "serp_nightly_regression_suite.py").read_text(encoding="utf-8")

    assert "write_nightly_suite_plan_artifact" in source
    assert "execute_gateway_cli_spec" in source
    assert "build_nightly_runner_cli_spec" in source
    assert "build_nightly_benchmark_export_cli_spec" in source
    assert "build_nightly_registry_submit_cli_spec" in source
    assert "write_nightly_report_artifact" not in source
    assert "write_nightly_registry_receipts_artifact" not in source


def test_serp_improvement_dag_uses_native_artifact_writers_for_d19_path() -> None:
    source = (REPO_ROOT / "dags" / "serp_benchmark_improvement_wave.py").read_text(encoding="utf-8")

    assert "write_improvement_spec_artifact" in source
    assert "write_improvement_candidate_eval_artifact" in source
    assert "write_benchmark_improvement_decision_artifact" in source
    assert "write_benchmark_improvement_scoreboard_artifact" in source
    assert "build_improvement_candidate_eval_cli_spec" not in source
    assert "build_benchmark_improvement_decision_cli_spec" not in source
    assert "build_benchmark_improvement_scoreboard_cli_spec" not in source


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
    return (isinstance(node.func, ast.Name) and node.func.id == function_name) or (
        isinstance(node.func, ast.Attribute) and node.func.attr == function_name
    )


def _refresh_artifact_sha256(artifact: dict[str, object]) -> None:
    payload_json = json.dumps(
        artifact["payload"], ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )
    artifact["artifactSha256"] = sha256(payload_json.encode("utf-8")).hexdigest()


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


def _online_eval_rollup_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "generated_at": "2026-07-08T10:00:00Z",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "pack",
        "reports": [_online_eval_report()],
        "tenant_id": TENANT_ID,
    }


def _online_eval_report() -> dict[str, object]:
    return {
        "contract_version": "2026.07.2",
        "evidence": {
            "metadata": {
                "evidence_bundle_operation_id": "evidence-bundle-online-sample-001",
                "evidence_bundle_sha256": "a" * 64,
                "online_eval_sample_operation_id": "online-eval-sample-001",
                "retrieval_operation_id": "retrieval-op-001",
                "run_mode": "online-eval-sample",
            },
            "tenant_id": TENANT_ID,
        },
        "metric_results": [
            {
                "metric": "MRR@10",
                "metric_family": "retrieval",
                "normalized_score": 0.97,
                "status": "passed",
            },
            {
                "metric": "Faithfulness",
                "metric_family": "answer-quality",
                "normalized_score": 0.96,
                "status": "passed",
            },
            {
                "metric": "Citation Accuracy",
                "metric_family": "citation",
                "normalized_score": 0.98,
                "status": "passed",
            },
            {
                "metric": "Policy Compliance Rate",
                "metric_family": "policy",
                "normalized_score": 1.0,
                "status": "passed",
            },
        ],
        "operation_id": "retrieval-report-001",
        "status": "passed",
        "suite_id": "online-request-sample",
        "suite_version": "online@2026.07.1",
    }


def _improvement_wave_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-eval-runner",
        "artifact_root_path": "/var/opt/adapstory/serp-evals",
        "baseline_run_id": "evalrun_public_reranker_baseline_001",
        "candidate_id": "candidate-reranker-v2",
        "feature_flags": ["serp.reranker.v2", "serp.d19.dry_run"],
        "generated_at": "2026-07-05T21:00:00Z",
        "guardrail_bundle_version": "guardrails@2026.07.1",
        "improvement_spec_id": "improve-public-retrieval-reranker-v1",
        "judge_model_id": "judge-serp-rubric",
        "judge_model_version": "judge@2026.07.1",
        "judge_prompt_template_version": "judge-template@2026.07.1",
        "max_benchmark_runs": 12,
        "model_catalog_entry_id": "model-catalog://serp/judge-serp-rubric@2026.07.1",
        "policy_bundle_version": "policy@2026.07.1",
        "provider_route_id": "llm-gateway://eval/judge-serp-rubric",
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "workflow",
        "reranker_profile_version": "reranker@2026.07.1",
        "retrieval_profile_version": "hybrid@2026.07.1",
        "rollback_policy_ref": "policy://rollback/last-validated-baseline@v1",
        "selected_suite_ids": list(MANDATORY_SERP_BENCHMARK_SUITES),
        "tenant_id": TENANT_ID,
    }


def _public_docs_seed_refresh_conf() -> dict[str, object]:
    return {
        "actor_id": "airflow-serp-public-docs-refresh",
        "artifact_root_path": "/var/opt/adapstory/serp-public-docs-refresh",
        "generated_at": "2026-07-08T21:00:00Z",
        "pack_id": "serp-public-docs-adapstory-stack",
        "pack_version_id": PACK_VERSION_ID,
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "pack",
        "seed_registry": [
            _public_docs_seed(
                "k3s-docs",
                "website",
                "https://docs.k3s.io/",
                component="K3s",
                version="v1.34.3+k3s1",
            ),
            _public_docs_seed(
                "spring-boot-docs",
                "openapi",
                "https://docs.spring.io/spring-boot/4.0/api/rest/application.yaml",
                component="Spring Boot",
                version="4.0.7",
            ),
            _public_docs_seed(
                "react-docs",
                "pdf",
                "https://react.dev/reference/react.pdf",
                component="React",
                version="19.2.6",
            ),
            _public_docs_seed(
                "adapstory-gitops-docs",
                "git",
                "git+file:///opt/adapstory/Adapstory-GitOps.git",
                component="Adapstory GitOps",
                version="main",
            ),
        ],
        "tenant_id": TENANT_ID,
    }


def _public_docs_seed(
    seed_id: str,
    source_type: str,
    source_uri: str,
    *,
    component: str,
    version: str,
) -> dict[str, object]:
    return {
        "approved": True,
        "connector_name": source_type,
        "crawl_policy": {
            "allowed_domains": ["docs.k3s.io", "docs.spring.io", "react.dev", "opt.adapstory"],
            "deny_patterns": ["/login", "/admin"],
            "max_depth": 2,
            "max_pages": 50,
            "respect_robots_txt": True,
            "sitemap_discovery": True,
            "user_agent": "AdapstorySERPDocsRefresh/2026.07",
        },
        "data_class": "PUBLIC",
        "inventory_evidence": {
            "component": component,
            "evidence_sha256": sha256(f"{component}:{version}".encode()).hexdigest(),
            "stack_inventory_path": "tmp/stack-inventory-2026-07-02.md",
            "version": version,
        },
        "license": {
            "distribution_rule": "cite-and-cache",
            "obligation_state": "reviewed-public-docs",
        },
        "metadata": {
            "origin": "tmp/stack-inventory-2026-07-02.md",
            "purpose": "public-docs-seed-to-serve",
        },
        "official_docs_uri": source_uri,
        "refresh_policy": {
            "cadence": "daily",
            "max_age_hours": 24,
        },
        "seed_id": seed_id,
        "source_id": str(
            __import__("uuid").uuid5(
                __import__("uuid").NAMESPACE_URL,
                f"adapstory-serp-public-docs:{seed_id}",
            )
        ),
        "source_type": source_type,
        "source_uri": source_uri,
    }
