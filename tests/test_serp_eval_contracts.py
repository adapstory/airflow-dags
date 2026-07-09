from __future__ import annotations

import ast
import importlib
import io
import json
import sys
import types
from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse
from uuid import UUID

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
    build_public_docs_publish_activation_cli_spec,
    build_public_docs_publish_activation_plan,
    build_public_docs_publish_activation_submit_cli_spec,
    build_public_docs_seed_refresh_plan,
    build_tenant_golden_registry_cli_spec,
    build_tenant_golden_regression_plan,
    build_tenant_golden_runner_cli_spec,
    default_public_docs_seed_refresh_conf,
    dispatch_public_docs_seed_refresh_handoff,
    evaluate_nightly_regression_gate,
    evaluate_tenant_golden_gate,
    execute_gateway_cli_spec,
    execute_pipeline_cli_spec,
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
    write_public_docs_publish_activation_trigger_conf_artifact,
    write_public_docs_seed_refresh_plan_artifact,
    write_public_docs_seed_registry_artifact,
)
from dags.serp_public_docs_seed_catalog import (
    P0_PUBLIC_DOCS_SOURCES,
    PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH,
    STACK_INVENTORY_SOURCE_PATH,
    p0_public_docs_sources,
)

TENANT_ID = "00000000-0000-4000-a000-000000000001"
PACK_ID = "00000000-0000-4000-a000-000000000201"
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
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/airflow-plan.json"
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
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/nightly-report.json"
        ),
        "benchmark_gate_export": (
            "/var/opt/adapstory/serp-evals/"
            f"{plan.payload['operation_id']}/benchmark-gate-export.json"
        ),
        "suite_plan": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/suite-plan.json"
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


def test_write_airflow_plan_artifact_writes_s3_artifact_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _nightly_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_nightly_regression_plan(conf)
    airflow_plan_path = plan.payload["artifact_paths"]["airflow_plan"]
    bucket, key = airflow_plan_path.removeprefix("s3://").split("/", 1)
    put_calls: list[tuple[str, str, str, str]] = []

    class FakeS3Client:
        def put_object(
            self,
            *,
            Bucket: str,
            Key: str,
            Body: bytes,
            ContentType: str,
        ) -> None:
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    plan_json = write_airflow_plan_artifact(plan)

    assert json.loads(plan_json) == plan.payload
    assert put_calls == [
        (
            bucket,
            key,
            json.dumps(plan.payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            "application/json",
        )
    ]


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


def test_execute_pipeline_cli_spec_runs_without_shell_and_persists_stdout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload("indexed")
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

    result = execute_pipeline_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
            ],
            "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
            "dag_id": "serp_web_seed_crawl_refresh",
            "input_paths": [str(input_path)],
            "operation_id": "op-1",
            "status": "ready_for_pipeline_cli_runner",
            "stdout_path": str(output_path),
            "task_id": "public_docs_seed_refresh_pipeline",
            "tenant_id": TENANT_ID,
        }
    )

    assert calls == [
        (
            ["python", "-m", "adapstory_serp_pipeline.orchestration.seed_refresh_cli"],
            True,
            False,
            True,
        )
    ]
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == payload
    assert result["payload"] == payload
    assert result["artifactPath"] == str(output_path)


def test_execute_pipeline_cli_spec_rejects_evidence_only_public_docs_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload("indexed", index_mode="evidence-only")

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    with pytest.raises(ValueError, match="requires live index_mode"):
        execute_pipeline_cli_spec(
            {
                "argv": [
                    "python",
                    "-m",
                    "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                ],
                "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
                "dag_id": "serp_web_seed_crawl_refresh",
                "input_paths": [str(input_path)],
                "index_mode": "evidence-only",
                "operation_id": "op-1",
                "status": "ready_for_pipeline_cli_runner",
                "stdout_path": str(output_path),
                "task_id": "public_docs_seed_refresh_pipeline",
                "tenant_id": TENANT_ID,
            }
        )

    assert json.loads(output_path.read_text(encoding="utf-8")) == payload


def test_execute_pipeline_cli_spec_fails_d20_task_after_persisting_failed_batch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "public-docs-seed-refresh-plan.json"
    output_path = tmp_path / "public-docs-seed-refresh-result.json"
    input_path.write_text("{}", encoding="utf-8")
    payload = _pipeline_seed_refresh_payload("failed", indexed_count=0, failed_count=1)

    class Result:
        returncode = 0
        stdout = json.dumps(payload)
        stderr = ""

    monkeypatch.setattr(
        "dags.serp_eval_contracts.subprocess.run", lambda *_args, **_kwargs: Result()
    )

    with pytest.raises(ValueError, match="public docs seed refresh did not fully index"):
        execute_pipeline_cli_spec(
            {
                "argv": [
                    "python",
                    "-m",
                    "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                ],
                "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
                "dag_id": "serp_web_seed_crawl_refresh",
                "input_paths": [str(input_path)],
                "operation_id": "op-1",
                "status": "ready_for_pipeline_cli_runner",
                "stdout_path": str(output_path),
                "task_id": "public_docs_seed_refresh_pipeline",
                "tenant_id": TENANT_ID,
            }
        )

    assert json.loads(output_path.read_text(encoding="utf-8")) == payload


def test_execute_pipeline_cli_spec_materializes_s3_inputs_and_uploads_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = {
        (
            "airflow-serp-artifacts",
            "serp-evals/op/public-docs-seed-refresh-plan.json",
        ): b"{}",
    }
    payload = _pipeline_seed_refresh_payload("indexed")
    put_calls: list[tuple[str, str, str, str]] = []
    run_calls: list[list[str]] = []

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
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    def fake_run(
        argv: list[str],
        *,
        capture_output: bool,
        check: bool,
        text: bool,
    ) -> object:
        run_calls.append(argv)
        assert capture_output is True
        assert check is False
        assert text is True
        assert "s3://" not in " ".join(argv)
        refresh_plan = Path(argv[argv.index("--refresh-plan") + 1])
        artifact_root = Path(argv[argv.index("--artifact-root") + 1])
        evidence_output = Path(argv[argv.index("--evidence-output") + 1])
        assert refresh_plan.is_file()
        assert artifact_root.is_dir()
        assert evidence_output.is_absolute()

        class Result:
            returncode = 0
            stdout = json.dumps(payload)
            stderr = ""

        return Result()

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())
    monkeypatch.setattr("dags.serp_eval_contracts.subprocess.run", fake_run)

    result = execute_pipeline_cli_spec(
        {
            "argv": [
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
                "--refresh-plan",
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-plan.json",
                "--artifact-root",
                "s3://airflow-serp-artifacts/serp-evals/op",
                "--evidence-output",
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json",
            ],
            "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
            "dag_id": "serp_web_seed_crawl_refresh",
            "input_paths": [
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-plan.json"
            ],
            "operation_id": "op-1",
            "status": "ready_for_pipeline_cli_runner",
            "stdout_path": (
                "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json"
            ),
            "task_id": "public_docs_seed_refresh_pipeline",
            "tenant_id": TENANT_ID,
        }
    )

    assert run_calls
    assert put_calls == [
        (
            "airflow-serp-artifacts",
            "serp-evals/op/public-docs-seed-refresh-result.json",
            json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            "application/json",
        )
    ]
    assert result["payload"] == payload
    assert result["artifactPath"] == (
        "s3://airflow-serp-artifacts/serp-evals/op/public-docs-seed-refresh-result.json"
    )


def test_dispatch_public_docs_seed_refresh_handoff_preserves_s3_artifact_root_uri() -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_public_docs_seed_refresh_plan(conf)

    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    artifact_root = cli_spec["argv"][cli_spec["argv"].index("--artifact-root") + 1]
    assert artifact_root.startswith("s3://airflow-serp-artifacts/serp-evals/")
    assert not artifact_root.startswith("s3:/airflow-serp-artifacts")


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
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/airflow-plan.json"
        ),
        "golden_set": (
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/golden-set.json"
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
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/airflow-plan.json"
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
            f"/var/opt/adapstory/serp-evals/{plan.payload['operation_id']}/improvement-spec.json"
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
    assert plan.payload["index_mode"] == "live"
    assert plan.payload["embedding_mode"] == "live-gateway"
    assert plan.payload["seed_count"] == 4
    assert plan.payload["status"] == "ready_for_public_docs_seed_refresh"
    assert plan.payload["source_type_counts"] == {
        "git": 1,
        "openapi": 1,
        "website": 2,
    }
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "airflow-plan.json")
        ),
        "public_docs_seed_refresh_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "public-docs-seed-refresh-plan.json")
        ),
        "public_docs_seed_refresh_result": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "public-docs-seed-refresh-result.json")
        ),
        "public_docs_publish_activation_trigger_conf": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-publish-activation-trigger-conf.json",
            )
        ),
        "public_docs_bc21_pipeline_state_receipt": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-bc21-pipeline-state-receipt.json",
            )
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
        "run_public_docs_seed_refresh_pipeline",
        "submit_public_docs_bc21_pipeline_state",
        "write_public_docs_publish_activation_trigger_conf",
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
    assert refresh_plan_artifact["payload"]["index_mode"] == "live"
    assert refresh_plan_artifact["payload"]["embedding_mode"] == "live-gateway"
    assert refresh_plan_artifact["payload"]["skipped_seed_count"] == 0
    assert refresh_plan_artifact["payload"]["seed_count"] == 9
    assert all(
        request["pipeline_run_spec"]["pipeline_stages"]
        == ["fetch", "parse", "chunk", "embed", "index"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    )
    assert [
        request["seed_id"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ] == [
        "k3s-docs",
        "k3s-docs--d15cca4a1ed9",
        "k3s-docs--906d7d24fe52",
    ]
    assert [
        request["source_uri"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
    ]
    assert {
        request["source_metadata"]["frontier"]["discovery_mode"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    } == {"governed-seed-frontier"}
    assert {
        request["source_type"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {"git", "openapi", "website"}
    assert all(
        request["source_uri_hash"].startswith("sha256:")
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    )
    assert all(
        "parse_run_id" in request["pipeline_run_spec"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    )
    assert {
        request["pipeline_run_spec"]["pack_id"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {PACK_ID}
    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())
    assert cli_spec["status"] == "ready_for_pipeline_cli_runner"
    assert cli_spec["task_id"] == "public_docs_seed_refresh_pipeline"
    assert (
        cli_spec["stdout_path"] == plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    assert "pending_pipeline_dispatch" not in json.dumps(cli_spec, sort_keys=True)
    assert cli_spec["seed_count"] == 9
    assert cli_spec["skipped_seed_count"] == 0
    assert "--index-mode" in cli_spec["argv"]
    assert cli_spec["argv"][cli_spec["argv"].index("--index-mode") + 1] == "live"
    assert "--embedding-mode" in cli_spec["argv"]
    assert cli_spec["argv"][cli_spec["argv"].index("--embedding-mode") + 1] == "live-gateway"
    assert cli_spec["argv"][cli_spec["argv"].index("--qdrant-collection") + 1] == "serp_vectors_dev"
    assert cli_spec["argv"][cli_spec["argv"].index("--opensearch-index") + 1] == "serp_lexical_dev"
    assert cli_spec["argv"][cli_spec["argv"].index("--neo4j-database") + 1] == "serp_graph_dev"
    assert cli_spec["argv"][:3] == [
        "python",
        "-m",
        "adapstory_serp_pipeline.orchestration.seed_refresh_cli",
    ]


def test_d20_writes_public_docs_publish_activation_trigger_conf_artifact(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["index_mode"] = "evidence-only"
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    _write_public_docs_seed_refresh_result(seed_refresh_result_path)

    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    assert Path(trigger_artifact["artifactPath"]).exists()
    assert trigger_artifact["artifactType"] == "public_docs_publish_activation_trigger_conf"
    assert (
        trigger_artifact["artifactPath"]
        == plan.payload["artifact_paths"]["public_docs_publish_activation_trigger_conf"]
    )
    payload = trigger_artifact["payload"]
    assert payload["status"] == "governance_inputs_required"
    assert payload["target_dag_id"] == "serp_publish_signed_pack"
    assert payload["d5_publish_target"] == "serp_publish_signed_pack"
    assert payload["source_seed_refresh_result_path"] == str(seed_refresh_result_path)
    assert payload["governance_required_fields"] == [
        "activation_idempotency_key",
        "approval_run_id",
        "benchmark_gate_export_sha256",
        "bc21_base_url",
        "evidence_bundle_id",
        "evidence_seal_hash",
    ]
    assert payload["target_dag_run_conf"] == {
        "activation_reason_code": "public-docs-d20-indexed",
        "actor_id": "airflow-serp-public-docs-refresh",
        "artifact_root_path": str(tmp_path),
        "generated_at": "2026-07-08T21:00:00Z",
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "public_docs_seed_refresh_result_path": str(seed_refresh_result_path),
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "pack",
        "tenant_id": TENANT_ID,
    }
    repeated_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        json.loads(plan.to_canonical_json())
    )
    assert trigger_artifact["artifactSha256"] == repeated_artifact["artifactSha256"]


def test_d20_trigger_conf_includes_bc21_base_url_from_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "ADAPSTORY_SERP_BC21_BASE_URL",
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080",
    )
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    _write_public_docs_seed_refresh_result(seed_refresh_result_path)

    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    assert plan.payload["bc21_base_url"] == (
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080"
    )
    assert trigger_artifact["payload"]["governance_required_fields"] == [
        "activation_idempotency_key",
        "approval_run_id",
        "benchmark_gate_export_sha256",
        "evidence_bundle_id",
        "evidence_seal_hash",
    ]
    assert trigger_artifact["payload"]["target_dag_run_conf"]["bc21_base_url"] == (
        "http://prod-serp-context-platform-svc.env-prod.svc.cluster.local:8080"
    )


def test_d20_writes_public_docs_publish_activation_trigger_conf_from_s3_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    trigger_conf_path = plan.payload["artifact_paths"][
        "public_docs_publish_activation_trigger_conf"
    ]
    result_bucket, result_key = seed_refresh_result_path.removeprefix("s3://").split("/", 1)
    trigger_bucket, trigger_key = trigger_conf_path.removeprefix("s3://").split("/", 1)
    storage = {
        (result_bucket, result_key): json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": {
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ).encode("utf-8")
    }
    put_calls: list[tuple[str, str, str, str]] = []

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
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    assert trigger_artifact["artifactPath"] == trigger_conf_path
    assert trigger_artifact["payload"]["source_seed_refresh_result_path"] == (
        seed_refresh_result_path
    )
    assert put_calls
    assert put_calls[0][0] == trigger_bucket
    assert put_calls[0][1] == trigger_key
    assert put_calls[0][3] == "application/json"


def test_d20_default_conf_rejects_unsafe_env_bc21_base_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADAPSTORY_SERP_BC21_BASE_URL", "http://example.invalid")

    with pytest.raises(ValueError, match="bc21_base_url must use https"):
        default_public_docs_seed_refresh_conf(generated_at="2026-07-08T21:00:00Z")


def test_d20_trigger_conf_rejects_invalid_seed_refresh_result(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_seed_refresh_plan(conf)

    with pytest.raises(ValueError, match="public_docs_seed_refresh_result file is not readable"):
        write_public_docs_publish_activation_trigger_conf_artifact(plan.to_canonical_json())

    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    seed_refresh_result_path.write_text(
        json.dumps(
            {
                "artifact_type": "unsupported",
                "batch_evidence": {
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="artifact_type is unsupported"):
        write_public_docs_publish_activation_trigger_conf_artifact(plan.to_canonical_json())

    _write_public_docs_seed_refresh_result(
        seed_refresh_result_path,
        tenant_id="00000000-0000-4000-a000-000000000099",
    )
    with pytest.raises(ValueError, match="identity must match tenant_id"):
        write_public_docs_publish_activation_trigger_conf_artifact(plan.to_canonical_json())


def test_d5_still_requires_governance_inputs_from_d20_trigger_conf(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    plan = build_public_docs_seed_refresh_plan(conf)
    seed_refresh_result_path = Path(
        plan.payload["artifact_paths"]["public_docs_seed_refresh_result"]
    )
    seed_refresh_result_path.parent.mkdir(parents=True, exist_ok=True)
    _write_public_docs_seed_refresh_result(seed_refresh_result_path)
    trigger_artifact = write_public_docs_publish_activation_trigger_conf_artifact(
        plan.to_canonical_json()
    )

    with pytest.raises(ValueError, match="approval_run_id is required"):
        build_public_docs_publish_activation_plan(
            trigger_artifact["payload"]["target_dag_run_conf"]
        )


def test_public_docs_seed_refresh_uses_single_website_request_when_frontier_disabled(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["crawl_policy"]["sitemap_discovery"] = False
    conf["seed_registry"][0]["crawl_policy"].pop("frontier_urls", None)

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert len(website_requests) == 1
    assert website_requests[0]["seed_id"] == "k3s-docs"
    assert website_requests[0]["source_uri"] == "https://docs.k3s.io/"


def test_public_docs_seed_refresh_allows_manual_frontier_without_sitemap_discovery(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["crawl_policy"]["sitemap_discovery"] = False

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert [request["source_uri"] for request in website_requests] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start",
        "https://docs.k3s.io/installation/requirements",
    ]


def test_public_docs_seed_refresh_deduplicates_canonical_frontier_urls(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["crawl_policy"]["frontier_urls"] = [
        "https://docs.k3s.io",
        "https://docs.k3s.io/#overview",
        "https://docs.k3s.io/quick-start#install",
        "https://docs.k3s.io/quick-start",
    ]

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    website_requests = [
        request
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
        if request["seed_id"].startswith("k3s-docs")
    ]
    assert [request["source_uri"] for request in website_requests] == [
        "https://docs.k3s.io/",
        "https://docs.k3s.io/quick-start#install",
    ]


def test_public_docs_seed_refresh_selects_due_seeds_and_records_skips(
    tmp_path: Path,
) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["seed_registry"][0]["freshness_state"] = {
        "last_success_at": "2026-07-08T12:00:00Z",
        "status": "indexed",
    }
    conf["seed_registry"][1]["freshness_state"] = {
        "last_success_at": "2026-07-07T20:00:00Z",
        "status": "indexed",
    }

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())
    payload = refresh_plan_artifact["payload"]

    assert payload["status"] == "ready_for_pipeline_dispatch"
    assert payload["seed_count"] == 6
    assert payload["skipped_seed_count"] == 1
    assert [skip["seed_id"] for skip in payload["skipped_seed_refreshes"]] == ["k3s-docs"]
    assert {
        request["seed_id"]
        for request in payload["source_fetch_requests"]
        if "--" not in request["seed_id"]
    } == {
        "adapstory-gitops-docs",
        "kubernetes-openapi-docs",
        "postgresql-reference-docs",
    }
    assert {
        request["source_metadata"]["refresh_selection"]["reason"]
        for request in payload["source_fetch_requests"]
    } == {"max_age_exceeded", "never_indexed"}

    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    assert cli_spec["status"] == "ready_for_pipeline_cli_runner"
    assert cli_spec["seed_count"] == 6
    assert cli_spec["skipped_seed_count"] == 1


def test_public_docs_seed_refresh_dispatches_live_index_mode(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    conf["index_mode"] = "live"
    conf["qdrant_collection"] = "serp_vectors_prod"
    conf["opensearch_index"] = "serp_lexical_prod"
    conf["neo4j_database"] = "neo4j"

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())
    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    assert plan.payload["index_mode"] == "live"
    assert plan.payload["embedding_mode"] == "live-gateway"
    assert plan.payload["qdrant_collection"] == "serp_vectors_prod"
    assert plan.payload["opensearch_index"] == "serp_lexical_prod"
    assert plan.payload["neo4j_database"] == "neo4j"
    assert refresh_plan_artifact["payload"]["index_mode"] == "live"
    assert refresh_plan_artifact["payload"]["embedding_mode"] == "live-gateway"
    assert cli_spec["argv"][cli_spec["argv"].index("--index-mode") + 1] == "live"
    assert cli_spec["argv"][cli_spec["argv"].index("--embedding-mode") + 1] == "live-gateway"
    assert (
        cli_spec["argv"][cli_spec["argv"].index("--qdrant-collection") + 1] == "serp_vectors_prod"
    )
    assert cli_spec["argv"][cli_spec["argv"].index("--opensearch-index") + 1] == "serp_lexical_prod"
    assert cli_spec["argv"][cli_spec["argv"].index("--neo4j-database") + 1] == "neo4j"


def test_public_docs_seed_refresh_noops_when_no_seed_is_due(tmp_path: Path) -> None:
    conf = _public_docs_seed_refresh_conf()
    conf["artifact_root_path"] = str(tmp_path)
    for seed in conf["seed_registry"]:
        seed["freshness_state"] = {
            "last_success_at": "2026-07-08T20:30:00Z",
            "status": "indexed",
        }

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())
    payload = refresh_plan_artifact["payload"]

    assert payload["status"] == "no_due_sources"
    assert payload["seed_count"] == 0
    assert payload["source_fetch_requests"] == []
    assert payload["skipped_seed_count"] == 4

    cli_spec = dispatch_public_docs_seed_refresh_handoff(plan.to_canonical_json())

    assert cli_spec["status"] == "no_due_sources"
    assert cli_spec["argv"] == []
    assert cli_spec["seed_count"] == 0
    assert cli_spec["skipped_seed_count"] == 4

    result = execute_pipeline_cli_spec(cli_spec)

    assert Path(result["artifactPath"]).exists()
    assert result["payload"]["status"] == "no_due_sources"
    assert result["payload"]["skipped_seed_count"] == 4


def test_public_docs_publish_activation_plan_dispatches_d5_handoff(tmp_path: Path) -> None:
    seed_refresh_result = tmp_path / "public-docs-seed-refresh-result.json"
    _write_public_docs_seed_refresh_result(seed_refresh_result)
    conf = _public_docs_publish_activation_conf(str(seed_refresh_result))
    conf["artifact_root_path"] = str(tmp_path)

    plan = build_public_docs_publish_activation_plan(conf)
    repeated = build_public_docs_publish_activation_plan(json.loads(plan.to_canonical_json()))
    cli_spec = build_public_docs_publish_activation_cli_spec(plan.to_canonical_json())

    assert plan.to_canonical_json() == repeated.to_canonical_json()
    assert plan.payload["dag_id"] == "serp_publish_signed_pack"
    assert plan.payload["status"] == "ready_for_publish_activation_handoff"
    assert plan.payload["artifact_paths"] == {
        "airflow_plan": "/".join(
            (str(tmp_path), plan.payload["operation_id"], "airflow-plan.json")
        ),
        "public_docs_publish_activation_request": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-publish-activation-request.json",
            )
        ),
        "public_docs_publish_activation_receipt": "/".join(
            (
                str(tmp_path),
                plan.payload["operation_id"],
                "public-docs-publish-activation-receipt.json",
            )
        ),
    }
    assert [task["task_id"] for task in plan.payload["tasks"]] == [
        "validate_publish_signed_pack_plan",
        "dispatch_publish_activation_handoff",
        "run_publish_activation_handoff",
        "dispatch_publish_activation_submit",
        "submit_publish_activation_to_bc21",
        "notify_governance_eval_surfaces",
    ]
    assert cli_spec["status"] == "ready_for_pipeline_cli_runner"
    assert cli_spec["task_id"] == "public_docs_publish_activation_handoff"
    assert cli_spec["d5_publish_target"] == "serp_publish_signed_pack"
    assert cli_spec["input_paths"] == [str(seed_refresh_result)]
    assert (
        cli_spec["stdout_path"]
        == plan.payload["artifact_paths"]["public_docs_publish_activation_request"]
    )
    assert cli_spec["argv"][:3] == [
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.publish_activation_cli",
    ]
    assert cli_spec["argv"][cli_spec["argv"].index("--seed-refresh-result") + 1] == str(
        seed_refresh_result
    )
    assert cli_spec["argv"][cli_spec["argv"].index("--benchmark-gate-export-sha256") + 1] == (
        "sha256:" + "c" * 64
    )
    request_artifact_path = Path(
        plan.payload["artifact_paths"]["public_docs_publish_activation_request"]
    )
    request_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    request_artifact_path.write_text(
        '{"artifact_type":"public_docs_publish_activation_submission"}',
        encoding="utf-8",
    )
    submit_spec = build_public_docs_publish_activation_submit_cli_spec(plan.to_canonical_json())
    assert submit_spec["status"] == "ready_for_pipeline_cli_runner"
    assert submit_spec["task_id"] == "public_docs_publish_activation_submit"
    assert submit_spec["input_paths"] == [
        plan.payload["artifact_paths"]["public_docs_publish_activation_request"]
    ]
    assert (
        submit_spec["stdout_path"]
        == plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"]
    )
    assert submit_spec["argv"][:4] == [
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.publish_activation_cli",
        "submit",
    ]
    assert submit_spec["argv"][submit_spec["argv"].index("--bc21-base-url") + 1] == (
        "http://serp-context-platform.env-dev.svc.cluster.local"
    )


def test_public_docs_publish_activation_plan_accepts_s3_d20_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_refresh_result_path = (
        "s3://airflow-serp-artifacts/serp-evals/d20/public-docs-seed-refresh-result.json"
    )
    request_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-request.json"
    )
    receipt_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-receipt.json"
    )
    result_bucket, result_key = seed_refresh_result_path.removeprefix("s3://").split("/", 1)
    request_bucket, request_key = request_path.removeprefix("s3://").split("/", 1)
    storage = {
        (result_bucket, result_key): json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": {
                    "pack_id": PACK_ID,
                    "pack_version_id": PACK_VERSION_ID,
                    "tenant_id": TENANT_ID,
                },
            },
            sort_keys=True,
        ).encode("utf-8"),
        (request_bucket, request_key): json.dumps(
            {
                "artifact_type": "public_docs_publish_activation_submission",
                "contract_version": "2026.07.1",
                "status": "ready_for_bc21_publish_activation",
                "submission": {"endpointPath": "/api/bc-21/serp/v1/packs/x"},
                "submission_sha256": "a" * 64,
            },
            sort_keys=True,
        ).encode("utf-8"),
    }

    class FakeS3Client:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
            return {"Body": io.BytesIO(storage[(Bucket, Key)])}

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    conf = _public_docs_publish_activation_conf(seed_refresh_result_path)
    conf["artifact_root_path"] = "s3://airflow-serp-artifacts/serp-evals"
    plan = build_public_docs_publish_activation_plan(conf)
    plan.payload["artifact_paths"]["public_docs_publish_activation_request"] = request_path
    plan.payload["artifact_paths"]["public_docs_publish_activation_receipt"] = receipt_path

    cli_spec = build_public_docs_publish_activation_cli_spec(plan.to_canonical_json())
    submit_spec = build_public_docs_publish_activation_submit_cli_spec(plan.to_canonical_json())

    assert plan.payload["public_docs_seed_refresh_result_path"] == seed_refresh_result_path
    assert cli_spec["input_paths"] == [seed_refresh_result_path]
    assert cli_spec["argv"][cli_spec["argv"].index("--seed-refresh-result") + 1] == (
        seed_refresh_result_path
    )
    assert submit_spec["input_paths"] == [request_path]
    assert submit_spec["argv"][submit_spec["argv"].index("--publish-activation-request") + 1] == (
        request_path
    )
    assert submit_spec["argv"][submit_spec["argv"].index("--activation-receipt-output") + 1] == (
        receipt_path
    )


def test_pipeline_cli_executor_materializes_s3_activation_receipt_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-request.json"
    )
    receipt_path = (
        "s3://airflow-serp-artifacts/serp-evals/d5/public-docs-publish-activation-receipt.json"
    )
    request_bucket, request_key = request_path.removeprefix("s3://").split("/", 1)
    bucket, key = receipt_path.removeprefix("s3://").split("/", 1)
    storage = {
        (
            request_bucket,
            request_key,
        ): b'{"artifact_type":"public_docs_publish_activation_submission"}'
    }
    put_calls: list[tuple[str, str, str, str]] = []

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
            put_calls.append((Bucket, Key, Body.decode("utf-8"), ContentType))

    monkeypatch.setattr("dags.serp_eval_contracts._s3_client", lambda: FakeS3Client())

    cli_spec = {
        "argv": [
            "python",
            "-c",
            (
                "import json, pathlib, sys; "
                "request_arg = sys.argv.index('--publish-activation-request') + 1; "
                "request = pathlib.Path(sys.argv[request_arg]); "
                "assert request.exists(); "
                "receipt_arg = sys.argv.index('--activation-receipt-output') + 1; "
                "p = pathlib.Path(sys.argv[receipt_arg]); "
                "assert not str(p).startswith('s3://'); "
                "p.parent.mkdir(parents=True, exist_ok=True); "
                "payload = {'artifact_type': 'public_docs_publish_activation_receipt', "
                "'status': 'active'}; "
                "p.write_text(json.dumps(payload), encoding='utf-8'); "
                "print(json.dumps(payload))"
            ),
            "--publish-activation-request",
            request_path,
            "--activation-receipt-output",
            receipt_path,
        ],
        "contract_version": "serp-airflow-pipeline-cli-bridge/v1",
        "dag_id": "serp_publish_signed_pack",
        "input_paths": [request_path],
        "operation_id": "serp-airflow-publish-signed-pack-test",
        "status": "ready_for_pipeline_cli_runner",
        "stdout_path": receipt_path,
        "task_id": "public_docs_publish_activation_submit",
    }

    result = execute_pipeline_cli_spec(cli_spec)

    assert result["artifactPath"] == receipt_path
    assert result["payload"]["status"] == "active"
    assert put_calls
    assert put_calls[0][0] == bucket
    assert put_calls[0][1] == key
    assert put_calls[0][3] == "application/json"


def test_public_docs_publish_activation_plan_requires_governed_inputs(tmp_path: Path) -> None:
    missing_result = _public_docs_publish_activation_conf(str(tmp_path / "missing.json"))
    missing_result["artifact_root_path"] = str(tmp_path)
    with pytest.raises(ValueError, match="public_docs_seed_refresh_result file is not readable"):
        build_public_docs_publish_activation_plan(missing_result)

    bad_seal = _public_docs_publish_activation_conf(str(tmp_path / "result.json"))
    bad_seal_path = str(bad_seal["public_docs_seed_refresh_result_path"])
    Path(bad_seal_path).write_text("{}", encoding="utf-8")
    bad_seal["evidence_seal_hash"] = "b" * 64
    with pytest.raises(ValueError, match="evidence_seal_hash"):
        build_public_docs_publish_activation_plan(bad_seal)

    identity_drift_path = tmp_path / "identity-drift-result.json"
    _write_public_docs_seed_refresh_result(
        identity_drift_path,
        pack_id="00000000-0000-4000-a000-000000000299",
    )
    identity_drift = _public_docs_publish_activation_conf(str(identity_drift_path))
    identity_drift["artifact_root_path"] = str(tmp_path)
    with pytest.raises(ValueError, match="identity must match pack_id"):
        build_public_docs_publish_activation_plan(identity_drift)


def test_default_public_docs_seed_refresh_conf_materializes_autonomous_d20_plan(
    tmp_path: Path,
) -> None:
    conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )

    plan = build_public_docs_seed_refresh_plan(conf)
    refresh_plan_artifact = write_public_docs_seed_refresh_plan_artifact(plan.to_canonical_json())

    assert conf["seed_registry"]
    assert plan.payload["dag_id"] == "serp_web_seed_crawl_refresh"
    assert plan.payload["status"] == "ready_for_public_docs_seed_refresh"
    assert plan.payload["seed_count"] == len(P0_PUBLIC_DOCS_SOURCES)
    assert plan.payload["source_type_counts"] == {
        "openapi": 1,
        "website": len(P0_PUBLIC_DOCS_SOURCES) - 1,
    }
    assert {
        seed["inventory_evidence"]["stack_inventory_path"] for seed in plan.payload["seed_registry"]
    } == {STACK_INVENTORY_SOURCE_PATH}
    assert {seed["metadata"]["origin"] for seed in plan.payload["seed_registry"]} == {
        STACK_INVENTORY_SOURCE_PATH
    }
    assert {
        seed["metadata"]["nightly_source_catalog_path"] for seed in plan.payload["seed_registry"]
    } == {PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH}
    assert {seed["metadata"]["priority"] for seed in plan.payload["seed_registry"]} == {"P0"}
    assert {seed["seed_id"]: seed["source_uri"] for seed in plan.payload["seed_registry"]} == {
        str(source["seed_id"]): str(source["docs_url"]) for source in P0_PUBLIC_DOCS_SOURCES
    }
    assert {
        seed["seed_id"]: seed["metadata"]["catalog_docs_url"]
        for seed in plan.payload["seed_registry"]
    } == {
        str(source["seed_id"]): str(source.get("catalog_docs_url", source["docs_url"]))
        for source in P0_PUBLIC_DOCS_SOURCES
    }
    assert {
        seed["seed_id"]: seed["metadata"]["repo_url"] for seed in plan.payload["seed_registry"]
    } == {str(source["seed_id"]): str(source["repo_url"]) for source in P0_PUBLIC_DOCS_SOURCES}
    assert {
        seed["seed_id"]: seed["metadata"]["releases_url"] for seed in plan.payload["seed_registry"]
    } == {str(source["seed_id"]): str(source["releases_url"]) for source in P0_PUBLIC_DOCS_SOURCES}
    assert {
        seed["seed_id"]: tuple(seed["metadata"]["suggested_ingest_modes"])
        for seed in plan.payload["seed_registry"]
    } == {
        str(source["seed_id"]): tuple(source["suggested_ingest_modes"])
        for source in P0_PUBLIC_DOCS_SOURCES
    }
    sources_by_seed_id = {str(source["seed_id"]): source for source in P0_PUBLIC_DOCS_SOURCES}
    assert {
        request["seed_id"]: request["source_metadata"]["repo_url"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {
        request["seed_id"]: str(
            sources_by_seed_id[request["seed_id"].split("--", maxsplit=1)[0]]["repo_url"]
        )
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    }
    assert {
        request["seed_id"]: request["source_metadata"]["releases_url"]
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {
        request["seed_id"]: str(
            sources_by_seed_id[request["seed_id"].split("--", maxsplit=1)[0]]["releases_url"]
        )
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    }
    assert {
        request["seed_id"]: tuple(request["source_metadata"]["suggested_ingest_modes"])
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    } == {
        request["seed_id"]: tuple(
            sources_by_seed_id[request["seed_id"].split("--", maxsplit=1)[0]][
                "suggested_ingest_modes"
            ]
        )
        for request in refresh_plan_artifact["payload"]["source_fetch_requests"]
    }


def test_default_public_docs_seed_refresh_conf_uses_run_scoped_pack_version(
    tmp_path: Path,
) -> None:
    first = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    first_retry = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    second = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-09T00:00:00Z",
        artifact_root_path=str(tmp_path),
    )

    assert first["pack_version_id"] == first_retry["pack_version_id"]
    assert first["pack_version_id"] == first["registry_resource_id"]
    assert first["pack_version_id"] != PACK_VERSION_ID
    assert second["pack_version_id"] != first["pack_version_id"]
    UUID(str(first["pack_version_id"]))
    UUID(str(second["pack_version_id"]))


def test_default_public_docs_seed_refresh_conf_changes_identity_on_catalog_metadata_drift(
    tmp_path: Path,
) -> None:
    baseline = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    drifted = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    drifted["seed_registry"][0]["metadata"]["repo_url"] = "https://git.proxmox.com/drift"
    drifted["seed_registry"][0]["inventory_evidence"]["evidence_sha256"] = "b" * 64

    baseline_plan = build_public_docs_seed_refresh_plan(baseline)
    drifted_plan = build_public_docs_seed_refresh_plan(drifted)

    assert (
        baseline_plan.payload["seed_registry_sha256"]
        != drifted_plan.payload["seed_registry_sha256"]
    )
    assert baseline_plan.payload["operation_id"] != drifted_plan.payload["operation_id"]


def test_default_public_docs_seed_refresh_conf_does_not_read_tmp_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    assert not (tmp_path / PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH).exists()
    assert not (tmp_path / STACK_INVENTORY_SOURCE_PATH).exists()

    conf = default_public_docs_seed_refresh_conf(
        generated_at="2026-07-08T21:00:00Z",
        artifact_root_path=str(tmp_path),
    )
    plan = build_public_docs_seed_refresh_plan(conf)

    assert plan.payload["seed_count"] == len(P0_PUBLIC_DOCS_SOURCES)
    assert {
        seed["metadata"]["nightly_source_catalog_path"] for seed in plan.payload["seed_registry"]
    } == {PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH}
    assert {
        seed["inventory_evidence"]["stack_inventory_path"] for seed in plan.payload["seed_registry"]
    } == {STACK_INVENTORY_SOURCE_PATH}


def test_p0_public_docs_seed_catalog_shape_is_runtime_safe() -> None:
    allowed_source_types = {"git", "openapi", "pdf", "website"}
    seen_seed_ids: set[str] = set()

    for source in p0_public_docs_sources():
        seed_id = str(source["seed_id"])
        docs_url = str(source["docs_url"])
        source_type = str(source.get("source_type", "website"))
        docs_origin = urlparse(docs_url)

        assert seed_id not in seen_seed_ids
        assert seed_id
        assert str(source["component"])
        assert str(source.get("priority", "P0")) == "P0"
        assert source_type in allowed_source_types
        assert docs_origin.scheme in {"git+file", "https"}

        if source_type == "git":
            assert docs_url.startswith("git+file://")
        if source_type == "pdf":
            assert docs_url.endswith(".pdf")
        if source_type in {"openapi", "website"}:
            assert docs_origin.scheme == "https"

        for frontier_url in source.get("frontier_urls", ()):
            frontier_origin = urlparse(str(frontier_url))
            assert frontier_origin.scheme == docs_origin.scheme
            assert frontier_origin.netloc == docs_origin.netloc

        seen_seed_ids.add(seed_id)

    assert seen_seed_ids == {str(source["seed_id"]) for source in P0_PUBLIC_DOCS_SOURCES}


def test_p0_public_docs_sources_match_nightly_markdown_catalog() -> None:
    catalog_path = REPO_ROOT.parent / PUBLIC_DOCS_NIGHTLY_SOURCE_CATALOG_PATH
    stack_inventory_path = REPO_ROOT.parent / STACK_INVENTORY_SOURCE_PATH

    assert catalog_path.exists()
    assert stack_inventory_path.exists()

    catalog_text = catalog_path.read_text(encoding="utf-8")
    assert STACK_INVENTORY_SOURCE_PATH in catalog_text

    catalog_rows = _p0_nightly_catalog_rows(catalog_text)
    executable_sources = {str(source["component"]): source for source in P0_PUBLIC_DOCS_SOURCES}

    assert set(executable_sources) == set(catalog_rows)
    for component, source in executable_sources.items():
        row = catalog_rows[component]
        docs_url = str(source["docs_url"])
        catalog_docs_url = str(source.get("catalog_docs_url", docs_url))
        source_type = str(source.get("source_type", "website"))
        suggested_modes = {
            mode.strip() for mode in row["suggested_ingest_modes"].split(",") if mode.strip()
        }

        assert row["docs_url"] == catalog_docs_url
        assert row["repo_url"] == str(source["repo_url"])
        assert row["releases_url"] == str(source["releases_url"])
        assert suggested_modes == set(source["suggested_ingest_modes"])
        assert source_type in suggested_modes
        assert str(source.get("priority", "P0")) == row["priority"]


def _p0_nightly_catalog_rows(catalog_text: str) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for line in catalog_text.splitlines():
        if not line.startswith("| P0 |"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 7:
            raise AssertionError(f"unexpected P0 catalog row shape: {line}")
        priority, technology, docs_url, repo_url, releases_url, suggested_ingest_modes, notes = (
            cells
        )
        rows[technology] = {
            "docs_url": docs_url,
            "notes": notes,
            "priority": priority,
            "releases_url": releases_url,
            "repo_url": repo_url,
            "suggested_ingest_modes": suggested_ingest_modes,
        }
    return rows


def test_build_public_docs_seed_refresh_plan_rejects_unsafe_seed_registry() -> None:
    disallowed_source_type = _public_docs_seed_refresh_conf()
    disallowed_source_type["seed_registry"][0]["source_type"] = "confluence"
    with pytest.raises(ValueError, match="source_type is not executable by current connectors"):
        build_public_docs_seed_refresh_plan(disallowed_source_type)

    planned_markdown = _public_docs_seed_refresh_conf()
    planned_markdown["seed_registry"][0]["source_type"] = "markdown"
    with pytest.raises(ValueError, match="source_type is not executable by current connectors"):
        build_public_docs_seed_refresh_plan(planned_markdown)

    unsupported_index_mode = _public_docs_seed_refresh_conf()
    unsupported_index_mode["index_mode"] = "shadow-live"
    with pytest.raises(ValueError, match="index_mode is unsupported"):
        build_public_docs_seed_refresh_plan(unsupported_index_mode)

    unsupported_embedding_mode = _public_docs_seed_refresh_conf()
    unsupported_embedding_mode["embedding_mode"] = "direct-provider"
    with pytest.raises(ValueError, match="embedding_mode is unsupported"):
        build_public_docs_seed_refresh_plan(unsupported_embedding_mode)

    live_with_dev_embedding = _public_docs_seed_refresh_conf()
    live_with_dev_embedding["index_mode"] = "live"
    live_with_dev_embedding["embedding_mode"] = "deterministic-dev"
    with pytest.raises(ValueError, match="live index mode requires live-gateway"):
        build_public_docs_seed_refresh_plan(live_with_dev_embedding)

    unsafe_store_name = _public_docs_seed_refresh_conf()
    unsafe_store_name["qdrant_collection"] = "serp vectors prod"
    with pytest.raises(ValueError, match="qdrant_collection must be a plain store name"):
        build_public_docs_seed_refresh_plan(unsafe_store_name)

    remote_git = _public_docs_seed_refresh_conf()
    remote_git["seed_registry"][3]["source_uri"] = "git+https://github.com/adapstory/docs.git"
    remote_git["seed_registry"][3]["official_docs_uri"] = (
        "git+https://github.com/adapstory/docs.git"
    )
    with pytest.raises(ValueError, match="git public docs seeds must use git\\+file"):
        build_public_docs_seed_refresh_plan(remote_git)

    missing_robot_policy = _public_docs_seed_refresh_conf()
    missing_robot_policy["seed_registry"][1]["crawl_policy"]["respect_robots_txt"] = False
    with pytest.raises(ValueError, match="respect_robots_txt must be true"):
        build_public_docs_seed_refresh_plan(missing_robot_policy)

    cross_domain_frontier = _public_docs_seed_refresh_conf()
    cross_domain_frontier["seed_registry"][0]["crawl_policy"]["frontier_urls"] = [
        "https://evil.example.com/k3s"
    ]
    with pytest.raises(ValueError, match="frontier_urls host must be in allowed_domains"):
        build_public_docs_seed_refresh_plan(cross_domain_frontier)

    denied_seed_uri = _public_docs_seed_refresh_conf()
    denied_seed_uri["seed_registry"][0]["source_uri"] = "https://docs.k3s.io/login"
    denied_seed_uri["seed_registry"][0]["official_docs_uri"] = "https://docs.k3s.io/login"
    with pytest.raises(ValueError, match="source_uri must not match deny_patterns"):
        build_public_docs_seed_refresh_plan(denied_seed_uri)

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
            "serp_publish_signed_pack.py",
            "serp_publish_signed_pack",
            [
                "validate_publish_signed_pack_plan",
                "dispatch_publish_activation_handoff",
                "run_publish_activation_handoff",
                "dispatch_publish_activation_submit",
                "submit_publish_activation_to_bc21",
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
                "run_public_docs_seed_refresh_pipeline",
                "submit_public_docs_bc21_pipeline_state",
                "write_public_docs_publish_activation_trigger_conf",
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
        "serp_publish_signed_pack.py",
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


def test_serp_public_docs_dag_runs_default_seed_registry_pipeline_path() -> None:
    source = (REPO_ROOT / "dags" / "serp_web_seed_crawl_refresh.py").read_text(encoding="utf-8")

    assert "default_public_docs_seed_refresh_conf" in source
    assert "_public_docs_seed_refresh_conf_with_defaults" in source
    assert "datetime.now(UTC)" in source
    assert "execute_pipeline_cli_spec" in source
    assert "run_public_docs_seed_refresh_pipeline" in source


def test_serp_public_docs_dag_overlays_partial_run_conf_on_default_seed_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_airflow_import_stubs(monkeypatch)
    module = importlib.import_module("dags.serp_web_seed_crawl_refresh")
    module = importlib.reload(module)

    class DagRun:
        def __init__(self) -> None:
            self.conf = {
                "artifact_root_path": str(tmp_path),
                "generated_at": "2026-07-08T21:30:00Z",
            }

    plan_json = module.validate_public_docs_seed_registry(dag_run=DagRun())
    plan = json.loads(plan_json)

    assert plan["generated_at"] == "2026-07-08T21:30:00Z"
    assert plan["seed_count"] == len(P0_PUBLIC_DOCS_SOURCES)
    assert {seed["seed_id"] for seed in plan["seed_registry"]} == {
        str(source["seed_id"]) for source in P0_PUBLIC_DOCS_SOURCES
    }
    assert all(path.startswith(str(tmp_path)) for path in plan["artifact_paths"].values())


def _install_airflow_import_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeDAG:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

    class FakePythonOperator:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def __rshift__(self, other: object) -> object:
            return other

    modules = {
        "airflow": types.ModuleType("airflow"),
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": types.ModuleType(
            "airflow.providers.standard.operators"
        ),
        "airflow.providers.standard.operators.python": types.ModuleType(
            "airflow.providers.standard.operators.python"
        ),
        "airflow.sdk": types.ModuleType("airflow.sdk"),
    }
    cast(
        Any, modules["airflow.providers.standard.operators.python"]
    ).PythonOperator = FakePythonOperator
    cast(Any, modules["airflow.sdk"]).DAG = FakeDAG
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


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


def _pipeline_seed_refresh_payload(
    status: str,
    *,
    indexed_count: int = 1,
    failed_count: int = 0,
    index_mode: str = "live",
) -> dict[str, Any]:
    return {
        "artifact_type": "public_docs_seed_refresh_batch_evidence",
        "batch_evidence": {
            "failed_count": failed_count,
            "indexed_count": indexed_count,
            "status": status,
        },
        "index_effect": "live" if index_mode == "live" else "dry-run",
        "index_mode": index_mode,
    }


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


def _public_docs_seed_refresh_conf() -> dict[str, Any]:
    return {
        "actor_id": "airflow-serp-public-docs-refresh",
        "artifact_root_path": "/var/opt/adapstory/serp-public-docs-refresh",
        "generated_at": "2026-07-08T21:00:00Z",
        "pack_id": PACK_ID,
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
                "kubernetes-openapi-docs",
                "openapi",
                "https://raw.githubusercontent.com/kubernetes/kubernetes/master/api/openapi-spec/swagger.json",
                component="Kubernetes OpenAPI",
                version="v1.34.3",
            ),
            _public_docs_seed(
                "postgresql-reference-docs",
                "website",
                "https://www.postgresql.org/docs/16/",
                component="PostgreSQL",
                version="16.1.0",
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


def _public_docs_publish_activation_conf(seed_refresh_result_path: str) -> dict[str, object]:
    return {
        "activation_idempotency_key": "018f5e13-2d73-7a77-a052-" + "8d1bcbf96603",
        "activation_reason_code": "public-docs-d20-indexed",
        "actor_id": "airflow-serp-public-docs-refresh",
        "approval_run_id": "018f5e13-2d73-7a77-a052-8d1bcbf96601",
        "artifact_root_path": "/var/opt/adapstory/serp-public-docs-publish",
        "benchmark_gate_export_sha256": "sha256:" + "c" * 64,
        "bc21_base_url": "http://serp-context-platform.env-dev.svc.cluster.local",
        "evidence_bundle_id": "018f5e13-2d73-7a77-a052-8d1bcbf96602",
        "evidence_seal_hash": "sha256:" + "b" * 64,
        "generated_at": "2026-07-08T22:00:00Z",
        "pack_id": PACK_ID,
        "pack_version_id": PACK_VERSION_ID,
        "public_docs_seed_refresh_result_path": seed_refresh_result_path,
        "registry_resource_id": REGISTRY_RESOURCE_ID,
        "registry_resource_type": "pack",
        "tenant_id": TENANT_ID,
    }


def _write_public_docs_seed_refresh_result(
    path: Path,
    *,
    tenant_id: str = TENANT_ID,
    pack_id: str = PACK_ID,
    pack_version_id: str = PACK_VERSION_ID,
) -> None:
    batch_evidence = {
        "pack_id": pack_id,
        "pack_version_id": pack_version_id,
        "tenant_id": tenant_id,
    }
    path.write_text(
        json.dumps(
            {
                "artifact_type": "public_docs_seed_refresh_batch_evidence",
                "batch_evidence": batch_evidence,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _public_docs_seed(
    seed_id: str,
    source_type: str,
    source_uri: str,
    *,
    component: str,
    version: str,
) -> dict[str, Any]:
    frontier_urls = (
        [
            "https://docs.k3s.io/quick-start",
            "https://docs.k3s.io/installation/requirements",
        ]
        if seed_id == "k3s-docs"
        else []
    )
    if seed_id == "postgresql-reference-docs":
        frontier_urls = [
            "https://www.postgresql.org/docs/16/tutorial.html",
            "https://www.postgresql.org/docs/16/sql.html",
            "https://www.postgresql.org/docs/16/index.html",
        ]
    parsed = urlparse(source_uri)
    allowed_domain = parsed.hostname or "opt.adapstory"
    return {
        "approved": True,
        "connector_name": source_type,
        "crawl_policy": {
            "allowed_domains": [allowed_domain],
            "deny_patterns": ["/login", "/admin"],
            "frontier_urls": frontier_urls,
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
