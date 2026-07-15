from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from dags.serp_benchmark_catalog_workload import (
    BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    benchmark_catalog_acquisition_container_security_context,
    benchmark_catalog_acquisition_env_vars,
    benchmark_catalog_acquisition_pod_security_context,
    benchmark_catalog_acquisition_web_identity_volume_mounts,
    benchmark_catalog_acquisition_web_identity_volumes,
)
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    build_benchmark_improvement_wave_plan,
    governance_notification_pending,
    load_benchmark_pack_lifecycle_result_snapshot,
    load_materialized_benchmark_catalog_snapshot,
    load_model_catalog_promotion_snapshot,
    write_airflow_plan_artifact,
    write_immutable_evidence_snapshot,
    write_paired_eval_request_artifact,
)
from dags.serp_evidence_workload_identity import (
    bc21_workload_env_vars,
    bc21_workload_volume_mounts,
    bc21_workload_volumes,
    hardened_runtime_container_security_context,
    hardened_runtime_pod_security_context,
    hardened_runtime_volume_mounts,
    hardened_runtime_volumes,
    kubernetes_pod_launcher_executor_config,
    minio_web_identity_env_vars,
    minio_web_identity_executor_config,
    minio_web_identity_volume_mounts,
    minio_web_identity_volumes,
)
from dags.serp_web_seed_crawl_refresh import current_airflow_runtime_image

D19_OFFICIAL_HARNESS_WORK_ITEMS = tuple(
    (suite_id, side, repetition)
    for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
    for repetition in range(1, 6)
    for side in ("baseline", "candidate")
)
D19_CODE_SANDBOX_SUITES = frozenset({"CodeRAG-Bench", "SWE-bench Verified"})

D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-aggregator"
D19_BUILDER_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-builder"
D19_MODEL_RUNNER_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-model-runner"
D19_CODE_SANDBOX_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-code-sandbox"
D19_AGGREGATOR_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-aggregator",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D19_MODEL_RUNNER_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-model-runner",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D19_CODE_SANDBOX_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-code-sandbox",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D19_BUILDER_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-builder",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D19_NATIVE_ADAPTER_RUNNER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1000m", "memory": "3Gi"},
)
D19_PACK_BUILDER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "1000m", "memory": "2Gi"},
    limits={"cpu": "8000m", "memory": "16Gi"},
)
D19_OFFICIAL_HARNESS_LIMITS: Mapping[str, Mapping[str, str]] = {
    "APIBench": {"cpu": "2000m", "memory": "4Gi"},
    "ARES": {"cpu": "4000m", "memory": "8Gi"},
    "BEIR": {"cpu": "4000m", "memory": "8Gi"},
    "CodeRAG-Bench": {"cpu": "8000m", "memory": "16Gi"},
    "RAGBench": {"cpu": "4000m", "memory": "8Gi"},
    "RepoQA": {"cpu": "8000m", "memory": "16Gi"},
    "SWE-bench Verified": {"cpu": "8000m", "memory": "16Gi"},
    "cwd-benchmark-data": {"cpu": "4000m", "memory": "8Gi"},
    "rusBEIR": {"cpu": "4000m", "memory": "8Gi"},
}
if tuple(D19_OFFICIAL_HARNESS_LIMITS) != MANDATORY_SERP_BENCHMARK_SUITES:
    raise RuntimeError("D19 resource limits must cover the canonical mandatory nine")
_D19_AGGREGATOR_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST",
)
_D19_BUILDER_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST",
    "ADAPSTORY_SERP_BC21_BASE_URL",
    "ADAPSTORY_OLLAMA_BASE_URL",
)
_D19_MODEL_RUNNER_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST",
    "ADAPSTORY_OLLAMA_BASE_URL",
)
D19_AGGREGATOR_VOLUMES = [
    *minio_web_identity_volumes(),
    *hardened_runtime_volumes(),
]
D19_AGGREGATOR_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    *hardened_runtime_volume_mounts(),
]
D19_BUILDER_VOLUMES = [
    *minio_web_identity_volumes(),
    *bc21_workload_volumes(),
    *hardened_runtime_volumes(),
]
D19_BUILDER_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    *bc21_workload_volume_mounts(),
    *hardened_runtime_volume_mounts(),
]
D19_MODEL_RUNNER_VOLUMES = [
    *minio_web_identity_volumes(),
    *hardened_runtime_volumes(),
]
D19_MODEL_RUNNER_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    *hardened_runtime_volume_mounts(),
]
D19_CODE_SANDBOX_INPUT_VOLUME = k8s.V1Volume(
    name="d19-code-sandbox-input",
    empty_dir=k8s.V1EmptyDirVolumeSource(size_limit="4Gi"),
)
D19_CODE_SANDBOX_OUTPUT_VOLUME = k8s.V1Volume(
    name="d19-code-sandbox-output",
    empty_dir=k8s.V1EmptyDirVolumeSource(size_limit="1Gi"),
)
D19_CODE_SANDBOX_TMP_VOLUME = k8s.V1Volume(
    name="d19-code-sandbox-tmp",
    empty_dir=k8s.V1EmptyDirVolumeSource(size_limit="1Gi"),
)
D19_CODE_SANDBOX_VOLUMES = [
    *minio_web_identity_volumes(),
    D19_CODE_SANDBOX_INPUT_VOLUME,
    D19_CODE_SANDBOX_OUTPUT_VOLUME,
    D19_CODE_SANDBOX_TMP_VOLUME,
]
D19_CODE_SANDBOX_PUBLISHER_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-output",
        mount_path="/sandbox/output",
        read_only=True,
    ),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-tmp",
        mount_path="/tmp",
        read_only=False,
    ),
]
D19_CODE_SANDBOX_STAGE_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-input",
        mount_path="/sandbox/input",
        read_only=False,
    ),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-tmp",
        mount_path="/tmp",
        read_only=False,
    ),
]
D19_CODE_SANDBOX_EXECUTOR_VOLUME_MOUNTS = [
    k8s.V1VolumeMount(
        name="d19-code-sandbox-input",
        mount_path="/sandbox/input",
        read_only=True,
    ),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-output",
        mount_path="/sandbox/output",
        read_only=False,
    ),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-tmp",
        mount_path="/tmp",
        read_only=False,
    ),
]
D19_AGGREGATOR_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
    labels=D19_AGGREGATOR_WORKLOAD_LABELS,
)


def d19_aggregator_env_vars() -> list[k8s.V1EnvVar]:
    """Return the minimal STS-only runtime contract for trusted aggregation."""

    return minio_web_identity_env_vars(_D19_AGGREGATOR_ENV_NAMES)


def d19_model_runner_env_vars() -> list[k8s.V1EnvVar]:
    """Expose only MinIO STS plus the governed in-cluster Ollama endpoint."""

    return minio_web_identity_env_vars(_D19_MODEL_RUNNER_ENV_NAMES)


def d19_builder_env_vars() -> list[k8s.V1EnvVar]:
    """Expose isolated-artifact indexing and BC21 binding authority only."""

    return [
        *minio_web_identity_env_vars(_D19_BUILDER_ENV_NAMES),
        *bc21_workload_env_vars(),
    ]


def d19_official_harness_runner_resources(
    suite_id: str,
) -> k8s.V1ResourceRequirements:
    """Provision the exact hard limits hashed by the suite execution envelope."""

    try:
        limits = D19_OFFICIAL_HARNESS_LIMITS[suite_id]
    except KeyError as error:
        raise ValueError("D19 official harness suite is unsupported") from error
    return k8s.V1ResourceRequirements(
        requests={"cpu": "1000m", "memory": "2Gi"},
        limits=dict(limits),
    )


def d19_code_sandbox_runtime_image() -> str:
    """Return the exact allowlisted Airflow runtime image by immutable digest."""

    repository = conf.get("kubernetes_executor", "worker_container_repository").strip()
    digest = os.environ.get("ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST", "").strip()
    normalized = digest.removeprefix("sha256:")
    if not repository or len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError("D19 code sandbox requires an immutable runtime image digest")
    return f"{repository}@sha256:{normalized}"


def validate_benchmark_improvement_wave_plan(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    plan_json = write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))
    plan = json.loads(plan_json)
    if not isinstance(plan, dict):
        raise ValueError("D19 Airflow plan must be an object")
    return plan


def load_materialized_benchmark_catalog(plan_json: str) -> dict[str, Any]:
    return load_materialized_benchmark_catalog_snapshot(plan_json)


def load_model_catalog_promotion(plan_json: str) -> dict[str, Any]:
    return load_model_catalog_promotion_snapshot(plan_json)


def write_paired_eval_request(
    plan_json: Mapping[str, Any] | str,
    catalog_snapshot: dict[str, Any],
    promotion_snapshot: dict[str, Any],
    lifecycle_result: dict[str, Any],
) -> dict[str, Any]:
    return write_paired_eval_request_artifact(
        plan_json,
        catalog_snapshot,
        promotion_snapshot,
        lifecycle_result,
    )


def load_exact_nine_evaluation_binding_snapshot(
    plan_json: Mapping[str, Any] | str,
    promotion_snapshot: Mapping[str, Any],
    lifecycle_pointer: Mapping[str, Any],
) -> dict[str, Any]:
    return load_benchmark_pack_lifecycle_result_snapshot(
        plan_json,
        promotion_snapshot,
        lifecycle_pointer,
    )


def write_paired_evaluation_assembly_plan(
    plan_json: Mapping[str, Any] | str,
    request_snapshot: Mapping[str, Any],
    work_item_plan: Mapping[str, Any],
    run_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Seal the exact 90 server-owned work-item/receipt pairs for aggregation."""

    plan = dict(plan_json) if isinstance(plan_json, Mapping) else json.loads(plan_json)
    if not isinstance(plan, Mapping):
        raise ValueError("D19 assembly plan input must be an object")
    if plan.get("dag_id") != "serp_benchmark_improvement_wave":
        raise ValueError("D19 assembly plan DAG identity does not match")
    request_evidence = _assembly_artifact_evidence(
        request_snapshot.get("requestEvidence"), "requestEvidence"
    )
    work_items = work_item_plan.get("workItems")
    if not isinstance(work_items, list) or len(work_items) != len(D19_OFFICIAL_HARNESS_WORK_ITEMS):
        raise ValueError("D19 work-item plan must contain the exact canonical 90")
    if len(run_results) != len(D19_OFFICIAL_HARNESS_WORK_ITEMS):
        raise ValueError("D19 runner results must contain the exact canonical 90")
    runs: list[dict[str, dict[str, str]]] = []
    for expected, work_item, result in zip(
        D19_OFFICIAL_HARNESS_WORK_ITEMS, work_items, run_results, strict=True
    ):
        if not isinstance(work_item, Mapping) or not isinstance(result, Mapping):
            raise ValueError("D19 work items and runner results must be objects")
        observed_work_item = (
            work_item.get("suiteId"),
            work_item.get("side"),
            work_item.get("repetition"),
        )
        observed_result = (
            result.get("suiteId"),
            result.get("side"),
            result.get("repetition"),
        )
        if observed_work_item != expected or observed_result != expected:
            raise ValueError("D19 work items and receipts must use canonical identity order")
        runs.append(
            {
                "workItemEvidence": _assembly_artifact_evidence(
                    work_item.get("workItemEvidence"), "workItemEvidence"
                ),
                "receiptEvidence": _assembly_artifact_evidence(
                    result.get("receiptEvidence"), "receiptEvidence"
                ),
            }
        )
    artifact_paths = plan.get("artifact_paths")
    if not isinstance(artifact_paths, Mapping):
        raise ValueError("D19 plan artifact_paths is required")
    assembly_path = artifact_paths.get("paired_evaluation_assembly_plan")
    manifest_output = artifact_paths.get("paired_execution_manifest")
    if not isinstance(assembly_path, str) or not assembly_path.startswith("s3://"):
        raise ValueError("D19 paired_evaluation_assembly_plan path must use s3://")
    if not isinstance(manifest_output, str) or not manifest_output.startswith("s3://"):
        raise ValueError("D19 paired_execution_manifest path must use s3://")
    payload = {
        "schema": "PairedEvaluationAssemblyPlan/v1",
        "requestEvidence": request_evidence,
        "manifestOutput": manifest_output,
        "runs": runs,
    }
    written = write_immutable_evidence_snapshot(
        assembly_path,
        artifact_type="paired_evaluation_assembly_plan",
        operation_id=str(plan.get("operation_id", "")),
        payload=payload,
    )
    return {
        "assemblyPlanEvidence": _assembly_artifact_evidence(written, "assemblyPlanEvidence"),
        "manifestOutput": manifest_output,
        "runCount": len(runs),
    }


def _assembly_artifact_evidence(value: object, field_name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an immutable artifact handle")
    path = value.get("artifactPath")
    version_id = value.get("artifactVersionId")
    digest = value.get("artifactSha256")
    if not isinstance(path, str) or not path.startswith("s3://"):
        raise ValueError(f"{field_name} artifactPath must use s3://")
    if not isinstance(version_id, str) or not version_id.strip():
        raise ValueError(f"{field_name} artifactVersionId is required")
    if not isinstance(digest, str):
        raise ValueError(f"{field_name} artifactSha256 is required")
    normalized_digest = digest if digest.startswith("sha256:") else "sha256:" + digest
    if len(normalized_digest) != 71 or any(
        character not in "0123456789abcdef" for character in normalized_digest[7:]
    ):
        raise ValueError(f"{field_name} artifactSha256 is invalid")
    if value.get("objectLockMode") != "COMPLIANCE":
        raise ValueError(f"{field_name} must use COMPLIANCE object lock")
    return {
        "artifactPath": path,
        "artifactSha256": normalized_digest,
        "artifactVersionId": version_id,
        "objectLockMode": "COMPLIANCE",
    }


default_args = {
    "owner": "serp-eval-runner",
    "start_date": datetime(2026, 7, 5, tzinfo=UTC),
    "retries": 0,
}

dag = DAG(
    "serp_benchmark_improvement_wave",
    default_args=default_args,
    description="SERP D19 benchmark ratchet keep/discard contract",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    tags=["serp", "evals", "benchmark", "improvement"],
)

validate_plan = PythonOperator(
    task_id="validate_benchmark_improvement_wave_plan",
    python_callable=validate_benchmark_improvement_wave_plan,
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

materialize_catalog = KubernetesPodOperator(
    task_id="materialize_live_benchmark_catalog",
    name="serp-d19-benchmark-catalog-acquisition",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "dags.serp_benchmark_catalog_materializer"],
    arguments=[
        "--plan-json-urlencoded",
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') "
            "| tojson | urlencode }}"
        ),
    ],
    env_vars=benchmark_catalog_acquisition_env_vars(),
    service_account_name=BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=benchmark_catalog_acquisition_web_identity_volumes(),
    volume_mounts=benchmark_catalog_acquisition_web_identity_volume_mounts(),
    labels=BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    container_resources=BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    security_context=benchmark_catalog_acquisition_pod_security_context(),
    container_security_context=benchmark_catalog_acquisition_container_security_context(),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=1,
    retry_delay=timedelta(seconds=BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS),
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

load_catalog = PythonOperator(
    task_id="load_materialized_benchmark_catalog",
    python_callable=load_materialized_benchmark_catalog,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

load_promotion = PythonOperator(
    task_id="load_model_catalog_promotion",
    python_callable=load_model_catalog_promotion,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

build_exact_nine_benchmark_packs = KubernetesPodOperator(
    task_id="build_exact_nine_benchmark_packs",
    name="serp-d19-build-exact-nine-benchmark-packs",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=[
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.bc21_benchmark_pack_lifecycle_cli",
    ],
    arguments=[
        "build-exact-nine",
        "--benchmark-catalog",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog')['artifactPath'] }}",
        "--benchmark-catalog-version-id",
        (
            "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog')"
            "['artifactVersionId'] }}"
        ),
        "--benchmark-catalog-sha256",
        (
            "sha256:{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog')"
            "['artifactSha256'] }}"
        ),
        "--promotion",
        (
            "{{ ti.xcom_pull(task_ids='load_model_catalog_promotion')"
            "['promotionEvidence']['s3Uri'] }}"
        ),
        "--promotion-version-id",
        (
            "{{ ti.xcom_pull(task_ids='load_model_catalog_promotion')"
            "['promotionEvidence']['versionId'] }}"
        ),
        "--promotion-sha256",
        (
            "{{ ti.xcom_pull(task_ids='load_model_catalog_promotion')"
            "['promotionEvidence']['sha256'] }}"
        ),
        "--result-output",
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            "['artifact_paths']['benchmark_pack_build_result'] }}"
        ),
    ],
    env_vars=d19_builder_env_vars(),
    service_account_name=D19_BUILDER_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_BUILDER_VOLUMES,
    volume_mounts=D19_BUILDER_VOLUME_MOUNTS,
    labels=D19_BUILDER_WORKLOAD_LABELS,
    container_resources=D19_PACK_BUILDER_RESOURCES,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    do_xcom_push=True,
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

register_exact_nine_evaluation_binding = KubernetesPodOperator(
    task_id="register_exact_nine_evaluation_binding",
    name="serp-d19-register-exact-nine-evaluation-binding",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=[
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.bc21_benchmark_pack_lifecycle_cli",
    ],
    arguments=[
        "register-binding",
        "--lifecycle-input",
        (
            "{{ ti.xcom_pull(task_ids='build_exact_nine_benchmark_packs')"
            "['packBuildResultEvidence']['artifactPath'] }}"
        ),
        "--lifecycle-input-version-id",
        (
            "{{ ti.xcom_pull(task_ids='build_exact_nine_benchmark_packs')"
            "['packBuildResultEvidence']['artifactVersionId'] }}"
        ),
        "--lifecycle-input-sha256",
        (
            "{{ ti.xcom_pull(task_ids='build_exact_nine_benchmark_packs')"
            "['packBuildResultEvidence']['artifactSha256'] }}"
        ),
        "--result-output",
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            "['artifact_paths']['benchmark_pack_lifecycle_result'] }}"
        ),
    ],
    env_vars=d19_builder_env_vars(),
    service_account_name=D19_BUILDER_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_BUILDER_VOLUMES,
    volume_mounts=D19_BUILDER_VOLUME_MOUNTS,
    labels=D19_BUILDER_WORKLOAD_LABELS,
    container_resources=D19_PACK_BUILDER_RESOURCES,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    do_xcom_push=True,
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

load_exact_nine_evaluation_binding = PythonOperator(
    task_id="load_exact_nine_evaluation_binding",
    python_callable=load_exact_nine_evaluation_binding_snapshot,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}",
        "{{ ti.xcom_pull(task_ids='load_model_catalog_promotion') }}",
        "{{ ti.xcom_pull(task_ids='register_exact_nine_evaluation_binding') }}",
    ],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

write_request = PythonOperator(
    task_id="write_paired_eval_request",
    python_callable=write_paired_eval_request,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog') }}",
        "{{ ti.xcom_pull(task_ids='load_model_catalog_promotion') }}",
        "{{ ti.xcom_pull(task_ids='load_exact_nine_evaluation_binding') }}",
    ],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

materialize_official_harness_work_items = KubernetesPodOperator(
    task_id="materialize_official_harness_work_items",
    name="serp-d19-materialize-official-harness-work-items",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=[
        "python",
        "-m",
        "adapstory_serp_pipeline.orchestration.official_harness_execution",
    ],
    arguments=[
        "materialize-work-items",
        "--paired-eval-request",
        "{{ ti.xcom_pull(task_ids='write_paired_eval_request')['artifactPath'] }}",
        "--paired-eval-request-version-id",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_eval_request')"
            "['requestEvidence']['artifactVersionId'] }}"
        ),
        "--paired-eval-request-sha256",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_eval_request')"
            "['requestEvidence']['artifactSha256'] }}"
        ),
        "--lifecycle-result",
        (
            "{{ ti.xcom_pull(task_ids='register_exact_nine_evaluation_binding')"
            "['lifecycleResultEvidence']['artifactPath'] }}"
        ),
        "--lifecycle-result-version-id",
        (
            "{{ ti.xcom_pull(task_ids='register_exact_nine_evaluation_binding')"
            "['lifecycleResultEvidence']['artifactVersionId'] }}"
        ),
        "--lifecycle-result-sha256",
        (
            "{{ ti.xcom_pull(task_ids='register_exact_nine_evaluation_binding')"
            "['lifecycleResultEvidence']['artifactSha256'] }}"
        ),
    ],
    env_vars=d19_aggregator_env_vars(),
    service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_AGGREGATOR_VOLUMES,
    volume_mounts=D19_AGGREGATOR_VOLUME_MOUNTS,
    labels=D19_AGGREGATOR_WORKLOAD_LABELS,
    container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    do_xcom_push=True,
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)


def _official_harness_task_id(suite_id: str, side: str, repetition: int) -> str:
    slug = suite_id.casefold().replace(" ", "_").replace("-", "_")
    return f"run_official_harness_{slug}_{side}_{repetition}"


def _code_sandbox_task_id(
    phase: str, suite_id: str, side: str, repetition: int
) -> str:
    slug = suite_id.casefold().replace(" ", "_").replace("-", "_")
    return f"{phase}_code_sandbox_{slug}_{side}_{repetition}"


def _final_harness_task_id(suite_id: str, side: str, repetition: int) -> str:
    if suite_id in D19_CODE_SANDBOX_SUITES:
        return _code_sandbox_task_id("seal", suite_id, side, repetition)
    return _official_harness_task_id(suite_id, side, repetition)


D19_STANDARD_HARNESS_RUN_TASKS: dict[
    tuple[str, str, int], KubernetesPodOperator
] = {}
D19_CODE_SANDBOX_PREPARE_TASKS: dict[
    tuple[str, str, int], KubernetesPodOperator
] = {}
D19_CODE_SANDBOX_TASKS: dict[tuple[str, str, int], KubernetesPodOperator] = {}
D19_CODE_SANDBOX_SEAL_TASKS: dict[
    tuple[str, str, int], KubernetesPodOperator
] = {}
D19_OFFICIAL_HARNESS_RUN_TASKS: dict[tuple[str, str, int], KubernetesPodOperator] = {}
for work_item_index, (suite_id, side, repetition) in enumerate(D19_OFFICIAL_HARNESS_WORK_ITEMS):
    identity = (suite_id, side, repetition)
    work_item_root = (
        "{{ ti.xcom_pull(task_ids='materialize_official_harness_work_items')"
        f"['workItems'][{work_item_index}]['workItemEvidence']"
    )
    if suite_id in D19_CODE_SANDBOX_SUITES:
        prepare_task_id = _code_sandbox_task_id("prepare", suite_id, side, repetition)
        sandbox_task_id = _code_sandbox_task_id("execute", suite_id, side, repetition)
        seal_task_id = _code_sandbox_task_id("seal", suite_id, side, repetition)
        prepare_task = KubernetesPodOperator(
            task_id=prepare_task_id,
            name=f"serp-d19-prepare-code-sandbox-{work_item_index + 1:02d}",
            namespace=conf.get("kubernetes_executor", "namespace"),
            image=current_airflow_runtime_image(),
            cmds=[
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.official_harness_execution",
            ],
            arguments=[
                "prepare-code-sandbox",
                "--work-item",
                work_item_root + "['artifactPath'] }}",
                "--work-item-version-id",
                work_item_root + "['artifactVersionId'] }}",
                "--work-item-sha256",
                work_item_root + "['artifactSha256'] }}",
            ],
            env_vars=d19_aggregator_env_vars(),
            service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
            automount_service_account_token=False,
            volumes=D19_AGGREGATOR_VOLUMES,
            volume_mounts=D19_AGGREGATOR_VOLUME_MOUNTS,
            labels=D19_AGGREGATOR_WORKLOAD_LABELS,
            container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
            security_context=hardened_runtime_pod_security_context(),
            container_security_context=hardened_runtime_container_security_context(),
            do_xcom_push=True,
            get_logs=True,
            log_events_on_failure=True,
            random_name_suffix=True,
            reattach_on_restart=True,
            on_kill_action="keep_pod",
            on_finish_action="delete_pod",
            retries=0,
            executor_config=kubernetes_pod_launcher_executor_config(),
            dag=dag,
        )
        sandbox_work_item_root = (
            "{{ ti.xcom_pull(task_ids='"
            + prepare_task_id
            + "')['sandboxWorkItemEvidence']"
        )
        sandbox_image = d19_code_sandbox_runtime_image()
        stage_container = k8s.V1Container(
            name="stage-code-sandbox",
            image=sandbox_image,
            command=[
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.official_harness_execution",
            ],
            args=[
                "stage-code-sandbox",
                "--sandbox-work-item",
                sandbox_work_item_root + "['artifactPath'] }}",
                "--sandbox-work-item-version-id",
                sandbox_work_item_root + "['artifactVersionId'] }}",
                "--sandbox-work-item-sha256",
                sandbox_work_item_root + "['artifactSha256'] }}",
                "--input-dir",
                "/sandbox/input",
            ],
            env=d19_aggregator_env_vars(),
            env_from=[],
            resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
            security_context=hardened_runtime_container_security_context(),
            volume_mounts=D19_CODE_SANDBOX_STAGE_VOLUME_MOUNTS,
        )
        sandbox_executor = k8s.V1Container(
            name="sandbox-executor",
            image=sandbox_image,
            command=[
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.official_harness_execution",
            ],
            args=[
                "execute-code-sandbox",
                "--input-manifest",
                "/sandbox/input/sandbox-work-item.json",
                "--output-result",
                "/sandbox/output/sandbox-result.json",
            ],
            env=[],
            env_from=[],
            resources=d19_official_harness_runner_resources(suite_id),
            security_context=hardened_runtime_container_security_context(),
            volume_mounts=D19_CODE_SANDBOX_EXECUTOR_VOLUME_MOUNTS,
        )
        sandbox_task = KubernetesPodOperator(
            task_id=sandbox_task_id,
            name=f"serp-d19-execute-code-sandbox-{work_item_index + 1:02d}",
            namespace=conf.get("kubernetes_executor", "namespace"),
            image=sandbox_image,
            cmds=[
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.official_harness_execution",
            ],
            arguments=[
                "publish-code-sandbox-result",
                "--sandbox-work-item",
                sandbox_work_item_root + "['artifactPath'] }}",
                "--sandbox-work-item-version-id",
                sandbox_work_item_root + "['artifactVersionId'] }}",
                "--sandbox-work-item-sha256",
                sandbox_work_item_root + "['artifactSha256'] }}",
                "--result",
                "/sandbox/output/sandbox-result.json",
                "--xcom-output",
                "/airflow/xcom/return.json",
            ],
            env_vars=d19_aggregator_env_vars(),
            service_account_name=D19_CODE_SANDBOX_WORKLOAD_SERVICE_ACCOUNT,
            automount_service_account_token=False,
            volumes=D19_CODE_SANDBOX_VOLUMES,
            volume_mounts=D19_CODE_SANDBOX_PUBLISHER_VOLUME_MOUNTS,
            labels=D19_CODE_SANDBOX_WORKLOAD_LABELS,
            container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
            security_context=hardened_runtime_pod_security_context(),
            container_security_context=hardened_runtime_container_security_context(),
            init_containers=[stage_container],
            full_pod_spec=k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    automount_service_account_token=False,
                    containers=[k8s.V1Container(name="base"), sandbox_executor],
                    share_process_namespace=False,
                )
            ),
            do_xcom_push=True,
            get_logs=True,
            container_logs=["base", "sandbox-executor"],
            init_container_logs=["stage-code-sandbox"],
            log_events_on_failure=True,
            random_name_suffix=True,
            reattach_on_restart=True,
            on_kill_action="keep_pod",
            on_finish_action="delete_pod",
            retries=0,
            executor_config=kubernetes_pod_launcher_executor_config(),
            dag=dag,
        )
        sandbox_result_root = (
            "{{ ti.xcom_pull(task_ids='"
            + sandbox_task_id
            + "')['sandboxResultEvidence']"
        )
        seal_task = KubernetesPodOperator(
            task_id=seal_task_id,
            name=f"serp-d19-seal-code-receipt-{work_item_index + 1:02d}",
            namespace=conf.get("kubernetes_executor", "namespace"),
            image=current_airflow_runtime_image(),
            cmds=[
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.official_harness_execution",
            ],
            arguments=[
                "seal-code-receipt",
                "--work-item",
                work_item_root + "['artifactPath'] }}",
                "--work-item-version-id",
                work_item_root + "['artifactVersionId'] }}",
                "--work-item-sha256",
                work_item_root + "['artifactSha256'] }}",
                "--sandbox-result",
                sandbox_result_root + "['artifactPath'] }}",
                "--sandbox-result-version-id",
                sandbox_result_root + "['artifactVersionId'] }}",
                "--sandbox-result-sha256",
                sandbox_result_root + "['artifactSha256'] }}",
            ],
            env_vars=d19_aggregator_env_vars(),
            service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
            automount_service_account_token=False,
            volumes=D19_AGGREGATOR_VOLUMES,
            volume_mounts=D19_AGGREGATOR_VOLUME_MOUNTS,
            labels=D19_AGGREGATOR_WORKLOAD_LABELS,
            container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
            security_context=hardened_runtime_pod_security_context(),
            container_security_context=hardened_runtime_container_security_context(),
            do_xcom_push=True,
            get_logs=True,
            log_events_on_failure=True,
            random_name_suffix=True,
            reattach_on_restart=True,
            on_kill_action="keep_pod",
            on_finish_action="delete_pod",
            retries=0,
            executor_config=kubernetes_pod_launcher_executor_config(),
            dag=dag,
        )
        D19_CODE_SANDBOX_PREPARE_TASKS[identity] = prepare_task
        D19_CODE_SANDBOX_TASKS[identity] = sandbox_task
        D19_CODE_SANDBOX_SEAL_TASKS[identity] = seal_task
        D19_OFFICIAL_HARNESS_RUN_TASKS[identity] = seal_task
        continue
    runner = KubernetesPodOperator(
        task_id=_official_harness_task_id(suite_id, side, repetition),
        name=f"serp-d19-official-harness-{work_item_index + 1:02d}",
        namespace=conf.get("kubernetes_executor", "namespace"),
        image=current_airflow_runtime_image(),
        cmds=[
            "python",
            "-m",
            "adapstory_serp_pipeline.orchestration.official_harness_execution",
        ],
        arguments=[
            "run-suite",
            "--work-item",
            work_item_root + "['artifactPath'] }}",
            "--work-item-version-id",
            work_item_root + "['artifactVersionId'] }}",
            "--work-item-sha256",
            work_item_root + "['artifactSha256'] }}",
        ],
        env_vars=d19_model_runner_env_vars(),
        service_account_name=D19_MODEL_RUNNER_WORKLOAD_SERVICE_ACCOUNT,
        automount_service_account_token=False,
        volumes=D19_MODEL_RUNNER_VOLUMES,
        volume_mounts=D19_MODEL_RUNNER_VOLUME_MOUNTS,
        labels=D19_MODEL_RUNNER_WORKLOAD_LABELS,
        container_resources=d19_official_harness_runner_resources(suite_id),
        security_context=hardened_runtime_pod_security_context(),
        container_security_context=hardened_runtime_container_security_context(),
        do_xcom_push=True,
        get_logs=True,
        log_events_on_failure=True,
        random_name_suffix=True,
        reattach_on_restart=True,
        on_kill_action="keep_pod",
        on_finish_action="delete_pod",
        retries=1,
        retry_delay=timedelta(seconds=15),
        executor_config=kubernetes_pod_launcher_executor_config(),
        dag=dag,
    )
    D19_STANDARD_HARNESS_RUN_TASKS[identity] = runner
    D19_OFFICIAL_HARNESS_RUN_TASKS[identity] = runner

runner_task_ids = [
    _final_harness_task_id(suite_id, side, repetition)
    for suite_id, side, repetition in D19_OFFICIAL_HARNESS_WORK_ITEMS
]
write_assembly_plan = PythonOperator(
    task_id="write_paired_evaluation_assembly_plan",
    python_callable=write_paired_evaluation_assembly_plan,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}",
        "{{ ti.xcom_pull(task_ids='write_paired_eval_request') }}",
        "{{ ti.xcom_pull(task_ids='materialize_official_harness_work_items') }}",
        "{{ ti.xcom_pull(task_ids=" + json.dumps(runner_task_ids) + ") }}",
    ],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

assemble_paired_execution_manifest = KubernetesPodOperator(
    task_id="assemble_paired_execution_manifest",
    name="serp-d19-assemble-paired-execution-manifest",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=[
        "python",
        "-m",
        "adapstory_serp_pipeline.orchestration.official_harness_execution",
    ],
    arguments=[
        "assemble-manifest",
        "--assembly-plan",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            "['assemblyPlanEvidence']['artifactPath'] }}"
        ),
        "--assembly-plan-version-id",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            "['assemblyPlanEvidence']['artifactVersionId'] }}"
        ),
        "--assembly-plan-sha256",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            "['assemblyPlanEvidence']['artifactSha256'] }}"
        ),
    ],
    env_vars=d19_aggregator_env_vars(),
    service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_AGGREGATOR_VOLUMES,
    volume_mounts=D19_AGGREGATOR_VOLUME_MOUNTS,
    labels=D19_AGGREGATOR_WORKLOAD_LABELS,
    container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    do_xcom_push=True,
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

run_paired_evaluation = KubernetesPodOperator(
    task_id="run_paired_benchmark_evaluation",
    name="serp-paired-benchmark-evaluation",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=["python", "-m", "adapstory_serp_pipeline.orchestration.paired_eval_receipt"],
    arguments=[
        "--paired-eval-request",
        "{{ ti.xcom_pull(task_ids='write_paired_eval_request')['artifactPath'] }}",
        "--paired-eval-request-version-id",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_eval_request')"
            "['requestEvidence']['artifactVersionId'] }}"
        ),
        "--paired-eval-request-sha256",
        (
            "{{ ti.xcom_pull(task_ids='write_paired_eval_request')"
            "['requestEvidence']['artifactSha256'] }}"
        ),
        "--execution-manifest",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestEvidence']['artifactPath'] }}"
        ),
        "--execution-manifest-version-id",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestEvidence']['artifactVersionId'] }}"
        ),
        "--execution-manifest-sha256",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestEvidence']['artifactSha256'] }}"
        ),
        "--evidence-output",
        "{{ ti.xcom_pull(task_ids='write_paired_eval_request')['evidenceOutputPath'] }}",
    ],
    env_vars=d19_aggregator_env_vars(),
    service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_AGGREGATOR_VOLUMES,
    volume_mounts=D19_AGGREGATOR_VOLUME_MOUNTS,
    labels=D19_AGGREGATOR_WORKLOAD_LABELS,
    container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="keep_pod",
    on_finish_action="delete_pod",
    retries=0,
    retry_delay=timedelta(seconds=5),
    executor_config=kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

notify_governance = PythonOperator(
    task_id="notify_governance_eval_surfaces",
    python_callable=governance_notification_pending,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

validate_plan >> materialize_catalog >> load_catalog
validate_plan >> load_promotion
load_catalog >> build_exact_nine_benchmark_packs
load_promotion >> build_exact_nine_benchmark_packs
build_exact_nine_benchmark_packs >> register_exact_nine_evaluation_binding
register_exact_nine_evaluation_binding >> load_exact_nine_evaluation_binding
load_catalog >> write_request
load_promotion >> write_request
load_exact_nine_evaluation_binding >> write_request
write_request >> materialize_official_harness_work_items
for standard_runner in D19_STANDARD_HARNESS_RUN_TASKS.values():
    materialize_official_harness_work_items >> standard_runner >> write_assembly_plan
for identity, prepare_task in D19_CODE_SANDBOX_PREPARE_TASKS.items():
    sandbox_task = D19_CODE_SANDBOX_TASKS[identity]
    seal_task = D19_CODE_SANDBOX_SEAL_TASKS[identity]
    materialize_official_harness_work_items >> prepare_task
    prepare_task >> sandbox_task >> seal_task >> write_assembly_plan
write_assembly_plan >> assemble_paired_execution_manifest
assemble_paired_execution_manifest >> run_paired_evaluation >> notify_governance
