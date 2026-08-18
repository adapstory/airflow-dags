from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from airflow.configuration import conf
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from airflow.task.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

from dags.serp_bc10_capacity_admission import (
    verify_bc10_ledger_capacity_admission,
)
from dags.serp_benchmark_catalog_workload import (
    BENCHMARK_CATALOG_ACQUISITION_NODE_SELECTOR,
    BENCHMARK_CATALOG_ACQUISITION_RESOURCES,
    BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS,
    BENCHMARK_CATALOG_ACQUISITION_TOLERATIONS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS,
    BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT,
    benchmark_catalog_acquisition_container_security_context,
    benchmark_catalog_acquisition_env_vars,
    benchmark_catalog_acquisition_pod_security_context,
    benchmark_catalog_acquisition_web_identity_volume_mounts,
    benchmark_catalog_acquisition_web_identity_volumes,
)
from dags.serp_d19_history_observer import admit_d19_run
from dags.serp_d19_release_handoff import build_terminal_outbox
from dags.serp_ds1000_contract import DS1000_EXECUTOR_COMMAND
from dags.serp_eval_contracts import (
    MANDATORY_SERP_BENCHMARK_SUITES,
    build_benchmark_improvement_wave_plan,
    load_benchmark_pack_lifecycle_result_snapshot,
    load_materialized_benchmark_catalog_snapshot,
    load_model_catalog_promotion_snapshot,
    publish_official_serp_mcp_measurement,
    verify_model_catalog_promotion_terminal_activation,
    write_airflow_plan_artifact,
    write_immutable_evidence_snapshot,
    write_official_serp_mcp_measurement,
    write_paired_eval_request_artifact,
    write_paired_evaluation_verification_evidence,
)
from dags.serp_evidence_workload_identity import (
    MINIO_WEB_IDENTITY_EXPIRATION_SECONDS,
    MINIO_WEB_IDENTITY_TOKEN_FILE,
    SERP_RUNTIME_GROUP_ID,
    SERP_RUNTIME_USER_ID,
    bc10_workload_env_vars,
    bc10_workload_volume_mounts,
    bc10_workload_volumes,
    bc21_authorized_executor_config,
    bc21_workload_env_vars,
    bc21_workload_volume_mounts,
    bc21_workload_volumes,
    evaluation_admission_verifier_executor_config,
    hardened_runtime_container_security_context,
    hardened_runtime_pod_security_context,
    hardened_runtime_volume_mounts,
    hardened_runtime_volumes,
    kubernetes_pod_launcher_executor_config,
    minio_web_identity_env_vars,
    minio_web_identity_executor_config,
    minio_web_identity_volume_mounts,
    minio_web_identity_volumes,
    vault_transit_env_vars,
    vault_transit_volume_mounts,
    vault_transit_volumes,
)
from dags.serp_kubernetes_job_operator import (
    DEFAULT_JOB_POD_DISCOVERY_TIMEOUT_SECONDS,
    BoundedKubernetesJobOperator,
    ReceiptKubernetesPodOperator,
)
from dags.serp_web_seed_crawl_refresh import current_airflow_runtime_image

D19_DAG_ID = "serp_benchmark_improvement_wave"
D19_TERMINAL_OUTBOX_TASK_ID = "write_d19_release_terminal_outbox"
D19_RELEASE_TRANSITION_TASK_ID = "choose_d19_release_transition"
D19_TRIGGER_NEXT_RUN_TASK_ID = "trigger_next_d19_release_run"
D19_RELEASE_COMPLETE_TASK_ID = "d19_release_terminal_complete"
D19_CATALOG_READINESS_GATE_TASK_ID = "choose_d19_catalog_readiness_transition"
D19_CATALOG_BLOCKER_TASK_ID = "fail_d19_blocked_catalog"
D19_PACK_CAS_CLASSIFIER_TASK_ID = "classify_pack_cas"
D19_PACK_CAS_GATE_TASK_ID = "pack_cas_readiness_gate"
D19_PACK_CAS_RECOVERY_PLAN_TASK_ID = "plan_pack_cas_migration_rebuild"
D19_PACK_CAS_RECOVERY_REQUIRED_TASK_ID = "pack_cas_migration_rebuild_required"
D19_OFFICIAL_HARNESS_WORK_ITEMS = tuple(
    (suite_id, side, repetition)
    for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
    for repetition in range(1, 6)
    for side in ("baseline", "candidate")
)
D19_CODE_SANDBOX_SUITES = frozenset({"CodeRAG-Bench", "SWE-bench Verified"})
D19_CODE_SANDBOX_EXECUTOR_SPEC: Mapping[str, tuple[str, tuple[str, ...]]] = {
    "CodeRAG-Bench": (
        DS1000_EXECUTOR_COMMAND,
        ("/sandbox/input/ds1000_executor.py",),
    ),
    "SWE-bench Verified": ("/bin/bash", ("/sandbox/input/swe_executor.sh",)),
}
if frozenset(D19_CODE_SANDBOX_EXECUTOR_SPEC) != D19_CODE_SANDBOX_SUITES:
    raise RuntimeError("D19 executor specs must cover the canonical code suites")
SANDBOX_WORK_ITEM_SET_SCHEMA = "SandboxWorkItemSet/v1"
SANDBOX_RESULT_SET_ASSEMBLY_PLAN_SCHEMA = "SandboxResultSetAssemblyPlan/v1"

_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_GIT_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_IMAGE_REFERENCE = re.compile(
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}\Z"
)
_REPOSITORY = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,98}[A-Za-z0-9])?/"
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,98}[A-Za-z0-9])?\Z"
)

D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-aggregator"
D19_OFFICIAL_MEASUREMENT_PUBLISHER_WORKLOAD_SERVICE_ACCOUNT = (
    "airflow-serp-official-measurement-publisher"
)
D19_ADMISSION_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-d19-history-observer"
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
D19_OFFICIAL_MEASUREMENT_PUBLISHER_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "official-measurement-publisher",
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
D19_ADMISSION_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "d19-fence-admission",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D19_RUNTIME_ADMISSION_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "evaluation-admission-verifier",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
D19_NATIVE_ADAPTER_RUNNER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "1Gi"},
    limits={"cpu": "1000m", "memory": "3Gi"},
)
D19_PACK_BUILDER_SCRATCH_PATH = "/var/lib/adapstory/serp-pack"
D19_PACK_BUILDER_SCRATCH_VOLUME = k8s.V1Volume(
    name="d19-pack-builder-scratch",
    empty_dir=k8s.V1EmptyDirVolumeSource(size_limit="24Gi"),
)
D19_SWE_PACK_BUILDER_SCRATCH_VOLUME = k8s.V1Volume(
    name="d19-pack-builder-scratch",
    empty_dir=k8s.V1EmptyDirVolumeSource(size_limit="48Gi"),
)
D19_PACK_BUILDER_SCRATCH_VOLUME_MOUNT = k8s.V1VolumeMount(
    name="d19-pack-builder-scratch",
    mount_path=D19_PACK_BUILDER_SCRATCH_PATH,
    read_only=False,
)
D19_PACK_BUILDER_POOL = "serp_d19_pack_builders"
D19_PACK_BUILDER_RESOURCES = k8s.V1ResourceRequirements(
    requests={
        "cpu": "4",
        "ephemeral-storage": "8Gi",
        "memory": "4Gi",
    },
    limits={
        "cpu": "4",
        "ephemeral-storage": "28Gi",
        "memory": "8Gi",
    },
)
D19_SWE_PACK_BUILDER_RESOURCES = k8s.V1ResourceRequirements(
    requests={
        "cpu": "4",
        "ephemeral-storage": "32Gi",
        "memory": "4Gi",
    },
    limits={
        "cpu": "4",
        "ephemeral-storage": "56Gi",
        "memory": "8Gi",
    },
)
D19_REMOTE_COMPUTE_NODE_SELECTOR = {"adapstory.com/compute-class": "remote"}
D19_REMOTE_COMPUTE_TOLERATIONS = [
    k8s.V1Toleration(
        effect="NoSchedule",
        key="adapstory.com/compute-class",
        operator="Equal",
        value="remote",
    )
]


def d19_remote_kubernetes_pod_launcher_executor_config() -> dict[str, Any]:
    """Keep long-lived D19 child-job supervision off the control-plane node."""

    return kubernetes_pod_launcher_executor_config(
        node_selector=D19_REMOTE_COMPUTE_NODE_SELECTOR,
        tolerations=D19_REMOTE_COMPUTE_TOLERATIONS,
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
    "ADAPSTORY_BC10_GATEWAY_URL",
    "ADAPSTORY_SERP_BC21_BASE_URL",
)
_D19_MODEL_RUNNER_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST",
    "ADAPSTORY_BC10_GATEWAY_URL",
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
    *bc10_workload_volumes(),
    *bc21_workload_volumes(),
    *hardened_runtime_volumes(),
    D19_PACK_BUILDER_SCRATCH_VOLUME,
]
D19_SWE_BUILDER_VOLUMES = [
    *minio_web_identity_volumes(),
    *bc10_workload_volumes(),
    *bc21_workload_volumes(),
    *hardened_runtime_volumes(),
    D19_SWE_PACK_BUILDER_SCRATCH_VOLUME,
]
D19_BUILDER_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    *bc10_workload_volume_mounts(),
    *bc21_workload_volume_mounts(),
    *hardened_runtime_volume_mounts(),
    D19_PACK_BUILDER_SCRATCH_VOLUME_MOUNT,
]
D19_MODEL_RUNNER_VOLUMES = [
    *minio_web_identity_volumes(),
    *bc10_workload_volumes(),
    *hardened_runtime_volumes(),
]
D19_MODEL_RUNNER_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    *bc10_workload_volume_mounts(),
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
D19_CODE_SANDBOX_WORKSPACE_VOLUME = k8s.V1Volume(
    name="d19-code-sandbox-workspace",
    empty_dir=k8s.V1EmptyDirVolumeSource(size_limit="32Gi"),
)
D19_CODE_SANDBOX_POD_STATUS_TOKEN_VOLUME = k8s.V1Volume(
    name="d19-code-sandbox-pod-status-token",
    projected=k8s.V1ProjectedVolumeSource(
        sources=[
            k8s.V1VolumeProjection(
                service_account_token=k8s.V1ServiceAccountTokenProjection(
                    expiration_seconds=900,
                    path="token",
                )
            ),
            k8s.V1VolumeProjection(
                config_map=k8s.V1ConfigMapProjection(
                    items=[k8s.V1KeyToPath(key="ca.crt", path="ca.crt")],
                    name="kube-root-ca.crt",
                )
            ),
        ]
    ),
)
D19_CODE_SANDBOX_VOLUMES = [
    *minio_web_identity_volumes(),
    D19_CODE_SANDBOX_INPUT_VOLUME,
    D19_CODE_SANDBOX_OUTPUT_VOLUME,
    D19_CODE_SANDBOX_TMP_VOLUME,
    D19_CODE_SANDBOX_WORKSPACE_VOLUME,
    D19_CODE_SANDBOX_POD_STATUS_TOKEN_VOLUME,
]
D19_CODE_SANDBOX_ORCHESTRATOR_VOLUME_MOUNTS = [
    *minio_web_identity_volume_mounts(),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-pod-status-token",
        mount_path="/var/run/secrets/kubernetes.io/serviceaccount",
        read_only=True,
    ),
    k8s.V1VolumeMount(
        name="d19-code-sandbox-input",
        mount_path="/sandbox/input",
        read_only=False,
    ),
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
    k8s.V1VolumeMount(
        name="d19-code-sandbox-workspace",
        mount_path="/workspace",
        read_only=False,
    ),
]
D19_AGGREGATOR_EXECUTOR_CONFIG = minio_web_identity_executor_config(
    service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
    labels=D19_AGGREGATOR_WORKLOAD_LABELS,
)
D19_OFFICIAL_MEASUREMENT_PUBLISHER_EXECUTOR_CONFIG = bc21_authorized_executor_config(
    service_account_name=D19_OFFICIAL_MEASUREMENT_PUBLISHER_WORKLOAD_SERVICE_ACCOUNT,
    labels=D19_OFFICIAL_MEASUREMENT_PUBLISHER_WORKLOAD_LABELS,
)
D19_ATTESTOR_VOLUMES = [*D19_AGGREGATOR_VOLUMES, *vault_transit_volumes()]
D19_ATTESTOR_VOLUME_MOUNTS = [
    *D19_AGGREGATOR_VOLUME_MOUNTS,
    *vault_transit_volume_mounts(),
]
D19_CODE_SANDBOX_PUBLISHER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "ephemeral-storage": "1Gi", "memory": "1Gi"},
    limits={"cpu": "1000m", "ephemeral-storage": "3Gi", "memory": "3Gi"},
)
D19_ADMISSION_KUBERNETES_API_VOLUME = k8s.V1Volume(
    name="d19-admission-kubernetes-api",
    projected=k8s.V1ProjectedVolumeSource(
        sources=[
            k8s.V1VolumeProjection(
                service_account_token=k8s.V1ServiceAccountTokenProjection(
                    expiration_seconds=600,
                    path="token",
                )
            ),
            k8s.V1VolumeProjection(
                config_map=k8s.V1ConfigMapProjection(
                    items=[k8s.V1KeyToPath(key="ca.crt", path="ca.crt")],
                    name="kube-root-ca.crt",
                )
            ),
        ]
    ),
)
D19_ADMISSION_EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels=D19_ADMISSION_WORKLOAD_LABELS),
        spec=k8s.V1PodSpec(
            automount_service_account_token=False,
            containers=[
                k8s.V1Container(
                    name="base",
                    env=[
                        k8s.V1EnvVar(
                            name="ADAPSTORY_KUBERNETES_API_CA_FILE",
                            value="/var/run/secrets/adapstory/kubernetes-api/ca.crt",
                        ),
                        k8s.V1EnvVar(
                            name="ADAPSTORY_KUBERNETES_API_TOKEN_FILE",
                            value="/var/run/secrets/adapstory/kubernetes-api/token",
                        ),
                    ],
                    security_context=hardened_runtime_container_security_context(),
                    volume_mounts=[
                        k8s.V1VolumeMount(
                            name="d19-admission-kubernetes-api",
                            mount_path="/var/run/secrets/adapstory/kubernetes-api",
                            read_only=True,
                        ),
                        *hardened_runtime_volume_mounts(),
                    ],
                )
            ],
            security_context=hardened_runtime_pod_security_context(),
            service_account_name=D19_ADMISSION_WORKLOAD_SERVICE_ACCOUNT,
            volumes=[
                D19_ADMISSION_KUBERNETES_API_VOLUME,
                *hardened_runtime_volumes(),
            ],
        ),
    )
}
D19_RUNTIME_ADMISSION_EXECUTOR_CONFIG = evaluation_admission_verifier_executor_config(
    labels=D19_RUNTIME_ADMISSION_WORKLOAD_LABELS,
)


def d19_aggregator_env_vars() -> list[k8s.V1EnvVar]:
    """Return the minimal STS-only runtime contract for trusted aggregation."""

    return [
        *minio_web_identity_env_vars(_D19_AGGREGATOR_ENV_NAMES),
        *_pod_identity_env_vars(),
    ]


def _pod_identity_env_vars() -> list[k8s.V1EnvVar]:
    return [
        k8s.V1EnvVar(
            name=name,
            value_from=k8s.V1EnvVarSource(
                field_ref=k8s.V1ObjectFieldSelector(field_path=field_path)
            ),
        )
        for name, field_path in (
            ("POD_NAME", "metadata.name"),
            ("POD_NAMESPACE", "metadata.namespace"),
            ("POD_UID", "metadata.uid"),
        )
    ]


def d19_attestor_env_vars() -> list[k8s.V1EnvVar]:
    """Expose Vault only to the two runtime signing/verification pods."""

    return [
        *d19_aggregator_env_vars(),
        *vault_transit_env_vars(auth_role="serp-evaluation-runtime-attestor-role"),
    ]


def d19_model_runner_env_vars() -> list[k8s.V1EnvVar]:
    """Expose only MinIO STS plus the governed BC-10 model gateway."""

    return [
        *minio_web_identity_env_vars(_D19_MODEL_RUNNER_ENV_NAMES),
        *bc10_workload_env_vars(),
        *_pod_identity_env_vars(),
    ]


def d19_code_sandbox_publisher_env_vars() -> list[k8s.V1EnvVar]:
    """Give only the trusted publisher its WORM identity and Pod-status identity."""

    return [
        *d19_aggregator_env_vars(),
        *_pod_identity_env_vars(),
    ]


def d19_builder_env_vars() -> list[k8s.V1EnvVar]:
    """Expose isolated-artifact indexing through BC-10 and BC-21 only."""

    def cpu_limit_env_var(name: str) -> k8s.V1EnvVar:
        return k8s.V1EnvVar(
            name=name,
            value_from=k8s.V1EnvVarSource(
                resource_field_ref=k8s.V1ResourceFieldSelector(
                    resource="limits.cpu",
                    divisor="1",
                )
            ),
        )

    return [
        *minio_web_identity_env_vars(_D19_BUILDER_ENV_NAMES),
        *bc10_workload_env_vars(),
        *bc21_workload_env_vars(),
        cpu_limit_env_var("MKL_NUM_THREADS"),
        cpu_limit_env_var("NUMEXPR_NUM_THREADS"),
        cpu_limit_env_var("OMP_NUM_THREADS"),
        cpu_limit_env_var("OPENBLAS_NUM_THREADS"),
        k8s.V1EnvVar(name="TOKENIZERS_PARALLELISM", value="false"),
        k8s.V1EnvVar(
            name="ADAPSTORY_SERP_LOCAL_WORK_DIR",
            value=D19_PACK_BUILDER_SCRATCH_PATH,
        ),
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
    """Return the immutable trusted staging and publishing runtime image."""

    repository = conf.get("kubernetes_executor", "worker_container_repository").strip()
    digest = os.environ.get("ADAPSTORY_AIRFLOW_RUNTIME_IMAGE_DIGEST", "").strip()
    normalized = digest.removeprefix("sha256:")
    if (
        not repository
        or len(normalized) != 64
        or any(character not in "0123456789abcdef" for character in normalized)
    ):
        raise ValueError("D19 code sandbox requires an immutable runtime image digest")
    return f"{repository}@sha256:{normalized}"


def build_code_sandbox_mapped_operator_kwargs(
    prepared_set: Mapping[str, Any],
    *,
    expected_suite_id: str,
    expected_side: str,
    expected_repetition: int,
    trusted_runtime_image: str,
) -> list[dict[str, Any]]:
    """Build one mapped pod per sealed sandbox work item.

    The trusted base container stages, observes and publishes. The concurrent
    executor image is selected from the immutable work-item inventory and
    receives no environment, projected token, Docker socket, or XCom mount.
    """

    if not _IMAGE_REFERENCE.fullmatch(trusted_runtime_image):
        raise ValueError("D19 trusted sandbox I/O image must be immutable")
    work_items, _work_item_set_evidence = _validated_sandbox_work_item_set(
        prepared_set,
        expected_suite_id=expected_suite_id,
        expected_side=expected_side,
        expected_repetition=expected_repetition,
    )
    mapped_kwargs: list[dict[str, Any]] = []
    for map_index, work_item in enumerate(work_items):
        evidence = work_item["sandboxWorkItemEvidence"]
        suite_slug = _suite_slug(expected_suite_id)
        mapped_kwargs.append(
            {
                "name": (
                    f"serp-d19-code-sandbox-{suite_slug}-{expected_side}-"
                    f"{expected_repetition}-{map_index + 1:04d}"
                ),
                "arguments": [
                    "orchestrate-code-sandbox",
                    "--sandbox-work-item",
                    evidence["s3Uri"],
                    "--sandbox-work-item-version-id",
                    evidence["versionId"],
                    "--sandbox-work-item-sha256",
                    evidence["sha256"],
                    "--result",
                    "/sandbox/output/raw-result.json",
                    "--input-dir",
                    "/sandbox/input",
                    "--xcom-output",
                    "/airflow/xcom/return.json",
                ],
                "pod_template_dict": _sandbox_pod_template_dict(
                    work_item=work_item,
                    suite_id=expected_suite_id,
                ),
            }
        )
    return mapped_kwargs


def _validated_sandbox_work_item_set(
    prepared_set: Mapping[str, Any],
    *,
    expected_suite_id: str,
    expected_side: str,
    expected_repetition: int,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    if expected_suite_id not in D19_CODE_SANDBOX_SUITES:
        raise ValueError("D19 sandbox suite is unsupported")
    if expected_side not in {"baseline", "candidate"}:
        raise ValueError("D19 sandbox side is unsupported")
    if expected_repetition not in range(1, 6):
        raise ValueError("D19 sandbox repetition is unsupported")
    if set(prepared_set) != {
        "repetition",
        "schema",
        "side",
        "suiteId",
        "workItemSetEvidence",
        "workItems",
    }:
        raise ValueError("D19 sandbox work-item set fields are unsupported")
    if prepared_set.get("schema") != SANDBOX_WORK_ITEM_SET_SCHEMA:
        raise ValueError("D19 sandbox work-item set schema is unsupported")
    identity = (
        prepared_set.get("suiteId"),
        prepared_set.get("side"),
        prepared_set.get("repetition"),
    )
    if identity != (expected_suite_id, expected_side, expected_repetition):
        raise ValueError("D19 sandbox work-item set identity is mismatched")
    work_item_set_evidence = _assembly_artifact_evidence(
        prepared_set.get("workItemSetEvidence"), "workItemSetEvidence"
    )
    raw_work_items = prepared_set.get("workItems")
    if not isinstance(raw_work_items, list) or not raw_work_items:
        raise ValueError("D19 sandbox work-item inventory is required")
    if expected_suite_id == "CodeRAG-Bench" and len(raw_work_items) != 1:
        raise ValueError("D19 DS-1000 requires one suite-specific sandbox work item")

    normalized: list[dict[str, Any]] = []
    expected_fields = {
        "caseIdSha256",
        "executorArgs",
        "executorCommand",
        "sandboxImageDigest",
        "sandboxImageReference",
        "sandboxWorkItemEvidence",
    }
    if expected_suite_id == "SWE-bench Verified":
        expected_fields |= {"baseCommit", "repository"}
    for raw_work_item in raw_work_items:
        if not isinstance(raw_work_item, Mapping) or set(raw_work_item) != expected_fields:
            raise ValueError("D19 sandbox work-item fields are unsupported")
        case_id_sha256 = _required_sha256(raw_work_item, "caseIdSha256")
        image_digest = _required_sha256(raw_work_item, "sandboxImageDigest")
        expected_command, expected_args = D19_CODE_SANDBOX_EXECUTOR_SPEC[expected_suite_id]
        if raw_work_item.get("executorCommand") != expected_command:
            raise ValueError("D19 sandbox executor command is unsupported")
        if raw_work_item.get("executorArgs") != list(expected_args):
            raise ValueError("D19 sandbox executor arguments are unsupported")
        image_reference = raw_work_item.get("sandboxImageReference")
        if not isinstance(image_reference, str) or not _IMAGE_REFERENCE.fullmatch(image_reference):
            raise ValueError("D19 sandbox image reference must use an immutable digest")
        if not image_reference.endswith("@" + image_digest):
            raise ValueError("D19 sandbox image reference and digest are mismatched")
        item: dict[str, Any] = {
            "caseIdSha256": case_id_sha256,
            "executorArgs": list(expected_args),
            "executorCommand": expected_command,
            "sandboxImageDigest": image_digest,
            "sandboxImageReference": image_reference,
            "sandboxWorkItemEvidence": _assembly_artifact_evidence(
                raw_work_item.get("sandboxWorkItemEvidence"), "sandboxWorkItemEvidence"
            ),
        }
        if expected_suite_id == "SWE-bench Verified":
            repository = raw_work_item.get("repository")
            base_commit = raw_work_item.get("baseCommit")
            if not isinstance(repository, str) or not _REPOSITORY.fullmatch(repository):
                raise ValueError("D19 SWE-bench repository is invalid")
            if not isinstance(base_commit, str) or not _GIT_REVISION.fullmatch(base_commit):
                raise ValueError("D19 SWE-bench base commit is invalid")
            item.update({"baseCommit": base_commit, "repository": repository})
        normalized.append(item)
    identities = [item["caseIdSha256"] for item in normalized]
    if identities != sorted(identities) or len(set(identities)) != len(identities):
        raise ValueError("D19 sandbox work items must use unique canonical case order")
    return normalized, work_item_set_evidence


def _required_sha256(value: Mapping[str, Any], field_name: str) -> str:
    nested = value.get(field_name)
    if not isinstance(nested, str) or not _SHA256.fullmatch(nested):
        raise ValueError(f"{field_name} must be sha256:<64 lowercase hex>")
    return nested


def _serialized_d19_aggregator_env() -> list[dict[str, str]]:
    env: list[dict[str, str]] = []
    for name in _D19_AGGREGATOR_ENV_NAMES:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"evidence workload environment is required: {name}")
        env.append({"name": name, "value": value.strip()})
    env.extend(
        (
            {
                "name": "ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE",
                "value": MINIO_WEB_IDENTITY_TOKEN_FILE,
            },
            {
                "name": "ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS",
                "value": str(MINIO_WEB_IDENTITY_EXPIRATION_SECONDS),
            },
        )
    )
    return env


def _sandbox_pod_template_dict(
    *,
    work_item: Mapping[str, Any],
    suite_id: str,
) -> dict[str, Any]:
    container_security_context = {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
        "runAsGroup": SERP_RUNTIME_GROUP_ID,
        "runAsNonRoot": True,
        "runAsUser": SERP_RUNTIME_USER_ID,
    }
    volume_mounts = [
        {"mountPath": "/sandbox/input", "name": "d19-code-sandbox-input", "readOnly": True},
        {"mountPath": "/sandbox/output", "name": "d19-code-sandbox-output"},
        {"mountPath": "/tmp", "name": "d19-code-sandbox-tmp"},
        {"mountPath": "/workspace", "name": "d19-code-sandbox-workspace"},
    ]
    command = str(work_item["executorCommand"])
    arguments = [str(value) for value in work_item["executorArgs"]]
    if (command, arguments) == (
        "/opt/ds1000-venv/bin/python",
        ["/sandbox/input/ds1000_executor.py"],
    ):
        invocation = "/opt/ds1000-venv/bin/python /sandbox/input/ds1000_executor.py"
    elif (command, arguments) == ("/bin/bash", ["/sandbox/input/swe_executor.sh"]):
        invocation = "/bin/bash /sandbox/input/swe_executor.sh"
    else:  # validated by the caller; keep this boundary independently fail-closed
        raise ValueError("D19 sandbox executor wrapper command is unsupported")
    executor_wrapper = f"""set -eu
while test ! -f /sandbox/input/.ready; do
  if test -f /sandbox/input/.abort; then exit 0; fi
  sleep 1
done
set +e
{invocation}
status=$?
set -e
printf '%s\n' "$status" > /sandbox/output/executor-exit-code
exit 0
"""
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "annotations": {
                "adapstory.com/sandbox-case-sha256": str(work_item["caseIdSha256"]),
                "adapstory.com/sandbox-image-digest": str(work_item["sandboxImageDigest"]),
            }
        },
        "spec": {
            "automountServiceAccountToken": False,
            "containers": [
                {
                    "name": "base",
                },
                {
                    "args": [executor_wrapper],
                    "command": ["/bin/sh", "-c"],
                    "env": [],
                    "envFrom": [],
                    "image": work_item["sandboxImageReference"],
                    "name": "sandbox-executor",
                    "resources": {
                        "limits": {
                            **D19_OFFICIAL_HARNESS_LIMITS[suite_id],
                            "ephemeral-storage": "36Gi",
                        },
                        "requests": {
                            "cpu": "1000m",
                            "ephemeral-storage": "8Gi",
                            "memory": "2Gi",
                        },
                    },
                    "securityContext": dict(container_security_context),
                    "volumeMounts": volume_mounts,
                },
            ],
            "securityContext": {
                "fsGroup": SERP_RUNTIME_GROUP_ID,
                "runAsGroup": SERP_RUNTIME_GROUP_ID,
                "runAsNonRoot": True,
                "runAsUser": SERP_RUNTIME_USER_ID,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            "shareProcessNamespace": False,
        },
    }


def _suite_slug(suite_id: str) -> str:
    return suite_id.casefold().replace(" ", "-").replace("_", "-")


def validate_benchmark_improvement_wave_plan(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    plan_json = write_airflow_plan_artifact(build_benchmark_improvement_wave_plan(conf))
    plan = json.loads(plan_json)
    if not isinstance(plan, dict):
        raise ValueError("D19 Airflow plan must be an object")
    return plan


def verify_runtime_terminal_activation_admission(**context: Any) -> dict[str, Any]:
    """Fail before a D19 fence if D17 terminal provenance cannot be reverified."""

    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    plan_json = build_benchmark_improvement_wave_plan(conf).to_canonical_json()
    return verify_model_catalog_promotion_terminal_activation(plan_json)


def validate_d19_fence_admission(**context: Any) -> dict[str, Any]:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise ValueError("D19 fence admission requires authoritative DagRun metadata")
    run_type = getattr(dag_run, "run_type", None)
    run_type_value = getattr(run_type, "value", run_type)
    logical_date = context.get("logical_date") or getattr(dag_run, "logical_date", None)
    if not isinstance(logical_date, datetime) or logical_date.tzinfo is None:
        raise ValueError("D19 fence admission logical_date must be timezone-aware")
    return admit_d19_run(
        dag_run_conf=getattr(dag_run, "conf", None) or {},
        airflow_run={
            "dagId": D19_DAG_ID,
            "logicalDate": logical_date.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "runId": str(getattr(dag_run, "run_id", "")),
            "runType": str(run_type_value),
        },
    )


def load_materialized_benchmark_catalog(plan_json: str) -> dict[str, Any]:
    return load_materialized_benchmark_catalog_snapshot(plan_json)


def choose_d19_catalog_readiness_transition(
    catalog_snapshot: Mapping[str, Any],
) -> str | list[str]:
    status = catalog_snapshot.get("catalogStatus")
    raw_blocking_suite_ids = catalog_snapshot.get("blockingSuiteIds")
    if not isinstance(raw_blocking_suite_ids, list) or any(
        not isinstance(suite_id, str) or not suite_id.strip() for suite_id in raw_blocking_suite_ids
    ):
        raise ValueError("D19 benchmark catalog blockingSuiteIds are invalid")
    blocking_suite_ids = list(dict.fromkeys(raw_blocking_suite_ids))
    if status == "blocked":
        if not blocking_suite_ids:
            raise ValueError("D19 blocked benchmark catalog has no blocking suites")
        return D19_CATALOG_BLOCKER_TASK_ID
    if status == "ready":
        if blocking_suite_ids:
            raise ValueError("D19 ready benchmark catalog contains blocking suites")
        return D19_PACK_CAS_CLASSIFIER_TASK_ID
    raise ValueError("D19 benchmark catalog status is unsupported")


def fail_d19_blocked_catalog(catalog_snapshot: Mapping[str, Any]) -> None:
    transition = choose_d19_catalog_readiness_transition(catalog_snapshot)
    if transition != D19_CATALOG_BLOCKER_TASK_ID:
        raise ValueError("D19 catalog blocker requires a blocked benchmark catalog")
    blocking_suite_ids = catalog_snapshot["blockingSuiteIds"]
    raise RuntimeError(
        "D19 benchmark catalog blocked preflight: "
        + ", ".join(str(suite_id) for suite_id in blocking_suite_ids)
    )


def choose_d19_pack_cas_transition(classification: Mapping[str, Any]) -> str | list[str]:
    classifications = _validated_d19_pack_cas_classifications(classification)
    if any(
        item["classification"] in {"INCOMPATIBLE", "PARTIAL/CORRUPT"} for item in classifications
    ):
        return D19_PACK_CAS_RECOVERY_PLAN_TASK_ID
    return [task.task_id for task in D19_PACK_SIDE_BUILD_TASKS.values()]


def _validated_d19_pack_cas_classifications(
    classification: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    if classification.get("schema") != "BC21PackCasClassification/v1":
        raise ValueError("D19 pack CAS classification schema is unsupported")
    raw = classification.get("classifications")
    if classification.get("sideCount") != 18 or not isinstance(raw, list) or len(raw) != 18:
        raise ValueError("D19 pack CAS classification must cover exactly eighteen sides")
    expected = [
        (suite_id, side)
        for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
        for side in ("baseline", "candidate")
    ]
    observed: list[tuple[str, str]] = []
    admitted = {"COMPATIBLE", "MISS", "LEGACY_RESEALABLE"}
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("D19 pack CAS side classification must be an object")
        identity = (str(item.get("suiteId", "")), str(item.get("side", "")))
        observed.append(identity)
        state = item.get("classification")
        if state not in admitted | {"INCOMPATIBLE", "PARTIAL/CORRUPT"}:
            raise ValueError("D19 pack CAS side classification state is unsupported")
    if observed != expected:
        raise ValueError("D19 pack CAS side classification order is noncanonical")
    return list(raw)


def plan_d19_pack_cas_migration_rebuild(
    classification: Mapping[str, Any],
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    for item in _validated_d19_pack_cas_classifications(classification):
        state = item["classification"]
        if state not in {"INCOMPATIBLE", "PARTIAL/CORRUPT"}:
            continue
        reason = item.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("D19 deterministic pack CAS failure requires a reason")
        subtype = item.get("subtype")
        if not isinstance(subtype, str) or not subtype.strip():
            raise ValueError("D19 deterministic pack CAS failure requires a safe subtype")
        actions.append(
            {
                "suiteId": item["suiteId"],
                "side": item["side"],
                "classification": state,
                "subtype": subtype,
                "action": (
                    "MIGRATE_IDENTITY_OR_SCHEMA"
                    if state == "INCOMPATIBLE"
                    else "REBUILD_FROM_LAST_VALID_CAS_CHECKPOINT"
                ),
                "reason": reason,
            }
        )
    if not actions:
        raise ValueError("D19 pack CAS recovery plan requires a deterministic failure")
    return {
        "schema": "D19PackCasRecoveryPlan/v1",
        "retryable": False,
        "sideCount": len(actions),
        "actions": actions,
    }


def fail_d19_pack_cas_recovery(recovery_plan: Mapping[str, Any]) -> None:
    if (
        recovery_plan.get("schema") != "D19PackCasRecoveryPlan/v1"
        or recovery_plan.get("retryable") is not False
    ):
        raise ValueError("D19 pack CAS recovery plan is unsupported")
    actions = recovery_plan.get("actions")
    if (
        not isinstance(actions, list)
        or recovery_plan.get("sideCount") != len(actions)
        or not actions
    ):
        raise ValueError("D19 pack CAS recovery plan actions are invalid")
    summary = ", ".join(f"{item['suiteId']}/{item['side']}={item['action']}" for item in actions)
    raise RuntimeError("D19 deterministic pack CAS recovery is required without retry: " + summary)


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


def write_code_sandbox_result_set_assembly_plan(
    plan_json: Mapping[str, Any] | str,
    prepared_set: Mapping[str, Any],
    sandbox_results: Sequence[Mapping[str, Any]],
    *,
    expected_suite_id: str,
    expected_side: str,
    expected_repetition: int,
) -> dict[str, Any]:
    """Seal every mapped sandbox result before one trusted suite receipt."""

    plan = dict(plan_json) if isinstance(plan_json, Mapping) else json.loads(plan_json)
    if not isinstance(plan, Mapping) or plan.get("dag_id") != "serp_benchmark_improvement_wave":
        raise ValueError("D19 sandbox result-set plan DAG identity does not match")
    work_items, work_item_set_evidence = _validated_sandbox_work_item_set(
        prepared_set,
        expected_suite_id=expected_suite_id,
        expected_side=expected_side,
        expected_repetition=expected_repetition,
    )
    if len(sandbox_results) != len(work_items):
        raise ValueError("D19 sandbox results must cover every sealed work item")

    artifact_paths = plan.get("artifact_paths")
    if not isinstance(artifact_paths, Mapping):
        raise ValueError("D19 sandbox result-set plan artifact_paths are required")
    paired_assembly_path = artifact_paths.get("paired_evaluation_assembly_plan")
    if not isinstance(paired_assembly_path, str) or not paired_assembly_path.startswith("s3://"):
        raise ValueError("D19 paired evaluation assembly path must use s3://")
    operation_root = paired_assembly_path.rsplit("/", 1)[0]
    operation_id = plan.get("operation_id")
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("D19 operation_id is required")
    _require_operation_evidence(work_item_set_evidence, operation_root, "workItemSetEvidence")

    normalized_results: list[dict[str, Any]] = []
    for work_item, raw_result in zip(work_items, sandbox_results, strict=True):
        if not isinstance(raw_result, Mapping) or set(raw_result) != {
            "caseIdSha256",
            "sandboxResultEvidence",
        }:
            raise ValueError("D19 sandbox result fields are unsupported")
        case_id_sha256 = _required_sha256(raw_result, "caseIdSha256")
        if case_id_sha256 != work_item["caseIdSha256"]:
            raise ValueError("D19 sandbox result order or case identity is mismatched")
        work_item_evidence = work_item["sandboxWorkItemEvidence"]
        result_evidence = _assembly_artifact_evidence(
            raw_result.get("sandboxResultEvidence"), "sandboxResultEvidence"
        )
        _require_operation_evidence(work_item_evidence, operation_root, "sandboxWorkItemEvidence")
        _require_operation_evidence(result_evidence, operation_root, "sandboxResultEvidence")
        normalized_results.append(
            {
                "caseIdSha256": case_id_sha256,
                "sandboxResultEvidence": result_evidence,
                "sandboxWorkItemEvidence": work_item_evidence,
            }
        )
    output_path = (
        f"{operation_root}/code-sandbox/{_suite_slug(expected_suite_id)}/"
        f"{expected_side}/{expected_repetition:02d}/sandbox-result-set-assembly-plan.json"
    )
    payload: dict[str, Any] = {
        "repetition": expected_repetition,
        "results": normalized_results,
        "schema": SANDBOX_RESULT_SET_ASSEMBLY_PLAN_SCHEMA,
        "side": expected_side,
        "suiteId": expected_suite_id,
        "workItemSetEvidence": work_item_set_evidence,
    }
    written = write_immutable_evidence_snapshot(
        output_path,
        artifact_type="sandbox_result_set_assembly_plan",
        operation_id=operation_id,
        payload=payload,
    )
    return {
        "resultCount": len(normalized_results),
        "resultSetPlanEvidence": _assembly_artifact_evidence(written, "resultSetPlanEvidence"),
        "repetition": expected_repetition,
        "side": expected_side,
        "suiteId": expected_suite_id,
    }


def _require_operation_evidence(
    evidence: Mapping[str, str], operation_root: str, field_name: str
) -> None:
    if not evidence["s3Uri"].startswith(operation_root + "/"):
        raise ValueError(f"{field_name} must stay inside the D19 evidence operation")


def _d19_canary_qualification(
    catalog: Mapping[str, Any],
    pack_lifecycle: Mapping[str, Any],
    work_item_plan: Mapping[str, Any],
) -> dict[str, int]:
    suites = catalog.get("suiteSummary")
    bindings = pack_lifecycle.get("packMaterialBindings")
    if not isinstance(suites, list) or len(suites) != 9:
        raise ValueError("D19 canary must qualify all nine suites")
    if not isinstance(bindings, list) or len(bindings) != 9:
        raise ValueError("D19 canary pack lifecycle must bind all nine suites")
    pack_count = sum(
        int(isinstance(binding, Mapping) and isinstance(binding.get("baseline"), Mapping))
        + int(isinstance(binding, Mapping) and isinstance(binding.get("candidate"), Mapping))
        for binding in bindings
    )
    if pack_count != 18 or work_item_plan.get("runCount") != 90:
        raise ValueError("D19 canary qualification is incomplete")
    return {"suiteCount": 9, "packCount": 18, "workItemCount": 90}


def write_d19_release_terminal_outbox(
    *,
    dag_run_conf: Mapping[str, Any],
    current_run_id: str,
    verification_result: Mapping[str, Any],
    catalog: Mapping[str, Any],
    pack_lifecycle: Mapping[str, Any],
    work_item_plan: Mapping[str, Any],
) -> dict[str, Any]:
    ownership = dag_run_conf.get("d19_release_ownership")
    if ownership is None:
        return {
            "managed": False,
            "terminal": True,
            "nextRun": None,
            "nextRunConf": None,
        }
    if not isinstance(ownership, Mapping):
        raise ValueError("D19 release ownership must be an object")
    runs = ownership.get("runs")
    if not isinstance(runs, list):
        raise ValueError("D19 release ownership runs are required")
    current = next(
        (run for run in runs if isinstance(run, Mapping) and run.get("runId") == current_run_id),
        None,
    )
    if not isinstance(current, Mapping):
        raise ValueError("current D19 run is outside release ownership")
    qualification = (
        _d19_canary_qualification(catalog, pack_lifecycle, work_item_plan)
        if current.get("role") == "canary"
        else None
    )
    artifact_root_path = dag_run_conf.get("artifact_root_path")
    if not isinstance(artifact_root_path, str):
        raise ValueError("D19 release artifact root is required")

    def write_outbox(path: str, payload: dict[str, Any]) -> dict[str, str]:
        written = write_immutable_evidence_snapshot(
            path,
            artifact_type="d19_terminal_outbox",
            operation_id=f"d19-release-{ownership['correlationId']}",
            payload=payload,
        )
        digest = written["artifactSha256"]
        return {
            "artifactPath": written["artifactPath"],
            "artifactVersionId": written["artifactVersionId"],
            "artifactSha256": digest if digest.startswith("sha256:") else "sha256:" + digest,
            "objectLockMode": written["objectLockMode"],
            "retainUntil": written["retainUntil"],
        }

    result = build_terminal_outbox(
        ownership=ownership,
        current_run_id=current_run_id,
        verification_pointer=verification_result,
        canary_qualification=qualification,
        observed_at=datetime.now(UTC),
        artifact_root_path=artifact_root_path,
        writer=write_outbox,
    )
    result["managed"] = True
    result["handoffUri"] = artifact_root_path.rstrip("/") + "/d19-release-handoff.json"
    next_run = result["nextRun"]
    result["nextRunConf"] = None
    if isinstance(next_run, Mapping):
        result["nextRunConf"] = {
            **dict(dag_run_conf),
            "generated_at": next_run["logicalDate"],
        }
    return result


def choose_d19_release_transition(outbox: Mapping[str, Any]) -> str:
    return (
        D19_RELEASE_COMPLETE_TASK_ID
        if outbox.get("terminal") is True
        else D19_TRIGGER_NEXT_RUN_TASK_ID
    )


def wake_d19_terminal_finalizer(context: Mapping[str, Any]) -> None:
    """Use the callback only as a wake-up signal after the WORM outbox exists."""

    task_instance = context.get("task_instance")
    if task_instance is None:
        raise RuntimeError("D19 finalizer callback task instance is unavailable")
    outbox = task_instance.xcom_pull(task_ids=D19_TERMINAL_OUTBOX_TASK_ID)
    if (
        not isinstance(outbox, Mapping)
        or outbox.get("managed") is False
        or outbox.get("terminal") is not True
    ):
        return
    callback_url = os.environ.get("ADAPSTORY_SERP_D19_FINALIZER_CALLBACK_URL", "")
    callback_token = os.environ.get("ADAPSTORY_SERP_D19_FINALIZER_CALLBACK_TOKEN", "")
    parsed_callback_url = urlsplit(callback_url)
    if (
        parsed_callback_url.scheme != "https"
        or not parsed_callback_url.netloc
        or parsed_callback_url.fragment
        or not callback_token
    ):
        raise RuntimeError("D19 finalizer callback authority is unavailable")
    event = outbox.get("event")
    if not isinstance(event, Mapping):
        raise RuntimeError("D19 terminal callback event is unavailable")
    body = json.dumps(
        {
            "correlationId": event["correlationId"],
            "handoffUri": outbox["handoffUri"],
            "outboxEvidenceJson": json.dumps(
                outbox["outboxEvidence"], separators=(",", ":"), sort_keys=True
            ),
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    with urlopen(
        Request(
            callback_url,
            data=body,
            headers={
                "Authorization": f"Bearer {callback_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        ),
        timeout=10,
    ) as response:
        if response.status not in {200, 201, 202}:
            raise RuntimeError("D19 finalizer callback was not accepted")


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
    execution_purpose = plan.get("execution_purpose", "measurement")
    if execution_purpose not in {"measurement", "critical-path-preflight"}:
        raise ValueError("D19 assembly plan execution purpose is unsupported")
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
        normalized_run = {
            "workItemEvidence": _assembly_artifact_evidence(
                work_item.get("workItemEvidence"), "workItemEvidence"
            )
        }
        if execution_purpose == "measurement":
            normalized_run["receiptEvidence"] = _assembly_artifact_evidence(
                result.get("receiptEvidence"), "receiptEvidence"
            )
        else:
            normalized_run["preflightResultEvidence"] = _assembly_artifact_evidence(
                result.get("preflightResultEvidence"), "preflightResultEvidence"
            )
            normalized_run["telemetryEvidence"] = _assembly_artifact_evidence(
                result.get("telemetryEvidence"), "telemetryEvidence"
            )
        runs.append(normalized_run)
    artifact_paths = plan.get("artifact_paths")
    if not isinstance(artifact_paths, Mapping):
        raise ValueError("D19 plan artifact_paths is required")
    assembly_path_key = (
        "paired_evaluation_assembly_plan"
        if execution_purpose == "measurement"
        else "preflight_assembly_plan"
    )
    assembly_path = artifact_paths.get(assembly_path_key)
    manifest_output_key = (
        "paired_execution_manifest"
        if execution_purpose == "measurement"
        else "preflight_execution_manifest"
    )
    manifest_output = artifact_paths.get(manifest_output_key)
    if not isinstance(assembly_path, str) or not assembly_path.startswith("s3://"):
        raise ValueError("D19 paired_evaluation_assembly_plan path must use s3://")
    if not isinstance(manifest_output, str) or not manifest_output.startswith("s3://"):
        raise ValueError("D19 paired_execution_manifest path must use s3://")
    payload: dict[str, Any] = {
        "schema": (
            "PairedEvaluationAssemblyPlan/v1"
            if execution_purpose == "measurement"
            else "SerpCriticalPathPreflightAssemblyPlan/v1"
        ),
        "requestEvidence": request_evidence,
        "manifestOutput": manifest_output,
        "runs": runs,
    }
    if execution_purpose == "critical-path-preflight":
        dry_run_output = artifact_paths.get("paired_evaluator_dry_run")
        progress_snapshot_output = artifact_paths.get("preflight_progress_snapshot")
        preflight_receipt_output = artifact_paths.get("critical_path_preflight_receipt")
        if not isinstance(dry_run_output, str) or not dry_run_output.startswith("s3://"):
            raise ValueError("D19 paired_evaluator_dry_run path must use s3://")
        if not isinstance(progress_snapshot_output, str) or not progress_snapshot_output.startswith(
            "s3://"
        ):
            raise ValueError("D19 preflight_progress_snapshot path must use s3://")
        if not isinstance(preflight_receipt_output, str) or not preflight_receipt_output.startswith(
            "s3://"
        ):
            raise ValueError("D19 critical_path_preflight_receipt path must use s3://")
        payload["pairedEvaluatorDryRunOutput"] = dry_run_output
        payload["progressSnapshotOutput"] = progress_snapshot_output
        payload["preflightReceiptOutput"] = preflight_receipt_output
        payload["sourceSha"] = plan.get("source_sha")
        for source_field, target_field in (
            ("adapter_parity_evidence", "adapterParityEvidence"),
            ("bc10_route_set_evidence", "bc10RouteSetEvidence"),
            ("fault_injection_evidence", "faultInjectionEvidence"),
            ("frozen_benchmark_release_evidence", "frozenBenchmarkReleaseEvidence"),
        ):
            payload[target_field] = _assembly_artifact_evidence(
                plan.get(source_field), source_field
            )
    written = write_immutable_evidence_snapshot(
        assembly_path,
        artifact_type=(
            "paired_evaluation_assembly_plan"
            if execution_purpose == "measurement"
            else "critical_path_preflight_assembly_plan"
        ),
        operation_id=str(plan.get("operation_id", "")),
        payload=payload,
    )
    pointer_key = (
        "assemblyPlanEvidence"
        if execution_purpose == "measurement"
        else "preflightAssemblyPlanEvidence"
    )
    return {
        pointer_key: _assembly_artifact_evidence(written, pointer_key),
        "executionPurpose": execution_purpose,
        "manifestOutput": manifest_output,
        "runCount": len(runs),
    }


def choose_post_assembly_path(plan_json: Mapping[str, Any] | str) -> str:
    plan = dict(plan_json) if isinstance(plan_json, Mapping) else json.loads(plan_json)
    if not isinstance(plan, Mapping):
        raise ValueError("D19 post-assembly plan must be an object")
    purpose = plan.get("execution_purpose", "measurement")
    if purpose == "measurement":
        return "run_paired_benchmark_evaluation"
    if purpose == "critical-path-preflight":
        return "persist_critical_path_preflight_evidence"
    raise ValueError("D19 post-assembly execution purpose is unsupported")


def persist_critical_path_preflight_evidence(
    assembly_result: Mapping[str, Any],
) -> dict[str, Any]:
    if assembly_result.get("status") != "ready" or assembly_result.get("runCount") != 90:
        raise ValueError("critical path preflight assembly is incomplete")
    evidence = _assembly_artifact_evidence(
        assembly_result.get("criticalPathPreflightEvidence"),
        "criticalPathPreflightEvidence",
    )
    return {
        "criticalPathPreflightEvidence": evidence,
        "measurementEligible": False,
        "runCount": 90,
        "schema": "SerpCriticalPathPreflightHandoff/v1",
        "status": "ready",
    }


def _assembly_artifact_evidence(value: object, field_name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an immutable artifact handle")
    canonical_shape = {"objectLockMode", "retainUntil", "s3Uri", "sha256", "versionId"}
    writer_shape = {
        "artifactETag",
        "artifactPath",
        "artifactSha256",
        "artifactType",
        "artifactVersionId",
        "contractVersion",
        "objectLockMode",
        "operationId",
        "retainUntil",
        "retentionDays",
        "status",
    }
    if set(value) == canonical_shape:
        path = value.get("s3Uri")
        version_id = value.get("versionId")
        digest = value.get("sha256")
        retain_until = value.get("retainUntil")
        if not isinstance(retain_until, str) or not retain_until.endswith("Z"):
            raise ValueError(f"{field_name} retainUntil must be RFC3339 UTC")
    elif set(value) == writer_shape:
        path = value.get("artifactPath")
        version_id = value.get("artifactVersionId")
        digest = value.get("artifactSha256")
        retain_until = value.get("retainUntil")
        if not isinstance(retain_until, str) or not retain_until.endswith("Z"):
            raise ValueError(f"{field_name} retainUntil must be RFC3339 UTC")
    else:
        raise ValueError(f"{field_name} immutable artifact fields are unsupported")
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
        "objectLockMode": "COMPLIANCE",
        "retainUntil": retain_until,
        "s3Uri": path,
        "sha256": normalized_digest,
        "versionId": version_id,
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

validate_admission = PythonOperator(
    task_id="validate_d19_fence_admission",
    python_callable=validate_d19_fence_admission,
    executor_config=D19_ADMISSION_EXECUTOR_CONFIG,
    dag=dag,
)

verify_terminal_activation = PythonOperator(
    task_id="verify_runtime_terminal_activation_admission",
    python_callable=verify_runtime_terminal_activation_admission,
    executor_config=D19_RUNTIME_ADMISSION_EXECUTOR_CONFIG,
    dag=dag,
)

validate_plan = PythonOperator(
    task_id="validate_benchmark_improvement_wave_plan",
    python_callable=validate_benchmark_improvement_wave_plan,
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

verify_bc10_ledger_capacity = PythonOperator(
    task_id="verify_bc10_ledger_capacity_admission",
    python_callable=verify_bc10_ledger_capacity_admission,
    executor_config=D19_ADMISSION_EXECUTOR_CONFIG,
    dag=dag,
)

materialize_catalog = BoundedKubernetesJobOperator(
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
    node_selector=BENCHMARK_CATALOG_ACQUISITION_NODE_SELECTOR,
    tolerations=BENCHMARK_CATALOG_ACQUISITION_TOLERATIONS,
    security_context=benchmark_catalog_acquisition_pod_security_context(),
    container_security_context=benchmark_catalog_acquisition_container_security_context(),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="delete_pod",
    on_finish_action="keep_pod",
    backoff_limit=0,
    ttl_seconds_after_finished=300,
    wait_until_job_complete=True,
    pod_discovery_timeout_seconds=DEFAULT_JOB_POD_DISCOVERY_TIMEOUT_SECONDS,
    pod_discovery_poll_interval_seconds=1,
    retries=1,
    retry_delay=timedelta(seconds=BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS),
    executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

load_catalog = PythonOperator(
    task_id="load_materialized_benchmark_catalog",
    python_callable=load_materialized_benchmark_catalog,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

catalog_readiness_gate = BranchPythonOperator(
    task_id=D19_CATALOG_READINESS_GATE_TASK_ID,
    python_callable=choose_d19_catalog_readiness_transition,
    op_args=["{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

catalog_blocker = PythonOperator(
    task_id=D19_CATALOG_BLOCKER_TASK_ID,
    python_callable=fail_d19_blocked_catalog,
    op_args=["{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog') }}"],
    retries=0,
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

classify_pack_cas = ReceiptKubernetesPodOperator(
    task_id=D19_PACK_CAS_CLASSIFIER_TASK_ID,
    name="serp-d19-classify-pack-cas",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=[
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.bc21_benchmark_pack_lifecycle_cli",
    ],
    arguments=[
        "classify-pack-cas",
        "--benchmark-catalog",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog')['artifactPath'] }}",
        "--benchmark-catalog-version-id",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog')['artifactVersionId'] }}",
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
        "--xcom-output",
        "/airflow/xcom/return.json",
    ],
    env_vars=d19_builder_env_vars(),
    service_account_name=D19_BUILDER_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_BUILDER_VOLUMES,
    volume_mounts=D19_BUILDER_VOLUME_MOUNTS,
    labels=D19_BUILDER_WORKLOAD_LABELS,
    container_resources=D19_PACK_BUILDER_RESOURCES,
    node_selector=D19_REMOTE_COMPUTE_NODE_SELECTOR,
    tolerations=D19_REMOTE_COMPUTE_TOLERATIONS,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    do_xcom_push=True,
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="delete_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

pack_cas_readiness_gate = BranchPythonOperator(
    task_id=D19_PACK_CAS_GATE_TASK_ID,
    python_callable=choose_d19_pack_cas_transition,
    op_args=["{{ ti.xcom_pull(task_ids='classify_pack_cas') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

plan_pack_cas_migration_rebuild_task = PythonOperator(
    task_id=D19_PACK_CAS_RECOVERY_PLAN_TASK_ID,
    python_callable=plan_d19_pack_cas_migration_rebuild,
    op_args=["{{ ti.xcom_pull(task_ids='classify_pack_cas') }}"],
    retries=0,
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

pack_cas_migration_rebuild_required = PythonOperator(
    task_id=D19_PACK_CAS_RECOVERY_REQUIRED_TASK_ID,
    python_callable=fail_d19_pack_cas_recovery,
    op_args=["{{ ti.xcom_pull(task_ids='plan_pack_cas_migration_rebuild') }}"],
    retries=0,
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

D19_PACK_SIDE_IDENTITIES = tuple(
    (suite_id, side)
    for suite_id in MANDATORY_SERP_BENCHMARK_SUITES
    for side in ("baseline", "candidate")
)
D19_PACK_SIDE_BUILD_TASKS: dict[tuple[str, str], BoundedKubernetesJobOperator] = {}
D19_DUAL_GPU_CRITICAL_PAIR = frozenset(
    {("SWE-bench Verified", "baseline"), ("CodeRAG-Bench", "candidate")}
)
for suite_id, side in D19_PACK_SIDE_IDENTITIES:
    suite_slug = suite_id.casefold().replace(" ", "_").replace("-", "_")
    artifact_slug = suite_id.casefold().replace(" ", "-").replace("_", "-")
    task_id = f"build_pack_side_{suite_slug}_{side}"
    D19_PACK_SIDE_BUILD_TASKS[(suite_id, side)] = BoundedKubernetesJobOperator(
        task_id=task_id,
        name=f"serp-d19-{task_id.replace('_', '-')}",
        namespace=conf.get("kubernetes_executor", "namespace"),
        image=current_airflow_runtime_image(),
        cmds=[
            "python",
            "-m",
            "adapstory_serp_pipeline.registry.bc21_benchmark_pack_lifecycle_cli",
        ],
        arguments=[
            "build-pack-side",
            "--suite",
            suite_id,
            "--side",
            side,
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
            "--shared-output-prefix",
            (
                "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
                "['artifact_paths']['benchmark_pack_build_result'] }}.shared/" + artifact_slug
            ),
            "--content-addressed-cache-prefix",
            "s3://airflow-serp-evidence/serp-evals/benchmark-cas/v4",
            "--result-output",
            (
                "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
                "['artifact_paths']['benchmark_pack_build_result'] }}.sides/"
                + artifact_slug
                + "-"
                + side
                + ".json"
            ),
        ],
        env_vars=d19_builder_env_vars(),
        service_account_name=D19_BUILDER_WORKLOAD_SERVICE_ACCOUNT,
        automount_service_account_token=False,
        volumes=(
            D19_SWE_BUILDER_VOLUMES if suite_id == "SWE-bench Verified" else D19_BUILDER_VOLUMES
        ),
        volume_mounts=D19_BUILDER_VOLUME_MOUNTS,
        labels=D19_BUILDER_WORKLOAD_LABELS,
        container_resources=(
            D19_SWE_PACK_BUILDER_RESOURCES
            if suite_id == "SWE-bench Verified"
            else D19_PACK_BUILDER_RESOURCES
        ),
        pool=D19_PACK_BUILDER_POOL,
        pool_slots=1,
        priority_weight=(1000 if (suite_id, side) in D19_DUAL_GPU_CRITICAL_PAIR else 1),
        weight_rule="absolute",
        node_selector=D19_REMOTE_COMPUTE_NODE_SELECTOR,
        tolerations=D19_REMOTE_COMPUTE_TOLERATIONS,
        security_context=hardened_runtime_pod_security_context(),
        container_security_context=hardened_runtime_container_security_context(),
        do_xcom_push=False,
        get_logs=True,
        log_events_on_failure=True,
        random_name_suffix=True,
        reattach_on_restart=True,
        on_kill_action="delete_pod",
        on_finish_action="delete_pod",
        backoff_limit=0,
        ttl_seconds_after_finished=300,
        wait_until_job_complete=True,
        pod_discovery_timeout_seconds=DEFAULT_JOB_POD_DISCOVERY_TIMEOUT_SECONDS,
        pod_discovery_poll_interval_seconds=1,
        retries=0,
        executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
        dag=dag,
    )

_D19_PACK_SIDE_RESULT_URIS_JSON = json.dumps(
    [
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            "['artifact_paths']['benchmark_pack_build_result'] }}.sides/"
            + suite_id.casefold().replace(" ", "-").replace("_", "-")
            + "-"
            + side
            + ".json"
        )
        for suite_id, side in D19_PACK_SIDE_IDENTITIES
    ],
    ensure_ascii=True,
    separators=(",", ":"),
)
aggregate_exact_nine_benchmark_packs = ReceiptKubernetesPodOperator(
    task_id="aggregate_exact_nine_benchmark_packs",
    name="serp-d19-aggregate-exact-nine-benchmark-packs",
    namespace=conf.get("kubernetes_executor", "namespace"),
    image=current_airflow_runtime_image(),
    cmds=[
        "python",
        "-m",
        "adapstory_serp_pipeline.registry.bc21_benchmark_pack_lifecycle_cli",
    ],
    arguments=[
        "aggregate-exact-nine",
        "--benchmark-catalog",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog')['artifactPath'] }}",
        "--benchmark-catalog-version-id",
        "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog')['artifactVersionId'] }}",
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
        "--side-result-uris-json",
        _D19_PACK_SIDE_RESULT_URIS_JSON,
        "--xcom-output",
        "/airflow/xcom/return.json",
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
    on_kill_action="delete_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

register_exact_nine_evaluation_binding = ReceiptKubernetesPodOperator(
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
            "{{ ti.xcom_pull(task_ids='aggregate_exact_nine_benchmark_packs')"
            "['packBuildResultEvidence']['artifactPath'] }}"
        ),
        "--lifecycle-input-version-id",
        (
            "{{ ti.xcom_pull(task_ids='aggregate_exact_nine_benchmark_packs')"
            "['packBuildResultEvidence']['artifactVersionId'] }}"
        ),
        "--lifecycle-input-sha256",
        (
            "{{ ti.xcom_pull(task_ids='aggregate_exact_nine_benchmark_packs')"
            "['packBuildResultEvidence']['artifactSha256'] }}"
        ),
        "--result-output",
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            "['artifact_paths']['benchmark_pack_lifecycle_result'] }}"
        ),
        "--xcom-output",
        "/airflow/xcom/return.json",
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
    on_kill_action="delete_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
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

materialize_official_harness_work_items = ReceiptKubernetesPodOperator(
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
        "--resume-manifest",
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            ".get('resume_manifest_evidence', {}).get('s3Uri', '') }}"
        ),
        "--resume-manifest-version-id",
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            ".get('resume_manifest_evidence', {}).get('versionId', '') }}"
        ),
        "--resume-manifest-sha256",
        (
            "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            ".get('resume_manifest_evidence', {}).get('sha256', '') }}"
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
    on_kill_action="delete_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
    dag=dag,
)


def _official_harness_task_id(suite_id: str, side: str, repetition: int) -> str:
    slug = suite_id.casefold().replace(" ", "_").replace("-", "_")
    return f"run_official_harness_{slug}_{side}_{repetition}"


def _code_sandbox_task_id(phase: str, suite_id: str, side: str, repetition: int) -> str:
    slug = suite_id.casefold().replace(" ", "_").replace("-", "_")
    return f"{phase}_code_sandbox_{slug}_{side}_{repetition}"


def _final_harness_task_id(suite_id: str, side: str, repetition: int) -> str:
    if suite_id in D19_CODE_SANDBOX_SUITES:
        return _code_sandbox_task_id("join", suite_id, side, repetition)
    return _official_harness_task_id(suite_id, side, repetition)


def select_code_sandbox_execution_path(
    work_item_plan: Mapping[str, Any],
    *,
    work_item_index: int,
    execute_task_id: str,
    reuse_task_id: str,
) -> str:
    work_items = work_item_plan.get("workItems")
    if not isinstance(work_items, list) or work_item_index >= len(work_items):
        raise ValueError("D19 code resume selector requires the canonical work-item plan")
    item = work_items[work_item_index]
    if not isinstance(item, Mapping):
        raise ValueError("D19 code resume selector work item must be an object")
    disposition = item.get("disposition")
    if disposition == "execute":
        return execute_task_id
    if disposition == "reuse":
        return reuse_task_id
    raise ValueError("D19 code work item disposition is unsupported")


def select_code_sandbox_result(
    reuse_result: Mapping[str, Any] | None,
    executed_result: Mapping[str, Any] | None,
) -> Mapping[str, Any]:
    available = [result for result in (reuse_result, executed_result) if result is not None]
    if len(available) != 1 or not isinstance(available[0], Mapping):
        raise ValueError("D19 code work item must produce exactly one execute or reuse result")
    return available[0]


D19_STANDARD_HARNESS_RUN_TASKS: dict[tuple[str, str, int], ReceiptKubernetesPodOperator] = {}
D19_CODE_SANDBOX_PREPARE_TASKS: dict[tuple[str, str, int], ReceiptKubernetesPodOperator] = {}
D19_CODE_SANDBOX_BRANCH_TASKS: dict[tuple[str, str, int], BranchPythonOperator] = {}
D19_CODE_SANDBOX_REUSE_TASKS: dict[tuple[str, str, int], ReceiptKubernetesPodOperator] = {}
D19_CODE_SANDBOX_FANOUT_TASKS: dict[tuple[str, str, int], PythonOperator] = {}
D19_CODE_SANDBOX_TASKS: dict[tuple[str, str, int], Any] = {}
D19_CODE_SANDBOX_RESULT_SET_PLAN_TASKS: dict[tuple[str, str, int], PythonOperator] = {}
D19_CODE_SANDBOX_SEAL_TASKS: dict[tuple[str, str, int], ReceiptKubernetesPodOperator] = {}
D19_CODE_SANDBOX_JOIN_TASKS: dict[tuple[str, str, int], PythonOperator] = {}
D19_OFFICIAL_HARNESS_RUN_TASKS: dict[tuple[str, str, int], Any] = {}
for work_item_index, (suite_id, side, repetition) in enumerate(D19_OFFICIAL_HARNESS_WORK_ITEMS):
    identity = (suite_id, side, repetition)
    work_item_root = (
        "{{ ti.xcom_pull(task_ids='materialize_official_harness_work_items')"
        f"['workItems'][{work_item_index}]['workItemEvidence']"
    )
    if suite_id in D19_CODE_SANDBOX_SUITES:
        branch_task_id = _code_sandbox_task_id("branch", suite_id, side, repetition)
        reuse_task_id = _code_sandbox_task_id("reuse", suite_id, side, repetition)
        prepare_task_id = _code_sandbox_task_id("prepare", suite_id, side, repetition)
        fanout_task_id = _code_sandbox_task_id("fanout", suite_id, side, repetition)
        sandbox_task_id = _code_sandbox_task_id("execute", suite_id, side, repetition)
        result_set_plan_task_id = _code_sandbox_task_id(
            "result_set_plan", suite_id, side, repetition
        )
        seal_task_id = _code_sandbox_task_id("seal", suite_id, side, repetition)
        join_task_id = _code_sandbox_task_id("join", suite_id, side, repetition)
        branch_task = BranchPythonOperator(
            task_id=branch_task_id,
            python_callable=select_code_sandbox_execution_path,
            op_args=["{{ ti.xcom_pull(task_ids='materialize_official_harness_work_items') }}"],
            op_kwargs={
                "execute_task_id": prepare_task_id,
                "reuse_task_id": reuse_task_id,
                "work_item_index": work_item_index,
            },
            executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
            dag=dag,
        )
        reuse_task = ReceiptKubernetesPodOperator(
            task_id=reuse_task_id,
            name=f"serp-d19-reuse-code-receipt-{work_item_index + 1:02d}",
            namespace=conf.get("kubernetes_executor", "namespace"),
            image=current_airflow_runtime_image(),
            cmds=[
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.official_harness_execution",
            ],
            arguments=[
                (
                    "{{ 'run-preflight-suite' if "
                    "ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
                    ".get('execution_purpose') == 'critical-path-preflight' else 'run-suite' }}"
                ),
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
            on_kill_action="delete_pod",
            on_finish_action="delete_pod",
            retries=0,
            executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
            dag=dag,
        )
        prepare_task = ReceiptKubernetesPodOperator(
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
                (
                    "{{ 'prepare-preflight-code-sandbox' if "
                    "ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
                    ".get('execution_purpose') == 'critical-path-preflight' "
                    "else 'prepare-code-sandbox' }}"
                ),
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
            container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
            security_context=hardened_runtime_pod_security_context(),
            container_security_context=hardened_runtime_container_security_context(),
            do_xcom_push=True,
            get_logs=True,
            log_events_on_failure=True,
            random_name_suffix=True,
            reattach_on_restart=True,
            on_kill_action="delete_pod",
            on_finish_action="delete_pod",
            retries=0,
            executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
            dag=dag,
        )
        trusted_sandbox_io_image = d19_code_sandbox_runtime_image()
        fanout_task = PythonOperator(
            task_id=fanout_task_id,
            python_callable=build_code_sandbox_mapped_operator_kwargs,
            op_args=["{{ ti.xcom_pull(task_ids='" + prepare_task_id + "') }}"],
            op_kwargs={
                "expected_repetition": repetition,
                "expected_side": side,
                "expected_suite_id": suite_id,
                "trusted_runtime_image": trusted_sandbox_io_image,
            },
            executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
            dag=dag,
        )
        sandbox_task = ReceiptKubernetesPodOperator.partial(
            task_id=sandbox_task_id,
            namespace=conf.get("kubernetes_executor", "namespace"),
            image=trusted_sandbox_io_image,
            cmds=[
                "python",
                "-m",
                "adapstory_serp_pipeline.orchestration.official_harness_execution",
            ],
            env_vars=d19_code_sandbox_publisher_env_vars(),
            service_account_name=D19_CODE_SANDBOX_WORKLOAD_SERVICE_ACCOUNT,
            automount_service_account_token=False,
            volumes=D19_CODE_SANDBOX_VOLUMES,
            volume_mounts=D19_CODE_SANDBOX_ORCHESTRATOR_VOLUME_MOUNTS,
            labels=D19_CODE_SANDBOX_WORKLOAD_LABELS,
            container_resources=D19_CODE_SANDBOX_PUBLISHER_RESOURCES,
            security_context=hardened_runtime_pod_security_context(),
            container_security_context=hardened_runtime_container_security_context(),
            do_xcom_push=True,
            get_logs=True,
            container_logs=["base", "sandbox-executor"],
            log_events_on_failure=True,
            random_name_suffix=True,
            reattach_on_restart=True,
            on_kill_action="delete_pod",
            on_finish_action="delete_pod",
            retries=0,
            executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
            dag=dag,
        ).expand_kwargs(fanout_task.output)
        result_set_plan_task = PythonOperator(
            task_id=result_set_plan_task_id,
            python_callable=write_code_sandbox_result_set_assembly_plan,
            op_args=[
                "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}",
                "{{ ti.xcom_pull(task_ids='" + prepare_task_id + "') }}",
                sandbox_task.output,
            ],
            op_kwargs={
                "expected_repetition": repetition,
                "expected_side": side,
                "expected_suite_id": suite_id,
            },
            executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
            dag=dag,
        )
        result_set_plan_root = (
            "{{ ti.xcom_pull(task_ids='" + result_set_plan_task_id + "')['resultSetPlanEvidence']"
        )
        seal_task = ReceiptKubernetesPodOperator(
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
                (
                    "{{ 'seal-preflight-code-receipt' if "
                    "ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
                    ".get('execution_purpose') == 'critical-path-preflight' "
                    "else 'seal-code-receipt' }}"
                ),
                "--work-item",
                work_item_root + "['artifactPath'] }}",
                "--work-item-version-id",
                work_item_root + "['artifactVersionId'] }}",
                "--work-item-sha256",
                work_item_root + "['artifactSha256'] }}",
                "--sandbox-result-set-plan",
                result_set_plan_root + "['artifactPath'] }}",
                "--sandbox-result-set-plan-version-id",
                result_set_plan_root + "['artifactVersionId'] }}",
                "--sandbox-result-set-plan-sha256",
                result_set_plan_root + "['artifactSha256'] }}",
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
            on_kill_action="delete_pod",
            on_finish_action="delete_pod",
            retries=0,
            executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
            dag=dag,
        )
        join_task = PythonOperator(
            task_id=join_task_id,
            python_callable=select_code_sandbox_result,
            op_args=[reuse_task.output, seal_task.output],
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
            dag=dag,
        )
        D19_CODE_SANDBOX_BRANCH_TASKS[identity] = branch_task
        D19_CODE_SANDBOX_REUSE_TASKS[identity] = reuse_task
        D19_CODE_SANDBOX_PREPARE_TASKS[identity] = prepare_task
        D19_CODE_SANDBOX_FANOUT_TASKS[identity] = fanout_task
        D19_CODE_SANDBOX_TASKS[identity] = sandbox_task
        D19_CODE_SANDBOX_RESULT_SET_PLAN_TASKS[identity] = result_set_plan_task
        D19_CODE_SANDBOX_SEAL_TASKS[identity] = seal_task
        D19_CODE_SANDBOX_JOIN_TASKS[identity] = join_task
        D19_OFFICIAL_HARNESS_RUN_TASKS[identity] = join_task
        continue
    runner = ReceiptKubernetesPodOperator(
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
            (
                "{{ 'run-preflight-suite' if "
                "ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
                ".get('execution_purpose') == 'critical-path-preflight' else 'run-suite' }}"
            ),
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
        on_kill_action="delete_pod",
        on_finish_action="delete_pod",
        retries=1,
        retry_delay=timedelta(seconds=15),
        executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
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

assemble_paired_execution_manifest = ReceiptKubernetesPodOperator(
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
        (
            "{{ 'assemble-preflight' if "
            "ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan')"
            ".get('execution_purpose') == 'critical-path-preflight' else 'assemble-manifest' }}"
        ),
        "--assembly-plan",
        (
            "{{ (ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            ".get('assemblyPlanEvidence') or "
            "ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            "['preflightAssemblyPlanEvidence'])['s3Uri'] }}"
        ),
        "--assembly-plan-version-id",
        (
            "{{ (ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            ".get('assemblyPlanEvidence') or "
            "ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            "['preflightAssemblyPlanEvidence'])['versionId'] }}"
        ),
        "--assembly-plan-sha256",
        (
            "{{ (ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            ".get('assemblyPlanEvidence') or "
            "ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan')"
            "['preflightAssemblyPlanEvidence'])['sha256'] }}"
        ),
    ],
    env_vars=d19_attestor_env_vars(),
    service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_ATTESTOR_VOLUMES,
    volume_mounts=D19_ATTESTOR_VOLUME_MOUNTS,
    labels=D19_AGGREGATOR_WORKLOAD_LABELS,
    container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    do_xcom_push=True,
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="delete_pod",
    on_finish_action="delete_pod",
    retries=0,
    executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

run_paired_evaluation = ReceiptKubernetesPodOperator(
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
            "['executionManifestEvidence']['s3Uri'] }}"
        ),
        "--execution-manifest-version-id",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestEvidence']['versionId'] }}"
        ),
        "--execution-manifest-sha256",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestEvidence']['sha256'] }}"
        ),
        "--execution-manifest-attestation",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestAttestationEvidence']['s3Uri'] }}"
        ),
        "--execution-manifest-attestation-version-id",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestAttestationEvidence']['versionId'] }}"
        ),
        "--execution-manifest-attestation-sha256",
        (
            "{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest')"
            "['executionManifestAttestationEvidence']['sha256'] }}"
        ),
        "--evidence-output",
        "{{ ti.xcom_pull(task_ids='write_paired_eval_request')['evidenceOutputPath'] }}",
    ],
    env_vars=d19_attestor_env_vars(),
    service_account_name=D19_AGGREGATOR_WORKLOAD_SERVICE_ACCOUNT,
    automount_service_account_token=False,
    volumes=D19_ATTESTOR_VOLUMES,
    volume_mounts=D19_ATTESTOR_VOLUME_MOUNTS,
    labels=D19_AGGREGATOR_WORKLOAD_LABELS,
    container_resources=D19_NATIVE_ADAPTER_RUNNER_RESOURCES,
    security_context=hardened_runtime_pod_security_context(),
    container_security_context=hardened_runtime_container_security_context(),
    get_logs=True,
    log_events_on_failure=True,
    random_name_suffix=True,
    reattach_on_restart=True,
    on_kill_action="delete_pod",
    on_finish_action="delete_pod",
    retries=0,
    retry_delay=timedelta(seconds=5),
    do_xcom_push=True,
    executor_config=d19_remote_kubernetes_pod_launcher_executor_config(),
    dag=dag,
)

choose_post_assembly = BranchPythonOperator(
    task_id="choose_post_assembly_path",
    python_callable=choose_post_assembly_path,
    op_args=["{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

persist_critical_path_preflight = PythonOperator(
    task_id="persist_critical_path_preflight_evidence",
    python_callable=persist_critical_path_preflight_evidence,
    op_args=["{{ ti.xcom_pull(task_ids='assemble_paired_execution_manifest') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

persist_paired_evaluation_verification = PythonOperator(
    task_id="persist_paired_evaluation_verification_evidence",
    python_callable=write_paired_evaluation_verification_evidence,
    op_kwargs={
        "airflow_run": {
            "dagId": "{{ dag.dag_id }}",
            "logicalDate": "{{ logical_date.isoformat() }}",
            "runId": "{{ run_id }}",
            "runType": "{{ dag_run.run_type.value }}",
        },
        "evaluator_result": ("{{ ti.xcom_pull(task_ids='run_paired_benchmark_evaluation') }}"),
        "plan_json": ("{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"),
    },
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

write_official_measurement = PythonOperator(
    task_id="write_official_serp_mcp_measurement",
    python_callable=write_official_serp_mcp_measurement,
    op_kwargs={
        "airflow_run": {
            "dagId": "{{ dag.dag_id }}",
            "logicalDate": "{{ logical_date.isoformat() }}",
            "runId": "{{ run_id }}",
            "runType": "{{ dag_run.run_type.value }}",
        },
        "plan_json": ("{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}"),
        "verification_result": (
            "{{ ti.xcom_pull(task_ids='persist_paired_evaluation_verification_evidence') }}"
        ),
    },
    retries=0,
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

publish_official_measurement = PythonOperator(
    task_id="publish_official_serp_mcp_measurement",
    python_callable=publish_official_serp_mcp_measurement,
    op_args=[
        "{{ ti.xcom_pull(task_ids='validate_benchmark_improvement_wave_plan') }}",
        "{{ ti.xcom_pull(task_ids='write_official_serp_mcp_measurement') }}",
    ],
    retries=2,
    retry_delay=timedelta(seconds=30),
    executor_config=D19_OFFICIAL_MEASUREMENT_PUBLISHER_EXECUTOR_CONFIG,
    dag=dag,
)

write_release_terminal_outbox = PythonOperator(
    task_id=D19_TERMINAL_OUTBOX_TASK_ID,
    python_callable=write_d19_release_terminal_outbox,
    op_kwargs={
        "dag_run_conf": "{{ dag_run.conf }}",
        "current_run_id": "{{ run_id }}",
        "verification_result": (
            "{{ ti.xcom_pull(task_ids='persist_paired_evaluation_verification_evidence') }}"
        ),
        "catalog": "{{ ti.xcom_pull(task_ids='load_materialized_benchmark_catalog') }}",
        "pack_lifecycle": ("{{ ti.xcom_pull(task_ids='register_exact_nine_evaluation_binding') }}"),
        "work_item_plan": ("{{ ti.xcom_pull(task_ids='write_paired_evaluation_assembly_plan') }}"),
    },
    on_success_callback=wake_d19_terminal_finalizer,
    retries=2,
    retry_delay=timedelta(seconds=30),
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

choose_release_transition = BranchPythonOperator(
    task_id=D19_RELEASE_TRANSITION_TASK_ID,
    python_callable=choose_d19_release_transition,
    op_args=["{{ ti.xcom_pull(task_ids='write_d19_release_terminal_outbox') }}"],
    executor_config=D19_AGGREGATOR_EXECUTOR_CONFIG,
    dag=dag,
)

trigger_next_release_run = TriggerDagRunOperator(
    task_id=D19_TRIGGER_NEXT_RUN_TASK_ID,
    trigger_dag_id=D19_DAG_ID,
    trigger_run_id=(
        "{{ ti.xcom_pull(task_ids='write_d19_release_terminal_outbox')['nextRun']['runId'] }}"
    ),
    logical_date=(
        "{{ ti.xcom_pull(task_ids='write_d19_release_terminal_outbox')['nextRun']['logicalDate'] }}"
    ),
    conf="{{ ti.xcom_pull(task_ids='write_d19_release_terminal_outbox')['nextRunConf'] }}",
    wait_for_completion=False,
    reset_dag_run=False,
    skip_when_already_exists=True,
    dag=dag,
)

release_terminal_complete = EmptyOperator(
    task_id=D19_RELEASE_COMPLETE_TASK_ID,
    dag=dag,
)

(
    verify_terminal_activation
    >> validate_admission
    >> validate_plan
    >> verify_bc10_ledger_capacity
    >> materialize_catalog
    >> load_catalog
    >> catalog_readiness_gate
)
validate_plan >> load_promotion
# Context: a blocked catalog previously expanded one acquisition failure into
# 18 pack-side failures. Decision: branch before the expensive fan-out and fail
# through one explicit catalog blocker. Reason: pack jobs cannot repair missing
# catalog evidence and must not obscure the root cause. Revisit when: pack-side
# execution can make catalog evidence ready without violating immutability.
catalog_readiness_gate >> catalog_blocker
catalog_readiness_gate >> classify_pack_cas
load_promotion >> classify_pack_cas
classify_pack_cas >> pack_cas_readiness_gate
pack_cas_readiness_gate >> plan_pack_cas_migration_rebuild_task
plan_pack_cas_migration_rebuild_task >> pack_cas_migration_rebuild_required
for suite_id, side in D19_PACK_SIDE_IDENTITIES:
    side_task = D19_PACK_SIDE_BUILD_TASKS[(suite_id, side)]
    pack_cas_readiness_gate >> side_task
    side_task >> aggregate_exact_nine_benchmark_packs
# Context: immutable baseline/candidate routes now terminate on different GPU
# endpoints and write distinct semantic CAS identities.
# Decision: let the two sides compete only for the two-slot governed pool.
# Reason: an artificial per-suite edge kept one GPU idle and blocked the exact
# SWE-baseline/CodeRAG-candidate parallel execution required by D19.
# Revisit when: a future paired algorithm has a real data dependency between sides.
aggregate_exact_nine_benchmark_packs >> register_exact_nine_evaluation_binding
register_exact_nine_evaluation_binding >> load_exact_nine_evaluation_binding
load_promotion >> write_request
load_exact_nine_evaluation_binding >> write_request
write_request >> materialize_official_harness_work_items
for standard_runner in D19_STANDARD_HARNESS_RUN_TASKS.values():
    materialize_official_harness_work_items >> standard_runner >> write_assembly_plan
for identity, prepare_task in D19_CODE_SANDBOX_PREPARE_TASKS.items():
    branch_task = D19_CODE_SANDBOX_BRANCH_TASKS[identity]
    reuse_task = D19_CODE_SANDBOX_REUSE_TASKS[identity]
    fanout_task = D19_CODE_SANDBOX_FANOUT_TASKS[identity]
    sandbox_task = D19_CODE_SANDBOX_TASKS[identity]
    result_set_plan_task = D19_CODE_SANDBOX_RESULT_SET_PLAN_TASKS[identity]
    seal_task = D19_CODE_SANDBOX_SEAL_TASKS[identity]
    join_task = D19_CODE_SANDBOX_JOIN_TASKS[identity]
    materialize_official_harness_work_items >> branch_task
    branch_task >> reuse_task >> join_task
    branch_task >> prepare_task
    prepare_task >> fanout_task >> sandbox_task >> result_set_plan_task
    result_set_plan_task >> seal_task >> join_task >> write_assembly_plan
write_assembly_plan >> assemble_paired_execution_manifest >> choose_post_assembly
(
    choose_post_assembly
    >> run_paired_evaluation
    >> persist_paired_evaluation_verification
    >> write_official_measurement
    >> publish_official_measurement
    >> write_release_terminal_outbox
    >> choose_release_transition
)
choose_post_assembly >> persist_critical_path_preflight
choose_release_transition >> trigger_next_release_run
choose_release_transition >> release_terminal_complete
