"""Least-privilege runtime contract for external mandatory-benchmark acquisition."""

from __future__ import annotations

import os

from kubernetes.client import models as k8s

from dags.serp_benchmark_runtime_contract import (
    BENCHMARK_SUBSTRATE_SOURCE_SET_CONFIG_MAP,
    BENCHMARK_SUBSTRATE_SOURCE_SET_CONFIG_MAP_KEY,
    BENCHMARK_SUBSTRATE_SOURCE_SET_FILE_ENV_NAME,
    BENCHMARK_SUBSTRATE_SOURCE_SET_FILE_PATH,
    BENCHMARK_SUBSTRATE_SOURCE_SET_MOUNT_PATH,
    BENCHMARK_SUBSTRATE_SOURCE_SET_VOLUME_NAME,
)
from dags.serp_evidence_workload_identity import (
    minio_web_identity_env_vars,
    minio_web_identity_volume_mounts,
    minio_web_identity_volumes,
    native_template_safe_env_value,
)

BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-acquisition"
BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-acquisition",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
BENCHMARK_CATALOG_ACQUISITION_RESOURCES = k8s.V1ResourceRequirements(
    # The exact SWE source set emits a bounded but multi-gigabyte canonical
    # payload. Line-bounded validation avoids a second copy; this reservation
    # covers the one required payload plus the sliding archive window.
    requests={"cpu": "500m", "memory": "6Gi"},
    limits={"cpu": "1000m", "memory": "8Gi"},
)
BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS = 90
_BENCHMARK_CATALOG_ACQUISITION_LITERAL_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
    "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
)
_BENCHMARK_CATALOG_ACQUISITION_WEB_IDENTITY_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
)
_BENCHMARK_CATALOG_ACQUISITION_NO_PROXY_HOSTS = (
    "localhost",
    "127.0.0.1",
    ".svc",
    ".svc.cluster.local",
)


def benchmark_catalog_acquisition_env_vars() -> list[k8s.V1EnvVar]:
    """Return proxy settings plus projected MinIO STS identity for acquisition."""

    values = minio_web_identity_env_vars(_BENCHMARK_CATALOG_ACQUISITION_WEB_IDENTITY_ENV_NAMES)
    for name in _BENCHMARK_CATALOG_ACQUISITION_LITERAL_ENV_NAMES:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"benchmark catalog acquisition environment is required: {name}")
        values.append(k8s.V1EnvVar(name=name, value=native_template_safe_env_value(value.strip())))
    values.append(
        k8s.V1EnvVar(
            name=BENCHMARK_SUBSTRATE_SOURCE_SET_FILE_ENV_NAME,
            value=BENCHMARK_SUBSTRATE_SOURCE_SET_FILE_PATH,
        )
    )
    proxy_url = os.environ.get("ADAPSTORY_SERP_SOURCE_PROXY_URL", "").strip()
    if not proxy_url:
        raise ValueError(
            "benchmark catalog acquisition environment is required: ADAPSTORY_SERP_SOURCE_PROXY_URL"
        )
    values.append(
        k8s.V1EnvVar(
            name="ADAPSTORY_SERP_SOURCE_PROXY_URL",
            value=native_template_safe_env_value(proxy_url),
        )
    )
    values.extend(
        (
            k8s.V1EnvVar(name="HTTP_PROXY", value=proxy_url),
            k8s.V1EnvVar(name="HTTPS_PROXY", value=proxy_url),
            k8s.V1EnvVar(
                name="NO_PROXY",
                value=_benchmark_catalog_acquisition_no_proxy_value(),
            ),
        )
    )
    return values


def benchmark_catalog_acquisition_web_identity_volumes() -> list[k8s.V1Volume]:
    """Project the bounded MinIO audience token without an ambient API token."""

    return [
        *minio_web_identity_volumes(),
        k8s.V1Volume(
            name=BENCHMARK_SUBSTRATE_SOURCE_SET_VOLUME_NAME,
            config_map=k8s.V1ConfigMapVolumeSource(
                name=BENCHMARK_SUBSTRATE_SOURCE_SET_CONFIG_MAP,
                optional=True,
                items=[
                    k8s.V1KeyToPath(
                        key=BENCHMARK_SUBSTRATE_SOURCE_SET_CONFIG_MAP_KEY,
                        path="source-set-evidence.json",
                    )
                ],
            ),
        ),
    ]


def benchmark_catalog_acquisition_web_identity_volume_mounts() -> list[k8s.V1VolumeMount]:
    """Mount the bounded MinIO identity token read-only."""

    return [
        *minio_web_identity_volume_mounts(),
        k8s.V1VolumeMount(
            name=BENCHMARK_SUBSTRATE_SOURCE_SET_VOLUME_NAME,
            mount_path=BENCHMARK_SUBSTRATE_SOURCE_SET_MOUNT_PATH,
            read_only=True,
        ),
    ]


def benchmark_catalog_acquisition_pod_security_context() -> k8s.V1PodSecurityContext:
    """Return the restricted PodSecurity context required for acquisition pods."""

    return k8s.V1PodSecurityContext(
        run_as_non_root=True,
        run_as_user=50000,
        seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
    )


def benchmark_catalog_acquisition_container_security_context() -> k8s.V1SecurityContext:
    """Return the least-privilege container context paired with the pod context."""

    return k8s.V1SecurityContext(
        allow_privilege_escalation=False,
        capabilities=k8s.V1Capabilities(drop=["ALL"]),
        run_as_non_root=True,
        run_as_user=50000,
    )


def _benchmark_catalog_acquisition_no_proxy_value() -> str:
    existing = [
        value.strip() for value in os.environ.get("NO_PROXY", "").split(",") if value.strip()
    ]
    return ",".join(dict.fromkeys((*existing, *_BENCHMARK_CATALOG_ACQUISITION_NO_PROXY_HOSTS)))
