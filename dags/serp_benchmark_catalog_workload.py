"""Least-privilege runtime contract for external mandatory-benchmark acquisition."""

from __future__ import annotations

import os

from kubernetes.client import models as k8s

BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_SERVICE_ACCOUNT = "airflow-serp-benchmark-acquisition"
BENCHMARK_CATALOG_ACQUISITION_WORKLOAD_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "benchmark-acquisition",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
BENCHMARK_CATALOG_ACQUISITION_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "256Mi"},
    limits={"cpu": "500m", "memory": "1Gi"},
)
BENCHMARK_CATALOG_ACQUISITION_RETRY_DELAY_SECONDS = 90
_BENCHMARK_CATALOG_ACQUISITION_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
    "ADAPSTORY_SERP_SOURCE_PROXY_URL",
)
_BENCHMARK_CATALOG_ACQUISITION_NO_PROXY_HOSTS = (
    "localhost",
    "127.0.0.1",
    ".svc",
    ".svc.cluster.local",
)


def benchmark_catalog_acquisition_env_vars() -> list[k8s.V1EnvVar]:
    """Return only proxy and evidence-store dependencies for catalog acquisition."""

    values: list[k8s.V1EnvVar] = []
    for name in _BENCHMARK_CATALOG_ACQUISITION_ENV_NAMES:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"benchmark catalog acquisition environment is required: {name}")
        values.append(k8s.V1EnvVar(name=name, value=repr(value)))
    proxy_url = os.environ["ADAPSTORY_SERP_SOURCE_PROXY_URL"].strip()
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
    values.extend(
        (
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-serp-evidence-store",
                        key="access-key",
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-serp-evidence-store",
                        key="secret-key",
                    )
                ),
            ),
        )
    )
    return values


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
