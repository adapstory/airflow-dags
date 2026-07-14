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
_BENCHMARK_CATALOG_ACQUISITION_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_PATH_STYLE",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION",
    "ADAPSTORY_AIRFLOW_EVIDENCE_RETENTION_DAYS",
    "ADAPSTORY_SERP_SOURCE_PROXY_URL",
)


def benchmark_catalog_acquisition_env_vars() -> list[k8s.V1EnvVar]:
    """Return only proxy and evidence-store dependencies for catalog acquisition."""

    values: list[k8s.V1EnvVar] = []
    for name in _BENCHMARK_CATALOG_ACQUISITION_ENV_NAMES:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"benchmark catalog acquisition environment is required: {name}")
        values.append(k8s.V1EnvVar(name=name, value=repr(value)))
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
