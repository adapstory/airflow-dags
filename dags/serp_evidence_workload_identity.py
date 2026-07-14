"""Projected MinIO web identity for least-privilege evidence workload pods."""

from __future__ import annotations

import os
from collections.abc import Sequence

from airflow.sdk import literal
from kubernetes.client import models as k8s

MINIO_WEB_IDENTITY_TOKEN_FILE = "/var/run/secrets/adapstory/minio-web-identity/token"
MINIO_WEB_IDENTITY_TOKEN_VOLUME_NAME = "minio-web-identity-token"
MINIO_WEB_IDENTITY_AUDIENCE = "minio"
MINIO_WEB_IDENTITY_EXPIRATION_SECONDS = 900


def minio_web_identity_env_vars(required_names: Sequence[str]) -> list[k8s.V1EnvVar]:
    """Return literal runtime values plus a bounded projected-token contract."""

    values: list[k8s.V1EnvVar] = []
    for name in required_names:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"evidence workload environment is required: {name}")
        # Airflow renders KPO templates through NativeEnvironment.  A plain
        # numeric-looking string (for example a retention period) is then
        # coerced to an integer and rejected by the Kubernetes EnvVar API.
        # `literal` preserves the required contract for every caller, rather
        # than making each workload remember to special-case numeric values.
        values.append(k8s.V1EnvVar(name=name, value=literal(value.strip())))
    values.extend(
        (
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE",
                value=MINIO_WEB_IDENTITY_TOKEN_FILE,
            ),
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS",
                value=literal(str(MINIO_WEB_IDENTITY_EXPIRATION_SECONDS)),
            ),
        )
    )
    return values


def minio_web_identity_volumes() -> list[k8s.V1Volume]:
    """Project a short-lived token without enabling ambient API credentials."""

    return [
        k8s.V1Volume(
            name=MINIO_WEB_IDENTITY_TOKEN_VOLUME_NAME,
            projected=k8s.V1ProjectedVolumeSource(
                sources=[
                    k8s.V1VolumeProjection(
                        service_account_token=k8s.V1ServiceAccountTokenProjection(
                            audience=MINIO_WEB_IDENTITY_AUDIENCE,
                            expiration_seconds=MINIO_WEB_IDENTITY_EXPIRATION_SECONDS,
                            path="token",
                        )
                    )
                ]
            ),
        )
    ]


def minio_web_identity_volume_mounts() -> list[k8s.V1VolumeMount]:
    return [
        k8s.V1VolumeMount(
            name=MINIO_WEB_IDENTITY_TOKEN_VOLUME_NAME,
            mount_path=MINIO_WEB_IDENTITY_TOKEN_FILE.rsplit("/", 1)[0],
            read_only=True,
        )
    ]
