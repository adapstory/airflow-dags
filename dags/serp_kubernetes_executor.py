"""Minimal KubernetesExecutor overrides for task-scoped secrets."""

from __future__ import annotations

from typing import Any

from kubernetes.client import models as k8s


def task_secret_env_var(*, name: str, secret_name: str, secret_key: str) -> k8s.V1EnvVar:
    """Build one task-scoped Secret projection without creating a second pod override."""

    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name=secret_name,
                key=secret_key,
            )
        ),
    )


def task_secret_executor_config(*, name: str, secret_name: str, secret_key: str) -> dict[str, Any]:
    """Project one named Kubernetes Secret key into exactly one task pod."""

    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    k8s.V1Container(
                        name="base",
                        env=[
                            task_secret_env_var(
                                name=name,
                                secret_name=secret_name,
                                secret_key=secret_key,
                            )
                        ],
                    )
                ],
            )
        )
    }
