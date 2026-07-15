"""Projected MinIO web identity for least-privilege evidence workload pods."""

from __future__ import annotations

import importlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from kubernetes.client import models as k8s

MINIO_WEB_IDENTITY_TOKEN_FILE = "/var/run/secrets/adapstory/minio-web-identity/token"
MINIO_WEB_IDENTITY_TOKEN_VOLUME_NAME = "minio-web-identity-token"
MINIO_WEB_IDENTITY_AUDIENCE = "minio"
MINIO_WEB_IDENTITY_EXPIRATION_SECONDS = 900
BC21_WORKLOAD_TOKEN_FILE = "/var/run/secrets/adapstory/bc21-workload/token"
BC21_WORKLOAD_TOKEN_VOLUME_NAME = "bc21-workload-token"
BC21_WORKLOAD_TOKEN_AUDIENCE = "https://kubernetes.default.svc.cluster.local"
BC21_WORKLOAD_TOKEN_EXPIRATION_SECONDS = 900
EVIDENCE_BUCKET = "airflow-serp-evidence"
EVIDENCE_PREFIX = "serp-evals/"
TASK_LOG_BUCKET = "airflow-serp-artifacts"
TASK_LOG_PREFIX = "airflow-task-logs/"
KUBERNETES_POD_LAUNCHER_SERVICE_ACCOUNT = "airflow-serp-kubernetes-pod-launcher"
KUBERNETES_POD_LAUNCHER_LABELS = {
    "adapstory.com/serp-evidence-workload": "true",
    "adapstory.com/serp-network-profile": "kubernetes-pod-launcher",
    "component": "worker",
    "release": "airflow",
    "tier": "airflow",
}
_STATIC_CREDENTIAL_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)


def native_template_safe_env_value(value: str) -> str:
    """Keep decimal environment values strings under native Jinja rendering.

    Airflow's native template mode parses a bare decimal as an integer before
    Kubernetes serializes the pod. JSON string syntax retains the original
    value as a Kubernetes ``EnvVar.value`` string.
    """

    return json.dumps(value) if value.isdecimal() else value


def minio_web_identity_env_vars(required_names: Sequence[str]) -> list[k8s.V1EnvVar]:
    """Return serializable runtime values plus a bounded projected-token contract."""

    values: list[k8s.V1EnvVar] = []
    for name in required_names:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"evidence workload environment is required: {name}")
        values.append(k8s.V1EnvVar(name=name, value=native_template_safe_env_value(value.strip())))
    values.extend(
        (
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE",
                value=MINIO_WEB_IDENTITY_TOKEN_FILE,
            ),
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS",
                value=native_template_safe_env_value(str(MINIO_WEB_IDENTITY_EXPIRATION_SECONDS)),
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


def bc21_workload_env_vars() -> list[k8s.V1EnvVar]:
    """Point BC-21 clients at their bounded projected authorization token."""

    return [
        k8s.V1EnvVar(
            name="ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH",
            value=BC21_WORKLOAD_TOKEN_FILE,
        )
    ]


def bc21_workload_volumes() -> list[k8s.V1Volume]:
    """Project the Kubernetes API audience without ambient token mounting."""

    return [
        k8s.V1Volume(
            name=BC21_WORKLOAD_TOKEN_VOLUME_NAME,
            projected=k8s.V1ProjectedVolumeSource(
                sources=[
                    k8s.V1VolumeProjection(
                        service_account_token=k8s.V1ServiceAccountTokenProjection(
                            audience=BC21_WORKLOAD_TOKEN_AUDIENCE,
                            expiration_seconds=BC21_WORKLOAD_TOKEN_EXPIRATION_SECONDS,
                            path="token",
                        )
                    )
                ]
            ),
        )
    ]


def bc21_workload_volume_mounts() -> list[k8s.V1VolumeMount]:
    """Mount the BC-21 authorization token read-only at its explicit path."""

    return [
        k8s.V1VolumeMount(
            name=BC21_WORKLOAD_TOKEN_VOLUME_NAME,
            mount_path=BC21_WORKLOAD_TOKEN_FILE.rsplit("/", 1)[0],
            read_only=True,
        )
    ]


def minio_web_identity_executor_config(
    *,
    service_account_name: str,
    labels: Mapping[str, str],
) -> dict[str, Any]:
    """Return a KubernetesExecutor override with only a MinIO STS token."""

    return _evidence_executor_config(
        service_account_name=service_account_name,
        labels=labels,
        env_vars=minio_web_identity_env_vars(()),
        volume_mounts=minio_web_identity_volume_mounts(),
        volumes=minio_web_identity_volumes(),
    )


def bc21_authorized_minio_executor_config(
    *,
    service_account_name: str,
    labels: Mapping[str, str],
) -> dict[str, Any]:
    """Build a no-ambient-token executor identity for BC-21 evidence tasks."""

    return _evidence_executor_config(
        service_account_name=service_account_name,
        labels=labels,
        env_vars=[*minio_web_identity_env_vars(()), *bc21_workload_env_vars()],
        volume_mounts=[*minio_web_identity_volume_mounts(), *bc21_workload_volume_mounts()],
        volumes=[*minio_web_identity_volumes(), *bc21_workload_volumes()],
    )


def _evidence_executor_config(
    *,
    service_account_name: str,
    labels: Mapping[str, str],
    env_vars: list[k8s.V1EnvVar],
    volume_mounts: list[k8s.V1VolumeMount],
    volumes: list[k8s.V1Volume],
) -> dict[str, Any]:
    """Return one least-privilege KubernetesExecutor pod override."""

    return {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels=dict(labels)),
            spec=k8s.V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    k8s.V1Container(
                        name="base",
                        env=env_vars,
                        volume_mounts=volume_mounts,
                    )
                ],
                service_account_name=service_account_name,
                volumes=volumes,
            ),
        )
    }


def kubernetes_pod_launcher_executor_config() -> dict[str, Any]:
    """Build the sole executor override allowed to create child Kubernetes pods.

    The controller receives a Kubernetes API token from its narrowly-bound
    ServiceAccount. MinIO access remains a separate projected ``minio`` token,
    never an ambient static credential or a Kubernetes API credential.
    """

    return {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels=dict(KUBERNETES_POD_LAUNCHER_LABELS)),
            spec=k8s.V1PodSpec(
                automount_service_account_token=True,
                containers=[
                    k8s.V1Container(
                        name="base",
                        env=minio_web_identity_env_vars(()),
                        volume_mounts=minio_web_identity_volume_mounts(),
                    )
                ],
                service_account_name=KUBERNETES_POD_LAUNCHER_SERVICE_ACCOUNT,
                volumes=minio_web_identity_volumes(),
            ),
        )
    }


def operation_prefix_s3_client(*, artifact_uris: Iterable[str]) -> Any:
    """Exchange the projected token for one exact evidence-operation session."""

    resources = tuple(sorted({operation_prefix_resource(uri) for uri in artifact_uris}))
    if len(resources) != 1:
        raise ValueError("all operation artifacts must belong to one evidence operation")
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectRetention",
                        "s3:GetObjectVersion",
                        "s3:PutObject",
                    ],
                    "Effect": "Allow",
                    "Resource": list(resources),
                }
            ],
        },
        sort_keys=True,
    )
    return _web_identity_s3_client(policy=policy)


def task_log_s3_client() -> Any:
    """Return an STS client constrained to the immutable Airflow task-log prefix."""

    return _web_identity_s3_client(
        policy=build_minio_prefix_policy(
            bucket=TASK_LOG_BUCKET,
            prefix=TASK_LOG_PREFIX,
            object_actions=("s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"),
        )
    )


def build_minio_prefix_policy(
    *,
    bucket: str,
    prefix: str,
    object_actions: Sequence[str],
) -> str:
    """Build a deterministic STS policy limited to one S3 object prefix."""

    _validate_prefix_scope(bucket=bucket, prefix=prefix, object_actions=object_actions)
    bucket_resource = f"arn:aws:s3:::{bucket}"
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["s3:GetBucketLocation"],
                    "Effect": "Allow",
                    "Resource": [bucket_resource],
                },
                {
                    "Action": ["s3:ListBucket"],
                    "Condition": {
                        "StringLike": {"s3:prefix": [prefix.removesuffix("/"), f"{prefix}*"]}
                    },
                    "Effect": "Allow",
                    "Resource": [bucket_resource],
                },
                {
                    "Action": list(object_actions),
                    "Effect": "Allow",
                    "Resource": [f"{bucket_resource}/{prefix}*"],
                },
            ],
        },
        sort_keys=True,
    )


def _web_identity_s3_client(*, policy: str) -> Any:
    """Exchange a projected token for an S3 client without ambient credentials."""

    _reject_static_credentials()
    endpoint_url = _required_env("ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT")
    region_name = os.environ.get("ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION", "us-west-1").strip()
    if not region_name:
        raise ValueError("ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION must not be empty")
    token = _projected_token()
    credentials = _assume_minio_role_with_web_identity(
        endpoint_url=endpoint_url,
        token=token,
        policy=policy,
    )
    boto3 = importlib.import_module("boto3")
    botocore_config = importlib.import_module("botocore.config")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region_name,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        config=botocore_config.Config(s3={"addressing_style": "path"}),
    )


def _validate_prefix_scope(*, bucket: str, prefix: str, object_actions: Sequence[str]) -> None:
    if not bucket or "/" in bucket or bucket.strip() != bucket:
        raise ValueError("MinIO bucket scope is invalid")
    if not prefix.endswith("/") or prefix.startswith("/") or ".." in prefix.split("/"):
        raise ValueError("MinIO prefix scope is invalid")
    if not object_actions or any(not action.startswith("s3:") for action in object_actions):
        raise ValueError("MinIO object actions are invalid")


def operation_prefix_resource(artifact_uri: str) -> str:
    parsed = urlparse(artifact_uri)
    if (
        parsed.scheme != "s3"
        or parsed.netloc != EVIDENCE_BUCKET
        or not parsed.path.startswith("/" + EVIDENCE_PREFIX)
    ):
        raise ValueError("evidence artifact URI must stay under airflow-serp-evidence/serp-evals")
    path_segments = parsed.path.lstrip("/").split("/")
    if len(path_segments) < 3 or not path_segments[1].strip():
        raise ValueError("evidence artifact URI must be operation-local")
    return f"arn:aws:s3:::{EVIDENCE_BUCKET}/{EVIDENCE_PREFIX}{path_segments[1]}/*"


def _assume_minio_role_with_web_identity(
    *,
    endpoint_url: str,
    token: str,
    policy: str,
) -> dict[str, str]:
    parsed = urlsplit(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT must be an absolute HTTP URL")
    request_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", "", ""))
    body = urlencode(
        {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "WebIdentityToken": token,
            "DurationSeconds": str(MINIO_WEB_IDENTITY_EXPIRATION_SECONDS),
            "Policy": policy,
        }
    ).encode("utf-8")
    try:
        with urlopen(
            Request(
                request_url,
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            ),
            timeout=10.0,
        ) as response:
            payload = response.read()
    except (HTTPError, URLError, OSError) as exc:
        raise ValueError("MinIO web-identity STS exchange failed") from exc
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as exc:
        raise ValueError("MinIO STS response is not valid XML") from exc
    credentials = next(
        (element for element in root.iter() if element.tag.rsplit("}", 1)[-1] == "Credentials"),
        None,
    )
    if credentials is None:
        raise ValueError("MinIO STS response is missing Credentials")
    values = {element.tag.rsplit("}", 1)[-1]: (element.text or "") for element in credentials}
    required = ("AccessKeyId", "SecretAccessKey", "SessionToken")
    if any(not values.get(name, "").strip() for name in required):
        raise ValueError("MinIO STS response is missing credentials")
    return {name: values[name] for name in required}


def _projected_token() -> str:
    token_path = _required_env("ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE")
    try:
        token = open(token_path, encoding="utf-8").read().strip()
    except OSError as exc:
        raise ValueError("MinIO web-identity token cannot be read") from exc
    if not token:
        raise ValueError("MinIO web-identity token is empty")
    return token


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _reject_static_credentials() -> None:
    if any(os.environ.get(name, "").strip() for name in _STATIC_CREDENTIAL_ENV_NAMES):
        raise ValueError("static MinIO credentials are forbidden for evidence workloads")
