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
EVIDENCE_BUCKET = "airflow-serp-evidence"
EVIDENCE_PREFIX = "serp-evals/"
_STATIC_CREDENTIAL_ENV_NAMES = (
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_ACCESS_KEY",
    "ADAPSTORY_AIRFLOW_ARTIFACT_S3_SECRET_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
)


def minio_web_identity_env_vars(required_names: Sequence[str]) -> list[k8s.V1EnvVar]:
    """Return serializable runtime values plus a bounded projected-token contract."""

    values: list[k8s.V1EnvVar] = []
    for name in required_names:
        value = os.environ.get(name)
        if value is None or not value.strip():
            raise ValueError(f"evidence workload environment is required: {name}")
        values.append(k8s.V1EnvVar(name=name, value=value.strip()))
    values.extend(
        (
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE",
                value=MINIO_WEB_IDENTITY_TOKEN_FILE,
            ),
            k8s.V1EnvVar(
                name="ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS",
                value=str(MINIO_WEB_IDENTITY_EXPIRATION_SECONDS),
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


def minio_web_identity_executor_config(
    *,
    service_account_name: str,
    labels: Mapping[str, str],
) -> dict[str, Any]:
    """Return a KubernetesExecutor override with only a MinIO STS token."""

    return {
        "pod_override": k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(labels=dict(labels)),
            spec=k8s.V1PodSpec(
                automount_service_account_token=False,
                containers=[
                    k8s.V1Container(
                        name="base",
                        env=minio_web_identity_env_vars(()),
                        volume_mounts=minio_web_identity_volume_mounts(),
                    )
                ],
                service_account_name=service_account_name,
                volumes=minio_web_identity_volumes(),
            ),
        )
    }


def operation_prefix_s3_client(*, artifact_uris: Iterable[str]) -> Any:
    """Exchange the projected token for one exact evidence-operation session."""

    resources = tuple(sorted({operation_prefix_resource(uri) for uri in artifact_uris}))
    if len(resources) != 1:
        raise ValueError("all operation artifacts must belong to one evidence operation")
    _reject_static_credentials()
    endpoint_url = _required_env("ADAPSTORY_AIRFLOW_ARTIFACT_S3_ENDPOINT")
    region_name = os.environ.get("ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION", "us-west-1").strip()
    if not region_name:
        raise ValueError("ADAPSTORY_AIRFLOW_ARTIFACT_S3_REGION must not be empty")
    token = _projected_token()
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
