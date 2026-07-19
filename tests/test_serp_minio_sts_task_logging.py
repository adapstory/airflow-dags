from __future__ import annotations

import importlib
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Any, ClassVar

import pytest


class _MissingObjectError(Exception):
    response: ClassVar[dict[str, dict[str, str]]] = {"Error": {"Code": "NoSuchKey"}}


class _Body:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def read(self) -> bytes:
        return self._value


class _S3:
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.fail_put = False
        self.objects: dict[str, bytes] = {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        assert Bucket == self.bucket
        if Key not in self.objects:
            raise _MissingObjectError()
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        assert Bucket == self.bucket
        if self.fail_put:
            raise RuntimeError("injected remote log failure")
        self.objects[Key] = Body

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["Bucket"] == self.bucket
        prefix = str(kwargs["Prefix"])
        return {
            "Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(prefix)],
            "IsTruncated": False,
        }


@contextmanager
def _isolated_task_log_modules() -> Iterator[tuple[ModuleType, ModuleType]]:
    module_names = (
        "dags.serp_minio_sts_task_log_io",
        "dags.serp_evidence_workload_identity",
    )
    previous = {name: sys.modules.pop(name, None) for name in module_names}
    try:
        task_log_io = importlib.import_module("dags.serp_minio_sts_task_log_io")
        workload_identity = importlib.import_module("dags.serp_evidence_workload_identity")
        yield task_log_io, workload_identity
    finally:
        for name in module_names:
            sys.modules.pop(name, None)
        sys.modules.update(
            {name: module for name, module in previous.items() if module is not None}
        )


def test_minio_sts_task_log_io_atomically_mirrors_full_local_log_without_truncation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _isolated_task_log_modules() as (task_logging, _workload_identity):
        client = _S3(task_logging.TASK_LOG_BUCKET)
        monkeypatch.setattr(task_logging, "task_log_s3_client", lambda: client)
        base_log_folder = tmp_path / "logs"
        local_log = base_log_folder / "dag_id=all-nine" / "task_id=materialize" / "attempt=1.log"
        local_log.parent.mkdir(parents=True)
        remote = task_logging.MinioStsTaskLogIO(base_log_folder=base_log_folder)

        local_log.write_text("first", encoding="utf-8")
        remote.upload(local_log)
        local_log.write_text("first\nsecond", encoding="utf-8")
        remote.upload(local_log)

        expected_key = "airflow-task-logs/dag_id=all-nine/task_id=materialize/attempt=1.log"
        assert client.objects == {expected_key: b"first\nsecond"}
        assert local_log.read_text(encoding="utf-8") == "first\nsecond"
        messages, logs = remote.read(
            "dag_id=all-nine/task_id=materialize/attempt=1.log", ti=object()
        )
        assert messages == [f"s3://{task_logging.TASK_LOG_BUCKET}/{expected_key}"]
        assert logs == ["first\nsecond"]


def test_minio_sts_task_log_io_preserves_local_log_when_remote_upload_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _isolated_task_log_modules() as (task_logging, _workload_identity):
        client = _S3(task_logging.TASK_LOG_BUCKET)
        client.fail_put = True
        monkeypatch.setattr(task_logging, "task_log_s3_client", lambda: client)
        base_log_folder = tmp_path / "logs"
        local_log = base_log_folder / "dag_id=d20" / "task_id=validate" / "attempt=1.log"
        local_log.parent.mkdir(parents=True)
        local_log.write_text("seed snapshot 17/43 written", encoding="utf-8")
        remote = task_logging.MinioStsTaskLogIO(base_log_folder=base_log_folder)

        remote.upload(local_log)

        assert local_log.read_text(encoding="utf-8") == "seed snapshot 17/43 written"
        assert client.objects == {}


def test_task_log_sts_policy_is_limited_to_the_log_prefix() -> None:
    with _isolated_task_log_modules() as (task_logging, workload_identity):
        policy = json.loads(
            workload_identity.build_minio_prefix_policy(
                bucket=task_logging.TASK_LOG_BUCKET,
                prefix=task_logging.TASK_LOG_PREFIX,
                object_actions=("s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"),
            )
        )

        assert policy["Version"] == "2012-10-17"
        assert policy["Statement"] == [
            {
                "Action": ["s3:GetBucketLocation"],
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::airflow-serp-artifacts"],
            },
            {
                "Action": ["s3:ListBucket"],
                "Condition": {
                    "StringLike": {"s3:prefix": ["airflow-task-logs", "airflow-task-logs/*"]}
                },
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::airflow-serp-artifacts"],
            },
            {
                "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::airflow-serp-artifacts/airflow-task-logs/*"],
            },
        ]


def test_multi_operation_evidence_reader_is_read_only_and_prefix_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        captured: dict[str, str] = {}
        sentinel = object()

        def capture_client(*, policy: str) -> object:
            captured["policy"] = policy
            return sentinel

        monkeypatch.setattr(workload_identity, "_web_identity_s3_client", capture_client)

        client = workload_identity.operation_prefix_read_s3_client(
            artifact_uris=(
                "s3://airflow-serp-evidence/serp-evals/d17-receipt/receipt.json",
                "s3://airflow-serp-evidence/serp-evals/ci-model-release-165/baseline.json",
                "s3://airflow-serp-evidence/serp-evals/ci-model-release-165/candidate.json",
            )
        )

    assert client is sentinel
    policy = json.loads(captured["policy"])
    assert policy["Statement"] == [
        {
            "Action": ["s3:GetObject", "s3:GetObjectRetention", "s3:GetObjectVersion"],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::airflow-serp-evidence/serp-evals/ci-model-release-165/*",
                "arn:aws:s3:::airflow-serp-evidence/serp-evals/d17-receipt/*",
            ],
        }
    ]


def test_task_log_sts_client_rejects_ambient_static_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "forbidden-static-value")

        with pytest.raises(ValueError, match="static MinIO credentials are forbidden"):
            workload_identity.task_log_s3_client()


def test_airflow_logging_module_exports_the_native_remote_log_object() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "dags" / "serp_minio_sts_task_logging.py"
    ).read_text(encoding="utf-8")

    assert (
        "from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG"
        in source
    )
    assert "LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)" in source
    assert "REMOTE_TASK_LOG = MinioStsTaskLogIO()" in source


def test_kubernetes_pod_launcher_executor_config_keeps_minio_sts_and_api_access_separate() -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        config = workload_identity.kubernetes_pod_launcher_executor_config()

    pod = config["pod_override"]
    assert pod.spec is not None
    assert pod.spec.service_account_name == "airflow-serp-kubernetes-pod-launcher"
    assert pod.spec.automount_service_account_token is True
    assert pod.metadata is not None
    assert pod.metadata.labels == {
        "adapstory.com/serp-evidence-workload": "true",
        "adapstory.com/serp-network-profile": "kubernetes-pod-launcher",
        "component": "worker",
        "release": "airflow",
        "tier": "airflow",
    }
    assert pod.spec.volumes is not None
    assert [volume.name for volume in pod.spec.volumes] == [
        "minio-web-identity-token",
        "serp-runtime-tmp",
        "serp-runtime-logs",
    ]
    _assert_hardened_runtime_pod(pod)


def test_evidence_executor_config_is_explicitly_hardened_and_writable_only_at_runtime_paths() -> (
    None
):
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        config = workload_identity.minio_web_identity_executor_config(
            service_account_name="airflow-serp-benchmark-evaluator",
            labels={"adapstory.com/serp-network-profile": "benchmark-evaluator"},
        )

    pod = config["pod_override"]
    assert pod.spec is not None
    assert pod.spec.automount_service_account_token is False
    assert pod.spec.service_account_name == "airflow-serp-benchmark-evaluator"
    assert pod.spec.volumes is not None
    assert [volume.name for volume in pod.spec.volumes] == [
        "minio-web-identity-token",
        "serp-runtime-tmp",
        "serp-runtime-logs",
    ]
    _assert_hardened_runtime_pod(pod)


def test_bc21_only_executor_has_no_minio_credentials_or_mount() -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        config = workload_identity.bc21_authorized_executor_config(
            service_account_name="airflow-serp-official-measurement-publisher",
            labels={"adapstory.com/serp-network-profile": "benchmark-aggregator"},
        )

    pod = config["pod_override"]
    assert pod.spec is not None
    assert pod.spec.automount_service_account_token is False
    assert pod.spec.service_account_name == "airflow-serp-official-measurement-publisher"
    assert pod.spec.volumes is not None
    assert [volume.name for volume in pod.spec.volumes] == [
        "bc21-workload-token",
        "serp-runtime-tmp",
        "serp-runtime-logs",
    ]
    assert pod.spec.containers is not None
    container = pod.spec.containers[0]
    assert [(item.name, item.value) for item in container.env] == [
        (
            "ADAPSTORY_SERP_SERVICE_ACCOUNT_TOKEN_PATH",
            "/var/run/secrets/adapstory/bc21-workload/token",
        )
    ]
    assert [
        (mount.name, mount.mount_path, mount.read_only) for mount in container.volume_mounts
    ] == [
        ("bc21-workload-token", "/var/run/secrets/adapstory/bc21-workload", True),
        ("serp-runtime-tmp", "/tmp", False),
        ("serp-runtime-logs", "/opt/airflow/logs", False),
    ]
    _assert_hardened_runtime_pod(
        pod,
        credential_mount=(
            "bc21-workload-token",
            "/var/run/secrets/adapstory/bc21-workload",
            True,
        ),
    )


def _assert_hardened_runtime_pod(
    pod: Any,
    *,
    credential_mount: tuple[str, str, bool] = (
        "minio-web-identity-token",
        "/var/run/secrets/adapstory/minio-web-identity",
        True,
    ),
) -> None:
    assert pod.spec.security_context is not None
    assert pod.spec.security_context.run_as_non_root is True
    assert pod.spec.security_context.run_as_user == 50000
    assert pod.spec.security_context.run_as_group == 50000
    assert pod.spec.security_context.fs_group == 50000
    assert pod.spec.security_context.seccomp_profile is not None
    assert pod.spec.security_context.seccomp_profile.type == "RuntimeDefault"
    assert pod.spec.volumes is not None
    writable_volumes = {volume.name: volume.empty_dir for volume in pod.spec.volumes}
    assert writable_volumes["serp-runtime-tmp"].size_limit == "2Gi"
    assert writable_volumes["serp-runtime-logs"].size_limit == "1Gi"

    assert pod.spec.containers is not None
    container = pod.spec.containers[0]
    assert container.security_context is not None
    assert container.security_context.allow_privilege_escalation is False
    assert container.security_context.read_only_root_filesystem is True
    assert container.security_context.run_as_non_root is True
    assert container.security_context.run_as_user == 50000
    assert container.security_context.run_as_group == 50000
    assert container.security_context.capabilities is not None
    assert container.security_context.capabilities.drop == ["ALL"]
    assert container.volume_mounts is not None
    assert [
        (mount.name, mount.mount_path, mount.read_only) for mount in container.volume_mounts
    ] == [
        credential_mount,
        ("serp-runtime-tmp", "/tmp", False),
        ("serp-runtime-logs", "/opt/airflow/logs", False),
    ]
