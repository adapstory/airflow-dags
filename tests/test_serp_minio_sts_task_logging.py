from __future__ import annotations

import importlib
import io
import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import GeneratorType, ModuleType
from typing import Any, ClassVar, cast

import pytest

from dags.serp_kubernetes_executor import task_secret_env_var

pytestmark = pytest.mark.unit


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


@contextmanager
def _isolated_airflow_logging_module() -> Iterator[tuple[ModuleType, ModuleType]]:
    module_names = (
        "airflow",
        "airflow.config_templates",
        "airflow.config_templates.airflow_local_settings",
        "airflow.utils",
        "airflow.utils.log",
        "airflow.utils.log.file_task_handler",
        "dags.serp_minio_sts_task_logging",
    )
    previous = {name: sys.modules.pop(name, None) for name in module_names}

    class FakeFileTaskHandler(logging.Handler):
        def __init__(
            self,
            base_log_folder: str,
            max_bytes: int = 0,
            backup_count: int = 0,
            delay: bool = False,
        ) -> None:
            super().__init__()
            del max_bytes, backup_count, delay
            self.local_base = base_log_folder
            self.handler: logging.Handler | None = None
            self.read_result: tuple[object, dict[str, object]] = ([], {})

        def _read(
            self,
            _task_instance: object,
            _try_number: int,
            _metadata: dict[str, object] | None = None,
        ) -> tuple[object, dict[str, object]]:
            return self.read_result

        def read(
            self,
            task_instance: object,
            try_number: int,
            metadata: dict[str, object] | None = None,
        ) -> tuple[object, dict[str, object]]:
            stream, output_metadata = self._read(task_instance, try_number, metadata)
            if isinstance(stream, GeneratorType):
                return stream, output_metadata
            raise TypeError(f"Invalid log stream type: {type(stream).__name__}")

        def emit(self, record: logging.LogRecord) -> None:
            if self.handler is None:
                raise RuntimeError("task log handler has no local file")
            self.handler.emit(record)

        def close(self) -> None:
            if self.handler is not None:
                self.handler.close()
            super().close()

    airflow = ModuleType("airflow")
    config_templates = ModuleType("airflow.config_templates")
    local_settings = ModuleType("airflow.config_templates.airflow_local_settings")
    cast(Any, local_settings).DEFAULT_LOGGING_CONFIG = {
        "handlers": {
            "task": {
                "class": "airflow.utils.log.file_task_handler.FileTaskHandler",
                "base_log_folder": "/opt/airflow/logs",
            }
        }
    }
    airflow_utils = ModuleType("airflow.utils")
    airflow_log = ModuleType("airflow.utils.log")
    file_task_handler = ModuleType("airflow.utils.log.file_task_handler")
    cast(Any, file_task_handler).FileTaskHandler = FakeFileTaskHandler
    sys.modules.update(
        {
            "airflow": airflow,
            "airflow.config_templates": config_templates,
            "airflow.config_templates.airflow_local_settings": local_settings,
            "airflow.utils": airflow_utils,
            "airflow.utils.log": airflow_log,
            "airflow.utils.log.file_task_handler": file_task_handler,
        }
    )
    try:
        logging_module = importlib.import_module("dags.serp_minio_sts_task_logging")
        task_log_io = importlib.import_module("dags.serp_minio_sts_task_log_io")
        yield logging_module, task_log_io
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


def test_evidence_graph_reader_is_read_only_across_discovered_operation_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        captured: dict[str, str] = {}
        sentinel = object()

        def capture_client(*, policy: str) -> object:
            captured["policy"] = policy
            return sentinel

        monkeypatch.setattr(workload_identity, "_web_identity_s3_client", capture_client)

        client = workload_identity.evidence_graph_read_s3_client()

    assert client is sentinel
    policy = json.loads(captured["policy"])
    assert policy["Statement"] == [
        {
            "Action": ["s3:GetObject", "s3:GetObjectRetention", "s3:GetObjectVersion"],
            "Effect": "Allow",
            "Resource": ["arn:aws:s3:::airflow-serp-evidence/serp-evals/*"],
        }
    ]
    assert "s3:PutObject" not in captured["policy"]
    assert "s3:ListBucket" not in captured["policy"]
    assert "s3:DeleteObject" not in captured["policy"]


def test_task_log_sts_client_rejects_ambient_static_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "forbidden-static-value")

        with pytest.raises(ValueError, match="static MinIO credentials are forbidden"):
            workload_identity.task_log_s3_client()


def test_minio_sts_retries_transport_timeout_with_bounded_request_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        calls: list[float] = []
        sleeps: list[float] = []
        response_body = b"""\
<AssumeRoleWithWebIdentityResponse>
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>access</AccessKeyId>
      <SecretAccessKey>secret</SecretAccessKey>
      <SessionToken>session</SessionToken>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>
"""

        class Response(io.BytesIO):
            def __enter__(self) -> Response:
                return self

            def __exit__(self, *_args: object) -> None:
                self.close()

        def flaky_urlopen(_request: object, *, timeout: float) -> Response:
            calls.append(timeout)
            if len(calls) == 1:
                raise TimeoutError("injected slow MinIO IAM refresh")
            return Response(response_body)

        monkeypatch.setattr(workload_identity, "urlopen", flaky_urlopen)
        monkeypatch.setattr(workload_identity, "sleep", sleeps.append)

        credentials = workload_identity._assume_minio_role_with_web_identity(
            endpoint_url="http://minio.env-prod.svc.cluster.local:9000",
            token="projected-token",
            policy="{}",
        )

    assert credentials == {
        "AccessKeyId": "access",
        "SecretAccessKey": "secret",
        "SessionToken": "session",
    }
    assert calls == [30.0, 30.0]
    assert sleeps == [1.0]


def test_minio_sts_does_not_retry_http_contract_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        calls = 0
        sleeps: list[float] = []

        def denied_urlopen(_request: object, *, timeout: float) -> object:
            nonlocal calls
            calls += 1
            assert timeout == 30.0
            raise workload_identity.HTTPError(
                "http://minio.env-prod.svc.cluster.local:9000",
                403,
                "AccessDenied",
                {},
                None,
            )

        monkeypatch.setattr(workload_identity, "urlopen", denied_urlopen)
        monkeypatch.setattr(workload_identity, "sleep", sleeps.append)

        with pytest.raises(ValueError, match="MinIO web-identity STS exchange failed"):
            workload_identity._assume_minio_role_with_web_identity(
                endpoint_url="http://minio.env-prod.svc.cluster.local:9000",
                token="projected-token",
                policy="{}",
            )

    assert calls == 1
    assert sleeps == []


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
    assert 'LOGGING_CONFIG["handlers"]["task"]' in source
    assert "MinioStsTaskHandler" in source


def test_minio_sts_task_handler_uploads_complete_log_on_close(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _isolated_airflow_logging_module() as (logging_module, task_logging):
        client = _S3(task_logging.TASK_LOG_BUCKET)
        monkeypatch.setattr(task_logging, "task_log_s3_client", lambda: client)
        base_log_folder = tmp_path / "logs"
        relative_path = Path("dag_id=d17/task_id=verify/attempt=1.log")
        local_log = base_log_folder / relative_path
        local_log.parent.mkdir(parents=True)
        local_log.write_text("root cause survives pod deletion", encoding="utf-8")

        handler = logging_module.MinioStsTaskHandler(
            base_log_folder=str(base_log_folder),
            remote_base_log_folder="s3://airflow-serp-artifacts/airflow-task-logs",
        )
        handler.handler = logging.FileHandler(local_log)
        handler.log_relative_path = relative_path.as_posix()
        handler.ti = object()
        handler.upload_on_close = True
        handler.close()

        assert client.objects == {
            "airflow-task-logs/dag_id=d17/task_id=verify/attempt=1.log": (
                b"root cause survives pod deletion"
            )
        }


def test_minio_sts_task_handler_periodically_mirrors_before_close(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with _isolated_airflow_logging_module() as (logging_module, task_logging):
        client = _S3(task_logging.TASK_LOG_BUCKET)
        monkeypatch.setattr(task_logging, "task_log_s3_client", lambda: client)
        now = [100.0]
        monkeypatch.setattr(logging_module, "monotonic", lambda: now[0])
        base_log_folder = tmp_path / "logs"
        relative_path = Path("dag_id=d19/task_id=pack/attempt=4.log")
        local_log = base_log_folder / relative_path
        local_log.parent.mkdir(parents=True)

        handler = logging_module.MinioStsTaskHandler(
            base_log_folder=str(base_log_folder),
            remote_base_log_folder="s3://airflow-serp-artifacts/airflow-task-logs",
            flush_interval_seconds=5.0,
        )
        handler.handler = logging.FileHandler(local_log)
        handler.log_relative_path = relative_path.as_posix()
        handler.ti = object()
        handler.upload_on_close = True

        handler.emit(logging.makeLogRecord({"msg": "checkpoint restored"}))
        assert client.objects == {}
        now[0] += 5.0
        handler.emit(logging.makeLogRecord({"msg": "provider outcome unknown"}))

        assert client.objects == {
            "airflow-task-logs/dag_id=d19/task_id=pack/attempt=4.log": (
                b"checkpoint restored\nprovider outcome unknown\n"
            )
        }
        # Deliberately do not call close(): this assertion models a worker that
        # is killed after the periodic durable snapshot.


def test_minio_sts_task_handler_adapts_airflow_paged_islice_to_supported_generator(
    tmp_path: Path,
) -> None:
    from itertools import islice

    with _isolated_airflow_logging_module() as (logging_module, _task_logging):
        handler = logging_module.MinioStsTaskHandler(
            base_log_folder=str(tmp_path / "logs"),
            remote_base_log_folder="s3://airflow-serp-artifacts/airflow-task-logs",
        )
        handler.read_result = (
            islice(iter(["already-returned", "next-page"]), 1, None),
            {"end_of_log": False, "log_pos": 2},
        )

        stream, metadata = handler.read(object(), 1, {"log_pos": 1})

        assert isinstance(stream, GeneratorType)
        assert list(stream) == ["next-page"]
        assert metadata == {"end_of_log": False, "log_pos": 2}


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


def test_kubernetes_pod_launcher_executor_config_cannot_target_remote_nodes() -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        config = workload_identity.kubernetes_pod_launcher_executor_config()

    pod = config["pod_override"]
    assert pod.spec is not None
    assert pod.spec.node_selector is None
    assert pod.spec.tolerations is None


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


def test_evidence_executor_config_can_add_one_task_scoped_secret_without_losing_identity() -> None:
    with _isolated_task_log_modules() as (_task_logging, workload_identity):
        config = workload_identity.minio_web_identity_executor_config(
            service_account_name="airflow-serp-public-docs-acquisition",
            labels={"adapstory.com/serp-network-profile": "public-docs-acquisition"},
            additional_env_vars=[
                task_secret_env_var(
                    name="ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN",
                    secret_name="airflow-serp-github-status",
                    secret_key="token",
                )
            ],
        )

    pod = config["pod_override"]
    assert pod.spec is not None
    assert pod.spec.service_account_name == "airflow-serp-public-docs-acquisition"
    assert pod.spec.automount_service_account_token is False
    assert pod.spec.containers is not None
    env = pod.spec.containers[0].env
    assert env is not None
    assert [item.name for item in env] == [
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE",
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS",
        "ADAPSTORY_SERP_CONTEXT_BENCHMARK_GITHUB_TOKEN",
    ]
    secret = env[-1].value_from.secret_key_ref
    assert secret.name == "airflow-serp-github-status"
    assert secret.key == "token"
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
