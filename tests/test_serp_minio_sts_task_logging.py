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
        self.objects: dict[str, bytes] = {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        assert Bucket == self.bucket
        if Key not in self.objects:
            raise _MissingObjectError()
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:
        assert Bucket == self.bucket
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


def test_minio_sts_task_log_io_appends_and_reads_without_an_airflow_connection(
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
        local_log.write_text("second", encoding="utf-8")
        remote.upload(local_log)

        expected_key = "airflow-task-logs/dag_id=all-nine/task_id=materialize/attempt=1.log"
        assert client.objects == {expected_key: b"first\nsecond"}
        assert local_log.read_text(encoding="utf-8") == ""
        messages, logs = remote.read(
            "dag_id=all-nine/task_id=materialize/attempt=1.log", ti=object()
        )
        assert messages == [f"s3://{task_logging.TASK_LOG_BUCKET}/{expected_key}"]
        assert logs == ["first\nsecond"]


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
