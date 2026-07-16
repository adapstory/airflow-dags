"""MinIO STS remote-log implementation independent from Airflow configuration loading."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any

from dags.serp_evidence_workload_identity import (
    TASK_LOG_BUCKET as _TASK_LOG_BUCKET,
)
from dags.serp_evidence_workload_identity import (
    TASK_LOG_PREFIX as _TASK_LOG_PREFIX,
)
from dags.serp_evidence_workload_identity import (
    task_log_s3_client,
)

_LOG = logging.getLogger(__name__)
_DEFAULT_BASE_LOG_FOLDER = "/opt/airflow/logs"
TASK_LOG_BUCKET = _TASK_LOG_BUCKET
TASK_LOG_PREFIX = _TASK_LOG_PREFIX


class MinioStsTaskLogIO:
    """Append and read task logs through per-operation MinIO STS sessions."""

    processors: tuple[()] = ()

    def __init__(
        self,
        *,
        base_log_folder: Path | None = None,
        delete_local_copy: bool = False,
    ) -> None:
        self.base_log_folder = base_log_folder or Path(
            os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER", _DEFAULT_BASE_LOG_FOLDER)
        )
        self.delete_local_copy = delete_local_copy
        self.remote_base = f"s3://{TASK_LOG_BUCKET}/{TASK_LOG_PREFIX.removesuffix('/')}"

    def upload(self, path: os.PathLike[str] | str, ti: object | None = None) -> None:
        """Atomically mirror the complete local log without truncating crash evidence.

        Replacing the remote object with the complete local file is idempotent:
        a process crash after ``PutObject`` can safely retry without duplicating
        the last fragment. The local copy remains available until an explicitly
        configured successful final cleanup.
        """

        del ti
        local_path, relative_path = self._resolve_local_log_path(path)
        if not local_path.is_file():
            return
        if self.write(
            local_path.read_text(encoding="utf-8"),
            self._remote_key(relative_path),
            append=False,
        ):
            if self.delete_local_copy:
                shutil.rmtree(local_path.parent)

    def read(self, relative_path: str, ti: object) -> tuple[list[str], list[str] | None]:
        """Return the source keys and log payloads expected by Airflow's log API."""

        del ti
        prefix = self._remote_key(Path(relative_path))
        client = task_log_s3_client()
        keys = self._list_keys(client=client, prefix=prefix)
        if not keys:
            return [], None
        messages = [f"s3://{TASK_LOG_BUCKET}/{key}" for key in keys]
        logs: list[str] = []
        for key in keys:
            try:
                payload = self._read_object(client=client, key=key)
            except Exception as exc:  # pragma: no cover - service failure path
                message = f"Could not read logs from s3://{TASK_LOG_BUCKET}/{key}: {exc}"
                _LOG.exception(message)
                logs.append(message)
                continue
            if payload is not None:
                logs.append(payload)
        return messages, logs or None

    def write(self, log: str, remote_key: str, *, append: bool = True) -> bool:
        """Append a log fragment without granting delete or bucket-wide object access."""

        client = task_log_s3_client()
        try:
            old_log = self._read_object(client=client, key=remote_key) if append else None
            if old_log:
                separator = "" if old_log.endswith("\n") else "\n"
                log = f"{old_log}{separator}{log}"
            client.put_object(Bucket=TASK_LOG_BUCKET, Key=remote_key, Body=log.encode("utf-8"))
        except Exception:  # pragma: no cover - provider failures are logged for Airflow
            _LOG.exception("Could not write task logs to s3://%s/%s", TASK_LOG_BUCKET, remote_key)
            return False
        return True

    def _resolve_local_log_path(self, path: os.PathLike[str] | str) -> tuple[Path, Path]:
        candidate = Path(path)
        if candidate.is_absolute():
            return candidate, candidate.relative_to(self.base_log_folder)
        return self.base_log_folder / candidate, candidate

    @staticmethod
    def _remote_key(relative_path: Path) -> str:
        normalized = relative_path.as_posix().lstrip("/")
        if not normalized or normalized.startswith("../") or "/../" in normalized:
            raise ValueError("Airflow task log path escapes the task-log prefix")
        return f"{TASK_LOG_PREFIX}{normalized}"

    @staticmethod
    def _read_object(*, client: Any, key: str) -> str | None:
        try:
            body = client.get_object(Bucket=TASK_LOG_BUCKET, Key=key)["Body"]
        except Exception as exc:
            if _is_missing_object(exc):
                return None
            raise
        payload = body.read()
        return payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)

    @staticmethod
    def _list_keys(*, client: Any, prefix: str) -> list[str]:
        keys: list[str] = []
        continuation_token: str | None = None
        while True:
            request: dict[str, str] = {"Bucket": TASK_LOG_BUCKET, "Prefix": prefix}
            if continuation_token:
                request["ContinuationToken"] = continuation_token
            response = client.list_objects_v2(**request)
            keys.extend(
                entry["Key"]
                for entry in response.get("Contents", [])
                if isinstance(entry.get("Key"), str) and entry["Key"].startswith(prefix)
            )
            if not response.get("IsTruncated"):
                return sorted(set(keys))
            continuation_token = response.get("NextContinuationToken")
            if not isinstance(continuation_token, str) or not continuation_token:
                raise RuntimeError("MinIO task-log listing omitted its continuation token")


def _is_missing_object(error: Exception) -> bool:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return False
    details = response.get("Error")
    if not isinstance(details, dict):
        return False
    return str(details.get("Code", "")).strip() in {"404", "NoSuchKey", "NoSuchObject"}
