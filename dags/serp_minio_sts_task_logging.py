"""Airflow logging configuration using projected MinIO web identity."""

from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path
from typing import Any

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.utils.log.file_task_handler import FileTaskHandler

from dags.serp_minio_sts_task_log_io import MinioStsTaskLogIO


class MinioStsTaskHandler(FileTaskHandler):  # type: ignore[misc]
    """Persist a task's complete local log through projected MinIO STS on close."""

    def __init__(
        self,
        base_log_folder: str,
        remote_base_log_folder: str,
        max_bytes: int = 0,
        backup_count: int = 0,
        delay: bool = False,
    ) -> None:
        super().__init__(
            base_log_folder=base_log_folder,
            max_bytes=max_bytes,
            backup_count=backup_count,
            delay=delay,
        )
        self.io = MinioStsTaskLogIO(base_log_folder=Path(base_log_folder))
        if remote_base_log_folder.rstrip("/") != self.io.remote_base:
            raise ValueError("MinIO task-log handler remote prefix is unsupported")
        self.log_relative_path = ""
        self.upload_on_close = False
        self.closed = False

    def set_context(self, ti: Any, *, identifier: str | None = None) -> None:
        super().set_context(ti, identifier=identifier)
        if self.handler is None:
            raise RuntimeError("task log handler did not create its local file")
        self.ti = ti
        self.log_relative_path = (
            Path(self.handler.baseFilename).relative_to(self.local_base).as_posix()
        )
        self.upload_on_close = not bool(getattr(ti, "raw", False))
        if self.upload_on_close:
            Path(self.handler.baseFilename).write_text("", encoding="utf-8")

    def _read(
        self,
        ti: Any,
        try_number: int,
        metadata: Any = None,
    ) -> tuple[Any, Any]:
        stream, output_metadata = super()._read(ti, try_number, metadata)
        if isinstance(stream, Iterator):
            # Airflow 3.3.0 turns a paged log stream into itertools.islice, but
            # FileTaskHandler.read accepts only chain or GeneratorType streams.
            # Adapt at our handler boundary without rereading or materializing
            # the potentially growing task log.
            stream = (message for message in stream)
        return stream, output_metadata

    def close(self) -> None:
        if self.closed:
            return
        super().close()
        if self.upload_on_close and hasattr(self, "ti"):
            self.io.upload(self.log_relative_path, self.ti)
        self.closed = True


LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
REMOTE_TASK_LOG = MinioStsTaskLogIO()
# Context: Airflow 3's REMOTE_TASK_LOG is a read interface; FileTaskHandler
# never calls its upload method. Decision: make the task handler own the local
# write plus close-time STS upload lifecycle. Reason: KubernetesExecutor pods
# are ephemeral even when the task SDK reports an application failure.
# Revisit when: Airflow exposes a native RemoteLogIO write lifecycle.
LOGGING_CONFIG["handlers"]["task"] = {
    **LOGGING_CONFIG["handlers"]["task"],
    "class": "dags.serp_minio_sts_task_logging.MinioStsTaskHandler",
    "remote_base_log_folder": REMOTE_TASK_LOG.remote_base,
}
