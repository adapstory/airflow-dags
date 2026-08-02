"""File-backed runtime access to the verified benchmark substrate source set."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

BENCHMARK_SUBSTRATE_SOURCE_SET_CONFIG_MAP = "airflow-evaluation-runtime-contract"
BENCHMARK_SUBSTRATE_SOURCE_SET_CONFIG_MAP_KEY = (
    "ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE"
)
BENCHMARK_SUBSTRATE_SOURCE_SET_FILE_ENV_NAME = (
    "ADAPSTORY_SERP_BENCHMARK_SUBSTRATE_SOURCE_SET_EVIDENCE_FILE"
)
BENCHMARK_SUBSTRATE_SOURCE_SET_VOLUME_NAME = "benchmark-substrate-source-set"
BENCHMARK_SUBSTRATE_SOURCE_SET_MOUNT_PATH = "/var/run/adapstory/evaluation-runtime"
BENCHMARK_SUBSTRATE_SOURCE_SET_FILE_PATH = (
    f"{BENCHMARK_SUBSTRATE_SOURCE_SET_MOUNT_PATH}/source-set-evidence.json"
)
_MAX_CONFIG_MAP_VALUE_BYTES = 1024 * 1024


def read_benchmark_substrate_source_set_evidence(
    environment: Mapping[str, str] = os.environ,
) -> str:
    """Read the optional ConfigMap projection without copying it into ``execve`` env."""

    configured_path = environment.get(BENCHMARK_SUBSTRATE_SOURCE_SET_FILE_ENV_NAME, "").strip()
    if not configured_path:
        return ""
    path = Path(configured_path)
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        # The ConfigMap item is optional until the first verified supply is promoted.
        return ""
    if len(payload) > _MAX_CONFIG_MAP_VALUE_BYTES:
        raise ValueError("benchmark substrate source set runtime contract exceeds 1 MiB")
    try:
        return payload.decode("utf-8").strip()
    except UnicodeDecodeError as error:
        raise ValueError("benchmark substrate source set runtime contract must be UTF-8") from error
