from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path

_AIRFLOW_PACKAGE = "adapstory-airflow-dags"
_PIPELINE_PACKAGE = "adapstory-serp-pipeline"
_RELEASE_VERSION = "2026.07.3"


def test_airflow_release_and_pinned_pipeline_are_locked_together() -> None:
    repository = Path(__file__).resolve().parents[1]
    project = tomllib.loads((repository / "pyproject.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((repository / "uv.lock").read_text(encoding="utf-8"))
    locked_packages = {package["name"]: package for package in lock["package"]}

    assert project["project"]["version"] == _RELEASE_VERSION
    assert f"{_PIPELINE_PACKAGE}=={_RELEASE_VERSION}" in project["project"]["dependencies"]
    assert locked_packages[_AIRFLOW_PACKAGE]["version"] == "2026.7.3"
    assert locked_packages[_PIPELINE_PACKAGE]["version"] == "2026.7.3"
    assert version(_AIRFLOW_PACKAGE) == "2026.7.3"
    assert version(_PIPELINE_PACKAGE) == "2026.7.3"
