from __future__ import annotations

import ast
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit


def test_remote_xcom_sidecar_admission_contract_is_fail_closed() -> None:
    source = (Path(__file__).parents[1] / "dags" / "serp_kubernetes_job_operator.py").read_text(
        encoding="utf-8"
    )

    assert "self._harden_remote_xcom_sidecar(job_request_obj)" in source
    assert '_REMOTE_COMPUTE_SELECTOR_KEY = "adapstory.com/compute-class"' in source
    assert '_XCOM_EPHEMERAL_STORAGE_REQUEST = "32Mi"' in source
    assert '_XCOM_EPHEMERAL_STORAGE_LIMIT = "128Mi"' in source
    assert 'security_context.capabilities = k8s.V1Capabilities(drop=["ALL"])' in source
    assert "security_context.allow_privilege_escalation = False" in source


def test_job_container_argv_is_normalized_after_native_template_rendering() -> None:
    source = (Path(__file__).parents[1] / "dags" / "serp_kubernetes_job_operator.py").read_text(
        encoding="utf-8"
    )

    assert "self._normalize_container_process_argv(job_request_obj)" in source
    assert "container.command = cls._normalize_argv(" in source
    assert "container.args = cls._normalize_argv(" in source
    assert "return [str(value) for value in values]" in source

    tree = ast.parse(source)
    operator = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "BoundedKubernetesJobOperator"
    )
    normalizer = next(
        node
        for node in operator.body
        if isinstance(node, ast.FunctionDef) and node.name == "_normalize_argv"
    )
    normalizer.decorator_list = []
    namespace: dict[str, Any] = {
        "AirflowException": RuntimeError,
        "Any": Any,
        "Sequence": Sequence,
    }
    exec(compile(ast.Module(body=[normalizer], type_ignores=[]), "<normalizer>", "exec"), namespace)

    normalize = namespace["_normalize_argv"]
    assert normalize([14, "16", True], container_name="worker", field_name="args") == [
        "14",
        "16",
        "True",
    ]
    with pytest.raises(RuntimeError, match="non-scalar value: dict"):
        normalize([{"unsafe": "value"}], container_name="worker", field_name="args")
