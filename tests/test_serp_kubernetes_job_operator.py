from __future__ import annotations

from pathlib import Path


def test_remote_xcom_sidecar_admission_contract_is_fail_closed() -> None:
    source = (
        Path(__file__).parents[1] / "dags" / "serp_kubernetes_job_operator.py"
    ).read_text(encoding="utf-8")

    assert "self._harden_remote_xcom_sidecar(job_request_obj)" in source
    assert '_REMOTE_COMPUTE_SELECTOR_KEY = "adapstory.com/compute-class"' in source
    assert '_XCOM_EPHEMERAL_STORAGE_REQUEST = "32Mi"' in source
    assert '_XCOM_EPHEMERAL_STORAGE_LIMIT = "128Mi"' in source
    assert 'security_context.capabilities = k8s.V1Capabilities(drop=["ALL"])' in source
    assert "security_context.allow_privilege_escalation = False" in source
