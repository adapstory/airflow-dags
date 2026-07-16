from __future__ import annotations

import json
import os
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote, unquote

import pytest

import dags.serp_public_docs_seed_refresh_remote_runner as remote_runner


def test_remote_runner_materializes_exact_refresh_plan_version_before_delegating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_payload = {
        "operation_id": "serp-web-seed-crawl-refresh-1234",
        "status": "ready_for_pipeline_dispatch",
    }
    payload_bytes = json.dumps(refresh_payload, separators=(",", ":"), sort_keys=True).encode()
    evidence = {
        "s3Uri": "s3://airflow-serp-evidence/serp-public-docs/op/refresh-plan.json",
        "sha256": "sha256:" + sha256(payload_bytes).hexdigest(),
        "versionId": "refresh-version-7",
    }
    spec = {
        "argv": ["python", "-m", "pipeline", "--refresh-plan", evidence["s3Uri"]],
        "input_paths": [evidence["s3Uri"]],
        "operation_id": refresh_payload["operation_id"],
        "refresh_plan_evidence": evidence,
        "status": "ready_for_pipeline_cli_runner",
    }
    monkeypatch.setenv(
        remote_runner.PIPELINE_CLI_SPEC_ENV,
        quote(json.dumps(spec, separators=(",", ":"), sort_keys=True), safe=""),
    )
    observed: dict[str, object] = {}

    def read_exact(
        normalized_evidence: dict[str, str],
        *,
        field_name: str,
        s3_client: object,
        max_bytes: int,
    ) -> bytes:
        assert normalized_evidence == evidence
        assert field_name == "public docs seed refresh plan evidence"
        assert s3_client is sentinel_client
        assert max_bytes == remote_runner.PUBLIC_DOCS_MAX_REFRESH_PLAN_BYTES
        return payload_bytes

    def delegated_main() -> int:
        delegated = json.loads(unquote(os.environ[remote_runner.PIPELINE_CLI_SPEC_ENV]))
        local_path = Path(delegated["argv"][-1])
        assert delegated["argv"][-2] == "--refresh-plan"
        assert delegated["input_paths"] == [str(local_path)]
        assert json.loads(local_path.read_bytes()) == refresh_payload
        observed["delegated"] = delegated
        return 0

    sentinel_client = object()
    monkeypatch.setattr(remote_runner, "_s3_client", lambda _uri: sentinel_client)
    monkeypatch.setattr(remote_runner, "_read_public_docs_exact_evidence_bytes", read_exact)
    monkeypatch.setattr(
        remote_runner.importlib,
        "import_module",
        lambda name: SimpleNamespace(main=delegated_main)
        if name == remote_runner.PIPELINE_REMOTE_RUNNER_MODULE
        else pytest.fail(f"unexpected import: {name}"),
    )

    assert remote_runner.main() == 0
    assert observed["delegated"]
    assert os.environ[remote_runner.PIPELINE_CLI_SPEC_ENV] == quote(
        json.dumps(spec, separators=(",", ":"), sort_keys=True), safe=""
    )


def test_remote_runner_rejects_missing_exact_refresh_plan_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        remote_runner.PIPELINE_CLI_SPEC_ENV,
        quote(
            json.dumps(
                {
                    "argv": [],
                    "input_paths": [],
                    "operation_id": "serp-web-seed-crawl-refresh-1234",
                    "status": "no_due_sources",
                }
            ),
            safe="",
        ),
    )

    with pytest.raises(ValueError, match="refresh_plan_evidence"):
        remote_runner.main()


def test_remote_runner_restores_original_spec_when_pipeline_delegate_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload_bytes = json.dumps(
        {
            "operation_id": "serp-web-seed-crawl-refresh-restore",
            "status": "ready_for_pipeline_dispatch",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    evidence = {
        "s3Uri": "s3://airflow-serp-evidence/serp-public-docs/restore/refresh-plan.json",
        "sha256": "sha256:" + sha256(payload_bytes).hexdigest(),
        "versionId": "refresh-version-restore",
    }
    spec = {
        "argv": ["python", "-m", "pipeline", "--refresh-plan", evidence["s3Uri"]],
        "input_paths": [evidence["s3Uri"]],
        "operation_id": "serp-web-seed-crawl-refresh-restore",
        "refresh_plan_evidence": evidence,
        "status": "ready_for_pipeline_cli_runner",
    }
    encoded_spec = quote(json.dumps(spec, separators=(",", ":"), sort_keys=True), safe="")
    monkeypatch.setenv(remote_runner.PIPELINE_CLI_SPEC_ENV, encoded_spec)
    monkeypatch.setattr(remote_runner, "_s3_client", lambda _uri: object())
    monkeypatch.setattr(
        remote_runner,
        "_read_public_docs_exact_evidence_bytes",
        lambda *_args, **_kwargs: payload_bytes,
    )

    def fail_delegate() -> int:
        raise RuntimeError("delegate failed")

    monkeypatch.setattr(
        remote_runner.importlib,
        "import_module",
        lambda _name: SimpleNamespace(main=fail_delegate),
    )

    with pytest.raises(RuntimeError, match="delegate failed"):
        remote_runner.main()

    assert os.environ[remote_runner.PIPELINE_CLI_SPEC_ENV] == encoded_spec
