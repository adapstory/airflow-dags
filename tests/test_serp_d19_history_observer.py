from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from dags.serp_d19_history_observer import (
    AIRFLOW_HISTORY_OBSERVER_USERNAME,
    AirflowD19HistoryClient,
    HistoryClientConfig,
    KubernetesD19HistoryFenceClient,
    admit_d19_run,
    build_d19_history_query,
    seal_d19_history_observation_attestation,
)

PARENT_LOGICAL_DATE = "2026-07-17T00:00:00Z"
AIRFLOW_ORIGIN = "https://airflow-api-server.airflow.svc.cluster.local:8080"


def test_history_client_uses_one_short_lived_dedicated_jwt_and_complete_v2_pagination(
    tmp_path: Path,
) -> None:
    config = _history_config(tmp_path)
    jwt = _jwt(
        sub=AIRFLOW_HISTORY_OBSERVER_USERNAME,
        issued_at=1_768_435_200,
        expires_at=1_768_435_500,
    )
    calls: list[dict[str, Any]] = []
    responses: list[dict[str, Any]] = [
        {"access_token": jwt, "token_type": "bearer"},
        {"git_version": "airflow-3.1.6", "version": "3.1.6"},
        {
            "dag_runs": [_api_run(index) for index in range(1, 3)],
            "total_entries": 3,
        },
        {"dag_runs": [_api_run(3)], "total_entries": 3},
        {"dag_runs": [], "total_entries": 0},
        *[_xcom_response(index) for index in range(1, 4)],
    ]

    def transport(**request: Any) -> dict[str, Any]:
        calls.append(request)
        return responses.pop(0)

    result = AirflowD19HistoryClient(
        config,
        transport=transport,
        clock=lambda: datetime(2026, 1, 15, tzinfo=UTC),
    ).collect(parent_logical_date=PARENT_LOGICAL_DATE)

    assert result == {
        "activeRunQuery": {
            "dagId": "serp_benchmark_improvement_wave",
            "states": ["queued", "running"],
            "totalEntries": 0,
        },
        "api": {
            "apiVersion": "v2",
            "airflowVersion": "3.1.6",
            "serverAuthority": "airflow-api-server.airflow.svc.cluster.local:8080",
        },
        "acceptedRunVerifications": [_verification_pointer(index) for index in range(1, 4)],
        "pagination": {
            "complete": True,
            "observedEntries": 3,
            "pageCount": 2,
            "pageLimit": 2,
            "totalEntries": 3,
        },
        "query": {
            "apiPath": "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns",
            "dagId": "serp_benchmark_improvement_wave",
            "logicalDateLt": PARENT_LOGICAL_DATE,
            "orderBy": ["logical_date", "run_id"],
        },
        "runs": [_normalized_run(index) for index in range(1, 4)],
        "verificationPointerQuery": {
            "apiPathTemplate": (
                "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns/{dagRunId}/"
                "taskInstances/persist_paired_evaluation_verification_evidence/"
                "xcomEntries/return_value"
            ),
            "deserialize": True,
            "mapIndex": -1,
            "stringify": False,
            "taskId": "persist_paired_evaluation_verification_evidence",
            "xcomKey": "return_value",
        },
    }
    assert [urlparse(call["url"]).path for call in calls] == [
        "/auth/token",
        "/api/v2/version",
        "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns",
        "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns",
        "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns",
        (
            "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns/manual__d19-1/"
            "taskInstances/persist_paired_evaluation_verification_evidence/"
            "xcomEntries/return_value"
        ),
        (
            "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns/manual__d19-2/"
            "taskInstances/persist_paired_evaluation_verification_evidence/"
            "xcomEntries/return_value"
        ),
        (
            "/api/v2/dags/serp_benchmark_improvement_wave/dagRuns/manual__d19-3/"
            "taskInstances/persist_paired_evaluation_verification_evidence/"
            "xcomEntries/return_value"
        ),
    ]
    auth_form = parse_qs(calls[0]["body"].decode("utf-8"), strict_parsing=True)
    assert auth_form == {
        "password": ["observer-password-not-a-real-secret"],
        "username": [AIRFLOW_HISTORY_OBSERVER_USERNAME],
    }
    assert all(call["timeout"] == 7 for call in calls)
    assert all(call["ca_file"] == str(config.ca_file) for call in calls)
    for call in calls[-3:]:
        assert parse_qs(urlparse(call["url"]).query, strict_parsing=True) == {
            "deserialize": ["true"],
            "map_index": ["-1"],
            "stringify": ["false"],
        }
    bearer_headers = [call["headers"].get("Authorization") for call in calls[1:]]
    assert bearer_headers == [f"Bearer {jwt}"] * 7
    assert not responses


def test_history_client_never_reuses_a_jwt_across_observations(tmp_path: Path) -> None:
    config = _history_config(tmp_path)
    tokens = [
        _jwt(sub=AIRFLOW_HISTORY_OBSERVER_USERNAME, issued_at=100, expires_at=300),
        _jwt(sub=AIRFLOW_HISTORY_OBSERVER_USERNAME, issued_at=301, expires_at=501),
    ]
    auth_calls = 0

    def transport(**request: Any) -> dict[str, Any]:
        nonlocal auth_calls
        path = urlparse(request["url"]).path
        if path == "/auth/token":
            token = tokens[auth_calls]
            auth_calls += 1
            return {"access_token": token, "token_type": "bearer"}
        if path == "/api/v2/version":
            return {"git_version": "airflow-3.1.6", "version": "3.1.6"}
        if path.endswith("/xcomEntries/return_value"):
            run_id = path.split("/dagRuns/", 1)[1].split("/", 1)[0]
            return _xcom_response(int(run_id.rsplit("-", 1)[1]))
        query = parse_qs(urlparse(request["url"]).query)
        if "state" in query:
            return {"dag_runs": [], "total_entries": 0}
        return {
            "dag_runs": [_api_run(index) for index in range(1, 4)],
            "total_entries": 3,
        }

    client = AirflowD19HistoryClient(
        config,
        transport=transport,
        clock=lambda: datetime.fromtimestamp(150 if auth_calls == 0 else 350, tz=UTC),
    )
    client.collect(parent_logical_date=PARENT_LOGICAL_DATE)
    client.collect(parent_logical_date=PARENT_LOGICAL_DATE)

    assert auth_calls == 2


@pytest.mark.parametrize(
    ("username", "match"),
    (
        ("admin", "dedicated least-privilege principal"),
        ("airflow", "dedicated least-privilege principal"),
        ("serp-admin", "dedicated least-privilege principal"),
    ),
)
def test_history_client_rejects_admin_or_shared_username(
    tmp_path: Path,
    username: str,
    match: str,
) -> None:
    config = _history_config(tmp_path, username=username)

    with pytest.raises(ValueError, match=match):
        AirflowD19HistoryClient(config, transport=lambda **_: {})


@pytest.mark.parametrize(
    "raw_name",
    (
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_PASSWORD",
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_USERNAME",
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_TOKEN",
    ),
)
def test_history_client_from_environment_rejects_raw_credentials(
    tmp_path: Path,
    raw_name: str,
) -> None:
    environment = _history_environment(tmp_path)
    environment[raw_name] = "forbidden-raw-value"

    with pytest.raises(ValueError, match="raw history-observer credentials are forbidden"):
        AirflowD19HistoryClient.from_environment(environment=environment)


@pytest.mark.parametrize(
    "base_url",
    (
        "http://airflow-api-server.airflow.svc.cluster.local:8080",
        "https://airflow.example.com",
        "https://admin:password@airflow-api-server.airflow.svc.cluster.local:8080",
        "https://airflow-api-server.default.svc.cluster.local:8080",
    ),
)
def test_history_client_rejects_http_credentials_and_untrusted_origins(
    tmp_path: Path,
    base_url: str,
) -> None:
    config = _history_config(tmp_path, base_url=base_url)

    with pytest.raises(ValueError, match="trusted HTTPS origin"):
        AirflowD19HistoryClient(config, transport=lambda **_: {})


def test_history_client_rejects_long_lived_jwt(tmp_path: Path) -> None:
    config = _history_config(tmp_path)
    responses = [
        {
            "access_token": _jwt(
                sub=AIRFLOW_HISTORY_OBSERVER_USERNAME,
                issued_at=1_768_435_200,
                expires_at=1_768_435_501,
            ),
            "token_type": "bearer",
        }
    ]

    with pytest.raises(ValueError, match="JWT lifetime exceeds 300 seconds"):
        AirflowD19HistoryClient(
            config,
            transport=lambda **_: responses.pop(0),
            clock=lambda: datetime(2026, 1, 15, tzinfo=UTC),
        ).collect(parent_logical_date=PARENT_LOGICAL_DATE)


@pytest.mark.parametrize("case", ("truncated", "total_changed"))
def test_history_client_rejects_truncated_or_total_mismatched_pagination(
    tmp_path: Path,
    case: str,
) -> None:
    config = _history_config(tmp_path, page_limit=1)
    responses = (
        [
            {"dag_runs": [_api_run(1)], "total_entries": 2},
            {"dag_runs": [], "total_entries": 2},
        ]
        if case == "truncated"
        else [
            {"dag_runs": [_api_run(1)], "total_entries": 2},
            {"dag_runs": [_api_run(2)], "total_entries": 3},
        ]
    )
    scripted: list[dict[str, Any]] = [
        {
            "access_token": _jwt(
                sub=AIRFLOW_HISTORY_OBSERVER_USERNAME,
                issued_at=1_768_435_200,
                expires_at=1_768_435_500,
            ),
            "token_type": "bearer",
        },
        {"git_version": "airflow-3.1.6", "version": "3.1.6"},
        *responses,
    ]

    with pytest.raises(ValueError, match="complete pagination"):
        AirflowD19HistoryClient(
            config,
            transport=lambda **_: scripted.pop(0),
            clock=lambda: datetime(2026, 1, 15, tzinfo=UTC),
        ).collect(parent_logical_date=PARENT_LOGICAL_DATE)


def test_history_query_is_exact_and_cannot_be_broadened() -> None:
    query = build_d19_history_query(
        parent_logical_date=PARENT_LOGICAL_DATE,
        offset=0,
        limit=100,
    )

    assert query == {
        "limit": 100,
        "logical_date_lt": PARENT_LOGICAL_DATE,
        "offset": 0,
        "order_by": ["logical_date", "run_id"],
    }
    with pytest.raises(ValueError, match="page limit"):
        build_d19_history_query(
            parent_logical_date=PARENT_LOGICAL_DATE,
            offset=0,
            limit=501,
        )
    with pytest.raises(ValueError, match="non-negative"):
        build_d19_history_query(
            parent_logical_date=PARENT_LOGICAL_DATE,
            offset=-1,
            limit=100,
        )


@pytest.mark.parametrize(
    ("mutation", "match"),
    (
        ("fewer_than_three", "at least three consecutive accepted D19 runs"),
        ("intervening_failure", "last three D19 runs must all succeed"),
        ("logical_date_tie", "strictly increasing logical dates"),
        ("missing_pointer", "verification pointer XCom"),
        ("replayed_request", "requestId values must be unique"),
    ),
)
def test_history_client_fails_closed_on_non_contiguous_or_replayed_streak(
    tmp_path: Path,
    mutation: str,
    match: str,
) -> None:
    runs = [_api_run(index) for index in range(1, 4)]
    if mutation == "fewer_than_three":
        runs.pop()
    elif mutation == "intervening_failure":
        runs[-2]["state"] = "failed"
    elif mutation == "logical_date_tie":
        runs[-1]["logical_date"] = runs[-2]["logical_date"]
    xcom_responses = [_xcom_response(index) for index in range(1, 4)]
    if mutation == "missing_pointer":
        xcom_responses[-1] = {"detail": "XCom entry not found"}
    elif mutation == "replayed_request":
        xcom_responses[-1]["value"]["requestId"] = xcom_responses[-2]["value"]["requestId"]
    scripted: list[dict[str, Any]] = [
        {
            "access_token": _jwt(
                sub=AIRFLOW_HISTORY_OBSERVER_USERNAME,
                issued_at=1_768_435_200,
                expires_at=1_768_435_500,
            ),
            "token_type": "bearer",
        },
        {"git_version": "airflow-3.1.6", "version": "3.1.6"},
        {"dag_runs": runs, "total_entries": len(runs)},
        {"dag_runs": [], "total_entries": 0},
        *xcom_responses,
    ]

    with pytest.raises(ValueError, match=match):
        AirflowD19HistoryClient(
            _history_config(tmp_path),
            transport=lambda **_: scripted.pop(0),
            clock=lambda: datetime(2026, 1, 15, tzinfo=UTC),
        ).collect(parent_logical_date=PARENT_LOGICAL_DATE)


def test_kubernetes_fence_create_is_parent_bound_and_resource_versioned() -> None:
    api = _LeaseApi(read_error=_ApiError(404), create_response=_lease(resource_version="17"))
    client = KubernetesD19HistoryFenceClient(
        api=api,
        clock=lambda: datetime(2026, 7, 17, 0, 0, 5, tzinfo=UTC),
    )

    evidence = client.acquire(parent_airflow_run=_parent_run())

    assert evidence == _fence(resource_version="17")
    assert api.created == [
        (
            "airflow",
            {
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "annotations": {
                        "serp.adapstory.ai/parent-dag-id": "serp_nightly_regression_suite",
                        "serp.adapstory.ai/parent-run-id": "scheduled__2026-07-17",
                    },
                    "name": "serp-d19-history-fence",
                    "namespace": "airflow",
                },
                "spec": {
                    "acquireTime": "2026-07-17T00:00:05Z",
                    "holderIdentity": "d6:scheduled__2026-07-17",
                    "leaseDurationSeconds": 43200,
                    "leaseTransitions": 1,
                    "renewTime": "2026-07-17T00:00:05Z",
                },
            },
        )
    ]


def test_kubernetes_fence_rejects_active_holder_and_cas_replaces_expired_holder() -> None:
    active_api = _LeaseApi(read_response=_lease(resource_version="17"))
    active = KubernetesD19HistoryFenceClient(
        api=active_api,
        clock=lambda: datetime(2026, 7, 17, 0, 0, 6, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="already active"):
        active.acquire(parent_airflow_run={**_parent_run(), "runId": "scheduled__other"})

    expired_lease = _lease(
        acquired_at="2026-07-16T00:00:00Z",
        renew_time="2026-07-16T00:00:00Z",
        resource_version="21",
    )
    replacement = _lease(
        acquired_at="2026-07-17T00:00:05Z",
        renew_time="2026-07-17T00:00:05Z",
        resource_version="22",
        transitions=2,
    )
    expired_api = _LeaseApi(read_response=expired_lease, replace_response=replacement)
    client = KubernetesD19HistoryFenceClient(
        api=expired_api,
        clock=lambda: datetime(2026, 7, 17, 0, 0, 5, tzinfo=UTC),
    )

    assert client.acquire(parent_airflow_run=_parent_run()) == _fence(resource_version="22")
    assert expired_api.replaced[0][2]["metadata"]["resourceVersion"] == "21"
    assert expired_api.replaced[0][2]["spec"]["leaseTransitions"] == 2


def test_kubernetes_fence_release_and_child_validation_are_exact() -> None:
    release_response = _lease(resource_version="18", holder="", transitions=1)
    api = _LeaseApi(
        read_response=_lease(resource_version="17"),
        replace_response=release_response,
    )
    client = KubernetesD19HistoryFenceClient(
        api=api,
        clock=lambda: datetime(2026, 7, 17, 0, 0, 10, tzinfo=UTC),
    )
    evidence = _fence(resource_version="17")

    assert client.require_active(evidence) == evidence
    client.release(evidence)

    assert api.replaced[-1][2]["metadata"]["resourceVersion"] == "17"
    assert api.replaced[-1][2]["spec"]["holderIdentity"] == ""

    wrong_version = _LeaseApi(read_response=_lease(resource_version="99"))
    with pytest.raises(ValueError, match="resourceVersion"):
        KubernetesD19HistoryFenceClient(
            api=wrong_version,
            clock=lambda: datetime(2026, 7, 17, 0, 0, 10, tzinfo=UTC),
        ).require_active(evidence)


def test_manual_d19_is_blocked_only_while_a_real_fence_is_active() -> None:
    active = KubernetesD19HistoryFenceClient(
        api=_LeaseApi(read_response=_lease(resource_version="17")),
        clock=lambda: datetime(2026, 7, 17, 0, 0, 10, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="blocks unfenced D19"):
        active.assert_unfenced_run_allowed()

    expired = KubernetesD19HistoryFenceClient(
        api=_LeaseApi(
            read_response=_lease(
                acquired_at="2026-07-16T00:00:00Z",
                renew_time="2026-07-16T00:00:00Z",
                resource_version="17",
            )
        ),
        clock=lambda: datetime(2026, 7, 17, 0, 0, 10, tzinfo=UTC),
    )
    expired.assert_unfenced_run_allowed()


def test_fence_environment_uses_only_projected_token_file(tmp_path: Path) -> None:
    token_file = tmp_path / "token"
    ca_file = tmp_path / "ca.crt"
    token_file.write_text("projected-kubernetes-jwt\n", encoding="utf-8")
    ca_file.write_text("test-ca\n", encoding="utf-8")
    observed: list[tuple[Path, Path]] = []
    api = _LeaseApi(read_error=_ApiError(404))

    def api_factory(*, token_path: Path, ca_path: Path) -> _LeaseApi:
        observed.append((token_path, ca_path))
        return api

    client = KubernetesD19HistoryFenceClient.from_environment(
        environment={
            "ADAPSTORY_KUBERNETES_API_CA_FILE": str(ca_file),
            "ADAPSTORY_KUBERNETES_API_TOKEN_FILE": str(token_file),
        },
        api_factory=api_factory,
    )

    assert isinstance(client, KubernetesD19HistoryFenceClient)
    assert observed == [(token_file, ca_file)]
    with pytest.raises(ValueError, match="raw Kubernetes bearer tokens are forbidden"):
        KubernetesD19HistoryFenceClient.from_environment(
            environment={
                "ADAPSTORY_KUBERNETES_API_CA_FILE": str(ca_file),
                "ADAPSTORY_KUBERNETES_API_TOKEN_FILE": str(token_file),
                "ADAPSTORY_KUBERNETES_BEARER_TOKEN": "forbidden",
            },
            api_factory=lambda **_: api,
        )


def test_history_attestation_sealer_delegates_to_canonical_pipeline_contract() -> None:
    written = {
        "artifactPath": (
            "s3://airflow-serp-artifacts/serp-evals/op/" "d19-run-history-observation.json"
        ),
        "artifactSha256": "a" * 64,
        "artifactVersionId": "version-history",
        "objectLockMode": "COMPLIANCE",
        "objectLockRetainUntil": "2027-07-17T00:00:00Z",
    }
    transit_client = object()
    s3_client = object()
    calls: list[dict[str, Any]] = []

    def runtime_sealer(receipt: Any, **kwargs: Any) -> tuple[dict[str, str], dict[str, Any]]:
        calls.append({"receipt": receipt, **kwargs})
        return ({"s3Uri": "attestation"}, {"consumerVerification": {"valid": True}})

    result = seal_d19_history_observation_attestation(
        written,
        purpose="serp-d19-run-history-observation",
        transit_client=transit_client,
        s3_client=s3_client,
        runtime_sealer=runtime_sealer,
    )

    assert result == (
        {"s3Uri": "attestation"},
        {"consumerVerification": {"valid": True}},
    )
    assert calls == [
        {
            "purpose": "serp-d19-run-history-observation",
            "receipt": written,
            "s3_client": s3_client,
            "transit_client": transit_client,
        }
    ]
    with pytest.raises(ValueError, match="purpose is unsupported"):
        seal_d19_history_observation_attestation(
            written,
            purpose="serp-paired-evaluation-final-receipt",
            transit_client=transit_client,
            s3_client=s3_client,
            runtime_sealer=runtime_sealer,
        )


def test_d19_admission_requires_the_real_fence_for_scheduled_d6_child() -> None:
    fence = _fence(resource_version="17")
    fence_client = _AdmissionFenceClient(active=fence)
    conf = {
        "generated_at": "2026-07-17T00:00:00Z",
        "scheduled_d6_fence": fence,
    }

    admitted = admit_d19_run(
        dag_run_conf=conf,
        airflow_run=_d19_airflow_run(),
        fence_client=fence_client,
    )

    assert admitted == {
        "airflowRun": _d19_airflow_run(),
        "fence": fence,
        "mode": "scheduled-d6-child",
        "schema": "D19RunAdmission/v1",
    }
    assert fence_client.required == [fence]
    assert fence_client.unfenced_checks == 0

    with pytest.raises(ValueError, match="active Kubernetes fence"):
        admit_d19_run(
            dag_run_conf=conf,
            airflow_run=_d19_airflow_run(),
            fence_client=_AdmissionFenceClient(active=None),
        )


def test_d19_unfenced_admission_is_blocked_during_window_and_checks_logical_date() -> None:
    conf = {"generated_at": "2026-07-17T00:00:00Z"}
    with pytest.raises(ValueError, match="blocks unfenced D19"):
        admit_d19_run(
            dag_run_conf=conf,
            airflow_run=_d19_airflow_run(),
            fence_client=_AdmissionFenceClient(active=_fence(resource_version="17")),
        )

    admitted = admit_d19_run(
        dag_run_conf=conf,
        airflow_run=_d19_airflow_run(),
        fence_client=_AdmissionFenceClient(active=None),
    )
    assert admitted == {
        "airflowRun": _d19_airflow_run(),
        "mode": "unfenced",
        "schema": "D19RunAdmission/v1",
    }

    with pytest.raises(ValueError, match="logicalDate must match generated_at"):
        admit_d19_run(
            dag_run_conf=conf,
            airflow_run={**_d19_airflow_run(), "logicalDate": "2026-07-17T00:00:01Z"},
            fence_client=_AdmissionFenceClient(active=None),
        )


def _history_config(
    tmp_path: Path,
    *,
    base_url: str = AIRFLOW_ORIGIN,
    page_limit: int = 2,
    username: str = AIRFLOW_HISTORY_OBSERVER_USERNAME,
) -> HistoryClientConfig:
    username_file = tmp_path / "username"
    password_file = tmp_path / "password"
    ca_file = tmp_path / "ca.crt"
    username_file.write_text(username + "\n", encoding="utf-8")
    password_file.write_text("observer-password-not-a-real-secret\n", encoding="utf-8")
    ca_file.write_text("test CA", encoding="utf-8")
    return HistoryClientConfig(
        base_url=base_url,
        ca_file=ca_file,
        max_pages=10,
        page_limit=page_limit,
        password_file=password_file,
        timeout_seconds=7,
        username_file=username_file,
    )


def _history_environment(tmp_path: Path) -> dict[str, str]:
    config = _history_config(tmp_path)
    return {
        "ADAPSTORY_AIRFLOW_API_BASE_URL": config.base_url,
        "ADAPSTORY_AIRFLOW_API_CA_FILE": str(config.ca_file),
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_PASSWORD_FILE": str(config.password_file),
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_USERNAME_FILE": str(config.username_file),
    }


def _api_run(index: int) -> dict[str, Any]:
    return {
        "bundle_version": "airflow-dags@sha256:test",
        "conf": {},
        "dag_display_name": "SERP benchmark improvement wave",
        "dag_id": "serp_benchmark_improvement_wave",
        "dag_run_id": f"manual__d19-{index}",
        "dag_versions": [],
        "data_interval_end": None,
        "data_interval_start": None,
        "duration": 120.0,
        "end_date": f"2026-07-{10 + index:02d}T21:02:00Z",
        "last_scheduling_decision": f"2026-07-{10 + index:02d}T21:02:00Z",
        "logical_date": f"2026-07-{10 + index:02d}T21:00:00Z",
        "note": None,
        "queued_at": f"2026-07-{10 + index:02d}T20:59:00Z",
        "run_after": f"2026-07-{10 + index:02d}T21:00:00Z",
        "run_type": "manual",
        "start_date": f"2026-07-{10 + index:02d}T21:00:00Z",
        "state": "success",
        "triggered_by": "rest_api",
        "triggering_user_name": AIRFLOW_HISTORY_OBSERVER_USERNAME,
    }


def _normalized_run(index: int) -> dict[str, str]:
    return {
        "dagId": "serp_benchmark_improvement_wave",
        "logicalDate": f"2026-07-{10 + index:02d}T21:00:00Z",
        "runId": f"manual__d19-{index}",
        "runType": "manual",
        "state": "success",
    }


def _verification_pointer(index: int) -> dict[str, Any]:
    airflow_run = _normalized_run(index)
    airflow_run.pop("state")
    return {
        "airflowRun": airflow_run,
        "observedNormalizedScoreCellsEvidence": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2033-07-17T00:00:00Z",
            "s3Uri": (
                "s3://airflow-serp-evidence/serp-evals/score-cells/"
                f"manual__d19-{index}.json"
            ),
            "sha256": "sha256:" + f"{(index + 7) % 10}" * 64,
            "versionId": f"score-cells-version-d19-{index}",
        },
        "pairedEvaluationVerificationEvidence": {
            "objectLockMode": "COMPLIANCE",
            "retainUntil": "2033-07-17T00:00:00Z",
            "s3Uri": (
                "s3://airflow-serp-evidence/serp-evals/verification/" f"manual__d19-{index}.json"
            ),
            "sha256": "sha256:" + f"{index}" * 64,
            "versionId": f"version-d19-{index}",
        },
        "receiptStatus": "accepted",
        "requestId": f"00000000-0000-4000-a000-{index:012d}",
    }


def _xcom_response(index: int) -> dict[str, Any]:
    return {
        "dag_display_name": "SERP benchmark improvement wave",
        "dag_id": "serp_benchmark_improvement_wave",
        "key": "return_value",
        "logical_date": _normalized_run(index)["logicalDate"],
        "map_index": -1,
        "run_id": f"manual__d19-{index}",
        "task_display_name": "Persist paired evaluation verification evidence",
        "task_id": "persist_paired_evaluation_verification_evidence",
        "timestamp": f"2026-07-{10 + index:02d}T21:03:00Z",
        "value": _verification_pointer(index),
    }


def _jwt(*, sub: str, issued_at: int, expires_at: int) -> str:
    def encoded(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    header = encoded({"alg": "RS256", "typ": "JWT"})
    claims = encoded({"exp": expires_at, "iat": issued_at, "sub": sub})
    return f"{header}.{claims}.signature"


class _ApiError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"API status {status}")
        self.status = status


class _LeaseApi:
    def __init__(
        self,
        *,
        read_response: dict[str, Any] | None = None,
        read_error: Exception | None = None,
        create_response: dict[str, Any] | None = None,
        replace_response: dict[str, Any] | None = None,
    ) -> None:
        self.read_response = read_response
        self.read_error = read_error
        self.create_response = create_response
        self.replace_response = replace_response
        self.created: list[tuple[str, dict[str, Any]]] = []
        self.replaced: list[tuple[str, str, dict[str, Any]]] = []

    def read_namespaced_lease(self, *, name: str, namespace: str) -> dict[str, Any]:
        assert name == "serp-d19-history-fence"
        assert namespace == "airflow"
        if self.read_error is not None:
            raise self.read_error
        assert self.read_response is not None
        return self.read_response

    def create_namespaced_lease(
        self,
        *,
        namespace: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        self.created.append((namespace, body))
        return self.create_response or _lease(resource_version="17")

    def replace_namespaced_lease(
        self,
        *,
        name: str,
        namespace: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        self.replaced.append((name, namespace, body))
        return self.replace_response or body


def _parent_run() -> dict[str, str]:
    return {
        "dagId": "serp_nightly_regression_suite",
        "logicalDate": "2026-07-17T00:00:00Z",
        "runId": "scheduled__2026-07-17",
        "runType": "scheduled",
        "startDate": "2026-07-17T00:00:01Z",
    }


def _fence(*, resource_version: str) -> dict[str, Any]:
    return {
        "acquiredAt": "2026-07-17T00:00:05Z",
        "expiresAt": "2026-07-17T12:00:05Z",
        "holderIdentity": "d6:scheduled__2026-07-17",
        "leaseDurationSeconds": 43200,
        "leaseName": "serp-d19-history-fence",
        "namespace": "airflow",
        "parentDagId": "serp_nightly_regression_suite",
        "parentRunId": "scheduled__2026-07-17",
        "resourceVersion": resource_version,
        "schema": "D19HistoryFence/v1",
    }


def _lease(
    *,
    acquired_at: str = "2026-07-17T00:00:05Z",
    holder: str = "d6:scheduled__2026-07-17",
    renew_time: str = "2026-07-17T00:00:05Z",
    resource_version: str,
    transitions: int = 1,
) -> dict[str, Any]:
    return {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "annotations": {
                "serp.adapstory.ai/parent-dag-id": "serp_nightly_regression_suite",
                "serp.adapstory.ai/parent-run-id": "scheduled__2026-07-17",
            },
            "name": "serp-d19-history-fence",
            "namespace": "airflow",
            "resourceVersion": resource_version,
        },
        "spec": {
            "acquireTime": acquired_at,
            "holderIdentity": holder,
            "leaseDurationSeconds": 43200,
            "leaseTransitions": transitions,
            "renewTime": renew_time,
        },
    }


class _AdmissionFenceClient:
    def __init__(self, *, active: dict[str, Any] | None) -> None:
        self.active = active
        self.required: list[dict[str, Any]] = []
        self.unfenced_checks = 0

    def require_active(self, fence: dict[str, Any]) -> dict[str, Any]:
        self.required.append(fence)
        if self.active is None or self.active != fence:
            raise ValueError("scheduled D19 requires its active Kubernetes fence")
        return fence

    def assert_unfenced_run_allowed(self) -> None:
        self.unfenced_checks += 1
        if self.active is not None:
            raise ValueError("active scheduled D6 fence blocks unfenced D19 admission")


def _d19_airflow_run() -> dict[str, str]:
    return {
        "dagId": "serp_benchmark_improvement_wave",
        "logicalDate": "2026-07-17T00:00:00Z",
        "runId": "manual__d6__scheduled__2026-07-17",
        "runType": "manual",
    }
