"""Least-privilege Airflow v2 history reader for the scheduled D6 fence.

The client intentionally owns no reusable credential.  Each observation reads
the dedicated projected username/password files, exchanges them once through
``POST /auth/token``, validates the bounded JWT envelope, performs one complete
stable-API snapshot, and drops the token when the call returns.
"""

from __future__ import annotations

import base64
import json
import os
import re
import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen
from uuid import UUID

AIRFLOW_HISTORY_OBSERVER_USERNAME = "serp-d19-history-observer"
AIRFLOW_HISTORY_DAG_ID = "serp_benchmark_improvement_wave"
AIRFLOW_HISTORY_API_VERSION = "v2"
AIRFLOW_HISTORY_SUPPORTED_SERVER_MAJOR = 3
AIRFLOW_HISTORY_TRUSTED_AUTHORITY = "airflow-api-server.airflow.svc.cluster.local:8080"
AIRFLOW_HISTORY_TRUSTED_BASE_URL = f"https://{AIRFLOW_HISTORY_TRUSTED_AUTHORITY}"
AIRFLOW_HISTORY_MAX_JWT_SECONDS = 300
AIRFLOW_HISTORY_MAX_PAGE_LIMIT = 500
AIRFLOW_HISTORY_MAX_PAGES = 1_000
AIRFLOW_HISTORY_MAX_TIMEOUT_SECONDS = 30
AIRFLOW_HISTORY_REQUIRED_ACCEPTED_RUNS = 3
AIRFLOW_HISTORY_VERIFICATION_TASK_ID = "persist_paired_evaluation_verification_evidence"
AIRFLOW_HISTORY_VERIFICATION_XCOM_KEY = "return_value"
D19_HISTORY_FENCE_SCHEMA = "D19HistoryFence/v1"
D19_HISTORY_FENCE_LEASE_NAME = "serp-d19-history-fence"
D19_HISTORY_FENCE_NAMESPACE = "airflow"
D19_HISTORY_FENCE_DURATION_SECONDS = 43_200
D19_HISTORY_PARENT_DAG_ID = "serp_nightly_regression_suite"
D19_HISTORY_ATTESTATION_PURPOSE = "serp-d19-run-history-observation"
D19_HISTORY_ATTESTOR_ROLE = "serp-d19-history-observer-attestor-role"
D19_HISTORY_ATTESTOR_SERVICE_ACCOUNT = "airflow-serp-d19-history-observer"
D19_HISTORY_ATTESTOR_TOKEN_POLICY = "serp-d19-history-observer-attestor"
D19_HISTORY_TRANSIT_KEY = "serp-d19-history-observation"
KUBERNETES_TRUSTED_API_ORIGIN = "https://kubernetes.default.svc"
_PARENT_DAG_ANNOTATION = "serp.adapstory.ai/parent-dag-id"
_PARENT_RUN_ANNOTATION = "serp.adapstory.ai/parent-run-id"
_KUBERNETES_RAW_TOKEN_ENV = frozenset(
    {
        "ADAPSTORY_KUBERNETES_BEARER_TOKEN",
        "KUBERNETES_BEARER_TOKEN",
    }
)
_AIRFLOW_HISTORY_RAW_SECRET_ENV = frozenset(
    {
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_PASSWORD",
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_TOKEN",
        "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_USERNAME",
    }
)
_Transport = Callable[..., Mapping[str, Any]]
_Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class HistoryClientConfig:
    """Deployment-owned, non-secret transport configuration."""

    base_url: str
    ca_file: Path
    max_pages: int
    page_limit: int
    password_file: Path
    timeout_seconds: int
    username_file: Path


def build_d19_history_query(
    *,
    parent_logical_date: str,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    """Build the only permitted historical query boundary."""

    logical_date = _datetime_string(parent_logical_date, "parent_logical_date")
    if offset < 0:
        raise ValueError("history query offset must be non-negative")
    if not 1 <= limit <= AIRFLOW_HISTORY_MAX_PAGE_LIMIT:
        raise ValueError("history query page limit is outside the supported bound")
    return {
        "limit": limit,
        "logical_date_lt": logical_date,
        "offset": offset,
        "order_by": ["logical_date", "run_id"],
    }


class AirflowD19HistoryClient:
    """One-shot credential exchanger and exact Airflow stable-API reader."""

    def __init__(
        self,
        config: HistoryClientConfig,
        *,
        transport: _Transport | None = None,
        clock: _Clock | None = None,
    ) -> None:
        self._config = _validated_config(config)
        self._transport = transport or _urllib_json_transport
        self._clock = clock or (lambda: datetime.now(UTC))
        username = _read_credential_file(
            self._config.username_file,
            field_name="history observer username",
            max_bytes=256,
        )
        if username != AIRFLOW_HISTORY_OBSERVER_USERNAME:
            raise ValueError("Airflow history requires the dedicated least-privilege principal")

    @classmethod
    def from_environment(
        cls,
        *,
        environment: Mapping[str, str] | None = None,
        transport: _Transport | None = None,
        clock: _Clock | None = None,
    ) -> AirflowD19HistoryClient:
        values = os.environ if environment is None else environment
        if any(values.get(name) for name in _AIRFLOW_HISTORY_RAW_SECRET_ENV):
            raise ValueError("raw history-observer credentials are forbidden")
        config = HistoryClientConfig(
            base_url=_required_environment(values, "ADAPSTORY_AIRFLOW_API_BASE_URL"),
            ca_file=Path(_required_environment(values, "ADAPSTORY_AIRFLOW_API_CA_FILE")),
            max_pages=_bounded_environment_int(
                values,
                "ADAPSTORY_AIRFLOW_HISTORY_MAX_PAGES",
                default=100,
                minimum=1,
                maximum=AIRFLOW_HISTORY_MAX_PAGES,
            ),
            page_limit=_bounded_environment_int(
                values,
                "ADAPSTORY_AIRFLOW_HISTORY_PAGE_LIMIT",
                default=100,
                minimum=1,
                maximum=AIRFLOW_HISTORY_MAX_PAGE_LIMIT,
            ),
            password_file=Path(
                _required_environment(
                    values,
                    "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_PASSWORD_FILE",
                )
            ),
            timeout_seconds=_bounded_environment_int(
                values,
                "ADAPSTORY_AIRFLOW_HISTORY_TIMEOUT_SECONDS",
                default=10,
                minimum=1,
                maximum=AIRFLOW_HISTORY_MAX_TIMEOUT_SECONDS,
            ),
            username_file=Path(
                _required_environment(
                    values,
                    "ADAPSTORY_AIRFLOW_HISTORY_OBSERVER_USERNAME_FILE",
                )
            ),
        )
        return cls(config, transport=transport, clock=clock)

    def collect(self, *, parent_logical_date: str) -> dict[str, Any]:
        """Exchange one JWT and return one complete, bounded D19 history view."""

        parent_date = _datetime_string(parent_logical_date, "parent_logical_date")
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("history observer clock must be timezone-aware")
        jwt = self._exchange_bounded_jwt(now=now.astimezone(UTC))
        try:
            version = self._authorized_json("GET", "/api/v2/version", jwt=jwt)
            if set(version) != {"git_version", "version"}:
                raise ValueError("Airflow version response fields are unsupported")
            _required_string(version, "git_version")
            server_version = _supported_airflow_history_server_version(
                _required_string(version, "version")
            )
            runs, page_count, total_entries = self._collect_history_pages(
                parent_logical_date=parent_date,
                jwt=jwt,
            )
            self._assert_no_active_runs(jwt=jwt)
            normalized_runs = [
                _normalized_dag_run(run, parent_logical_date=parent_date) for run in runs
            ]
            identities = [(run["logicalDate"], run["runId"]) for run in normalized_runs]
            if identities != sorted(identities):
                raise ValueError("Airflow history response is not in canonical order")
            if len(set(identities)) != len(identities):
                raise ValueError("Airflow history response contains duplicate run identities")
            accepted_verifications = self._collect_accepted_run_verifications(
                normalized_runs,
                parent_logical_date=parent_date,
                jwt=jwt,
            )
        finally:
            jwt = ""  # Do not retain a reusable bearer beyond this observation.
        return {
            "activeRunQuery": {
                "dagId": AIRFLOW_HISTORY_DAG_ID,
                "states": ["queued", "running"],
                "totalEntries": 0,
            },
            "api": {
                "apiVersion": AIRFLOW_HISTORY_API_VERSION,
                "airflowVersion": server_version,
                "serverAuthority": AIRFLOW_HISTORY_TRUSTED_AUTHORITY,
            },
            "acceptedRunVerifications": accepted_verifications,
            "pagination": {
                "complete": True,
                "observedEntries": len(normalized_runs),
                "pageCount": page_count,
                "pageLimit": self._config.page_limit,
                "totalEntries": total_entries,
            },
            "query": {
                "apiPath": f"/api/v2/dags/{AIRFLOW_HISTORY_DAG_ID}/dagRuns",
                "dagId": AIRFLOW_HISTORY_DAG_ID,
                "logicalDateLt": parent_date,
                "orderBy": ["logical_date", "run_id"],
            },
            "runs": normalized_runs,
            "verificationPointerQuery": _verification_pointer_query(),
        }

    def _exchange_bounded_jwt(self, *, now: datetime) -> str:
        username = _read_credential_file(
            self._config.username_file,
            field_name="history observer username",
            max_bytes=256,
        )
        if username != AIRFLOW_HISTORY_OBSERVER_USERNAME:
            raise ValueError("Airflow history requires the dedicated least-privilege principal")
        password = _read_credential_file(
            self._config.password_file,
            field_name="history observer password",
            max_bytes=16_384,
        )
        if len(password) < 16:
            raise ValueError("history observer password file is invalid")
        body = urlencode({"password": password, "username": username}).encode("utf-8")
        response = self._request_json(
            "POST",
            "/auth/token",
            body=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        password = ""
        if set(response) != {"access_token", "token_type"}:
            raise ValueError("Airflow token response fields are unsupported")
        if str(response.get("token_type", "")).casefold() != "bearer":
            raise ValueError("Airflow token type is unsupported")
        token = response.get("access_token")
        if not isinstance(token, str) or not token:
            raise ValueError("Airflow access token is missing")
        _validate_bounded_jwt(token, now=now)
        return token

    def _collect_history_pages(
        self,
        *,
        parent_logical_date: str,
        jwt: str,
    ) -> tuple[list[Mapping[str, Any]], int, int]:
        observed: list[Mapping[str, Any]] = []
        expected_total: int | None = None
        page_count = 0
        for page_index in range(self._config.max_pages):
            query = build_d19_history_query(
                parent_logical_date=parent_logical_date,
                offset=page_index * self._config.page_limit,
                limit=self._config.page_limit,
            )
            response = self._authorized_json(
                "GET",
                f"/api/v2/dags/{AIRFLOW_HISTORY_DAG_ID}/dagRuns?{urlencode(query, doseq=True)}",
                jwt=jwt,
            )
            page, total = _dag_run_page(response)
            if expected_total is None:
                expected_total = total
            elif total != expected_total:
                raise ValueError("Airflow history requires complete pagination with one total")
            if not page:
                if len(observed) != total:
                    raise ValueError("Airflow history requires complete pagination")
                break
            observed.extend(page)
            page_count += 1
            if len(observed) > total:
                raise ValueError("Airflow history pagination exceeds total_entries")
            if len(observed) == total:
                break
        else:
            raise ValueError("Airflow history requires complete pagination within max_pages")
        assert expected_total is not None
        if len(observed) != expected_total:
            raise ValueError("Airflow history requires complete pagination")
        return observed, page_count, expected_total

    def _assert_no_active_runs(self, *, jwt: str) -> None:
        query = {
            "limit": 1,
            "offset": 0,
            "order_by": ["logical_date", "run_id"],
            "state": ["queued", "running"],
        }
        response = self._authorized_json(
            "GET",
            f"/api/v2/dags/{AIRFLOW_HISTORY_DAG_ID}/dagRuns?{urlencode(query, doseq=True)}",
            jwt=jwt,
        )
        runs, total = _dag_run_page(response)
        if runs or total != 0:
            raise ValueError("D19 history fence cannot start while a D19 run is active")

    def _collect_accepted_run_verifications(
        self,
        runs: list[dict[str, str]],
        *,
        parent_logical_date: str,
        jwt: str,
    ) -> list[dict[str, Any]]:
        if len(runs) < AIRFLOW_HISTORY_REQUIRED_ACCEPTED_RUNS:
            raise ValueError("history requires at least three consecutive accepted D19 runs")
        selected = runs[-AIRFLOW_HISTORY_REQUIRED_ACCEPTED_RUNS:]
        if any(run["state"] != "success" for run in selected):
            raise ValueError("last three D19 runs must all succeed without an intervening run")
        if any(run["runType"] != "manual" for run in selected):
            raise ValueError("last three D19 runs must use the admitted manual run type")
        logical_dates = [_parse_datetime(run["logicalDate"]) for run in selected]
        if logical_dates != sorted(logical_dates) or len(set(logical_dates)) != len(logical_dates):
            raise ValueError("last three D19 runs must have strictly increasing logical dates")
        pointers = [
            self._accepted_run_verification_pointer(
                run,
                parent_logical_date=parent_logical_date,
                jwt=jwt,
            )
            for run in selected
        ]
        request_ids = [pointer["requestId"] for pointer in pointers]
        if len(set(request_ids)) != len(request_ids):
            raise ValueError("accepted D19 requestId values must be unique")
        evidence_identities = [
            (
                pointer["pairedEvaluationVerificationEvidence"]["s3Uri"],
                pointer["pairedEvaluationVerificationEvidence"]["versionId"],
            )
            for pointer in pointers
        ]
        if len(set(evidence_identities)) != len(evidence_identities):
            raise ValueError("accepted D19 WORM verification handles must be unique")
        score_evidence_identities = [
            (
                pointer["observedNormalizedScoreCellsEvidence"]["s3Uri"],
                pointer["observedNormalizedScoreCellsEvidence"]["versionId"],
            )
            for pointer in pointers
        ]
        if len(set(score_evidence_identities)) != len(score_evidence_identities):
            raise ValueError("accepted D19 WORM score-cell handles must be unique")
        return pointers

    def _accepted_run_verification_pointer(
        self,
        run: Mapping[str, str],
        *,
        parent_logical_date: str,
        jwt: str,
    ) -> dict[str, Any]:
        run_id = quote(_required_string(run, "runId"), safe="")
        path = (
            f"/api/v2/dags/{AIRFLOW_HISTORY_DAG_ID}/dagRuns/{run_id}/"
            f"taskInstances/{AIRFLOW_HISTORY_VERIFICATION_TASK_ID}/"
            f"xcomEntries/{AIRFLOW_HISTORY_VERIFICATION_XCOM_KEY}"
            "?map_index=-1&deserialize=true&stringify=false"
        )
        response = self._authorized_json("GET", path, jwt=jwt)
        return _normalized_verification_pointer_xcom(
            response,
            expected_run=run,
            parent_logical_date=parent_logical_date,
        )

    def _authorized_json(self, method: str, path: str, *, jwt: str) -> Mapping[str, Any]:
        return self._request_json(
            method,
            path,
            body=None,
            headers={"Accept": "application/json", "Authorization": f"Bearer {jwt}"},
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None,
        headers: Mapping[str, str],
    ) -> Mapping[str, Any]:
        response = self._transport(
            method=method,
            url=self._config.base_url + path,
            headers=dict(headers),
            body=body,
            timeout=self._config.timeout_seconds,
            ca_file=str(self._config.ca_file),
        )
        if not isinstance(response, Mapping):
            raise ValueError("Airflow API response must be a JSON object")
        return response


def _verification_pointer_query() -> dict[str, Any]:
    return {
        "apiPathTemplate": (
            f"/api/v2/dags/{AIRFLOW_HISTORY_DAG_ID}/dagRuns/{{dagRunId}}/"
            f"taskInstances/{AIRFLOW_HISTORY_VERIFICATION_TASK_ID}/"
            f"xcomEntries/{AIRFLOW_HISTORY_VERIFICATION_XCOM_KEY}"
        ),
        "deserialize": True,
        "mapIndex": -1,
        "stringify": False,
        "taskId": AIRFLOW_HISTORY_VERIFICATION_TASK_ID,
        "xcomKey": AIRFLOW_HISTORY_VERIFICATION_XCOM_KEY,
    }


def _normalized_verification_pointer_xcom(
    response: Mapping[str, Any],
    *,
    expected_run: Mapping[str, str],
    parent_logical_date: str,
) -> dict[str, Any]:
    expected_fields = {
        "dag_display_name",
        "dag_id",
        "key",
        "logical_date",
        "map_index",
        "run_id",
        "task_display_name",
        "task_id",
        "timestamp",
        "value",
    }
    if set(response) != expected_fields:
        raise ValueError("D19 verification pointer XCom fields are unsupported")
    if _required_string(response, "dag_id") != AIRFLOW_HISTORY_DAG_ID:
        raise ValueError("D19 verification pointer XCom dag_id does not match")
    if _required_string(response, "run_id") != expected_run["runId"]:
        raise ValueError("D19 verification pointer XCom run_id does not match")
    if _required_string(response, "task_id") != AIRFLOW_HISTORY_VERIFICATION_TASK_ID:
        raise ValueError("D19 verification pointer XCom task_id does not match")
    if _required_string(response, "key") != AIRFLOW_HISTORY_VERIFICATION_XCOM_KEY:
        raise ValueError("D19 verification pointer XCom key does not match")
    map_index = response.get("map_index")
    if not isinstance(map_index, int) or isinstance(map_index, bool) or map_index != -1:
        raise ValueError("D19 verification pointer XCom map_index does not match")
    logical_date = _datetime_string(
        _required_string(response, "logical_date"),
        "D19 verification pointer XCom logical_date",
    )
    if logical_date != expected_run["logicalDate"]:
        raise ValueError("D19 verification pointer XCom logical_date does not match")
    timestamp = _parse_datetime(_required_string(response, "timestamp"))
    if timestamp < _parse_datetime(logical_date) or timestamp >= _parse_datetime(
        parent_logical_date
    ):
        raise ValueError("D19 verification pointer XCom timestamp is outside its run window")
    _required_string(response, "dag_display_name")
    _required_string(response, "task_display_name")
    value = response.get("value")
    if not isinstance(value, Mapping):
        raise ValueError("D19 verification pointer XCom value must be a native object")
    return _normalized_verification_pointer(value, expected_run=expected_run)


def _normalized_verification_pointer(
    value: Mapping[str, Any],
    *,
    expected_run: Mapping[str, str],
) -> dict[str, Any]:
    if set(value) != {
        "airflowRun",
        "observedNormalizedScoreCellsEvidence",
        "pairedEvaluationVerificationEvidence",
        "receiptStatus",
        "requestId",
    }:
        raise ValueError("D19 verification pointer XCom value fields are unsupported")
    airflow_run = value.get("airflowRun")
    if not isinstance(airflow_run, Mapping) or set(airflow_run) != {
        "dagId",
        "logicalDate",
        "runId",
        "runType",
    }:
        raise ValueError("D19 verification pointer airflowRun fields are unsupported")
    normalized_run = {
        "dagId": _required_string(airflow_run, "dagId"),
        "logicalDate": _datetime_string(
            _required_string(airflow_run, "logicalDate"),
            "D19 verification pointer airflowRun.logicalDate",
        ),
        "runId": _required_string(airflow_run, "runId"),
        "runType": _required_string(airflow_run, "runType"),
    }
    expected_identity = {
        key: expected_run[key] for key in ("dagId", "logicalDate", "runId", "runType")
    }
    if normalized_run != expected_identity or normalized_run["runType"] != "manual":
        raise ValueError("D19 verification pointer airflowRun does not match its history run")
    if _required_string(value, "receiptStatus") != "accepted":
        raise ValueError("D19 verification pointer receiptStatus must be accepted")
    request_id = _canonical_uuid(_required_string(value, "requestId"), "requestId")
    evidence = value.get("pairedEvaluationVerificationEvidence")
    if not isinstance(evidence, Mapping):
        raise ValueError("D19 verification pointer WORM evidence is required")
    score_evidence = value.get("observedNormalizedScoreCellsEvidence")
    if not isinstance(score_evidence, Mapping):
        raise ValueError("D19 observed score-cell WORM evidence is required")
    return {
        "airflowRun": normalized_run,
        "observedNormalizedScoreCellsEvidence": _normalized_worm_evidence(score_evidence),
        "pairedEvaluationVerificationEvidence": _normalized_worm_evidence(evidence),
        "receiptStatus": "accepted",
        "requestId": request_id,
    }


def _normalized_worm_evidence(value: Mapping[str, Any]) -> dict[str, str]:
    if set(value) != {"objectLockMode", "retainUntil", "s3Uri", "sha256", "versionId"}:
        raise ValueError("D19 verification pointer WORM evidence fields are unsupported")
    if _required_string(value, "objectLockMode") != "COMPLIANCE":
        raise ValueError("D19 verification pointer WORM evidence must use COMPLIANCE lock")
    retain_until = _datetime_string(
        _required_string(value, "retainUntil"),
        "D19 verification pointer retainUntil",
    )
    s3_uri = _required_string(value, "s3Uri")
    parsed = urlparse(s3_uri)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.endswith(".json")
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("D19 verification pointer WORM evidence S3 URI is invalid")
    digest = _required_string(value, "sha256")
    if not digest.startswith("sha256:") or len(digest) != 71:
        raise ValueError("D19 verification pointer WORM evidence SHA-256 is invalid")
    try:
        int(digest.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise ValueError("D19 verification pointer WORM evidence SHA-256 is invalid") from exc
    return {
        "objectLockMode": "COMPLIANCE",
        "retainUntil": retain_until,
        "s3Uri": s3_uri,
        "sha256": digest,
        "versionId": _required_string(value, "versionId"),
    }


def _canonical_uuid(value: str, field_name: str) -> str:
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise ValueError(f"D19 verification pointer {field_name} is invalid") from exc
    normalized = str(parsed)
    if normalized != value:
        raise ValueError(f"D19 verification pointer {field_name} is not canonical")
    return normalized


class KubernetesD19HistoryFenceClient:
    """CAS-backed D19 exclusion fence on one fixed Kubernetes Lease."""

    def __init__(
        self,
        *,
        api: Any,
        clock: _Clock | None = None,
    ) -> None:
        if api is None:
            raise ValueError("Kubernetes Coordination API client is required")
        self._api = api
        self._clock = clock or (lambda: datetime.now(UTC))

    @classmethod
    def from_environment(
        cls,
        *,
        environment: Mapping[str, str] | None = None,
        clock: _Clock | None = None,
        api_factory: Callable[..., Any] | None = None,
    ) -> KubernetesD19HistoryFenceClient:
        """Build only from explicit projected-token and CA files."""

        values = os.environ if environment is None else environment
        if any(values.get(name) for name in _KUBERNETES_RAW_TOKEN_ENV):
            raise ValueError("raw Kubernetes bearer tokens are forbidden")
        token_path = Path(_required_environment(values, "ADAPSTORY_KUBERNETES_API_TOKEN_FILE"))
        ca_path = Path(_required_environment(values, "ADAPSTORY_KUBERNETES_API_CA_FILE"))
        for path, field_name in (
            (token_path, "projected Kubernetes API token file"),
            (ca_path, "Kubernetes API CA file"),
        ):
            if not path.is_absolute() or not path.is_file():
                raise ValueError(f"{field_name} must be an existing absolute file")
        factory = api_factory or _projected_coordination_api
        return cls(api=factory(token_path=token_path, ca_path=ca_path), clock=clock)

    def acquire(self, *, parent_airflow_run: Mapping[str, Any]) -> dict[str, Any]:
        """CAS-patch a predeclared expired fence; an active foreign holder wins."""

        parent = _normalized_parent_run(parent_airflow_run)
        now = _aware_utc(self._clock(), "Kubernetes fence clock")
        holder = f"d6:{parent['runId']}"
        try:
            raw_existing = self._api.read_namespaced_lease(
                name=D19_HISTORY_FENCE_LEASE_NAME,
                namespace=D19_HISTORY_FENCE_NAMESPACE,
            )
        except Exception as exc:
            if _api_error_status(exc) != 404:
                raise ValueError("Kubernetes D19 fence read failed") from exc
            raise ValueError("Kubernetes D19 fence must be predeclared") from exc

        existing = _normalized_lease(raw_existing)
        if _lease_is_active(existing, observed_at=now):
            if (
                existing["holderIdentity"] == holder
                and existing["parentDagId"] == parent["dagId"]
                and existing["parentRunId"] == parent["runId"]
            ):
                return _fence_evidence(
                    raw_existing,
                    expected_parent=parent,
                    expected_holder=holder,
                )
            raise ValueError("Kubernetes D19 history fence is already active")

        body = _lease_patch(
            parent=parent,
            holder=holder,
            acquired_at=now,
            renew_time=now,
            transitions=existing["leaseTransitions"] + 1,
            resource_version=existing["resourceVersion"],
        )
        try:
            patched = self._api.patch_namespaced_lease(
                name=D19_HISTORY_FENCE_LEASE_NAME,
                namespace=D19_HISTORY_FENCE_NAMESPACE,
                body=body,
            )
        except Exception as exc:
            if _api_error_status(exc) == 409:
                raise ValueError("Kubernetes D19 fence acquisition lost its CAS race") from exc
            raise ValueError("Kubernetes D19 fence patch failed") from exc
        evidence = _fence_evidence(patched, expected_parent=parent, expected_holder=holder)
        if evidence["resourceVersion"] == existing["resourceVersion"]:
            raise ValueError("Kubernetes D19 fence CAS did not advance resourceVersion")
        return evidence

    def require_active(self, fence: Mapping[str, Any]) -> dict[str, Any]:
        """Require the exact live Lease version supplied by scheduled D6."""

        expected = _normalized_fence_evidence(fence)
        now = _aware_utc(self._clock(), "Kubernetes fence clock")
        try:
            observed = _normalized_lease(
                self._api.read_namespaced_lease(
                    name=D19_HISTORY_FENCE_LEASE_NAME,
                    namespace=D19_HISTORY_FENCE_NAMESPACE,
                )
            )
        except Exception as exc:
            raise ValueError("scheduled D19 requires its active Kubernetes fence") from exc
        if not _lease_is_active(observed, observed_at=now):
            raise ValueError("scheduled D19 requires its active Kubernetes fence")
        comparisons = {
            "acquiredAt": expected["acquiredAt"],
            "holderIdentity": expected["holderIdentity"],
            "leaseDurationSeconds": expected["leaseDurationSeconds"],
            "parentDagId": expected["parentDagId"],
            "parentRunId": expected["parentRunId"],
            "resourceVersion": expected["resourceVersion"],
        }
        for field_name, expected_value in comparisons.items():
            if observed[field_name] != expected_value:
                raise ValueError(f"scheduled D19 fence {field_name} does not match")
        return expected

    def assert_unfenced_run_allowed(self) -> None:
        """Block manual D19 admission while scheduled D6 owns the Lease."""

        now = _aware_utc(self._clock(), "Kubernetes fence clock")
        try:
            raw = self._api.read_namespaced_lease(
                name=D19_HISTORY_FENCE_LEASE_NAME,
                namespace=D19_HISTORY_FENCE_NAMESPACE,
            )
        except Exception as exc:
            if _api_error_status(exc) == 404:
                raise ValueError("Kubernetes D19 fence must be predeclared") from exc
            raise ValueError("Kubernetes D19 fence read failed") from exc
        if _lease_is_active(_normalized_lease(raw), observed_at=now):
            raise ValueError("active scheduled D6 fence blocks unfenced D19 admission")

    def release(self, fence: Mapping[str, Any]) -> None:
        """CAS-release only the exact holder and resourceVersion we acquired."""

        expected = self.require_active(fence)
        observed = _normalized_lease(
            self._api.read_namespaced_lease(
                name=D19_HISTORY_FENCE_LEASE_NAME,
                namespace=D19_HISTORY_FENCE_NAMESPACE,
            )
        )
        now = _aware_utc(self._clock(), "Kubernetes fence clock")
        body = _lease_patch(
            parent={
                "dagId": expected["parentDagId"],
                "runId": expected["parentRunId"],
            },
            holder="",
            acquired_at=_parse_datetime(expected["acquiredAt"]),
            renew_time=now,
            transitions=observed["leaseTransitions"],
            resource_version=expected["resourceVersion"],
        )
        try:
            released = _normalized_lease(
                self._api.patch_namespaced_lease(
                    name=D19_HISTORY_FENCE_LEASE_NAME,
                    namespace=D19_HISTORY_FENCE_NAMESPACE,
                    body=body,
                )
            )
        except Exception as exc:
            raise ValueError("Kubernetes D19 fence release CAS failed") from exc
        if released["holderIdentity"] or released["resourceVersion"] == expected["resourceVersion"]:
            raise ValueError("Kubernetes D19 fence release was not persisted")


def admit_d19_run(
    *,
    dag_run_conf: Mapping[str, Any],
    airflow_run: Mapping[str, Any],
    fence_client: KubernetesD19HistoryFenceClient | Any | None = None,
) -> dict[str, Any]:
    """Admit one D19 run without allowing it to race a scheduled D6 snapshot."""

    if not isinstance(dag_run_conf, Mapping):
        raise ValueError("D19 dag_run.conf must be an object")
    expected_run_fields = {"dagId", "logicalDate", "runId", "runType"}
    if set(airflow_run) != expected_run_fields:
        raise ValueError("D19 airflowRun fields are unsupported")
    if _required_string(airflow_run, "dagId") != AIRFLOW_HISTORY_DAG_ID:
        raise ValueError("D19 airflowRun dagId is unsupported")
    if _required_string(airflow_run, "runType") != "manual":
        raise ValueError("D19 airflowRun runType must be manual")
    normalized_run = {
        "dagId": AIRFLOW_HISTORY_DAG_ID,
        "logicalDate": _datetime_string(
            _required_string(airflow_run, "logicalDate"),
            "D19 airflowRun.logicalDate",
        ),
        "runId": _required_string(airflow_run, "runId"),
        "runType": "manual",
    }
    generated_at = _datetime_string(
        _required_string(dag_run_conf, "generated_at"),
        "D19 generated_at",
    )
    if normalized_run["logicalDate"] != generated_at:
        raise ValueError("D19 airflowRun logicalDate must match generated_at")
    client = fence_client or KubernetesD19HistoryFenceClient.from_environment()
    supplied_fence = dag_run_conf.get("scheduled_d6_fence")
    if supplied_fence is not None:
        if not isinstance(supplied_fence, Mapping):
            raise ValueError("scheduled_d6_fence must be an object")
        active_fence = client.require_active(dict(supplied_fence))
        return {
            "airflowRun": normalized_run,
            "fence": active_fence,
            "mode": "scheduled-d6-child",
            "schema": "D19RunAdmission/v1",
        }
    client.assert_unfenced_run_allowed()
    return {
        "airflowRun": normalized_run,
        "mode": "unfenced",
        "schema": "D19RunAdmission/v1",
    }


def seal_d19_history_observation_attestation(
    immutable_write_receipt: Mapping[str, Any],
    *,
    purpose: str,
    transit_client: Any | None = None,
    s3_client: Any | None = None,
    runtime_sealer: Callable[..., tuple[dict[str, str], dict[str, Any]]] | None = None,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Delegate history signing and WORM sealing to the canonical pipeline owner."""

    if purpose != D19_HISTORY_ATTESTATION_PURPOSE:
        raise ValueError("D19 history attestation purpose is unsupported")
    if runtime_sealer is None:
        receipt_module = import_module("adapstory_serp_pipeline.orchestration.paired_eval_receipt")
        runtime_sealer = cast(
            Callable[..., tuple[dict[str, str], dict[str, Any]]],
            receipt_module.seal_runtime_artifact_attestation,
        )
    if transit_client is None:
        transit_module = import_module(
            "adapstory_serp_pipeline.orchestration.vault_transit_attestation"
        )
        owner_contract = (
            transit_module.D19_RUN_HISTORY_OBSERVATION_PURPOSE,
            transit_module.TRUSTED_HISTORY_OBSERVER_AUTH_ROLE,
            transit_module.TRUSTED_HISTORY_OBSERVER_NAMESPACE,
            transit_module.TRUSTED_HISTORY_OBSERVER_SERVICE_ACCOUNT,
            transit_module.TRUSTED_HISTORY_OBSERVER_TOKEN_POLICY,
            transit_module.TRUSTED_HISTORY_TRANSIT_KEY,
        )
        expected_contract = (
            D19_HISTORY_ATTESTATION_PURPOSE,
            D19_HISTORY_ATTESTOR_ROLE,
            D19_HISTORY_FENCE_NAMESPACE,
            D19_HISTORY_ATTESTOR_SERVICE_ACCOUNT,
            D19_HISTORY_ATTESTOR_TOKEN_POLICY,
            D19_HISTORY_TRANSIT_KEY,
        )
        if owner_contract != expected_contract:
            raise ValueError("pipeline D19 history attestation trust contract is mismatched")
        vault_transit_client = transit_module.VaultTransitClient
        transit_client = vault_transit_client.from_environment(
            expected_service_account_namespace=D19_HISTORY_FENCE_NAMESPACE,
            expected_service_account_name=D19_HISTORY_ATTESTOR_SERVICE_ACCOUNT,
            expected_token_policy=D19_HISTORY_ATTESTOR_TOKEN_POLICY,
        )
    if s3_client is None:
        evidence_module = import_module(
            "adapstory_serp_pipeline.orchestration.evidence_web_identity"
        )
        operation_scoped_s3_client = cast(
            Callable[..., Any],
            evidence_module.operation_scoped_s3_client,
        )
        artifact_path = _required_string(immutable_write_receipt, "artifactPath")
        if not artifact_path.startswith("s3://") or not artifact_path.endswith(".json"):
            raise ValueError("D19 history observation requires an S3 JSON artifact")
        attestation_path = artifact_path.removesuffix(".json") + ".attestation.json"
        s3_client = operation_scoped_s3_client(
            read_artifact_uris=(attestation_path,),
            write_artifact_uris=(attestation_path,),
        )
    attestation, verification = runtime_sealer(
        immutable_write_receipt,
        purpose=purpose,
        transit_client=transit_client,
        s3_client=s3_client,
    )
    return dict(attestation), dict(verification)


def _normalized_parent_run(value: Mapping[str, Any]) -> dict[str, str]:
    expected = {"dagId", "logicalDate", "runId", "runType", "startDate"}
    if set(value) != expected:
        raise ValueError("D19 fence parentAirflowRun fields are unsupported")
    if _required_string(value, "dagId") != D19_HISTORY_PARENT_DAG_ID:
        raise ValueError("D19 fence parent dagId is unsupported")
    if _required_string(value, "runType") != "scheduled":
        raise ValueError("D19 fence parent runType must be scheduled")
    logical_date = _datetime_string(_required_string(value, "logicalDate"), "logicalDate")
    start_date = _datetime_string(_required_string(value, "startDate"), "startDate")
    if _parse_datetime(start_date) < _parse_datetime(logical_date):
        raise ValueError("D19 fence parent startDate precedes logicalDate")
    return {
        "dagId": D19_HISTORY_PARENT_DAG_ID,
        "logicalDate": logical_date,
        "runId": _required_string(value, "runId"),
        "runType": "scheduled",
        "startDate": start_date,
    }


def _lease_patch(
    *,
    parent: Mapping[str, str],
    holder: str,
    acquired_at: datetime,
    renew_time: datetime,
    transitions: int,
    resource_version: str,
) -> dict[str, Any]:
    if transitions < 0:
        raise ValueError("Kubernetes D19 fence leaseTransitions is invalid")
    if not resource_version:
        raise ValueError("Kubernetes D19 fence resourceVersion is required")
    return {
        "metadata": {
            "annotations": {
                _PARENT_DAG_ANNOTATION: _required_string(parent, "dagId"),
                _PARENT_RUN_ANNOTATION: _required_string(parent, "runId"),
            },
            "resourceVersion": resource_version,
        },
        "spec": {
            "acquireTime": _utc_string(acquired_at),
            "holderIdentity": holder,
            "leaseDurationSeconds": D19_HISTORY_FENCE_DURATION_SECONDS,
            "leaseTransitions": transitions,
            "renewTime": _utc_string(renew_time),
        },
    }


def _normalized_lease(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        value = raw
    else:
        to_dict = getattr(raw, "to_dict", None)
        if not callable(to_dict):
            raise ValueError("Kubernetes D19 fence response is invalid")
        value = to_dict()
        if not isinstance(value, Mapping):
            raise ValueError("Kubernetes D19 fence response is invalid")
    metadata = _mapping_field(value, "metadata")
    spec = _mapping_field(value, "spec")
    if _aliased_string(metadata, "name") != D19_HISTORY_FENCE_LEASE_NAME:
        raise ValueError("Kubernetes D19 fence Lease name is unsupported")
    if _aliased_string(metadata, "namespace") != D19_HISTORY_FENCE_NAMESPACE:
        raise ValueError("Kubernetes D19 fence Lease namespace is unsupported")
    annotations = _mapping_field(metadata, "annotations")
    parent_dag_id = _aliased_string(annotations, _PARENT_DAG_ANNOTATION)
    parent_run_id = _aliased_string(annotations, _PARENT_RUN_ANNOTATION)
    if parent_dag_id != D19_HISTORY_PARENT_DAG_ID:
        raise ValueError("Kubernetes D19 fence parent annotation is unsupported")
    resource_version = _aliased_string(metadata, "resourceVersion", "resource_version")
    acquired_at = _lease_datetime(_aliased_value(spec, "acquireTime", "acquire_time"))
    renew_at = _lease_datetime(_aliased_value(spec, "renewTime", "renew_time"))
    duration = _aliased_value(spec, "leaseDurationSeconds", "lease_duration_seconds")
    if (
        not isinstance(duration, int)
        or isinstance(duration, bool)
        or duration != D19_HISTORY_FENCE_DURATION_SECONDS
    ):
        raise ValueError("Kubernetes D19 fence lease duration is unsupported")
    transitions = _aliased_value(spec, "leaseTransitions", "lease_transitions")
    if not isinstance(transitions, int) or isinstance(transitions, bool) or transitions < 0:
        raise ValueError("Kubernetes D19 fence leaseTransitions is invalid")
    holder_value = _aliased_value(spec, "holderIdentity", "holder_identity")
    if holder_value is None:
        holder = ""
    elif isinstance(holder_value, str):
        holder = holder_value.strip()
    else:
        raise ValueError("Kubernetes D19 fence holderIdentity is invalid")
    return {
        "acquiredAt": _utc_string(acquired_at),
        "holderIdentity": holder,
        "leaseDurationSeconds": duration,
        "leaseTransitions": transitions,
        "parentDagId": parent_dag_id,
        "parentRunId": parent_run_id,
        "renewTime": _utc_string(renew_at),
        "resourceVersion": resource_version,
    }


def _fence_evidence(
    raw: Any,
    *,
    expected_parent: Mapping[str, str],
    expected_holder: str,
) -> dict[str, Any]:
    lease = _normalized_lease(raw)
    if (
        lease["holderIdentity"] != expected_holder
        or lease["parentDagId"] != expected_parent["dagId"]
        or lease["parentRunId"] != expected_parent["runId"]
    ):
        raise ValueError("Kubernetes D19 fence response does not match its parent holder")
    acquired_at = _parse_datetime(lease["acquiredAt"])
    return {
        "acquiredAt": lease["acquiredAt"],
        "expiresAt": _utc_string(
            acquired_at + timedelta(seconds=D19_HISTORY_FENCE_DURATION_SECONDS)
        ),
        "holderIdentity": expected_holder,
        "leaseDurationSeconds": D19_HISTORY_FENCE_DURATION_SECONDS,
        "leaseName": D19_HISTORY_FENCE_LEASE_NAME,
        "namespace": D19_HISTORY_FENCE_NAMESPACE,
        "parentDagId": expected_parent["dagId"],
        "parentRunId": expected_parent["runId"],
        "resourceVersion": lease["resourceVersion"],
        "schema": D19_HISTORY_FENCE_SCHEMA,
    }


def _normalized_fence_evidence(value: Mapping[str, Any]) -> dict[str, Any]:
    expected_fields = {
        "acquiredAt",
        "expiresAt",
        "holderIdentity",
        "leaseDurationSeconds",
        "leaseName",
        "namespace",
        "parentDagId",
        "parentRunId",
        "resourceVersion",
        "schema",
    }
    if set(value) != expected_fields:
        raise ValueError("D19 history fence evidence fields are unsupported")
    if _required_string(value, "schema") != D19_HISTORY_FENCE_SCHEMA:
        raise ValueError("D19 history fence evidence schema is unsupported")
    if _required_string(value, "leaseName") != D19_HISTORY_FENCE_LEASE_NAME:
        raise ValueError("D19 history fence evidence leaseName is unsupported")
    if _required_string(value, "namespace") != D19_HISTORY_FENCE_NAMESPACE:
        raise ValueError("D19 history fence evidence namespace is unsupported")
    parent_dag_id = _required_string(value, "parentDagId")
    parent_run_id = _required_string(value, "parentRunId")
    if parent_dag_id != D19_HISTORY_PARENT_DAG_ID:
        raise ValueError("D19 history fence evidence parentDagId is unsupported")
    holder = _required_string(value, "holderIdentity")
    if holder != f"d6:{parent_run_id}":
        raise ValueError("D19 history fence evidence holderIdentity does not match")
    duration = value.get("leaseDurationSeconds")
    if duration != D19_HISTORY_FENCE_DURATION_SECONDS:
        raise ValueError("D19 history fence evidence duration is unsupported")
    acquired = _parse_datetime(_required_string(value, "acquiredAt"))
    expires = _parse_datetime(_required_string(value, "expiresAt"))
    if expires - acquired != timedelta(seconds=D19_HISTORY_FENCE_DURATION_SECONDS):
        raise ValueError("D19 history fence evidence expiry is unsupported")
    return {
        "acquiredAt": _utc_string(acquired),
        "expiresAt": _utc_string(expires),
        "holderIdentity": holder,
        "leaseDurationSeconds": D19_HISTORY_FENCE_DURATION_SECONDS,
        "leaseName": D19_HISTORY_FENCE_LEASE_NAME,
        "namespace": D19_HISTORY_FENCE_NAMESPACE,
        "parentDagId": parent_dag_id,
        "parentRunId": parent_run_id,
        "resourceVersion": _required_string(value, "resourceVersion"),
        "schema": D19_HISTORY_FENCE_SCHEMA,
    }


def _lease_is_active(lease: Mapping[str, Any], *, observed_at: datetime) -> bool:
    holder = lease.get("holderIdentity")
    if not isinstance(holder, str) or not holder:
        return False
    renew_time = _parse_datetime(_required_string(lease, "renewTime"))
    return observed_at < renew_time + timedelta(seconds=D19_HISTORY_FENCE_DURATION_SECONDS)


def _mapping_field(value: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    item = value.get(field_name)
    if not isinstance(item, Mapping):
        raise ValueError(f"Kubernetes D19 fence {field_name} is invalid")
    return item


def _aliased_value(value: Mapping[str, Any], *field_names: str) -> Any:
    found = [value[name] for name in field_names if name in value]
    if len(found) != 1:
        raise ValueError(f"Kubernetes D19 fence {field_names[0]} is invalid")
    return found[0]


def _aliased_string(value: Mapping[str, Any], *field_names: str) -> str:
    item = _aliased_value(value, *field_names)
    if not isinstance(item, str) or not item.strip():
        raise ValueError(f"Kubernetes D19 fence {field_names[0]} is invalid")
    return item.strip()


def _lease_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _aware_utc(value, "Kubernetes D19 fence datetime")
    if isinstance(value, str):
        return _parse_datetime(value).astimezone(UTC)
    raise ValueError("Kubernetes D19 fence datetime is invalid")


def _aware_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _utc_string(value: datetime) -> str:
    return _aware_utc(value, "datetime").isoformat().replace("+00:00", "Z")


def _api_error_status(exc: Exception) -> int | None:
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) and not isinstance(status, bool) else None


def _projected_coordination_api(*, token_path: Path, ca_path: Path) -> Any:
    from kubernetes import client

    configuration = client.Configuration(
        host=KUBERNETES_TRUSTED_API_ORIGIN,
        ssl_ca_cert=str(ca_path),
    )
    configuration.verify_ssl = True

    def refresh_api_key(config: Any) -> None:
        config.api_key["authorization"] = _read_credential_file(
            token_path,
            field_name="projected Kubernetes API token",
            max_bytes=32_768,
        )
        config.api_key_prefix["authorization"] = "Bearer"

    configuration.refresh_api_key_hook = refresh_api_key
    refresh_api_key(configuration)
    return client.CoordinationV1Api(client.ApiClient(configuration=configuration))


def _validated_config(config: HistoryClientConfig) -> HistoryClientConfig:
    parsed = urlparse(config.base_url)
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or parsed.netloc != AIRFLOW_HISTORY_TRUSTED_AUTHORITY
    ):
        raise ValueError("Airflow history API must use the trusted HTTPS origin")
    if not 1 <= config.timeout_seconds <= AIRFLOW_HISTORY_MAX_TIMEOUT_SECONDS:
        raise ValueError("history observer timeout is outside the supported bound")
    if not 1 <= config.page_limit <= AIRFLOW_HISTORY_MAX_PAGE_LIMIT:
        raise ValueError("history observer page limit is outside the supported bound")
    if not 1 <= config.max_pages <= AIRFLOW_HISTORY_MAX_PAGES:
        raise ValueError("history observer max_pages is outside the supported bound")
    for path, field_name in (
        (config.ca_file, "Airflow API CA file"),
        (config.username_file, "history observer username file"),
        (config.password_file, "history observer password file"),
    ):
        if not path.is_absolute() or not path.is_file():
            raise ValueError(f"{field_name} must be an existing absolute file")
    return HistoryClientConfig(
        base_url=config.base_url.rstrip("/"),
        ca_file=config.ca_file,
        max_pages=config.max_pages,
        page_limit=config.page_limit,
        password_file=config.password_file,
        timeout_seconds=config.timeout_seconds,
        username_file=config.username_file,
    )


def _validate_bounded_jwt(token: str, *, now: datetime) -> None:
    parts = token.split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError("Airflow access token is not a JWT")
    header = _jwt_part(parts[0], "header")
    claims = _jwt_part(parts[1], "claims")
    if header.get("alg") in {None, "none", "None"}:
        raise ValueError("Airflow JWT algorithm is unsupported")
    if claims.get("sub") != AIRFLOW_HISTORY_OBSERVER_USERNAME:
        raise ValueError("Airflow JWT subject is not the dedicated history observer")
    issued_at = claims.get("iat")
    expires_at = claims.get("exp")
    if not isinstance(issued_at, int) or isinstance(issued_at, bool):
        raise ValueError("Airflow JWT iat is invalid")
    if not isinstance(expires_at, int) or isinstance(expires_at, bool):
        raise ValueError("Airflow JWT exp is invalid")
    lifetime = expires_at - issued_at
    if lifetime <= 0:
        raise ValueError("Airflow JWT lifetime is invalid")
    if lifetime > AIRFLOW_HISTORY_MAX_JWT_SECONDS:
        raise ValueError("Airflow JWT lifetime exceeds 300 seconds")
    now_epoch = int(now.timestamp())
    if issued_at > now_epoch + 5 or expires_at <= now_epoch:
        raise ValueError("Airflow JWT is not current")


def _jwt_part(value: str, field_name: str) -> Mapping[str, Any]:
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode(value + padding)
        payload = json.loads(decoded)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Airflow JWT {field_name} is invalid") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"Airflow JWT {field_name} is invalid")
    return payload


def _dag_run_page(response: Mapping[str, Any]) -> tuple[list[Mapping[str, Any]], int]:
    if set(response) != {"dag_runs", "total_entries"}:
        raise ValueError("Airflow dag-run page fields are unsupported")
    raw_runs = response.get("dag_runs")
    total_entries = response.get("total_entries")
    if not isinstance(raw_runs, list) or not all(isinstance(run, Mapping) for run in raw_runs):
        raise ValueError("Airflow dag-run page is invalid")
    if not isinstance(total_entries, int) or isinstance(total_entries, bool) or total_entries < 0:
        raise ValueError("Airflow dag-run total_entries is invalid")
    return list(raw_runs), total_entries


def _normalized_dag_run(
    run: Mapping[str, Any],
    *,
    parent_logical_date: str,
) -> dict[str, str]:
    dag_id = _required_string(run, "dag_id")
    if dag_id != AIRFLOW_HISTORY_DAG_ID:
        raise ValueError("Airflow history contains a foreign dag_id")
    logical_date = _datetime_string(
        _required_string(run, "logical_date"),
        "dag run logical_date",
    )
    if _parse_datetime(logical_date) >= _parse_datetime(parent_logical_date):
        raise ValueError("Airflow history query returned a run outside logical_date_lt")
    run_type = _required_string(run, "run_type")
    if run_type not in {"manual", "scheduled", "backfill", "asset_triggered"}:
        raise ValueError("Airflow history run_type is unsupported")
    state = _required_string(run, "state")
    if state not in {
        "failed",
        "queued",
        "running",
        "success",
    }:
        raise ValueError("Airflow history state is unsupported")
    return {
        "dagId": dag_id,
        "logicalDate": logical_date,
        "runId": _required_string(run, "dag_run_id"),
        "runType": run_type,
        "state": state,
    }


def _read_credential_file(path: Path, *, field_name: str, max_bytes: int) -> str:
    size = path.stat().st_size
    if size <= 0 or size > max_bytes:
        raise ValueError(f"{field_name} has an invalid size")
    value = path.read_text(encoding="utf-8").strip()
    if not value or "\x00" in value:
        raise ValueError(f"{field_name} is invalid")
    return value


def _required_environment(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value.strip()


def _bounded_environment_int(
    environment: Mapping[str, str],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = environment.get(name, str(default))
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside the supported bound")
    return value


def _required_string(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")
    return value.strip()


def _supported_airflow_history_server_version(value: str) -> str:
    """Validate the stable Airflow v2 API's supported server major line."""

    stable_semver = (
        rf"{AIRFLOW_HISTORY_SUPPORTED_SERVER_MAJOR}\." r"(?:0|[1-9][0-9]*)\." r"(?:0|[1-9][0-9]*)"
    )
    if re.fullmatch(stable_semver, value) is None:
        raise ValueError("Airflow server version is outside the supported 3.x contract")
    return value


def _datetime_string(value: str, field_name: str) -> str:
    parsed = _parse_datetime(value)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("history observer datetime is invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError("history observer datetime must be timezone-aware")
    return parsed


def _urllib_json_transport(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None,
    timeout: int,
    ca_file: str,
) -> Mapping[str, Any]:
    request = Request(url, data=body, headers=dict(headers), method=method)
    context = ssl.create_default_context(cafile=ca_file)
    try:
        with urlopen(request, timeout=timeout, context=context) as response:
            status = int(response.status)
            payload = response.read(16_000_001)
    except HTTPError as exc:
        raise ValueError(f"Airflow API request failed with HTTP {exc.code}") from exc
    except (TimeoutError, URLError, OSError) as exc:
        raise ValueError("Airflow API request failed") from exc
    if status < 200 or status >= 300:
        raise ValueError(f"Airflow API request failed with HTTP {status}")
    if len(payload) > 16_000_000:
        raise ValueError("Airflow API response exceeds the supported bound")
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Airflow API response is not valid JSON") from exc
    if not isinstance(decoded, Mapping):
        raise ValueError("Airflow API response must be a JSON object")
    return decoded
