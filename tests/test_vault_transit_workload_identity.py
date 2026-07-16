from __future__ import annotations

from dags.serp_evidence_workload_identity import (
    EVALUATION_ADMISSION_VERIFIER_SERVICE_ACCOUNT,
    EVALUATION_ADMISSION_VERIFIER_VAULT_AUTH_ROLE,
    VAULT_INTERNAL_CA_CONFIG_MAP,
    VAULT_INTERNAL_CA_FILE,
    VAULT_KUBERNETES_TOKEN_AUDIENCE,
    VAULT_KUBERNETES_TOKEN_EXPIRATION_SECONDS,
    VAULT_KUBERNETES_TOKEN_FILE,
    evaluation_admission_verifier_executor_config,
    vault_transit_minio_executor_config,
)


def test_vault_executor_projects_exact_audience_and_separate_public_ca() -> None:
    config = vault_transit_minio_executor_config(
        service_account_name="airflow-serp-benchmark-aggregator",
        labels={"adapstory.com/serp-network-profile": "benchmark-aggregator"},
        auth_role="serp-evaluation-runtime-attestor-role",
    )

    pod = config["pod_override"]
    assert pod.spec.automount_service_account_token is False
    assert pod.spec.service_account_name == "airflow-serp-benchmark-aggregator"
    container = pod.spec.containers[0]
    env = {item.name: item.value for item in container.env}
    assert env == {
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_WEB_IDENTITY_TOKEN_FILE": (
            "/var/run/secrets/adapstory/minio-web-identity/token"
        ),
        "ADAPSTORY_AIRFLOW_ARTIFACT_S3_STS_DURATION_SECONDS": '"900"',
        "ADAPSTORY_VAULT_ADDR": "https://vault.vault.svc.cluster.local:8200",
        "ADAPSTORY_VAULT_KUBERNETES_AUTH_ROLE": ("serp-evaluation-runtime-attestor-role"),
        "ADAPSTORY_VAULT_KUBERNETES_TOKEN_FILE": VAULT_KUBERNETES_TOKEN_FILE,
        "SSL_CERT_FILE": VAULT_INTERNAL_CA_FILE,
    }
    volumes = {item.name: item for item in pod.spec.volumes}
    token_projection = volumes["vault-kubernetes-token"].projected.sources[0]
    assert token_projection.service_account_token.audience == VAULT_KUBERNETES_TOKEN_AUDIENCE
    assert (
        token_projection.service_account_token.expiration_seconds
        == VAULT_KUBERNETES_TOKEN_EXPIRATION_SECONDS
    )
    assert token_projection.service_account_token.path == "token"
    assert volumes["vault-internal-ca"].config_map.name == VAULT_INTERNAL_CA_CONFIG_MAP
    mounts = {item.name: item for item in container.volume_mounts}
    assert mounts["vault-kubernetes-token"].read_only is True
    assert mounts["vault-internal-ca"].read_only is True


def test_vault_executor_rejects_non_evaluation_auth_role() -> None:
    try:
        vault_transit_minio_executor_config(
            service_account_name="airflow-serp-benchmark-aggregator",
            labels={"adapstory.com/serp-network-profile": "benchmark-aggregator"},
            auth_role="default",
        )
    except ValueError as exc:
        assert "evaluation Vault role" in str(exc)
    else:
        raise AssertionError("non-evaluation Vault role was accepted")


def test_admission_verifier_has_a_fixed_verify_only_identity() -> None:
    config = evaluation_admission_verifier_executor_config(
        labels={"adapstory.com/serp-network-profile": "evaluation-admission-verifier"}
    )

    pod = config["pod_override"]
    assert pod.spec.automount_service_account_token is False
    assert pod.spec.service_account_name == EVALUATION_ADMISSION_VERIFIER_SERVICE_ACCOUNT
    assert pod.metadata.labels == {
        "adapstory.com/serp-network-profile": "evaluation-admission-verifier"
    }
    container = pod.spec.containers[0]
    env = {item.name: item.value for item in container.env}
    assert env == {
        "ADAPSTORY_EVALUATION_EVIDENCE_S3_WEB_IDENTITY_TOKEN_FILE": (
            "/var/run/secrets/adapstory/minio-web-identity/token"
        ),
        "ADAPSTORY_EVALUATION_EVIDENCE_S3_STS_DURATION_SECONDS": '"900"',
        "ADAPSTORY_VAULT_ADDR": "https://vault.vault.svc.cluster.local:8200",
        "ADAPSTORY_VAULT_KUBERNETES_AUTH_ROLE": (EVALUATION_ADMISSION_VERIFIER_VAULT_AUTH_ROLE),
        "ADAPSTORY_VAULT_KUBERNETES_TOKEN_FILE": VAULT_KUBERNETES_TOKEN_FILE,
        "SSL_CERT_FILE": VAULT_INTERNAL_CA_FILE,
    }
    volumes = {item.name: item for item in pod.spec.volumes}
    assert (
        volumes["minio-web-identity-token"].projected.sources[0].service_account_token.audience
        == "minio"
    )
    assert (
        volumes["vault-kubernetes-token"].projected.sources[0].service_account_token.audience
        == "vault"
    )


def test_signer_executor_cannot_select_the_verifier_role() -> None:
    try:
        vault_transit_minio_executor_config(
            service_account_name="airflow-serp-benchmark-aggregator",
            labels={"adapstory.com/serp-network-profile": "benchmark-aggregator"},
            auth_role=EVALUATION_ADMISSION_VERIFIER_VAULT_AUTH_ROLE,
        )
    except ValueError as exc:
        assert "evaluation Vault role" in str(exc)
    else:
        raise AssertionError("signer executor accepted the verify-only Vault role")
